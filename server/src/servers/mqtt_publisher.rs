//! Presence-driven MQTT publisher: mirrors the internal broadcast stream to
//! `AWS IoT Core`, but only for (coin, variant) pairs some browser is watching.
//! Browsers heartbeat `order-book-presence/...` topics; a watch expires when
//! the beats stop. Payloads are the exact WS wire format.

use crate::{
    listeners::order_book::{InternalMessage, L2SnapshotParams, OrderBookListener, RECENT_TRADES_CAP},
    prelude::*,
    servers::payloads,
    types::{
        Trade,
        subscription::{MAX_LEVELS, ServerResponse},
    },
};
use log::{info, warn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeReasonCode, TlsConfiguration, Transport};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    select,
    sync::{
        Mutex,
        broadcast::{Receiver, error::RecvError},
    },
};

// The deployed IoT policies pin these prefixes: publisher may publish only
// order-book-stream/* and order-book-snapshot/*, browsers only
// order-book-presence/*. Renaming here requires an IoT policy deploy.
const PRESENCE_FILTER: &str = "order-book-presence/#";
const HEALTH_TOPIC: &str = "order-book-stream/health";

const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(5);
// Legitimate maximum is ~200 coins x 7 variants + 200 trades ≈ 1600; the cap
// only exists so presence spam can't grow the registry without bound.
const MAX_WATCHES: usize = 5_000;
// AWS IoT allows ~100 publishes/s per connection; staying under it means
// throttling happens here (with fair rotation) instead of at the broker.
// This is also the abuse cost ceiling: ~80 msg/s is ~$210/mo worst case.
const PUBLISH_RATE_PER_SEC: f64 = 80.0;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const WARN_INTERVAL: Duration = Duration::from_secs(10);
const SNAPSHOT_FALLBACK_DEBOUNCE: Duration = Duration::from_secs(1);
// Frames queued while the connection is down share this channel with the
// post-reconnect resubscribe; headroom keeps a backlog from starving it.
const MQTT_CHANNEL_CAP: usize = 512;
// AWS IoT caps MQTT packets at 128KB, and rumqttc raises a violation inside
// the event loop as a connection-level error — one oversized frame tears the
// connection down (2026-09-04: a 148KB trades frame). Payloads are therefore
// capped before publish, with margin for topic + protocol overhead.
const MAX_PACKET_SIZE: usize = 128 * 1024;
const MAX_PAYLOAD_BYTES: usize = MAX_PACKET_SIZE - 4 * 1024;

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub endpoint: String,
    pub port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub client_id_prefix: String,
    pub l2_levels: usize,
    pub trades_seed_interval: Duration,
    pub watch_expiry: Duration,
}

pub(crate) struct MqttPems {
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl MqttPems {
    // Startup-fatal by design: a bad path should fail the deploy loudly, not
    // leave a server running that never publishes.
    pub(crate) fn load(config: &MqttConfig) -> Result<Self> {
        let read = |path: &PathBuf, what: &str| -> Result<Vec<u8>> {
            fs::read(path).map_err(|err| format!("reading MQTT {what} {}: {err}", path.display()).into())
        };
        Ok(Self {
            ca: read(&config.ca_path, "CA")?,
            cert: read(&config.cert_path, "certificate")?,
            key: read(&config.key_path, "private key")?,
        })
    }
}

/// The seven aggregations `compute_l2_snapshots` produces for every coin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VariantKey {
    Full,
    Sf2,
    Sf3,
    Sf4,
    Sf5,
    Sf5M2,
    Sf5M5,
}

impl VariantKey {
    pub(crate) const ALL: [Self; 7] =
        [Self::Full, Self::Sf2, Self::Sf3, Self::Sf4, Self::Sf5, Self::Sf5M2, Self::Sf5M5];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Sf2 => "sf2",
            Self::Sf3 => "sf3",
            Self::Sf4 => "sf4",
            Self::Sf5 => "sf5",
            Self::Sf5M2 => "sf5m2",
            Self::Sf5M5 => "sf5m5",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }

    pub(crate) const fn params(self) -> L2SnapshotParams {
        match self {
            Self::Full => L2SnapshotParams::new(None, None),
            Self::Sf2 => L2SnapshotParams::new(Some(2), None),
            Self::Sf3 => L2SnapshotParams::new(Some(3), None),
            Self::Sf4 => L2SnapshotParams::new(Some(4), None),
            Self::Sf5 => L2SnapshotParams::new(Some(5), None),
            Self::Sf5M2 => L2SnapshotParams::new(Some(5), Some(2)),
            Self::Sf5M5 => L2SnapshotParams::new(Some(5), Some(5)),
        }
    }
}

// Presence topics are attacker-controlled input (the browser policy lets
// anyone publish under order-book-presence/*), so parsing is strict: exact
// segment counts and a conservative coin alphabet. Spot coins ("@142",
// "PURR/USDC") are rejected like the WS server rejects them.
fn is_valid_coin(coin: &str) -> bool {
    !coin.is_empty() && coin.len() <= 32 && coin.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum WatchKey {
    L2Book { coin: String, variant: VariantKey },
    Trades { coin: String },
}

impl WatchKey {
    pub(crate) fn from_presence_topic(topic: &str) -> Option<Self> {
        let mut parts = topic.split('/');
        if parts.next()? != "order-book-presence" {
            return None;
        }
        let key = match parts.next()? {
            "l2Book" => {
                let coin = parts.next()?;
                let variant = VariantKey::parse(parts.next()?)?;
                if !is_valid_coin(coin) {
                    return None;
                }
                Self::L2Book { coin: coin.to_string(), variant }
            }
            "trades" => {
                let coin = parts.next()?;
                if !is_valid_coin(coin) {
                    return None;
                }
                Self::Trades { coin: coin.to_string() }
            }
            _ => return None,
        };
        if parts.next().is_some() {
            return None;
        }
        Some(key)
    }

    // The topic whose retained frame must be cleared when this watch expires.
    // Live trades frames are not retained, so only the seed topic matters.
    fn retained_topic(&self) -> String {
        match self {
            Self::L2Book { coin, variant } => l2_stream_topic(coin, *variant),
            Self::Trades { coin } => trades_snapshot_topic(coin),
        }
    }
}

fn l2_stream_topic(coin: &str, variant: VariantKey) -> String {
    format!("order-book-stream/l2Book/{coin}/{}", variant.as_str())
}

fn trades_stream_topic(coin: &str) -> String {
    format!("order-book-stream/trades/{coin}")
}

fn trades_snapshot_topic(coin: &str) -> String {
    format!("order-book-snapshot/trades/{coin}")
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BeatOutcome {
    New,
    Refreshed,
    Rejected,
}

pub(crate) struct WatchRegistry {
    watches: HashMap<WatchKey, Instant>,
    expiry: Duration,
    max_watches: usize,
}

impl WatchRegistry {
    pub(crate) fn new(expiry: Duration, max_watches: usize) -> Self {
        Self { watches: HashMap::new(), expiry, max_watches }
    }

    pub(crate) fn beat(&mut self, key: WatchKey, now: Instant) -> BeatOutcome {
        if let Some(last) = self.watches.get_mut(&key) {
            *last = now;
            return BeatOutcome::Refreshed;
        }
        if self.watches.len() >= self.max_watches {
            return BeatOutcome::Rejected;
        }
        self.watches.insert(key, now);
        BeatOutcome::New
    }

    pub(crate) fn sweep(&mut self, now: Instant) -> Vec<WatchKey> {
        let expired: Vec<WatchKey> = self
            .watches
            .iter()
            .filter(|(_, last)| now.duration_since(**last) >= self.expiry)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &expired {
            self.watches.remove(key);
        }
        expired
    }

    // Sorted so frame-to-frame rotation walks a stable order; HashMap order
    // would make the rotation offset meaningless.
    fn l2_watches(&self) -> Vec<(String, VariantKey)> {
        let mut watches: Vec<(String, VariantKey)> = self
            .watches
            .keys()
            .filter_map(|key| match key {
                WatchKey::L2Book { coin, variant } => Some((coin.clone(), *variant)),
                WatchKey::Trades { .. } => None,
            })
            .collect();
        watches.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        watches
    }

    fn trades_watched(&self, coin: &str) -> bool {
        self.watches.contains_key(&WatchKey::Trades { coin: coin.to_string() })
    }
}

pub(crate) struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    pub(crate) const fn new(capacity: f64, refill_per_sec: f64, now: Instant) -> Self {
        Self { tokens: capacity, capacity, refill_per_sec, last: now }
    }

    pub(crate) fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = elapsed.mul_add(self.refill_per_sec, self.tokens).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct WarnLimiter {
    last: Option<Instant>,
    interval: Duration,
}

impl WarnLimiter {
    const fn new(interval: Duration) -> Self {
        Self { last: None, interval }
    }

    fn allow(&mut self, now: Instant) -> bool {
        if self.last.is_none_or(|last| now.duration_since(last) >= self.interval) {
            self.last = Some(now);
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
struct TradesSeed {
    ring: VecDeque<Trade>,
    dirty: bool,
    last_published: Option<Instant>,
}

pub(crate) fn seed_due(dirty: bool, last_published: Option<Instant>, now: Instant, interval: Duration) -> bool {
    dirty && last_published.is_none_or(|last| now.duration_since(last) >= interval)
}

// Trade JSON has no fixed size (px/sz digit counts vary), so the seed keeps
// the newest suffix of the ring that fits the packet cap. Returns the frame
// and how many oldest trades were dropped; None if nothing fits.
fn capped_trades_seed(ring: &VecDeque<Trade>) -> Option<(Vec<u8>, usize)> {
    let mut skip = 0;
    loop {
        let response = ServerResponse::Trades(ring.iter().skip(skip).cloned().collect());
        let payload = match serde_json::to_vec(&response) {
            Ok(payload) => payload,
            Err(err) => {
                warn!("Trades seed serialization error: {err}");
                return None;
            }
        };
        if payload.len() <= MAX_PAYLOAD_BYTES {
            return (skip < ring.len()).then_some((payload, skip));
        }
        let avg = payload.len() / (ring.len() - skip);
        skip = (skip + (payload.len() - MAX_PAYLOAD_BYTES) / avg.max(1) + 1).min(ring.len());
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

struct Publisher {
    client: AsyncClient,
    config: MqttConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    registry: WatchRegistry,
    bucket: TokenBucket,
    trades: HashMap<String, TradesSeed>,
    // Latest broadcast Snapshot, kept as the Arc so new-watch seeds are built
    // without touching the listener mutex.
    last_snapshot: Option<Arc<InternalMessage>>,
    last_snapshot_fallback: Option<Instant>,
    rr_offset: usize,
    drop_warn: WarnLimiter,
    reject_warn: WarnLimiter,
    oversize_warn: WarnLimiter,
    // False from ConnAck until the SubAck lands: try_subscribe can fail when
    // the request channel is still full right after a reconnect, and a missed
    // subscribe otherwise leaves the publisher deaf to presence forever.
    presence_subscribed: bool,
}

impl Publisher {
    fn new(client: AsyncClient, mut config: MqttConfig, listener: Arc<Mutex<OrderBookListener>>) -> Self {
        config.l2_levels = config.l2_levels.min(MAX_LEVELS);
        let registry = WatchRegistry::new(config.watch_expiry, MAX_WATCHES);
        Self {
            client,
            config,
            listener,
            registry,
            bucket: TokenBucket::new(PUBLISH_RATE_PER_SEC, PUBLISH_RATE_PER_SEC, Instant::now()),
            trades: HashMap::new(),
            last_snapshot: None,
            last_snapshot_fallback: None,
            rr_offset: 0,
            drop_warn: WarnLimiter::new(WARN_INTERVAL),
            reject_warn: WarnLimiter::new(WARN_INTERVAL),
            oversize_warn: WarnLimiter::new(WARN_INTERVAL),
            presence_subscribed: false,
        }
    }

    fn ensure_presence_subscribed(&self) {
        if self.presence_subscribed {
            return;
        }
        if let Err(err) = self.client.try_subscribe(PRESENCE_FILTER, QoS::AtMostOnce) {
            warn!("MQTT presence subscribe failed (will retry): {err}");
        }
    }

    async fn handle_presence(&mut self, topic: &str) {
        let now = Instant::now();
        let Some(key) = WatchKey::from_presence_topic(topic) else {
            if self.reject_warn.allow(now) {
                warn!("Ignoring malformed presence topic: {topic}");
            }
            return;
        };
        match self.registry.beat(key.clone(), now) {
            BeatOutcome::Refreshed => {}
            BeatOutcome::Rejected => {
                if self.reject_warn.allow(now) {
                    warn!("Watch registry full ({MAX_WATCHES}); ignoring new watch {topic}");
                }
            }
            BeatOutcome::New => self.seed_new_watch(key, now).await,
        }
    }

    // First frame for a watch, so the browser isn't blank until the next
    // block-driven broadcast (~150ms). Unknown coins seed nothing and the
    // watch idles until it expires.
    async fn seed_new_watch(&mut self, key: WatchKey, now: Instant) {
        match key {
            WatchKey::L2Book { coin, variant } => {
                // The l2_snapshots_now fallback clones the full universe, so
                // it's debounced and only serves the startup window before
                // the first broadcast lands in the cache.
                let response = if let Some(msg) = &self.last_snapshot {
                    if let InternalMessage::Snapshot { l2_snapshots, time } = msg.as_ref() {
                        payloads::l2_book_response(l2_snapshots, &coin, &variant.params(), self.config.l2_levels, *time)
                    } else {
                        None
                    }
                } else if self
                    .last_snapshot_fallback
                    .is_none_or(|last| now.duration_since(last) >= SNAPSHOT_FALLBACK_DEBOUNCE)
                {
                    self.last_snapshot_fallback = Some(now);
                    self.listener.lock().await.l2_snapshots_now().and_then(|(time, snapshots)| {
                        payloads::l2_book_response(&snapshots, &coin, &variant.params(), self.config.l2_levels, time)
                    })
                } else {
                    None
                };
                if let Some(response) = response {
                    self.publish_json(&l2_stream_topic(&coin, variant), &response, true, now);
                }
            }
            WatchKey::Trades { coin } => {
                // The ring's history starts from the listener's recent trades;
                // live batches keep it fresh from then on, so seed republishes
                // never touch the listener mutex again.
                let needs_history = self.trades.get(&coin).is_none_or(|seed| seed.ring.is_empty());
                let history = if needs_history { self.listener.lock().await.recent_trades(&coin) } else { Vec::new() };
                let seed = self.trades.entry(coin.clone()).or_default();
                if seed.ring.is_empty() {
                    seed.ring.extend(history);
                }
                if seed.ring.is_empty() {
                    return;
                }
                let Some((payload, dropped)) = capped_trades_seed(&seed.ring) else { return };
                seed.dirty = false;
                seed.last_published = Some(now);
                self.warn_seed_trim(&coin, dropped, now);
                self.publish_payload(&trades_snapshot_topic(&coin), payload, true, now);
            }
        }
    }

    fn handle_internal(&mut self, msg: Arc<InternalMessage>) {
        let now = Instant::now();
        match msg.as_ref() {
            InternalMessage::Snapshot { .. } => {
                self.last_snapshot = Some(msg.clone());
                self.publish_l2_frames(now);
            }
            InternalMessage::Trades { trades } => self.publish_trades(trades, now),
            InternalMessage::L4BookUpdates { .. } => {}
        }
    }

    fn publish_l2_frames(&mut self, now: Instant) {
        let Some(msg) = self.last_snapshot.clone() else { return };
        let InternalMessage::Snapshot { l2_snapshots, time } = msg.as_ref() else { return };
        let watches = self.registry.l2_watches();
        if watches.is_empty() {
            return;
        }
        // Rotate the start point each frame so a drained token bucket starves
        // every watch equally instead of always the same tail of the list.
        self.rr_offset = self.rr_offset.wrapping_add(1);
        let start = self.rr_offset % watches.len();
        for (coin, variant) in watches.iter().cycle().skip(start).take(watches.len()) {
            let Some(response) =
                payloads::l2_book_response(l2_snapshots, coin, &variant.params(), self.config.l2_levels, *time)
            else {
                continue;
            };
            if !self.publish_json(&l2_stream_topic(coin, *variant), &response, true, now) {
                break;
            }
        }
    }

    fn publish_trades(&mut self, trades: &HashMap<String, Vec<Trade>>, now: Instant) {
        for (coin, batch) in trades {
            if batch.is_empty() || !self.registry.trades_watched(coin) {
                continue;
            }
            // Non-retained: a dropped live frame is recovered by the next
            // retained seed republish within trades_seed_interval.
            self.publish_trades_batch(coin, batch, now);
            let seed = self.trades.entry(coin.clone()).or_default();
            seed.ring.extend(batch.iter().cloned());
            while seed.ring.len() > RECENT_TRADES_CAP {
                seed.ring.pop_front();
            }
            seed.dirty = true;
        }
    }

    // A burst batch can serialize past the packet cap; halving splits it into
    // consecutive frames (order preserved) instead of losing the batch.
    fn publish_trades_batch(&mut self, coin: &str, batch: &[Trade], now: Instant) {
        match serde_json::to_vec(&ServerResponse::Trades(batch.to_vec())) {
            Ok(payload) if payload.len() <= MAX_PAYLOAD_BYTES => {
                self.publish_payload(&trades_stream_topic(coin), payload, false, now);
            }
            Ok(_) if batch.len() > 1 => {
                let mid = batch.len() / 2;
                self.publish_trades_batch(coin, &batch[..mid], now);
                self.publish_trades_batch(coin, &batch[mid..], now);
            }
            Ok(payload) => {
                if self.oversize_warn.allow(now) {
                    warn!("Dropping {}-byte single-trade frame over packet cap on {coin}", payload.len());
                }
            }
            Err(err) => warn!("MQTT payload serialization error for {coin} trades: {err}"),
        }
    }

    fn warn_seed_trim(&mut self, coin: &str, dropped: usize, now: Instant) {
        if dropped > 0 && self.oversize_warn.allow(now) {
            warn!("Trades seed for {coin} over packet cap; dropped {dropped} oldest trades");
        }
    }

    fn housekeeping(&mut self) {
        let now = Instant::now();
        // clean_session drops server-side subscription state on reconnect and
        // the ConnAck-time subscribe can race a full request channel, so keep
        // retrying until the SubAck confirms it.
        self.ensure_presence_subscribed();
        for key in self.registry.sweep(now) {
            // Clearing the retained frame keeps late subscribers from
            // rendering a book that stopped updating when its last watcher
            // left. Clears bypass the token bucket: hygiene shouldn't lose to
            // data frames, and expiry cadence bounds their rate anyway.
            let topic = key.retained_topic();
            if let Err(err) = self.client.try_publish(&topic, QoS::AtMostOnce, true, Vec::new()) {
                warn!("MQTT retained clear for {topic} failed: {err}");
            }
            if let WatchKey::Trades { coin } = &key {
                self.trades.remove(coin);
            }
        }
        self.publish_trade_seeds(now);
        let health = format!("{{\"time\":{}}}", unix_millis());
        if let Err(err) = self.client.try_publish(HEALTH_TOPIC, QoS::AtMostOnce, true, health.into_bytes()) {
            warn!("MQTT health publish failed: {err}");
        }
    }

    fn publish_trade_seeds(&mut self, now: Instant) {
        let due: Vec<String> = self
            .trades
            .iter()
            .filter(|(coin, seed)| {
                self.registry.trades_watched(coin)
                    && seed_due(seed.dirty, seed.last_published, now, self.config.trades_seed_interval)
            })
            .map(|(coin, _)| coin.clone())
            .collect();
        for coin in due {
            let Some(seed) = self.trades.get(&coin) else { continue };
            let Some((payload, dropped)) = capped_trades_seed(&seed.ring) else { continue };
            self.warn_seed_trim(&coin, dropped, now);
            if self.publish_payload(&trades_snapshot_topic(&coin), payload, true, now)
                && let Some(seed) = self.trades.get_mut(&coin)
            {
                seed.dirty = false;
                seed.last_published = Some(now);
            }
        }
    }

    fn publish_json(&mut self, topic: &str, response: &ServerResponse, retain: bool, now: Instant) -> bool {
        match serde_json::to_vec(response) {
            Ok(payload) => self.publish_payload(topic, payload, retain, now),
            Err(err) => {
                warn!("MQTT payload serialization error for {topic}: {err}");
                false
            }
        }
    }

    // try_publish only: an awaited publish from the task that polls the
    // eventloop deadlocks when the request channel fills.
    fn publish_payload(&mut self, topic: &str, payload: Vec<u8>, retain: bool, now: Instant) -> bool {
        // Belt-and-braces: an over-cap packet reaching rumqttc kills the whole
        // connection, so it must die here as one dropped frame instead.
        if payload.len() > MAX_PAYLOAD_BYTES {
            if self.oversize_warn.allow(now) {
                warn!("Dropping {}-byte MQTT payload over packet cap for {topic}", payload.len());
            }
            return false;
        }
        if !self.bucket.try_take(now) {
            if self.drop_warn.allow(now) {
                warn!("MQTT publish budget ({PUBLISH_RATE_PER_SEC}/s) exhausted; dropping frames (at {topic})");
            }
            return false;
        }
        if let Err(err) = self.client.try_publish(topic, QoS::AtMostOnce, retain, payload) {
            if self.drop_warn.allow(now) {
                warn!("MQTT publish to {topic} failed: {err}");
            }
            return false;
        }
        true
    }
}

pub(crate) async fn run_mqtt_publisher(
    config: MqttConfig,
    pems: MqttPems,
    mut internal_message_rx: Receiver<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    // AWS IoT kills BOTH connections on a client-id collision, so the id must
    // be unique per process incarnation.
    let client_id = format!("{}-{}-{}", config.client_id_prefix, std::process::id(), unix_millis());
    let mut options = MqttOptions::new(client_id.clone(), config.endpoint.clone(), config.port);
    options.set_transport(Transport::Tls(TlsConfiguration::Simple {
        ca: pems.ca,
        alpn: None,
        client_auth: Some((pems.cert, pems.key)),
    }));
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(true);
    options.set_max_packet_size(MAX_PACKET_SIZE, MAX_PACKET_SIZE);

    let (client, mut eventloop) = AsyncClient::new(options, MQTT_CHANNEL_CAP);
    info!("MQTT publisher connecting to {}:{} as {client_id}", config.endpoint, config.port);

    let mut publisher = Publisher::new(client, config, listener);
    let mut housekeeping = tokio::time::interval(HOUSEKEEPING_INTERVAL);
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut backoff = BACKOFF_MIN;

    loop {
        select! {
            event = eventloop.poll() => match event {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("MQTT connected; subscribing {PRESENCE_FILTER}");
                    backoff = BACKOFF_MIN;
                    publisher.presence_subscribed = false;
                    publisher.ensure_presence_subscribed();
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    if ack.return_codes.iter().any(|code| matches!(code, SubscribeReasonCode::Failure)) {
                        warn!("MQTT presence subscribe rejected by broker (will retry)");
                    } else {
                        info!("MQTT presence subscription confirmed");
                        publisher.presence_subscribed = true;
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    publisher.handle_presence(&publish.topic).await;
                }
                Ok(_) => {}
                Err(err) => {
                    // The publisher mirrors local state into an independently
                    // failing remote; exiting would turn a partial outage into
                    // a total one, so it must never take the WS server down.
                    // Sleeping here stalls the broadcast rx, which is fine:
                    // Lagged is tolerated below.
                    warn!("MQTT connection error (retrying in {backoff:?}): {err}");
                    publisher.presence_subscribed = false;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            },
            msg = internal_message_rx.recv() => match msg {
                Ok(msg) => publisher.handle_internal(msg),
                // Unlike the WS handler, Lagged is not fatal here: frames are
                // full snapshots, so skipping some is lossless.
                Err(RecvError::Lagged(skipped)) => {
                    warn!("MQTT publisher lagged behind broadcast by {skipped} messages");
                }
                Err(RecvError::Closed) => {
                    warn!("Internal broadcast channel closed; MQTT publisher exiting");
                    return;
                }
            },
            _ = housekeeping.tick() => publisher.housekeeping(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_key_round_trips() {
        for variant in VariantKey::ALL {
            assert_eq!(VariantKey::parse(variant.as_str()), Some(variant));
        }
        assert_eq!(VariantKey::parse("sf9"), None);
        assert_eq!(VariantKey::parse("FULL"), None);
        assert_eq!(VariantKey::parse(""), None);
    }

    #[test]
    fn variant_params_match_computed_aggregations() {
        // Mirrors the (n_sig_figs, mantissa) set compute_l2_snapshots emits.
        let expected = [
            (VariantKey::Full, None, None),
            (VariantKey::Sf2, Some(2), None),
            (VariantKey::Sf3, Some(3), None),
            (VariantKey::Sf4, Some(4), None),
            (VariantKey::Sf5, Some(5), None),
            (VariantKey::Sf5M2, Some(5), Some(2)),
            (VariantKey::Sf5M5, Some(5), Some(5)),
        ];
        for (variant, n_sig_figs, mantissa) in expected {
            assert!(variant.params() == L2SnapshotParams::new(n_sig_figs, mantissa));
        }
    }

    #[test]
    fn presence_topic_parse_accepts() {
        assert_eq!(
            WatchKey::from_presence_topic("order-book-presence/l2Book/BTC/full"),
            Some(WatchKey::L2Book { coin: "BTC".to_string(), variant: VariantKey::Full })
        );
        assert_eq!(
            WatchKey::from_presence_topic("order-book-presence/l2Book/kPEPE/sf5m2"),
            Some(WatchKey::L2Book { coin: "kPEPE".to_string(), variant: VariantKey::Sf5M2 })
        );
        assert_eq!(
            WatchKey::from_presence_topic("order-book-presence/trades/HYPE"),
            Some(WatchKey::Trades { coin: "HYPE".to_string() })
        );
    }

    #[test]
    fn presence_topic_parse_rejects() {
        let rejected = [
            "order-book-presence/l2Book/BTC",            // missing variant
            "order-book-presence/l2Book/BTC/sf9",        // unknown variant
            "order-book-presence/l2Book/BTC/full/extra", // trailing segment
            "order-book-presence/l2Book/PURR/USDC/full", // spot pair splits into segments
            "order-book-presence/trades/PURR/USDC",      // same, trades
            "order-book-presence/trades/@142",           // spot index
            "order-book-presence/trades/",               // empty coin
            "order-book-presence/trades/BTC+",           // wildcard chars
            "order-book-presence/trades/#",
            "order-book-presence/other/BTC",     // unknown stream kind
            "order-book-stream/l2Book/BTC/full", // wrong prefix
            "order-book-presence",
            "",
        ];
        for topic in rejected {
            assert_eq!(WatchKey::from_presence_topic(topic), None, "should reject {topic:?}");
        }
        let long_coin = "A".repeat(33);
        assert_eq!(WatchKey::from_presence_topic(&format!("order-book-presence/trades/{long_coin}")), None);
    }

    #[test]
    fn registry_beat_refresh_and_expiry() {
        let expiry = Duration::from_secs(90);
        let mut registry = WatchRegistry::new(expiry, 10);
        let key = WatchKey::Trades { coin: "BTC".to_string() };
        let t0 = Instant::now();
        assert_eq!(registry.beat(key.clone(), t0), BeatOutcome::New);
        assert_eq!(registry.beat(key.clone(), t0 + Duration::from_secs(30)), BeatOutcome::Refreshed);
        // Expiry counts from the LAST beat.
        assert!(registry.sweep(t0 + Duration::from_secs(89 + 30)).is_empty());
        assert_eq!(registry.sweep(t0 + Duration::from_secs(90 + 30)), vec![key.clone()]);
        // Swept watches can come back as new.
        assert_eq!(registry.beat(key, t0 + Duration::from_secs(121)), BeatOutcome::New);
    }

    #[test]
    fn registry_rejects_beats_over_cap_but_still_refreshes() {
        let mut registry = WatchRegistry::new(Duration::from_secs(90), 2);
        let now = Instant::now();
        let first = WatchKey::Trades { coin: "BTC".to_string() };
        assert_eq!(registry.beat(first.clone(), now), BeatOutcome::New);
        assert_eq!(registry.beat(WatchKey::Trades { coin: "ETH".to_string() }, now), BeatOutcome::New);
        assert_eq!(registry.beat(WatchKey::Trades { coin: "SOL".to_string() }, now), BeatOutcome::Rejected);
        assert_eq!(registry.beat(first, now), BeatOutcome::Refreshed);
    }

    #[test]
    fn l2_watches_sorted_and_filtered() {
        let mut registry = WatchRegistry::new(Duration::from_secs(90), 10);
        let now = Instant::now();
        registry.beat(WatchKey::L2Book { coin: "ETH".to_string(), variant: VariantKey::Full }, now);
        registry.beat(WatchKey::L2Book { coin: "BTC".to_string(), variant: VariantKey::Sf5 }, now);
        registry.beat(WatchKey::L2Book { coin: "BTC".to_string(), variant: VariantKey::Full }, now);
        registry.beat(WatchKey::Trades { coin: "BTC".to_string() }, now);
        assert_eq!(
            registry.l2_watches(),
            vec![
                ("BTC".to_string(), VariantKey::Full),
                ("BTC".to_string(), VariantKey::Sf5),
                ("ETH".to_string(), VariantKey::Full),
            ]
        );
        assert!(registry.trades_watched("BTC"));
        assert!(!registry.trades_watched("ETH"));
    }

    #[test]
    fn token_bucket_caps_and_refills() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::new(2.0, 1.0, t0);
        assert!(bucket.try_take(t0));
        assert!(bucket.try_take(t0));
        assert!(!bucket.try_take(t0));
        assert!(bucket.try_take(t0 + Duration::from_secs(1)));
        assert!(!bucket.try_take(t0 + Duration::from_secs(1)));
        // Refill is clamped to capacity: a long idle doesn't bank extra burst.
        let later = t0 + Duration::from_secs(3600);
        assert!(bucket.try_take(later));
        assert!(bucket.try_take(later));
        assert!(!bucket.try_take(later));
    }

    #[test]
    fn seed_due_table() {
        let interval = Duration::from_secs(10);
        let t0 = Instant::now();
        // Never published + dirty → due immediately.
        assert!(seed_due(true, None, t0, interval));
        // Clean → never due, regardless of age.
        assert!(!seed_due(false, None, t0, interval));
        assert!(!seed_due(false, Some(t0), t0 + Duration::from_secs(3600), interval));
        // Dirty but inside the interval → not yet.
        assert!(!seed_due(true, Some(t0), t0 + Duration::from_secs(9), interval));
        assert!(seed_due(true, Some(t0), t0 + Duration::from_secs(10), interval));
    }

    #[test]
    fn topics_match_deployed_iot_policies() {
        assert_eq!(l2_stream_topic("BTC", VariantKey::Sf5M5), "order-book-stream/l2Book/BTC/sf5m5");
        assert_eq!(trades_stream_topic("BTC"), "order-book-stream/trades/BTC");
        assert_eq!(trades_snapshot_topic("BTC"), "order-book-snapshot/trades/BTC");
        let l2 = WatchKey::L2Book { coin: "BTC".to_string(), variant: VariantKey::Full };
        assert_eq!(l2.retained_topic(), "order-book-stream/l2Book/BTC/full");
        let trades = WatchKey::Trades { coin: "BTC".to_string() };
        assert_eq!(trades.retained_topic(), "order-book-snapshot/trades/BTC");
    }

    // Trade's fields are private; tests build them through Deserialize, with
    // px length as the size dial.
    fn test_trade(tid: u64, px_len: usize) -> Trade {
        serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "side": "A",
            "px": "9".repeat(px_len),
            "sz": "1.0",
            "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "time": 1_756_000_000_000_u64,
            "tid": tid,
            "users": [
                "0x0000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000001"
            ]
        }))
        .unwrap()
    }

    fn seed_tids(payload: &[u8]) -> Vec<u64> {
        let frame: serde_json::Value = serde_json::from_slice(payload).unwrap();
        frame["data"].as_array().unwrap().iter().map(|trade| trade["tid"].as_u64().unwrap()).collect()
    }

    #[test]
    fn capped_seed_passes_small_ring_through() {
        let ring: VecDeque<Trade> = (0..RECENT_TRADES_CAP as u64).map(|tid| test_trade(tid, 8)).collect();
        let (payload, dropped) = capped_trades_seed(&ring).unwrap();
        assert_eq!(dropped, 0);
        assert_eq!(seed_tids(&payload).len(), RECENT_TRADES_CAP);
    }

    #[test]
    fn capped_seed_drops_oldest_to_fit() {
        // ~2KB per trade x 100 ≈ 200KB, well past the cap.
        let ring: VecDeque<Trade> = (0..100_u64).map(|tid| test_trade(tid, 2_000)).collect();
        let (payload, dropped) = capped_trades_seed(&ring).unwrap();
        assert!(payload.len() <= MAX_PAYLOAD_BYTES);
        assert!(dropped > 0 && dropped < 100);
        let tids = seed_tids(&payload);
        // The survivors are the newest suffix, order preserved.
        assert_eq!(tids.first().copied(), Some(dropped as u64));
        assert_eq!(tids.last().copied(), Some(99));
    }

    #[test]
    fn capped_seed_refuses_when_nothing_fits() {
        let ring: VecDeque<Trade> = std::iter::once(test_trade(0, MAX_PAYLOAD_BYTES)).collect();
        assert!(capped_trades_seed(&ring).is_none());
    }
}
