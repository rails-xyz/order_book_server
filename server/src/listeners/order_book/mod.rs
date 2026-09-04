use crate::{
    HL_NODE,
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Side, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order, Trade,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(
    listener: Arc<Mutex<OrderBookListener>>,
    dir: PathBuf,
    snapshot_validation_interval: Duration,
) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    let mut last_snapshot_fetch: Option<Instant> = None;
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                // Retry at ticker cadence until the first snapshot seeds the book,
                // but validate at the configured interval once ready: each fetch
                // makes the node dump its entire L4 state, which is heavy enough
                // to stall block execution when repeated too often.
                let ready = listener.lock().await.is_ready();
                if !ready || last_snapshot_fetch.is_none_or(|at| at.elapsed() >= snapshot_validation_interval) {
                    last_snapshot_fetch = Some(Instant::now());
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                }
            }
            () = sleep(Duration::from_secs(5)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    return Err(format!("Stream has fallen behind ({HL_NODE} failed?)").into());
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let state = {
                    let mut listener = listener.lock().await;
                    listener.begin_caching();
                    listener.clone_state()
                };
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    state.apply_updates(order_statuses, order_diffs)?;
                                } else {
                                    // Early returns here skip tx.send, so they are
                                    // non-fatal by construction; without the log they
                                    // are invisible.
                                    warn!("Not enough cached updates; skipping validation this cycle");
                                    return Ok::<(), Error>(());
                                }
                            }
                            if state.height() > height {
                                warn!("Fetched snapshot lagging stored state; skipping validation this cycle");
                                return Ok(());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot");
                            match validate_snapshot_consistency(&stored_snapshot, expected_snapshot, ignore_spot) {
                                Ok(()) => Ok(()),
                                Err(err) => {
                                    // The node re-prices triggered stop orders as they
                                    // enter the book, and the status event only carries
                                    // the placement-time px, so the reconstruction
                                    // drifts whenever a triggered stop rests. Exiting
                                    // here (the upstream behavior) drops every WS and
                                    // MQTT consumer for a divergence the boot path can
                                    // repair; resync in-process instead.
                                    error!("Snapshot validation failed; resyncing from next snapshot: {err}");
                                    listener.lock().await.reset_state();
                                    Ok(())
                                }
                            }
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

// Matches the rough depth of the public HL WS trades snapshot; the UI panel
// shows far fewer.
pub(crate) const RECENT_TRADES_CAP: usize = 100;

// Trades are assembled once here rather than per connection: every fills batch
// fans out to all subscribers, and the recent-trades buffer needs them anyway.
fn coin_to_trades(batch: Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    // The two fills of a trade are not necessarily adjacent in the batch (a
    // taker sweeping several makers interleaves them), so pair by tid rather
    // than position, and drop incomplete pairs rather than panicking.
    let block_number = batch.block_number();
    let mut pending: HashMap<u64, HashMap<Side, NodeDataFill>> = HashMap::new();
    let mut trades: HashMap<String, Vec<Trade>> = HashMap::new();
    let mut dropped = 0_usize;
    for fill in batch.events() {
        let tid = fill.1.tid;
        let sides = pending.entry(tid).or_default();
        sides.insert(fill.1.side, fill);
        if sides.len() == 2 {
            match pending.remove(&tid).and_then(Trade::from_fills) {
                Some(trade) => trades.entry(trade.coin.clone()).or_default().push(trade),
                None => dropped += 1,
            }
        }
    }
    dropped += pending.len();
    if dropped > 0 {
        warn!("Dropped {dropped} unpaired fills at block {block_number}");
    }
    trades
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    // Chronological per coin, capped at RECENT_TRADES_CAP; seeds new trades
    // subscribers so they don't stare at an empty panel until the next trade
    recent_trades: HashMap<String, VecDeque<Trade>>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // At most one l2 snapshot broadcast per this interval (zero = every block,
    // the upstream behavior). Throttling is lossless: snapshots are full
    // frames, so the next broadcast carries everything the dropped ones did.
    l2_broadcast_min_interval: Duration,
    last_l2_broadcast: Option<Instant>,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        l2_broadcast_min_interval: Duration,
    ) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            recent_trades: HashMap::new(),
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            l2_broadcast_min_interval,
            last_l2_broadcast: None,
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    // Forget the book so the next ticker fetch re-seeds via init_from_snapshot;
    // while None, receive_batch queues updates instead of applying them, and the
    // fallen-behind watchdog is disarmed - the same regime as before first seed.
    fn reset_state(&mut self) {
        self.order_book_state = None;
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                // Recording last_fill (upstream never assigned it) makes this
                // guard effective against torn-read replays: a partial last
                // line rewinds the whole chunk, re-parsing completed blocks —
                // without the guard those trades would be re-broadcast and
                // double-inserted into recent_trades.
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    self.last_fill = Some(batch.block_number());
                    let trades = coin_to_trades(batch);
                    if !trades.is_empty() {
                        for (coin, coin_trades) in &trades {
                            let buffer = self.recent_trades.entry(coin.clone()).or_default();
                            buffer.extend(coin_trades.iter().cloned());
                            while buffer.len() > RECENT_TRADES_CAP {
                                buffer.pop_front();
                            }
                        }
                        if let Some(tx) = &self.internal_message_tx {
                            // broadcast::send is sync; sending inline (upstream
                            // spawned a task) keeps frames ordered with the
                            // subscribe-time snapshot reads.
                            let _unused = tx.send(Arc::new(InternalMessage::Trades { trades }));
                        }
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }

    // None until the first snapshot seeds the book
    pub(crate) fn l2_snapshots_now(&self) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_ref().map(OrderBookState::l2_snapshots_now)
    }

    // Oldest first, matching the order of streamed frames
    pub(crate) fn recent_trades(&self, coin: &str) -> Vec<Trade> {
        self.recent_trades.get(coin).map_or_else(Vec::new, |buffer| buffer.iter().cloned().collect())
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                *file = Some(open_tail_at_line_boundary(new_path)?);
            }
        }
        Ok(())
    }
}

// Attaching to a live file must land on a line boundary: End(0) can fall
// inside a line the writer is mid-flush on, and the rewind-on-error recovery
// in process_data then retries that mid-line offset forever (2026-09-04:
// stalled OrderStatuses — and the book — until a restart). Position at the
// start of the trailing partial line instead; its batch is then processed
// once the writer finishes it.
fn open_tail_at_line_boundary(path: &PathBuf) -> Result<File> {
    let mut file = File::open(path)?;
    let mut pos = file.seek(SeekFrom::End(0))?;
    let mut buf = vec![0_u8; 64 * 1024];
    while pos > 0 {
        let read_from = pos.saturating_sub(buf.len() as u64);
        let len = usize::try_from(pos - read_from)?;
        file.seek(SeekFrom::Start(read_from))?;
        file.read_exact(&mut buf[..len])?;
        if let Some(newline) = buf[..len].iter().rposition(|&byte| byte == b'\n') {
            file.seek(SeekFrom::Start(read_from + u64::try_from(newline)? + 1))?;
            return Ok(file);
        }
        pos = read_from;
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        let total_len = data.len();
        let lines: Vec<&str> = data.lines().collect();
        for (index, line) in lines.iter().copied().enumerate() {
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        // A torn line can be cut before byte 100; a hard [..100]
                        // slice panics then (the content is ASCII JSON, so no
                        // char-boundary concern)
                        &line[..line.len().min(100)],
                    );
                    // Only the chunk's last line can be a torn read that heals
                    // once the writer finishes it; rewind and let the next
                    // read retry. A bad line with data after it never heals —
                    // rewinding to it retries forever and stalls the stream —
                    // so drop it and let snapshot validation resync any drift.
                    if index + 1 < lines.len() {
                        error!("{event_source} unparseable line has data after it; skipping one line");
                        continue;
                    }
                    #[allow(clippy::unwrap_used)]
                    let total_len: i64 = total_len.try_into().unwrap();
                    self.file_mut(event_source).as_mut().map(|f| f.seek_relative(-total_len));
                    break;
                }
            };
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }
        // Gating BEFORE l2_snapshots also skips building the all-coins
        // snapshot map, so CPU falls with the frame rate. Skipped blocks lose
        // nothing (see the field comment); worst-case subscriber staleness is
        // one interval, because new blocks (~75ms apart) re-enter this path.
        // Subscribe-time snapshots (l2_snapshots_now) bypass this throttle.
        if self.last_l2_broadcast.is_none_or(|last| last.elapsed() >= self.l2_broadcast_min_interval) {
            let snapshot = self.l2_snapshots(true);
            if let Some(snapshot) = snapshot {
                if let Some(tx) = &self.internal_message_tx {
                    self.last_l2_broadcast = Some(Instant::now());
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let snapshot =
                            Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                        let _unused = tx.send(snapshot);
                    });
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    // Trades are pre-assembled from the fills batch so the work happens once,
    // not on every connection
    Trades { trades: HashMap<String, Vec<Trade>> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tail_offset(name: &str, contents: &[u8]) -> u64 {
        let path = std::env::temp_dir().join(format!("obs_tail_{}_{name}", std::process::id()));
        fs::write(&path, contents).unwrap();
        let mut file = open_tail_at_line_boundary(&path).unwrap();
        let offset = file.stream_position().unwrap();
        fs::remove_file(&path).unwrap();
        offset
    }

    #[test]
    fn tail_open_lands_on_line_boundaries() {
        assert_eq!(tail_offset("empty", b""), 0);
        assert_eq!(tail_offset("complete", b"{\"a\":1}\n{\"b\":2}\n"), 16);
        // Trailing partial line: position at its start, not at End(0).
        assert_eq!(tail_offset("partial", b"{\"a\":1}\n{\"b\""), 8);
        assert_eq!(tail_offset("no_newline", b"{\"a\""), 0);
    }

    #[test]
    fn tail_open_scans_back_past_chunk_size() {
        // Partial line longer than the 64KB scan chunk: the newline sits in
        // an earlier chunk.
        let mut contents = Vec::new();
        contents.write_all(b"{\"a\":1}\n").unwrap();
        contents.extend(std::iter::repeat_n(b'x', 100 * 1024));
        assert_eq!(tail_offset("long_partial", &contents), 8);
    }
}
