#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use server::{MqttConfig, Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// Seconds between snapshot validations once the book is seeded. Each
    /// validation makes the node dump its full L4 state to disk, which is
    /// heavy enough to stall the node's block execution when run too often.
    /// Seeding always retries every 10s until the first snapshot succeeds.
    #[arg(long, default_value_t = 10)]
    snapshot_validation_interval_secs: u64,

    /// Minimum milliseconds between l2Book broadcasts (0 = one per block).
    /// Lossless coalescing: every broadcast is a full frame, so subscribers
    /// are never more than one interval behind the book. Subscribe-time
    /// snapshots and the trades stream are not throttled.
    #[arg(long, default_value_t = 0)]
    l2_broadcast_min_interval_ms: u64,

    /// `AWS IoT Core` ATS data endpoint (bare hostname, no scheme). Setting
    /// this enables the presence-driven MQTT publisher; the three PEM paths
    /// below are then required.
    #[arg(long)]
    mqtt_endpoint: Option<String>,

    #[arg(long, default_value_t = 8883)]
    mqtt_port: u16,

    /// X.509 device certificate (PEM) registered with `AWS IoT`.
    #[arg(long)]
    mqtt_cert_path: Option<PathBuf>,

    /// Private key (PEM) for the device certificate.
    #[arg(long)]
    mqtt_key_path: Option<PathBuf>,

    /// Root CA (PEM), e.g. AmazonRootCA1.pem for ATS endpoints.
    #[arg(long)]
    mqtt_ca_path: Option<PathBuf>,

    /// AWS kills both connections on a client-id collision, so the process
    /// appends its pid and a timestamp to this prefix. The deployed publisher
    /// policy pins `client/order-book-publisher-*`.
    #[arg(long, default_value = "order-book-publisher")]
    mqtt_client_id_prefix: String,

    /// Book depth per MQTT l2Book frame (clamped to the server maximum).
    #[arg(long, default_value_t = 25)]
    mqtt_l2_levels: usize,

    /// Minimum seconds between retained trades-seed republishes per coin.
    #[arg(long, default_value_t = 10)]
    mqtt_trades_seed_interval_secs: u64,

    /// Seconds after the last presence beat before a watch expires and its
    /// retained frames are cleared. Browsers beat every 30s.
    #[arg(long, default_value_t = 90)]
    mqtt_watch_expiry_secs: u64,
}

impl Args {
    // Manual instead of clap `requires` so the error names every missing
    // flag at once.
    fn mqtt_config(&self) -> Result<Option<MqttConfig>> {
        let Some(endpoint) = &self.mqtt_endpoint else {
            return Ok(None);
        };
        let missing: Vec<&str> = [
            ("--mqtt-cert-path", self.mqtt_cert_path.is_none()),
            ("--mqtt-key-path", self.mqtt_key_path.is_none()),
            ("--mqtt-ca-path", self.mqtt_ca_path.is_none()),
        ]
        .into_iter()
        .filter_map(|(flag, is_missing)| is_missing.then_some(flag))
        .collect();
        if !missing.is_empty() {
            return Err(format!("--mqtt-endpoint requires {}", missing.join(", ")).into());
        }
        let (Some(cert_path), Some(key_path), Some(ca_path)) =
            (self.mqtt_cert_path.clone(), self.mqtt_key_path.clone(), self.mqtt_ca_path.clone())
        else {
            return Err("unreachable: PEM paths checked above".into());
        };
        Ok(Some(MqttConfig {
            endpoint: endpoint.clone(),
            port: self.mqtt_port,
            cert_path,
            key_path,
            ca_path,
            client_id_prefix: self.mqtt_client_id_prefix.clone(),
            l2_levels: self.mqtt_l2_levels,
            trades_seed_interval: Duration::from_secs(self.mqtt_trades_seed_interval_secs),
            watch_expiry: Duration::from_secs(self.mqtt_watch_expiry_secs),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Tokio swallows panics in spawned tasks: a panic in the listener task
    // leaves the process alive serving a frozen book, with the stall watchdog
    // and snapshot validation dead alongside it, so systemd never restarts it.
    // Crash-only healing requires every panic to take the process down.
    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_panic_hook(panic_info);
        std::process::exit(1);
    }));

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let mqtt_config = args.mqtt_config()?;
    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    run_websocket_server(
        &full_address,
        true,
        compression_level,
        Duration::from_secs(args.snapshot_validation_interval_secs),
        Duration::from_millis(args.l2_broadcast_min_interval_ms),
        mqtt_config,
    )
    .await?;

    Ok(())
}
