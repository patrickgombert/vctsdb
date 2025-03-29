use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};

mod storage;
mod ingestion;
mod query;
mod metrics;

#[tokio::main]
async fn main() {
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(true)
        .pretty()
        .init();

    // Initialize metrics
    let metrics_addr = SocketAddr::from(([127, 0, 0, 1], 9090));
    if let Err(e) = metrics::init_metrics(metrics_addr) {
        eprintln!("Failed to initialize metrics: {}", e);
    } else {
        info!("Metrics server listening on {}", metrics_addr);
    }

    info!("Starting VCTSDB...");
    
    // Spawn a task to record test metrics
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            // Record some test metrics
            metrics::record_ingestion(42.0);
            metrics::update_memory_usage(1024 * 1024); // 1MB
            metrics::record_query(15.5);
            metrics::record_wal_write(512);
            metrics::record_sstable_operation("compaction", 1);
        }
    });

    // Keep the application running
    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
    info!("Shutting down...");
}
