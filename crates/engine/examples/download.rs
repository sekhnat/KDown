//! Download a URL with a simple progress display.
//!
//! ```text
//! cargo run --example download -- URL DESTINATION
//! ```
//! The default `FailIfExists` policy rejects an existing destination early and
//! also prevents a destination created during the download from being replaced.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use kdown_engine::{DownloadController, DownloadRequest, EngineConfig, HttpTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let Some(url) = args.next() else {
        eprintln!("usage: cargo run --example download -- URL DESTINATION");
        std::process::exit(2);
    };
    let Some(destination) = args.next() else {
        eprintln!("usage: cargo run --example download -- URL DESTINATION");
        std::process::exit(2);
    };

    let config = EngineConfig::default();
    let transport = HttpTransport::from_config(&config)?;
    let controller = DownloadController::new(transport, config);
    let request = DownloadRequest::new(url, PathBuf::from(destination));
    let (handle, task) = controller.start(request);

    while !task.is_finished() {
        let snapshot = handle.snapshot();
        eprint!(
            "\rnetwork={} B, completed={} B, retries={}",
            snapshot.network_bytes, snapshot.completed_bytes, snapshot.retries
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let result = task.await??;
    eprintln!();
    match result.final_path {
        Some(path) => println!("completed: {}", path.display()),
        None => println!("job ended as {:?}", result.status),
    }
    if let Some(error) = result.error {
        eprintln!("error: {error}");
    }
    Ok(())
}
