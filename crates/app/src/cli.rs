//! Process CLI: `kdown-app serve` with a stable loopback default.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "kdown-app", version, about = "KDown local download manager")]
pub struct Cli {
    /// The only command is `serve`.
    #[arg(default_value = "serve")]
    pub command: String,
    /// Listen address; loopback only.
    #[arg(long, default_value = "127.0.0.1:8734")]
    pub listen: SocketAddr,
    /// Durable state directory (defaults to the XDG state directory).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Web asset directory when not compiled with `bundled-web`.
    #[arg(long)]
    pub web_dir: Option<PathBuf>,
    /// Opens the browser after readiness.
    #[arg(long, default_value_t = false)]
    pub open: bool,
    /// Existing directories to register as download roots at startup.
    #[arg(long = "root")]
    pub roots: Vec<PathBuf>,
}

impl Cli {
    /// Rejects unknown commands.
    pub fn require_serve(&self) -> Result<(), String> {
        if self.command == "serve" {
            Ok(())
        } else {
            Err(format!(
                "unknown command: {} (expected serve)",
                self.command
            ))
        }
    }
}

/// Rejects non-loopback listen addresses; the service never exposes
/// beyond the local machine.
pub fn validate_listen(listen: SocketAddr) -> Result<SocketAddr, crate::error::AppError> {
    if !listen.ip().is_loopback() {
        return Err(crate::error::AppError::ListenNotLoopback);
    }
    Ok(listen)
}
