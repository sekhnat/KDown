//! `kdown-app serve`: loopback-only process startup and graceful shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use kdown_app::api::AppState;
use kdown_app::cli::{validate_listen, Cli};
use kdown_app::engine_adapter::KdownEngineLauncher;
use kdown_app::events::EventBroker;
use kdown_app::path_policy::PathPolicy;
use kdown_app::registry::Registry;
use kdown_app::supervisor::{spawn_supervisor_with_broker, RecoveryPlanner, SupervisorLimits};

fn default_state_dir() -> PathBuf {
    if let Some(state) = dirs::state_dir() {
        return state.join("kdown");
    }
    if let Some(home) = dirs::home_dir() {
        return home.join(".local/state/kdown");
    }
    PathBuf::from(".kdown-state")
}

#[allow(dead_code)] // kept next to run_serve for future non-default use
fn default_listen() -> SocketAddr {
    "127.0.0.1:8734".parse().expect("valid default listen")
}

async fn run_serve(
    listen: SocketAddr,
    state_dir: Option<PathBuf>,
    web_dir: Option<PathBuf>,
    open: bool,
    initial_roots: Vec<PathBuf>,
) -> Result<(), kdown_app::error::AppError> {
    let listen = validate_listen(listen)?;

    let state_dir = state_dir.unwrap_or_else(default_state_dir);
    std::fs::create_dir_all(&state_dir).map_err(|_| kdown_app::error::AppError::Persistence)?;

    let registry = Registry::connect(state_dir.join("kdown.db")).await?;
    registry.migrate().await?;

    for root in &initial_roots {
        let policy = PathPolicy::new(registry.clone());
        // Idempotent: an already-configured directory returns its record.
        policy
            .add_root("Downloads", root, registry.list_roots().await?.is_empty())
            .await?;
    }

    let config = kdown_engine::EngineConfig::default();
    let transport = kdown_engine::HttpTransport::from_config(&config)
        .map_err(|error| kdown_app::error::AppError::EngineLaunch(error.to_string()))?;
    let controller = kdown_engine::DownloadController::new(transport, config);
    let launcher = KdownEngineLauncher::new(controller);

    let settings = registry.load_settings().await?;
    let policy = PathPolicy::new(registry.clone());
    // One shared broker: the supervisor publishes, /api/v1/events serves.
    let broker = EventBroker::new(256);
    let supervisor = spawn_supervisor_with_broker(
        broker.clone(),
        registry.clone(),
        policy,
        launcher,
        SupervisorLimits {
            max_active: usize::try_from(settings.active_concurrency).unwrap_or(1),
            rate_limit_bytes_per_second: settings.rate_limit_bytes_per_second,
        },
    );

    // Startup recovery runs before the service accepts requests.
    let planner = RecoveryPlanner::new(
        registry.clone(),
        supervisor.clone(),
        kdown_app::domain::LaunchKey::new(),
    );
    planner.recover_startup().await?;

    let suggested = dirs::download_dir().map(|p| p.to_string_lossy().into_owned());
    let state =
        AppState::with_suggestion(registry, supervisor.clone(), suggested).with_events(broker);

    #[cfg(feature = "bundled-web")]
    let app = {
        let _ = &web_dir; // ignored under bundled-web
        kdown_app::api::build_router_with_web_dir(state, None)
    };
    #[cfg(not(feature = "bundled-web"))]
    let app = kdown_app::api::build_router_with_web_dir(state, web_dir);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|_| kdown_app::error::AppError::ServiceDegraded)?;
    let bound = listener
        .local_addr()
        .map_err(|_| kdown_app::error::AppError::ServiceDegraded)?;
    println!("READY http://{bound}");

    if open {
        let ready_url = format!("http://{bound}");
        // Spawn without waiting: xdg-open may outlive this call and the
        // server must start serving immediately.
        let _ = tokio::process::Command::new("xdg-open")
            .arg(&ready_url)
            .spawn();
    }

    // Graceful shutdown on SIGINT/SIGTERM (Unix) or Ctrl+C: stop admission,
    // cancel active runs preserving resumable artifacts, give in-flight
    // finalizations a moment to persist, then exit. The process exits
    // directly because SSE responses never end on their own — a graceful
    // axum drain would wait for them forever. Recovery re-classifies any
    // attempt that did not finish, so a missed finalize is safe.
    let shutdown_supervisor = supervisor.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        if let Err(error) = shutdown_supervisor.shutdown().await {
            eprintln!("supervisor shutdown failed: {error}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
        std::process::exit(0);
    });

    let serve_result = axum::serve(listener, app).await;
    serve_result.map_err(|_| kdown_app::error::AppError::ServiceDegraded)?;
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    if let Err(message) = cli.require_serve() {
        eprintln!("kdown-app: {message}");
        std::process::exit(2);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = runtime.block_on(async move {
        run_serve(cli.listen, cli.state_dir, cli.web_dir, cli.open, cli.roots).await
    });
    if let Err(error) = result {
        eprintln!("kdown-app: {error}");
        std::process::exit(1);
    }
}
