use clap::Parser;
use kdown_app::cli::Cli;

#[test]
fn serve_defaults_to_loopback_and_manual_browser_start() {
    let cli = Cli::try_parse_from(["kdown-app", "serve"]).unwrap();
    assert_eq!(cli.listen, "127.0.0.1:8734".parse().unwrap());
    assert!(!cli.open);
}

#[test]
fn serve_accepts_explicit_options() {
    let cli = Cli::try_parse_from([
        "kdown-app",
        "serve",
        "--listen",
        "127.0.0.1:0",
        "--state-dir",
        "/tmp/kdown-state",
        "--web-dir",
        "/tmp/kdown-web",
        "--open",
        "--root",
        "/tmp/root-a",
        "--root",
        "/tmp/root-b",
    ])
    .unwrap();
    assert_eq!(cli.listen.port(), 0);
    assert_eq!(
        cli.state_dir,
        Some(std::path::PathBuf::from("/tmp/kdown-state"))
    );
    assert_eq!(
        cli.web_dir,
        Some(std::path::PathBuf::from("/tmp/kdown-web"))
    );
    assert!(cli.open);
    assert_eq!(
        cli.roots,
        vec![
            std::path::PathBuf::from("/tmp/root-a"),
            std::path::PathBuf::from("/tmp/root-b")
        ]
    );
}

#[test]
fn non_loopback_listen_addresses_are_refused() {
    let cli = Cli::try_parse_from(["kdown-app", "serve", "--listen", "0.0.0.0:8734"]).unwrap();
    let error = kdown_app::cli::validate_listen(cli.listen).unwrap_err();
    assert_eq!(error.code(), "listen_not_loopback");

    let loopback = Cli::try_parse_from(["kdown-app", "serve"]).unwrap();
    kdown_app::cli::validate_listen(loopback.listen).unwrap();
}
