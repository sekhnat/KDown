//! Download a URL into a directory with automatic filename resolution and a
//! live progress display (decimal MB/s and estimated time remaining).
//!
//! ```text
//! scripts/download.sh [OPTIONS] URL [DIRECTORY]
//! cargo run --release --example download_link -- [OPTIONS] URL [DIRECTORY]
//! ```
//!
//! Options tune the transfer: `--segments N` (parallel range workers),
//! `--segment-size N` (initial range size with K/M/G suffixes), `--rate N`
//! (per-job bytes/s limit), `--retries N`, `--resume allowed|never|required`,
//! and `--overwrite rename|fail|replace`.
//!
//! `DIRECTORY` defaults to the current directory and must already exist.
//! The final filename is resolved by the engine from the final HEAD
//! metadata, the redirected/original URLs, or the validated fallback, and
//! `OverwritePolicy::Rename` never replaces an existing output: a free
//! `name (1).ext` sibling is selected instead.
//!
//! Exit codes: 0 success, 1 download failure, 2 usage/configuration error.

use std::collections::VecDeque;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::{Duration, Instant};

use kdown_engine::{
    DirectoryDownloadRequest, DownloadController, DownloadRequest, EngineConfig, Event,
    H2ConnectionPolicy, HttpTransport, OverwritePolicy, ProgressSnapshot, ResumePolicy,
};

const USAGE: &str = "usage: download_link [OPTIONS] URL [DIRECTORY]";

const OPTIONS: &str = "\
options:
  -s, --segments N      parallel range workers (1+); also lowers the
                        segmentation threshold so N-way splitting applies
                        (server range support still required)
  -S, --segment-size N  initial range size, e.g. 4M (K/M/G decimal suffixes)
  -r, --rate N          per-job rate limit in bytes/s, e.g. 10M
      --retries N       max attempts per segment (engine default 8)
      --resume MODE     allowed | never | required (default allowed)
      --overwrite MODE  rename | fail | replace (default rename)
  -c, --connections N   parallel HTTP/2 connection slots per origin
                        (default 8; each adds a bounded 2 MiB flow-control
                        window — raise for high-bandwidth × high-RTT paths)
  -h, --help            show this help";

/// Poll cadence for the progress display.
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Rolling window used for the displayed transfer rate.
const RATE_WINDOW_SECS: f64 = 5.0;
/// Rate below which an ETA is meaningless (matches the engine's §19.3 gate).
const MIN_ETA_RATE: f64 = 1024.0;
/// Non-TTY output logs at most one progress line per interval.
const NON_TTY_LOG_INTERVAL: Duration = Duration::from_secs(1);

/// User-selectable job tuning passed to the engine configuration.
#[derive(Debug, Default, Clone)]
struct JobOptions {
    segments: Option<u32>,
    segment_size: Option<u64>,
    rate_limit: Option<u64>,
    retries: Option<u32>,
    resume: Option<ResumePolicy>,
    overwrite: Option<OverwritePolicy>,
    /// Parallel h2 connections per origin (§24 D5): `None` keeps the
    /// downloader's WAN default of 8 connection slots.
    connections: Option<u32>,
}

enum Cli {
    Run {
        url: String,
        directory: PathBuf,
        options: JobOptions,
    },
    Help,
}

/// Pull the value for an option, supporting `--flag value` and `--flag=value`.
fn take_value(
    args: &[String],
    index: &mut usize,
    name: &str,
    inline: Option<String>,
) -> Result<String, String> {
    if let Some(value) = inline {
        return Ok(value);
    }
    match args.get(*index) {
        Some(value) => {
            *index += 1;
            Ok(value.clone())
        }
        None => Err(format!("{name} requires a value")),
    }
}

fn parse_u32(raw: &str, name: &str) -> Result<u32, String> {
    let value: u32 = raw
        .parse()
        .map_err(|_| format!("{name} expects a positive integer, got {raw:?}"))?;
    if value == 0 {
        return Err(format!("{name} must be at least 1"));
    }
    Ok(value)
}

/// Parse a byte count with an optional decimal suffix: 500, 512K, 10M, 2G.
fn parse_byte_value(raw: &str, name: &str) -> Result<u64, String> {
    let mut digits = raw.trim().to_string();
    let Some(last) = digits.pop() else {
        return Err(format!("{name} requires a value"));
    };
    let multiplier = match last {
        'k' | 'K' => 1_000_u64,
        'm' | 'M' => 1_000_000,
        'g' | 'G' => 1_000_000_000,
        _ => {
            digits.push(last);
            1
        }
    };
    let base: u64 = digits
        .trim()
        .parse()
        .map_err(|_| format!("{name} expects bytes or a K/M/G suffix, got {raw:?}"))?;
    let value = base
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{name} is too large: {raw:?}"))?;
    if value == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(value)
}

fn parse_resume(raw: &str) -> Result<ResumePolicy, String> {
    match raw.to_ascii_lowercase().as_str() {
        "allowed" => Ok(ResumePolicy::Allowed),
        "never" => Ok(ResumePolicy::Never),
        "required" => Ok(ResumePolicy::Required),
        _ => Err("resume expects allowed | never | required".to_string()),
    }
}

fn parse_overwrite(raw: &str) -> Result<OverwritePolicy, String> {
    match raw.to_ascii_lowercase().as_str() {
        "rename" => Ok(OverwritePolicy::Rename),
        "fail" => Ok(OverwritePolicy::FailIfExists),
        "replace" => Ok(OverwritePolicy::Replace),
        _ => Err("overwrite expects rename | fail | replace".to_string()),
    }
}

fn parse_args(args: &[String]) -> Result<Cli, String> {
    let mut url: Option<String> = None;
    let mut directory: Option<PathBuf> = None;
    let mut options = JobOptions::default();
    let mut index = 0;
    let mut flags_ended = false;

    while index < args.len() {
        let arg = args[index].clone();
        index += 1;
        if !flags_ended && arg == "--" {
            flags_ended = true;
            continue;
        }
        if !flags_ended && arg.starts_with('-') && arg.len() > 1 {
            let (name, inline) = match arg.split_once('=') {
                Some((name, value)) => (name.to_string(), Some(value.to_string())),
                None => (arg.clone(), None),
            };
            match name.as_str() {
                "-h" | "--help" => return Ok(Cli::Help),
                "-s" | "--segments" => {
                    let raw = take_value(args, &mut index, "--segments", inline)?;
                    options.segments = Some(parse_u32(&raw, "--segments")?);
                }
                "-S" | "--segment-size" => {
                    let raw = take_value(args, &mut index, "--segment-size", inline)?;
                    options.segment_size = Some(parse_byte_value(&raw, "--segment-size")?);
                }
                "-r" | "--rate" => {
                    let raw = take_value(args, &mut index, "--rate", inline)?;
                    options.rate_limit = Some(parse_byte_value(&raw, "--rate")?);
                }
                "--retries" => {
                    let raw = take_value(args, &mut index, "--retries", inline)?;
                    options.retries = Some(parse_u32(&raw, "--retries")?);
                }
                "--resume" => {
                    let raw = take_value(args, &mut index, "--resume", inline)?;
                    options.resume = Some(parse_resume(&raw)?);
                }
                "--overwrite" => {
                    let raw = take_value(args, &mut index, "--overwrite", inline)?;
                    options.overwrite = Some(parse_overwrite(&raw)?);
                }
                "-c" | "--connections" => {
                    let raw = take_value(args, &mut index, "--connections", inline)?;
                    options.connections = Some(parse_u32(&raw, "--connections")?);
                }
                other => return Err(format!("unknown option {other:?}")),
            }
            continue;
        }
        // Positional: URL then DIRECTORY.
        if url.is_none() {
            url = Some(arg);
        } else if directory.is_none() {
            directory = Some(PathBuf::from(arg));
        } else {
            return Err("unexpected extra argument (expected URL [DIRECTORY])".to_string());
        }
    }

    let Some(url) = url else {
        return Err("missing URL argument".to_string());
    };
    Ok(Cli::Run {
        url,
        directory: directory.unwrap_or_else(|| PathBuf::from(".")),
        options,
    })
}

/// Rolling completed-bytes window backing the displayed rate.
#[derive(Debug, Default)]
struct RateWindow {
    samples: VecDeque<(f64, u64)>,
}

impl RateWindow {
    fn push(&mut self, now_secs: f64, position_bytes: u64) {
        self.samples.push_back((now_secs, position_bytes));
        while self.samples.len() > 1
            && self
                .samples
                .front()
                .is_some_and(|&(t, _)| t < now_secs - RATE_WINDOW_SECS)
        {
            self.samples.pop_front();
        }
    }

    /// Mean completed bytes per second across the window, clamped at zero
    /// so a backwards-moving position never yields a negative rate.
    fn rate_per_sec(&self) -> f64 {
        let Some(&(t0, p0)) = self.samples.front() else {
            return 0.0;
        };
        let Some(&(t1, p1)) = self.samples.back() else {
            return 0.0;
        };
        let dt = t1 - t0;
        if dt <= 0.0 {
            return 0.0;
        }
        p1.saturating_sub(p0) as f64 / dt
    }
}

fn file_name_of(path: &str) -> String {
    Path::new(path).file_name().map_or_else(
        || path.to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}

fn pad_to_width(line: &str, previous_width: usize) -> String {
    let pad = previous_width.saturating_sub(line.chars().count());
    format!("{line}{}", " ".repeat(pad))
}

struct ProgressTracker {
    started: Instant,
    window: RateWindow,
    total_size: Option<u64>,
    saving_as: Option<String>,
    warnings: usize,
    last_line_width: usize,
}

impl ProgressTracker {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            window: RateWindow::default(),
            total_size: None,
            saving_as: None,
            warnings: 0,
            last_line_width: 0,
        }
    }

    fn on_event(&mut self, event: &Event) {
        match event {
            Event::ProbeCompleted { total_size, .. } => self.total_size = *total_size,
            Event::DestinationResolved { path } => self.saving_as = Some(file_name_of(path)),
            Event::Warning { .. } => self.warnings += 1,
            _ => {}
        }
    }

    fn progress_line(&mut self, snapshot: &ProgressSnapshot) -> String {
        let now = self.started.elapsed().as_secs_f64();
        self.progress_line_at(snapshot, now)
    }

    /// Build the single-line progress view at an explicit clock reading so
    /// rate/ETA rendering is testable without real delays.
    fn progress_line_at(&mut self, snapshot: &ProgressSnapshot, now_secs: f64) -> String {
        let position = snapshot
            .completed_bytes
            .saturating_add(snapshot.reused_bytes);
        self.window.push(now_secs, position);
        let rate = self.window.rate_per_sec();

        let mut line = match self.total_size {
            Some(total) => {
                let remaining = total.saturating_sub(position);
                let pct = if total == 0 {
                    100.0
                } else {
                    position.min(total) as f64 / total as f64 * 100.0
                };
                let eta = if remaining == 0 {
                    "00:00".to_string()
                } else if rate >= MIN_ETA_RATE {
                    format_eta(Duration::from_secs_f64(remaining as f64 / rate))
                } else {
                    "--".to_string()
                };
                format!(
                    "[download] {} / {} ({pct:.1}%) | {} | ETA {eta}",
                    format_bytes(position as f64),
                    format_bytes(total as f64),
                    format_rate(rate),
                )
            }
            None => format!(
                "[download] {} | {} | ETA --",
                format_bytes(position as f64),
                format_rate(rate)
            ),
        };
        line.push_str(&format!(" | retries {}", snapshot.retries));
        if let Some(name) = &self.saving_as {
            line.push_str(&format!(" | saving as {name}"));
        }
        if self.warnings > 0 {
            line.push_str(&format!(" | warnings {}", self.warnings));
        }
        line
    }

    fn render(&mut self, snapshot: &ProgressSnapshot, tty: bool) {
        let line = self.progress_line(snapshot);
        if tty {
            let width = line.chars().count();
            let padded = pad_to_width(&line, self.last_line_width);
            self.last_line_width = width;
            eprint!("\r{padded}");
        } else {
            eprintln!("{line}");
        }
    }

    fn finish_line(&self, tty: bool) {
        if tty {
            eprintln!();
        }
    }
}

/// Decimal (SI) byte formatting: B, KB, MB, GB, TB.
fn format_bytes(value: f64) -> String {
    let value = value.max(0.0);
    if value < 1_000.0 {
        format!("{} B", value as u64)
    } else if value < 1_000_000.0 {
        format!("{:.1} KB", value / 1_000.0)
    } else if value < 1_000_000_000.0 {
        format!("{:.1} MB", value / 1_000_000.0)
    } else if value < 1_000_000_000_000.0 {
        format!("{:.1} GB", value / 1_000_000_000.0)
    } else {
        format!("{:.1} TB", value / 1_000_000_000_000.0)
    }
}

fn format_rate(bytes_per_sec: f64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec))
}

fn format_eta(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs >= 86_400 {
        format!("{}d {:02}h", secs / 86_400, (secs % 86_400) / 3_600)
    } else if secs >= 3_600 {
        format!(
            "{}:{:02}:{:02}",
            secs / 3_600,
            (secs % 3_600) / 60,
            secs % 60
        )
    } else {
        format!("{:02}:{:02}", secs / 60, secs % 60)
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_args(&args) {
        Ok(Cli::Help) => {
            println!("{USAGE}");
            println!();
            println!("{OPTIONS}");
            println!();
            println!("Downloads URL into DIRECTORY (default '.') with automatic filename");
            println!("resolution, showing transfer speed in decimal MB/s and an ETA.");
            println!("An existing file is never replaced unless --overwrite says otherwise.");
            println!("Exit codes: 0 success, 1 failure, 2 usage.");
        }
        Ok(Cli::Run {
            url,
            directory,
            options,
        }) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let code = runtime.block_on(run(url, directory, options));
            exit(code);
        }
        Err(message) => {
            eprintln!("error: {message}");
            eprintln!("{USAGE}");
            exit(2);
        }
    }
}

/// Default parallel h2 connection slots for the downloader (§24 D5): each
/// slot adds another bounded 2 MiB flow-control window, so segmented
/// transfers scale past the single-connection window/RTT ceiling on
/// high-bandwidth or high-RTT paths. Mirrors what multi-connection
/// download managers do; the engine library default stays `Single`.
const DEFAULT_CONNECTIONS: u32 = 8;

/// Apply parsed CLI options onto the engine configuration and the wrapped
/// request. Options left at `None` keep engine/request defaults; overwrite
/// defaults to `Rename` so the downloader never replaces an existing file.
/// request. Options left at `None` keep engine/request defaults; overwrite
/// defaults to `Rename` so the downloader never replaces an existing file.
fn apply_job_options(
    config: &mut EngineConfig,
    request: &mut DownloadRequest,
    options: &JobOptions,
) {
    if let Some(segments) = options.segments {
        config.transfer.min_workers = segments;
        config.transfer.max_workers = segments;
        // Honor the explicit worker count regardless of file size; the
        // engine still falls back to a single stream without range support.
        config.transfer.segmentation_threshold = 1;
    }
    if let Some(size) = options.segment_size {
        config.transfer.initial_segment_size = size;
        // Widen the validation bounds so any requested size is accepted.
        config.transfer.min_segment_size = config.transfer.min_segment_size.min(size);
        config.transfer.max_segment_size = config.transfer.max_segment_size.max(size);
        config.transfer.segmentation_threshold = 1;
    }
    if let Some(rate) = options.rate_limit {
        config.network.rate_limit = Some(rate);
    }
    if let Some(retries) = options.retries {
        config.retry.max_attempts_per_segment = retries;
    }
    request.overwrite = options.overwrite.unwrap_or(OverwritePolicy::Rename);
    if let Some(resume) = options.resume {
        request.resume = resume;
    }
    let connections = options.connections.unwrap_or(DEFAULT_CONNECTIONS);
    config.h2_policy = H2ConnectionPolicy::Additional {
        max_connections: connections,
    };
    // The per-origin permits/pool must admit every connection slot; the
    // engine default (16) already covers the downloader default, larger
    // slot counts scale it.
    let per_origin = connections.max(16);
    config.max_connections_per_origin = per_origin;
    config.pool.max_per_origin = per_origin;
}

async fn run(url: String, directory: PathBuf, options: JobOptions) -> i32 {
    // Friendly fast fail; the engine revalidates authoritatively before any
    // network activity and reports a Configuration error otherwise.
    if !directory.is_dir() {
        eprintln!("error: not a directory: {}", directory.display());
        return 2;
    }

    let mut config = EngineConfig::default();
    let mut request = DirectoryDownloadRequest::new(url, directory);
    apply_job_options(&mut config, request.request_mut(), &options);

    let transport = match HttpTransport::from_config(&config) {
        Ok(transport) => transport,
        Err(error) => {
            eprintln!("error: transport setup failed: {error}");
            return 1;
        }
    };
    let controller = DownloadController::new(transport, config);

    let (handle, task) = controller.start_to_directory(request);
    // Subscribe before any progress so lifecycle events are not missed.
    let mut events = handle.events();
    let tty = std::io::stderr().is_terminal();
    let mut tracker = ProgressTracker::new();
    let mut last_logged: Option<Instant> = None;

    loop {
        while let Some(event) = events.try_next() {
            tracker.on_event(&event);
        }
        if task.is_finished() {
            break;
        }
        if tty {
            tracker.render(&handle.snapshot(), true);
        } else if last_logged.is_none_or(|at| at.elapsed() >= NON_TTY_LOG_INTERVAL) {
            tracker.render(&handle.snapshot(), false);
            last_logged = Some(Instant::now());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    // Final drain so queued terminal events (e.g. Committed) are observed.
    while let Some(event) = events.try_next() {
        tracker.on_event(&event);
    }

    let result = match task.await {
        Ok(result) => result,
        Err(join_error) => {
            tracker.finish_line(tty);
            eprintln!("error: job task failed: {join_error}");
            return 1;
        }
    };
    tracker.finish_line(tty);

    match result {
        Ok(completed) => {
            let accounting = &completed.accounting;
            let position = accounting
                .completed_bytes
                .saturating_add(accounting.bytes_reused_from_checkpoint);
            let wall = tracker.started.elapsed().as_secs_f64();
            let average = position as f64 / wall.max(1e-6);
            println!("completed: {}", completed.final_path.display());
            println!(
                "summary: {} in {:.1}s | average {} | reused {} | wasted {} | retries {} | range-requests {}",
                format_bytes(position as f64),
                wall,
                format_rate(average),
                format_bytes(accounting.bytes_reused_from_checkpoint as f64),
                format_bytes(accounting.wasted_bytes as f64),
                accounting.retries,
                accounting.segment_requests,
            );
            0
        }
        Err(error) => {
            let accounting = error.accounting();
            eprintln!("error: {error}");
            eprintln!(
                "       downloaded={} B, completed={} B, reused={} B, retries={}",
                accounting.bytes_downloaded_from_network,
                accounting.completed_bytes,
                accounting.bytes_reused_from_checkpoint,
                accounting.retries,
            );
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|&s| s.to_string()).collect()
    }

    #[test]
    fn parse_requires_a_url() {
        assert!(parse_args(&[]).is_err());
    }

    #[test]
    fn parse_url_defaults_to_the_current_directory() {
        let parsed = parse_args(&args(&["http://example.test/f.bin"])).expect("parsed");
        let Cli::Run {
            url,
            directory,
            options: _,
        } = parsed
        else {
            panic!("expected a run configuration");
        };
        assert_eq!(url, "http://example.test/f.bin");
        assert_eq!(directory, PathBuf::from("."));
    }

    #[test]
    fn parse_accepts_an_explicit_directory() {
        let parsed = parse_args(&args(&["http://example.test/f.bin", "out dir"])).expect("parsed");
        let Cli::Run {
            url,
            directory,
            options: _,
        } = parsed
        else {
            panic!("expected a run configuration");
        };
        assert_eq!(url, "http://example.test/f.bin");
        assert_eq!(directory, PathBuf::from("out dir"));
    }

    #[test]
    fn parse_rejects_extra_arguments() {
        assert!(parse_args(&args(&["u", "a", "b"])).is_err());
    }

    #[test]
    fn parse_recognizes_help_flags() {
        assert!(matches!(parse_args(&args(&["-h"])), Ok(Cli::Help)));
        assert!(matches!(parse_args(&args(&["--help"])), Ok(Cli::Help)));
    }

    #[test]
    fn byte_formatting_is_decimal() {
        assert_eq!(format_bytes(0.0), "0 B");
        assert_eq!(format_bytes(999.0), "999 B");
        assert_eq!(format_bytes(1_000.0), "1.0 KB");
        assert_eq!(format_bytes(1_500_000.0), "1.5 MB");
        assert_eq!(format_bytes(1.5e9), "1.5 GB");
    }

    #[test]
    fn rate_formatting_appends_per_second() {
        assert_eq!(format_rate(0.0), "0 B/s");
        assert_eq!(format_rate(512.0), "512 B/s");
        assert_eq!(format_rate(8_200_000.0), "8.2 MB/s");
    }

    #[test]
    fn eta_formatting_scales_units() {
        assert_eq!(format_eta(Duration::from_secs(0)), "00:00");
        assert_eq!(format_eta(Duration::from_secs(15)), "00:15");
        assert_eq!(format_eta(Duration::from_secs(60)), "01:00");
        assert_eq!(format_eta(Duration::from_secs(3_661)), "1:01:01");
        assert_eq!(format_eta(Duration::from_secs(90_000)), "1d 01h");
    }

    #[test]
    fn window_rate_is_positive_and_clamped() {
        let mut window = RateWindow::default();
        window.push(0.0, 0);
        window.push(1.0, 1_000);
        window.push(2.0, 2_000);
        assert_eq!(window.rate_per_sec(), 1_000.0);

        let mut backwards = RateWindow::default();
        backwards.push(0.0, 100);
        backwards.push(1.0, 50);
        assert_eq!(backwards.rate_per_sec(), 0.0);
    }

    #[test]
    fn window_rate_expires_old_samples() {
        let mut window = RateWindow::default();
        window.push(0.0, 0);
        window.push(6.0, 1_000);
        // The 0 s sample fell out of the 5 s window: only one sample left.
        assert_eq!(window.rate_per_sec(), 0.0);
        window.push(6.5, 1_500);
        assert_eq!(window.rate_per_sec(), 1_000.0);
    }

    #[test]
    fn unknown_total_shows_a_placeholder_eta() {
        let mut tracker = ProgressTracker::new();
        tracker.window.push(0.0, 0);
        tracker.window.push(5.0, 20_480);
        let snapshot = ProgressSnapshot {
            completed_bytes: 20_480,
            ..ProgressSnapshot::default()
        };
        let line = tracker.progress_line_at(&snapshot, 10.0);
        assert!(line.contains("ETA --"), "line: {line}");
        assert!(!line.contains(" / "), "line: {line}");
    }

    #[test]
    fn known_total_renders_percent_and_eta() {
        let mut tracker = ProgressTracker::new();
        tracker.total_size = Some(100_000);
        tracker.window.push(5.0, 0);
        let snapshot = ProgressSnapshot {
            completed_bytes: 20_480,
            ..ProgressSnapshot::default()
        };
        let line = tracker.progress_line_at(&snapshot, 10.0);
        assert!(line.contains("20.5 KB / 100.0 KB (20.5%)"), "line: {line}");
        assert!(line.contains("4.1 KB/s"), "line: {line}");
        assert!(line.contains("ETA 00:19"), "line: {line}");
    }

    #[test]
    fn slow_rate_hides_the_eta() {
        let mut tracker = ProgressTracker::new();
        tracker.total_size = Some(100_000);
        tracker.window.push(5.0, 0);
        let snapshot = ProgressSnapshot {
            completed_bytes: 1_024,
            ..ProgressSnapshot::default()
        };
        let line = tracker.progress_line_at(&snapshot, 10.0);
        assert!(line.contains("ETA --"), "line: {line}");
    }

    #[test]
    fn zero_total_reports_completion() {
        let mut tracker = ProgressTracker::new();
        tracker.total_size = Some(0);
        let snapshot = ProgressSnapshot::default();
        let line = tracker.progress_line_at(&snapshot, 1.0);
        assert!(line.contains("(100.0%)"), "line: {line}");
        assert!(line.contains("ETA 00:00"), "line: {line}");
    }

    #[test]
    fn lifecycle_events_update_the_line() {
        let mut tracker = ProgressTracker::new();
        tracker.total_size = Some(1_000);
        tracker.on_event(&Event::DestinationResolved {
            path: "/tmp/destination/file.bin".to_string(),
        });
        tracker.on_event(&Event::Warning {
            detail: "something advisory".to_string(),
        });
        let snapshot = ProgressSnapshot {
            completed_bytes: 250,
            retries: 1,
            ..ProgressSnapshot::default()
        };
        let line = tracker.progress_line_at(&snapshot, 1.0);
        assert!(line.contains("saving as file.bin"), "line: {line}");
        assert!(line.contains("warnings 1"), "line: {line}");
        assert!(line.contains("retries 1"), "line: {line}");
    }

    #[test]
    fn probe_completed_seeds_the_total() {
        let mut tracker = ProgressTracker::new();
        tracker.on_event(&Event::ProbeCompleted {
            total_size: Some(1_000),
            range_support: true,
        });
        assert_eq!(tracker.total_size, Some(1_000));
        tracker.on_event(&Event::ProbeCompleted {
            total_size: None,
            range_support: false,
        });
        assert_eq!(tracker.total_size, None);
    }

    #[test]
    fn padding_covers_the_previous_line() {
        assert_eq!(pad_to_width("abc", 5), "abc  ");
        assert_eq!(pad_to_width("longer line", 3), "longer line");
    }

    #[test]
    fn flags_accept_value_and_inline_forms() {
        for argv in [
            vec!["--segments", "4", "http://example.test/f.bin"],
            vec!["-s", "4", "http://example.test/f.bin"],
            vec!["--segments=4", "http://example.test/f.bin"],
        ] {
            let parsed = parse_args(&args(&argv)).expect("parsed");
            let Cli::Run { options, .. } = parsed else {
                panic!("expected a run configuration");
            };
            assert_eq!(options.segments, Some(4), "argv: {argv:?}");
        }
    }

    #[test]
    fn byte_values_accept_decimal_suffixes() {
        assert_eq!(parse_byte_value("1024", "--rate").expect("bytes"), 1_024);
        assert_eq!(parse_byte_value("512K", "--rate").expect("kilo"), 512_000);
        assert_eq!(parse_byte_value("10M", "--rate").expect("mega"), 10_000_000);
        assert_eq!(
            parse_byte_value("2g", "--rate").expect("giga"),
            2_000_000_000
        );
        assert!(parse_byte_value("abc", "--rate").is_err());
        assert!(parse_byte_value("0", "--rate").is_err());
        assert!(parse_byte_value("", "--rate").is_err());
    }

    #[test]
    fn resume_and_overwrite_values_parse_case_insensitively() {
        assert_eq!(parse_resume("Allowed").ok(), Some(ResumePolicy::Allowed));
        assert_eq!(parse_resume("NEVER").ok(), Some(ResumePolicy::Never));
        assert_eq!(parse_resume("required").ok(), Some(ResumePolicy::Required));
        assert!(parse_resume("sometimes").is_err());
        assert_eq!(
            parse_overwrite("Rename").ok(),
            Some(OverwritePolicy::Rename)
        );
        assert_eq!(
            parse_overwrite("FAIL").ok(),
            Some(OverwritePolicy::FailIfExists)
        );
        assert_eq!(
            parse_overwrite("replace").ok(),
            Some(OverwritePolicy::Replace)
        );
        assert!(parse_overwrite("always").is_err());
    }

    #[test]
    fn parse_rejects_unknown_flags_and_missing_values() {
        assert!(parse_args(&args(&["--bogus"])).is_err());
        assert!(parse_args(&args(&["--segments"])).is_err());
        assert!(parse_args(&args(&["--rate"])).is_err());
    }

    #[test]
    fn parse_mixes_flags_with_positionals() {
        let parsed = parse_args(&args(&[
            "--rate",
            "10M",
            "http://example.test/f.bin",
            "out",
        ]))
        .expect("parsed");
        let Cli::Run {
            url,
            directory,
            options,
        } = parsed
        else {
            panic!("expected a run configuration");
        };
        assert_eq!(url, "http://example.test/f.bin");
        assert_eq!(directory, PathBuf::from("out"));
        assert_eq!(options.rate_limit, Some(10_000_000));
    }

    #[test]
    fn double_dash_ends_flag_parsing() {
        let parsed = parse_args(&args(&["--", "-weird-url"])).expect("parsed");
        let Cli::Run { url, .. } = parsed else {
            panic!("expected a run configuration");
        };
        assert_eq!(url, "-weird-url");
    }

    #[test]
    fn help_wins_even_after_flags() {
        assert!(matches!(
            parse_args(&args(&["--segments", "4", "--help"])),
            Ok(Cli::Help)
        ));
    }

    #[test]
    fn options_tune_config_and_request() {
        let mut config = EngineConfig::default();
        let mut request = DownloadRequest::new("u", PathBuf::from("d"));
        let options = JobOptions {
            segments: Some(4),
            segment_size: Some(4_000_000),
            rate_limit: Some(5_000_000),
            retries: Some(3),
            resume: Some(ResumePolicy::Never),
            overwrite: Some(OverwritePolicy::FailIfExists),
            connections: None,
        };
        apply_job_options(&mut config, &mut request, &options);
        assert_eq!(config.transfer.min_workers, 4);
        assert_eq!(config.transfer.max_workers, 4);
        assert_eq!(config.transfer.segmentation_threshold, 1);
        assert_eq!(config.transfer.initial_segment_size, 4_000_000);
        assert_eq!(config.transfer.min_segment_size, 1024 * 1024);
        assert_eq!(config.transfer.max_segment_size, 64 * 1024 * 1024);
        assert_eq!(config.network.rate_limit, Some(5_000_000));
        assert_eq!(config.retry.max_attempts_per_segment, 3);
        assert_eq!(request.resume, ResumePolicy::Never);
        assert_eq!(request.overwrite, OverwritePolicy::FailIfExists);
        assert_eq!(
            config.h2_policy,
            H2ConnectionPolicy::Additional { max_connections: 8 }
        );
        assert_eq!(config.max_connections_per_origin, 16);
        assert_eq!(config.pool.max_per_origin, 16);
    }

    #[test]
    fn segment_size_widens_validation_bounds() {
        let mut config = EngineConfig::default();
        let mut request = DownloadRequest::new("u", PathBuf::from("d"));
        apply_job_options(
            &mut config,
            &mut request,
            &JobOptions {
                segment_size: Some(128 * 1024 * 1024),
                ..JobOptions::default()
            },
        );
        assert_eq!(config.transfer.initial_segment_size, 128 * 1024 * 1024);
        assert_eq!(config.transfer.max_segment_size, 128 * 1024 * 1024);
        assert_eq!(config.transfer.segmentation_threshold, 1);
    }

    #[test]
    fn default_options_keep_rename_and_engine_defaults() {
        let mut config = EngineConfig::default();
        let mut request = DownloadRequest::new("u", PathBuf::from("d"));
        apply_job_options(&mut config, &mut request, &JobOptions::default());
        assert_eq!(request.overwrite, OverwritePolicy::Rename);
        assert_eq!(request.resume, ResumePolicy::Allowed);
        assert_eq!(config.transfer.max_workers, 8);
        assert_eq!(config.network.rate_limit, None);
        assert_eq!(config.transfer.segmentation_threshold, 16 * 1024 * 1024);
        assert_eq!(
            config.h2_policy,
            H2ConnectionPolicy::Additional { max_connections: 8 }
        );
    }

    #[test]
    fn connections_scale_permits_and_slots() {
        let mut config = EngineConfig::default();
        let mut request = DownloadRequest::new("u", PathBuf::from("d"));
        apply_job_options(
            &mut config,
            &mut request,
            &JobOptions {
                connections: Some(32),
                ..JobOptions::default()
            },
        );
        assert_eq!(
            config.h2_policy,
            H2ConnectionPolicy::Additional {
                max_connections: 32
            }
        );
        assert_eq!(config.max_connections_per_origin, 32);
        assert_eq!(config.pool.max_per_origin, 32);
        // A small slot count keeps the engine's per-origin floor.
        apply_job_options(
            &mut config,
            &mut request,
            &JobOptions {
                connections: Some(2),
                ..JobOptions::default()
            },
        );
        assert_eq!(config.max_connections_per_origin, 16);
        assert_eq!(config.pool.max_per_origin, 16);
    }
}
