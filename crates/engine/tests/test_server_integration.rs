//! Integration tests for the misbehaving-HTTP test server (§36.2).
//!
//! Each behavior gets its own test hitting the raw server with a minimal
//! hand-rolled HTTP client, proving the server itself before the engine
//! is built on top of it.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use support::test_server::{RangeMode, ScriptedResponse, TestServer};

/// Minimal HTTP/1.1 GET client for exercising the test server directly.
/// Returns (status, headers, body).
async fn raw_get(
    url: &str,
    extra_headers: &[(&str, &str)],
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let url = url.strip_prefix("http://").expect("http url");
    let (host, path) = url.split_once('/').expect("path");
    let addr: String = if host.contains(':') {
        host.to_string()
    } else {
        format!("{host}:80")
    };
    let mut stream = tokio::net::TcpStream::connect(&addr).await.expect("connect");
    let mut req = format!("GET /{path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n");
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.expect("write request");
    let mut buf = Vec::new();
    let mut read_buf = [0u8; 16384];
    // Read until content-length is satisfied or connection closes.
    loop {
        match stream.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
            Err(_) => break, // reset mid-body: keep what we got
        }
        if let Some(len) = content_length_of(&buf) {
            let head_end = find_head_end(&buf);
            if let Some(head_end) = head_end {
                if buf.len() >= head_end + 4 + len {
                    break;
                }
            }
        }
    }
    parse_response(&buf)
}

fn find_head_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length_of(raw: &[u8]) -> Option<usize> {
    let head_end = find_head_end(raw)?;
    let head = String::from_utf8_lossy(&raw[..head_end]);
    head.lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())
        })
        .flatten()
}

fn parse_response(raw: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let head_end = find_head_end(raw).expect("head terminator");
    let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let body = raw[head_end + 4..].to_vec();
    (status, headers, body)
}

#[tokio::test]
async fn serves_static_content() {
    let server = TestServer::new()
        .serve_static("/file", vec![1, 2, 3, 4, 5])
        .start()
        .await
        .expect("start");
    let (status, _, body) = raw_get(&server.url("/file"), &[]).await;
    assert_eq!(status, 200);
    assert_eq!(body, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn correct_range_returns_206_with_content_range() {
    let content: Vec<u8> = (0u8..=100u8).collect();
    let server = TestServer::new()
        .serve_static("/r", content)
        .start()
        .await
        .expect("start");
    let (status, headers, body) =
        raw_get(&server.url("/r"), &[("range", "bytes=10-19")]).await;
    assert_eq!(status, 206);
    assert_eq!(
        headers.iter().find(|(k, _)| k == "content-range").unwrap().1,
        "bytes 10-19/101"
    );
    assert_eq!(body, (10u8..=19u8).collect::<Vec<u8>>());
}

#[tokio::test]
async fn lying_server_returns_200_full_body_on_range() {
    let content: Vec<u8> = vec![7u8; 64];
    let server = TestServer::new()
        .serve_ranges("/liar", content.clone(), RangeMode::Full200)
        .start()
        .await
        .expect("start");
    let (status, _, body) = raw_get(&server.url("/liar"), &[("range", "bytes=0-7")]).await;
    assert_eq!(status, 200);
    assert_eq!(body, content);
}

#[tokio::test]
async fn no_ranges_mode_never_advertises() {
    let server = TestServer::new()
        .serve_ranges("/plain", vec![1; 32], RangeMode::NoRanges)
        .start()
        .await
        .expect("start");
    // HEAD-like probe: headers only.
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port_of(&server))).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"HEAD /plain HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut raw = Vec::new();
    let mut b = [0u8; 4096];
    // HEAD responses end after headers; a single read suffices here.
    if let Ok(n) = stream.read(&mut b).await {
        raw.extend_from_slice(&b[..n]);
    }
    let text = String::from_utf8_lossy(&raw);
    assert!(!text.to_lowercase().contains("accept-ranges"));
}

#[tokio::test]
async fn malformed_content_range_is_served() {
    let server = TestServer::new()
        .serve_ranges("/bad", vec![1; 64], RangeMode::MalformedContentRange)
        .start()
        .await
        .expect("start");
    let (status, headers, _) = raw_get(&server.url("/bad"), &[("range", "bytes=0-7")]).await;
    assert_eq!(status, 206);
    let cr = headers
        .iter()
        .find(|(k, _)| k == "content-range")
        .expect("content-range present");
    assert!(cr.1.contains("bogus"));
}

#[tokio::test]
async fn truncated_body_then_connection_close() {
    let body = vec![9u8; 1000];
    let server = TestServer::new()
        .serve_handler("/cut", move |_req| ScriptedResponse::ok(body.clone()).reset_after(200))
        .start()
        .await
        .expect("start");
    let (status, _, got) = raw_get(&server.url("/cut"), &[]).await;
    assert_eq!(status, 200);
    assert_eq!(got.len(), 200, "connection reset mid-body must truncate");
}

#[tokio::test]
async fn delayed_chunks_arrive_slowly() {
    let body = vec![3u8; 200_000]; // 4 chunks of 64 KiB
    let server = TestServer::new()
        .serve_handler("/slow", move |_req| {
            ScriptedResponse::ok(body.clone()).chunked(Duration::from_millis(100))
        })
        .start()
        .await
        .expect("start");
    let start = std::time::Instant::now();
    let (_, _, got) = raw_get(&server.url("/slow"), &[]).await;
    assert_eq!(got, vec![3u8; 200_000]);
    assert!(start.elapsed() >= Duration::from_millis(250), "chunks must be delayed");
}

#[tokio::test]
async fn scripted_sequence_then_fallback_404() {
    let server = TestServer::new()
        .serve_n("/flaky", 2, ScriptedResponse::ok(vec![1, 2, 3]))
        .start()
        .await
        .expect("start");
    for _ in 0..2 {
        let (status, _, body) = raw_get(&server.url("/flaky"), &[]).await;
        assert_eq!((status, body), (200, vec![1, 2, 3]));
    }
    let (status, _, _) = raw_get(&server.url("/flaky"), &[]).await;
    assert_eq!(status, 404, "script exhausted -> fallback");
}

#[tokio::test]
async fn records_requests_for_inspection() {
    let server = TestServer::new()
        .serve_static("/seen", vec![1])
        .start()
        .await
        .expect("start");
    let _ = raw_get(&server.url("/seen"), &[("range", "bytes=0-0")]).await;
    let _ = raw_get(&server.url("/seen"), &[]).await;
    assert_eq!(server.request_count("/seen").await, 2);
    let reqs = server.requests().await;
    assert!(reqs[0].range.is_some());
    assert!(reqs[1].range.is_none());
}

// Helper for the HEAD test.
fn port_of(server: &support::test_server::RunningServer) -> u16 {
    let url = server.url("/");
    url.trim_start_matches("http://")
        .split(':')
        .next_back()
        .and_then(|p| p.trim_end_matches('/').parse().ok())
        .unwrap_or(0)
}