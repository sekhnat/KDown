//! Static web assets: filesystem (`--web-dir`) or embedded
//! (`bundled-web`). Hashed `/assets/*` responses are immutable; HTML is
//! `no-cache`. The SPA fallback answers only extensionless known routes;
//! `/api/*` and missing `/assets/*` stay real 404s.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

#[cfg(feature = "bundled-web")]
#[derive(rust_embed::RustEmbed)]
#[folder = "../../web/dist"]
struct EmbeddedAssets;

/// Loads one asset by its URL path (for example `assets/index-abc.js`).
fn load_asset(web_dir: Option<&std::path::Path>, path: &str) -> Option<(Vec<u8>, String)> {
    let safe = validate_asset_path(path)?;
    let mime = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();

    #[cfg(feature = "bundled-web")]
    {
        if let Some(file) = EmbeddedAssets::get(&safe) {
            return Some((file.data.to_vec(), mime));
        }
    }

    if let Some(dir) = web_dir {
        let candidate = dir.join(&safe);
        let canonical = std::fs::canonicalize(&candidate).ok()?;
        if !canonical.starts_with(std::fs::canonicalize(dir).ok()?) {
            return None;
        }
        let bytes = std::fs::read(&canonical).ok()?;
        return Some((bytes, mime));
    }
    None
}

/// Rejects traversal and absolute components in asset paths.
fn validate_asset_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty()
        || trimmed.contains('\0')
        || trimmed.split('/').any(|part| part == ".." || part == ".")
    {
        return None;
    }
    Some(trimmed.to_string())
}

fn cache_control_for(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

fn asset_response(web_dir: Option<&std::path::Path>, path: &str) -> Response {
    let Some((bytes, mime)) = load_asset(web_dir, path) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let mut response = (StatusCode::OK, [(header::CONTENT_TYPE, mime)], bytes).into_response();
    if let Ok(value) = header::HeaderValue::from_str(cache_control_for(path)) {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

/// The known SPA routes that fall back to `index.html`; extensionless
/// paths outside these prefixes are real 404s. Accepts the request path
/// with or without its leading slash.
fn is_known_spa_path(path: &str) -> bool {
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    trimmed.is_empty()
        || trimmed == "downloads"
        || trimmed.starts_with("downloads/")
        || trimmed == "history"
        || trimmed == "settings"
}

/// Routes served when a web directory (or bundled assets) is available.
pub fn router(web_dir: Option<std::path::PathBuf>) -> Router<()> {
    let for_assets = web_dir.clone();
    let for_fallback = web_dir;
    Router::new()
        .route(
            "/assets/{*path}",
            get(
                |request: axum::extract::Request| async move {
                    let raw = request.uri().path().to_string();
                    let raw = raw.trim_start_matches('/').to_string();
                    asset_response(for_assets.as_deref(), &raw)
                },
            ),
        )
        .fallback(move |request: axum::extract::Request| {
            let web_dir = for_fallback.clone();
            async move {
                let raw = request.uri().path().to_string();
                let raw = raw.trim_start_matches('/').to_string();
                // API misses stay JSON 404s: the SPA fallback never masks
                // them, even when the nested router's own fallback loses
                // the scope race during merge.
                if raw == "api" || raw.starts_with("api/") {
                    return (
                        StatusCode::NOT_FOUND,
                        [(header::CONTENT_TYPE, "application/json")],
                        r#"{"code":"not_found","message":"the requested resource was not found","retryable":false}"#,
                    )
                        .into_response();
                }
                if is_known_spa_path(&raw) && !raw.contains('.') {
                    asset_response(web_dir.as_deref(), "index.html")
                } else {
                    (StatusCode::NOT_FOUND, "not found").into_response()
                }
            }
        })
}
