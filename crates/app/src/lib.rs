//! KDown local application host: durable jobs, path policy, supervision,
//! and the loopback web API. See `docs/superpowers/specs/2026-09-29-kdown-web-ui-design.md`.

pub mod domain;
pub mod error;
pub mod path_policy;
pub mod registry;
