//! KDown local application host: durable jobs, path policy, supervision,
//! and the loopback web API. See `docs/superpowers/specs/2026-09-29-kdown-web-ui-design.md`.

pub mod api;
pub mod cli;
pub mod domain;
pub mod engine_adapter;
pub mod error;
pub mod events;
pub mod path_policy;
pub mod platform;
pub mod registry;
pub mod supervisor;
