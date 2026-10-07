//! Herdr adapter for the `Terminal` port.
//!
//! [`HerdrTerminal`] implements `chimera_core::terminal::Terminal` over Herdr's JSON protocol on
//! its Unix socket. The transport is async: each request is a native `tokio::net::UnixStream`
//! exchange (one request per connection, newline-delimited JSON, bounded response, per-request
//! timeout), so it never blocks the Tokio runtime. Nothing is retried, and the adapter keeps no
//! workspace, pane, or agent state, so a new instance reconnects from the socket path alone.
//!
//! [`HerdrError`] classifies every failure as *failed* or *uncertain* and converts into
//! `chimera_core::PortError`.

mod herdr;
mod launch;
mod status;
mod terminal;
mod workspace;

pub(crate) use herdr::HerdrClient;
pub use herdr::HerdrError;
pub(crate) use launch::DEFAULT_READY_TIMEOUT;
pub(crate) use status::{AgentRecord, turn_status_of_lookup};
pub use terminal::HerdrTerminal;
