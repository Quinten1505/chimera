//! Herdr client and session tracking.
//!
//! The transport is async: each request is a native `tokio::net::UnixStream` exchange (one
//! request per connection, newline-delimited JSON, bounded response, per-request timeout), so it
//! never blocks the Tokio runtime. It is not wrapped in `spawn_blocking`. Nothing is retried.
//!
//! [`HerdrError`] classifies every failure as *failed* or *uncertain* and converts into
//! `chimera_core::PortError`.

mod herdr;
mod launch;
mod session;
mod status;
mod workspace;

pub use herdr::{
    CreatedWorkspace, HerdrClient, HerdrError, OUTPUT_MAX_BYTES, OUTPUT_MAX_LINES, Pane,
    PaneOptions, SplitDirection, Tab, Workspace, WorkspaceOptions,
};

pub use status::{AgentRecord, is_not_found, turn_status, turn_status_of_lookup};

pub use launch::DEFAULT_READY_TIMEOUT;
pub use session::{Session, SessionTarget, WorkspacePanes};
