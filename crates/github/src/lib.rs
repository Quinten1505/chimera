//! GitHub integration and translation between external data and the core domain.
//!
//! Expose the module interface here; keep implementation submodules private.

mod client;
mod close_issue;
mod error;
mod forge;
mod plan;
mod pulls;

pub use forge::GitHubForge;
