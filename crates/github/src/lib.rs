//! GitHub integration and translation between external data and the core domain.
//!
//! Expose the module interface here; keep implementation submodules private.

mod client;
mod close_issue;
mod error;
mod plan;
mod pulls;

pub use client::{Access, Client, DEFAULT_API_URL};
pub use error::GitHubError;
