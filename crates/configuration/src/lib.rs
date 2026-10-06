//! Loading and validating application settings for the composition root.

mod codex;
mod codex_args;
mod schema;

pub use codex::{AgentKind, CodexConfiguration, CodexOptions, ConfigurationError};
pub use codex_args::codex_launch_args;
pub use schema::Configuration;
