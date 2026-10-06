//! Loading and validating application settings for the composition root.

mod codex;
mod schema;

pub use codex::{AgentKind, CodexConfiguration, CodexOptions, ConfigurationError};
pub use schema::Configuration;
