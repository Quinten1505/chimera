//! Loading and validating application settings for the composition root.

mod codex_args;
mod error;
mod schema;

use std::path::Path;

pub use codex_args::codex_launch_args;
pub use error::ConfigurationError;
pub use schema::Configuration;

/// Reads and validates the YAML file at `path`, returning both agent configurations,
/// their reset commands and the limits, or the first error with its field path.
pub fn load(path: impl AsRef<Path>) -> Result<Configuration, ConfigurationError> {
    Configuration::load(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../chimera.example.yaml");

    #[test]
    fn load_returns_the_validated_example() {
        let configuration = load(EXAMPLE).unwrap();
        assert_eq!(configuration.ticket.review.model, "gpt-6-luna");
        assert_eq!(configuration.final_review.merge.reset_command, "/clear");
        assert_eq!(configuration.limits.merge_attempts, 100);
    }

    #[test]
    fn load_reports_missing_files() {
        assert!(matches!(
            load("/nonexistent/chimera.yaml"),
            Err(ConfigurationError::Io(_))
        ));
    }
}
