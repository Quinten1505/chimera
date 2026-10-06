use std::io;
use thiserror::Error;

/// A nonempty string free of every control character (including LF, CR, tab and NUL).
pub(crate) fn is_plain_text(text: &str) -> bool {
    !text.trim().is_empty() && !text.chars().any(char::is_control)
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("cannot read configuration file: {0}")]
    Io(#[from] io::Error),
    #[error("invalid YAML at {path} (line {line}, column {column}): {source}")]
    Yaml {
        path: String,
        line: usize,
        column: usize,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("{field} must be nonempty and contain no control characters")]
    Invalid { field: String },
    #[error("unknown Codex setting {field}")]
    UnknownSetting { field: String },
    #[error("{field} must be {expected}")]
    InvalidSetting {
        field: String,
        expected: &'static str,
    },
    #[error("cannot build Codex arguments for provider {0}")]
    NotCodex(String),
    #[error("{field} is required")]
    Missing { field: String },
    #[error("{field} must be a positive integer")]
    InvalidLimit { field: String },
    #[error("{field}: unsupported provider {provider:?} (supported: codex)")]
    UnsupportedProvider { field: String, provider: String },
    #[error("unknown agent profile: {0}")]
    UnknownProfile(String),
}

impl From<serde_path_to_error::Error<serde_yaml::Error>> for ConfigurationError {
    fn from(error: serde_path_to_error::Error<serde_yaml::Error>) -> Self {
        let path = error.path().to_string();
        let source = error.into_inner();
        let (line, column) = source
            .location()
            .map_or((0, 0), |location| (location.line(), location.column()));
        Self::Yaml {
            path,
            line,
            column,
            source,
        }
    }
}
