use chimera_core::error::PortError;
use thiserror::Error;

/// A GitHub request failure, classified by whether the effect is known not to have happened.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GitHubError {
    /// The request did not take effect.
    #[error("{0}")]
    Failed(String),
    /// A mutating request may or may not have taken effect.
    #[error("{0}")]
    Uncertain(String),
}

impl From<GitHubError> for PortError {
    fn from(error: GitHubError) -> Self {
        match error {
            GitHubError::Failed(cause) => PortError::failed(cause),
            GitHubError::Uncertain(cause) => PortError::uncertain(cause),
        }
    }
}
