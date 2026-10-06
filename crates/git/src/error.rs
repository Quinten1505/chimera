use std::io;

use chimera_core::error::PortError;
use thiserror::Error;

/// Failure of a `git` invocation, classified as failed or uncertain.
#[derive(Debug, Error)]
pub enum GitError {
    /// `git` is not installed or not on `PATH`. The effect did not happen.
    #[error("git not found: {0}")]
    NotFound(#[source] io::Error),
    /// The process could not be started. The effect did not happen.
    #[error("failed to spawn git: {0}")]
    Spawn(#[source] io::Error),
    /// `git` ran and the effect is known not to have happened.
    #[error("git {command} failed ({status}): {stderr}")]
    Failed {
        command: String,
        status: String,
        stderr: String,
    },
    /// `git` was changing the remote and the outcome is unknown.
    #[error("git {command} outcome unknown ({status}): {stderr}")]
    Uncertain {
        command: String,
        status: String,
        stderr: String,
    },
}

impl GitError {
    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain { .. })
    }
}

impl From<GitError> for PortError {
    fn from(error: GitError) -> Self {
        let cause = error.to_string();
        match error {
            GitError::Uncertain { .. } => PortError::uncertain(cause),
            _ => PortError::failed(cause),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_variants_convert_to_failed_port_errors() {
        let errors = [
            GitError::NotFound(io::Error::from(io::ErrorKind::NotFound)),
            GitError::Spawn(io::Error::from(io::ErrorKind::PermissionDenied)),
            GitError::Failed {
                command: "commit".into(),
                status: "exit status: 1".into(),
                stderr: "nothing to commit".into(),
            },
        ];
        for error in errors {
            assert!(!error.is_uncertain());
            assert!(PortError::from(error).is_failed());
        }
    }

    #[test]
    fn uncertain_converts_to_uncertain_port_error_with_stderr() {
        let error = GitError::Uncertain {
            command: "push".into(),
            status: "signal: 9 (SIGKILL)".into(),
            stderr: "Writing objects: 50%".into(),
        };
        assert!(error.is_uncertain());
        let port = PortError::from(error);
        assert!(port.is_uncertain());
        assert!(port.to_string().contains("Writing objects: 50%"));
    }

    #[test]
    fn failed_conversion_keeps_stderr_in_cause() {
        let port = PortError::from(GitError::Failed {
            command: "fetch".into(),
            status: "exit status: 128".into(),
            stderr: "fatal: bad object".into(),
        });
        assert!(port.is_failed());
        assert!(port.to_string().contains("fatal: bad object"));
    }
}
