use std::path::PathBuf;

use chimera_core::error::PortError;
use thiserror::Error;

/// Error raised by the store. Every file-related variant names the path involved.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error(
        "cannot determine the state directory: neither an absolute XDG_STATE_HOME nor an absolute HOME is set"
    )]
    StateRootUnresolved,
    #[error("invalid state root {}: {reason}", root.display())]
    InvalidRoot { root: PathBuf, reason: &'static str },
    #[error("state root {} is inside the repository or worktree {}", root.display(), repository.display())]
    RootInRepository { root: PathBuf, repository: PathBuf },
    #[error("invalid run id {id:?} for the run directory under {}: it must be a single plain path component", root.display())]
    InvalidRunId { root: PathBuf, id: String },
    #[error("unknown run: no run data in the run directory {}", directory.display())]
    RunNotFound { directory: PathBuf },
    #[error("invalid pipeline id {id:?}: it must be a single plain path component")]
    InvalidPipelineId { id: String },
    #[error("effect {key:?} of pipeline {pipeline:?} already has an intent recorded")]
    EffectAlreadyRecorded { pipeline: String, key: String },
    #[error("effect {key:?} of pipeline {pipeline:?} has no intent recorded")]
    EffectNotRecorded { pipeline: String, key: String },
    #[error("file system error at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The file was replaced, but flushing its directory failed: a crash may revert the change.
    #[error("{} was replaced, but syncing its directory failed, so the change may not survive a crash: {source}", path.display())]
    Unsynced {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Appending to the file failed, and so did durably removing what the append wrote: the
    /// appended content may be in the file, now or after a crash.
    #[error("appending to {} failed ({source}), and removing the partial append failed too, so it may remain: {revert}", path.display())]
    Unreverted {
        path: PathBuf,
        source: Box<StoreError>,
        revert: std::io::Error,
    },
    #[error("cannot serialize {}: {source}", path.display())]
    Serialize {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("cannot deserialize {}: {source}", path.display())]
    Deserialize {
        path: PathBuf,
        source: serde_json::Error,
    },
}

impl StoreError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// A change that took effect but may not survive a crash, or a failed change that may not have been
/// undone, is uncertain; every other failure leaves the stored state unchanged and is classified as
/// failed.
impl From<StoreError> for PortError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Unsynced { .. } | StoreError::Unreverted { .. } => {
                PortError::uncertain(error.to_string())
            }
            _ => PortError::failed(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_to_failed_port_error_with_path_in_cause() {
        let error = StoreError::io(
            "/state/run/file.json",
            std::io::Error::other("disk on fire"),
        );
        let port: PortError = error.into();
        assert!(port.is_failed());
        let text = port.to_string();
        assert!(text.contains("/state/run/file.json"));
        assert!(text.contains("disk on fire"));
    }

    #[test]
    fn unsynced_change_is_uncertain_with_path_in_cause() {
        let error = StoreError::Unsynced {
            path: "/state/run/file.json".into(),
            source: std::io::Error::other("disk on fire"),
        };
        let port: PortError = error.into();
        assert!(port.is_uncertain());
        assert!(port.to_string().contains("/state/run/file.json"));
    }

    #[test]
    fn unreverted_append_is_uncertain_with_both_causes() {
        let error = StoreError::Unreverted {
            path: "/state/run/history.md".into(),
            source: Box::new(StoreError::io(
                "/state/run/history.md",
                std::io::Error::other("disk on fire"),
            )),
            revert: std::io::Error::other("still on fire"),
        };
        let port: PortError = error.into();
        assert!(port.is_uncertain());
        let text = port.to_string();
        assert!(text.contains("/state/run/history.md"));
        assert!(text.contains("disk on fire"));
        assert!(text.contains("still on fire"));
    }

    #[test]
    fn unresolved_root_is_failed() {
        let port: PortError = StoreError::StateRootUnresolved.into();
        assert!(port.is_failed());
        assert!(port.to_string().contains("XDG_STATE_HOME"));
    }
}
