use std::path::PathBuf;

use chimera_core::error::PortError;
use thiserror::Error;

/// Error raised by the store. Every file-related variant names the path involved.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("cannot determine the state directory: neither XDG_STATE_HOME nor HOME is set")]
    StateRootUnresolved,
    #[error("invalid run id {id:?} for the run directory under {}: it must be a single plain path component", root.display())]
    InvalidRunId { root: PathBuf, id: String },
    #[error("file system error at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
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

/// The store never reports an uncertain effect: every failure is classified as failed.
impl From<StoreError> for PortError {
    fn from(error: StoreError) -> Self {
        PortError::failed(error.to_string())
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
    fn unresolved_root_is_failed() {
        let port: PortError = StoreError::StateRootUnresolved.into();
        assert!(port.is_failed());
        assert!(port.to_string().contains("XDG_STATE_HOME"));
    }
}
