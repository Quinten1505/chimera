use chimera_core::error::PortError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error of a pipeline step or of the driver. Port errors keep their failed/uncertain
/// classification so callers can decide between retrying and reconciling.
#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Port(#[from] PortError),
    /// A pipeline state could not be converted to or from its stored form.
    #[error("pipeline state could not be (de)serialized: {0}")]
    State(#[from] serde_json::Error),
}

impl PipelineError {
    /// The effect is known not to have happened.
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Port(error) if error.is_failed())
    }

    /// The effect may have happened and must be reconciled before retrying.
    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Port(error) if error.is_uncertain())
    }
}

/// Why a pipeline stopped without finishing. Shared by all pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PauseReason {
    /// A review, implementation or merge limit was used up.
    LimitExhausted,
    /// The run-wide pause flag was set.
    GlobalPause,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_errors_keep_their_classification() {
        let failed = PipelineError::from(PortError::failed("boom"));
        assert!(failed.is_failed() && !failed.is_uncertain());
        let uncertain = PipelineError::from(PortError::uncertain("lost"));
        assert!(uncertain.is_uncertain() && !uncertain.is_failed());
    }

    #[test]
    fn state_errors_are_neither_failed_nor_uncertain() {
        let error = PipelineError::from(serde_json::from_str::<u32>("x").unwrap_err());
        assert!(!error.is_failed() && !error.is_uncertain());
    }

    #[test]
    fn pause_reason_serde_round_trip() {
        for reason in [PauseReason::LimitExhausted, PauseReason::GlobalPause] {
            let json = serde_json::to_value(reason).unwrap();
            assert_eq!(serde_json::from_value::<PauseReason>(json).unwrap(), reason);
        }
    }
}
