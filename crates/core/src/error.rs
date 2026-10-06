use thiserror::Error;

/// Error returned by every port. Adapters classify whether the external effect is known not to
/// have happened or is unknown.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PortError {
    /// The effect did not happen; safe to retry.
    #[error("failed: {0}")]
    Failed(String),
    /// The response was lost; the effect must be reconciled before retrying.
    #[error("uncertain: {0}")]
    Uncertain(String),
}

impl PortError {
    pub fn failed(cause: impl Into<String>) -> Self {
        Self::Failed(cause.into())
    }

    pub fn uncertain(cause: impl Into<String>) -> Self {
        Self::Uncertain(cause.into())
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_is_classified_as_failed() {
        let error = PortError::failed("connection refused");
        assert!(error.is_failed());
        assert!(!error.is_uncertain());
    }

    #[test]
    fn uncertain_is_classified_as_uncertain() {
        let error = PortError::uncertain("timed out waiting for response");
        assert!(error.is_uncertain());
        assert!(!error.is_failed());
    }

    #[test]
    fn display_carries_cause() {
        assert_eq!(PortError::failed("boom").to_string(), "failed: boom");
        assert_eq!(PortError::uncertain("lost").to_string(), "uncertain: lost");
    }
}
