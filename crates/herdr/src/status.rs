//! Herdr agent records and their mapping to [`TurnStatus`].

use chimera_core::terminal::TurnStatus;
use serde::Deserialize;

use crate::HerdrError;

/// The part of a Herdr agent record that decides a turn's fate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AgentRecord {
    /// Herdr's agent status: `idle`, `working`, `blocked`, `done` or `unknown` (protocol 22).
    /// Kept as a string so a status added by a newer Herdr still decodes.
    pub status: String,
}

impl AgentRecord {
    /// Decodes an `agent_info` result (`{"type":"agent_info","agent":{...}}`). An `agent` of
    /// `null` or a missing one means the pane hosts no agent.
    pub fn from_agent_info(result: &serde_json::Value) -> Result<Option<Self>, HerdrError> {
        decode_optional(result.get("agent"))
    }

    /// Decodes the agent out of a `pane_info` result (`{"type":"pane_info","pane":{...}}`).
    /// A pane without an `agent` has none running.
    pub fn from_pane_info(result: &serde_json::Value) -> Result<Option<Self>, HerdrError> {
        decode_optional(result.get("pane").and_then(|pane| pane.get("agent")))
    }
}

fn decode_optional(value: Option<&serde_json::Value>) -> Result<Option<AgentRecord>, HerdrError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => Ok(Some(AgentRecord::deserialize(value)?)),
    }
}

/// Whether `error` says the pane or workspace no longer exists.
pub fn is_not_found(error: &HerdrError) -> bool {
    matches!(error, HerdrError::Server { code, .. } if code == "not_found")
}

/// Maps the agent on a pane (`None` when there is none, or the pane or workspace is gone) to the
/// state of its turn. Chimera has no inactivity timeout of its own, so this alone ends a turn.
///
/// - `working`, `blocked`, `unknown` → [`TurnStatus::Running`]. `blocked` means the agent waits
///   on something (an approval, a question) and the turn is not over: finishing it would
///   collect a half-done result. A stuck agent is surfaced through the pane, not by ending the
///   turn.
/// - `idle`, `done` → [`TurnStatus::Finished`].
/// - no agent → [`TurnStatus::Gone`].
/// - any other string → [`TurnStatus::Running`]: a newer Herdr must never end a turn early.
pub fn turn_status(agent: Option<&AgentRecord>) -> TurnStatus {
    match agent.map(|agent| agent.status.as_str()) {
        None => TurnStatus::Gone,
        Some("idle" | "done") => TurnStatus::Finished,
        Some(_) => TurnStatus::Running,
    }
}

/// [`turn_status`] for the outcome of looking the agent up: a not-found error means the pane or
/// workspace is gone; any other error is passed on.
pub fn turn_status_of_lookup(
    lookup: Result<Option<AgentRecord>, HerdrError>,
) -> Result<TurnStatus, HerdrError> {
    match lookup {
        Ok(agent) => Ok(turn_status(agent.as_ref())),
        Err(error) if is_not_found(&error) => Ok(TurnStatus::Gone),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn status_of_agent_info(status: &str) -> TurnStatus {
        let result = json!({"type":"agent_info","agent":{
            "pane_id":"w2:p1","agent":"codex","status":status,"extra":1
        }});
        turn_status(AgentRecord::from_agent_info(&result).unwrap().as_ref())
    }

    #[test]
    fn each_herdr_status_maps() {
        assert_eq!(status_of_agent_info("working"), TurnStatus::Running);
        assert_eq!(status_of_agent_info("blocked"), TurnStatus::Running);
        assert_eq!(status_of_agent_info("unknown"), TurnStatus::Running);
        assert_eq!(status_of_agent_info("idle"), TurnStatus::Finished);
        assert_eq!(status_of_agent_info("done"), TurnStatus::Finished);
    }

    #[test]
    fn unrecognised_status_is_running() {
        assert_eq!(status_of_agent_info("hibernating"), TurnStatus::Running);
        assert_eq!(status_of_agent_info(""), TurnStatus::Running);
    }

    #[test]
    fn null_or_missing_agent_in_agent_info_is_gone() {
        for result in [
            json!({"type":"agent_info","agent":null}),
            json!({"type":"agent_info"}),
        ] {
            let agent = AgentRecord::from_agent_info(&result).unwrap();
            assert_eq!(turn_status(agent.as_ref()), TurnStatus::Gone);
        }
    }

    #[test]
    fn pane_info_with_and_without_agent() {
        let with = json!({"type":"pane_info","pane":{
            "pane_id":"w2:p1","workspace_id":"w2","tab_id":"w2:t1",
            "agent":{"status":"done"}
        }});
        let agent = AgentRecord::from_pane_info(&with).unwrap();
        assert_eq!(turn_status(agent.as_ref()), TurnStatus::Finished);

        let without = json!({"type":"pane_info","pane":{
            "pane_id":"w2:p1","workspace_id":"w2","tab_id":"w2:t1"
        }});
        let agent = AgentRecord::from_pane_info(&without).unwrap();
        assert_eq!(turn_status(agent.as_ref()), TurnStatus::Gone);
    }

    #[test]
    fn not_found_error_is_gone() {
        let not_found = HerdrError::Server {
            code: "not_found".into(),
            message: "pane missing".into(),
        };
        assert_eq!(
            turn_status_of_lookup(Err(not_found)).unwrap(),
            TurnStatus::Gone
        );
    }

    #[test]
    fn other_errors_are_not_mapped() {
        let other = HerdrError::Server {
            code: "internal".into(),
            message: "boom".into(),
        };
        assert!(turn_status_of_lookup(Err(other)).is_err());
        assert!(turn_status_of_lookup(Err(HerdrError::Protocol("x".into()))).is_err());
    }

    #[test]
    fn malformed_record_is_an_error() {
        let result = json!({"type":"agent_info","agent":{"pane_id":"w2:p1"}});
        assert!(AgentRecord::from_agent_info(&result).is_err());
    }
}
