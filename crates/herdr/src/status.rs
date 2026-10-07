//! Herdr agent records and their mapping to [`TurnStatus`].

use chimera_core::terminal::TurnStatus;

use crate::HerdrError;

/// The part of a Herdr agent record that decides a turn's fate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRecord {
    /// Herdr's agent status: `idle`, `working`, `blocked`, `done` or `unknown` (protocol 22).
    /// Kept as a string so a status added by a newer Herdr still decodes.
    pub status: String,
}

impl AgentRecord {
    /// Decodes an `agent_info` result (`{"type":"agent_info","agent":{...}}`). The record
    /// carries the agent's name in `agent` and its state in `agent_status`; a missing or `null`
    /// record, or one whose own `agent` is `null`, means the pane hosts no agent.
    pub fn from_agent_info(result: &serde_json::Value) -> Result<Option<Self>, HerdrError> {
        match result.get("agent") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(record) => Self::from_fields(record),
        }
    }

    /// Decodes the agent out of a `pane_info` result (`{"type":"pane_info","pane":{...}}`).
    /// There `agent` is the agent's name (a string, absent or `null` when the pane hosts none)
    /// and `agent_status` is its sibling field.
    pub fn from_pane_info(result: &serde_json::Value) -> Result<Option<Self>, HerdrError> {
        match result.get("pane") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(pane) => Self::from_fields(pane),
        }
    }

    /// Reads `agent` (string or null) and `agent_status` from a record shared by both shapes.
    fn from_fields(record: &serde_json::Value) -> Result<Option<Self>, HerdrError> {
        match record.get("agent") {
            None | Some(serde_json::Value::Null) => return Ok(None),
            Some(serde_json::Value::String(_)) => {}
            Some(_) => {
                return Err(HerdrError::Protocol {
                    message: "agent is not a string".into(),
                    uncertain: false,
                });
            }
        }
        match record
            .get("agent_status")
            .and_then(|status| status.as_str())
        {
            Some(status) => Ok(Some(Self {
                status: status.to_owned(),
            })),
            None => Err(HerdrError::Protocol {
                message: "agent record has no agent_status".into(),
                uncertain: false,
            }),
        }
    }
}

/// Whether `error` says the agent, pane or workspace no longer exists
/// (`agent_not_found`, `pane_not_found`, `workspace_not_found`).
pub fn is_not_found(error: &HerdrError) -> bool {
    matches!(
        error,
        HerdrError::Server { code, .. }
            if matches!(code.as_str(), "agent_not_found" | "pane_not_found" | "workspace_not_found")
    )
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

    // Shapes below are taken from `herdr agent get` / `herdr pane get` (protocol 22).
    fn agent_info(status: &str) -> serde_json::Value {
        json!({"type":"agent_info","agent":{
            "agent":"claude",
            "agent_session":{"agent":"claude","kind":"id","source":"herdr:claude","value":"43769262-f084-410f-a6eb-ade3dc65173c"},
            "agent_status":status,"completion_seq":200,"cwd":"/home/u/proj","focused":false,
            "interactive_ready":true,"name":"orchestrator","pane_id":"w8:p2","revision":5,
            "state_change_seq":200,"tab_id":"w8:t1","terminal_id":"term_1",
            "workspace_id":"w8"
        }})
    }

    fn pane_info(status: &str) -> serde_json::Value {
        json!({"type":"pane_info","pane":{
            "agent":"codex",
            "agent_session":{"agent":"codex","kind":"id","source":"herdr:codex","value":"01a0fdad"},
            "agent_status":status,"cwd":"/home/u/proj","focused":false,"pane_id":"w9:p1",
            "revision":5,"tab_id":"w9:t1","terminal_id":"term_2","workspace_id":"w9"
        }})
    }

    fn status_of_agent_info(status: &str) -> TurnStatus {
        turn_status(
            AgentRecord::from_agent_info(&agent_info(status))
                .unwrap()
                .as_ref(),
        )
    }

    fn status_of_pane_info(status: &str) -> TurnStatus {
        turn_status(
            AgentRecord::from_pane_info(&pane_info(status))
                .unwrap()
                .as_ref(),
        )
    }

    fn server_error(code: &str, message: &str) -> HerdrError {
        HerdrError::Server {
            code: code.into(),
            message: message.into(),
        }
    }

    #[test]
    fn working_is_running() {
        assert_eq!(status_of_agent_info("working"), TurnStatus::Running);
        assert_eq!(status_of_pane_info("working"), TurnStatus::Running);
    }

    #[test]
    fn blocked_is_running() {
        assert_eq!(status_of_agent_info("blocked"), TurnStatus::Running);
        assert_eq!(status_of_pane_info("blocked"), TurnStatus::Running);
    }

    #[test]
    fn unknown_is_running() {
        assert_eq!(status_of_agent_info("unknown"), TurnStatus::Running);
        assert_eq!(status_of_pane_info("unknown"), TurnStatus::Running);
    }

    #[test]
    fn idle_is_finished() {
        assert_eq!(status_of_agent_info("idle"), TurnStatus::Finished);
        assert_eq!(status_of_pane_info("idle"), TurnStatus::Finished);
    }

    #[test]
    fn done_is_finished() {
        assert_eq!(status_of_agent_info("done"), TurnStatus::Finished);
        assert_eq!(status_of_pane_info("done"), TurnStatus::Finished);
    }

    #[test]
    fn unrecognised_status_is_running() {
        assert_eq!(status_of_agent_info("hibernating"), TurnStatus::Running);
        assert_eq!(status_of_pane_info("hibernating"), TurnStatus::Running);
        assert_eq!(status_of_agent_info(""), TurnStatus::Running);
    }

    #[test]
    fn agent_info_without_agent_is_gone() {
        for result in [
            json!({"type":"agent_info","agent":null}),
            json!({"type":"agent_info"}),
            // the record is present but names no agent
            json!({"type":"agent_info","agent":{"agent":null,"agent_status":"unknown","pane_id":"w5:p1"}}),
            json!({"type":"agent_info","agent":{"agent_status":"unknown","pane_id":"w5:p1"}}),
        ] {
            let agent = AgentRecord::from_agent_info(&result).unwrap();
            assert_eq!(turn_status(agent.as_ref()), TurnStatus::Gone, "{result}");
        }
    }

    #[test]
    fn pane_without_agent_is_gone() {
        // A shell pane as `herdr pane get` reports it: no `agent`, status `unknown`.
        let plain = json!({"type":"pane_info","pane":{
            "agent_status":"unknown","cwd":"/home/u/proj","focused":false,"pane_id":"w5:p1",
            "revision":1,"tab_id":"w5:t1","terminal_id":"term_3","workspace_id":"w5"
        }});
        let null_agent = json!({"type":"pane_info","pane":{
            "agent":null,"agent_status":"unknown","pane_id":"w5:p1"
        }});
        for result in [plain, null_agent] {
            let agent = AgentRecord::from_pane_info(&result).unwrap();
            assert_eq!(turn_status(agent.as_ref()), TurnStatus::Gone, "{result}");
        }
    }

    #[test]
    fn not_found_errors_are_gone() {
        for error in [
            server_error("agent_not_found", "agent target w8:p9 not found"),
            server_error("pane_not_found", "pane w99:p1 not found"),
            server_error("workspace_not_found", "workspace w99 not found"),
        ] {
            assert_eq!(turn_status_of_lookup(Err(error)).unwrap(), TurnStatus::Gone);
        }
    }

    #[test]
    fn other_errors_are_not_mapped() {
        let other = server_error("internal", "boom");
        assert!(turn_status_of_lookup(Err(other)).is_err());
        assert!(
            turn_status_of_lookup(Err(HerdrError::Protocol {
                message: "x".into(),
                uncertain: false,
            }))
            .is_err()
        );
    }

    #[test]
    fn malformed_records_are_errors() {
        let no_status = json!({"type":"agent_info","agent":{"agent":"claude","pane_id":"w2:p1"}});
        assert!(AgentRecord::from_agent_info(&no_status).is_err());
        let object_agent = json!({"type":"pane_info","pane":{"agent":{"agent_status":"idle"}}});
        assert!(AgentRecord::from_pane_info(&object_agent).is_err());
    }
}
