use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// An application-tracked agent running in a Herdr pane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    pub name: String,
    /// Agent type used when launching through Herdr.
    pub kind: String,
    /// Herdr pane hosting this agent.
    pub pane_id: String,
    /// Provider session reference, when known; separate from the application session ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<AgentSessionReference>,
}

/// A provider's conversation ID or session file path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AgentSessionReference {
    Id(String),
    Path(PathBuf),
}
