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

impl crate::HerdrClient {
    /// Start Codex in every untracked pane and record each success immediately.
    ///
    /// Stops on the first error. Previously tracked panes are skipped. A timeout may
    /// leave an untracked running agent: inspect Herdr before retrying.
    pub fn start_codex_agents(
        &self,
        workspace: &mut crate::WorkspacePanes,
        args: &[String],
    ) -> Result<(), crate::HerdrError> {
        let mut panes = std::collections::HashSet::new();
        for pane in &workspace.pane_ids {
            if pane.trim().is_empty() || !panes.insert(pane) {
                return Err(crate::HerdrError::InvalidInput(
                    "pane IDs must be nonempty and unique",
                ));
            }
        }
        let mut tracked = std::collections::HashSet::new();
        for agent in &workspace.agents {
            if !panes.contains(&agent.pane_id) || !tracked.insert(&agent.pane_id) {
                return Err(crate::HerdrError::InvalidInput(
                    "agents must belong to distinct workspace panes",
                ));
            }
        }
        for pane in &workspace.pane_ids {
            if workspace.agents.iter().any(|agent| agent.pane_id == *pane) {
                continue;
            }
            let agent = self.start_codex_agent(&format!("codex-{pane}"), pane, args)?;
            workspace.agents.push(agent);
        }
        Ok(())
    }
}
