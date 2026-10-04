use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A Herdr workspace and its panes, for either a repo root or a linked worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePanes {
    /// The Herdr workspace ID, including for workspaces opened at the repo root.
    pub workspace_id: String,
    pub pane_ids: Vec<String>,
    /// Associated Git checkout, when known; may be the repo root or a linked worktree.
    /// None means the association is unknown, not necessarily that this is outside Git.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkout_path: Option<PathBuf>,
}

/// An application session containing multiple tracked Herdr workspaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// Application-owned session identifier, separate from an agent's conversation ID.
    pub session_id: String,
    pub workspaces: Vec<WorkspacePanes>,
}
