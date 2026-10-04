use crate::{HerdrClient, HerdrError, WorkspaceOptions, WorktreeOptions};
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

/// Where to create the first workspace of an application session.
#[derive(Debug, Clone)]
pub enum SessionTarget {
    /// Open an existing directory without creating a Git checkout.
    Workspace(WorkspaceOptions),
    /// Create a Git checkout and open it in Herdr.
    Worktree(WorktreeOptions),
}

impl HerdrClient {
    /// Start an application session and record its initial Herdr workspace and pane.
    ///
    /// The caller supplies the application session ID. State is returned in memory;
    /// callers may serialize it for persistence. This does not launch an AI agent.
    pub fn start_session(
        &self,
        session_id: impl Into<String>,
        target: &SessionTarget,
    ) -> Result<Session, HerdrError> {
        let session_id = session_id.into();
        if session_id.trim().is_empty() {
            return Err(HerdrError::InvalidInput("session_id must not be empty"));
        }
        let workspace = match target {
            SessionTarget::Workspace(options) => {
                let created = self.create_workspace(options)?;
                WorkspacePanes {
                    workspace_id: created.workspace.workspace_id,
                    pane_ids: vec![created.root_pane.pane_id],
                    checkout_path: created.workspace.checkout_path,
                }
            }
            SessionTarget::Worktree(options) => {
                let created = self.create_worktree(options)?;
                WorkspacePanes {
                    workspace_id: created.workspace.workspace_id,
                    pane_ids: vec![created.root_pane.pane_id],
                    checkout_path: Some(created.worktree.path),
                }
            }
        };
        Ok(Session {
            session_id,
            workspaces: vec![workspace],
        })
    }
}
