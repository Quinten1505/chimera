use std::{collections::HashMap, path::Path};

use async_trait::async_trait;
use chimera_core::{
    PaneId, WorkspaceId,
    error::PortError,
    terminal::{Terminal, TurnStatus},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    DEFAULT_READY_TIMEOUT, HerdrClient, HerdrError, herdr::Effect, herdr::require_absolute,
};

/// Source name under which Chimera reports metadata to Herdr.
const METADATA_SOURCE: &str = "chimera";

/// Pane token holding the number of prompts the pane has received. It lives in Herdr, so the
/// receipt survives a Chimera restart.
const PROMPTS_TOKEN: &str = "chimera_prompts";

/// The [`Terminal`] port over a running Herdr's Unix socket.
///
/// It keeps no workspace, pane, or agent state: it holds only the socket path, and every
/// operation addresses Herdr by the IDs it is given. An instance built from the same socket
/// path after a Chimera restart therefore works with IDs saved by an earlier one.
#[derive(Debug, Clone)]
pub struct HerdrTerminal {
    client: HerdrClient,
}

impl HerdrTerminal {
    /// Connects to the Herdr server at `socket_path` and verifies it with a ping.
    ///
    /// Native Windows is unsupported; use WSL.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, HerdrError> {
        Ok(Self {
            client: HerdrClient::connect(socket_path).await?,
        })
    }
}

#[derive(Deserialize)]
struct PaneRecord {
    pane_id: String,
    workspace_id: String,
    cwd: Option<std::path::PathBuf>,
    #[serde(default)]
    tokens: HashMap<String, String>,
}

impl HerdrClient {
    async fn list_panes(&self) -> Result<Vec<PaneRecord>, HerdrError> {
        #[derive(Deserialize)]
        struct List {
            panes: Vec<PaneRecord>,
        }
        let list: List = self
            .request("pane.list", &json!({}), "pane_list", Effect::Read)
            .await?;
        Ok(list.panes)
    }

    async fn get_pane(&self, pane: &PaneId) -> Result<PaneRecord, HerdrError> {
        #[derive(Deserialize)]
        struct Found {
            pane: PaneRecord,
        }
        let found: Found = self
            .request(
                "pane.get",
                &json!({"pane_id": pane.as_str()}),
                "pane_info",
                Effect::Read,
            )
            .await?;
        Ok(found.pane)
    }

    async fn prompts_received(&self, pane: &PaneId) -> Result<u64, HerdrError> {
        let record = self.get_pane(pane).await?;
        match record.tokens.get(PROMPTS_TOKEN) {
            None => Ok(0),
            Some(value) => value.parse().map_err(|_| HerdrError::Protocol {
                message: format!("pane token {PROMPTS_TOKEN} is not a count: {value:?}"),
                uncertain: false,
            }),
        }
    }

    async fn record_prompt(&self, pane: &PaneId, received: u64) -> Result<(), HerdrError> {
        let _: serde_json::Value = self
            .request(
                "pane.report_metadata",
                &json!({
                    "pane_id": pane.as_str(),
                    "source": METADATA_SOURCE,
                    "tokens": {PROMPTS_TOKEN: received.to_string()},
                }),
                "ok",
                Effect::Change,
            )
            .await?;
        Ok(())
    }
}

/// The creation number of a pane ID such as `w8:p2`, for ordering panes within a workspace.
fn pane_number(pane_id: &str) -> u64 {
    pane_id
        .rsplit_once(":p")
        .and_then(|(_, number)| number.parse().ok())
        .unwrap_or(u64::MAX)
}

fn same_directory(reported: &Path, wanted: &Path) -> bool {
    reported == wanted
        || matches!(
            (reported.canonicalize(), wanted.canonicalize()),
            (Ok(a), Ok(b)) if a == b
        )
}

#[async_trait]
impl Terminal for HerdrTerminal {
    async fn create_workspace(&self, directory: &Path) -> Result<(WorkspaceId, PaneId), PortError> {
        Ok(self.client.create_workspace_in(directory).await?)
    }

    /// The first workspace, in Herdr's order, whose first pane is in `directory`. Herdr has no
    /// per-workspace directory, so this goes by the first pane's current directory.
    async fn find_workspace(
        &self,
        directory: &Path,
    ) -> Result<Option<(WorkspaceId, Vec<PaneId>)>, PortError> {
        require_absolute(directory)?;
        let mut workspaces: Vec<(String, Vec<PaneRecord>)> = Vec::new();
        for pane in self.client.list_panes().await? {
            match workspaces
                .iter_mut()
                .find(|(id, _)| *id == pane.workspace_id)
            {
                Some((_, panes)) => panes.push(pane),
                None => workspaces.push((pane.workspace_id.clone(), vec![pane])),
            }
        }
        for (workspace_id, mut panes) in workspaces {
            panes.sort_by_key(|pane| pane_number(&pane.pane_id));
            let rooted = panes[0]
                .cwd
                .as_deref()
                .is_some_and(|cwd| same_directory(cwd, directory));
            if rooted {
                let ids = panes
                    .into_iter()
                    .map(|pane| PaneId::new(pane.pane_id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| PortError::failed("Herdr returned an empty pane ID"))?;
                let workspace = WorkspaceId::new(workspace_id)
                    .map_err(|_| PortError::failed("Herdr returned an empty workspace ID"))?;
                return Ok(Some((workspace, ids)));
            }
        }
        Ok(None)
    }

    /// Herdr addresses a pane by an ID that already names its workspace, so `workspace` is not
    /// needed to split.
    async fn split_pane(
        &self,
        _workspace: &WorkspaceId,
        pane: &PaneId,
    ) -> Result<PaneId, PortError> {
        Ok(self.client.split_pane(pane).await?)
    }

    async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
        Ok(self
            .client
            .launch_command_line(pane, command_line, DEFAULT_READY_TIMEOUT)
            .await?)
    }

    /// Sends the prompt, then counts it in a Herdr pane token. A failure to record the count
    /// after Herdr accepted the prompt is reported as uncertain.
    async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
        let before = self.client.prompts_received(pane).await?;
        self.client.send_prompt(pane, prompt).await?;
        self.client
            .record_prompt(pane, before + 1)
            .await
            .map_err(|error| HerdrError::Protocol {
                message: format!("prompt accepted but not counted: {error}"),
                uncertain: true,
            })?;
        Ok(())
    }

    async fn prompts_received(&self, pane: &PaneId) -> Result<u64, PortError> {
        Ok(self.client.prompts_received(pane).await?)
    }

    async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
        Ok(self.client.read_status(pane).await?)
    }

    async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
        Ok(self.client.read_output(pane).await?)
    }

    async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
        Ok(self.client.close_workspace(workspace).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_panes_by_creation_number() {
        let mut ids = vec!["w1:p10", "w1:p2", "w1:p1"];
        ids.sort_by_key(|id| pane_number(id));
        assert_eq!(ids, ["w1:p1", "w1:p2", "w1:p10"]);
        assert_eq!(pane_number("odd"), u64::MAX);
    }

    #[test]
    fn matches_directories_exactly_or_by_real_path() {
        assert!(same_directory(Path::new("/a/b"), Path::new("/a/b")));
        assert!(!same_directory(Path::new("/a/b"), Path::new("/a/c")));
        let tmp = std::env::temp_dir();
        assert!(same_directory(&tmp.join("."), &tmp));
    }

    #[tokio::test]
    async fn terminal_is_usable_as_a_trait_object_and_rejects_relative_directories() {
        // A port that cannot reach Herdr still validates input before sending anything.
        let terminal = HerdrTerminal {
            client: HerdrClient {
                socket_path: "/nonexistent/chimera.sock".into(),
                timeout: std::time::Duration::from_secs(1),
            },
        };
        let terminal: std::sync::Arc<dyn Terminal> = std::sync::Arc::new(terminal);
        let error = terminal
            .find_workspace(Path::new("relative"))
            .await
            .unwrap_err();
        assert!(error.is_failed());
    }
}
