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
    workspace::root_key,
};

/// Source name under which Chimera reports metadata to Herdr.
pub(crate) const METADATA_SOURCE: &str = "chimera";

/// Pane token holding the number of prompts the pane has received. It lives in Herdr, so the
/// receipt survives a Chimera restart.
const PROMPTS_TOKEN: &str = "chimera_prompts";

/// Workspace token holding the key of the directory the workspace was created for.
pub(crate) const ROOT_TOKEN: &str = "chimera_root";

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
    #[serde(default)]
    tokens: HashMap<String, String>,
}

#[derive(Deserialize)]
struct WorkspaceRecord {
    workspace_id: String,
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

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, HerdrError> {
        #[derive(Deserialize)]
        struct List {
            workspaces: Vec<WorkspaceRecord>,
        }
        let list: List = self
            .request("workspace.list", &json!({}), "workspace_list", Effect::Read)
            .await?;
        Ok(list.workspaces)
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

#[async_trait]
impl Terminal for HerdrTerminal {
    async fn create_workspace(&self, directory: &Path) -> Result<(WorkspaceId, PaneId), PortError> {
        Ok(self.client.create_workspace_in(directory).await?)
    }

    /// The first workspace, in Herdr's order, that Chimera created for `directory`. The root is
    /// recorded in a workspace token at creation, so it does not follow a pane's current
    /// directory.
    async fn find_workspace(
        &self,
        directory: &Path,
    ) -> Result<Option<(WorkspaceId, Vec<PaneId>)>, PortError> {
        require_absolute(directory)?;
        let key = root_key(directory);
        let Some(found) = self
            .client
            .list_workspaces()
            .await?
            .into_iter()
            .find(|workspace| {
                workspace.tokens.get(ROOT_TOKEN).map(String::as_str) == Some(key.as_str())
            })
        else {
            return Ok(None);
        };
        let mut panes: Vec<PaneRecord> = self
            .client
            .list_panes()
            .await?
            .into_iter()
            .filter(|pane| pane.workspace_id == found.workspace_id)
            .collect();
        panes.sort_by_key(|pane| pane_number(&pane.pane_id));
        let ids = panes
            .into_iter()
            .map(|pane| PaneId::new(pane.pane_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PortError::failed("Herdr returned an empty pane ID"))?;
        let workspace = WorkspaceId::new(found.workspace_id)
            .map_err(|_| PortError::failed("Herdr returned an empty workspace ID"))?;
        Ok(Some((workspace, ids)))
    }

    /// Splits `pane` after checking that it belongs to `workspace`; a mismatch is failed and
    /// changes nothing in Herdr.
    async fn split_pane(
        &self,
        workspace: &WorkspaceId,
        pane: &PaneId,
    ) -> Result<PaneId, PortError> {
        let record = self.client.get_pane(pane).await?;
        if record.workspace_id != workspace.as_str() {
            return Err(HerdrError::InvalidInput("pane does not belong to the workspace").into());
        }
        Ok(self.client.split_pane(pane).await?)
    }

    async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
        Ok(self
            .client
            .launch_command_line(pane, command_line, DEFAULT_READY_TIMEOUT)
            .await?)
    }

    /// Counts the prompt in a Herdr pane token, then sends it. The count is written first, so
    /// a send whose reply is lost (the prompt may have been delivered) still counts, and it
    /// survives a Chimera restart. A send that is certain to have failed takes the count back;
    /// if that fails too, the count may be wrong and the result is uncertain.
    async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
        let before = self.client.prompts_received(pane).await?;
        self.client.record_prompt(pane, before + 1).await?;
        let Err(error) = self.client.send_prompt(pane, prompt).await else {
            return Ok(());
        };
        if error.is_uncertain() {
            return Err(error.into());
        }
        match self.client.record_prompt(pane, before).await {
            Ok(()) => Err(error.into()),
            Err(rollback) => Err(HerdrError::Protocol {
                message: format!(
                    "prompt failed ({error}) and its count was not undone: {rollback}"
                ),
                uncertain: true,
            }
            .into()),
        }
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
    use serde_json::json;

    #[test]
    fn orders_panes_by_creation_number() {
        let mut ids = vec!["w1:p10", "w1:p2", "w1:p1"];
        ids.sort_by_key(|id| pane_number(id));
        assert_eq!(ids, ["w1:p1", "w1:p2", "w1:p10"]);
        assert_eq!(pane_number("odd"), u64::MAX);
    }

    #[test]
    fn root_key_is_stable_short_and_follows_the_real_path() {
        let tmp = std::env::temp_dir();
        assert_eq!(root_key(&tmp.join(".")), root_key(&tmp));
        assert_ne!(root_key(Path::new("/a/b")), root_key(Path::new("/a/c")));
        assert_eq!(root_key(Path::new("/a/b")).len(), 32);
    }

    /// A fake Herdr: one connection per request, answered from `script` by method. A method
    /// mapped to `None` drops the connection without a reply. Records every request.
    struct Fake {
        dir: std::path::PathBuf,
        requests: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl Fake {
        fn start(
            name: &str,
            script: impl Fn(&serde_json::Value) -> Option<serde_json::Value> + Send + 'static,
        ) -> (HerdrTerminal, Self) {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            let dir = std::env::temp_dir()
                .join(format!("chimera-herdr-term-{}-{name}", std::process::id()));
            std::fs::create_dir(&dir).unwrap();
            let socket = dir.join("api.sock");
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = requests.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let mut stream = BufReader::new(stream);
                    let mut line = String::new();
                    stream.read_line(&mut line).await.unwrap();
                    let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                    seen.lock().unwrap().push(request.clone());
                    if let Some(result) = script(&request) {
                        let reply = match result.get("code") {
                            Some(_) => json!({"id": request["id"], "error": result}),
                            None => json!({"id": request["id"], "result": result}),
                        };
                        let mut bytes = reply.to_string().into_bytes();
                        bytes.push(b'\n');
                        stream.get_mut().write_all(&bytes).await.unwrap();
                    }
                }
            });
            let terminal = HerdrTerminal {
                client: HerdrClient {
                    socket_path: socket,
                    timeout: std::time::Duration::from_secs(2),
                },
            };
            (terminal, Self { dir, requests })
        }

        fn methods(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|request| request["method"].as_str().unwrap().to_owned())
                .collect()
        }

        /// Token values written by `pane.report_metadata`, in order.
        fn written_counts(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == "pane.report_metadata")
                .map(|request| request["params"]["tokens"][PROMPTS_TOKEN].to_string())
                .collect()
        }
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn pane_info(workspace: &str, tokens: serde_json::Value) -> serde_json::Value {
        json!({"type":"pane_info","pane":{
            "pane_id":"w1:p1","workspace_id":workspace,"tokens":tokens}})
    }

    fn pane(id: &str) -> PaneId {
        PaneId::new(id).unwrap()
    }

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::new(id).unwrap()
    }

    #[tokio::test]
    async fn split_with_a_pane_from_another_workspace_is_failed_and_changes_nothing() {
        let (terminal, fake) = Fake::start("split-mismatch", |_| Some(pane_info("w2", json!({}))));
        let error = terminal
            .split_pane(&ws("w1"), &pane("w1:p1"))
            .await
            .unwrap_err();
        assert!(error.is_failed());
        assert_eq!(fake.methods(), ["pane.get"]);
    }

    #[tokio::test]
    async fn split_within_the_workspace_splits() {
        let (terminal, fake) = Fake::start("split-ok", |request| {
            Some(match request["method"].as_str().unwrap() {
                "pane.get" => pane_info("w1", json!({})),
                _ => json!({"type":"pane_info","pane":{"pane_id":"w1:p2","workspace_id":"w1"}}),
            })
        });
        let new = terminal
            .split_pane(&ws("w1"), &pane("w1:p1"))
            .await
            .unwrap();
        assert_eq!(new, pane("w1:p2"));
        assert_eq!(fake.methods(), ["pane.get", "pane.split"]);
    }

    #[tokio::test]
    async fn workspace_is_found_by_its_recorded_root_not_by_pane_cwd() {
        let root = std::env::temp_dir();
        let key = root_key(&root);
        let (terminal, _fake) = Fake::start("find", move |request| {
            Some(match request["method"].as_str().unwrap() {
                "workspace.list" => json!({"type":"workspace_list","workspaces":[
                    {"workspace_id":"w1","tokens":{}},
                    {"workspace_id":"w2","tokens":{ROOT_TOKEN: key}}]}),
                // The first pane has since moved to another directory.
                _ => json!({"type":"pane_list","panes":[
                    {"pane_id":"w1:p1","workspace_id":"w1","cwd":"/elsewhere"},
                    {"pane_id":"w2:p2","workspace_id":"w2","cwd":"/moved"},
                    {"pane_id":"w2:p1","workspace_id":"w2","cwd":"/also/moved"}]}),
            })
        });
        let (workspace, panes) = terminal.find_workspace(&root).await.unwrap().unwrap();
        assert_eq!(workspace, ws("w2"));
        assert_eq!(panes, [pane("w2:p1"), pane("w2:p2")]);
        assert!(
            terminal
                .find_workspace(Path::new("/nowhere/else"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn create_records_the_root_token() {
        let dir = std::env::temp_dir();
        let (terminal, fake) = Fake::start("create", |request| {
            Some(match request["method"].as_str().unwrap() {
                "workspace.create" => json!({"type":"workspace_created",
                    "workspace":{"workspace_id":"w3"},"root_pane":{"pane_id":"w3:p1"}}),
                _ => json!({"type":"ok"}),
            })
        });
        terminal.create_workspace(&dir).await.unwrap();
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests[1]["method"], "workspace.report_metadata");
        assert_eq!(requests[1]["params"]["workspace_id"], "w3");
        assert_eq!(requests[1]["params"]["tokens"][ROOT_TOKEN], root_key(&dir));
    }

    #[tokio::test]
    async fn create_whose_root_cannot_be_recorded_is_uncertain_and_closes_the_workspace() {
        let (terminal, fake) = Fake::start("create-fail", |request| {
            Some(match request["method"].as_str().unwrap() {
                "workspace.create" => json!({"type":"workspace_created",
                    "workspace":{"workspace_id":"w3"},"root_pane":{"pane_id":"w3:p1"}}),
                "workspace.report_metadata" => json!({"code":"internal","message":"m"}),
                _ => json!({"type":"ok"}),
            })
        });
        let error = terminal
            .create_workspace(&std::env::temp_dir())
            .await
            .unwrap_err();
        assert!(error.is_uncertain());
        assert_eq!(fake.methods().last().unwrap(), "workspace.close");
    }

    /// Fake whose pane starts with `before` prompts and answers `agent.prompt` with `prompt`.
    fn prompting(
        name: &str,
        before: u64,
        prompt: Option<serde_json::Value>,
        report_fails: bool,
    ) -> (HerdrTerminal, Fake) {
        let reports = std::sync::atomic::AtomicU64::new(0);
        Fake::start(name, move |request| {
            match request["method"].as_str().unwrap() {
                "pane.get" => Some(pane_info("w1", json!({PROMPTS_TOKEN: before.to_string()}))),
                "pane.report_metadata" => {
                    // Only the rollback (the second report) fails when `report_fails`.
                    let nth = reports.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Some(if report_fails && nth > 0 {
                        json!({"code":"internal","message":"m"})
                    } else {
                        json!({"type":"ok"})
                    })
                }
                _ => prompt.clone(),
            }
        })
    }

    fn accepted() -> Option<serde_json::Value> {
        Some(json!({"type":"agent_prompted","agent":{"agent_status":"working"}}))
    }

    #[tokio::test]
    async fn delivered_prompt_is_counted_before_it_is_sent() {
        let (terminal, fake) = prompting("sent", 4, accepted(), false);
        terminal.send_prompt(&pane("w1:p1"), "go").await.unwrap();
        assert_eq!(
            fake.methods(),
            ["pane.get", "pane.report_metadata", "agent.prompt"]
        );
        assert_eq!(fake.written_counts(), ["\"5\""]);
    }

    #[tokio::test]
    async fn lost_reply_stays_counted_and_uncertain() {
        // The connection drops after the request was written: the prompt may have arrived.
        let (terminal, fake) = prompting("lost", 4, None, false);
        let error = terminal
            .send_prompt(&pane("w1:p1"), "go")
            .await
            .unwrap_err();
        assert!(error.is_uncertain());
        assert_eq!(fake.written_counts(), ["\"5\""]);
    }

    #[tokio::test]
    async fn prompt_stalled_after_submission_stays_counted_and_uncertain() {
        let stalled = Some(json!({"code":"agent_prompt_stalled","message":"m"}));
        let (terminal, fake) = prompting("stalled", 0, stalled, false);
        let error = terminal
            .send_prompt(&pane("w1:p1"), "go")
            .await
            .unwrap_err();
        assert!(error.is_uncertain());
        assert_eq!(fake.written_counts(), ["\"1\""]);
    }

    #[tokio::test]
    async fn rejected_prompt_takes_its_count_back_and_is_failed() {
        let rejected = Some(json!({"code":"agent_blocked","message":"m"}));
        let (terminal, fake) = prompting("rejected", 4, rejected, false);
        let error = terminal
            .send_prompt(&pane("w1:p1"), "go")
            .await
            .unwrap_err();
        assert!(error.is_failed());
        assert_eq!(fake.written_counts(), ["\"5\"", "\"4\""]);
    }

    #[tokio::test]
    async fn rejected_prompt_whose_count_cannot_be_undone_is_uncertain() {
        let rejected = Some(json!({"code":"agent_blocked","message":"m"}));
        let (terminal, _fake) = prompting("rollback-fails", 4, rejected, true);
        let error = terminal
            .send_prompt(&pane("w1:p1"), "go")
            .await
            .unwrap_err();
        assert!(error.is_uncertain());
    }

    #[tokio::test]
    async fn prompt_is_not_sent_when_its_count_cannot_be_recorded() {
        let (terminal, fake) = Fake::start("record-fails", |request| {
            Some(match request["method"].as_str().unwrap() {
                "pane.get" => pane_info("w1", json!({})),
                _ => json!({"code":"internal","message":"m"}),
            })
        });
        let error = terminal
            .send_prompt(&pane("w1:p1"), "go")
            .await
            .unwrap_err();
        assert!(error.is_failed());
        assert!(!fake.methods().contains(&"agent.prompt".to_owned()));
    }

    #[tokio::test]
    async fn count_is_read_from_the_pane_so_a_new_instance_sees_it() {
        let (terminal, _fake) = prompting("count", 7, accepted(), false);
        assert_eq!(terminal.prompts_received(&pane("w1:p1")).await.unwrap(), 7);
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
