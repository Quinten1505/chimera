use crate::{HerdrClient, HerdrError, herdr::Effect, herdr::require_absolute};
use chimera_core::{PaneId, WorkspaceId};
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

/// Herdr's error code for a workspace that does not exist.
const WORKSPACE_NOT_FOUND: &str = "workspace_not_found";

impl HerdrClient {
    /// Open an existing absolute directory in a new workspace and return its ID and root pane.
    /// Creates no Git checkout.
    pub async fn create_workspace_in(
        &self,
        directory: &Path,
    ) -> Result<(WorkspaceId, PaneId), HerdrError> {
        require_absolute(directory)?;
        let created: crate::herdr::CreatedWorkspace = self
            .request(
                "workspace.create",
                &json!({"cwd": directory, "focus": false}),
                "workspace_created",
                Effect::Change,
            )
            .await?;
        Ok((
            workspace_id(created.workspace.workspace_id)?,
            pane_id(created.root_pane.pane_id)?,
        ))
    }

    /// Split a pane and return the new pane.
    pub async fn split_pane(&self, pane: &PaneId) -> Result<PaneId, HerdrError> {
        #[derive(Deserialize)]
        struct Split {
            pane: crate::herdr::Pane,
        }
        let split: Split = self
            .request(
                "pane.split",
                &json!({"target_pane_id": pane.as_str(), "direction": "right", "focus": false}),
                "pane_info",
                Effect::Change,
            )
            .await?;
        pane_id(split.pane.pane_id)
    }

    /// Close a workspace without closing its worktree group. A workspace that no longer exists
    /// counts as closed, so this is safe to repeat after an uncertain result.
    pub async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), HerdrError> {
        let result: Result<serde_json::Value, _> = self
            .request(
                "workspace.close",
                &json!({"workspace_id": workspace.as_str()}),
                "ok",
                Effect::Change,
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(HerdrError::Server { code, .. }) if code == WORKSPACE_NOT_FOUND => Ok(()),
            Err(error) => Err(error),
        }
    }
}

// An empty ID in a response means Herdr may have created the resource.
fn workspace_id(value: String) -> Result<WorkspaceId, HerdrError> {
    WorkspaceId::new(value).map_err(|_| empty_id("workspace"))
}

fn pane_id(value: String) -> Result<PaneId, HerdrError> {
    PaneId::new(value).map_err(|_| empty_id("pane"))
}

fn empty_id(kind: &str) -> HerdrError {
    HerdrError::Protocol {
        message: format!("Herdr returned an empty {kind} ID"),
        uncertain: true,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::{path::PathBuf, time::Duration};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("chimera-herdr-ws-{}-{name}", std::process::id()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir)
        }
        fn socket(&self) -> PathBuf {
            self.0.join("api.sock")
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.socket());
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    /// Serves one request with `reply` (`None` drops the connection) and returns the request.
    async fn serve(
        dir: &Dir,
        reply: Option<Value>,
    ) -> (HerdrClient, tokio::task::JoinHandle<Value>) {
        let listener = UnixListener::bind(dir.socket()).unwrap();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            if let Some(reply) = reply {
                let mut response = match reply.get("code") {
                    Some(_) => json!({"id": request["id"], "error": reply}),
                    None => json!({"id": request["id"], "result": reply}),
                }
                .to_string();
                response.push('\n');
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            request
        });
        let client = HerdrClient {
            socket_path: dir.socket(),
            timeout: Duration::from_secs(2),
        };
        (client, handle)
    }

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::new(id).unwrap()
    }

    fn pane(id: &str) -> PaneId {
        PaneId::new(id).unwrap()
    }

    #[tokio::test]
    async fn creates_workspace_and_decodes_ids() {
        let dir = Dir::new("create");
        let reply = json!({
            "type": "workspace_created",
            "workspace": {"workspace_id": "w3", "label": "x"},
            "tab": {"tab_id": "w3:t1", "workspace_id": "w3"},
            "root_pane": {"pane_id": "w3:p1", "workspace_id": "w3", "tab_id": "w3:t1"},
        });
        let (client, server) = serve(&dir, Some(reply)).await;
        let (workspace, root) = client
            .create_workspace_in(Path::new("/work/tree"))
            .await
            .unwrap();
        assert_eq!((workspace, root), (ws("w3"), pane("w3:p1")));
        let request = server.await.unwrap();
        assert_eq!(request["method"], "workspace.create");
        assert_eq!(
            request["params"],
            json!({"cwd": "/work/tree", "focus": false})
        );
    }

    #[tokio::test]
    async fn splits_pane_and_decodes_new_id() {
        let dir = Dir::new("split");
        let reply = json!({"type": "pane_info", "pane": {
            "pane_id": "w3:p2", "workspace_id": "w3", "tab_id": "w3:t1"}});
        let (client, server) = serve(&dir, Some(reply)).await;
        assert_eq!(
            client.split_pane(&pane("w3:p1")).await.unwrap(),
            pane("w3:p2")
        );
        let request = server.await.unwrap();
        assert_eq!(request["method"], "pane.split");
        assert_eq!(request["params"]["target_pane_id"], "w3:p1");
    }

    #[tokio::test]
    async fn closes_workspace_without_closing_the_group() {
        let dir = Dir::new("close");
        let (client, server) = serve(&dir, Some(json!({"type": "ok"}))).await;
        client.close_workspace(&ws("w3")).await.unwrap();
        let request = server.await.unwrap();
        assert_eq!(request["method"], "workspace.close");
        assert_eq!(request["params"], json!({"workspace_id": "w3"}));
    }

    #[tokio::test]
    async fn closing_a_missing_workspace_succeeds() {
        let dir = Dir::new("missing");
        let reply = json!({"code": "workspace_not_found", "message": "workspace not found: w9"});
        let (client, _server) = serve(&dir, Some(reply)).await;
        client.close_workspace(&ws("w9")).await.unwrap();
    }

    #[tokio::test]
    async fn other_close_errors_are_failed() {
        let dir = Dir::new("other");
        let reply = json!({"code": "workspace_group_close_required", "message": "group"});
        let (client, _server) = serve(&dir, Some(reply)).await;
        let error = client.close_workspace(&ws("w3")).await.unwrap_err();
        assert!(error.is_failed());
    }

    #[tokio::test]
    async fn relative_directory_is_failed_without_contacting_herdr() {
        let client = HerdrClient {
            socket_path: "/nonexistent/chimera-herdr.sock".into(),
            timeout: Duration::from_secs(1),
        };
        let error = client
            .create_workspace_in(Path::new("relative/dir"))
            .await
            .unwrap_err();
        assert!(matches!(error, HerdrError::InvalidInput(_)));
        assert!(error.is_failed());
    }

    #[test]
    fn empty_ids_are_rejected() {
        assert!(WorkspaceId::new("").is_err());
        assert!(PaneId::new(" ").is_err());
        assert!(empty_id("pane").is_uncertain());
    }

    #[tokio::test]
    async fn dropped_connection_is_uncertain_for_every_change() {
        let dir = Dir::new("dropped");
        let (client, _server) = serve(&dir, None).await;
        assert!(
            client
                .create_workspace_in(Path::new("/work"))
                .await
                .unwrap_err()
                .is_uncertain()
        );
        let dir = Dir::new("dropped2");
        let (client, _server) = serve(&dir, None).await;
        assert!(
            client
                .split_pane(&pane("w1:p1"))
                .await
                .unwrap_err()
                .is_uncertain()
        );
        let dir = Dir::new("dropped3");
        let (client, _server) = serve(&dir, None).await;
        assert!(
            client
                .close_workspace(&ws("w1"))
                .await
                .unwrap_err()
                .is_uncertain()
        );
    }

    #[tokio::test]
    async fn timeout_is_uncertain() {
        let dir = Dir::new("timeout");
        let listener = UnixListener::bind(dir.socket()).unwrap();
        let _held = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(stream);
        });
        let client = HerdrClient {
            socket_path: dir.socket(),
            timeout: Duration::from_millis(100),
        };
        let error = client.close_workspace(&ws("w1")).await.unwrap_err();
        assert!(matches!(error, HerdrError::Timeout { uncertain: true }));
    }
}
