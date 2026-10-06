use std::path::Path;

use async_trait::async_trait;

use crate::error::PortError;
use crate::{PaneId, WorkspaceId};

/// Lifecycle of the agent running in a pane, as far as a turn is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStatus {
    Running,
    Finished,
    Gone,
}

/// Terminal multiplexer hosting agents. Deals only in plain text: it knows nothing about roles,
/// prompts, or results.
#[async_trait]
pub trait Terminal: Send + Sync {
    /// Creates a workspace rooted at `directory`, returning it with its first pane.
    async fn create_workspace(&self, directory: &Path) -> Result<(WorkspaceId, PaneId), PortError>;

    /// Splits `pane` within `workspace`, returning the new pane.
    async fn split_pane(&self, workspace: &WorkspaceId, pane: &PaneId)
    -> Result<PaneId, PortError>;

    /// Starts an agent in `pane` from a ready command line.
    async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError>;

    async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError>;

    async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError>;

    async fn read_output(&self, pane: &PaneId) -> Result<String, PortError>;

    async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError>;
}

#[cfg(any(test, feature = "testing"))]
pub use fake::FakeTerminal;

#[cfg(any(test, feature = "testing"))]
mod fake {
    use std::collections::{HashMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::{Terminal, TurnStatus};
    use crate::error::PortError;
    use crate::{PaneId, WorkspaceId};

    #[derive(Default)]
    struct PaneState {
        workspace: Option<WorkspaceId>,
        statuses: VecDeque<TurnStatus>,
        output: String,
        launched: Option<String>,
        prompts: Vec<String>,
    }

    #[derive(Default)]
    struct State {
        next_id: u64,
        workspaces: HashMap<WorkspaceId, PathBuf>,
        panes: HashMap<PaneId, PaneState>,
    }

    /// In-memory [`Terminal`] whose per-pane status sequences and output are scripted by tests.
    #[derive(Default)]
    pub struct FakeTerminal {
        state: Mutex<State>,
    }

    impl FakeTerminal {
        pub fn new() -> Self {
            Self::default()
        }

        /// Queues statuses for `pane`. Each read consumes one; the last is repeated forever.
        /// A pane with no script reports `Running`.
        pub fn script_statuses(
            &self,
            pane: &PaneId,
            statuses: impl IntoIterator<Item = TurnStatus>,
        ) {
            self.with_pane(pane, |state| state.statuses.extend(statuses))
                .expect("unknown pane");
        }

        pub fn script_output(&self, pane: &PaneId, output: impl Into<String>) {
            self.with_pane(pane, |state| state.output = output.into())
                .expect("unknown pane");
        }

        pub fn launched_command(&self, pane: &PaneId) -> Option<String> {
            self.with_pane(pane, |state| state.launched.clone())
                .expect("unknown pane")
        }

        pub fn prompts(&self, pane: &PaneId) -> Vec<String> {
            self.with_pane(pane, |state| state.prompts.clone())
                .expect("unknown pane")
        }

        pub fn workspace_directory(&self, workspace: &WorkspaceId) -> Option<PathBuf> {
            self.state
                .lock()
                .unwrap()
                .workspaces
                .get(workspace)
                .cloned()
        }

        fn with_pane<T>(
            &self,
            pane: &PaneId,
            f: impl FnOnce(&mut PaneState) -> T,
        ) -> Result<T, PortError> {
            let mut state = self.state.lock().unwrap();
            state
                .panes
                .get_mut(pane)
                .map(f)
                .ok_or_else(|| PortError::failed(format!("unknown pane {pane}")))
        }
    }

    impl State {
        fn new_pane(&mut self, workspace: &WorkspaceId) -> PaneId {
            self.next_id += 1;
            let pane = PaneId::new(format!("pane-{}", self.next_id)).unwrap();
            self.panes.insert(
                pane.clone(),
                PaneState {
                    workspace: Some(workspace.clone()),
                    ..PaneState::default()
                },
            );
            pane
        }
    }

    #[async_trait]
    impl Terminal for FakeTerminal {
        async fn create_workspace(
            &self,
            directory: &Path,
        ) -> Result<(WorkspaceId, PaneId), PortError> {
            let mut state = self.state.lock().unwrap();
            state.next_id += 1;
            let workspace = WorkspaceId::new(format!("workspace-{}", state.next_id)).unwrap();
            state
                .workspaces
                .insert(workspace.clone(), directory.to_path_buf());
            let pane = state.new_pane(&workspace);
            Ok((workspace, pane))
        }

        async fn split_pane(
            &self,
            workspace: &WorkspaceId,
            pane: &PaneId,
        ) -> Result<PaneId, PortError> {
            let mut state = self.state.lock().unwrap();
            if state.panes.get(pane).and_then(|p| p.workspace.as_ref()) != Some(workspace) {
                return Err(PortError::failed(format!(
                    "pane {pane} is not in workspace {workspace}"
                )));
            }
            Ok(state.new_pane(workspace))
        }

        async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
            self.with_pane(pane, |state| {
                state.launched = Some(command_line.to_string())
            })
        }

        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            self.with_pane(pane, |state| state.prompts.push(prompt.to_string()))
        }

        async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
            self.with_pane(pane, |state| {
                if state.statuses.len() > 1 {
                    state.statuses.pop_front().unwrap()
                } else {
                    state
                        .statuses
                        .front()
                        .copied()
                        .unwrap_or(TurnStatus::Running)
                }
            })
        }

        async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
            self.with_pane(pane, |state| state.output.clone())
        }

        async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            if state.workspaces.remove(workspace).is_none() {
                return Err(PortError::failed(format!("unknown workspace {workspace}")));
            }
            state
                .panes
                .retain(|_, p| p.workspace.as_ref() != Some(workspace));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    async fn started() -> (Arc<FakeTerminal>, WorkspaceId, PaneId) {
        let terminal = Arc::new(FakeTerminal::new());
        let dyn_terminal: Arc<dyn Terminal> = terminal.clone();
        let (workspace, pane) = dyn_terminal
            .create_workspace(Path::new("/work"))
            .await
            .unwrap();
        (terminal, workspace, pane)
    }

    #[tokio::test]
    async fn launch_records_command_line_and_directory() {
        let (terminal, workspace, pane) = started().await;
        terminal
            .launch_agent(&pane, "agent --model m")
            .await
            .unwrap();
        assert_eq!(
            terminal.launched_command(&pane).as_deref(),
            Some("agent --model m")
        );
        assert_eq!(
            terminal.workspace_directory(&workspace),
            Some(Path::new("/work").to_path_buf())
        );
        let split = terminal.split_pane(&workspace, &pane).await.unwrap();
        assert_ne!(split, pane);
    }

    #[tokio::test]
    async fn prompt_is_delivered() {
        let (terminal, _, pane) = started().await;
        terminal.send_prompt(&pane, "do it").await.unwrap();
        assert_eq!(terminal.prompts(&pane), vec!["do it".to_string()]);
    }

    #[tokio::test]
    async fn status_progresses_to_finished_and_stays() {
        let (terminal, _, pane) = started().await;
        assert_eq!(
            terminal.read_status(&pane).await.unwrap(),
            TurnStatus::Running
        );
        terminal.script_statuses(
            &pane,
            [
                TurnStatus::Running,
                TurnStatus::Running,
                TurnStatus::Finished,
            ],
        );
        for expected in [
            TurnStatus::Running,
            TurnStatus::Running,
            TurnStatus::Finished,
            TurnStatus::Finished,
        ] {
            assert_eq!(terminal.read_status(&pane).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn output_is_read_per_pane() {
        let (terminal, workspace, pane) = started().await;
        let other = terminal.split_pane(&workspace, &pane).await.unwrap();
        terminal.script_output(&pane, "result text");
        assert_eq!(terminal.read_output(&pane).await.unwrap(), "result text");
        assert_eq!(terminal.read_output(&other).await.unwrap(), "");
    }

    #[tokio::test]
    async fn gone_agent_is_reported() {
        let (terminal, workspace, pane) = started().await;
        terminal.script_statuses(&pane, [TurnStatus::Running, TurnStatus::Gone]);
        assert_eq!(
            terminal.read_status(&pane).await.unwrap(),
            TurnStatus::Running
        );
        assert_eq!(terminal.read_status(&pane).await.unwrap(), TurnStatus::Gone);

        terminal.close_workspace(&workspace).await.unwrap();
        assert!(terminal.read_status(&pane).await.unwrap_err().is_failed());
    }
}
