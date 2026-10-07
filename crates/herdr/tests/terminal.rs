//! Integration tests of `HerdrTerminal` through `dyn Terminal` against a running Herdr. They skip
//! (and pass) when `HERDR_SOCKET_PATH` is unset or unreachable.
#![cfg(unix)]

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chimera_core::{
    PaneId,
    terminal::{Terminal, TurnStatus},
};
use chimera_herdr::HerdrTerminal;

/// A simple command-line agent. Herdr recognises it as `codex` by its process name, and it
/// reports its own lifecycle: idle while waiting, working while it answers each prompt line.
const AGENT_STUB: &str = r#"#!/bin/bash
report() { herdr pane report-agent "$HERDR_PANE_ID" --source chimera-it --agent codex --state "$1" >/dev/null 2>&1; }
report idle
while IFS= read -r line; do
  report working
  sleep 1
  echo "ANSWER: $line"
  report idle
done
"#;

/// As [`AGENT_STUB`], but it answers at once, so many prompts can be sent in a test.
const FAST_AGENT_STUB: &str = r#"#!/bin/bash
report() { herdr pane report-agent "$HERDR_PANE_ID" --source chimera-it --agent codex --state "$1" >/dev/null 2>&1; }
report idle
while IFS= read -r line; do
  report working
  echo "ANSWER: $line"
  report idle
done
"#;

async fn live() -> Option<(PathBuf, Arc<dyn Terminal>)> {
    let Some(path) = std::env::var_os("HERDR_SOCKET_PATH") else {
        eprintln!("skipping: HERDR_SOCKET_PATH is not set");
        return None;
    };
    let path = PathBuf::from(path);
    match HerdrTerminal::connect(&path).await {
        Ok(terminal) => Some((path, Arc::new(terminal))),
        Err(error) => {
            eprintln!("skipping: Herdr at {path:?} is unreachable: {error}");
            None
        }
    }
}

async fn wait_for_status(terminal: &dyn Terminal, pane: &PaneId, want: TurnStatus) {
    for _ in 0..120 {
        if terminal.read_status(pane).await.unwrap() == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("pane never reached {want:?}");
}

/// Waits until a prompt has moved the agent to running and then back to finished.
async fn wait_for_turn(terminal: &dyn Terminal, pane: &PaneId) {
    // `send_prompt` returns once the agent is working, so the turn is already running.
    assert_eq!(
        terminal.read_status(pane).await.unwrap(),
        TurnStatus::Running
    );
    wait_for_status(terminal, pane, TurnStatus::Finished).await;
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("chimera-herdr-it-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir.canonicalize().unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn full_lifecycle_through_dyn_terminal() {
    let Some((socket, terminal)) = live().await else {
        return;
    };
    let dir = TempDir::new("lifecycle");
    let agent = dir.path().join("codex");
    std::fs::write(&agent, AGENT_STUB).unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();

    let (workspace, root) = terminal.create_workspace(dir.path()).await.unwrap();
    // Run the steps in a task so the workspace is closed even if an assertion fails.
    let steps = {
        let terminal = terminal.clone();
        let (workspace, root, dir) = (workspace.clone(), root.clone(), dir.path().to_owned());
        tokio::spawn(async move {
            let (found, panes) = terminal.find_workspace(&dir).await.unwrap().unwrap();
            assert_eq!(found, workspace);
            assert_eq!(panes, vec![root.clone()]);

            let second = terminal.split_pane(&workspace, &root).await.unwrap();
            let (_, panes) = terminal.find_workspace(&dir).await.unwrap().unwrap();
            assert_eq!(panes, vec![root.clone(), second.clone()]);

            terminal
                .launch_agent(&root, &format!("'{}'", agent.display()))
                .await
                .unwrap();
            assert_eq!(terminal.prompts_received(&root).await.unwrap(), 0);

            terminal.send_prompt(&root, "hello").await.unwrap();
            assert_eq!(terminal.prompts_received(&root).await.unwrap(), 1);
            wait_for_turn(&*terminal, &root).await;
            let output = terminal.read_output(&root).await.unwrap();
            assert!(output.contains("ANSWER: hello"), "{output}");

            // A fresh instance built from the socket path alone picks the work up again.
            let again: Arc<dyn Terminal> = Arc::new(HerdrTerminal::connect(&socket).await.unwrap());
            assert_eq!(
                again.read_status(&root).await.unwrap(),
                TurnStatus::Finished
            );
            assert_eq!(again.prompts_received(&root).await.unwrap(), 1);
            assert!(
                again
                    .read_output(&root)
                    .await
                    .unwrap()
                    .contains("ANSWER: hello")
            );
            again.send_prompt(&root, "again").await.unwrap();
            assert_eq!(again.prompts_received(&root).await.unwrap(), 2);
            wait_for_turn(&*again, &root).await;
            assert!(
                again
                    .read_output(&root)
                    .await
                    .unwrap()
                    .contains("ANSWER: again")
            );
            again
        })
    }
    .await;
    let closed = terminal.close_workspace(&workspace).await;
    let again = steps.unwrap();
    closed.unwrap();

    // Closing is idempotent, and everything is gone afterwards.
    again.close_workspace(&workspace).await.unwrap();
    assert_eq!(again.read_status(&root).await.unwrap(), TurnStatus::Gone);
    assert!(again.find_workspace(dir.path()).await.unwrap().is_none());
}

#[tokio::test]
async fn a_pane_counts_more_prompts_than_it_has_metadata_tokens_across_restarts() {
    let Some((socket, terminal)) = live().await else {
        return;
    };
    let dir = TempDir::new("many");
    let agent = dir.path().join("codex");
    std::fs::write(&agent, FAST_AGENT_STUB).unwrap();
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();

    let (workspace, root) = terminal.create_workspace(dir.path()).await.unwrap();
    let steps = {
        let terminal = terminal.clone();
        tokio::spawn(async move {
            terminal
                .launch_agent(&root, &format!("'{}'", agent.display()))
                .await
                .unwrap();
            // Herdr keeps 32 tokens per pane; every send comes from a fresh instance.
            for n in 1..=40 {
                let fresh = HerdrTerminal::connect(&socket).await.unwrap();
                fresh.send_prompt(&root, &format!("p{n}")).await.unwrap();
                wait_for_status(&fresh, &root, TurnStatus::Finished).await;
                let again = HerdrTerminal::connect(&socket).await.unwrap();
                assert_eq!(again.prompts_received(&root).await.unwrap(), n);
            }
        })
    }
    .await;
    let closed = terminal.close_workspace(&workspace).await;
    steps.unwrap();
    closed.unwrap();
}

#[tokio::test]
async fn missing_socket_is_a_failed_connect() {
    let error = HerdrTerminal::connect("/nonexistent/chimera-herdr.sock")
        .await
        .unwrap_err();
    assert!(error.is_failed());
}
