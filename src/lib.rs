//! Composition helpers connecting configuration to the core modules.

use chimera_configuration::CodexConfiguration;
use chimera_core::{HerdrClient, Session};
use std::path::Path;

/// Load and validate YAML before starting one builder-profile Codex agent in each tracked pane.
///
/// Records successful launches in the session's workspaces and stops on the first
/// failure. Does not create worktrees or panes, persist state, or retry requests.
pub fn start_session_agents(
    client: &HerdrClient,
    session: &mut Session,
    config_path: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let args = CodexConfiguration::load(config_path)?
        .profile("builder")?
        .launch_args()?;
    for workspace in &mut session.workspaces {
        client.start_codex_agents(workspace, &args)?;
    }
    Ok(())
}
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use chimera_core::{AgentSessionReference, WorkspacePanes};
    use serde_json::{Value, json};
    use std::{
        io::{BufRead, BufReader, Write},
        os::unix::net::UnixListener,
        sync::atomic::{AtomicU64, Ordering},
        thread,
        time::Duration,
    };

    fn run(fail_at: Option<usize>) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "chimera-agents-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let socket = directory.join("api.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let panes = ["w1:p1", "w1:p2", "w2:p1", "w2:p2"];
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 0..=fail_at.map_or(4, |index| index + 1) {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut line = String::new();
                BufReader::new(&mut stream).read_line(&mut line).unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                let response = if index == 0 {
                    json!({"id": "chimera", "result": {"type": "pong"}})
                } else if fail_at == Some(index - 1) {
                    json!({"id":"chimera","error":{"code":"launch_failed","message":"not at shell prompt"}})
                } else {
                    json!({"id":"chimera","result":{
                        "type":"agent_started","argv":["codex"],
                        "agent":{
                            "name":format!("codex-{}", panes[index - 1]),
                            "pane_id":panes[index - 1], "agent":"codex",
                            "agent_session":{"source":"integration","agent":"codex","kind":"id","value":format!("thread-{index}")}
                        }
                    }})
                };
                writeln!(stream, "{response}").unwrap();
                requests.push(request);
            }
            requests
        });
        let client = HerdrClient::connect(&socket).unwrap();
        let mut session = Session {
            session_id: "test".into(),
            workspaces: (1..=2)
                .map(|number| WorkspacePanes {
                    workspace_id: format!("w{number}"),
                    pane_ids: vec![format!("w{number}:p1"), format!("w{number}:p2")],
                    checkout_path: Some(format!("/repo-{number}").into()),
                    agents: vec![],
                })
                .collect(),
        };
        let yaml = concat!(env!("CARGO_MANIFEST_DIR"), "/codex.yaml");
        let result = start_session_agents(&client, &mut session, yaml);
        assert_eq!(result.is_err(), fail_at.is_some());
        let requests = server.join().unwrap();
        std::fs::remove_file(&socket).unwrap();
        std::fs::remove_dir(&directory).unwrap();
        assert_eq!(requests[0]["method"], "ping");
        for (index, request) in requests[1..].iter().enumerate() {
            assert_eq!(request["method"], "agent.start");
            assert_eq!(
                request["params"],
                json!({
                    "name":format!("codex-{}", panes[index]),
                    "kind":"codex","pane_id":panes[index],"timeout_ms":30000,
                    "args":["--model","gpt-6-luna","--config","model_reasoning_effort=\"medium\"",
                        "--config","service_tier=\"fast\"","--approve-for-me"]
                })
            );
        }
        let agents: Vec<_> = session
            .workspaces
            .iter()
            .flat_map(|workspace| &workspace.agents)
            .collect();
        assert_eq!(agents.len(), fail_at.unwrap_or(4));
        for (index, agent) in agents.iter().enumerate() {
            assert_eq!(agent.pane_id, panes[index]);
            assert_eq!(
                agent.session,
                Some(AgentSessionReference::Id(format!("thread-{}", index + 1)))
            );
        }
        let saved = serde_json::to_string(&session).unwrap();
        assert_eq!(serde_json::from_str::<Session>(&saved).unwrap(), session);
        if fail_at.is_none() {
            // The socket has gone away: a second call must skip every tracked pane.
            start_session_agents(&client, &mut session, yaml).unwrap();
        }
    }

    #[test]
    fn starts_four_luna_agents_from_yaml_and_records_them_by_workspace() {
        run(None);
    }

    #[test]
    fn keeps_successful_agents_and_stops_on_first_launch_failure() {
        run(Some(2));
    }
}
