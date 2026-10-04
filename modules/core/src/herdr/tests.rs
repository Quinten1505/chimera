use super::*;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::{UnixListener, UnixStream},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

fn response_error(response: &[u8]) -> HerdrError {
    let (mut client, mut server) = UnixStream::pair().unwrap();
    server.write_all(response).unwrap();
    server.shutdown(std::net::Shutdown::Write).unwrap();
    exchange::<Value>(&mut client, "ping", &json!({}), "pong").unwrap_err()
}

#[test]
fn rejects_invalid_responses() {
    for response in [
        b"{\"id\":\"wrong\",\"result\":{\"type\":\"pong\"}}\n".as_slice(),
        b"{\"id\":\"chimera\",\"result\":{\"type\":\"other\"}}\n",
        b"{\"id\":\"chimera\"}\n",
        b"{\"id\":\"chimera\",\"result\":{\"type\":\"pong\"}}",
        b"",
    ] {
        assert!(matches!(response_error(response), HerdrError::Protocol(_)));
    }
    assert!(matches!(response_error(b"invalid\n"), HerdrError::Json(_)));
}

#[test]
fn preserves_server_error_details() {
    let error = response_error(
        b"{\"id\":\"chimera\",\"error\":{\"code\":\"not_found\",\"message\":\"pane missing\"}}\n",
    );
    assert!(matches!(error, HerdrError::Server { code, message }
        if code == "not_found" && message == "pane missing"));
}

#[test]
fn times_out_when_server_does_not_reply() {
    let (mut client, _server) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let error = exchange::<Value>(&mut client, "ping", &json!({}), "pong").unwrap_err();
    assert!(matches!(error, HerdrError::Io(error)
        if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)));
}

#[test]
fn validates_inputs_before_contacting_server() {
    let client = HerdrClient {
        socket_path: "/nonexistent/chimera-herdr.sock".into(),
        timeout: Duration::from_secs(1),
    };
    let mut worktree = WorktreeOptions::new(
        WorktreeSource::Directory {
            cwd: "relative".into(),
        },
        "feature/test",
    );
    assert!(matches!(
        client.create_worktree(&worktree),
        Err(HerdrError::InvalidInput(_))
    ));
    worktree.source = WorktreeSource::Workspace {
        workspace_id: "w1".into(),
    };
    worktree.branch.clear();
    assert!(matches!(
        client.create_worktree(&worktree),
        Err(HerdrError::InvalidInput(_))
    ));
    worktree.branch = "feature/test".into();
    worktree.path = Some("relative".into());
    assert!(matches!(
        client.create_worktree(&worktree),
        Err(HerdrError::InvalidInput(_))
    ));
    let mut pane = PaneOptions::new("", SplitDirection::Down);
    assert!(matches!(
        client.add_pane(&pane),
        Err(HerdrError::InvalidInput(_))
    ));
    pane.target_pane_id = "w1:p1".into();
    pane.cwd = Some("relative".into());
    assert!(matches!(
        client.add_pane(&pane),
        Err(HerdrError::InvalidInput(_))
    ));
    assert!(matches!(
        HerdrClient::connect_with_timeout("/unused", Duration::ZERO),
        Err(HerdrError::InvalidInput(_))
    ));
}

struct SocketDirectory(PathBuf);

impl Drop for SocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("api.sock"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[test]
fn connects_creates_worktree_and_adds_pane_over_socket() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = SocketDirectory(std::env::temp_dir().join(format!(
        "chimera-herdr-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    )));
    std::fs::create_dir(&dir.0).unwrap();
    let path = dir.0.join("api.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = thread::spawn(move || {
        let requests = [
            (
                json!({"method":"ping","params":{}}),
                json!({"type":"pong","version":"0.9.3","protocol":22}),
            ),
            (
                json!({"method":"worktree.create","params":{
                    "cwd":"/repo with spaces","branch":"feature/test","base":"main",
                    "path":"/worktrees/test","label":"test","focus":false
                }}),
                json!({
                    "type":"worktree_created",
                    "workspace":{"workspace_id":"w2","label":"test","extra":true,
                        "worktree":{"checkout_path":"/worktrees/test","is_linked_worktree":true}},
                    "tab":{"tab_id":"w2:t1","workspace_id":"w2"},
                    "root_pane":{"pane_id":"w2:p1","workspace_id":"w2","tab_id":"w2:t1"},
                    "worktree":{"path":"/worktrees/test","branch":"feature/test"}
                }),
            ),
            (
                json!({"method":"pane.split","params":{
                    "target_pane_id":"w2:p1","direction":"right","cwd":"/worktrees/test","focus":true
                }}),
                json!({"type":"pane_info","pane":{
                    "pane_id":"w2:p2","workspace_id":"w2","tab_id":"w2:t1"
                }}),
            ),
        ];
        for (expected, result) in requests {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream).read_line(&mut line).unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], expected["method"]);
            assert_eq!(request["params"], expected["params"]);
            let response = json!({"id":request["id"],"result":result}).to_string();
            stream.write_all(&response.as_bytes()[..8]).unwrap();
            stream.write_all(&response.as_bytes()[8..]).unwrap();
            stream.write_all(b"\n").unwrap();
        }
    });
    let client = HerdrClient::connect_with_timeout(&path, Duration::from_secs(2)).unwrap();
    let mut options = WorktreeOptions::new(
        WorktreeSource::Directory {
            cwd: "/repo with spaces".into(),
        },
        "feature/test",
    );
    options.base = Some("main".into());
    options.path = Some("/worktrees/test".into());
    options.label = Some("test".into());
    let created = client.create_worktree(&options).unwrap();
    assert_eq!(created.workspace.workspace_id, "w2");
    assert_eq!(
        created.workspace.checkout_path.as_deref(),
        Some(Path::new("/worktrees/test"))
    );
    assert_eq!(created.worktree.path, Path::new("/worktrees/test"));
    let mut options = PaneOptions::new(created.root_pane.pane_id, SplitDirection::Right);
    options.cwd = Some(created.worktree.path);
    options.focus = true;
    let pane = client.add_pane(&options).unwrap();
    assert_eq!(pane.pane_id, "w2:p2");
    server.join().unwrap();
}

#[test]
fn serializes_workspace_source_and_down_split() {
    let worktree = WorktreeOptions::new(
        WorktreeSource::Workspace {
            workspace_id: "w1".into(),
        },
        "feature/test",
    );
    assert_eq!(
        serde_json::to_value(worktree).unwrap(),
        json!({
            "workspace_id":"w1","branch":"feature/test","focus":false
        })
    );
    assert_eq!(
        serde_json::to_value(PaneOptions::new("w1:p1", SplitDirection::Down)).unwrap(),
        json!({"target_pane_id":"w1:p1","direction":"down","focus":false})
    );
}

#[test]
#[ignore = "requires a running Herdr server and HERDR_SOCKET_PATH"]
fn live_connection() {
    HerdrClient::connect(std::env::var_os("HERDR_SOCKET_PATH").unwrap()).unwrap();
}

#[test]
fn reads_main_checkout_and_missing_workspace_metadata() {
    let workspace: Workspace = serde_json::from_value(json!({
        "workspace_id": "w1",
        "label": "repo",
        "worktree": {
            "checkout_path": "/repo",
            "is_linked_worktree": false,
            "repo_root": "/repo"
        }
    }))
    .unwrap();
    assert_eq!(workspace.checkout_path.as_deref(), Some(Path::new("/repo")));

    for data in [
        json!({"workspace_id": "w4", "label": "repo"}),
        json!({"workspace_id": "w4", "label": "repo", "worktree": null}),
    ] {
        let workspace: Workspace = serde_json::from_value(data).unwrap();
        assert_eq!(workspace.checkout_path, None);
    }
}

fn run_session_start(
    target: &crate::SessionTarget,
    expected: Value,
    response: Value,
) -> Result<crate::Session, HerdrError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = SocketDirectory(std::env::temp_dir().join(format!(
        "chimera-session-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    )));
    std::fs::create_dir(&dir.0).unwrap();
    let path = dir.0.join("api.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line).unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["method"], expected["method"]);
        assert_eq!(request["params"], expected["params"]);
        writeln!(stream, "{response}").unwrap();
    });
    let client = HerdrClient {
        socket_path: path,
        timeout: Duration::from_secs(2),
    };
    let result = client.start_session("session-123", target);
    server.join().unwrap();
    result
}

#[test]
fn starts_session_in_existing_directory_and_saves_returned_ids() {
    let target = crate::SessionTarget::Workspace(WorkspaceOptions {
        cwd: "/repo".into(),
        label: Some("my session".into()),
        focus: true,
    });
    let session = run_session_start(&target,
        json!({"method":"workspace.create","params":{"cwd":"/repo","label":"my session","focus":true}}),
        json!({"id":"chimera","result":{
            "type":"workspace_created",
            "workspace":{"workspace_id":"w4","label":"my session"},
            "tab":{"tab_id":"w4:t1","workspace_id":"w4"},
            "root_pane":{"pane_id":"w4:p1","workspace_id":"w4","tab_id":"w4:t1"}
        }}),
    ).unwrap();
    assert_eq!(
        session,
        crate::Session {
            session_id: "session-123".into(),
            workspaces: vec![crate::WorkspacePanes {
                workspace_id: "w4".into(),
                pane_ids: vec!["w4:p1".into()],
                checkout_path: None,
            }],
        }
    );
    let saved = serde_json::to_string(&session).unwrap();
    assert_eq!(
        serde_json::from_str::<crate::Session>(&saved).unwrap(),
        session
    );
}

#[test]
fn starts_session_with_new_checkout_and_saves_actual_path() {
    let target = crate::SessionTarget::Worktree(WorktreeOptions::new(
        WorktreeSource::Directory {
            cwd: "/repo".into(),
        },
        "feature/session",
    ));
    let session = run_session_start(&target,
        json!({"method":"worktree.create","params":{"cwd":"/repo","branch":"feature/session","focus":false}}),
        json!({"id":"chimera","result":{
            "type":"worktree_created",
            "workspace":{"workspace_id":"w3","label":"feature/session"},
            "tab":{"tab_id":"w3:t1","workspace_id":"w3"},
            "root_pane":{"pane_id":"w3:p1","workspace_id":"w3","tab_id":"w3:t1"},
            "worktree":{"path":"/herdr/worktrees/feature-session","branch":"feature/session"}
        }}),
    ).unwrap();
    assert_eq!(
        session,
        crate::Session {
            session_id: "session-123".into(),
            workspaces: vec![crate::WorkspacePanes {
                workspace_id: "w3".into(),
                pane_ids: vec!["w3:p1".into()],
                checkout_path: Some("/herdr/worktrees/feature-session".into()),
            }],
        }
    );
}

#[test]
fn failed_session_start_returns_server_error() {
    let target = crate::SessionTarget::Workspace(WorkspaceOptions::new("/missing"));
    let error = run_session_start(
        &target,
        json!({"method":"workspace.create","params":{"cwd":"/missing","focus":false}}),
        json!({"id":"chimera","error":{"code":"invalid_params","message":"directory missing"}}),
    )
    .unwrap_err();
    assert!(matches!(error, HerdrError::Server { code, message }
        if code == "invalid_params" && message == "directory missing"));
}

#[test]
fn session_start_rejects_invalid_input_before_creating_resources() {
    let client = HerdrClient {
        socket_path: "/nonexistent/chimera.sock".into(),
        timeout: Duration::from_secs(1),
    };
    let target = crate::SessionTarget::Workspace(WorkspaceOptions::new("/repo"));
    assert!(matches!(
        client.start_session("  ", &target),
        Err(HerdrError::InvalidInput(_))
    ));
    let target = crate::SessionTarget::Workspace(WorkspaceOptions::new("relative"));
    assert!(matches!(
        client.start_session("session-123", &target),
        Err(HerdrError::InvalidInput(_))
    ));
}
