use super::*;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};

fn pane() -> PaneId {
    PaneId::new("w1:p2").unwrap()
}

fn command(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| part.to_string()).collect()
}

struct Server {
    socket: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let dir = self.socket.parent().unwrap();
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_dir(dir);
    }
}

/// Serves each connection with `respond(request)`; `None` closes it without replying.
fn serve(respond: impl Fn(&Value) -> Option<Value> + Send + 'static) -> Server {
    serve_after(Duration::ZERO, respond)
}

/// As [`serve`], waiting `delay` before answering each non-ping request.
fn serve_after(
    delay: Duration,
    respond: impl Fn(&Value) -> Option<Value> + Send + 'static,
) -> Server {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "chimera-launch-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    let socket = dir.join("api.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            let reply = if request["method"] == "ping" {
                Some(json!({"type": "pong"}))
            } else {
                seen.lock().unwrap().push(request.clone());
                tokio::time::sleep(delay).await;
                respond(&request)
            };
            if let Some(result) = reply {
                let response = match result.get("error") {
                    Some(error) => json!({"id": request["id"], "error": error}),
                    None => json!({"id": request["id"], "result": result}),
                };
                let _ = stream
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await;
            }
        }
    });
    Server { socket, requests }
}

fn pane_info(agent: Option<&str>) -> Value {
    json!({"type": "pane_info", "pane": {
        "pane_id": "w1:p2", "workspace_id": "w1", "tab_id": "w1:t1", "focused": false,
        "terminal_id": "t", "agent_status": "unknown", "revision": 1, "agent": agent,
    }})
}

async fn client(server: &Server) -> HerdrClient {
    HerdrClient::connect_with_timeout(&server.socket, Duration::from_secs(2))
        .await
        .unwrap()
}

#[tokio::test]
async fn types_the_quoted_command_then_waits_for_an_agent() {
    let polls = Arc::new(Mutex::new(0));
    let counter = polls.clone();
    let server = serve(move |request| match request["method"].as_str().unwrap() {
        "pane.send_input" => Some(json!({"type": "ok"})),
        "pane.get" => {
            let mut polls = counter.lock().unwrap();
            *polls += 1;
            Some(pane_info((*polls >= 2).then_some("codex")))
        }
        other => panic!("unexpected {other}"),
    });
    let client = client(&server).await;
    let args = command(&[
        "codex",
        "--model",
        "gpt 6",
        "it's",
        "$HOME",
        "say \"hi\"",
        "",
    ]);
    client.launch_agent(&pane(), &args).await.unwrap();

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests[0]["method"], "pane.send_input");
    assert_eq!(
        requests[0]["params"],
        json!({
            "pane_id": "w1:p2",
            "text": r#"codex --model 'gpt 6' 'it'\''s' '$HOME' 'say "hi"' ''"#,
            "keys": ["enter"],
        })
    );
    assert_eq!(requests.len(), 3);
    assert!(
        requests[1..]
            .iter()
            .all(|r| r["method"] == "pane.get" && r["params"] == json!({"pane_id": "w1:p2"}))
    );
    assert!(!requests[0].to_string().contains("kind"));
}

#[test]
fn quotes_for_a_posix_shell() {
    assert_eq!(shell_quote("plain-arg_1.2/x=y"), "plain-arg_1.2/x=y");
    assert_eq!(shell_quote("a b"), "'a b'");
    assert_eq!(shell_quote("it's"), r"'it'\''s'");
    assert_eq!(shell_quote("$(rm -rf ~)"), "'$(rm -rf ~)'");
    assert_eq!(shell_quote("a\nb"), "'a\nb'");
    assert_eq!(shell_quote(""), "''");
}

#[tokio::test]
async fn rejects_invalid_input_as_failed_before_contacting_herdr() {
    let server = serve(|_| panic!("must not be contacted"));
    let client = client(&server).await;
    let cases = [
        (pane(), command(&[])),
        (pane(), command(&[""])),
        (pane(), command(&["codex", "bad\0arg"])),
        (pane(), command(&["co\0dex"])),
    ];
    for (pane, command) in cases {
        let error = client.launch_agent(&pane, &command).await.unwrap_err();
        assert!(matches!(error, HerdrError::InvalidInput(_)), "{error}");
        assert!(error.is_failed());
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn readiness_timeout_is_uncertain() {
    let server = serve(|request| match request["method"].as_str().unwrap() {
        "pane.send_input" => Some(json!({"type": "ok"})),
        _ => Some(pane_info(None)),
    });
    let client = client(&server).await;
    let error = client
        .launch_agent_within(&pane(), &command(&["codex"]), Duration::from_millis(600))
        .await
        .unwrap_err();
    assert!(
        matches!(error, HerdrError::Timeout { uncertain: true }),
        "{error}"
    );
    assert!(error.is_uncertain());
}

#[tokio::test]
async fn dropped_connection_after_the_send_is_uncertain() {
    let server = serve(|request| match request["method"].as_str().unwrap() {
        "pane.send_input" => Some(json!({"type": "ok"})),
        _ => None,
    });
    let client = client(&server).await;
    let error = client
        .launch_agent(&pane(), &command(&["codex"]))
        .await
        .unwrap_err();
    assert!(error.is_uncertain(), "{error}");
}

#[tokio::test]
async fn dropped_connection_on_the_send_itself_is_uncertain() {
    let server = serve(|_| None);
    let client = client(&server).await;
    let error = client
        .launch_agent(&pane(), &command(&["codex"]))
        .await
        .unwrap_err();
    assert!(error.is_uncertain(), "{error}");
}

#[tokio::test]
async fn server_rejecting_the_send_is_failed() {
    let server = serve(|_| Some(json!({"error": {"code": "pane_not_found", "message": "no"}})));
    let client = client(&server).await;
    let error = client
        .launch_agent(&pane(), &command(&["codex"]))
        .await
        .unwrap_err();
    assert!(matches!(error, HerdrError::Server { .. }));
    assert!(error.is_failed());
}

#[tokio::test]
async fn slow_responses_are_not_cut_short_by_a_shorter_client_timeout() {
    let polls = Arc::new(Mutex::new(0));
    let counter = polls.clone();
    let server = serve_after(
        Duration::from_millis(400),
        move |request| match request["method"].as_str().unwrap() {
            "pane.send_input" => Some(json!({"type": "ok"})),
            _ => {
                let mut polls = counter.lock().unwrap();
                *polls += 1;
                Some(pane_info((*polls >= 2).then_some("codex")))
            }
        },
    );
    let client = HerdrClient::connect_with_timeout(&server.socket, Duration::from_millis(200))
        .await
        .unwrap();
    client
        .launch_agent_within(&pane(), &command(&["codex"]), Duration::from_secs(5))
        .await
        .unwrap();
}
