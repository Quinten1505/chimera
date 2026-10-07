use super::*;
use chimera_core::error::PortError;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex},
    net::{UnixListener, UnixStream},
};

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

/// Runs one exchange against an in-memory server that reads the request line, replies with
/// `response` and closes. Returns the outcome and the request the server saw.
async fn run_exchange(
    response: &[u8],
    effect: Effect,
    deadline: Instant,
) -> (Result<Value, HerdrError>, Value) {
    let (mut client, server) = duplex(64 * 1024);
    let response = response.to_vec();
    let server = tokio::spawn(async move {
        let mut server = BufReader::new(server);
        let mut line = String::new();
        server.read_line(&mut line).await.unwrap();
        server.get_mut().write_all(&response).await.unwrap();
        // Dropping the stream closes it, so a truncated response ends in EOF.
        serde_json::from_str::<Value>(&line).unwrap()
    });
    let result = exchange::<Value>(
        &mut client,
        "ping",
        &json!({"a": 1}),
        "pong",
        effect,
        deadline,
    )
    .await;
    (result, server.await.unwrap())
}

async fn response_error(response: &[u8], effect: Effect) -> HerdrError {
    run_exchange(response, effect, soon()).await.0.unwrap_err()
}

#[tokio::test]
async fn encodes_request_and_decodes_matching_response() {
    let (result, request) = run_exchange(
        b"{\"id\":\"chimera\",\"result\":{\"type\":\"pong\",\"version\":\"1\"}}\n",
        Effect::Read,
        soon(),
    )
    .await;
    assert_eq!(
        request,
        json!({"id":"chimera","method":"ping","params":{"a":1}})
    );
    assert_eq!(result.unwrap()["version"], "1");
}

const MISMATCHED_ID: &[u8] = b"{\"id\":\"wrong\",\"result\":{\"type\":\"pong\"}}\n";
const WRONG_TYPE: &[u8] = b"{\"id\":\"chimera\",\"result\":{\"type\":\"other\"}}\n";
const NO_RESULT: &[u8] = b"{\"id\":\"chimera\"}\n";
const TRUNCATED: &[u8] = b"{\"id\":\"chimera\",\"result\":{\"type\":\"pong\"}}";

#[tokio::test]
async fn rejects_invalid_responses_as_protocol_errors() {
    for response in [MISMATCHED_ID, WRONG_TYPE, NO_RESULT, TRUNCATED, b""] {
        assert!(
            matches!(
                response_error(response, Effect::Change).await,
                HerdrError::Protocol { .. }
            ),
            "{}",
            String::from_utf8_lossy(response)
        );
    }
    assert!(matches!(
        response_error(b"invalid\n", Effect::Change).await,
        HerdrError::Json { .. }
    ));
}

#[tokio::test]
async fn rejects_oversized_responses() {
    let mut with_newline = vec![b'x'; MAX_RESPONSE as usize + 10];
    with_newline.push(b'\n');
    let without_newline = vec![b'x'; MAX_RESPONSE as usize + 10];
    for response in [with_newline, without_newline] {
        assert!(matches!(
            response_error(&response, Effect::Change).await,
            HerdrError::Protocol { .. }
        ));
    }
}

#[tokio::test]
async fn preserves_server_error_details() {
    let error = response_error(
        b"{\"id\":\"chimera\",\"error\":{\"code\":\"not_found\",\"message\":\"pane missing\"}}\n",
        Effect::Change,
    )
    .await;
    assert!(matches!(&error, HerdrError::Server { code, message }
        if code == "not_found" && message == "pane missing"));
    assert!(error.to_string().contains("not_found"));
    assert!(error.to_string().contains("pane missing"));
}

#[tokio::test]
async fn times_out_when_server_does_not_reply() {
    let (mut client, _server): (DuplexStream, _) = duplex(1024);
    let deadline = Instant::now() + Duration::from_millis(30);
    let error = exchange::<Value>(
        &mut client,
        "ping",
        &json!({}),
        "pong",
        Effect::Change,
        deadline,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, HerdrError::Timeout { uncertain: true }));
}

#[tokio::test]
async fn read_operations_are_never_uncertain() {
    for response in [
        MISMATCHED_ID,
        WRONG_TYPE,
        NO_RESULT,
        TRUNCATED,
        b"",
        b"invalid\n",
    ] {
        assert!(response_error(response, Effect::Read).await.is_failed());
    }
    let (mut client, _server) = duplex(1024);
    let deadline = Instant::now() + Duration::from_millis(30);
    let error = exchange::<Value>(
        &mut client,
        "ping",
        &json!({}),
        "pong",
        Effect::Read,
        deadline,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, HerdrError::Timeout { uncertain: false }));
}

#[tokio::test]
async fn changing_operations_are_uncertain_after_the_request_was_written() {
    for response in [
        MISMATCHED_ID,
        WRONG_TYPE,
        NO_RESULT,
        TRUNCATED,
        b"",
        b"invalid\n",
    ] {
        assert!(
            response_error(response, Effect::Change)
                .await
                .is_uncertain()
        );
    }
}

#[tokio::test]
async fn server_error_response_is_failed_even_for_changes() {
    let error = response_error(
        b"{\"id\":\"chimera\",\"error\":{\"code\":\"x\",\"message\":\"y\"}}\n",
        Effect::Change,
    )
    .await;
    assert!(error.is_failed());
}

#[tokio::test]
async fn failed_write_is_failed_even_for_changes() {
    let (mut client, server) = duplex(1024);
    drop(server);
    let error = exchange::<Value>(
        &mut client,
        "ping",
        &json!({}),
        "pong",
        Effect::Change,
        soon(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, HerdrError::Send(_)));
    assert!(error.is_failed());
}

struct SocketDirectory(PathBuf);

impl SocketDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "chimera-herdr-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn socket(&self) -> PathBuf {
        self.0.join("api.sock")
    }
}

impl Drop for SocketDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("api.sock"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

fn client_for(path: PathBuf, timeout: Duration) -> HerdrClient {
    HerdrClient {
        socket_path: path,
        timeout,
    }
}

#[tokio::test]
async fn validates_inputs_before_contacting_server() {
    let client = client_for(
        "/nonexistent/chimera-herdr.sock".into(),
        Duration::from_secs(1),
    );
    let error = client
        .create_workspace_in(Path::new("relative"))
        .await
        .unwrap_err();
    assert!(matches!(error, HerdrError::InvalidInput(_)));
    assert!(error.is_failed());
    assert!(matches!(
        HerdrClient::connect_with_timeout("/unused", Duration::ZERO).await,
        Err(HerdrError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn missing_socket_is_failed() {
    let dir = SocketDirectory::new();
    let client = client_for(dir.socket(), Duration::from_secs(1));
    let error = client
        .split_pane(&chimera_core::PaneId::new("w1:p1").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, HerdrError::Connect(_)));
    assert!(error.is_failed());
}

#[tokio::test]
async fn refused_connection_is_failed() {
    let dir = SocketDirectory::new();
    // A socket file with nobody listening refuses connections.
    drop(UnixListener::bind(dir.socket()).unwrap());
    let client = client_for(dir.socket(), Duration::from_secs(1));
    let error = client
        .create_workspace_in(Path::new("/repo"))
        .await
        .unwrap_err();
    assert!(matches!(&error, HerdrError::Connect(source)
        if source.kind() == io::ErrorKind::ConnectionRefused));
    assert!(error.is_failed());
}

/// Accepts one connection, reads the request line, then runs `then` on the stream.
async fn serve_once<F, Fut>(path: &Path, then: F) -> tokio::task::JoinHandle<()>
where
    F: FnOnce(Value, BufReader<UnixStream>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let listener = UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        then(serde_json::from_str(&line).unwrap(), stream).await;
    })
}

#[tokio::test]
async fn silent_server_makes_a_change_uncertain() {
    let dir = SocketDirectory::new();
    let server = serve_once(&dir.socket(), |_, stream| async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(stream);
    })
    .await;
    let client = client_for(dir.socket(), Duration::from_millis(50));
    let error = client
        .create_workspace_in(Path::new("/repo"))
        .await
        .unwrap_err();
    assert!(matches!(error, HerdrError::Timeout { uncertain: true }));
    server.await.unwrap();
}

#[tokio::test]
async fn dropped_connection_makes_a_change_uncertain() {
    let dir = SocketDirectory::new();
    let server = serve_once(&dir.socket(), |_, stream| async move { drop(stream) }).await;
    let client = client_for(dir.socket(), Duration::from_secs(2));
    let error = client
        .split_pane(&chimera_core::PaneId::new("w1:p1").unwrap())
        .await
        .unwrap_err();
    assert!(error.is_uncertain());
    server.await.unwrap();
}

#[tokio::test]
async fn silent_server_makes_ping_failed() {
    let dir = SocketDirectory::new();
    let server = serve_once(&dir.socket(), |_, stream| async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(stream);
    })
    .await;
    let error = HerdrClient::connect_with_timeout(dir.socket(), Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(matches!(error, HerdrError::Timeout { uncertain: false }));
    server.await.unwrap();
}

#[test]
fn converts_to_port_error_preserving_classification_and_cause() {
    let failed: PortError = HerdrError::Server {
        code: "not_found".into(),
        message: "pane missing".into(),
    }
    .into();
    assert!(failed.is_failed());
    let text = failed.to_string();
    assert!(
        text.contains("not_found") && text.contains("pane missing"),
        "{text}"
    );

    let uncertain: PortError = HerdrError::Timeout { uncertain: true }.into();
    assert!(uncertain.is_uncertain());
    assert!(uncertain.to_string().contains("timed out"));

    let failed: PortError = HerdrError::InvalidInput("bad").into();
    assert!(failed.is_failed());
}

#[tokio::test]
async fn connects_creates_workspace_and_splits_pane_over_socket() {
    let dir = SocketDirectory::new();
    let path = dir.socket();
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let requests = [
            (
                json!({"method":"ping","params":{}}),
                json!({"type":"pong","version":"0.9.3","protocol":22}),
            ),
            (
                json!({"method":"workspace.create","params":{
                    "cwd":"/repo with spaces","focus":false
                }}),
                json!({
                    "type":"workspace_created",
                    "workspace":{"workspace_id":"w2","label":"test","extra":true,
                        "worktree":{"checkout_path":"/worktrees/test","is_linked_worktree":true}},
                    "tab":{"tab_id":"w2:t1","workspace_id":"w2"},
                    "root_pane":{"pane_id":"w2:p1","workspace_id":"w2","tab_id":"w2:t1"}
                }),
            ),
            (
                json!({"method":"pane.split","params":{
                    "target_pane_id":"w2:p1","direction":"right","focus":false
                }}),
                json!({"type":"pane_info","pane":{
                    "pane_id":"w2:p2","workspace_id":"w2","tab_id":"w2:t1"
                }}),
            ),
        ];
        for (expected, result) in requests {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], expected["method"]);
            assert_eq!(request["params"], expected["params"]);
            let response = json!({"id":request["id"],"result":result}).to_string();
            stream.write_all(&response.as_bytes()[..8]).await.unwrap();
            stream.write_all(&response.as_bytes()[8..]).await.unwrap();
            stream.write_all(b"\n").await.unwrap();
        }
    });
    let client = HerdrClient::connect_with_timeout(&path, Duration::from_secs(2))
        .await
        .unwrap();
    let (workspace, root) = client
        .create_workspace_in(Path::new("/repo with spaces"))
        .await
        .unwrap();
    assert_eq!(workspace.as_str(), "w2");
    let pane = client.split_pane(&root).await.unwrap();
    assert_eq!(pane.as_str(), "w2:p2");
    server.await.unwrap();
}

#[tokio::test]
async fn live_connection() {
    // Integration test: skips (passes) without a reachable HERDR_SOCKET_PATH.
    let _ = super::test_support::live_client().await;
}

#[tokio::test]
async fn skips_live_helper_when_socket_path_is_unreachable() {
    // Only meaningful when the variable is unset; never mutate the process environment.
    if std::env::var_os("HERDR_SOCKET_PATH").is_none() {
        assert!(super::test_support::live_client().await.is_none());
    }
}
