//! Per-turn operations: send a prompt, read status, read output. All address a pane by its
//! [`PaneId`] and deal in plain text.

use chimera_core::{PaneId, terminal::TurnStatus};
use serde::Deserialize;
use serde_json::json;

use super::{Effect, HerdrClient, HerdrError};
use crate::{AgentRecord, turn_status_of_lookup};

/// Most output lines requested from Herdr by [`HerdrClient::read_output`].
pub const OUTPUT_MAX_LINES: u32 = 500;

/// Largest output returned by [`HerdrClient::read_output`], in bytes. When the pane's recent
/// output is longer, the oldest part is dropped (at a character boundary) so the end, where the
/// agent's latest answer is, is kept.
pub const OUTPUT_MAX_BYTES: usize = 64 * 1024;

/// Herdr error codes that arrive after the prompt was already submitted: the outcome of the
/// send is not known to have failed.
const SUBMITTED_ERROR_CODES: [&str; 2] = ["agent_prompt_stalled", "timeout"];

impl HerdrClient {
    /// Submits `text` as a prompt to the agent in `pane` (`agent.prompt`).
    ///
    /// Returns once Herdr has accepted the prompt and the agent has left its previous idle or
    /// done state (it is `working` or `blocked`), so a status read right after cannot report
    /// the previous turn as finished. It does not wait for the turn to finish, and has no
    /// timeout of its own on the turn: callers poll [`read_status`](Self::read_status).
    ///
    /// Failed when Herdr rejects the prompt before sending any input (for example the pane is
    /// gone or the agent is blocked). Uncertain once the request was written and no clear
    /// answer came back, or when the agent never left its previous state after submission.
    pub async fn send_prompt(&self, pane: &PaneId, text: &str) -> Result<(), HerdrError> {
        self.request::<serde_json::Value>(
            "agent.prompt",
            &json!({
                "target": pane.as_str(),
                "text": text,
                "wait": {"until": ["working", "blocked"]},
            }),
            "agent_prompted",
            Effect::Change,
        )
        .await
        .map(drop)
        .map_err(|error| match error {
            HerdrError::Server { code, message }
                if SUBMITTED_ERROR_CODES.contains(&code.as_str()) =>
            {
                HerdrError::Protocol {
                    message: format!("prompt submitted but not confirmed ({code}): {message}"),
                    uncertain: true,
                }
            }
            other => other,
        })
    }

    /// The state of the turn in `pane`. A missing agent, pane or workspace is
    /// [`TurnStatus::Gone`], not an error. A read, so never uncertain.
    pub async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, HerdrError> {
        let lookup = self
            .request::<serde_json::Value>(
                "pane.get",
                &json!({"pane_id": pane.as_str()}),
                "pane_info",
                Effect::Read,
            )
            .await
            .and_then(|result| AgentRecord::from_pane_info(&result));
        turn_status_of_lookup(lookup)
    }

    /// The recent output of `pane` as plain text (`pane.read`, unwrapped recent source, ANSI
    /// stripped), at most [`OUTPUT_MAX_LINES`] lines and [`OUTPUT_MAX_BYTES`] bytes; the end is
    /// kept. A missing pane is an error. A read, so never uncertain.
    pub async fn read_output(&self, pane: &PaneId) -> Result<String, HerdrError> {
        #[derive(Deserialize)]
        struct Read {
            text: String,
        }
        #[derive(Deserialize)]
        struct PaneRead {
            read: Read,
        }
        let result: PaneRead = self
            .request(
                "pane.read",
                &json!({
                    "pane_id": pane.as_str(),
                    "source": "recent_unwrapped",
                    "format": "text",
                    "strip_ansi": true,
                    "lines": OUTPUT_MAX_LINES,
                }),
                "pane_read",
                Effect::Read,
            )
            .await?;
        Ok(truncate_tail(result.read.text, OUTPUT_MAX_BYTES))
    }
}

/// Keeps the last `max` bytes of `text`, starting on a character boundary.
fn truncate_tail(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut start = text.len() - max;
        while !text.is_char_boundary(start) {
            start += 1;
        }
        text.drain(..start);
    }
    text
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    use super::*;

    struct Socket(PathBuf);

    impl Socket {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "chimera-herdr-turn-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            std::fs::create_dir(&dir).unwrap();
            Self(dir.join("api.sock"))
        }

        fn client(&self) -> HerdrClient {
            HerdrClient {
                socket_path: self.0.clone(),
                timeout: Duration::from_secs(2),
            }
        }

        /// Serves one request: replies with `reply` (or just closes when `None`) and returns
        /// the request seen.
        fn serve(&self, reply: Option<Value>) -> tokio::task::JoinHandle<Value> {
            let listener = UnixListener::bind(&self.0).unwrap();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                if let Some(reply) = reply {
                    let mut bytes = serde_json::to_vec(&reply).unwrap();
                    bytes.push(b'\n');
                    stream.get_mut().write_all(&bytes).await.unwrap();
                }
                serde_json::from_str(&line).unwrap()
            })
        }
    }

    impl Drop for Socket {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_dir(self.0.parent().unwrap());
        }
    }

    fn ok(result: Value) -> Option<Value> {
        Some(json!({"id": "chimera", "result": result}))
    }

    fn err(code: &str) -> Option<Value> {
        Some(json!({"id": "chimera", "error": {"code": code, "message": "m"}}))
    }

    fn pane() -> PaneId {
        PaneId::new("w8:p2").unwrap()
    }

    fn pane_info(status: &str) -> Option<Value> {
        ok(json!({"type":"pane_info","pane":{
            "agent":"claude","agent_status":status,"pane_id":"w8:p2"}}))
    }

    fn pane_read(text: &str) -> Option<Value> {
        ok(json!({"type":"pane_read","read":{
            "pane_id":"w8:p2","workspace_id":"w8","tab_id":"w8:t1","source":"recent_unwrapped",
            "format":"text","text":text,"revision":1,"truncated":false}}))
    }

    #[tokio::test]
    async fn prompt_request_waits_for_the_agent_to_leave_idle() {
        let socket = Socket::new();
        let server = socket.serve(ok(json!({"type":"agent_prompted","agent":{
            "agent_status":"working","pane_id":"w8:p2"}})));
        socket
            .client()
            .send_prompt(&pane(), "do it\nnow")
            .await
            .unwrap();
        assert_eq!(
            server.await.unwrap(),
            json!({"id":"chimera","method":"agent.prompt","params":{
                "target":"w8:p2","text":"do it\nnow",
                "wait":{"until":["working","blocked"]}}})
        );
    }

    #[tokio::test]
    async fn prompt_rejected_by_herdr_is_failed() {
        for code in ["agent_not_found", "pane_not_found", "agent_blocked"] {
            let socket = Socket::new();
            let server = socket.serve(err(code));
            let error = socket.client().send_prompt(&pane(), "x").await.unwrap_err();
            server.await.unwrap();
            assert!(matches!(&error, HerdrError::Server { code: c, .. } if c == code));
            assert!(error.is_failed(), "{code}");
        }
    }

    #[tokio::test]
    async fn prompt_not_confirmed_after_submission_is_uncertain() {
        for code in ["agent_prompt_stalled", "timeout"] {
            let socket = Socket::new();
            let server = socket.serve(err(code));
            let error = socket.client().send_prompt(&pane(), "x").await.unwrap_err();
            server.await.unwrap();
            assert!(error.is_uncertain(), "{code}");
        }
    }

    #[tokio::test]
    async fn prompt_without_a_clear_answer_is_uncertain() {
        // Connection dropped after the request was written.
        let socket = Socket::new();
        let server = socket.serve(None);
        let error = socket.client().send_prompt(&pane(), "x").await.unwrap_err();
        server.await.unwrap();
        assert!(error.is_uncertain());
        // Wrong result type.
        let socket = Socket::new();
        let server = socket.serve(ok(json!({"type":"pong","version":"1"})));
        let error = socket.client().send_prompt(&pane(), "x").await.unwrap_err();
        server.await.unwrap();
        assert!(error.is_uncertain());
    }

    #[tokio::test]
    async fn prompt_not_written_is_failed() {
        let socket = Socket::new();
        let error = socket.client().send_prompt(&pane(), "x").await.unwrap_err();
        assert!(error.is_failed());
    }

    #[tokio::test]
    async fn status_maps_each_case() {
        for (status, expected) in [
            ("working", TurnStatus::Running),
            ("blocked", TurnStatus::Running),
            ("unknown", TurnStatus::Running),
            ("idle", TurnStatus::Finished),
            ("done", TurnStatus::Finished),
        ] {
            let socket = Socket::new();
            let server = socket.serve(pane_info(status));
            assert_eq!(
                socket.client().read_status(&pane()).await.unwrap(),
                expected
            );
            assert_eq!(
                server.await.unwrap(),
                json!({"id":"chimera","method":"pane.get","params":{"pane_id":"w8:p2"}})
            );
        }
    }

    #[tokio::test]
    async fn status_is_gone_without_agent_pane_or_workspace() {
        let socket = Socket::new();
        let _server = socket.serve(ok(json!({"type":"pane_info","pane":{
            "agent_status":"unknown","pane_id":"w8:p2"}})));
        assert_eq!(
            socket.client().read_status(&pane()).await.unwrap(),
            TurnStatus::Gone
        );
        for code in ["agent_not_found", "pane_not_found", "workspace_not_found"] {
            let socket = Socket::new();
            let _server = socket.serve(err(code));
            assert_eq!(
                socket.client().read_status(&pane()).await.unwrap(),
                TurnStatus::Gone,
                "{code}"
            );
        }
    }

    #[tokio::test]
    async fn status_errors_are_failed_never_uncertain() {
        let socket = Socket::new();
        let _server = socket.serve(err("internal"));
        assert!(
            socket
                .client()
                .read_status(&pane())
                .await
                .unwrap_err()
                .is_failed()
        );
        let socket = Socket::new();
        let _server = socket.serve(None);
        assert!(
            socket
                .client()
                .read_status(&pane())
                .await
                .unwrap_err()
                .is_failed()
        );
        let socket = Socket::new();
        let _server = socket.serve(ok(json!({"type":"pong","version":"1"})));
        assert!(
            socket
                .client()
                .read_status(&pane())
                .await
                .unwrap_err()
                .is_failed()
        );
    }

    #[tokio::test]
    async fn output_request_and_decoding() {
        let socket = Socket::new();
        let server = socket.serve(pane_read("line one\nline two\n"));
        let text = socket.client().read_output(&pane()).await.unwrap();
        assert_eq!(text, "line one\nline two\n");
        assert_eq!(
            server.await.unwrap(),
            json!({"id":"chimera","method":"pane.read","params":{
                "pane_id":"w8:p2","source":"recent_unwrapped","format":"text",
                "strip_ansi":true,"lines":OUTPUT_MAX_LINES}})
        );
    }

    #[tokio::test]
    async fn output_is_truncated_keeping_the_end() {
        let long = format!("{}end", "é".repeat(OUTPUT_MAX_BYTES));
        let socket = Socket::new();
        let _server = socket.serve(pane_read(&long));
        let text = socket.client().read_output(&pane()).await.unwrap();
        assert!(text.len() <= OUTPUT_MAX_BYTES);
        assert!(text.len() > OUTPUT_MAX_BYTES - 2);
        assert!(text.ends_with("éend"));
    }

    #[test]
    fn short_output_is_untouched() {
        assert_eq!(truncate_tail("abc".into(), 3), "abc");
        assert_eq!(truncate_tail("abcd".into(), 3), "bcd");
    }

    #[tokio::test]
    async fn output_errors_are_failed_never_uncertain() {
        for reply in [err("pane_not_found"), None, ok(json!({"type":"pong"}))] {
            let socket = Socket::new();
            let _server = socket.serve(reply);
            let error = socket.client().read_output(&pane()).await.unwrap_err();
            assert!(error.is_failed());
        }
    }
}
