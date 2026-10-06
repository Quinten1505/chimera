use std::time::Duration;

use reqwest::Method;
use serde_json::{Value, json};

use crate::error::GitHubError;

pub const DEFAULT_API_URL: &str = "https://api.github.com";

const API_VERSION: &str = "2022-11-28";
const USER_AGENT: &str = concat!("chimera/", env!("CARGO_PKG_VERSION"));
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CAUSE_BODY: usize = 300;

/// Whether a request changes state on GitHub. Reads are never classified as uncertain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Mutate,
}

impl Access {
    /// Classify a failure after the request may have been received.
    fn lost(self, cause: String) -> GitHubError {
        match self {
            Access::Read => GitHubError::Failed(cause),
            Access::Mutate => GitHubError::Uncertain(cause),
        }
    }
}

/// HTTP client for the GitHub REST and GraphQL APIs. It never retries.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    token: String,
    base_url: String,
    timeout: Duration,
}

impl Client {
    /// A client for `https://api.github.com`.
    pub fn new(token: impl Into<String>) -> Self {
        Self::with_base_url(token, DEFAULT_API_URL)
    }

    pub fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            http: http_client(DEFAULT_TIMEOUT),
            token: token.into(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Overall time allowed per request, from connecting to reading the response.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.http = http_client(timeout);
        self.timeout = timeout;
        self
    }

    /// Send a REST request to `path` (e.g. `/repos/o/r/issues`) and return the JSON response,
    /// or `Value::Null` for an empty body.
    pub async fn rest(
        &self,
        access: Access,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, GitHubError> {
        let mut request = self.request(method, &format!("{}{path}", self.base_url));
        if let Some(body) = body {
            request = request.json(body);
        }
        let bytes = self.send(request, access).await?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes)
            .map_err(|e| access.lost(format!("unreadable GitHub response: {e}")))
    }

    /// Run a GraphQL query or mutation and return its `data`. A response carrying `errors` is a
    /// failure.
    pub async fn graphql(
        &self,
        access: Access,
        query: &str,
        variables: Value,
    ) -> Result<Value, GitHubError> {
        let request = self
            .request(Method::POST, &format!("{}/graphql", self.base_url))
            .json(&json!({ "query": query, "variables": variables }));
        let bytes = self.send(request, access).await?;
        let mut response: Value = serde_json::from_slice(&bytes)
            .map_err(|e| access.lost(format!("unreadable GitHub response: {e}")))?;
        if let Some(errors) = response.get("errors").filter(|e| !e.is_null()) {
            return Err(GitHubError::Failed(format!("GraphQL errors: {errors}")));
        }
        match response.get_mut("data").map(Value::take) {
            Some(data) if !data.is_null() => Ok(data),
            _ => Err(access.lost("GraphQL response has no data".to_owned())),
        }
    }

    fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", USER_AGENT)
            .header("X-GitHub-Api-Version", API_VERSION)
            .timeout(self.timeout)
    }

    /// Send once and return the body of a successful response.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        access: Access,
    ) -> Result<Vec<u8>, GitHubError> {
        // The connect timeout is shorter than the overall one, so a timeout while connecting
        // (including the TLS handshake) always surfaces as a connect error.
        let response = request.send().await.map_err(|e| {
            if e.is_connect() || e.is_builder() {
                GitHubError::Failed(format!("could not send GitHub request: {e}"))
            } else {
                access.lost(format!("no response from GitHub: {e}"))
            }
        })?;
        let status = response.status();
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => {
                let cause = format!("GitHub returned {status} but the body could not be read: {e}");
                return Err(if status.is_client_error() {
                    GitHubError::Failed(cause)
                } else {
                    access.lost(cause)
                });
            }
        };
        if status.is_success() {
            return Ok(bytes.to_vec());
        }
        let cause = format!("GitHub returned {status}: {}", message(&bytes));
        if status.is_server_error() {
            Err(access.lost(cause))
        } else {
            Err(GitHubError::Failed(cause))
        }
    }
}

/// A client that never retries and bounds connection setup below the overall timeout.
///
/// Pooling is disabled because hyper-util transparently retries requests canceled before
/// being written on a stale pooled connection, which `retry(never())` does not cover.
fn http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .retry(reqwest::retry::never())
        .pool_max_idle_per_host(0)
        .connect_timeout(CONNECT_TIMEOUT.min(timeout / 2))
        .build()
        .expect("reqwest client builds with static configuration")
}

fn message(body: &[u8]) -> String {
    let json = serde_json::from_slice::<Value>(body).ok();
    let text = match json.as_ref().and_then(|v| v.get("message")?.as_str()) {
        Some(message) => {
            // Validation failures keep their reason in `errors`, e.g. "A pull request already exists".
            let details = json
                .as_ref()
                .and_then(|v| v.get("errors")?.as_array())
                .into_iter()
                .flatten()
                .filter_map(|e| e.get("message").and_then(Value::as_str).or(e.as_str()))
                .collect::<Vec<_>>();
            if details.is_empty() {
                message.to_owned()
            } else {
                format!("{message}: {}", details.join("; "))
            }
        }
        None => String::from_utf8_lossy(body).into_owned(),
    };
    text.chars().take(MAX_CAUSE_BODY).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimera_core::error::PortError;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// How the mock server answers one connection.
    enum Reply {
        Raw(&'static str),
        /// Read the request, then close without responding.
        Drop,
        /// Read the request, then never respond.
        Hang,
        /// Promise a body longer than what is sent, then close.
        Truncated,
        /// Send a 4xx status line promising a longer body, then close.
        TruncatedClientError,
        /// Send a 4xx status line promising a body, then never send it.
        HangingClientError,
    }

    /// Serve one connection and return the base URL and a handle yielding the raw request.
    async fn serve(reply: Reply) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let mut request = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..n]);
                if n == 0 || request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            match reply {
                Reply::Raw(raw) => stream.write_all(raw.as_bytes()).await.unwrap(),
                Reply::Truncated => stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{\"a\"")
                    .await
                    .unwrap(),
                Reply::TruncatedClientError => stream
                    .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 100\r\n\r\n{\"me")
                    .await
                    .unwrap(),
                Reply::HangingClientError => {
                    stream
                        .write_all(
                            b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 100\r\n\r\n{\"me",
                        )
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_secs(5)).await
                }
                Reply::Drop => {}
                Reply::Hang => tokio::time::sleep(Duration::from_secs(5)).await,
            }
            String::from_utf8_lossy(&request).into_owned()
        });
        (url, handle)
    }

    fn http(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn raw(status: &'static str, body: &str) -> Reply {
        Reply::Raw(Box::leak(http(status, body).into_boxed_str()))
    }

    fn client(url: &str) -> Client {
        Client::with_base_url("tok", url).with_timeout(Duration::from_millis(300))
    }

    async fn rest(access: Access, reply: Reply) -> Result<Value, GitHubError> {
        let (url, _) = serve(reply).await;
        client(&url).rest(access, Method::POST, "/x", None).await
    }

    async fn graphql(access: Access, reply: Reply) -> Result<Value, GitHubError> {
        let (url, _) = serve(reply).await;
        client(&url).graphql(access, "query", json!({})).await
    }

    fn failed(r: Result<Value, GitHubError>) -> bool {
        matches!(r, Err(GitHubError::Failed(_)))
    }

    fn uncertain(r: Result<Value, GitHubError>) -> bool {
        matches!(r, Err(GitHubError::Uncertain(_)))
    }

    #[tokio::test]
    async fn sends_auth_and_api_headers() {
        let (url, handle) = serve(raw("200 OK", "{\"ok\":true}")).await;
        let value = Client::with_base_url("secret", format!("{url}/"))
            .rest(Access::Read, Method::GET, "/repos", None)
            .await
            .unwrap();
        assert_eq!(value, json!({"ok": true}));
        let request = handle.await.unwrap().to_lowercase();
        assert!(request.starts_with("get /repos http/1.1"));
        assert!(request.contains("authorization: bearer secret"));
        assert!(request.contains("accept: application/vnd.github+json"));
        assert!(request.contains("user-agent: chimera/"));
        assert!(request.contains("x-github-api-version: 2022-11-28"));
    }

    #[tokio::test]
    async fn graphql_returns_data() {
        let value = graphql(Access::Read, raw("200 OK", "{\"data\":{\"a\":1}}")).await;
        assert_eq!(value.unwrap(), json!({"a": 1}));
    }

    #[tokio::test]
    async fn empty_success_body_is_null() {
        let value = rest(Access::Mutate, raw("204 No Content", "")).await;
        assert_eq!(value.unwrap(), Value::Null);
    }

    #[tokio::test]
    async fn connection_refused_is_failed_even_for_mutations() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let result = client(&url)
            .rest(Access::Mutate, Method::POST, "/x", None)
            .await;
        assert!(failed(result));
    }

    #[tokio::test]
    async fn client_errors_are_failed_including_rate_limits() {
        for status in ["404 Not Found", "403 Forbidden", "429 Too Many Requests"] {
            let reply = raw(status, "{\"message\":\"nope\"}");
            let result = rest(Access::Mutate, reply).await;
            assert!(failed(result), "{status}");
        }
    }

    #[tokio::test]
    async fn failure_cause_carries_status_and_message() {
        let error = rest(
            Access::Read,
            raw("403 Forbidden", "{\"message\":\"rate limited\"}"),
        )
        .await
        .unwrap_err();
        let cause = error.to_string();
        assert!(
            cause.contains("403") && cause.contains("rate limited"),
            "{cause}"
        );
    }

    #[tokio::test]
    async fn graphql_errors_are_failed_even_for_mutations() {
        let body = "{\"data\":null,\"errors\":[{\"message\":\"bad\"}]}";
        assert!(failed(graphql(Access::Mutate, raw("200 OK", body)).await));
    }

    #[tokio::test]
    async fn server_errors_are_uncertain_for_mutations_only() {
        let reply = || raw("502 Bad Gateway", "");
        assert!(uncertain(rest(Access::Mutate, reply()).await));
        assert!(failed(rest(Access::Read, reply()).await));
    }

    #[tokio::test]
    async fn timeout_is_uncertain_for_mutations_only() {
        assert!(uncertain(rest(Access::Mutate, Reply::Hang).await));
        assert!(failed(rest(Access::Read, Reply::Hang).await));
    }

    #[tokio::test]
    async fn dropped_connection_is_uncertain_for_mutations_only() {
        assert!(uncertain(rest(Access::Mutate, Reply::Drop).await));
        assert!(failed(rest(Access::Read, Reply::Drop).await));
    }

    #[tokio::test]
    async fn truncated_response_is_uncertain_for_mutations_only() {
        assert!(uncertain(rest(Access::Mutate, Reply::Truncated).await));
        assert!(failed(rest(Access::Read, Reply::Truncated).await));
    }

    #[tokio::test]
    async fn unparseable_response_is_uncertain_for_mutations_only() {
        let reply = || raw("200 OK", "not json");
        assert!(uncertain(rest(Access::Mutate, reply()).await));
        assert!(failed(rest(Access::Read, reply()).await));
        let reply = || raw("200 OK", "not json");
        assert!(uncertain(graphql(Access::Mutate, reply()).await));
        assert!(failed(graphql(Access::Read, reply()).await));
    }

    #[test]
    fn converts_to_port_error_preserving_classification_and_cause() {
        let failed: PortError = GitHubError::Failed("boom".into()).into();
        assert_eq!(failed, PortError::failed("boom"));
        let uncertain: PortError = GitHubError::Uncertain("lost".into()).into();
        assert_eq!(uncertain, PortError::uncertain("lost"));
    }

    #[tokio::test]
    async fn unreadable_client_error_body_is_failed_with_status() {
        for (reply, status) in [
            (Reply::TruncatedClientError, "403"),
            (Reply::HangingClientError, "429"),
        ] {
            let error = rest(Access::Mutate, reply).await.unwrap_err();
            assert!(matches!(error, GitHubError::Failed(_)), "{error}");
            assert!(error.to_string().contains(status), "{error}");
        }
    }

    #[tokio::test]
    async fn timeout_before_sending_is_failed_for_mutations() {
        // Accepts TCP but never answers the TLS handshake, so no request is ever sent.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            // Only a TLS handshake record, never plaintext HTTP.
            assert_eq!(buf[0], 0x16);
            assert!(!buf[..n].windows(4).any(|w| w == b"POST"));
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let result = client(&url)
            .rest(Access::Mutate, Method::POST, "/x", None)
            .await;
        assert!(failed(result));
        server.abort();
    }

    /// Connections are never reused, so hyper cannot replay a request canceled on a stale one.
    #[tokio::test]
    async fn does_not_reuse_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut connections = 0;
            while let Ok(Ok((mut s, _))) =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
            {
                connections += 1;
                let mut buf = [0; 4096];
                assert!(s.read(&mut buf).await.unwrap() > 0);
                s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                    .await
                    .unwrap();
                // Keep the connection open so a pooled client would reuse it.
                tokio::spawn(async move {
                    let _ = s.read(&mut buf).await;
                });
            }
            connections
        });
        let client = Client::with_base_url("tok", &url).with_timeout(Duration::from_secs(2));
        for _ in 0..2 {
            client
                .rest(Access::Mutate, Method::POST, "/x", None)
                .await
                .unwrap();
        }
        assert_eq!(server.await.unwrap(), 2);
    }

    #[tokio::test]
    async fn does_not_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let counter = tokio::spawn(async move {
            let mut accepted = 0;
            while let Ok(Ok((mut s, _))) =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
            {
                accepted += 1;
                let mut buf = [0; 4096];
                let _ = s.read(&mut buf).await;
            }
            accepted
        });
        let _ = client(&url)
            .rest(Access::Mutate, Method::POST, "/x", None)
            .await;
        assert_eq!(counter.await.unwrap(), 1);
    }
}
