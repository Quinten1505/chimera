use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    time::{Instant, timeout_at},
};

/// Whether an operation changes Herdr state. Only changing operations can be uncertain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    Read,
    Change,
}

/// An async client for a running Herdr server.
///
/// Each operation opens its own connection. Requests are never retried automatically:
/// a timeout may occur after Herdr has already performed the operation.
#[derive(Debug, Clone)]
pub(crate) struct HerdrClient {
    pub(crate) socket_path: PathBuf,
    pub(crate) timeout: Duration,
}

impl HerdrClient {
    /// Connect to an explicit server socket and verify it with a ping.
    ///
    /// Uses a 30-second timeout per request. Native Windows is unsupported; use WSL.
    pub(crate) async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, HerdrError> {
        Self::connect_with_timeout(socket_path, Duration::from_secs(30)).await
    }

    /// Connect with a nonzero timeout for each request.
    pub(crate) async fn connect_with_timeout(
        socket_path: impl AsRef<Path>,
        timeout: Duration,
    ) -> Result<Self, HerdrError> {
        if timeout.is_zero() {
            return Err(HerdrError::InvalidInput("timeout must be nonzero"));
        }
        let client = Self {
            socket_path: socket_path.as_ref().to_owned(),
            timeout,
        };
        let _: serde_json::Value = client
            .request("ping", &serde_json::json!({}), "pong", Effect::Read)
            .await?;
        Ok(client)
    }

    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        result_type: &str,
        effect: Effect,
    ) -> Result<T, HerdrError> {
        self.request_within(method, params, result_type, effect, self.timeout)
            .await
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// As `request`, bounded by `timeout` instead of the client's own.
    #[cfg(unix)]
    pub(crate) async fn request_within<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        result_type: &str,
        effect: Effect,
        timeout: Duration,
    ) -> Result<T, HerdrError> {
        use tokio::net::UnixStream;
        let deadline = Instant::now() + timeout;
        let mut stream = timeout_at(deadline, UnixStream::connect(&self.socket_path))
            .await
            .map_err(|_| {
                HerdrError::Connect(io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))
            })?
            .map_err(HerdrError::Connect)?;
        exchange(&mut stream, method, params, result_type, effect, deadline).await
    }

    #[cfg(not(unix))]
    pub(crate) async fn request_within<T: DeserializeOwned>(
        &self,
        _method: &str,
        _params: &impl Serialize,
        _result_type: &str,
        _effect: Effect,
        _timeout: Duration,
    ) -> Result<T, HerdrError> {
        let _ = (&self.socket_path, self.timeout);
        Err(HerdrError::Connect(io::Error::new(
            io::ErrorKind::Unsupported,
            "Herdr Unix socket integration requires Unix or WSL",
        )))
    }
}

/// A workspace and its initial tab and pane returned by Herdr.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CreatedWorkspace {
    pub workspace: Workspace,
    pub root_pane: Pane,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Workspace {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Pane {
    pub pane_id: String,
}

/// A failed Herdr operation, classified as *failed* or *uncertain*.
///
/// Failed means the operation is known not to have happened. Uncertain means the request was
/// fully written to a state-changing operation and the outcome is unknown, so Herdr must be
/// inspected before retrying. Read operations are never uncertain.
#[derive(Debug, Error)]
pub enum HerdrError {
    /// Rejected before anything was sent.
    #[error("invalid Herdr input: {0}")]
    InvalidInput(&'static str),
    /// The socket is missing, refused the connection, or did not accept it in time.
    #[error("cannot connect to Herdr: {0}")]
    Connect(#[source] io::Error),
    /// The request was not completely written, so Herdr cannot have acted on it.
    #[error("cannot send request to Herdr: {0}")]
    Send(#[source] io::Error),
    #[error("Herdr request timed out")]
    Timeout { uncertain: bool },
    /// The connection broke while reading the response.
    #[error("Herdr connection failed: {source}")]
    Receive {
        #[source]
        source: io::Error,
        uncertain: bool,
    },
    #[error("Herdr protocol error: {message}")]
    Protocol { message: String, uncertain: bool },
    #[error("malformed Herdr response: {source}")]
    Json {
        #[source]
        source: serde_json::Error,
        uncertain: bool,
    },
    /// Herdr understood the request and answered with an error.
    #[error("Herdr error ({code}): {message}")]
    Server { code: String, message: String },
}

impl HerdrError {
    /// Whether the outcome of a state-changing operation is unknown.
    pub fn is_uncertain(&self) -> bool {
        match self {
            Self::Timeout { uncertain }
            | Self::Receive { uncertain, .. }
            | Self::Protocol { uncertain, .. }
            | Self::Json { uncertain, .. } => *uncertain,
            Self::InvalidInput(_) | Self::Connect(_) | Self::Send(_) | Self::Server { .. } => false,
        }
    }

    pub fn is_failed(&self) -> bool {
        !self.is_uncertain()
    }
}

impl From<HerdrError> for chimera_core::error::PortError {
    fn from(error: HerdrError) -> Self {
        let cause = error.to_string();
        if error.is_uncertain() {
            Self::uncertain(cause)
        } else {
            Self::failed(cause)
        }
    }
}

pub(crate) fn require_absolute(path: &Path) -> Result<(), HerdrError> {
    if !path.is_absolute() {
        return Err(HerdrError::InvalidInput("cwd and path must be absolute"));
    }
    Ok(())
}

/// Largest accepted response line, to bound a malformed response.
const MAX_RESPONSE: u64 = 1024 * 1024;

async fn exchange<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    method: &str,
    params: &impl Serialize,
    result_type: &str,
    effect: Effect,
    deadline: Instant,
) -> Result<T, HerdrError> {
    // One request per connection, so a fixed correlation ID is sufficient.
    let request = serde_json::json!({"id": "chimera", "method": method, "params": params});
    let mut bytes = serde_json::to_vec(&request).map_err(|source| HerdrError::Json {
        source,
        uncertain: false,
    })?;
    bytes.push(b'\n');
    // Until the newline is written Herdr cannot act on the request.
    let sent = async {
        stream.write_all(&bytes).await?;
        stream.flush().await
    };
    match timeout_at(deadline, sent).await {
        Err(_) => return Err(HerdrError::Timeout { uncertain: false }),
        Ok(Err(error)) => return Err(HerdrError::Send(error)),
        Ok(Ok(())) => {}
    }

    let uncertain = effect == Effect::Change;
    let protocol = |message: &str| HerdrError::Protocol {
        message: message.into(),
        uncertain,
    };
    let json = |source| HerdrError::Json { source, uncertain };
    let mut line = String::new();
    let mut reader = BufReader::new(&mut *stream).take(MAX_RESPONSE + 1);
    let read = reader.read_line(&mut line);
    match timeout_at(deadline, read).await {
        Err(_) => return Err(HerdrError::Timeout { uncertain }),
        Ok(Err(source)) => return Err(HerdrError::Receive { source, uncertain }),
        Ok(Ok(_)) => {}
    }
    if line.len() as u64 > MAX_RESPONSE || !line.ends_with('\n') {
        return Err(protocol("response is oversized or incomplete"));
    }
    let response: serde_json::Value = serde_json::from_str(&line).map_err(json)?;
    if response.get("id").and_then(|id| id.as_str()) != Some("chimera") {
        return Err(protocol("response ID does not match request"));
    }
    if let Some(error) = response.get("error") {
        #[derive(Deserialize)]
        struct ErrorBody {
            code: String,
            message: String,
        }
        let error: ErrorBody = serde_json::from_value(error.clone()).map_err(json)?;
        return Err(HerdrError::Server {
            code: error.code,
            message: error.message,
        });
    }
    let result = response
        .get("result")
        .ok_or_else(|| protocol("response has no result"))?;
    if result.get("type").and_then(|kind| kind.as_str()) != Some(result_type) {
        return Err(HerdrError::Protocol {
            message: format!("expected {result_type} result"),
            uncertain,
        });
    }
    serde_json::from_value(result.clone()).map_err(json)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::HerdrClient;

    /// Connects to the server at `HERDR_SOCKET_PATH` for integration tests. Returns `None`, after
    /// printing why, when it is unset or unreachable, so the calling test skips and passes.
    pub(crate) async fn live_client() -> Option<HerdrClient> {
        let Some(path) = std::env::var_os("HERDR_SOCKET_PATH") else {
            eprintln!("skipping: HERDR_SOCKET_PATH is not set");
            return None;
        };
        match HerdrClient::connect(&path).await {
            Ok(client) => Some(client),
            Err(error) => {
                eprintln!("skipping: Herdr at {path:?} is unreachable: {error}");
                None
            }
        }
    }
}

mod turn;

#[cfg(all(test, unix))]
mod tests;
