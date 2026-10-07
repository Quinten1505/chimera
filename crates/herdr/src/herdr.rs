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
pub struct HerdrClient {
    pub(crate) socket_path: PathBuf,
    pub(crate) timeout: Duration,
}

impl HerdrClient {
    /// Connect to an explicit server socket and verify it with a ping.
    ///
    /// Uses a 30-second timeout per request. Native Windows is unsupported; use WSL.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, HerdrError> {
        Self::connect_with_timeout(socket_path, Duration::from_secs(30)).await
    }

    /// Connect with a nonzero timeout for each request.
    pub async fn connect_with_timeout(
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

    /// Open an existing directory in a new Herdr workspace, without creating a Git checkout.
    pub async fn create_workspace(
        &self,
        options: &WorkspaceOptions,
    ) -> Result<CreatedWorkspace, HerdrError> {
        require_absolute(&options.cwd)?;
        self.request(
            "workspace.create",
            options,
            "workspace_created",
            Effect::Change,
        )
        .await
    }

    /// Add a pane by splitting a specific existing pane.
    pub async fn add_pane(&self, options: &PaneOptions) -> Result<Pane, HerdrError> {
        if options.target_pane_id.trim().is_empty() {
            return Err(HerdrError::InvalidInput("target_pane_id must not be empty"));
        }
        if let Some(cwd) = &options.cwd {
            require_absolute(cwd)?;
        }
        #[derive(Deserialize)]
        struct PaneResult {
            pane: Pane,
        }
        let result: PaneResult = self
            .request("pane.split", options, "pane_info", Effect::Change)
            .await?;
        Ok(result.pane)
    }

    /// Launch Codex in an existing shell pane and wait for interactive readiness.
    /// No automatic retries: an error can occur after the process has started.
    pub async fn start_codex_agent(
        &self,
        name: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<crate::Agent, HerdrError> {
        if name.trim().is_empty() || pane_id.trim().is_empty() {
            return Err(HerdrError::InvalidInput(
                "agent name and pane_id must not be empty",
            ));
        }
        if args.iter().any(|arg| arg.contains('\0')) {
            return Err(HerdrError::InvalidInput(
                "agent arguments must not contain NUL",
            ));
        }
        #[derive(Deserialize)]
        struct StartedAgent {
            name: Option<String>,
            agent: Option<String>,
            pane_id: String,
            #[serde(default)]
            agent_session: Option<crate::AgentSessionReference>,
        }
        #[derive(Deserialize)]
        struct Started {
            agent: StartedAgent,
        }
        // Allow the server's 30-second readiness wait to finish before the socket expires.
        let mut client = self.clone();
        client.timeout = client.timeout.max(Duration::from_secs(35));
        let started: Started = client
            .request(
                "agent.start",
                &serde_json::json!({
                    "name": name, "kind": "codex", "pane_id": pane_id,
                    "args": args, "timeout_ms": 30_000,
                }),
                "agent_started",
                Effect::Change,
            )
            .await?;
        if started.agent.pane_id != pane_id || started.agent.agent.as_deref() != Some("codex") {
            return Err(HerdrError::Protocol {
                message: "agent.start returned a different pane or agent kind".into(),
                uncertain: true,
            });
        }
        Ok(crate::Agent {
            name: started.agent.name.unwrap_or_else(|| name.to_owned()),
            kind: "codex".into(),
            pane_id: started.agent.pane_id,
            session: started.agent.agent_session,
        })
    }

    #[cfg(unix)]
    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        result_type: &str,
        effect: Effect,
    ) -> Result<T, HerdrError> {
        use tokio::net::UnixStream;
        let deadline = Instant::now() + self.timeout;
        let mut stream = timeout_at(deadline, UnixStream::connect(&self.socket_path))
            .await
            .map_err(|_| {
                HerdrError::Connect(io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))
            })?
            .map_err(HerdrError::Connect)?;
        exchange(&mut stream, method, params, result_type, effect, deadline).await
    }

    #[cfg(not(unix))]
    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        _method: &str,
        _params: &impl Serialize,
        _result_type: &str,
        _effect: Effect,
    ) -> Result<T, HerdrError> {
        let _ = (&self.socket_path, self.timeout);
        Err(HerdrError::Connect(io::Error::new(
            io::ErrorKind::Unsupported,
            "Herdr Unix socket integration requires Unix or WSL",
        )))
    }
}

/// Options for opening an existing directory in Herdr.
#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceOptions {
    /// Absolute directory on the Herdr server.
    pub cwd: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub focus: bool,
}

impl WorkspaceOptions {
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            label: None,
            focus: false,
        }
    }
}

/// A workspace and its initial tab and pane returned by Herdr.
#[derive(Debug, Clone, Deserialize)]
pub struct CreatedWorkspace {
    pub workspace: Workspace,
    pub tab: Tab,
    pub root_pane: Pane,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitDirection {
    Right,
    Down,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaneOptions {
    pub target_pane_id: String,
    pub direction: SplitDirection,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    pub focus: bool,
}

impl PaneOptions {
    pub fn new(target_pane_id: impl Into<String>, direction: SplitDirection) -> Self {
        Self {
            target_pane_id: target_pane_id.into(),
            direction,
            cwd: None,
            focus: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Workspace {
    pub workspace_id: String,
    pub label: String,
    /// Git checkout path from Herdr's optional worktree metadata.
    /// None means Herdr did not report an association, even if panes run in a repo.
    #[serde(
        default,
        rename = "worktree",
        deserialize_with = "deserialize_checkout_path"
    )]
    pub checkout_path: Option<PathBuf>,
}

fn deserialize_checkout_path<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Metadata {
        checkout_path: PathBuf,
    }

    Ok(Option::<Metadata>::deserialize(deserializer)?.map(|metadata| metadata.checkout_path))
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tab {
    pub tab_id: String,
    pub workspace_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pane {
    pub pane_id: String,
    pub workspace_id: String,
    pub tab_id: String,
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
pub use turn::{OUTPUT_MAX_BYTES, OUTPUT_MAX_LINES};

#[cfg(all(test, unix))]
mod tests;
