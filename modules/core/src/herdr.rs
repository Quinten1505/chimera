use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fmt, io,
    path::{Path, PathBuf},
    time::Duration,
};

/// A synchronous client for a running Herdr server.
///
/// Each operation opens its own connection. Requests are never retried automatically:
/// a timeout may occur after Herdr has already performed the operation.
#[derive(Debug, Clone)]
pub struct HerdrClient {
    socket_path: PathBuf,
    timeout: Duration,
}

impl HerdrClient {
    /// Connect to an explicit server socket and verify it with a ping.
    ///
    /// Uses a 30-second read/write timeout. Native Windows is unsupported; use WSL.
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self, HerdrError> {
        Self::connect_with_timeout(socket_path, Duration::from_secs(30))
    }

    /// Connect with a nonzero read/write timeout for each operation.
    pub fn connect_with_timeout(
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
        let _: serde_json::Value = client.request("ping", &serde_json::json!({}), "pong")?;
        Ok(client)
    }

    /// Create a Git checkout and open it as a Herdr workspace.
    ///
    /// Herdr uses an existing local branch when present, or creates it from
    /// the base (HEAD by default). Repository trust policy is left to Herdr.
    pub fn create_worktree(
        &self,
        options: &WorktreeOptions,
    ) -> Result<CreatedWorktree, HerdrError> {
        if options.branch.trim().is_empty() {
            return Err(HerdrError::InvalidInput("branch must not be empty"));
        }
        if let WorktreeSource::Directory { cwd } = &options.source {
            require_absolute(cwd)?;
        }
        if let WorktreeSource::Workspace { workspace_id } = &options.source
            && workspace_id.trim().is_empty()
        {
            return Err(HerdrError::InvalidInput("workspace_id must not be empty"));
        }
        if let Some(path) = &options.path {
            require_absolute(path)?;
        }
        self.request("worktree.create", options, "worktree_created")
    }

    /// Add a pane by splitting a specific existing pane.
    pub fn add_pane(&self, options: &PaneOptions) -> Result<Pane, HerdrError> {
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
        let result: PaneResult = self.request("pane.split", options, "pane_info")?;
        Ok(result.pane)
    }

    #[cfg(unix)]
    fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        result_type: &str,
    ) -> Result<T, HerdrError> {
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;
        exchange(&mut stream, method, params, result_type)
    }

    #[cfg(not(unix))]
    fn request<T: DeserializeOwned>(
        &self,
        _method: &str,
        _params: &impl Serialize,
        _result_type: &str,
    ) -> Result<T, HerdrError> {
        let _ = (&self.socket_path, self.timeout);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Herdr Unix socket integration requires Unix or WSL",
        )
        .into())
    }
}

/// Explicitly select the repository without depending on Herdr's focused workspace.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum WorktreeSource {
    Workspace {
        workspace_id: String,
    },
    /// Absolute repository directory on the Herdr server.
    Directory {
        cwd: PathBuf,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeOptions {
    #[serde(flatten)]
    pub source: WorktreeSource,
    pub branch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// Absolute checkout path; omitted to use Herdr's configured location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub focus: bool,
}

impl WorktreeOptions {
    pub fn new(source: WorktreeSource, branch: impl Into<String>) -> Self {
        Self {
            source,
            branch: branch.into(),
            base: None,
            path: None,
            label: None,
            focus: false,
        }
    }
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

/// IDs returned by Herdr can be used as targets for subsequent operations.
/// Unknown server fields are ignored for forward compatibility.
#[derive(Debug, Clone, Deserialize)]
pub struct CreatedWorktree {
    pub workspace: Workspace,
    pub tab: Tab,
    pub root_pane: Pane,
    pub worktree: Worktree,
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

#[derive(Debug, Clone, Deserialize)]
pub struct Worktree {
    pub path: PathBuf,
    pub branch: Option<String>,
}

#[derive(Debug)]
pub enum HerdrError {
    Io(io::Error),
    Json(serde_json::Error),
    InvalidInput(&'static str),
    Protocol(String),
    Server { code: String, message: String },
}

impl fmt::Display for HerdrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "Herdr transport error: {error}"),
            Self::Json(error) => write!(f, "Herdr JSON error: {error}"),
            Self::InvalidInput(message) => write!(f, "Invalid Herdr input: {message}"),
            Self::Protocol(message) => write!(f, "Herdr protocol error: {message}"),
            Self::Server { code, message } => write!(f, "Herdr error ({code}): {message}"),
        }
    }
}

impl std::error::Error for HerdrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for HerdrError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for HerdrError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

fn require_absolute(path: &Path) -> Result<(), HerdrError> {
    if !path.is_absolute() {
        return Err(HerdrError::InvalidInput("cwd and path must be absolute"));
    }
    Ok(())
}

#[cfg(unix)]
fn exchange<T: DeserializeOwned>(
    stream: &mut (impl io::Read + io::Write),
    method: &str,
    params: &impl Serialize,
    result_type: &str,
) -> Result<T, HerdrError> {
    use std::io::{BufRead, BufReader, Read};
    // One request per connection, so a fixed correlation ID is sufficient.
    let request = serde_json::json!({"id": "chimera", "method": method, "params": params});
    let mut bytes = serde_json::to_vec(&request)?;
    bytes.push(b'\n');
    stream.write_all(&bytes)?;
    stream.flush()?;

    // Bound a malformed response instead of allocating indefinitely.
    const MAX_RESPONSE: u64 = 1024 * 1024;
    let mut line = String::new();
    BufReader::new(stream)
        .take(MAX_RESPONSE + 1)
        .read_line(&mut line)?;
    if line.len() as u64 > MAX_RESPONSE || !line.ends_with('\n') {
        return Err(HerdrError::Protocol(
            "response is oversized or incomplete".into(),
        ));
    }
    let response: serde_json::Value = serde_json::from_str(&line)?;
    if response.get("id").and_then(|id| id.as_str()) != Some("chimera") {
        return Err(HerdrError::Protocol(
            "response ID does not match request".into(),
        ));
    }
    if let Some(error) = response.get("error") {
        #[derive(Deserialize)]
        struct ErrorBody {
            code: String,
            message: String,
        }
        let error: ErrorBody = serde_json::from_value(error.clone())?;
        return Err(HerdrError::Server {
            code: error.code,
            message: error.message,
        });
    }
    let result = response
        .get("result")
        .ok_or_else(|| HerdrError::Protocol("response has no result".into()))?;
    if result.get("type").and_then(|kind| kind.as_str()) != Some(result_type) {
        return Err(HerdrError::Protocol(format!(
            "expected {result_type} result"
        )));
    }
    Ok(serde_json::from_value(result.clone())?)
}

#[cfg(all(test, unix))]
mod tests;
