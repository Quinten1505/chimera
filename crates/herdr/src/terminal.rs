use std::{collections::HashMap, path::Path};

use async_trait::async_trait;
use chimera_core::{
    PaneId, WorkspaceId,
    error::PortError,
    terminal::{Terminal, TurnStatus},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    DEFAULT_READY_TIMEOUT, HerdrClient, HerdrError, herdr::Effect, herdr::require_absolute,
    workspace::root_label,
};

/// Source name under which Chimera reports metadata to Herdr.
const METADATA_SOURCE: &str = "chimera";

/// Prefix of the pane tokens that record prompt deliveries. Each send owns one token, named by
/// a unique nonce, so no send ever reads, rewrites, or counts another's: Herdr merges tokens
/// per key, which makes concurrent sends, rollbacks, and restarts independent by construction.
/// A token is [`PENDING`] from before the send until its outcome is known, then [`DELIVERED`]
/// or removed. Delivered tokens are later folded into [`COUNT_KEY`].
const RECEIPT_PREFIX: &str = "chimera_p_";
const PENDING: &str = "pending";
const DELIVERED: &str = "ok";

/// The pane token holding `<version>:<count>`: the deliveries folded out of their own tokens,
/// and the number of folds so far. Herdr keeps at most 32 tokens per pane, so without folding
/// a pane could record only that many prompts.
const COUNT_KEY: &str = "chimera_count";

/// The most receipts one fold removes: a report carries at most 16 tokens, one being the count.
const FOLD_LIMIT: usize = 15;

/// The receipt tokens of a pane, as one atomic read of them shows.
struct Receipts {
    version: u64,
    folded: u64,
    delivered: Vec<String>,
    unresolved: u64,
}

impl Receipts {
    fn of(tokens: &HashMap<String, String>) -> Result<Self, HerdrError> {
        let (version, folded) = match tokens.get(COUNT_KEY) {
            None => (0, 0),
            Some(value) => value
                .split_once(':')
                .and_then(|(version, count)| Some((version.parse().ok()?, count.parse().ok()?)))
                .ok_or_else(|| HerdrError::Protocol {
                    message: format!("the pane's prompt count {value:?} is malformed"),
                    uncertain: true,
                })?,
        };
        let mut receipts = Self {
            version,
            folded,
            delivered: vec![],
            unresolved: 0,
        };
        for (name, value) in tokens {
            if name.starts_with(RECEIPT_PREFIX) {
                if value == DELIVERED {
                    receipts.delivered.push(name.clone());
                } else {
                    receipts.unresolved += 1;
                }
            }
        }
        Ok(receipts)
    }
}

/// The [`Terminal`] port over a running Herdr's Unix socket.
///
/// It keeps no workspace, pane, or agent state: it holds only the socket path, and every
/// operation addresses Herdr by the IDs it is given. An instance built from the same socket
/// path after a Chimera restart therefore works with IDs saved by an earlier one.
#[derive(Debug, Clone)]
pub struct HerdrTerminal {
    client: HerdrClient,
}

impl HerdrTerminal {
    /// Connects to the Herdr server at `socket_path` and verifies it with a ping.
    ///
    /// Native Windows is unsupported; use WSL.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, HerdrError> {
        Ok(Self {
            client: HerdrClient::connect(socket_path).await?,
        })
    }
}

#[derive(Deserialize)]
struct PaneRecord {
    pane_id: String,
    workspace_id: String,
    #[serde(default)]
    tokens: HashMap<String, String>,
}

#[derive(Deserialize)]
struct WorkspaceRecord {
    workspace_id: String,
    #[serde(default)]
    label: String,
}

impl HerdrClient {
    async fn list_panes(&self) -> Result<Vec<PaneRecord>, HerdrError> {
        #[derive(Deserialize)]
        struct List {
            panes: Vec<PaneRecord>,
        }
        let list: List = self
            .request("pane.list", &json!({}), "pane_list", Effect::Read)
            .await?;
        Ok(list.panes)
    }

    async fn get_pane(&self, pane: &PaneId) -> Result<PaneRecord, HerdrError> {
        #[derive(Deserialize)]
        struct Found {
            pane: PaneRecord,
        }
        let found: Found = self
            .request(
                "pane.get",
                &json!({"pane_id": pane.as_str()}),
                "pane_info",
                Effect::Read,
            )
            .await?;
        Ok(found.pane)
    }

    /// The number of confirmed deliveries. Any unresolved send makes the count unknowable, so
    /// that is an uncertain error rather than a number.
    async fn prompts_received(&self, pane: &PaneId) -> Result<u64, HerdrError> {
        let receipts = Receipts::of(&self.get_pane(pane).await?.tokens)?;
        if receipts.unresolved > 0 {
            return Err(HerdrError::Protocol {
                message: format!(
                    "{} prompt delivery(ies) to the pane are unresolved, so the count is unknown",
                    receipts.unresolved
                ),
                uncertain: true,
            });
        }
        Ok(receipts.folded + receipts.delivered.len() as u64)
    }

    /// Folds delivered receipts into the count, so a pane's tokens stay bounded however many
    /// prompts it receives. A fold removes receipts and adds them to the count in one report,
    /// which Herdr applies whole or not at all, and only if no other fold came first: the report
    /// carries the next version as its `seq`, and Herdr drops, without an error, any report from
    /// the source whose `seq` does not exceed the last one it applied. A fold based on a stale
    /// read therefore changes nothing. Every outcome, including an unknown one, leaves the
    /// receipts consistent, so a fold that fails is simply left for a later send.
    async fn fold_receipts(&self, pane: &PaneId) {
        // A pane holds at most 32 tokens, so three folds clear it.
        for _ in 0..3 {
            let Ok(record) = self.get_pane(pane).await else {
                return;
            };
            let Ok(receipts) = Receipts::of(&record.tokens) else {
                return;
            };
            let more = receipts.delivered.len() > FOLD_LIMIT;
            let batch: Vec<String> = receipts.delivered.into_iter().take(FOLD_LIMIT).collect();
            let Some(version) = receipts.version.checked_add(1) else {
                return;
            };
            if batch.is_empty() {
                return;
            }
            let mut tokens = serde_json::Map::new();
            let count = receipts.folded + batch.len() as u64;
            tokens.insert(COUNT_KEY.into(), format!("{version}:{count}").into());
            for key in batch {
                tokens.insert(key, serde_json::Value::Null);
            }
            let folded: Result<serde_json::Value, _> = self
                .request(
                    "pane.report_metadata",
                    &json!({
                        "pane_id": pane.as_str(),
                        "source": METADATA_SOURCE,
                        "seq": version,
                        "tokens": tokens,
                    }),
                    "ok",
                    Effect::Change,
                )
                .await;
            if folded.is_err() || !more {
                return;
            }
        }
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, HerdrError> {
        #[derive(Deserialize)]
        struct List {
            workspaces: Vec<WorkspaceRecord>,
        }
        let list: List = self
            .request("workspace.list", &json!({}), "workspace_list", Effect::Read)
            .await?;
        Ok(list.workspaces)
    }

    /// Sets one receipt token, or removes it with `None`.
    async fn write_receipt(
        &self,
        pane: &PaneId,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), HerdrError> {
        let _: serde_json::Value = self
            .request(
                "pane.report_metadata",
                &json!({
                    "pane_id": pane.as_str(),
                    "source": METADATA_SOURCE,
                    "tokens": {key: value},
                }),
                "ok",
                Effect::Change,
            )
            .await?;
        Ok(())
    }
}

/// A receipt token name that no other send, in this or another process, shares.
fn receipt_key() -> String {
    use std::{
        collections::hash_map::RandomState,
        hash::{BuildHasher, Hasher},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };
    static SENDS: AtomicU64 = AtomicU64::new(0);
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(SENDS.fetch_add(1, Ordering::Relaxed));
    hasher.write_u32(std::process::id());
    hasher.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    );
    format!("{RECEIPT_PREFIX}{:016x}", hasher.finish())
}

/// The creation number of a pane ID such as `w8:p2`, for ordering panes within a workspace.
fn pane_number(pane_id: &str) -> u64 {
    pane_id
        .rsplit_once(":p")
        .and_then(|(_, number)| number.parse().ok())
        .unwrap_or(u64::MAX)
}

#[async_trait]
impl Terminal for HerdrTerminal {
    async fn create_workspace(&self, directory: &Path) -> Result<(WorkspaceId, PaneId), PortError> {
        Ok(self.client.create_workspace_in(directory).await?)
    }

    /// The first workspace, in Herdr's order, that Chimera created for `directory`. The root is
    /// the workspace's label, set by the request that created it, so it does not follow a
    /// pane's current directory and exists whenever the workspace does.
    async fn find_workspace(
        &self,
        directory: &Path,
    ) -> Result<Option<(WorkspaceId, Vec<PaneId>)>, PortError> {
        require_absolute(directory)?;
        let label = root_label(directory);
        let Some(found) = self
            .client
            .list_workspaces()
            .await?
            .into_iter()
            .find(|workspace| workspace.label == label)
        else {
            return Ok(None);
        };
        let mut panes: Vec<PaneRecord> = self
            .client
            .list_panes()
            .await?
            .into_iter()
            .filter(|pane| pane.workspace_id == found.workspace_id)
            .collect();
        panes.sort_by_key(|pane| pane_number(&pane.pane_id));
        let ids = panes
            .into_iter()
            .map(|pane| PaneId::new(pane.pane_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PortError::failed("Herdr returned an empty pane ID"))?;
        let workspace = WorkspaceId::new(found.workspace_id)
            .map_err(|_| PortError::failed("Herdr returned an empty workspace ID"))?;
        Ok(Some((workspace, ids)))
    }

    /// Splits `pane` after checking that it belongs to `workspace`; a mismatch is failed and
    /// changes nothing in Herdr.
    async fn split_pane(
        &self,
        workspace: &WorkspaceId,
        pane: &PaneId,
    ) -> Result<PaneId, PortError> {
        let record = self.client.get_pane(pane).await?;
        if record.workspace_id != workspace.as_str() {
            return Err(HerdrError::InvalidInput("pane does not belong to the workspace").into());
        }
        Ok(self.client.split_pane(pane).await?)
    }

    async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
        Ok(self
            .client
            .launch_command_line(pane, command_line, DEFAULT_READY_TIMEOUT)
            .await?)
    }

    /// Records the send in a pane token of its own before sending, and settles the token once
    /// the outcome is known: confirmed on delivery, removed on a certain failure. A send
    /// whose outcome is unknown, or whose token cannot be settled, leaves the token pending,
    /// which makes `prompts_received` uncertain instead of letting it report a prompt that may
    /// not have arrived. Nothing is sent unless the pending token was recorded. Each send
    /// first folds earlier confirmed receipts into the count to keep room for its own.
    async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
        self.client.fold_receipts(pane).await;
        let key = receipt_key();
        if let Err(error) = self.client.write_receipt(pane, &key, Some(PENDING)).await {
            if error.is_uncertain() {
                // Nothing was sent, so the token, if it was written, can go.
                let _ = self.client.write_receipt(pane, &key, None).await;
            }
            return Err(error.into());
        }
        match self.client.send_prompt(pane, prompt).await {
            Ok(()) => self
                .client
                .write_receipt(pane, &key, Some(DELIVERED))
                .await
                .map_err(|error| unsettled("delivered but its receipt was not confirmed", &error)),
            Err(error) if error.is_uncertain() => Err(error.into()),
            Err(error) => match self.client.write_receipt(pane, &key, None).await {
                Ok(()) => Err(error.into()),
                Err(rollback) => Err(unsettled(
                    &format!("failed ({error}) but its receipt was not removed"),
                    &rollback,
                )),
            },
        }
    }

    async fn prompts_received(&self, pane: &PaneId) -> Result<u64, PortError> {
        Ok(self.client.prompts_received(pane).await?)
    }

    async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
        Ok(self.client.read_status(pane).await?)
    }

    async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
        Ok(self.client.read_output(pane).await?)
    }

    async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
        Ok(self.client.close_workspace(workspace).await?)
    }
}

/// An uncertain error for a send whose receipt could not be settled.
fn unsettled(what: &str, cause: &HerdrError) -> PortError {
    HerdrError::Protocol {
        message: format!("prompt {what}: {cause}"),
        uncertain: true,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn orders_panes_by_creation_number() {
        let mut ids = vec!["w1:p10", "w1:p2", "w1:p1"];
        ids.sort_by_key(|id| pane_number(id));
        assert_eq!(ids, ["w1:p1", "w1:p2", "w1:p10"]);
        assert_eq!(pane_number("odd"), u64::MAX);
    }

    #[test]
    fn root_key_is_stable_short_and_follows_the_real_path() {
        let tmp = std::env::temp_dir();
        assert_eq!(
            crate::workspace::root_key(&tmp.join(".")),
            crate::workspace::root_key(&tmp)
        );
        assert_ne!(
            crate::workspace::root_key(Path::new("/a/b")),
            crate::workspace::root_key(Path::new("/a/c"))
        );
        assert_eq!(crate::workspace::root_key(Path::new("/a/b")).len(), 32);
        assert!(root_label(Path::new("/a/b")).len() <= 64);
    }

    #[test]
    fn receipt_keys_are_unique_and_valid_token_names() {
        let keys: std::collections::HashSet<_> = (0..1000).map(|_| receipt_key()).collect();
        assert_eq!(keys.len(), 1000);
        assert!(keys.iter().all(|key| {
            key.len() <= 32
                && key
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        }));
    }

    /// What the fake's `agent.prompt` does.
    #[derive(Clone, Copy, PartialEq)]
    enum Prompt {
        Accept,
        /// Delivered, but the reply is lost.
        DropAfterDelivery,
        /// Not delivered and no reply: Chimera restarting before the send.
        DropBeforeDelivery,
        Stall,
    }

    /// Which `pane.report_metadata` writes fail.
    #[derive(Clone, Copy, PartialEq)]
    enum Fault {
        None,
        /// Writes that remove a token.
        Removals,
        /// Writes that confirm a delivery.
        Confirmations,
        /// Writes that record a pending send.
        Pendings,
        /// Writes that fold receipts into the count.
        Folds,
        /// Every write.
        All,
    }

    impl Fault {
        fn hits(self, tokens: &serde_json::Map<String, Value>) -> bool {
            let removes = tokens.values().any(Value::is_null);
            let confirms = tokens.values().any(|v| v == DELIVERED);
            let pends = tokens.values().any(|v| v == PENDING);
            let folds = tokens.contains_key(COUNT_KEY);
            match self {
                Fault::None => false,
                Fault::Removals => removes,
                Fault::Confirmations => confirms,
                Fault::Pendings => pends,
                Fault::Folds => folds,
                Fault::All => true,
            }
        }
    }

    /// The state of a fake Herdr. It outlives any `HerdrTerminal`, as Herdr outlives Chimera, and
    /// applies each request atomically like the real server.
    struct World {
        tokens: BTreeMap<String, String>,
        /// The last `seq` applied per metadata source.
        seqs: BTreeMap<String, u64>,
        workspaces: Vec<(String, String)>,
        panes: Vec<(String, String)>,
        delivered: Vec<String>,
        requests: Vec<Value>,
        prompt: Prompt,
        fault: Fault,
        /// Writes that are applied and then get no reply.
        lost: Fault,
        /// The next pending write is applied and its reply is withheld, connection held open.
        hang_pending: bool,
        hung: bool,
        drop_create_reply: bool,
    }

    struct Fake {
        dir: std::path::PathBuf,
        world: Arc<Mutex<World>>,
    }

    impl Fake {
        fn start(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("chimera-herdr-term-{}-{name}", std::process::id()));
            std::fs::create_dir(&dir).unwrap();
            let listener = tokio::net::UnixListener::bind(dir.join("api.sock")).unwrap();
            let world = Arc::new(Mutex::new(World {
                tokens: BTreeMap::new(),
                seqs: BTreeMap::new(),
                workspaces: vec![],
                panes: vec![("w1:p1".into(), "w1".into())],
                delivered: vec![],
                requests: vec![],
                prompt: Prompt::Accept,
                fault: Fault::None,
                lost: Fault::None,
                hang_pending: false,
                hung: false,
                drop_create_reply: false,
            }));
            let shared = world.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let world = shared.clone();
                    tokio::spawn(async move {
                        let mut stream = BufReader::new(stream);
                        let mut line = String::new();
                        stream.read_line(&mut line).await.unwrap();
                        let request: Value = serde_json::from_str(&line).unwrap();
                        let (result, hung) = {
                            let mut world = world.lock().unwrap();
                            let result = world.handle(&request);
                            (result, std::mem::take(&mut world.hung))
                        };
                        if hung {
                            std::future::pending::<()>().await;
                        }
                        if let Some(result) = result {
                            let reply = match result.get("code") {
                                Some(_) => json!({"id": request["id"], "error": result}),
                                None => json!({"id": request["id"], "result": result}),
                            };
                            let _ = stream
                                .get_mut()
                                .write_all(format!("{reply}\n").as_bytes())
                                .await;
                        }
                    });
                }
            });
            Self { dir, world }
        }

        /// A new instance on the same socket: a Chimera restart.
        fn terminal(&self) -> HerdrTerminal {
            HerdrTerminal {
                client: HerdrClient {
                    socket_path: self.dir.join("api.sock"),
                    timeout: std::time::Duration::from_secs(2),
                },
            }
        }

        fn with(&self, change: impl FnOnce(&mut World)) {
            change(&mut self.world.lock().unwrap());
        }

        fn methods(&self) -> Vec<String> {
            let world = self.world.lock().unwrap();
            let methods = world.requests.iter();
            methods
                .map(|r| r["method"].as_str().unwrap().to_owned())
                .collect()
        }

        fn delivered(&self) -> Vec<String> {
            self.world.lock().unwrap().delivered.clone()
        }
    }

    impl World {
        fn handle(&mut self, request: &Value) -> Option<Value> {
            self.requests.push(request.clone());
            let params = &request["params"];
            Some(match request["method"].as_str().unwrap() {
                "workspace.create" => {
                    let id = format!("w{}", self.workspaces.len() + 2);
                    let label = params["label"].as_str().unwrap_or_default().to_owned();
                    self.workspaces.push((id.clone(), label));
                    self.panes.push((format!("{id}:p1"), id.clone()));
                    if std::mem::take(&mut self.drop_create_reply) {
                        return None;
                    }
                    json!({"type":"workspace_created","workspace":{"workspace_id":id},
                        "root_pane":{"pane_id":format!("{id}:p1")}})
                }
                "workspace.list" => {
                    let list = self.workspaces.iter();
                    let list: Vec<_> = list
                        .map(|(id, label)| json!({"workspace_id": id, "label": label}))
                        .collect();
                    json!({"type":"workspace_list","workspaces":list})
                }
                "workspace.close" => {
                    let id = params["workspace_id"].as_str().unwrap();
                    self.workspaces.retain(|(w, _)| w != id);
                    self.panes.retain(|(_, w)| w != id);
                    json!({"type":"ok"})
                }
                "pane.list" => {
                    let list = self.panes.iter();
                    let list: Vec<_> = list
                        .map(|(id, ws)| json!({"pane_id": id, "workspace_id": ws}))
                        .collect();
                    json!({"type":"pane_list","panes":list})
                }
                "pane.get" => {
                    let id = params["pane_id"].as_str().unwrap();
                    let Some((_, ws)) = self.panes.iter().find(|(p, _)| p == id) else {
                        return Some(json!({"code":"pane_not_found","message":"m"}));
                    };
                    json!({"type":"pane_info","pane":{"pane_id":id,"workspace_id":ws,
                        "tokens":self.tokens}})
                }
                "pane.split" => json!({"type":"pane_info","pane":{
                    "pane_id":"w1:p2","workspace_id":"w1"}}),
                "pane.report_metadata" => {
                    let tokens = params["tokens"].as_object().unwrap();
                    let source = params["source"].as_str().unwrap().to_owned();
                    let seq = params["seq"].as_u64();
                    // Like Herdr, a report not newer than the source's last is dropped silently.
                    if seq
                        .is_some_and(|seq| self.seqs.get(&source).is_some_and(|last| seq <= *last))
                    {
                        return Some(json!({"type":"ok"}));
                    }
                    let faulty = self.fault.hits(tokens);
                    let mut merged = self.tokens.clone();
                    for (key, value) in tokens {
                        match value.as_str() {
                            Some(value) => merged.insert(key.clone(), value.to_owned()),
                            None => merged.remove(key),
                        };
                    }
                    if faulty || merged.len() > 32 {
                        return Some(json!({"code":"metadata_token_limit","message":"m"}));
                    }
                    self.tokens = merged;
                    if let Some(seq) = seq {
                        self.seqs.insert(source, seq);
                    }
                    if self.lost.hits(tokens) {
                        return None;
                    }
                    if self.hang_pending && tokens.values().any(|v| v == PENDING) {
                        self.hang_pending = false;
                        self.hung = true;
                        return None;
                    }
                    json!({"type":"ok"})
                }
                "agent.prompt" => {
                    let text = params["text"].as_str().unwrap().to_owned();
                    if text == "bad" {
                        return Some(json!({"code":"agent_blocked","message":"m"}));
                    }
                    match self.prompt {
                        Prompt::DropBeforeDelivery => return None,
                        Prompt::DropAfterDelivery => {
                            self.delivered.push(text);
                            return None;
                        }
                        Prompt::Stall => {
                            self.delivered.push(text);
                            return Some(json!({"code":"agent_prompt_stalled","message":"m"}));
                        }
                        Prompt::Accept => self.delivered.push(text),
                    }
                    json!({"type":"agent_prompted","agent":{"agent_status":"working"}})
                }
                other => panic!("unexpected {other}"),
            })
        }
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn pane(id: &str) -> PaneId {
        PaneId::new(id).unwrap()
    }

    fn ws(id: &str) -> WorkspaceId {
        WorkspaceId::new(id).unwrap()
    }

    #[tokio::test]
    async fn split_with_a_pane_from_another_workspace_is_failed_and_changes_nothing() {
        let fake = Fake::start("split-mismatch");
        let error = fake
            .terminal()
            .split_pane(&ws("w2"), &pane("w1:p1"))
            .await
            .unwrap_err();
        assert!(error.is_failed());
        assert_eq!(fake.methods(), ["pane.get"]);
    }

    #[tokio::test]
    async fn split_within_the_workspace_splits() {
        let fake = Fake::start("split-ok");
        let new = fake
            .terminal()
            .split_pane(&ws("w1"), &pane("w1:p1"))
            .await
            .unwrap();
        assert_eq!(new, pane("w1:p2"));
        assert_eq!(fake.methods(), ["pane.get", "pane.split"]);
    }

    #[tokio::test]
    async fn created_workspace_is_found_again_by_a_new_instance() {
        let fake = Fake::start("find");
        let dir = std::env::temp_dir();
        let (workspace, root) = fake.terminal().create_workspace(&dir).await.unwrap();
        let (found, panes) = fake.terminal().find_workspace(&dir).await.unwrap().unwrap();
        assert_eq!((found, panes), (workspace, vec![root]));
        assert!(
            fake.terminal()
                .find_workspace(Path::new("/nowhere/else"))
                .await
                .unwrap()
                .is_none()
        );
        // Identity is set by the creating request alone.
        assert_eq!(
            fake.methods(),
            [
                "workspace.create",
                "workspace.list",
                "pane.list",
                "workspace.list"
            ]
        );
    }

    #[tokio::test]
    async fn workspace_whose_create_reply_was_lost_is_still_found() {
        let fake = Fake::start("create-lost");
        let dir = std::env::temp_dir();
        fake.with(|world| world.drop_create_reply = true);
        let error = fake.terminal().create_workspace(&dir).await.unwrap_err();
        assert!(error.is_uncertain());
        // After a restart the workspace is recovered from Herdr alone.
        let (workspace, panes) = fake.terminal().find_workspace(&dir).await.unwrap().unwrap();
        assert_eq!(workspace, ws("w2"));
        assert_eq!(panes, [pane("w2:p1")]);
    }

    #[tokio::test]
    async fn find_matches_only_the_exact_root_and_the_first_such_workspace() {
        let fake = Fake::start("find-exact");
        let dir = std::env::temp_dir();
        fake.with(|world| {
            world.workspaces = vec![
                ("w2".into(), "scratch".into()),
                ("w3".into(), format!("{}x", root_label(&dir))),
                ("w4".into(), root_label(&dir)),
                ("w5".into(), root_label(&dir)),
            ];
            world.panes = vec![("w4:p1".into(), "w4".into())];
        });
        let (workspace, _) = fake.terminal().find_workspace(&dir).await.unwrap().unwrap();
        assert_eq!(workspace, ws("w4"));
    }

    /// Sends `prompt` from a fresh instance.
    async fn send(fake: &Fake, prompt: &str) -> Result<(), PortError> {
        fake.terminal().send_prompt(&pane("w1:p1"), prompt).await
    }

    /// Reads the count from a fresh instance.
    async fn received(fake: &Fake) -> Result<u64, PortError> {
        fake.terminal().prompts_received(&pane("w1:p1")).await
    }

    #[tokio::test]
    async fn delivered_prompts_are_counted_by_any_instance() {
        let fake = Fake::start("sent");
        assert_eq!(received(&fake).await.unwrap(), 0);
        send(&fake, "one").await.unwrap();
        send(&fake, "two").await.unwrap();
        assert_eq!(received(&fake).await.unwrap(), 2);
        assert_eq!(fake.delivered(), ["one", "two"]);
    }

    #[tokio::test]
    async fn lost_reply_leaves_the_receipt_uncertain_after_a_restart() {
        let fake = Fake::start("lost");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.prompt = Prompt::DropAfterDelivery);
        assert!(send(&fake, "two").await.unwrap_err().is_uncertain());
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn restart_before_the_send_does_not_confirm_the_prompt() {
        // The receipt was written and the process died before `agent.prompt` took effect.
        let fake = Fake::start("restart");
        fake.with(|world| world.prompt = Prompt::DropBeforeDelivery);
        assert!(send(&fake, "one").await.unwrap_err().is_uncertain());
        assert!(fake.delivered().is_empty());
        fake.with(|world| world.prompt = Prompt::Accept);
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn stalled_prompt_is_uncertain_to_readers() {
        let fake = Fake::start("stalled");
        fake.with(|world| world.prompt = Prompt::Stall);
        assert!(send(&fake, "one").await.unwrap_err().is_uncertain());
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn rejected_prompt_is_failed_and_leaves_no_receipt() {
        let fake = Fake::start("rejected");
        send(&fake, "one").await.unwrap();
        assert!(send(&fake, "bad").await.unwrap_err().is_failed());
        assert_eq!(received(&fake).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn rejected_prompt_whose_receipt_cannot_be_removed_is_uncertain_to_readers() {
        let fake = Fake::start("rollback-fails");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.fault = Fault::Removals);
        assert!(send(&fake, "bad").await.unwrap_err().is_uncertain());
        fake.with(|world| world.fault = Fault::None);
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn delivery_whose_confirmation_is_lost_is_uncertain_not_counted() {
        let fake = Fake::start("confirm-fails");
        fake.with(|world| world.fault = Fault::Confirmations);
        assert!(send(&fake, "one").await.unwrap_err().is_uncertain());
        assert_eq!(fake.delivered(), ["one"]);
        fake.with(|world| world.fault = Fault::None);
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    fn pending_tokens(fake: &Fake) -> usize {
        let world = fake.world.lock().unwrap();
        world.tokens.values().filter(|v| *v == PENDING).count()
    }

    #[tokio::test]
    async fn pending_write_applied_but_unanswered_is_cleaned_up_and_nothing_is_sent() {
        let fake = Fake::start("pending-lost");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.lost = Fault::Pendings);
        assert!(send(&fake, "two").await.unwrap_err().is_uncertain());
        fake.with(|world| world.lost = Fault::None);
        assert_eq!(fake.delivered(), ["one"]);
        assert_eq!(pending_tokens(&fake), 0);
        assert_eq!(received(&fake).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn pending_write_applied_but_unanswered_with_failed_cleanup_is_uncertain() {
        let fake = Fake::start("pending-lost-cleanup-fails");
        send(&fake, "one").await.unwrap();
        fake.with(|world| {
            world.lost = Fault::Pendings;
            world.fault = Fault::Removals;
        });
        assert!(send(&fake, "two").await.unwrap_err().is_uncertain());
        fake.with(|world| {
            world.lost = Fault::None;
            world.fault = Fault::None;
        });
        assert_eq!(fake.delivered(), ["one"]);
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn pending_and_cleanup_both_applied_but_unanswered_leave_no_receipt() {
        let fake = Fake::start("all-lost");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.lost = Fault::All);
        assert!(send(&fake, "two").await.unwrap_err().is_uncertain());
        fake.with(|world| world.lost = Fault::None);
        assert_eq!(fake.delivered(), ["one"]);
        assert_eq!(received(&fake).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn confirmation_applied_but_unanswered_still_counts_the_delivery() {
        let fake = Fake::start("confirm-lost");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.lost = Fault::Confirmations);
        assert!(send(&fake, "two").await.unwrap_err().is_uncertain());
        fake.with(|world| world.lost = Fault::None);
        assert_eq!(fake.delivered(), ["one", "two"]);
        assert_eq!(received(&fake).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn removal_applied_but_unanswered_after_a_rejection_does_not_count_it() {
        let fake = Fake::start("removal-lost");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.lost = Fault::Removals);
        assert!(send(&fake, "bad").await.unwrap_err().is_uncertain());
        fake.with(|world| world.lost = Fault::None);
        assert_eq!(fake.delivered(), ["one"]);
        assert_eq!(received(&fake).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn send_cancelled_after_its_pending_receipt_but_before_the_prompt_is_uncertain() {
        let fake = Arc::new(Fake::start("cancelled"));
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.hang_pending = true);
        let task = {
            let fake = fake.clone();
            tokio::spawn(async move { send(&fake, "two").await })
        };
        // The pending receipt is persisted while its reply is still outstanding.
        while pending_tokens(&fake) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let prompts = fake
            .methods()
            .iter()
            .filter(|m| *m == "agent.prompt")
            .count();
        assert_eq!(prompts, 1, "only the first send issued agent.prompt");
        assert_eq!(fake.delivered(), ["one"]);
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn prompt_is_not_sent_when_its_receipt_cannot_be_recorded() {
        let fake = Fake::start("record-fails");
        fake.with(|world| world.fault = Fault::All);
        let error = send(&fake, "one").await.unwrap_err();
        assert!(error.is_failed());
        assert!(fake.delivered().is_empty());
        assert_eq!(received(&fake).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_full_pane_refuses_prompts_before_sending() {
        let fake = Fake::start("full");
        fake.with(|world| {
            for n in 0..32 {
                world.tokens.insert(format!("other{n}"), "x".into());
            }
        });
        assert!(send(&fake, "one").await.unwrap_err().is_failed());
        assert!(fake.delivered().is_empty());
    }

    #[tokio::test]
    async fn concurrent_sends_from_separate_instances_are_all_counted() {
        let fake = Arc::new(Fake::start("concurrent"));
        // Three rounds deliver more prompts than a pane has tokens, while the sends' folds race.
        for round in 1..=3 {
            let mut sends = tokio::task::JoinSet::new();
            for n in 0..24 {
                let fake = fake.clone();
                // Rejected sends roll their receipts back while the others confirm theirs.
                let prompt = if n % 3 == 0 {
                    "bad".to_owned()
                } else {
                    format!("p{n}")
                };
                sends.spawn(async move { send(&fake, &prompt).await });
            }
            let mut accepted = 0;
            while let Some(result) = sends.join_next().await {
                accepted += u64::from(result.unwrap().is_ok());
            }
            assert_eq!(accepted, 16);
            assert_eq!(received(&fake).await.unwrap(), 16 * round);
            assert_eq!(fake.delivered().len() as u64, 16 * round);
        }
    }

    fn token_count(fake: &Fake) -> usize {
        fake.world.lock().unwrap().tokens.len()
    }

    #[tokio::test]
    async fn a_pane_receives_more_prompts_than_it_has_tokens() {
        let fake = Fake::start("many");
        for n in 1..=100 {
            send(&fake, &format!("p{n}")).await.unwrap();
            // A fresh instance reads every confirmed delivery.
            assert_eq!(received(&fake).await.unwrap(), n);
            assert!(token_count(&fake) <= 2);
        }
        assert_eq!(fake.delivered().len(), 100);
    }

    #[tokio::test]
    async fn a_pane_full_of_confirmed_receipts_is_folded_and_keeps_its_count() {
        let fake = Fake::start("full-of-receipts");
        fake.with(|world| {
            for n in 0..32 {
                let key = format!("{RECEIPT_PREFIX}{n}");
                world.tokens.insert(key, DELIVERED.into());
            }
        });
        send(&fake, "one").await.unwrap();
        assert_eq!(received(&fake).await.unwrap(), 33);
        assert_eq!(token_count(&fake), 2);
    }

    #[tokio::test]
    async fn fold_applied_but_unanswered_keeps_the_count() {
        let fake = Fake::start("fold-lost");
        fake.with(|world| world.lost = Fault::Folds);
        for n in 1..=40 {
            send(&fake, &format!("p{n}")).await.unwrap();
            assert_eq!(received(&fake).await.unwrap(), n);
        }
        assert!(token_count(&fake) <= 2);
    }

    #[tokio::test]
    async fn failing_folds_never_change_the_count_or_block_a_send_with_room() {
        let fake = Fake::start("fold-fails");
        send(&fake, "one").await.unwrap();
        fake.with(|world| world.fault = Fault::Folds);
        send(&fake, "two").await.unwrap();
        assert_eq!(received(&fake).await.unwrap(), 2);
        fake.with(|world| world.fault = Fault::None);
        send(&fake, "three").await.unwrap();
        assert_eq!(received(&fake).await.unwrap(), 3);
        assert_eq!(token_count(&fake), 2);
    }

    #[tokio::test]
    async fn a_fold_based_on_a_stale_read_changes_nothing() {
        let fake = Fake::start("fold-stale");
        send(&fake, "one").await.unwrap();
        send(&fake, "two").await.unwrap();
        send(&fake, "three").await.unwrap();
        assert_eq!(received(&fake).await.unwrap(), 3);
        // The fold of the third send applied version 2 and removed the second's receipt. One
        // that read the same version before the second confirmed would set a lower count.
        let count = fake.world.lock().unwrap().tokens[COUNT_KEY].clone();
        assert_eq!(count, "2:2");
        let stale: Value = fake
            .terminal()
            .client
            .request(
                "pane.report_metadata",
                &json!({"pane_id": "w1:p1", "source": METADATA_SOURCE, "seq": 2,
                    "tokens": {COUNT_KEY: "2:1"}}),
                "ok",
                Effect::Change,
            )
            .await
            .unwrap();
        assert_eq!(stale["type"], "ok");
        assert_eq!(received(&fake).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn a_malformed_count_is_uncertain() {
        let fake = Fake::start("malformed");
        fake.with(|world| {
            world.tokens.insert(COUNT_KEY.into(), "x".into());
        });
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn a_read_during_an_unsettled_send_is_uncertain() {
        let fake = Fake::start("in-flight");
        send(&fake, "one").await.unwrap();
        fake.with(|world| {
            world
                .tokens
                .insert(format!("{RECEIPT_PREFIX}other"), PENDING.into());
        });
        assert!(received(&fake).await.unwrap_err().is_uncertain());
    }

    #[tokio::test]
    async fn terminal_is_usable_as_a_trait_object_and_rejects_relative_directories() {
        // A port that cannot reach Herdr still validates input before sending anything.
        let terminal = HerdrTerminal {
            client: HerdrClient {
                socket_path: "/nonexistent/chimera.sock".into(),
                timeout: std::time::Duration::from_secs(1),
            },
        };
        let terminal: Arc<dyn Terminal> = Arc::new(terminal);
        let error = terminal
            .find_workspace(Path::new("relative"))
            .await
            .unwrap_err();
        assert!(error.is_failed());
    }
}
