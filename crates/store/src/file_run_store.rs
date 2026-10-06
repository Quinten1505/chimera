use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use chimera_core::RunId;
use chimera_core::TurnResult;
use chimera_core::error::PortError;
use chimera_core::run_store::{EffectRecord, RunStore};

use crate::history::{HistoryEntry, TurnKind, append_history, append_turn_record, load_turns};
use crate::paths::{run_directory, state_root};
use crate::run_data::{self, RunData};
use crate::{StoreError, pipeline};

/// The pipeline file that holds a run's effect records: the port records effects per run, the
/// store keeps them per pipeline file. The name cannot be used as a real pipeline id.
const EFFECTS_PIPELINE: &str = "_effects";

/// The pipeline name `history.md` shows for turns, which the port does not attribute to one.
const HISTORY_PIPELINE: &str = "-";

/// [`RunStore`] on the file system: one directory per run under the state root.
#[derive(Debug, Clone)]
pub struct FileRunStore {
    root: PathBuf,
    /// Serializes operations of this store so read-modify-write updates of a file never overlap.
    lock: Arc<Mutex<()>>,
}

impl FileRunStore {
    /// A store under `$XDG_STATE_HOME/chimera/runs`, or `~/.local/state/chimera/runs`.
    pub fn from_xdg_default() -> Result<Self, StoreError> {
        Ok(Self::with_root(state_root()?))
    }

    /// A store whose run directories are created under `root`.
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            lock: Arc::default(),
        }
    }

    /// Runs `operation` on a blocking thread with the run's directory.
    async fn blocking<T, F>(&self, run: &RunId, operation: F) -> Result<T, PortError>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<T, StoreError> + Send + 'static,
    {
        let directory = run_directory(&self.root, run)?;
        let lock = Arc::clone(&self.lock);
        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
            operation(&directory)
        })
        .await
        .map_err(|e| PortError::failed(format!("store task failed: {e}")))?
        .map_err(PortError::from)
    }
}

fn check_pipeline(pipeline: &str) -> Result<(), PortError> {
    if pipeline == EFFECTS_PIPELINE {
        return Err(PortError::failed(format!(
            "pipeline id {pipeline:?} is reserved for effect records"
        )));
    }
    Ok(())
}

#[async_trait]
impl RunStore for FileRunStore {
    async fn save_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
        state: serde_json::Value,
    ) -> Result<(), PortError> {
        check_pipeline(pipeline)?;
        let pipeline = pipeline.to_string();
        self.blocking(run, move |dir| pipeline::save_state(dir, &pipeline, state))
            .await
    }

    async fn load_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
    ) -> Result<Option<serde_json::Value>, PortError> {
        check_pipeline(pipeline)?;
        let pipeline = pipeline.to_string();
        self.blocking(run, move |dir| pipeline::load_state(dir, &pipeline))
            .await
    }

    async fn save_run_data(&self, run: &RunId, data: serde_json::Value) -> Result<(), PortError> {
        let data: RunData = serde_json::from_value(data)
            .map_err(|e| PortError::failed(format!("invalid run data: {e}")))?;
        let (root, run_id) = (self.root.clone(), run.clone());
        self.blocking(run, move |_| run_data::save_run_data(&root, &run_id, &data))
            .await
    }

    async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
        let (root, run_id) = (self.root.clone(), run.clone());
        let data = self
            .blocking(run, move |_| {
                match run_data::load_run_data(&root, &run_id) {
                    Ok(data) => Ok(Some(data)),
                    Err(StoreError::RunNotFound { .. }) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await?;
        data.map(|data| {
            serde_json::to_value(data)
                .map_err(|e| PortError::failed(format!("cannot represent run data: {e}")))
        })
        .transpose()
    }

    async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
        self.blocking(run, move |dir| {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::io(dir, e))?;
            // The JSON line is the source for `load_history`; `history.md` is the readable log.
            append_turn_record(dir, &turn)?;
            append_history(
                dir,
                &HistoryEntry {
                    time: SystemTime::now(),
                    pipeline: HISTORY_PIPELINE,
                    kind: TurnKind::Initial,
                    turn: &turn,
                },
            )
        })
        .await
    }

    async fn load_history(&self, run: &RunId) -> Result<Vec<TurnResult>, PortError> {
        self.blocking(run, load_turns).await
    }

    async fn record_effect_intent(
        &self,
        run: &RunId,
        key: &str,
        intent: &str,
    ) -> Result<(), PortError> {
        let (key, intent) = (key.to_string(), intent.to_string());
        self.blocking(run, move |dir| {
            pipeline::record_intent(dir, EFFECTS_PIPELINE, &key, &intent)
        })
        .await
    }

    async fn record_effect_outcome(
        &self,
        run: &RunId,
        key: &str,
        outcome: &str,
    ) -> Result<(), PortError> {
        let (key, outcome) = (key.to_string(), outcome.to_string());
        self.blocking(run, move |dir| {
            pipeline::record_outcome(dir, EFFECTS_PIPELINE, &key, &outcome)
        })
        .await
    }

    async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
        self.blocking(run, |dir| {
            Ok(pipeline::load_effects(dir, EFFECTS_PIPELINE)?
                .into_iter()
                .map(|loaded| loaded.record)
                .collect())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use chimera_core::{
        AgentConfiguration, AgentId, AgentProfile, BranchName, CommitId, Feature, IssueRef,
        IssueStatus, Outcome, Role, Ticket, TicketPlan, TurnOutcome,
    };
    use serde_json::json;

    use super::*;
    use crate::merge_lock::{MergeLockEntry, MergeLockState, load_merge_lock, save_merge_lock};
    use crate::run_data::RunInput;

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("octo", "repo", number).unwrap()
    }

    fn run_data() -> serde_json::Value {
        let data = RunData {
            input: RunInput {
                repository: PathBuf::from("/work/repo"),
                specification: issue(8),
                configuration_file: PathBuf::from("/work/chimera.toml"),
            },
            ticket_plan: TicketPlan {
                tickets: vec![Ticket {
                    issue: issue(56),
                    status: IssueStatus::Open,
                    blockers: vec![],
                }],
            },
            feature: Feature {
                specification: issue(8),
                base_branch: BranchName::new("main").unwrap(),
                feature_branch: BranchName::new("spec/8-store").unwrap(),
                expected_remote_head: CommitId::new("abc123").unwrap(),
                draft_pull_request: issue(9),
            },
            configuration: AgentConfiguration {
                implementation: AgentProfile::new("codex", "m1", "implement"),
                review: AgentProfile::new("codex", "m2", "review"),
                merge: AgentProfile::new("claude", "m3", "merge"),
            },
        };
        serde_json::to_value(data).unwrap()
    }

    fn turn(explanation: &str) -> TurnResult {
        TurnResult {
            agent: AgentId::new("a1").unwrap(),
            role: Role::Review,
            outcome: TurnOutcome::Valid(Outcome::ReviewApproved(explanation.into())),
        }
    }

    fn lock_state() -> MergeLockState {
        let entry = |owner: &str, sequence| MergeLockEntry {
            owner: owner.into(),
            sequence,
        };
        MergeLockState {
            holder: Some(entry("impl-1", 1)),
            waiters: vec![entry("impl-3", 3), entry("impl-2", 2)],
        }
    }

    #[tokio::test]
    async fn restart_restores_everything_in_order() {
        let root = tempfile::tempdir().unwrap();
        let id = run();
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::with_root(root.path()));

        store.save_run_data(&id, run_data()).await.unwrap();
        store
            .save_pipeline_state(&id, "impl-1", json!({"step": "Review"}))
            .await
            .unwrap();
        store
            .save_pipeline_state(&id, "impl-2", json!(null))
            .await
            .unwrap();
        store
            .record_effect_intent(&id, "merge-1", "merge #1")
            .await
            .unwrap();
        store
            .record_effect_outcome(&id, "merge-1", "merged abc")
            .await
            .unwrap();
        store
            .record_effect_intent(&id, "merge-2", "merge #2")
            .await
            .unwrap();
        save_merge_lock(root.path(), &id, &lock_state()).unwrap();
        for explanation in ["first", "second", "third"] {
            store.append_turn(&id, turn(explanation)).await.unwrap();
        }
        drop(store);

        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::with_root(root.path()));

        assert_eq!(store.load_run_data(&id).await.unwrap(), Some(run_data()));
        assert_eq!(
            store.load_pipeline_state(&id, "impl-1").await.unwrap(),
            Some(json!({"step": "Review"}))
        );
        assert_eq!(
            store.load_pipeline_state(&id, "impl-2").await.unwrap(),
            Some(json!(null))
        );
        assert_eq!(
            store.load_effects(&id).await.unwrap(),
            [
                EffectRecord {
                    key: "merge-1".into(),
                    intent: "merge #1".into(),
                    outcome: Some("merged abc".into()),
                },
                EffectRecord {
                    key: "merge-2".into(),
                    intent: "merge #2".into(),
                    outcome: None,
                },
            ]
        );
        assert_eq!(load_merge_lock(root.path(), &id).unwrap(), lock_state());
        assert_eq!(
            store.load_history(&id).await.unwrap(),
            [turn("first"), turn("second"), turn("third")]
        );
        let log = std::fs::read_to_string(root.path().join("run-1/history.md")).unwrap();
        assert_eq!(log.matches("## ").count(), 3);
    }

    #[tokio::test]
    async fn unknown_run_loads_as_empty() {
        let root = tempfile::tempdir().unwrap();
        let store = FileRunStore::with_root(root.path());
        let id = run();

        assert_eq!(store.load_run_data(&id).await.unwrap(), None);
        assert_eq!(store.load_pipeline_state(&id, "p").await.unwrap(), None);
        assert!(store.load_history(&id).await.unwrap().is_empty());
        assert!(store.load_effects(&id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn effect_misuse_fails_and_keeps_records() {
        let root = tempfile::tempdir().unwrap();
        let store = FileRunStore::with_root(root.path());
        let id = run();
        store.record_effect_intent(&id, "k", "first").await.unwrap();

        let duplicate = store.record_effect_intent(&id, "k", "again").await;
        let orphan = store.record_effect_outcome(&id, "other", "x").await;

        assert!(duplicate.unwrap_err().is_failed());
        assert!(orphan.unwrap_err().is_failed());
        assert_eq!(store.load_effects(&id).await.unwrap()[0].intent, "first");
    }

    #[tokio::test]
    async fn invalid_inputs_fail_without_touching_the_root() {
        let root = tempfile::tempdir().unwrap();
        let store = FileRunStore::with_root(root.path());
        let escaping = RunId::new("../outside").unwrap();

        assert!(store.save_run_data(&run(), json!({"x": 1})).await.is_err());
        assert!(
            store
                .save_pipeline_state(&escaping, "p", json!(1))
                .await
                .is_err()
        );
        assert!(
            store
                .save_pipeline_state(&run(), "_effects", json!(1))
                .await
                .unwrap_err()
                .is_failed()
        );
        assert!(
            store
                .save_pipeline_state(&run(), "../p", json!(1))
                .await
                .unwrap_err()
                .is_failed()
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn concurrent_effect_intents_are_all_kept() {
        let root = tempfile::tempdir().unwrap();
        let store = FileRunStore::with_root(root.path());
        let id = run();

        let tasks: Vec<_> = (0..16)
            .map(|n| {
                let (store, id) = (store.clone(), id.clone());
                tokio::spawn(async move {
                    store
                        .record_effect_intent(&id, &format!("k{n}"), "i")
                        .await
                        .unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(store.load_effects(&id).await.unwrap().len(), 16);
    }
}
