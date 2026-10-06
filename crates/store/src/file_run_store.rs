use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use chimera_core::RunId;
use chimera_core::TurnResult;
use chimera_core::error::PortError;
use chimera_core::run_store::{EffectRecord, RunStore};

use crate::history::{HistoryEntry, append_history, load_turns};
use crate::paths::{run_directory, state_root, validate_root};
use crate::{StoreError, merge_lock, pipeline, run_data};

/// [`RunStore`] on the file system: one directory per run under the state root.
///
/// Pipeline instances whose id starts with `merge_lock:` are merge locks and live in
/// `merge-lock.json`; every other instance has its own file under `pipelines/`.
#[derive(Debug, Clone)]
pub struct FileRunStore {
    root: PathBuf,
    /// Serializes operations of this store so read-modify-write updates of a file never overlap.
    lock: Arc<Mutex<()>>,
}

impl FileRunStore {
    /// A store under `$XDG_STATE_HOME/chimera/runs`, or `~/.local/state/chimera/runs`. A relative
    /// `XDG_STATE_HOME` is ignored; the root is never under the current directory or a repository.
    pub fn from_xdg_default() -> Result<Self, StoreError> {
        Self::with_root(state_root()?)
    }

    /// A store whose run directories are created under `root`, which must be an absolute path
    /// outside every git repository and worktree.
    pub fn with_root(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        validate_root(&root)?;
        Ok(Self {
            root,
            lock: Arc::default(),
        })
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

#[async_trait]
impl RunStore for FileRunStore {
    async fn save_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
        state: serde_json::Value,
    ) -> Result<(), PortError> {
        let pipeline = pipeline.to_string();
        self.blocking(run, move |dir| {
            if pipeline.starts_with(merge_lock::INSTANCE_PREFIX) {
                merge_lock::save_lock(dir, &pipeline, state)
            } else {
                pipeline::save_state(dir, &pipeline, state)
            }
        })
        .await
    }

    async fn load_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
    ) -> Result<Option<serde_json::Value>, PortError> {
        let pipeline = pipeline.to_string();
        self.blocking(run, move |dir| {
            if pipeline.starts_with(merge_lock::INSTANCE_PREFIX) {
                merge_lock::load_lock(dir, &pipeline)
            } else {
                pipeline::load_state(dir, &pipeline)
            }
        })
        .await
    }

    async fn save_run_data(&self, run: &RunId, data: serde_json::Value) -> Result<(), PortError> {
        let (root, run_id) = (self.root.clone(), run.clone());
        self.blocking(run, move |_| run_data::save_run_data(&root, &run_id, &data))
            .await
    }

    async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
        let (root, run_id) = (self.root.clone(), run.clone());
        self.blocking(run, move |_| {
            match run_data::load_run_data(&root, &run_id) {
                Ok(data) => Ok(Some(data)),
                Err(StoreError::RunNotFound { .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
        self.blocking(run, move |dir| {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::io(dir, e))?;
            // `history.md` is the only record; the port says neither which pipeline ran the turn
            // nor why, so the entry does not claim to know.
            append_history(
                dir,
                &HistoryEntry {
                    time: SystemTime::now(),
                    pipeline: None,
                    kind: None,
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
            pipeline::record_run_intent(dir, &key, &intent)
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
            pipeline::record_run_outcome(dir, &key, &outcome)
        })
        .await
    }

    async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
        self.blocking(run, |dir| {
            Ok(pipeline::load_run_effects(dir)?
                .into_iter()
                .map(|loaded| loaded.record)
                .collect())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use chimera_core::{AgentId, Outcome, Role, TurnOutcome};
    use serde_json::json;

    use super::*;

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn store(root: &tempfile::TempDir) -> FileRunStore {
        FileRunStore::with_root(root.path()).unwrap()
    }

    fn turn(explanation: &str) -> TurnResult {
        TurnResult {
            agent: AgentId::new("a1").unwrap(),
            role: Role::Review,
            outcome: TurnOutcome::Valid(Outcome::ReviewApproved(explanation.into())),
        }
    }

    #[tokio::test]
    async fn unknown_run_loads_as_empty() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let id = run();

        assert_eq!(store.load_run_data(&id).await.unwrap(), None);
        assert_eq!(store.load_pipeline_state(&id, "p").await.unwrap(), None);
        assert_eq!(
            store
                .load_pipeline_state(&id, "merge_lock:b")
                .await
                .unwrap(),
            None
        );
        assert!(store.load_history(&id).await.unwrap().is_empty());
        assert!(store.load_effects(&id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn effect_misuse_fails_and_keeps_records() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let id = run();
        store.record_effect_intent(&id, "k", "first").await.unwrap();

        let duplicate = store.record_effect_intent(&id, "k", "again").await;
        let orphan = store.record_effect_outcome(&id, "other", "x").await;

        assert!(duplicate.unwrap_err().is_failed());
        assert!(orphan.unwrap_err().is_failed());
        assert_eq!(store.load_effects(&id).await.unwrap()[0].intent, "first");
    }

    #[tokio::test]
    async fn run_data_is_stored_losslessly() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let values = [
            json!({"x": 1}),
            json!(null),
            json!([1, {"a": null}]),
            json!({"input": {"repository": "/r", "future": [1]}, "unknown": true}),
        ];
        for value in values {
            store.save_run_data(&run(), value.clone()).await.unwrap();
            assert_eq!(store.load_run_data(&run()).await.unwrap(), Some(value));
        }
    }

    #[tokio::test]
    async fn caller_ids_never_collide_with_run_effects_or_each_other() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let id = run();
        store
            .record_effect_intent(&id, "k", "run effect")
            .await
            .unwrap();
        let pipelines = [
            "_effects",
            "%run-effects",
            "effects",
            "a/b",
            "a%2Fb",
            "../x",
        ];
        for name in pipelines {
            store
                .save_pipeline_state(&id, name, json!(name))
                .await
                .unwrap();
        }

        for name in pipelines {
            assert_eq!(
                store.load_pipeline_state(&id, name).await.unwrap(),
                Some(json!(name))
            );
        }
        let effects = store.load_effects(&id).await.unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].intent, "run effect");
        // Nothing was written outside the run directory.
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn merge_lock_ids_are_stored_in_merge_lock_json() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let state = json!({"holder": {"owner": "a", "sequence": 0}});

        store
            .save_pipeline_state(&run(), "merge_lock:spec/8-store", state.clone())
            .await
            .unwrap();

        assert!(root.path().join("run-1/merge-lock.json").is_file());
        assert!(!root.path().join("run-1/pipelines").exists());
        assert_eq!(
            store
                .load_pipeline_state(&run(), "merge_lock:spec/8-store")
                .await
                .unwrap(),
            Some(state)
        );
    }

    #[tokio::test]
    async fn invalid_run_ids_fail_without_touching_the_root() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
        let escaping = RunId::new("../outside").unwrap();

        assert!(store.save_run_data(&escaping, json!(1)).await.is_err());
        assert!(
            store
                .save_pipeline_state(&escaping, "p", json!(1))
                .await
                .is_err()
        );
        assert!(
            store
                .save_pipeline_state(&run(), "", json!(1))
                .await
                .unwrap_err()
                .is_failed()
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn failed_append_is_not_loaded_and_a_retry_records_once() {
        let root = tempfile::tempdir().unwrap();
        let id = run();
        let history = root.path().join("run-1/history.md");
        let first = store(&root);
        // A directory in place of the file makes the Markdown write fail.
        std::fs::create_dir_all(&history).unwrap();

        let failed = first.append_turn(&id, turn("once")).await.unwrap_err();
        assert!(failed.is_failed());
        std::fs::remove_dir(&history).unwrap();
        drop(first);

        let restarted = store(&root);
        assert!(restarted.load_history(&id).await.unwrap().is_empty());
        restarted.append_turn(&id, turn("once")).await.unwrap();

        assert_eq!(restarted.load_history(&id).await.unwrap(), [turn("once")]);
        let log = std::fs::read_to_string(&history).unwrap();
        assert_eq!(
            log.matches("\n## ").count() + log.starts_with("## ") as usize,
            1
        );
        assert!(!root.path().join("run-1/turns.jsonl").exists());
    }

    #[tokio::test]
    async fn concurrent_effect_intents_are_all_kept() {
        let root = tempfile::tempdir().unwrap();
        let store = store(&root);
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

    #[test]
    fn constructor_rejects_relative_and_repository_roots() {
        assert!(FileRunStore::with_root("relative-state").is_err());
        let repository = tempfile::tempdir().unwrap();
        std::fs::create_dir(repository.path().join(".git")).unwrap();
        let error = FileRunStore::with_root(repository.path().join("runs")).unwrap_err();
        assert!(matches!(error, StoreError::RootInRepository { .. }));
    }
}
