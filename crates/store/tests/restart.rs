//! Restart behavior of the file system store, through `dyn RunStore` only.

use std::sync::Arc;

use chimera_core::run_store::{EffectRecord, RunStore};
use chimera_core::{AgentId, Outcome, Role, RunId, TurnOutcome, TurnResult};
use chimera_store::FileRunStore;
use serde_json::json;

const LOCK: &str = "merge_lock:spec/8-store";

fn run() -> RunId {
    RunId::new("run-1").unwrap()
}

fn open(root: &tempfile::TempDir) -> Arc<dyn RunStore> {
    Arc::new(FileRunStore::with_root(root.path()).unwrap())
}

fn turn(explanation: &str) -> TurnResult {
    TurnResult {
        agent: AgentId::new("a1").unwrap(),
        role: Role::Review,
        outcome: TurnOutcome::Valid(Outcome::ReviewApproved(explanation.into())),
    }
}

fn invalid_turn() -> TurnResult {
    TurnResult {
        agent: AgentId::new("a2").unwrap(),
        role: Role::Implementation,
        outcome: TurnOutcome::Invalid {
            output: "no handoff\n```\n## fake".into(),
            problem: "missing handoff".into(),
        },
    }
}

fn run_data() -> serde_json::Value {
    json!({
        "input": {"repository": "/work/repo", "future_field": [1, null]},
        "ticket_plan": {"tickets": []},
        "unknown": {"kept": true},
    })
}

fn lock_state() -> serde_json::Value {
    // Sequence numbers deliberately differ from queue order.
    json!({
        "holder": {"owner": "impl-1", "sequence": 1},
        "queue": [{"owner": "impl-3", "sequence": 7}, {"owner": "impl-2", "sequence": 3}],
        "next_sequence": 8,
    })
}

#[tokio::test]
async fn restart_restores_everything_in_order() {
    let root = tempfile::tempdir().unwrap();
    let id = run();
    let store = open(&root);

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
    store
        .save_pipeline_state(&id, LOCK, lock_state())
        .await
        .unwrap();
    let turns = [turn("first"), invalid_turn(), turn("third")];
    for t in &turns {
        store.append_turn(&id, t.clone()).await.unwrap();
    }
    drop(store);

    let store = open(&root);

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
    assert_eq!(store.load_history(&id).await.unwrap(), turns);
    let log = std::fs::read_to_string(root.path().join("run-1/history.md")).unwrap();
    // One timestamped heading per turn; the "## fake" line is turn output inside a fence.
    assert_eq!(log.lines().filter(|l| l.starts_with("## 20")).count(), 3);
}

#[tokio::test]
async fn restart_restores_merge_lock_holder_and_fifo_waiters() {
    let root = tempfile::tempdir().unwrap();
    let id = run();
    let store = open(&root);
    store
        .save_pipeline_state(&id, LOCK, json!({"holder": null, "queue": []}))
        .await
        .unwrap();
    store
        .save_pipeline_state(&id, LOCK, lock_state())
        .await
        .unwrap();
    drop(store);

    let store = open(&root);

    assert_eq!(
        store.load_pipeline_state(&id, LOCK).await.unwrap(),
        Some(lock_state())
    );
    assert_eq!(
        store
            .load_pipeline_state(&id, "merge_lock:other")
            .await
            .unwrap(),
        None
    );
    assert!(root.path().join("run-1/merge-lock.json").is_file());
}

#[tokio::test]
async fn failed_history_append_survives_restart_and_retry_records_once() {
    let root = tempfile::tempdir().unwrap();
    let id = run();
    let history = root.path().join("run-1/history.md");
    let store = open(&root);
    store.append_turn(&id, turn("first")).await.unwrap();
    std::fs::remove_file(&history).unwrap();
    // A directory in place of the log makes the append fail.
    std::fs::create_dir(&history).unwrap();

    assert!(store.append_turn(&id, turn("second")).await.is_err());
    std::fs::remove_dir(&history).unwrap();
    drop(store);

    let store = open(&root);
    assert!(store.load_history(&id).await.unwrap().is_empty());
    store.append_turn(&id, turn("second")).await.unwrap();

    assert_eq!(store.load_history(&id).await.unwrap(), [turn("second")]);
}

#[tokio::test]
async fn pipeline_ids_with_separators_and_reserved_looking_names_do_not_collide() {
    let root = tempfile::tempdir().unwrap();
    let id = run();
    let store = open(&root);
    store.record_effect_intent(&id, "k", "run").await.unwrap();
    for name in ["_effects", "a/b", "../escape"] {
        store
            .save_pipeline_state(&id, name, json!(name))
            .await
            .unwrap();
    }
    drop(store);

    let store = open(&root);

    for name in ["_effects", "a/b", "../escape"] {
        assert_eq!(
            store.load_pipeline_state(&id, name).await.unwrap(),
            Some(json!(name))
        );
    }
    assert_eq!(store.load_effects(&id).await.unwrap().len(), 1);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}
