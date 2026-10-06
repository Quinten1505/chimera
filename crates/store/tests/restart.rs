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

/// What a reader of the rendered `history.md` sees: the text of each level-2 heading and of each
/// code block, in order. Nothing inside an HTML comment is among them.
fn rendered(markdown: &str) -> (Vec<String>, Vec<String>) {
    use pulldown_cmark::{Event, HeadingLevel, Parser, Tag, TagEnd};

    let (mut headings, mut blocks) = (Vec::new(), Vec::new());
    let mut current: Option<String> = None;
    for event in Parser::new(markdown) {
        match event {
            Event::Start(Tag::Heading {
                level: HeadingLevel::H2,
                ..
            })
            | Event::Start(Tag::CodeBlock(_)) => current = Some(String::new()),
            Event::Text(text) => {
                if let Some(current) = &mut current {
                    current.push_str(&text);
                }
            }
            Event::End(TagEnd::Heading(HeadingLevel::H2)) => headings.extend(current.take()),
            Event::End(TagEnd::CodeBlock) => blocks.extend(current.take()),
            _ => {}
        }
    }
    (headings, blocks)
}

/// The bytes of the entry that appending `turn` after `before` adds to `history.md`.
async fn entry_bytes(turn: &TurnResult, before: &[TurnResult]) -> Vec<u8> {
    let root = tempfile::tempdir().unwrap();
    let store = open(&root);
    for t in before.iter().chain([turn]) {
        store.append_turn(&run(), t.clone()).await.unwrap();
    }
    std::fs::read(root.path().join("run-1/history.md")).unwrap()
}

/// The turn whose append is interrupted: fenced output with its own backticks, and multibyte
/// characters in every field.
fn torn_turn() -> TurnResult {
    TurnResult {
        agent: AgentId::new("a3").unwrap(),
        role: Role::Implementation,
        outcome: TurnOutcome::Invalid {
            output: "h\u{e9}llo \u{1F600}\n```\n## fake\n\u{4e2d}\u{6587}".into(),
            problem: "pr\u{f6}blem \u{1F980}".into(),
        },
    }
}

/// Starts a store on a `history.md` holding `bytes`: the entry of `committed`, then what
/// interrupted appends left. Only `committed` loads; retrying the torn turn and appending another
/// records both, and both are visible when the Markdown is rendered.
async fn assert_recovers(bytes: Vec<u8>, committed: &TurnResult, label: &str) {
    let root = tempfile::tempdir().unwrap();
    let id = run();
    std::fs::create_dir_all(root.path().join("run-1")).unwrap();
    // The interrupted entry may end in a partial character.
    let (torn_headings, torn_blocks) = rendered(&String::from_utf8_lossy(&bytes));
    std::fs::write(root.path().join("run-1/history.md"), bytes).unwrap();

    let store = open(&root);
    assert_eq!(
        store.load_history(&id).await.unwrap(),
        std::slice::from_ref(committed),
        "{label}"
    );
    let torn = torn_turn();
    store.append_turn(&id, torn.clone()).await.unwrap();
    store.append_turn(&id, turn("last")).await.unwrap();
    drop(store);

    let expected = [committed.clone(), torn.clone(), turn("last")];
    assert_eq!(
        open(&root).load_history(&id).await.unwrap(),
        expected,
        "{label}"
    );

    let bytes = std::fs::read(root.path().join("run-1/history.md")).unwrap();
    let (headings, blocks) = rendered(&String::from_utf8_lossy(&bytes));
    assert_eq!(headings.len(), torn_headings.len() + 2, "{label}");
    assert!(headings[headings.len() - 2].ends_with(" · Implementation"));
    assert!(headings[headings.len() - 1].ends_with(" · Review"));
    let TurnOutcome::Invalid { output, .. } = &torn.outcome else {
        unreachable!()
    };
    assert_eq!(blocks.len(), torn_blocks.len() + 2, "{label}");
    assert_eq!(
        blocks[blocks.len() - 2..],
        [format!("{output}\n"), "last\n".to_string()],
        "{label}"
    );
}

#[tokio::test]
async fn append_interrupted_at_any_byte_is_not_loaded_and_later_appends_are() {
    let committed = turn("first");
    let prefix = entry_bytes(&committed, &[]).await;
    let full = entry_bytes(&torn_turn(), std::slice::from_ref(&committed)).await;
    assert!(full.starts_with(&prefix));
    let tail = &full[prefix.len()..];

    for cut in 0..tail.len() {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&tail[..cut]);
        assert_recovers(bytes, &committed, &format!("cut {cut}")).await;
    }
}

#[tokio::test]
async fn append_interrupted_while_recovering_an_interrupted_append_is_recovered_too() {
    let committed = turn("first");
    let prefix = entry_bytes(&committed, &[]).await;
    let full = entry_bytes(&torn_turn(), std::slice::from_ref(&committed)).await;
    let tail = &full[prefix.len()..];

    for cut in 0..tail.len() {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&tail[..cut]);
        // What the next append writes: the repair of the interrupted entry, then its own entry.
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("run-1")).unwrap();
        let history = root.path().join("run-1/history.md");
        std::fs::write(&history, &bytes).unwrap();
        open(&root).append_turn(&run(), torn_turn()).await.unwrap();
        let appended = std::fs::read(&history).unwrap()[bytes.len()..].to_vec();
        let repair = appended.len() - tail.len();

        // That append is interrupted within the repair, and the next one within its own repair.
        for first in 1..repair {
            let mut once = bytes.clone();
            once.extend_from_slice(&appended[..first]);
            assert_recovers(once.clone(), &committed, &format!("cut {cut}, {first}")).await;
            std::fs::write(&history, &once).unwrap();
            open(&root).append_turn(&run(), torn_turn()).await.unwrap();
            let appended = std::fs::read(&history).unwrap()[once.len()..].to_vec();
            for second in 1..appended.len() - tail.len() {
                let mut twice = once.clone();
                twice.extend_from_slice(&appended[..second]);
                let label = format!("cut {cut}, {first}, {second}");
                assert_recovers(twice, &committed, &label).await;
            }
        }
    }
}
