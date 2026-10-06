use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chimera_core::run_store::EffectRecord;
use serde::{Deserialize, Serialize};

use crate::StoreError;
use crate::atomic::write_json_atomic;
use crate::paths::is_plain_component;

/// Whether an effect finished, as seen on load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectStatus {
    /// Intent recorded, no outcome: the effect may or may not have happened, so restart checks it.
    Uncertain,
    /// Intent and outcome recorded.
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadedEffect {
    pub record: EffectRecord,
    pub status: EffectStatus,
}

/// Contents of `pipelines/<id>.json`. The state is opaque: the store never looks inside it.
#[derive(Default, Serialize, Deserialize)]
struct PipelineFile {
    /// Absent when no state was saved; a saved JSON `null` is `Some(Null)`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    state: Option<serde_json::Value>,
    effects: Vec<EffectRecord>,
}

/// Deserializes a present field as `Some`, even when it is JSON `null` (plain `Option` maps that to `None`).
fn present_value<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde_json::Value::deserialize(deserializer).map(Some)
}

// The operations below are used by the `RunStore` implementation built on this module.
// Callers serialize the operations on one pipeline instance: each read-modify-write is atomic on
// disk but not exclusive between concurrent writers of the same instance.

fn pipelines_directory(run_dir: &Path) -> PathBuf {
    run_dir.join("pipelines")
}

fn pipeline_path(run_dir: &Path, pipeline: &str) -> Result<PathBuf, StoreError> {
    if !is_plain_component(pipeline) {
        return Err(StoreError::InvalidPipelineId {
            id: pipeline.to_string(),
        });
    }
    Ok(pipelines_directory(run_dir).join(format!("{pipeline}.json")))
}

fn read_file(path: &Path) -> Result<Option<PipelineFile>, StoreError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StoreError::io(path, e)),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| StoreError::Deserialize {
            path: path.to_path_buf(),
            source,
        })
}

fn write_file(path: &Path, file: &PipelineFile) -> Result<(), StoreError> {
    let directory = path.parent().expect("pipeline path has a parent");
    fs::create_dir_all(directory).map_err(|e| StoreError::io(directory, e))?;
    write_json_atomic(path, file)
}

/// Replaces the state of `pipeline`, keeping its effect records.
#[allow(dead_code)]
pub(crate) fn save_state(
    run_dir: &Path,
    pipeline: &str,
    state: serde_json::Value,
) -> Result<(), StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    let mut file = read_file(&path)?.unwrap_or_default();
    file.state = Some(state);
    write_file(&path, &file)
}

/// The saved state of `pipeline`, or `None` when none was saved.
#[allow(dead_code)]
pub(crate) fn load_state(
    run_dir: &Path,
    pipeline: &str,
) -> Result<Option<serde_json::Value>, StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    Ok(read_file(&path)?.and_then(|file| file.state))
}

/// Saves the intent of effect `key` before it runs. Fails when `key` already has a record.
#[allow(dead_code)]
pub(crate) fn record_intent(
    run_dir: &Path,
    pipeline: &str,
    key: &str,
    intent: &str,
) -> Result<(), StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    let mut file = read_file(&path)?.unwrap_or_default();
    if file.effects.iter().any(|effect| effect.key == key) {
        return Err(StoreError::EffectAlreadyRecorded {
            pipeline: pipeline.to_string(),
            key: key.to_string(),
        });
    }
    file.effects.push(EffectRecord {
        key: key.to_string(),
        intent: intent.to_string(),
        outcome: None,
    });
    write_file(&path, &file)
}

/// Saves the outcome of effect `key` after it finished. Fails when no intent was recorded.
#[allow(dead_code)]
pub(crate) fn record_outcome(
    run_dir: &Path,
    pipeline: &str,
    key: &str,
    outcome: &str,
) -> Result<(), StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    let mut file = read_file(&path)?.unwrap_or_default();
    let record = file
        .effects
        .iter_mut()
        .find(|effect| effect.key == key)
        .ok_or_else(|| StoreError::EffectNotRecorded {
            pipeline: pipeline.to_string(),
            key: key.to_string(),
        })?;
    record.outcome = Some(outcome.to_string());
    write_file(&path, &file)
}

/// The effects of `pipeline` in the order their intents were recorded, each classified.
#[allow(dead_code)]
pub(crate) fn load_effects(
    run_dir: &Path,
    pipeline: &str,
) -> Result<Vec<LoadedEffect>, StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    let effects = read_file(&path)?
        .map(|file| file.effects)
        .unwrap_or_default();
    Ok(effects
        .into_iter()
        .map(|record| LoadedEffect {
            status: if record.outcome.is_some() {
                EffectStatus::Completed
            } else {
                EffectStatus::Uncertain
            },
            record,
        })
        .collect())
}

/// Ids of all pipeline instances with a saved file, sorted.
#[allow(dead_code)]
pub(crate) fn list_pipelines(run_dir: &Path) -> Result<Vec<String>, StoreError> {
    let directory = pipelines_directory(run_dir);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(StoreError::io(&directory, e)),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| StoreError::io(&directory, e))?;
        // Temporary files of in-flight writes are dot-prefixed and end in `.tmp`.
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_suffix(".json"))
        {
            ids.push(id.to_string());
        }
    }
    ids.sort();
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn state_round_trips_in_pipelines_directory() {
        let run = dir();
        let state = json!({"step": "Review", "counters": {"cycles": 2}, "extra": [1, null]});

        save_state(run.path(), "impl-1", state.clone()).unwrap();

        assert_eq!(load_state(run.path(), "impl-1").unwrap(), Some(state));
        assert!(run.path().join("pipelines/impl-1.json").is_file());
    }

    #[test]
    fn unknown_pipeline_is_not_found() {
        let run = dir();
        assert_eq!(load_state(run.path(), "nope").unwrap(), None);
        save_state(run.path(), "other", json!(1)).unwrap();
        assert_eq!(load_state(run.path(), "nope").unwrap(), None);
    }

    #[test]
    fn overwrite_keeps_only_latest_state() {
        let run = dir();
        save_state(run.path(), "p", json!({"step": 1, "old": true})).unwrap();
        save_state(run.path(), "p", json!({"step": 2})).unwrap();

        assert_eq!(
            load_state(run.path(), "p").unwrap(),
            Some(json!({"step": 2}))
        );
    }

    #[test]
    fn intent_only_effect_loads_as_uncertain() {
        let run = dir();
        record_intent(run.path(), "p", "merge-pr", "merge #5").unwrap();

        let effects = load_effects(run.path(), "p").unwrap();

        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].record.intent, "merge #5");
        assert_eq!(effects[0].record.outcome, None);
        assert_eq!(effects[0].status, EffectStatus::Uncertain);
    }

    #[test]
    fn intent_and_outcome_load_as_completed() {
        let run = dir();
        record_intent(run.path(), "p", "merge-pr", "merge #5").unwrap();
        record_outcome(run.path(), "p", "merge-pr", "merged abc").unwrap();

        let effects = load_effects(run.path(), "p").unwrap();

        assert_eq!(effects[0].record.outcome.as_deref(), Some("merged abc"));
        assert_eq!(effects[0].status, EffectStatus::Completed);
    }

    #[test]
    fn effects_are_saved_before_returning_and_survive_state_saves() {
        let run = dir();
        record_intent(run.path(), "p", "a", "first").unwrap();
        // Read straight from disk: the record is already there.
        let raw: serde_json::Value =
            serde_json::from_slice(&fs::read(run.path().join("pipelines/p.json")).unwrap())
                .unwrap();
        assert_eq!(raw["effects"][0]["key"], "a");

        save_state(run.path(), "p", json!({"step": 1})).unwrap();
        record_intent(run.path(), "p", "b", "second").unwrap();
        save_state(run.path(), "p", json!({"step": 2})).unwrap();

        let keys: Vec<_> = load_effects(run.path(), "p")
            .unwrap()
            .into_iter()
            .map(|e| e.record.key)
            .collect();
        assert_eq!(keys, ["a", "b"]);
        assert_eq!(
            load_state(run.path(), "p").unwrap(),
            Some(json!({"step": 2}))
        );
    }

    #[test]
    fn effects_alone_do_not_make_state_exist() {
        let run = dir();
        record_intent(run.path(), "p", "a", "first").unwrap();
        assert_eq!(load_state(run.path(), "p").unwrap(), None);
    }

    #[test]
    fn null_state_round_trips_distinct_from_absent() {
        let run = dir();
        save_state(run.path(), "p", serde_json::Value::Null).unwrap();

        assert_eq!(
            load_state(run.path(), "p").unwrap(),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn overwriting_with_null_replaces_previous_state() {
        let run = dir();
        save_state(run.path(), "p", json!({"step": 1})).unwrap();
        save_state(run.path(), "p", serde_json::Value::Null).unwrap();

        assert_eq!(
            load_state(run.path(), "p").unwrap(),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn null_state_survives_intent_and_outcome_writes() {
        let run = dir();
        save_state(run.path(), "p", serde_json::Value::Null).unwrap();
        record_intent(run.path(), "p", "k", "i").unwrap();
        record_outcome(run.path(), "p", "k", "o").unwrap();

        assert_eq!(
            load_state(run.path(), "p").unwrap(),
            Some(serde_json::Value::Null)
        );
        assert_eq!(load_effects(run.path(), "p").unwrap().len(), 1);
    }

    #[test]
    fn duplicate_intent_and_orphan_outcome_fail_without_changing_records() {
        let run = dir();
        record_intent(run.path(), "p", "a", "first").unwrap();

        let duplicate = record_intent(run.path(), "p", "a", "again").unwrap_err();
        let orphan = record_outcome(run.path(), "p", "missing", "x").unwrap_err();

        assert!(matches!(
            duplicate,
            StoreError::EffectAlreadyRecorded { .. }
        ));
        assert!(matches!(orphan, StoreError::EffectNotRecorded { .. }));
        let effects = load_effects(run.path(), "p").unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].record.intent, "first");
    }

    #[test]
    fn listing_returns_all_saved_instances() {
        let run = dir();
        assert!(list_pipelines(run.path()).unwrap().is_empty());
        save_state(run.path(), "b", json!(1)).unwrap();
        save_state(run.path(), "a", json!(2)).unwrap();
        record_intent(run.path(), "c", "k", "i").unwrap();
        save_state(run.path(), "a", json!(3)).unwrap();

        assert_eq!(list_pipelines(run.path()).unwrap(), ["a", "b", "c"]);
    }

    #[test]
    fn invalid_pipeline_ids_are_rejected() {
        let run = dir();
        for id in ["../x", "a/b", "..", "/abs", ""] {
            let error = save_state(run.path(), id, json!(1)).unwrap_err();
            assert!(
                matches!(error, StoreError::InvalidPipelineId { .. }),
                "{id}"
            );
        }
    }

    #[test]
    fn corrupt_file_is_a_deserialize_error_naming_the_file() {
        let run = dir();
        fs::create_dir(run.path().join("pipelines")).unwrap();
        fs::write(run.path().join("pipelines/p.json"), "{not json").unwrap();

        let error = load_state(run.path(), "p").unwrap_err();

        assert!(matches!(error, StoreError::Deserialize { .. }));
        assert!(error.to_string().contains("p.json"));
    }
}
