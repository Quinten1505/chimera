use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chimera_core::run_store::EffectRecord;
use serde::{Deserialize, Serialize};

use crate::StoreError;
use crate::atomic::write_json_atomic;

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

/// File stem of the run-level effect records. It starts with a `%` that is not followed by two
/// hex digits, which [`encode_id`] never produces, so no pipeline id can map to it.
const RUN_EFFECTS_STEM: &str = "%run-effects";

/// Name shown in errors for the run-level effect records.
const RUN_EFFECTS_LABEL: &str = "<run>";

/// Encodes a pipeline id as a file stem: ASCII letters, digits, `-` and `_` stay, every other
/// byte becomes `%XX`. The mapping is injective and never yields a path separator or a leading dot.
fn encode_id(id: &str) -> String {
    let mut stem = String::with_capacity(id.len());
    for byte in id.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            stem.push(char::from(byte));
        } else {
            stem.push_str(&format!("%{byte:02X}"));
        }
    }
    stem
}

/// The inverse of [`encode_id`]; `None` for a stem it cannot have produced.
fn decode_id(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            let hex = stem.get(at + 1..at + 3)?;
            if !hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
            {
                return None;
            }
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            at += 3;
        } else {
            decoded.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn pipeline_path(run_dir: &Path, pipeline: &str) -> Result<PathBuf, StoreError> {
    if pipeline.is_empty() {
        return Err(StoreError::InvalidPipelineId {
            id: pipeline.to_string(),
        });
    }
    Ok(pipelines_directory(run_dir).join(format!("{}.json", encode_id(pipeline))))
}

fn run_effects_path(run_dir: &Path) -> PathBuf {
    pipelines_directory(run_dir).join(format!("{RUN_EFFECTS_STEM}.json"))
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
pub(crate) fn load_state(
    run_dir: &Path,
    pipeline: &str,
) -> Result<Option<serde_json::Value>, StoreError> {
    let path = pipeline_path(run_dir, pipeline)?;
    Ok(read_file(&path)?.and_then(|file| file.state))
}

/// Saves the intent of effect `key` of `pipeline` before it runs. Fails when `key` already has a
/// record.
#[allow(dead_code)]
pub(crate) fn record_intent(
    run_dir: &Path,
    pipeline: &str,
    key: &str,
    intent: &str,
) -> Result<(), StoreError> {
    record_intent_at(&pipeline_path(run_dir, pipeline)?, pipeline, key, intent)
}

/// Saves the outcome of effect `key` of `pipeline` after it finished. Fails when no intent was
/// recorded.
#[allow(dead_code)]
pub(crate) fn record_outcome(
    run_dir: &Path,
    pipeline: &str,
    key: &str,
    outcome: &str,
) -> Result<(), StoreError> {
    record_outcome_at(&pipeline_path(run_dir, pipeline)?, pipeline, key, outcome)
}

/// The effects of `pipeline` in the order their intents were recorded, each classified.
#[allow(dead_code)]
pub(crate) fn load_effects(
    run_dir: &Path,
    pipeline: &str,
) -> Result<Vec<LoadedEffect>, StoreError> {
    load_effects_at(&pipeline_path(run_dir, pipeline)?)
}

/// [`record_intent`] for the effects of the run itself, which belong to no pipeline instance.
pub(crate) fn record_run_intent(run_dir: &Path, key: &str, intent: &str) -> Result<(), StoreError> {
    record_intent_at(&run_effects_path(run_dir), RUN_EFFECTS_LABEL, key, intent)
}

/// [`record_outcome`] for the effects of the run itself.
pub(crate) fn record_run_outcome(
    run_dir: &Path,
    key: &str,
    outcome: &str,
) -> Result<(), StoreError> {
    record_outcome_at(&run_effects_path(run_dir), RUN_EFFECTS_LABEL, key, outcome)
}

/// [`load_effects`] for the effects of the run itself.
pub(crate) fn load_run_effects(run_dir: &Path) -> Result<Vec<LoadedEffect>, StoreError> {
    load_effects_at(&run_effects_path(run_dir))
}

fn record_intent_at(path: &Path, owner: &str, key: &str, intent: &str) -> Result<(), StoreError> {
    let mut file = read_file(path)?.unwrap_or_default();
    if file.effects.iter().any(|effect| effect.key == key) {
        return Err(StoreError::EffectAlreadyRecorded {
            pipeline: owner.to_string(),
            key: key.to_string(),
        });
    }
    file.effects.push(EffectRecord {
        key: key.to_string(),
        intent: intent.to_string(),
        outcome: None,
    });
    write_file(path, &file)
}

fn record_outcome_at(path: &Path, owner: &str, key: &str, outcome: &str) -> Result<(), StoreError> {
    let mut file = read_file(path)?.unwrap_or_default();
    let record = file
        .effects
        .iter_mut()
        .find(|effect| effect.key == key)
        .ok_or_else(|| StoreError::EffectNotRecorded {
            pipeline: owner.to_string(),
            key: key.to_string(),
        })?;
    record.outcome = Some(outcome.to_string());
    write_file(path, &file)
}

fn load_effects_at(path: &Path) -> Result<Vec<LoadedEffect>, StoreError> {
    let effects = read_file(path)?
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

/// Ids of all pipeline instances with a saved file, sorted. The run-level effect records are not
/// a pipeline instance.
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
            .and_then(decode_id)
        {
            ids.push(id);
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
    fn empty_pipeline_id_is_rejected() {
        let run = dir();
        let error = save_state(run.path(), "", json!(1)).unwrap_err();
        assert!(matches!(error, StoreError::InvalidPipelineId { .. }));
    }

    #[test]
    fn ids_with_separators_stay_inside_the_pipelines_directory() {
        let run = dir();
        let ids = [
            "../x",
            "a/b",
            "..",
            "/abs",
            ".",
            "merge_lock:spec/8-store",
            "ü",
            "%41",
        ];
        for (n, id) in ids.iter().enumerate() {
            save_state(run.path(), id, json!(n)).unwrap();
        }
        for (n, id) in ids.iter().enumerate() {
            assert_eq!(load_state(run.path(), id).unwrap(), Some(json!(n)), "{id}");
        }
        let files = fs::read_dir(run.path().join("pipelines")).unwrap().count();
        assert_eq!(files, ids.len());
        assert_eq!(fs::read_dir(run.path()).unwrap().count(), 1);
        let mut listed = list_pipelines(run.path()).unwrap();
        listed.sort();
        let mut expected: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        expected.sort();
        assert_eq!(listed, expected);
    }

    #[test]
    fn id_encoding_round_trips() {
        for id in [
            "a",
            "a.b",
            "%",
            "%25",
            "x y",
            "é/√",
            "_effects",
            "%run-effects",
        ] {
            assert_eq!(decode_id(&encode_id(id)).as_deref(), Some(id));
        }
        assert_eq!(decode_id(RUN_EFFECTS_STEM), None);
    }

    #[test]
    fn run_effects_do_not_collide_with_any_pipeline_id() {
        let run = dir();
        record_run_intent(run.path(), "k", "run intent").unwrap();
        for id in ["_effects", "%run-effects", "effects", "<run>"] {
            save_state(run.path(), id, json!(id)).unwrap();
            record_intent(run.path(), id, "k", id).unwrap();
        }

        let run_effects = load_run_effects(run.path()).unwrap();
        assert_eq!(run_effects.len(), 1);
        assert_eq!(run_effects[0].record.intent, "run intent");
        for id in ["_effects", "%run-effects", "effects", "<run>"] {
            assert_eq!(load_state(run.path(), id).unwrap(), Some(json!(id)));
            assert_eq!(load_effects(run.path(), id).unwrap()[0].record.intent, id);
        }
        assert_eq!(
            list_pipelines(run.path()).unwrap(),
            ["%run-effects", "<run>", "_effects", "effects"]
        );
        record_run_outcome(run.path(), "k", "done").unwrap();
        assert_eq!(
            load_run_effects(run.path()).unwrap()[0].status,
            EffectStatus::Completed
        );
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
