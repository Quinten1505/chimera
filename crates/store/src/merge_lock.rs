use std::collections::BTreeMap;
use std::path::Path;

use serde_json::Value;

use crate::StoreError;
use crate::atomic::{create_dir_all_durable, read_json, write_json_atomic};

const FILE_NAME: &str = "merge-lock.json";

/// Pipeline instance ids with this prefix are merge locks: their state is the queue order and
/// current holder, kept in `merge-lock.json` instead of `pipelines/<id>.json`.
pub(crate) const INSTANCE_PREFIX: &str = "merge_lock:";

/// The contents of `merge-lock.json`: the opaque lock state of each lock instance of the run. The
/// state is never looked inside, so holder, queue order and sequence numbers round-trip as saved.
type Locks = BTreeMap<String, Value>;

/// Replaces the state of the lock `instance` in the run directory's `merge-lock.json` atomically,
/// creating the directory if needed and keeping the other instances.
pub(crate) fn save_lock(run_dir: &Path, instance: &str, state: Value) -> Result<(), StoreError> {
    let path = run_dir.join(FILE_NAME);
    let mut locks: Locks = read_json(&path)?.unwrap_or_default();
    locks.insert(instance.to_string(), state);
    create_dir_all_durable(run_dir)?;
    write_json_atomic(&path, &locks)
}

/// The saved state of the lock `instance`; `None` when none was saved.
pub(crate) fn load_lock(run_dir: &Path, instance: &str) -> Result<Option<Value>, StoreError> {
    let mut locks: Locks = read_json(&run_dir.join(FILE_NAME))?.unwrap_or_default();
    Ok(locks.remove(instance))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;

    fn lock() -> Value {
        // Sequence numbers deliberately differ from queue order.
        json!({
            "holder": {"owner": "a", "sequence": 0},
            "queue": [{"owner": "c", "sequence": 7}, {"owner": "b", "sequence": 3}],
            "next_sequence": 9,
            "unknown": [null],
        })
    }

    #[test]
    fn holder_and_waiter_order_round_trip_in_merge_lock_json() {
        let run = tempfile::tempdir().unwrap();

        save_lock(run.path(), "merge_lock:spec/8-store", lock()).unwrap();

        assert_eq!(
            load_lock(run.path(), "merge_lock:spec/8-store").unwrap(),
            Some(lock())
        );
        assert!(run.path().join("merge-lock.json").is_file());
    }

    #[test]
    fn instances_are_kept_apart_and_replaced_individually() {
        let run = tempfile::tempdir().unwrap();
        save_lock(run.path(), "merge_lock:a", lock()).unwrap();
        save_lock(run.path(), "merge_lock:b", json!(null)).unwrap();
        save_lock(run.path(), "merge_lock:a", json!({"holder": null})).unwrap();

        assert_eq!(
            load_lock(run.path(), "merge_lock:a").unwrap(),
            Some(json!({"holder": null}))
        );
        assert_eq!(
            load_lock(run.path(), "merge_lock:b").unwrap(),
            Some(json!(null))
        );
        assert_eq!(load_lock(run.path(), "merge_lock:c").unwrap(), None);
    }

    #[test]
    fn missing_file_loads_as_none() {
        let run = tempfile::tempdir().unwrap();
        assert_eq!(load_lock(run.path(), "merge_lock:a").unwrap(), None);
    }

    #[test]
    fn corrupt_file_is_a_deserialize_error() {
        let run = tempfile::tempdir().unwrap();
        fs::write(run.path().join("merge-lock.json"), "{").unwrap();

        let error = load_lock(run.path(), "merge_lock:a").unwrap_err();

        assert!(matches!(error, StoreError::Deserialize { .. }));
        assert!(save_lock(run.path(), "merge_lock:a", lock()).is_err());
    }
}
