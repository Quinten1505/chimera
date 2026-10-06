use std::fs;
use std::path::Path;

use chimera_core::RunId;
use serde::{Deserialize, Serialize};

use crate::StoreError;
use crate::atomic::{read_json, write_json_atomic};
use crate::paths::run_directory;

const FILE_NAME: &str = "merge-lock.json";

/// A merge lock holder or waiter: the owner key and its enqueue sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeLockEntry {
    pub owner: String,
    pub sequence: u64,
}

/// The contents of `merge-lock.json`. `waiters` are in queue order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeLockState {
    pub holder: Option<MergeLockEntry>,
    pub waiters: Vec<MergeLockEntry>,
}

/// Writes `merge-lock.json` in the run's directory atomically, creating the directory if needed.
#[allow(dead_code)]
pub(crate) fn save_merge_lock(
    root: &Path,
    run: &RunId,
    state: &MergeLockState,
) -> Result<(), StoreError> {
    let directory = run_directory(root, run)?;
    fs::create_dir_all(&directory).map_err(|e| StoreError::io(&directory, e))?;
    write_json_atomic(&directory.join(FILE_NAME), state)
}

/// Reads `merge-lock.json` of `run`; a run without one has an empty lock.
#[allow(dead_code)]
pub(crate) fn load_merge_lock(root: &Path, run: &RunId) -> Result<MergeLockState, StoreError> {
    let path = run_directory(root, run)?.join(FILE_NAME);
    Ok(read_json(&path)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn entry(owner: &str, sequence: u64) -> MergeLockEntry {
        MergeLockEntry {
            owner: owner.into(),
            sequence,
        }
    }

    #[test]
    fn holder_and_waiter_order_round_trip() {
        let root = tempfile::tempdir().unwrap();
        // Sequence numbers deliberately differ from queue order.
        let state = MergeLockState {
            holder: Some(entry("a", 0)),
            waiters: vec![entry("c", 7), entry("b", 3), entry("d", 9)],
        };

        save_merge_lock(root.path(), &run(), &state).unwrap();

        assert_eq!(load_merge_lock(root.path(), &run()).unwrap(), state);
        assert!(root.path().join("run-1/merge-lock.json").is_file());
    }

    #[test]
    fn lock_without_holder_round_trips() {
        let root = tempfile::tempdir().unwrap();
        let state = MergeLockState {
            holder: None,
            waiters: vec![entry("b", 1)],
        };

        save_merge_lock(root.path(), &run(), &state).unwrap();

        assert_eq!(load_merge_lock(root.path(), &run()).unwrap(), state);
    }

    #[test]
    fn missing_file_loads_as_empty_lock() {
        let root = tempfile::tempdir().unwrap();
        let empty = MergeLockState {
            holder: None,
            waiters: vec![],
        };

        assert_eq!(load_merge_lock(root.path(), &run()).unwrap(), empty);

        fs::create_dir(root.path().join("run-1")).unwrap();
        assert_eq!(load_merge_lock(root.path(), &run()).unwrap(), empty);
    }

    #[test]
    fn corrupt_file_is_a_deserialize_error() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("run-1")).unwrap();
        fs::write(root.path().join("run-1/merge-lock.json"), "{").unwrap();

        let error = load_merge_lock(root.path(), &run()).unwrap_err();

        assert!(matches!(error, StoreError::Deserialize { .. }));
    }
}
