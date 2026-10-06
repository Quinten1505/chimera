use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::StoreError;

/// Writes `value` as JSON to `path`: serialize, write and flush a temporary file in the same
/// directory, rename it over the target, then flush the directory so the rename survives a crash.
/// On a failure before the rename the previous content of `path` stays intact and the temporary
/// file is removed; when only flushing the directory fails, `path` already holds the new content
/// but may revert to the previous one after a crash, reported as [`StoreError::Unsynced`].
pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    write_json_atomic_with(
        path,
        value,
        &mut std::iter::repeat_with(|| COUNTER.fetch_add(1, Ordering::Relaxed)),
        &mut sync_directory,
    )
}

/// Flushes the entries of `directory`, such as a file created or renamed in it, to disk.
pub(crate) fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// Creates `directory` and its missing ancestors like [`fs::create_dir_all`], flushing the parent
/// of each directory so that its entry survives a crash. A directory that already exists may have
/// been left by a process that died before flushing its parent, so its parent is flushed too, once
/// per process.
pub(crate) fn create_dir_all_durable(directory: &Path) -> Result<(), StoreError> {
    static CONFIRMED: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
    let mut confirmed = CONFIRMED.lock().unwrap_or_else(|e| e.into_inner());
    create_dir_all_with(directory, &mut confirmed, &mut sync_directory)
}

/// `confirmed` holds the directories whose entries were flushed; `sync` flushes a directory.
fn create_dir_all_with(
    directory: &Path,
    confirmed: &mut BTreeSet<PathBuf>,
    sync: &mut dyn FnMut(&Path) -> io::Result<()>,
) -> Result<(), StoreError> {
    if confirmed.contains(directory) && directory.is_dir() {
        return Ok(());
    }
    // A file system root is in no directory.
    let Some(parent) = directory.parent() else {
        return Ok(());
    };
    create_dir_all_with(parent, confirmed, sync)?;
    match fs::create_dir(directory) {
        Ok(()) => {}
        Err(_) if directory.is_dir() => {}
        Err(e) => return Err(StoreError::io(directory, e)),
    }
    sync(parent).map_err(|e| StoreError::io(parent, e))?;
    confirmed.insert(directory.to_path_buf());
    Ok(())
}

/// Reads and deserializes the JSON file at `path`; `None` when the file does not exist.
pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, StoreError> {
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

/// `suffixes` supplies the candidate temporary-name suffixes, in order; `sync` flushes a directory.
fn write_json_atomic_with<T: Serialize>(
    path: &Path,
    value: &T,
    suffixes: &mut dyn Iterator<Item = u64>,
    sync: &mut dyn FnMut(&Path) -> io::Result<()>,
) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| StoreError::Serialize {
        path: path.to_path_buf(),
        source,
    })?;

    let (mut file, temporary) = create_temporary(path, suffixes)?;
    let result = write_and_rename(&mut file, &temporary, path, &bytes);
    drop(file);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    let directory = path.parent().expect("a file path has a parent");
    sync(directory).map_err(|source| StoreError::Unsynced {
        path: path.to_path_buf(),
        source,
    })
}

/// Exclusively creates a uniquely named temporary file next to `path`. An existing entry at a
/// candidate name (file, symlink, ...) is never opened or removed; the next name is tried.
fn create_temporary(
    path: &Path,
    suffixes: &mut dyn Iterator<Item = u64>,
) -> Result<(File, PathBuf), StoreError> {
    for suffix in suffixes {
        let mut name = OsString::from(".");
        name.push(path.file_name().unwrap_or_default());
        name.push(format!(".{}.{suffix}.tmp", std::process::id()));
        let temporary = path.with_file_name(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((file, temporary)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(StoreError::io(temporary, e)),
        }
    }
    unreachable!("the suffix iterator is infinite")
}

fn write_and_rename(
    file: &mut File,
    temporary: &Path,
    path: &Path,
    bytes: &[u8],
) -> Result<(), StoreError> {
    file.write_all(bytes)
        .map_err(|e| StoreError::io(temporary, e))?;
    file.sync_all().map_err(|e| StoreError::io(temporary, e))?;
    fs::rename(temporary, path).map_err(|e| StoreError::io(path, e))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::ser::{Error as _, Serializer};

    use super::*;

    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("refuses to serialize"))
        }
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn creates_and_replaces_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        write_json_atomic(&path, &BTreeMap::from([("a", 1)])).unwrap();
        write_json_atomic(&path, &BTreeMap::from([("b", 2)])).unwrap();

        let read: BTreeMap<String, i32> =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(read, BTreeMap::from([("b".to_string(), 2)]));
        assert_eq!(entries(dir.path()), ["state.json"]);
    }

    #[test]
    fn serialization_failure_keeps_previous_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "previous").unwrap();

        let error = write_json_atomic(&path, &Unserializable).unwrap_err();

        assert!(matches!(error, StoreError::Serialize { .. }));
        assert!(error.to_string().contains("state.json"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "previous");
        assert_eq!(entries(dir.path()), ["state.json"]);
    }

    #[test]
    fn rename_failure_removes_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        // A non-empty directory at the target makes the final rename fail.
        let path = dir.path().join("state.json");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("keep"), "previous").unwrap();

        let error = write_json_atomic(&path, &1).unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert_eq!(fs::read_to_string(path.join("keep")).unwrap(), "previous");
        assert_eq!(entries(dir.path()), ["state.json"]);
    }

    #[test]
    fn missing_directory_is_an_io_error_naming_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("state.json");

        let error = write_json_atomic(&path, &1).unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert!(error.to_string().contains("missing"));
    }

    #[cfg(unix)]
    #[test]
    fn read_only_directory_keeps_previous_content() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "previous").unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o555)).unwrap();

        let result = write_json_atomic(&path, &1);

        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
        // Permission bits are not enforced for privileged users.
        if result.is_err() {
            assert_eq!(fs::read_to_string(&path).unwrap(), "previous");
            assert_eq!(entries(dir.path()), ["state.json"]);
        }
    }

    #[test]
    fn concurrent_writes_leave_one_complete_value_and_no_temporaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        std::thread::scope(|scope| {
            for i in 0..16 {
                let path = &path;
                scope.spawn(move || {
                    for j in 0..25 {
                        write_json_atomic(path, &vec![i * 100 + j; 1000]).unwrap();
                    }
                });
            }
        });

        let read: Vec<i32> = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(read.len(), 1000);
        assert!(read.iter().all(|v| *v == read[0]));
        assert_eq!(entries(dir.path()), ["state.json"]);
    }

    #[test]
    fn directory_is_synced_after_the_rename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "previous").unwrap();
        let mut synced = Vec::new();

        write_json_atomic_with(&path, &7, &mut (0..), &mut |directory| {
            // The rename is what the sync must persist, so it has already happened.
            synced.push((directory.to_path_buf(), fs::read_to_string(&path).unwrap()));
            assert_eq!(entries(dir.path()), ["state.json"]);
            Ok(())
        })
        .unwrap();

        assert_eq!(synced, [(dir.path().to_path_buf(), "7".to_string())]);
    }

    #[test]
    fn directory_sync_failure_is_unsynced_and_does_not_claim_the_previous_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "previous").unwrap();

        let error = write_json_atomic_with(&path, &7, &mut (0..), &mut |_| {
            Err(io::Error::other("disk on fire"))
        })
        .unwrap_err();

        assert!(matches!(error, StoreError::Unsynced { .. }));
        assert!(error.to_string().contains("state.json"));
        assert!(error.to_string().contains("may not survive a crash"));
        // The new content is in place; only its durability is unknown.
        assert_eq!(fs::read_to_string(&path).unwrap(), "7");
        assert_eq!(entries(dir.path()), ["state.json"]);
    }

    #[test]
    fn writes_before_the_rename_do_not_sync_the_directory_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("keep"), "previous").unwrap();
        let mut synced = 0;

        let error = write_json_atomic_with(&path, &1, &mut (0..), &mut |_| {
            synced += 1;
            Ok(())
        })
        .unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert_eq!(synced, 0);
    }

    /// Records the directories `create_dir_all_with` flushes, below `base` only.
    fn create_recording(
        directory: &Path,
        confirmed: &mut BTreeSet<PathBuf>,
        base: &Path,
    ) -> Result<Vec<PathBuf>, StoreError> {
        let mut synced = Vec::new();
        create_dir_all_with(directory, confirmed, &mut |parent| {
            if parent.starts_with(base) {
                synced.push(parent.to_path_buf());
            }
            Ok(())
        })?;
        Ok(synced)
    }

    #[test]
    fn created_directories_are_synced_into_their_parents_in_creation_order() {
        let dir = tempfile::tempdir().unwrap();
        let deepest = dir.path().join("a/b/c");
        let mut synced = Vec::new();
        let mut confirmed = BTreeSet::new();

        create_dir_all_with(&deepest, &mut confirmed, &mut |parent| {
            if parent.starts_with(dir.path()) {
                let created: Vec<_> = fs::read_dir(parent).unwrap().collect();
                assert_eq!(
                    created.len(),
                    1,
                    "the new entry exists before its parent is synced"
                );
            }
            synced.push(parent.to_path_buf());
            Ok(())
        })
        .unwrap();

        assert!(deepest.is_dir());
        // Every ancestor's entry is flushed too, from the file system root down.
        let ancestors: Vec<_> = deepest.ancestors().skip(1).collect();
        let expected: Vec<_> = ancestors.into_iter().rev().collect();
        assert_eq!(synced, expected);

        // Directories already flushed by this process are not flushed again.
        create_dir_all_with(&deepest, &mut confirmed, &mut |_| {
            panic!("all are confirmed")
        })
        .unwrap();
        create_dir_all_durable(&deepest).unwrap();
    }

    #[test]
    fn directories_left_unsynced_by_an_interrupted_process_are_synced() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("a/b"));
        // A process created `a/b` and died before flushing their parents.
        fs::create_dir_all(&b).unwrap();

        let synced = create_recording(&b, &mut BTreeSet::new(), dir.path()).unwrap();
        assert_eq!(synced, [dir.path().to_path_buf(), a.clone()]);

        let synced = create_recording(&b.join("c"), &mut BTreeSet::new(), dir.path()).unwrap();
        assert_eq!(synced, [dir.path().to_path_buf(), a, b]);
    }

    #[test]
    fn failed_directory_sync_is_retried_by_the_next_call() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().join("run-1");
        let mut confirmed = BTreeSet::new();

        let error = create_dir_all_with(&run, &mut confirmed, &mut |parent| {
            if parent == dir.path() {
                return Err(io::Error::other("disk on fire"));
            }
            Ok(())
        })
        .unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert!(!confirmed.contains(&run));
        let synced = create_recording(&run, &mut confirmed, dir.path()).unwrap();
        assert_eq!(synced, [dir.path().to_path_buf()]);
        assert!(confirmed.contains(&run));
    }

    #[test]
    fn a_confirmed_directory_removed_since_is_created_again() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().join("run-1");
        let mut confirmed = BTreeSet::new();
        create_recording(&run, &mut confirmed, dir.path()).unwrap();
        fs::remove_dir(&run).unwrap();

        let synced = create_recording(&run, &mut confirmed, dir.path()).unwrap();

        assert!(run.is_dir());
        assert_eq!(synced, [dir.path().to_path_buf()]);
    }

    #[cfg(unix)]
    #[test]
    fn preexisting_temporary_entries_are_preserved_and_skipped() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, "previous").unwrap();
        let pid = std::process::id();
        let squat_file = dir.path().join(format!(".state.json.{pid}.0.tmp"));
        let squat_link = dir.path().join(format!(".state.json.{pid}.1.tmp"));
        fs::write(&squat_file, "squatter").unwrap();
        symlink(&path, &squat_link).unwrap();

        write_json_atomic_with(&path, &7, &mut (0..), &mut sync_directory).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "7");
        assert_eq!(fs::read_to_string(&squat_file).unwrap(), "squatter");
        assert_eq!(fs::read_link(&squat_link).unwrap(), path);
        assert_eq!(entries(dir.path()).len(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn failure_preserves_preexisting_temporary_entries_and_removes_only_its_own() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("keep"), "previous").unwrap();
        let pid = std::process::id();
        let squat_file = dir.path().join(format!(".state.json.{pid}.0.tmp"));
        let squat_link = dir.path().join(format!(".state.json.{pid}.1.tmp"));
        fs::write(&squat_file, "squatter").unwrap();
        symlink(&path, &squat_link).unwrap();

        let error = write_json_atomic_with(&path, &1, &mut (0..), &mut sync_directory).unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert_eq!(fs::read_to_string(&squat_file).unwrap(), "squatter");
        assert_eq!(fs::read_link(&squat_link).unwrap(), path);
        assert_eq!(fs::read_to_string(path.join("keep")).unwrap(), "previous");
        assert_eq!(entries(dir.path()).len(), 3);
    }
}
