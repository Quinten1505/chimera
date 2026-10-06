use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::StoreError;

/// Writes `value` as JSON to `path`: serialize, write and flush a temporary file in the same
/// directory, then rename it over the target. On any failure the previous content of `path`
/// stays intact and the temporary file is removed.
pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    write_json_atomic_with(
        path,
        value,
        &mut std::iter::repeat_with(|| COUNTER.fetch_add(1, Ordering::Relaxed)),
    )
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

/// `suffixes` supplies the candidate temporary-name suffixes, in order.
fn write_json_atomic_with<T: Serialize>(
    path: &Path,
    value: &T,
    suffixes: &mut dyn Iterator<Item = u64>,
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
    result
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

        write_json_atomic_with(&path, &7, &mut (0..)).unwrap();

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

        let error = write_json_atomic_with(&path, &1, &mut (0..)).unwrap_err();

        assert!(matches!(error, StoreError::Io { .. }));
        assert_eq!(fs::read_to_string(&squat_file).unwrap(), "squatter");
        assert_eq!(fs::read_link(&squat_link).unwrap(), path);
        assert_eq!(fs::read_to_string(path.join("keep")).unwrap(), "previous");
        assert_eq!(entries(dir.path()).len(), 3);
    }
}
