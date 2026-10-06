use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::StoreError;

/// Writes `value` as JSON to `path`: serialize, write and flush a temporary file in the same
/// directory, then rename it over the target. On any failure the previous content of `path`
/// stays intact and the temporary file is removed.
// Used by the operation tickets that build on this scaffold.
#[allow(dead_code)]
pub(crate) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| StoreError::Serialize {
        path: path.to_path_buf(),
        source,
    })?;

    let temporary = temporary_path(path);
    let result = write_and_rename(&temporary, path, &bytes);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = std::ffi::OsString::from(".");
    name.push(path.file_name().unwrap_or_default());
    name.push(format!(".{}.tmp", std::process::id()));
    path.with_file_name(name)
}

fn write_and_rename(temporary: &Path, path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = File::create(temporary).map_err(|e| StoreError::io(temporary, e))?;
    file.write_all(bytes)
        .map_err(|e| StoreError::io(temporary, e))?;
    file.sync_all().map_err(|e| StoreError::io(temporary, e))?;
    drop(file);
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
}
