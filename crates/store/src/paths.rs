use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use chimera_core::RunId;

use crate::StoreError;

/// The directory holding all runs: `$XDG_STATE_HOME/chimera/runs`, falling back to
/// `~/.local/state/chimera/runs`.
pub(crate) fn state_root() -> Result<PathBuf, StoreError> {
    resolve_state_root(std::env::var_os("XDG_STATE_HOME"), std::env::var_os("HOME"))
}

/// The directory of one run: `<root>/<run-id>/`. Fails when the run ID is not a single plain
/// path component, so it cannot escape or alias the root.
pub(crate) fn run_directory(root: &Path, run: &RunId) -> Result<PathBuf, StoreError> {
    let id = run.as_str();
    if is_plain_component(id) {
        Ok(root.join(id))
    } else {
        Err(StoreError::InvalidRunId {
            root: root.to_path_buf(),
            id: id.to_string(),
        })
    }
}

/// Whether `id` is a single plain path component.
pub(crate) fn is_plain_component(id: &str) -> bool {
    let mut components = Path::new(id).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(name)), None) if name == id
    )
}

/// An environment value that names an absolute path; empty and relative values are ignored, as the
/// XDG specification requires, so the root can never resolve against the current directory.
fn absolute(value: Option<OsString>) -> Option<PathBuf> {
    value
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

fn resolve_state_root(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf, StoreError> {
    let base = match absolute(xdg_state_home) {
        Some(xdg) => xdg,
        None => absolute(home)
            .ok_or(StoreError::StateRootUnresolved)?
            .join(".local/state"),
    };
    Ok(base.join("chimera").join("runs"))
}

/// Checks that `root` can hold run directories: it must be absolute, free of `..` components and
/// outside every git repository and worktree, so agents working there cannot read the runs.
pub(crate) fn validate_root(root: &Path) -> Result<(), StoreError> {
    let invalid = |reason| StoreError::InvalidRoot {
        root: root.to_path_buf(),
        reason,
    };
    if !root.is_absolute() {
        return Err(invalid("it must be an absolute path"));
    }
    if root.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(invalid("it must not contain `..`"));
    }
    // Resolve symlinks of the part that exists, so a link into a repository is seen as inside it.
    let mut existing = root;
    let mut missing = Vec::new();
    while !existing.exists() {
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name);
                existing = parent;
            }
            _ => break,
        }
    }
    let mut resolved = existing
        .canonicalize()
        .map_err(|e| StoreError::io(existing, e))?;
    resolved.extend(missing.into_iter().rev());
    // A repository or worktree has a `.git` directory or file at its top.
    match resolved.ancestors().find(|dir| dir.join(".git").exists()) {
        Some(repository) => Err(StoreError::RootInRepository {
            root: root.to_path_buf(),
            repository: repository.to_path_buf(),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(value: &str) -> Option<OsString> {
        Some(OsString::from(value))
    }

    #[test]
    fn uses_xdg_state_home() {
        assert_eq!(
            resolve_state_root(os("/xdg"), os("/home/me")).unwrap(),
            PathBuf::from("/xdg/chimera/runs")
        );
    }

    #[test]
    fn falls_back_to_home_when_xdg_unset_or_empty() {
        let expected = PathBuf::from("/home/me/.local/state/chimera/runs");
        assert_eq!(resolve_state_root(None, os("/home/me")).unwrap(), expected);
        assert_eq!(
            resolve_state_root(os(""), os("/home/me")).unwrap(),
            expected
        );
    }

    #[test]
    fn ignores_relative_xdg_state_home() {
        assert_eq!(
            resolve_state_root(os("relative-state"), os("/home/me")).unwrap(),
            PathBuf::from("/home/me/.local/state/chimera/runs")
        );
        assert!(resolve_state_root(os("relative-state"), None).is_err());
    }

    #[test]
    fn rejects_relative_home() {
        for home in ["home", ".", "../x"] {
            let error = resolve_state_root(None, os(home)).unwrap_err();
            assert!(matches!(error, StoreError::StateRootUnresolved), "{home}");
        }
    }

    #[test]
    fn fails_when_nothing_is_known() {
        for (xdg, home) in [(None, None), (os(""), os(""))] {
            let error = resolve_state_root(xdg, home).unwrap_err();
            assert!(matches!(error, StoreError::StateRootUnresolved));
        }
    }

    #[test]
    fn run_directory_is_under_root() {
        let run = RunId::new("run-1").unwrap();
        assert_eq!(
            run_directory(Path::new("/root"), &run).unwrap(),
            PathBuf::from("/root/run-1")
        );
    }

    #[test]
    fn run_directory_rejects_ids_that_are_not_a_single_component() {
        for id in [
            "/outside",
            "../outside",
            "..",
            ".",
            "a/b",
            "a/",
            "./a",
            "a/..",
            "a/../b",
        ] {
            let run = RunId::new(id).unwrap();
            let error = run_directory(Path::new("/root"), &run).unwrap_err();
            assert!(matches!(error, StoreError::InvalidRunId { .. }), "{id}");
            let text = error.to_string();
            assert!(text.contains("/root") && text.contains(id), "{text}");
            assert!(chimera_core::error::PortError::from(error).is_failed());
        }
    }

    #[test]
    fn root_outside_any_repository_is_valid() {
        let outside = tempfile::tempdir().unwrap();
        validate_root(outside.path()).unwrap();
        validate_root(&outside.path().join("not/yet/created")).unwrap();
    }

    #[test]
    fn relative_roots_and_parent_components_are_rejected() {
        let outside = tempfile::tempdir().unwrap();
        for root in [
            PathBuf::from("relative-state"),
            PathBuf::from("."),
            PathBuf::new(),
            outside.path().join("a/../b"),
        ] {
            let error = validate_root(&root).unwrap_err();
            assert!(matches!(error, StoreError::InvalidRoot { .. }), "{root:?}");
        }
    }

    #[test]
    fn roots_inside_a_repository_or_worktree_are_rejected() {
        let repository = tempfile::tempdir().unwrap();
        std::fs::create_dir(repository.path().join(".git")).unwrap();
        let worktree = tempfile::tempdir().unwrap();
        // A linked worktree has a `.git` file instead of a directory.
        std::fs::write(worktree.path().join(".git"), "gitdir: /elsewhere\n").unwrap();

        for top in [repository.path(), worktree.path()] {
            for root in [top.to_path_buf(), top.join("runs"), top.join("a/b/c")] {
                let error = validate_root(&root).unwrap_err();
                assert!(
                    matches!(error, StoreError::RootInRepository { .. }),
                    "{root:?}"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlink_into_a_repository_is_rejected() {
        let repository = tempfile::tempdir().unwrap();
        std::fs::create_dir(repository.path().join(".git")).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let link = outside.path().join("link");
        std::os::unix::fs::symlink(repository.path(), &link).unwrap();

        let error = validate_root(&link.join("runs")).unwrap_err();

        assert!(matches!(error, StoreError::RootInRepository { .. }));
    }
}
