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

fn non_empty(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|v| !v.is_empty()).map(PathBuf::from)
}

fn resolve_state_root(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf, StoreError> {
    let base = match non_empty(xdg_state_home) {
        Some(xdg) => xdg,
        None => non_empty(home)
            .ok_or(StoreError::StateRootUnresolved)?
            .join(".local/state"),
    };
    Ok(base.join("chimera").join("runs"))
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
}
