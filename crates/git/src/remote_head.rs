use std::path::Path;

use chimera_core::{BranchName, CommitId};

use crate::error::GitError;
use crate::runner::{Effect, Runner};

/// Reads the commit `branch` points to on `origin` with `git ls-remote`, bypassing local
/// remote-tracking refs. `None` when the remote has no such branch.
pub(crate) fn remote_head(
    runner: &Runner,
    repo: &Path,
    branch: &BranchName,
) -> Result<Option<CommitId>, GitError> {
    let reference = format!("refs/heads/{branch}");
    let output = runner.run(
        repo,
        &["ls-remote", "--heads", "origin", &reference],
        Effect::Read,
    )?;
    Ok(output
        .stdout
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find(|(_, name)| *name == reference)
        .and_then(|(id, _)| CommitId::new(id).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{TestRepo, git, git_output};

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    fn head(repo: &TestRepo, name: &str) -> Option<CommitId> {
        remote_head(&Runner::new(), repo.work(), &branch(name)).unwrap()
    }

    #[test]
    fn head_after_push_matches_pushed_commit() {
        let repo = TestRepo::new();
        git(repo.work(), &["checkout", "-b", "feature"]);
        repo.commit_file(repo.work(), "a.txt", "a");
        git(repo.work(), &["push", "origin", "feature"]);
        let pushed = git_output(repo.work(), &["rev-parse", "HEAD"]);
        assert_eq!(head(&repo, "feature").unwrap().as_str(), pushed);
    }

    #[test]
    fn head_changes_after_another_clone_pushes() {
        let repo = TestRepo::new();
        let before = head(&repo, "main").unwrap();
        let other = repo.clone_origin("other");
        repo.commit_file(&other, "b.txt", "b");
        git(&other, &["push", "origin", "main"]);
        let after = head(&repo, "main").unwrap();
        assert_ne!(before, after);
        assert_eq!(after.as_str(), git_output(&other, &["rev-parse", "HEAD"]));
    }

    #[test]
    fn reads_remote_not_stale_tracking_ref() {
        let repo = TestRepo::new();
        let other = repo.clone_origin("other");
        repo.commit_file(&other, "b.txt", "b");
        git(&other, &["push", "origin", "main"]);
        // The work repository never fetched, so its tracking ref is stale.
        let stale = git_output(repo.work(), &["rev-parse", "origin/main"]);
        assert_ne!(head(&repo, "main").unwrap().as_str(), stale);
    }

    #[test]
    fn missing_branch_is_none() {
        let repo = TestRepo::new();
        assert_eq!(head(&repo, "nope"), None);
    }

    #[test]
    fn similarly_named_branch_is_not_matched() {
        let repo = TestRepo::new();
        git(repo.work(), &["push", "origin", "main:release/main"]);
        assert_eq!(head(&repo, "release"), None);
    }

    #[test]
    fn unreachable_remote_is_failed() {
        let repo = TestRepo::new();
        std::fs::remove_dir_all(repo.origin()).unwrap();
        let error = remote_head(&Runner::new(), repo.work(), &branch("main")).unwrap_err();
        assert!(!error.is_uncertain());
        assert!(chimera_core::error::PortError::from(error).is_failed());
    }
}
