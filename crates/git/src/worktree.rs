//! Per-task worktrees: create, remove and prune.

use std::path::{Path, PathBuf};

use crate::error::GitError;
use crate::runner::{Effect, Runner};

/// Creates the worktree for `task_branch` at `path` and returns the path.
///
/// A missing `task_branch` is created from the latest `origin/<feature>` after a fetch; an
/// existing one is reused as is. A worktree already at `path` on `task_branch` is left alone.
pub(crate) fn create(
    runner: &Runner,
    repo: &Path,
    path: &Path,
    task_branch: &str,
    feature: &str,
) -> Result<PathBuf, GitError> {
    let path = &repo.join(path);
    let branch_ref = format!("refs/heads/{task_branch}");
    if path.symlink_metadata().is_ok() {
        let on_task_branch = registered(runner, repo)?.into_iter().any(|worktree| {
            same_path(&worktree.path, path)
                && worktree.branch.as_deref() == Some(branch_ref.as_str())
        });
        if on_task_branch {
            return Ok(path.to_path_buf());
        }
        return Err(GitError::Failed {
            command: "worktree add".to_owned(),
            status: "not run".to_owned(),
            stderr: format!("{} already exists", path.display()),
        });
    }
    let path_arg = path.to_string_lossy();
    if branch_exists(runner, repo, &branch_ref)? {
        runner.run(
            repo,
            &["worktree", "add", &path_arg, task_branch],
            Effect::Local,
        )?;
    } else {
        let refspec = format!("+refs/heads/{feature}:refs/remotes/origin/{feature}");
        runner.run(repo, &["fetch", "origin", &refspec], Effect::Local)?;
        let start = format!("origin/{feature}");
        runner.run(
            repo,
            &[
                "worktree",
                "add",
                "--no-track",
                "-b",
                task_branch,
                &path_arg,
                &start,
            ],
            Effect::Local,
        )?;
    }
    Ok(path.to_path_buf())
}

/// Removes the worktree at `path`, then force-deletes `task_branch`. Either already being gone
/// is not an error.
pub(crate) fn remove(
    runner: &Runner,
    repo: &Path,
    path: &Path,
    task_branch: &str,
) -> Result<(), GitError> {
    let path = &repo.join(path);
    if registered(runner, repo)?
        .iter()
        .any(|worktree| same_path(&worktree.path, path))
    {
        if path.exists() {
            runner.run(
                repo,
                &["worktree", "remove", "--force", &path.to_string_lossy()],
                Effect::Local,
            )?;
        } else {
            prune(runner, repo)?;
        }
    }
    let branch_ref = format!("refs/heads/{task_branch}");
    if branch_exists(runner, repo, &branch_ref)? {
        runner.run(repo, &["branch", "-D", task_branch], Effect::Local)?;
    }
    Ok(())
}

/// Removes metadata of worktrees whose directories no longer exist.
pub(crate) fn prune(runner: &Runner, repo: &Path) -> Result<(), GitError> {
    runner.run(repo, &["worktree", "prune"], Effect::Local)?;
    Ok(())
}

/// Whether `path` is a registered, usable worktree of `repo`: its directory must exist and
/// belong to the same repository, so stale metadata and unrelated directories do not count.
pub(crate) fn exists(runner: &Runner, repo: &Path, path: &Path) -> Result<bool, GitError> {
    let path = &repo.join(path);
    let is_registered = registered(runner, repo)?
        .iter()
        .any(|worktree| same_path(&worktree.path, path));
    if !is_registered || !path.is_dir() {
        return Ok(false);
    }
    let query = |dir: &Path, arg: &str| {
        runner
            .run(
                dir,
                &["rev-parse", "--path-format=absolute", arg],
                Effect::Read,
            )
            .ok()
            .map(|output| PathBuf::from(output.stdout.trim()))
    };
    let (Some(top), Some(common), Some(repo_common)) = (
        query(path, "--show-toplevel"),
        query(path, "--git-common-dir"),
        query(repo, "--git-common-dir"),
    ) else {
        return Ok(false);
    };
    Ok(same_path(&top, path) && same_path(&common, &repo_common))
}

/// Moves the branch checked out in the worktree at `path` to `commit`, fetching `origin` first if
/// the commit is not available locally (requested by id, independent of the configured fetch
/// refspecs). Already being at `commit` is not an error.
pub(crate) fn update(
    runner: &Runner,
    repo: &Path,
    path: &Path,
    commit: &str,
) -> Result<(), GitError> {
    if !exists(runner, repo, path)? {
        return Err(GitError::Failed {
            command: "reset".to_owned(),
            status: "not run".to_owned(),
            stderr: format!("{} is not a worktree", repo.join(path).display()),
        });
    }
    let path = &repo.join(path);
    let object = format!("{commit}^{{commit}}");
    if runner
        .run(repo, &["cat-file", "-e", &object], Effect::Read)
        .is_err()
        && runner
            .run(repo, &["fetch", "origin", commit], Effect::Local)
            .is_err()
    {
        // Servers may refuse unadvertised ids, so fetch every branch explicitly, independent of
        // the configured refspecs, then check the commit arrived.
        runner.run(
            repo,
            &[
                "fetch",
                "--prune",
                "origin",
                "+refs/heads/*:refs/remotes/origin/*",
            ],
            Effect::Local,
        )?;
        runner.run(repo, &["cat-file", "-e", &object], Effect::Read)?;
    }
    runner.run(path, &["reset", "--hard", &object], Effect::Local)?;
    Ok(())
}

struct Worktree {
    path: PathBuf,
    /// Full ref name, `None` when detached.
    branch: Option<String>,
}

fn registered(runner: &Runner, repo: &Path) -> Result<Vec<Worktree>, GitError> {
    let output = runner.run(repo, &["worktree", "list", "--porcelain"], Effect::Read)?;
    let mut worktrees: Vec<Worktree> = Vec::new();
    for line in output.stdout.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            worktrees.push(Worktree {
                path: PathBuf::from(path),
                branch: None,
            });
        } else if let Some(branch) = line.strip_prefix("branch ")
            && let Some(last) = worktrees.last_mut()
        {
            last.branch = Some(branch.to_owned());
        }
    }
    Ok(worktrees)
}

fn branch_exists(runner: &Runner, repo: &Path, branch_ref: &str) -> Result<bool, GitError> {
    let output = runner.run(
        repo,
        &["for-each-ref", "--format=%(refname)", branch_ref],
        Effect::Read,
    )?;
    Ok(output.stdout.lines().any(|line| line == branch_ref))
}

fn same_path(a: &Path, b: &Path) -> bool {
    resolve(a) == resolve(b)
}

/// Canonicalizes `path`, or just its parent when `path` itself no longer exists.
fn resolve(path: &Path) -> PathBuf {
    if let Ok(resolved) = path.canonicalize() {
        return resolved;
    }
    match (
        path.parent().and_then(|p| p.canonicalize().ok()),
        path.file_name(),
    ) {
        (Some(parent), Some(name)) => parent.join(name),
        _ => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TestRepo;

    fn rev(dir: &Path, rev: &str) -> String {
        Runner::new()
            .run(dir, &["rev-parse", rev], Effect::Read)
            .unwrap()
            .stdout
            .trim()
            .to_owned()
    }

    fn branch_exists_in(repo: &TestRepo, name: &str) -> bool {
        branch_exists(&Runner::new(), repo.work(), &format!("refs/heads/{name}")).unwrap()
    }

    /// A repository whose remote has a `feature` branch ahead of the local one.
    fn repo_with_feature() -> (TestRepo, PathBuf) {
        let repo = TestRepo::new();
        let other = repo.clone_origin("other");
        let git = |args: &[&str]| {
            Runner::new().run(&other, args, Effect::Local).unwrap();
        };
        git(&["checkout", "-b", "feature"]);
        repo.commit_file(&other, "f.txt", "feature");
        git(&["push", "origin", "feature"]);
        let path = repo.work().join("../wt-task");
        (repo, path)
    }

    #[test]
    fn create_branches_from_remote_feature_head() {
        let (repo, path) = repo_with_feature();
        let created = create(&Runner::new(), repo.work(), &path, "task/a", "feature").unwrap();
        assert_eq!(created, path);
        assert!(path.join("f.txt").exists());
        assert_eq!(rev(&path, "HEAD"), rev(&repo.origin(), "feature"));
        assert_eq!(rev(repo.work(), "task/a"), rev(&repo.origin(), "feature"));
    }

    #[test]
    fn create_twice_succeeds() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let head = rev(&path, "HEAD");
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        assert_eq!(rev(&path, "HEAD"), head);
    }

    #[test]
    fn create_reuses_existing_task_branch() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        runner
            .run(repo.work(), &["branch", "task/a", "main"], Effect::Local)
            .unwrap();
        let main = rev(repo.work(), "main");
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        assert_eq!(rev(&path, "HEAD"), main);
        assert!(!path.join("f.txt").exists());
    }

    #[test]
    fn create_at_path_occupied_by_other_branch_fails() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let error = create(&runner, repo.work(), &path, "task/b", "feature").unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn create_at_non_empty_directory_fails() {
        let (repo, path) = repo_with_feature();
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("x"), "x").unwrap();
        let error = create(&Runner::new(), repo.work(), &path, "task/a", "feature").unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn create_at_empty_directory_fails_and_preserves_it() {
        let (repo, path) = repo_with_feature();
        std::fs::create_dir_all(&path).unwrap();
        let error = create(&Runner::new(), repo.work(), &path, "task/a", "feature").unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
        assert!(path.is_dir());
    }

    #[test]
    fn create_twice_and_remove_with_relative_path() {
        let (repo, _) = repo_with_feature();
        let runner = Runner::new();
        let relative = Path::new("../wt-relative");
        let created = create(&runner, repo.work(), relative, "task/a", "feature").unwrap();
        assert!(created.join("f.txt").exists());
        create(&runner, repo.work(), relative, "task/a", "feature").unwrap();
        remove(&runner, repo.work(), relative, "task/a").unwrap();
        assert!(!created.exists());
        assert!(!branch_exists_in(&repo, "task/a"));
    }

    #[test]
    fn remove_ignores_descendant_branch_of_missing_task() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        runner
            .run(
                repo.work(),
                &["branch", "task/prefix/child", "main"],
                Effect::Local,
            )
            .unwrap();
        remove(&runner, repo.work(), &path, "task/prefix").unwrap();
        assert!(branch_exists_in(&repo, "task/prefix/child"));
    }

    #[test]
    fn create_with_branch_checked_out_elsewhere_fails() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let second = repo.work().join("../wt-second");
        let error = create(&runner, repo.work(), &second, "task/a", "feature").unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn remove_deletes_directory_and_task_branch() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        // Unmerged and uncommitted work does not block removal.
        repo.commit_file(&path, "t.txt", "task");
        std::fs::write(path.join("dirty.txt"), "dirty").unwrap();
        remove(&runner, repo.work(), &path, "task/a").unwrap();
        assert!(!path.exists());
        assert!(!branch_exists_in(&repo, "task/a"));
        assert!(registered(&runner, repo.work()).unwrap().len() == 1);
    }

    #[test]
    fn remove_missing_worktree_succeeds() {
        let (repo, path) = repo_with_feature();
        remove(&Runner::new(), repo.work(), &path, "task/a").unwrap();
    }

    #[test]
    fn remove_after_directory_deleted_manually_succeeds() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        std::fs::remove_dir_all(&path).unwrap();
        remove(&runner, repo.work(), &path, "task/a").unwrap();
        assert!(!branch_exists_in(&repo, "task/a"));
    }

    #[test]
    fn prune_drops_metadata_of_deleted_worktree() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        std::fs::remove_dir_all(&path).unwrap();
        assert_eq!(registered(&runner, repo.work()).unwrap().len(), 2);
        prune(&runner, repo.work()).unwrap();
        assert_eq!(registered(&runner, repo.work()).unwrap().len(), 1);
    }

    #[test]
    fn prune_with_nothing_to_prune_succeeds() {
        let repo = TestRepo::new();
        prune(&Runner::new(), repo.work()).unwrap();
    }

    #[test]
    fn task_branch_is_never_pushed() {
        let (repo, path) = repo_with_feature();
        create(&Runner::new(), repo.work(), &path, "task/a", "feature").unwrap();
        assert!(!branch_exists(&Runner::new(), &repo.origin(), "refs/heads/task/a").unwrap());
    }

    #[test]
    fn exists_reflects_registration() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        assert!(!exists(&runner, repo.work(), &path).unwrap());
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        assert!(exists(&runner, repo.work(), &path).unwrap());
        assert!(exists(&runner, repo.work(), repo.work()).unwrap());
    }

    #[test]
    fn update_moves_branch_to_fetched_commit() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let other = repo.work().join("../other");
        repo.commit_file(&other, "g.txt", "more");
        runner
            .run(&other, &["push", "origin", "feature"], Effect::Remote)
            .unwrap();
        let target = rev(&other, "HEAD");
        update(&runner, repo.work(), &path, &target).unwrap();
        assert_eq!(rev(&path, "HEAD"), target);
        assert_eq!(rev(repo.work(), "task/a"), target);
        update(&runner, repo.work(), &path, &target).unwrap();
    }

    #[test]
    fn exists_is_false_after_directory_deleted_without_prune() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        std::fs::remove_dir_all(&path).unwrap();
        assert!(!exists(&runner, repo.work(), &path).unwrap());
    }

    #[test]
    fn exists_is_false_for_unrelated_replacement_directory() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        std::fs::remove_dir_all(&path).unwrap();
        std::fs::create_dir_all(&path).unwrap();
        assert!(!exists(&runner, repo.work(), &path).unwrap());
        runner.run(&path, &["init", "-q"], Effect::Local).unwrap();
        assert!(!exists(&runner, repo.work(), &path).unwrap());
    }

    #[test]
    fn update_fetches_commit_outside_configured_refspec() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        runner
            .run(
                repo.work(),
                &[
                    "config",
                    "remote.origin.fetch",
                    "+refs/heads/main:refs/remotes/origin/main",
                ],
                Effect::Local,
            )
            .unwrap();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let other = repo.work().join("../other");
        repo.commit_file(&other, "g.txt", "more");
        runner
            .run(&other, &["push", "origin", "feature"], Effect::Remote)
            .unwrap();
        let target = rev(&other, "HEAD");
        update(&runner, repo.work(), &path, &target).unwrap();
        assert_eq!(rev(&path, "HEAD"), target);
    }

    #[test]
    fn update_of_missing_worktree_fails() {
        let (repo, path) = repo_with_feature();
        let head = rev(repo.work(), "HEAD");
        let error = update(&Runner::new(), repo.work(), &path, &head).unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }

    #[test]
    fn update_to_unknown_commit_fails() {
        let (repo, path) = repo_with_feature();
        let runner = Runner::new();
        create(&runner, repo.work(), &path, "task/a", "feature").unwrap();
        let error = update(&runner, repo.work(), &path, &"1".repeat(40)).unwrap_err();
        assert!(matches!(error, GitError::Failed { .. }), "{error:?}");
    }
}
