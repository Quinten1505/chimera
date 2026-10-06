//! The task lifecycle through `dyn Repository` against real repositories.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use chimera_core::repository::Repository;
use chimera_core::{BranchName, CommitId};
use chimera_git::GitRepository;
use tempfile::TempDir;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn branch(name: &str) -> BranchName {
    BranchName::new(name).unwrap()
}

fn configure(dir: &Path) {
    git(dir, &["config", "user.name", "Test"]);
    git(dir, &["config", "user.email", "test@example.com"]);
    git(dir, &["config", "commit.gpgsign", "false"]);
}

/// A repository on `main` with a bare `origin`.
fn setup() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let origin = dir.path().join("origin.git");
    let work = dir.path().join("work");
    git(
        dir.path(),
        &["init", "--bare", "--initial-branch=main", "origin.git"],
    );
    git(dir.path(), &["init", "--initial-branch=main", "work"]);
    configure(&work);
    git(
        &work,
        &["remote", "add", "origin", origin.to_str().unwrap()],
    );
    std::fs::write(work.join("README.md"), "initial").unwrap();
    git(&work, &["add", "."]);
    git(&work, &["commit", "-m", "Initial commit"]);
    git(&work, &["push", "--set-upstream", "origin", "main"]);
    (dir, work)
}

#[tokio::test]
async fn full_task_lifecycle() {
    let (dir, work) = setup();
    let repository: Arc<dyn Repository> = Arc::new(GitRepository::new(&work));
    let feature = branch("feature/x");
    let task = branch("task/1");
    let worktree = dir.path().join("wt-1");

    let base = repository.resolve_base_branch().await.unwrap();
    assert_eq!(base, branch("main"));

    repository
        .create_feature_branch(&feature, &base)
        .await
        .unwrap();
    let start = repository.remote_head(&feature).await.unwrap().unwrap();
    assert_eq!(start.as_str(), git(&work, &["rev-parse", "main"]));

    assert!(!repository.worktree_exists(&worktree).await.unwrap());
    repository
        .create_worktree(&worktree, &task, &feature)
        .await
        .unwrap();
    assert!(repository.worktree_exists(&worktree).await.unwrap());

    configure(&worktree);
    std::fs::write(worktree.join("task.txt"), "work").unwrap();
    git(&worktree, &["add", "."]);
    git(&worktree, &["commit", "-m", "Do the task"]);
    let committed = git(&worktree, &["rev-parse", "HEAD"]);
    git(&worktree, &["push", "origin", "task/1:feature/x"]);

    let head = repository.remote_head(&feature).await.unwrap().unwrap();
    assert_eq!(head.as_str(), committed);
    assert_ne!(head, start);
    assert_eq!(repository.remote_head(&task).await.unwrap(), None);

    repository.remove_worktree(&worktree, &task).await.unwrap();
    assert!(!repository.worktree_exists(&worktree).await.unwrap());
    assert!(!worktree.exists());
    assert_eq!(
        git(&work, &["branch", "--list", "task/1"]),
        "",
        "task branch is deleted"
    );
    repository.prune_worktrees().await.unwrap();
}

#[tokio::test]
async fn develop_is_preferred_as_base() {
    let (_dir, work) = setup();
    git(&work, &["push", "origin", "main:develop"]);
    let repository = GitRepository::new(&work);
    assert_eq!(
        repository.resolve_base_branch().await.unwrap(),
        branch("develop")
    );
}

#[tokio::test]
async fn update_worktree_moves_task_branch_to_remote_commit() {
    let (dir, work) = setup();
    let repository = GitRepository::new(&work);
    let feature = branch("feature/x");
    let task = branch("task/1");
    let worktree = dir.path().join("wt-1");
    repository
        .create_feature_branch(&feature, &branch("main"))
        .await
        .unwrap();
    repository
        .create_worktree(&worktree, &task, &feature)
        .await
        .unwrap();

    let other = dir.path().join("other");
    git(
        dir.path(),
        &[
            "clone",
            dir.path().join("origin.git").to_str().unwrap(),
            "other",
        ],
    );
    configure(&other);
    git(&other, &["checkout", "feature/x"]);
    std::fs::write(other.join("new.txt"), "new").unwrap();
    git(&other, &["add", "."]);
    git(&other, &["commit", "-m", "Move the feature"]);
    git(&other, &["push", "origin", "feature/x"]);
    let target = CommitId::new(git(&other, &["rev-parse", "HEAD"])).unwrap();

    repository
        .update_worktree(&worktree, &target)
        .await
        .unwrap();
    assert_eq!(git(&worktree, &["rev-parse", "HEAD"]), target.as_str());
    repository
        .update_worktree(&worktree, &target)
        .await
        .unwrap();
}

#[tokio::test]
async fn failures_are_reported_as_port_errors() {
    let (dir, work) = setup();
    let repository = GitRepository::new(&work);
    let error = repository
        .create_feature_branch(&branch("feature/x"), &branch("missing"))
        .await
        .unwrap_err();
    assert!(error.is_failed(), "{error:?}");
    let error = repository
        .update_worktree(
            &dir.path().join("none"),
            &CommitId::new("1".repeat(40)).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.is_failed(), "{error:?}");
}

#[tokio::test]
async fn deleted_or_replaced_worktree_directory_does_not_exist() {
    let (dir, work) = setup();
    let repository = GitRepository::new(&work);
    let feature = branch("feature/x");
    let worktree = dir.path().join("wt-1");
    repository
        .create_feature_branch(&feature, &branch("main"))
        .await
        .unwrap();
    repository
        .create_worktree(&worktree, &branch("task/1"), &feature)
        .await
        .unwrap();

    std::fs::remove_dir_all(&worktree).unwrap();
    assert!(!repository.worktree_exists(&worktree).await.unwrap());
    std::fs::create_dir(&worktree).unwrap();
    assert!(!repository.worktree_exists(&worktree).await.unwrap());
}

#[tokio::test]
async fn update_worktree_fetches_commit_outside_configured_refspec() {
    let (dir, work) = setup();
    let repository = GitRepository::new(&work);
    let feature = branch("feature/x");
    let worktree = dir.path().join("wt-1");
    git(
        &work,
        &[
            "config",
            "remote.origin.fetch",
            "+refs/heads/main:refs/remotes/origin/main",
        ],
    );
    repository
        .create_feature_branch(&feature, &branch("main"))
        .await
        .unwrap();
    repository
        .create_worktree(&worktree, &branch("task/1"), &feature)
        .await
        .unwrap();

    let other = dir.path().join("other");
    git(
        dir.path(),
        &[
            "clone",
            dir.path().join("origin.git").to_str().unwrap(),
            "other",
        ],
    );
    configure(&other);
    git(&other, &["checkout", "feature/x"]);
    std::fs::write(other.join("new.txt"), "new").unwrap();
    git(&other, &["add", "."]);
    git(&other, &["commit", "-m", "Move the feature"]);
    git(&other, &["push", "origin", "feature/x"]);
    let target = CommitId::new(git(&other, &["rev-parse", "HEAD"])).unwrap();

    repository
        .update_worktree(&worktree, &target)
        .await
        .unwrap();
    assert_eq!(git(&worktree, &["rev-parse", "HEAD"]), target.as_str());
}
