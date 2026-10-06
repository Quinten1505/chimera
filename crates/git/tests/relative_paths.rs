//! Relative repository and worktree paths, resolved against the current directory.
//!
//! Kept in its own test binary because it changes the process's current directory.

use std::path::Path;
use std::process::Command;

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

#[tokio::test]
async fn relative_repository_and_worktree_paths() {
    let dir = TempDir::new().unwrap();
    let parent = dir.path();
    let work = parent.join("work");
    git(
        parent,
        &["init", "--bare", "--initial-branch=main", "origin.git"],
    );
    git(parent, &["init", "--initial-branch=main", "work"]);
    for (key, value) in [
        ("user.name", "Test"),
        ("user.email", "test@example.com"),
        ("commit.gpgsign", "false"),
    ] {
        git(&work, &["config", key, value]);
    }
    git(&work, &["remote", "add", "origin", "../origin.git"]);
    std::fs::write(work.join("README.md"), "initial").unwrap();
    git(&work, &["add", "."]);
    git(&work, &["commit", "-m", "Initial commit"]);
    git(&work, &["push", "--set-upstream", "origin", "main"]);

    std::env::set_current_dir(parent).unwrap();
    let repository = GitRepository::new("work");
    let feature = branch("feature/x");
    let task = branch("task/1");
    let relative = Path::new("../wt");
    repository
        .create_feature_branch(&feature, &branch("main"))
        .await
        .unwrap();
    repository
        .create_worktree(relative, &task, &feature)
        .await
        .unwrap();

    let expected = parent.join("wt");
    assert!(expected.join("README.md").exists());
    assert!(!work.join("wt").exists());
    assert_eq!(
        git(&expected, &["rev-parse", "--abbrev-ref", "HEAD"]),
        "task/1"
    );
    assert!(repository.worktree_exists(relative).await.unwrap());
    assert!(repository.worktree_exists(&expected).await.unwrap());
    // Creating again is already applied.
    repository
        .create_worktree(relative, &task, &feature)
        .await
        .unwrap();
    let head = CommitId::new(git(&work, &["rev-parse", "main"])).unwrap();
    repository.update_worktree(relative, &head).await.unwrap();

    repository.remove_worktree(relative, &task).await.unwrap();
    assert!(!expected.exists());
    assert!(!repository.worktree_exists(relative).await.unwrap());
    assert_eq!(git(&work, &["branch", "--list", "task/1"]), "");
}
