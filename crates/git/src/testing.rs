//! Shared fixtures for the operation tests.

use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

/// A temporary repository on `main` with an initial commit and a bare `origin` it has pushed to.
pub(crate) struct TestRepo {
    dir: TempDir,
    work: PathBuf,
}

impl TestRepo {
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let work = dir.path().join("work");
        let repo = Self { dir, work };
        git(
            repo.dir.path(),
            &["init", "--bare", "--initial-branch=main", "origin.git"],
        );
        git(repo.dir.path(), &["init", "--initial-branch=main", "work"]);
        let work = repo.work();
        git(work, &["config", "user.name", "Test"]);
        git(work, &["config", "user.email", "test@example.com"]);
        git(work, &["config", "commit.gpgsign", "false"]);
        git(
            work,
            &["remote", "add", "origin", repo.origin().to_str().unwrap()],
        );
        repo.commit_file(work, "README.md", "initial");
        git(work, &["push", "--set-upstream", "origin", "main"]);
        repo
    }

    /// The working repository.
    pub fn work(&self) -> &Path {
        &self.work
    }

    /// The bare remote.
    pub fn origin(&self) -> PathBuf {
        self.dir.path().join("origin.git")
    }

    /// Clones `origin` into a sibling directory called `name`.
    pub fn clone_origin(&self, name: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        git(
            self.dir.path(),
            &["clone", self.origin().to_str().unwrap(), name],
        );
        git(&path, &["config", "user.name", "Test"]);
        git(&path, &["config", "user.email", "test@example.com"]);
        git(&path, &["config", "commit.gpgsign", "false"]);
        path
    }

    /// Writes `file` in `repo` and commits it.
    pub fn commit_file(&self, repo: &Path, file: &str, content: &str) {
        std::fs::write(repo.join(file), content).unwrap();
        git(repo, &["add", file]);
        git(repo, &["commit", "-m", &format!("Add {file}")]);
    }
}

pub(crate) fn git(dir: &Path, args: &[&str]) {
    git_output(dir, args);
}

/// Runs git and returns trimmed stdout.
pub(crate) fn git_output(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}
