//! The `Repository` port on the `git` CLI.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chimera_core::error::PortError;
use chimera_core::repository::Repository;
use chimera_core::{BranchName, CommitId};

use crate::error::GitError;
use crate::runner::Runner;
use crate::{branch, remote_head, worktree};

/// A git repository on disk with an `origin` remote.
#[derive(Debug, Clone)]
pub struct GitRepository {
    runner: Runner,
    dir: PathBuf,
}

impl GitRepository {
    /// `dir` is the repository's working directory; relative worktree paths resolve against it.
    /// A relative `dir` is resolved against the current directory here, once, because `git` runs
    /// inside `dir` and would otherwise resolve paths derived from it a second time.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            runner: Runner::new(),
            dir: std::path::absolute(&dir).unwrap_or(dir),
        }
    }

    /// Runs the blocking `git` operation `f` off the async runtime.
    async fn blocking<T, F>(&self, f: F) -> Result<T, PortError>
    where
        T: Send + 'static,
        F: FnOnce(&Runner, &Path) -> Result<T, GitError> + Send + 'static,
    {
        let runner = self.runner.clone();
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || f(&runner, &dir))
            .await
            .map_err(|error| PortError::failed(format!("git task did not complete: {error}")))?
            .map_err(PortError::from)
    }
}

#[async_trait]
impl Repository for GitRepository {
    async fn resolve_base_branch(&self) -> Result<BranchName, PortError> {
        self.blocking(branch::resolve_base_branch).await
    }

    async fn create_feature_branch(
        &self,
        feature: &BranchName,
        base: &BranchName,
    ) -> Result<(), PortError> {
        let (feature, base) = (feature.clone(), base.clone());
        self.blocking(move |runner, dir| {
            branch::create_feature_branch(runner, dir, &feature, &base)
        })
        .await
    }

    async fn create_worktree(
        &self,
        path: &Path,
        task_branch: &BranchName,
        feature: &BranchName,
    ) -> Result<(), PortError> {
        let (path, task, feature) = (path.to_path_buf(), task_branch.clone(), feature.clone());
        self.blocking(move |runner, dir| {
            worktree::create(runner, dir, &path, task.as_str(), feature.as_str()).map(|_| ())
        })
        .await
    }

    async fn worktree_exists(&self, path: &Path) -> Result<bool, PortError> {
        let path = path.to_path_buf();
        self.blocking(move |runner, dir| worktree::exists(runner, dir, &path))
            .await
    }

    async fn update_worktree(&self, path: &Path, commit: &CommitId) -> Result<(), PortError> {
        let (path, commit) = (path.to_path_buf(), commit.clone());
        self.blocking(move |runner, dir| worktree::update(runner, dir, &path, commit.as_str()))
            .await
    }

    async fn remove_worktree(
        &self,
        path: &Path,
        task_branch: &BranchName,
    ) -> Result<(), PortError> {
        let (path, task) = (path.to_path_buf(), task_branch.clone());
        self.blocking(move |runner, dir| worktree::remove(runner, dir, &path, task.as_str()))
            .await
    }

    async fn prune_worktrees(&self) -> Result<(), PortError> {
        self.blocking(worktree::prune).await
    }

    async fn remote_head(&self, branch: &BranchName) -> Result<Option<CommitId>, PortError> {
        let branch = branch.clone();
        self.blocking(move |runner, dir| remote_head::remote_head(runner, dir, &branch))
            .await
    }
}
