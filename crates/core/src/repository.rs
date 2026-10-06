use std::path::Path;

use async_trait::async_trait;

use crate::error::PortError;
use crate::{BranchName, CommitId};

/// The git repository the run works in.
#[async_trait]
pub trait Repository: Send + Sync {
    /// `develop` if it exists, otherwise the repository's default branch.
    async fn resolve_base_branch(&self) -> Result<BranchName, PortError>;

    /// Creates `feature` from `base`.
    async fn create_feature_branch(
        &self,
        feature: &BranchName,
        base: &BranchName,
    ) -> Result<(), PortError>;

    /// Creates a worktree at `path` on `task_branch`. A missing `task_branch` is created from
    /// `feature`; an existing one is reused.
    async fn create_worktree(
        &self,
        path: &Path,
        task_branch: &BranchName,
        feature: &BranchName,
    ) -> Result<(), PortError>;

    /// Removes the worktree at `path` and deletes `task_branch`. Succeeds when they are already
    /// gone, so that any error means the removal did not happen (or is uncertain).
    async fn remove_worktree(&self, path: &Path, task_branch: &BranchName)
    -> Result<(), PortError>;

    /// Drops metadata of worktrees whose directories no longer exist.
    async fn prune_worktrees(&self) -> Result<(), PortError>;

    /// The commit `branch` points to on the remote.
    async fn remote_head(&self, branch: &BranchName) -> Result<CommitId, PortError>;
}

#[cfg(any(test, feature = "testing"))]
pub use fake::FakeRepository;

#[cfg(any(test, feature = "testing"))]
mod fake {
    use std::collections::{HashMap, VecDeque};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::Repository;
    use crate::error::PortError;
    use crate::{BranchName, CommitId};

    #[derive(Default)]
    struct State {
        default_branch: Option<BranchName>,
        branches: HashMap<BranchName, CommitId>,
        worktrees: HashMap<PathBuf, BranchName>,
        remote_heads: HashMap<BranchName, CommitId>,
        failures: VecDeque<PortError>,
    }

    /// In-memory [`Repository`]. Branch heads double as remote heads until
    /// [`FakeRepository::set_remote_head`] makes them diverge.
    pub struct FakeRepository {
        state: Mutex<State>,
    }

    impl FakeRepository {
        pub fn new(default_branch: BranchName, head: CommitId) -> Self {
            let mut state = State::default();
            state.branches.insert(default_branch.clone(), head.clone());
            state.remote_heads.insert(default_branch.clone(), head);
            state.default_branch = Some(default_branch);
            Self {
                state: Mutex::new(state),
            }
        }

        pub fn add_branch(&self, branch: BranchName, head: CommitId) {
            let mut state = self.state.lock().unwrap();
            state.remote_heads.insert(branch.clone(), head.clone());
            state.branches.insert(branch, head);
        }

        /// Simulates the remote moving `branch` without this repository's involvement.
        pub fn set_remote_head(&self, branch: BranchName, head: CommitId) {
            self.state.lock().unwrap().remote_heads.insert(branch, head);
        }

        /// The next operation returns `error` instead of running.
        pub fn fail_next(&self, error: PortError) {
            self.state.lock().unwrap().failures.push_back(error);
        }

        pub fn has_branch(&self, branch: &BranchName) -> bool {
            self.state.lock().unwrap().branches.contains_key(branch)
        }

        pub fn worktree_branch(&self, path: &Path) -> Option<BranchName> {
            self.state.lock().unwrap().worktrees.get(path).cloned()
        }
    }

    impl State {
        fn begin(&mut self) -> Result<(), PortError> {
            self.failures.pop_front().map_or(Ok(()), Err)
        }

        fn head(&self, branch: &BranchName) -> Result<CommitId, PortError> {
            self.branches
                .get(branch)
                .cloned()
                .ok_or_else(|| PortError::failed(format!("branch {branch} does not exist")))
        }
    }

    #[async_trait]
    impl Repository for FakeRepository {
        async fn resolve_base_branch(&self) -> Result<BranchName, PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin()?;
            let develop = BranchName::new("develop").unwrap();
            if state.branches.contains_key(&develop) {
                return Ok(develop);
            }
            state
                .default_branch
                .clone()
                .ok_or_else(|| PortError::failed("no default branch"))
        }

        async fn create_feature_branch(
            &self,
            feature: &BranchName,
            base: &BranchName,
        ) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin()?;
            if state.branches.contains_key(feature) {
                return Err(PortError::failed(format!(
                    "branch {feature} already exists"
                )));
            }
            let head = state.head(base)?;
            state.remote_heads.insert(feature.clone(), head.clone());
            state.branches.insert(feature.clone(), head);
            Ok(())
        }

        async fn create_worktree(
            &self,
            path: &Path,
            task_branch: &BranchName,
            feature: &BranchName,
        ) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin()?;
            if state.worktrees.contains_key(path) {
                return Err(PortError::failed(format!(
                    "worktree {} already exists",
                    path.display()
                )));
            }
            if !state.branches.contains_key(task_branch) {
                let head = state.head(feature)?;
                state.branches.insert(task_branch.clone(), head);
            }
            state
                .worktrees
                .insert(path.to_path_buf(), task_branch.clone());
            Ok(())
        }

        async fn remove_worktree(
            &self,
            path: &Path,
            task_branch: &BranchName,
        ) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin()?;
            state.worktrees.remove(path);
            state.branches.remove(task_branch);
            Ok(())
        }

        async fn prune_worktrees(&self) -> Result<(), PortError> {
            self.state.lock().unwrap().begin()
        }

        async fn remote_head(&self, branch: &BranchName) -> Result<CommitId, PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin()?;
            state
                .remote_heads
                .get(branch)
                .cloned()
                .ok_or_else(|| PortError::failed(format!("no remote branch {branch}")))
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use futures_executor::block_on;

        use super::*;

        fn branch(name: &str) -> BranchName {
            BranchName::new(name).unwrap()
        }

        fn commit(id: &str) -> CommitId {
            CommitId::new(id).unwrap()
        }

        fn repository() -> Arc<dyn Repository> {
            Arc::new(FakeRepository::new(branch("main"), commit("c0")))
        }

        #[test]
        fn base_falls_back_to_default_without_develop() {
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            assert_eq!(
                block_on(fake.resolve_base_branch()).unwrap(),
                branch("main")
            );
            fake.add_branch(branch("develop"), commit("c1"));
            assert_eq!(
                block_on(fake.resolve_base_branch()).unwrap(),
                branch("develop")
            );
        }

        #[test]
        fn feature_branch_starts_at_base_head() {
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            block_on(fake.create_feature_branch(&branch("feat"), &branch("main"))).unwrap();
            assert!(fake.has_branch(&branch("feat")));
            assert_eq!(
                block_on(fake.remote_head(&branch("feat"))).unwrap(),
                commit("c0")
            );
            assert!(
                block_on(fake.create_feature_branch(&branch("feat"), &branch("main"))).is_err()
            );
        }

        #[test]
        fn worktree_create_and_remove_manage_task_branch() {
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            let path = Path::new("/wt/15");
            block_on(fake.create_feature_branch(&branch("feat"), &branch("main"))).unwrap();
            block_on(fake.create_worktree(path, &branch("task"), &branch("feat"))).unwrap();
            assert!(fake.has_branch(&branch("task")));
            assert_eq!(fake.worktree_branch(path), Some(branch("task")));

            block_on(fake.remove_worktree(path, &branch("task"))).unwrap();
            assert!(!fake.has_branch(&branch("task")));
            assert_eq!(fake.worktree_branch(path), None);
        }

        #[test]
        fn worktree_reuses_existing_task_branch() {
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            fake.add_branch(branch("task"), commit("c9"));
            block_on(fake.create_worktree(Path::new("/wt"), &branch("task"), &branch("main")))
                .unwrap();
            assert_eq!(fake.worktree_branch(Path::new("/wt")), Some(branch("task")));
        }

        #[test]
        fn remote_head_reads_and_follows_unexpected_change() {
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            assert_eq!(
                block_on(fake.remote_head(&branch("main"))).unwrap(),
                commit("c0")
            );
            fake.set_remote_head(branch("main"), commit("c1"));
            assert_eq!(
                block_on(fake.remote_head(&branch("main"))).unwrap(),
                commit("c1")
            );
            assert!(block_on(fake.remote_head(&branch("nope"))).is_err());
        }

        #[test]
        fn scripted_failure_applies_once() {
            let repository = repository();
            let fake = FakeRepository::new(branch("main"), commit("c0"));
            fake.fail_next(PortError::uncertain("lost"));
            assert!(block_on(fake.prune_worktrees()).unwrap_err().is_uncertain());
            block_on(fake.prune_worktrees()).unwrap();
            block_on(repository.prune_worktrees()).unwrap();
        }
    }
}
