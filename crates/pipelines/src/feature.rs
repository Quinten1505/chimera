use std::sync::{Arc, Mutex};

use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::repository::Repository;
use chimera_core::{BranchName, CommitId, Feature, Specification};
use serde::{Deserialize, Serialize};

use crate::driver::{Pipeline, PipelineState};
use crate::error::{PauseReason, PipelineError};
use crate::policy::{Budget, Policy, RetryRefused};

/// State of the feature pipeline; each step performs at most one external effect.
///
/// Every creation is preceded by a lookup step and, when its response is lost, followed by a
/// reconciliation step, so a retry or restart never duplicates the branch or the pull request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureState {
    SelectingBase,
    /// Looks for a feature branch an earlier attempt already created.
    CheckingBranch {
        base: BranchName,
    },
    CreatingBranch {
        base: BranchName,
    },
    /// Creating the branch did not report success; looks for it before the creation may be
    /// repeated.
    ReconcilingBranch {
        base: BranchName,
    },
    /// The branch exists; read its head to record as the expected remote head.
    RecordingHead {
        base: BranchName,
    },
    /// Looks for a pull request an earlier attempt already opened.
    CheckingPullRequest {
        base: BranchName,
        head: CommitId,
    },
    OpeningPullRequest {
        base: BranchName,
        head: CommitId,
    },
    /// Opening the pull request did not report success; looks for it before the creation may be
    /// repeated.
    ReconcilingPullRequest {
        base: BranchName,
        head: CommitId,
    },
    Done(Feature),
    Paused {
        reason: PauseReason,
        resume_at: Box<FeatureState>,
    },
}

impl FeatureState {
    /// The state a paused pipeline continues from; any other state is returned unchanged.
    pub fn resume(self) -> Self {
        match self {
            Self::Paused { resume_at, .. } => *resume_at,
            other => other,
        }
    }
}

impl PipelineState for FeatureState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }
}

/// A failed step: the error, and the state to retry from if the policy permits a retry.
type StepFailure = (PortError, FeatureState);

/// Specification -> Feature: selects the base, creates the feature branch and opens the draft PR.
pub struct FeaturePipeline {
    specification: Specification,
    feature_branch: BranchName,
    repository: Arc<dyn Repository>,
    forge: Arc<dyn Forge>,
    policy: Arc<Policy>,
    /// The state this instance last returned. A creation state that is not this one was loaded
    /// from the saved state, so an earlier process may already have performed the creation.
    last_returned: Mutex<Option<FeatureState>>,
}

impl FeaturePipeline {
    /// The feature branch is named `feature/<specification number>`.
    pub fn new(
        specification: Specification,
        repository: Arc<dyn Repository>,
        forge: Arc<dyn Forge>,
        policy: Arc<Policy>,
    ) -> Self {
        let feature_branch = BranchName::new(format!("feature/{}", specification.issue.number()))
            .expect("a feature branch name is never blank");
        Self {
            specification,
            feature_branch,
            repository,
            forge,
            policy,
            last_returned: Mutex::new(None),
        }
    }

    fn is_resumed(&self, state: &FeatureState) -> bool {
        self.last_returned.lock().unwrap().as_ref() != Some(state)
    }

    async fn advance(&self, state: &FeatureState) -> Result<FeatureState, StepFailure> {
        let fail = |error| (error, state.clone());
        match state {
            FeatureState::SelectingBase => Ok(FeatureState::CheckingBranch {
                base: self.repository.resolve_base_branch().await.map_err(fail)?,
            }),
            FeatureState::CheckingBranch { base } | FeatureState::ReconcilingBranch { base } => {
                let found = self
                    .repository
                    .remote_head(&self.feature_branch)
                    .await
                    .map_err(fail)?;
                match (found, state) {
                    (Some(_), _) => Ok(FeatureState::RecordingHead { base: base.clone() }),
                    (None, FeatureState::CheckingBranch { .. }) => {
                        Ok(FeatureState::CreatingBranch { base: base.clone() })
                    }
                    // The creation did not happen after all; repeating it needs permission.
                    (None, _) => Err((
                        PortError::uncertain("feature branch creation did not take effect"),
                        FeatureState::CreatingBranch { base: base.clone() },
                    )),
                }
            }
            FeatureState::CreatingBranch { base } if self.is_resumed(state) => {
                Ok(FeatureState::ReconcilingBranch { base: base.clone() })
            }
            FeatureState::CreatingBranch { base } => {
                match self
                    .repository
                    .create_feature_branch(&self.feature_branch, base)
                    .await
                {
                    Ok(()) => Ok(FeatureState::RecordingHead { base: base.clone() }),
                    Err(_) => Ok(FeatureState::ReconcilingBranch { base: base.clone() }),
                }
            }
            FeatureState::RecordingHead { base } => {
                let head = self
                    .repository
                    .remote_head(&self.feature_branch)
                    .await
                    .map_err(fail)?
                    .ok_or_else(|| fail(PortError::failed("feature branch not on the remote")))?;
                Ok(FeatureState::CheckingPullRequest {
                    base: base.clone(),
                    head,
                })
            }
            FeatureState::CheckingPullRequest { base, head }
            | FeatureState::ReconcilingPullRequest { base, head } => {
                let found = self
                    .forge
                    .find_open_pull_request(&self.feature_branch)
                    .await
                    .map_err(fail)?;
                match (found, state) {
                    (Some(pull_request), _) => Ok(FeatureState::Done(Feature {
                        specification: self.specification.issue.clone(),
                        base_branch: base.clone(),
                        feature_branch: self.feature_branch.clone(),
                        expected_remote_head: head.clone(),
                        draft_pull_request: pull_request,
                    })),
                    (None, FeatureState::CheckingPullRequest { .. }) => {
                        Ok(FeatureState::OpeningPullRequest {
                            base: base.clone(),
                            head: head.clone(),
                        })
                    }
                    (None, _) => Err((
                        PortError::uncertain("pull request creation did not take effect"),
                        FeatureState::OpeningPullRequest {
                            base: base.clone(),
                            head: head.clone(),
                        },
                    )),
                }
            }
            FeatureState::OpeningPullRequest { base, head } if self.is_resumed(state) => {
                Ok(FeatureState::ReconcilingPullRequest {
                    base: base.clone(),
                    head: head.clone(),
                })
            }
            FeatureState::OpeningPullRequest { base, head } => {
                match self
                    .forge
                    .create_draft_pull_request(
                        &self.feature_branch,
                        base,
                        &format!("Implement {}", self.specification.issue),
                        &format!("Draft pull request for {}.", self.specification.issue),
                    )
                    .await
                {
                    Ok(_) => Ok(FeatureState::CheckingPullRequest {
                        base: base.clone(),
                        head: head.clone(),
                    }),
                    Err(_) => Ok(FeatureState::ReconcilingPullRequest {
                        base: base.clone(),
                        head: head.clone(),
                    }),
                }
            }
            FeatureState::Done(_) | FeatureState::Paused { .. } => Ok(state.clone()),
        }
    }
}

impl Pipeline for FeaturePipeline {
    type State = FeatureState;

    fn initial_state(&self) -> FeatureState {
        FeatureState::SelectingBase
    }

    /// Pauses instead of acting while the run is paused. A failed effect is retried only while
    /// the policy permits, otherwise the pipeline pauses. A creation is repeated only after the
    /// reconciling step found it missing, so `reconciled` is always true here. A creation state
    /// loaded from a saved state is reconciled first, as the saved state cannot tell whether the
    /// creation already took effect before a crash.
    async fn step(&self, state: FeatureState) -> Result<FeatureState, PipelineError> {
        if let Err(reason) = self.policy.check_start() {
            return Ok(FeatureState::Paused {
                reason,
                resume_at: Box::new(state),
            });
        }
        let next = match self.advance(&state).await {
            Ok(next) => next,
            Err((error, retry_from)) => {
                match self.policy.permit_retry(Budget::GithubRetry, &error, true) {
                    Ok(()) => retry_from,
                    Err(RetryRefused::Paused(reason)) => FeatureState::Paused {
                        reason,
                        resume_at: Box::new(state),
                    },
                    Err(RetryRefused::NeedsReconciliation) => return Err(error.into()),
                }
            }
        };
        *self.last_returned.lock().unwrap() = Some(next.clone());
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::{FakeRunStore, RunStore};
    use chimera_core::{IssueRef, IssueStatus, Limits, RunId, TicketPlan};
    use futures_executor::block_on;

    use super::*;
    use crate::drive;

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    fn commit(id: &str) -> CommitId {
        CommitId::new(id).unwrap()
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("o", "r", number).unwrap()
    }

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn feature_branch() -> BranchName {
        branch("feature/4")
    }

    /// Reports the effect as uncertain after performing it once, as when a response is lost.
    struct LostResponse {
        repository: Arc<FakeRepository>,
        forge: Arc<FakeForge>,
        lose_branch: AtomicBool,
        lose_pull_request: AtomicBool,
        /// Every repository operation, in order.
        operations: Mutex<Vec<&'static str>>,
    }

    impl LostResponse {
        fn record(&self, operation: &'static str) {
            self.operations.lock().unwrap().push(operation);
        }
    }

    #[async_trait]
    impl Repository for LostResponse {
        async fn resolve_base_branch(&self) -> Result<BranchName, PortError> {
            self.record("resolve_base_branch");
            self.repository.resolve_base_branch().await
        }

        async fn create_feature_branch(
            &self,
            feature: &BranchName,
            base: &BranchName,
        ) -> Result<(), PortError> {
            self.record("create_feature_branch");
            self.repository.create_feature_branch(feature, base).await?;
            if self.lose_branch.swap(false, Ordering::SeqCst) {
                return Err(PortError::uncertain("response lost"));
            }
            Ok(())
        }

        async fn create_worktree(
            &self,
            path: &Path,
            task_branch: &BranchName,
            feature: &BranchName,
        ) -> Result<(), PortError> {
            self.repository
                .create_worktree(path, task_branch, feature)
                .await
        }

        async fn remove_worktree(
            &self,
            path: &Path,
            task_branch: &BranchName,
        ) -> Result<(), PortError> {
            self.repository.remove_worktree(path, task_branch).await
        }

        async fn prune_worktrees(&self) -> Result<(), PortError> {
            self.repository.prune_worktrees().await
        }

        async fn update_worktree(&self, path: &Path, commit: &CommitId) -> Result<(), PortError> {
            self.record("update_worktree");
            self.repository.update_worktree(path, commit).await
        }

        async fn remote_head(&self, branch: &BranchName) -> Result<Option<CommitId>, PortError> {
            self.record("remote_head");
            self.repository.remote_head(branch).await
        }
    }

    #[async_trait]
    impl Forge for LostResponse {
        async fn read_plan(&self, specification: &IssueRef) -> Result<TicketPlan, PortError> {
            self.forge.read_plan(specification).await
        }

        async fn create_draft_pull_request(
            &self,
            head: &BranchName,
            base: &BranchName,
            title: &str,
            body: &str,
        ) -> Result<IssueRef, PortError> {
            let pull_request = self
                .forge
                .create_draft_pull_request(head, base, title, body)
                .await?;
            if self.lose_pull_request.swap(false, Ordering::SeqCst) {
                return Err(PortError::uncertain("response lost"));
            }
            Ok(pull_request)
        }

        async fn find_open_pull_request(
            &self,
            head: &BranchName,
        ) -> Result<Option<IssueRef>, PortError> {
            self.forge.find_open_pull_request(head).await
        }

        async fn close_issue(&self, issue: &IssueRef) -> Result<(), PortError> {
            self.forge.close_issue(issue).await
        }

        async fn issue_status(&self, issue: &IssueRef) -> Result<IssueStatus, PortError> {
            self.forge.issue_status(issue).await
        }

        async fn mark_pull_request_ready(&self, pull_request: &IssueRef) -> Result<(), PortError> {
            self.forge.mark_pull_request_ready(pull_request).await
        }

        async fn pull_request_is_draft(&self, pull_request: &IssueRef) -> Result<bool, PortError> {
            self.forge.pull_request_is_draft(pull_request).await
        }
    }

    struct World {
        repository: Arc<FakeRepository>,
        forge: Arc<FakeForge>,
        policy: Arc<Policy>,
        lost: Arc<LostResponse>,
        store: FakeRunStore,
    }

    impl World {
        fn new() -> Self {
            Self::with_limits(Limits::default())
        }

        fn with_limits(limits: Limits) -> Self {
            let repository = Arc::new(FakeRepository::new(branch("main"), commit("c0")));
            let forge = Arc::new(FakeForge::new("o", "r"));
            Self {
                lost: Arc::new(LostResponse {
                    repository: repository.clone(),
                    forge: forge.clone(),
                    lose_branch: AtomicBool::new(false),
                    lose_pull_request: AtomicBool::new(false),
                    operations: Mutex::new(Vec::new()),
                }),
                repository,
                forge,
                policy: Arc::new(Policy::new(&limits)),
                store: FakeRunStore::new(),
            }
        }

        fn pipeline(&self) -> FeaturePipeline {
            FeaturePipeline::new(
                Specification { issue: issue(4) },
                self.lost.clone(),
                self.lost.clone(),
                self.policy.clone(),
            )
        }

        fn drive(&self) -> FeatureState {
            block_on(drive(&self.store, &run(), "feature", &self.pipeline())).unwrap()
        }

        fn save(&self, state: &FeatureState) {
            block_on(self.store.save_pipeline_state(
                &run(),
                "feature",
                serde_json::to_value(state).unwrap(),
            ))
            .unwrap();
        }

        /// Repository operations performed so far, whether or not they succeeded.
        fn operations(&self, operation: &str) -> usize {
            let operations = self.lost.operations.lock().unwrap();
            operations.iter().filter(|name| **name == operation).count()
        }

        /// All external operations performed so far.
        fn effects(&self) -> usize {
            self.lost.operations.lock().unwrap().len() + self.forge.calls().len()
        }

        fn creations(&self) -> usize {
            self.forge
                .calls()
                .iter()
                .filter(|call| matches!(call, ForgeCall::CreateDraftPullRequest { .. }))
                .count()
        }
    }

    fn expect_done(state: FeatureState) -> Feature {
        match state {
            FeatureState::Done(feature) => feature,
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn base_is_develop_when_it_exists() {
        let world = World::new();
        world.repository.add_branch(branch("develop"), commit("d0"));
        let feature = expect_done(world.drive());
        assert_eq!(feature.base_branch, branch("develop"));
        assert_eq!(feature.expected_remote_head, commit("d0"));
    }

    #[test]
    fn base_falls_back_to_the_default_branch() {
        let world = World::new();
        let feature = expect_done(world.drive());
        assert_eq!(feature.base_branch, branch("main"));
        assert_eq!(feature.expected_remote_head, commit("c0"));
    }

    #[test]
    fn full_run_creates_branch_and_draft_pull_request() {
        let world = World::new();
        let feature = expect_done(world.drive());

        assert_eq!(feature.specification, issue(4));
        assert_eq!(feature.feature_branch, feature_branch());
        assert!(world.repository.has_branch(&feature_branch()));
        assert_eq!(
            world.forge.is_draft(&feature.draft_pull_request),
            Some(true)
        );
        assert_eq!(world.creations(), 1);
        assert!(
            world
                .forge
                .calls()
                .contains(&ForgeCall::CreateDraftPullRequest {
                    head: feature_branch(),
                    base: branch("main"),
                    title: "Implement o/r#4".to_string(),
                    body: "Draft pull request for o/r#4.".to_string(),
                })
        );
        let saved = block_on(world.store.load_pipeline_state(&run(), "feature")).unwrap();
        assert_eq!(
            saved,
            Some(serde_json::to_value(FeatureState::Done(feature)).unwrap())
        );
    }

    fn saved_states() -> Vec<FeatureState> {
        let main = branch("main");
        let head = commit("c0");
        vec![
            FeatureState::SelectingBase,
            FeatureState::CheckingBranch { base: main.clone() },
            FeatureState::CreatingBranch { base: main.clone() },
            FeatureState::ReconcilingBranch { base: main.clone() },
            FeatureState::RecordingHead { base: main.clone() },
            FeatureState::CheckingPullRequest {
                base: main.clone(),
                head: head.clone(),
            },
            FeatureState::OpeningPullRequest {
                base: main.clone(),
                head: head.clone(),
            },
            FeatureState::ReconcilingPullRequest { base: main, head },
        ]
    }

    #[test]
    fn restart_from_every_saved_state_creates_nothing_twice() {
        let main = branch("main");
        // Whether the effects of the saved step already happened before the crash.
        for (index, state) in saved_states().iter().enumerate() {
            for branch_exists in [false, true] {
                for pull_request_exists in [false, true] {
                    let valid = (index > 0 || !branch_exists)
                        && (index < 4 || branch_exists)
                        && (!pull_request_exists || (branch_exists && index >= 5));
                    if !valid {
                        continue;
                    }
                    let world = World::new();
                    if branch_exists {
                        block_on(
                            world
                                .repository
                                .create_feature_branch(&feature_branch(), &main),
                        )
                        .unwrap();
                    }
                    if pull_request_exists {
                        block_on(world.forge.create_draft_pull_request(
                            &feature_branch(),
                            &main,
                            "t",
                            "b",
                        ))
                        .unwrap();
                    }
                    world.save(state);

                    let feature = expect_done(world.drive());

                    let context = format!("{state:?} {branch_exists} {pull_request_exists}");
                    assert_eq!(feature.expected_remote_head, commit("c0"), "{context}");
                    assert_eq!(
                        world.operations("create_feature_branch"),
                        usize::from(!branch_exists),
                        "{context}"
                    );
                    // Counts the setup's creation too: exactly one PR is ever created.
                    assert_eq!(world.creations(), 1, "{context}");
                    assert_eq!(
                        block_on(world.forge.find_open_pull_request(&feature_branch())).unwrap(),
                        Some(feature.draft_pull_request),
                        "{context}"
                    );
                }
            }
        }
    }

    #[test]
    fn every_step_performs_one_external_operation_and_survives_a_restart() {
        let reference = World::new();
        let pipeline = reference.pipeline();
        let mut transitions = vec![FeatureState::SelectingBase];
        while !transitions.last().unwrap().is_terminal() {
            let before = reference.effects();
            let next = block_on(pipeline.step(transitions.last().unwrap().clone())).unwrap();
            assert_eq!(reference.effects(), before + 1, "{next:?}");
            transitions.push(next);
        }
        assert_eq!(transitions.len(), 8);

        // Crash after each saved transition: the restarted run performs the same effects.
        for saved in &transitions {
            let world = World::new();
            let pipeline = world.pipeline();
            let mut replay = FeatureState::SelectingBase;
            while replay != *saved {
                replay = block_on(pipeline.step(replay)).unwrap();
            }
            world.save(&replay);

            expect_done(world.drive());

            assert_eq!(world.operations("create_feature_branch"), 1, "{saved:?}");
            assert_eq!(world.creations(), 1, "{saved:?}");
        }
    }

    #[test]
    fn failed_effect_is_retried_while_the_policy_permits() {
        let world = World::new();
        // Two failures of base selection, then one of the pull request lookup.
        world.repository.fail_next(PortError::failed("first"));
        world.repository.fail_next(PortError::failed("second"));
        world.forge.fail_next(PortError::failed("find"));

        let feature = expect_done(world.drive());

        assert_eq!(feature.base_branch, branch("main"));
        assert_eq!(world.creations(), 1);
        assert_eq!(
            world.policy.snapshot().github_retries_remaining,
            Limits::default().github_retries - 3
        );
    }

    #[test]
    fn failed_effect_pauses_when_the_retry_budget_is_used_up() {
        let world = World::with_limits(Limits {
            github_retries: 0,
            ..Limits::default()
        });
        world.repository.fail_next(PortError::failed("down"));

        let paused = world.drive();

        assert_eq!(
            paused,
            FeatureState::Paused {
                reason: PauseReason::GithubRetriesExhausted,
                resume_at: Box::new(FeatureState::SelectingBase),
            }
        );
        assert!(!world.repository.has_branch(&feature_branch()));
        assert_eq!(world.drive(), paused);
    }

    #[test]
    fn uncertain_branch_creation_is_reconciled_before_retrying() {
        let world = World::new();
        world.lost.lose_branch.store(true, Ordering::SeqCst);

        let feature = expect_done(world.drive());

        assert_eq!(feature.feature_branch, feature_branch());
        assert_eq!(world.creations(), 1);
    }

    #[test]
    fn failed_lookup_never_creates_an_existing_branch() {
        let world = World::new();
        block_on(
            world
                .repository
                .create_feature_branch(&feature_branch(), &branch("main")),
        )
        .unwrap();
        world.save(&FeatureState::CheckingBranch {
            base: branch("main"),
        });
        world
            .repository
            .fail_next(PortError::failed("connection reset"));

        let feature = expect_done(world.drive());

        assert_eq!(feature.expected_remote_head, commit("c0"));
        assert_eq!(world.operations("create_feature_branch"), 0);
        assert_eq!(world.operations("remote_head"), 3);
    }

    #[test]
    fn failed_lookup_with_no_retries_left_pauses_without_creating() {
        let world = World::with_limits(Limits {
            github_retries: 0,
            ..Limits::default()
        });
        block_on(
            world
                .repository
                .create_feature_branch(&feature_branch(), &branch("main")),
        )
        .unwrap();
        let checking = FeatureState::CheckingBranch {
            base: branch("main"),
        };
        world.save(&checking);
        world
            .repository
            .fail_next(PortError::failed("connection reset"));

        assert_eq!(
            world.drive(),
            FeatureState::Paused {
                reason: PauseReason::GithubRetriesExhausted,
                resume_at: Box::new(checking),
            }
        );
        assert_eq!(world.operations("create_feature_branch"), 0);
    }

    #[test]
    fn lost_responses_need_no_retry_when_the_effect_happened() {
        let world = World::with_limits(Limits {
            github_retries: 0,
            ..Limits::default()
        });
        world.lost.lose_branch.store(true, Ordering::SeqCst);
        world.lost.lose_pull_request.store(true, Ordering::SeqCst);

        let feature = expect_done(world.drive());

        assert_eq!(world.operations("create_feature_branch"), 1);
        assert_eq!(world.creations(), 1);
        assert_eq!(
            block_on(world.forge.find_open_pull_request(&feature_branch())).unwrap(),
            Some(feature.draft_pull_request)
        );
        assert_eq!(world.policy.snapshot().github_retries_remaining, 0);
    }

    /// Steps `state` with `pipeline` until the pipeline is done or paused.
    fn run_steps(pipeline: &FeaturePipeline, mut state: FeatureState) -> FeatureState {
        while !state.is_terminal() && !state.is_paused() {
            state = block_on(pipeline.step(state)).unwrap();
        }
        state
    }

    #[test]
    fn uncertain_creation_that_did_not_happen_is_repeated_only_with_permission() {
        let main = branch("main");
        for retries in [0, 1] {
            let limits = Limits {
                github_retries: retries,
                ..Limits::default()
            };

            // The lookup finds no branch, then the creation reports a lost response without
            // having happened.
            let world = World::with_limits(limits);
            let pipeline = world.pipeline();
            let creating =
                block_on(pipeline.step(FeatureState::CheckingBranch { base: main.clone() }))
                    .unwrap();
            assert_eq!(
                creating,
                FeatureState::CreatingBranch { base: main.clone() }
            );
            world.repository.fail_next(PortError::uncertain("lost"));
            let outcome = run_steps(&pipeline, creating);
            check_unperformed_creation(
                &world,
                retries,
                outcome,
                FeatureState::ReconcilingBranch { base: main.clone() },
                |world| world.repository.has_branch(&feature_branch()),
            );

            let world = World::with_limits(limits);
            block_on(
                world
                    .repository
                    .create_feature_branch(&feature_branch(), &main),
            )
            .unwrap();
            let checking = FeatureState::CheckingPullRequest {
                base: main.clone(),
                head: commit("c0"),
            };
            let pipeline = world.pipeline();
            let opening = block_on(pipeline.step(checking)).unwrap();
            world.forge.fail_next(PortError::uncertain("lost"));
            let outcome = run_steps(&pipeline, opening);
            check_unperformed_creation(
                &world,
                retries,
                outcome,
                FeatureState::ReconcilingPullRequest {
                    base: main.clone(),
                    head: commit("c0"),
                },
                |world| {
                    block_on(world.forge.find_open_pull_request(&feature_branch()))
                        .unwrap()
                        .is_some()
                },
            );
        }
    }

    fn check_unperformed_creation(
        world: &World,
        retries: u32,
        outcome: FeatureState,
        reconciling: FeatureState,
        created: impl Fn(&World) -> bool,
    ) {
        if retries == 0 {
            assert_eq!(
                outcome,
                FeatureState::Paused {
                    reason: PauseReason::GithubRetriesExhausted,
                    resume_at: Box::new(reconciling),
                }
            );
            assert!(!created(world));
        } else {
            expect_done(outcome);
            assert!(created(world));
            assert_eq!(world.policy.snapshot().github_retries_remaining, 0);
        }
    }

    #[test]
    fn uncertain_pull_request_creation_is_reconciled_before_retrying() {
        let world = World::new();
        world.lost.lose_pull_request.store(true, Ordering::SeqCst);

        let feature = expect_done(world.drive());

        assert_eq!(world.creations(), 1);
        assert_eq!(
            block_on(world.forge.find_open_pull_request(&feature_branch())).unwrap(),
            Some(feature.draft_pull_request)
        );
    }

    #[test]
    fn global_pause_stops_before_the_step_and_resumes_later() {
        let world = World::new();
        world.policy.pause(PauseReason::GlobalPause);

        let paused = world.drive();

        assert_eq!(
            paused,
            FeatureState::Paused {
                reason: PauseReason::GlobalPause,
                resume_at: Box::new(FeatureState::SelectingBase),
            }
        );
        assert!(world.forge.calls().is_empty());
        assert!(!world.repository.has_branch(&feature_branch()));

        // A restart with the pause lifted continues from the paused step.
        let resumed = World {
            policy: Arc::new(Policy::new(&Limits::default())),
            ..world
        };
        resumed.save(&paused.resume());
        expect_done(resumed.drive());
        assert_eq!(resumed.creations(), 1);
    }

    #[test]
    fn global_pause_mid_run_pauses_at_the_next_step() {
        let world = World::new();
        let pipeline = world.pipeline();
        let after_base = block_on(pipeline.step(FeatureState::SelectingBase)).unwrap();
        world.policy.pause(PauseReason::GlobalPause);

        let paused = block_on(pipeline.step(after_base.clone())).unwrap();

        assert_eq!(
            paused,
            FeatureState::Paused {
                reason: PauseReason::GlobalPause,
                resume_at: Box::new(after_base),
            }
        );
        assert!(!world.repository.has_branch(&feature_branch()));
    }

    #[test]
    fn state_round_trips_through_json() {
        let state = FeatureState::Paused {
            reason: PauseReason::GlobalPause,
            resume_at: Box::new(FeatureState::CheckingPullRequest {
                base: branch("main"),
                head: commit("c0"),
            }),
        };
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!(serde_json::from_value::<FeatureState>(json).unwrap(), state);
    }
}
