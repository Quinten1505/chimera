use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::repository::Repository;
use chimera_core::{BranchName, CommitId, Feature, Specification};
use serde::{Deserialize, Serialize};

use crate::driver::{Pipeline, PipelineState};
use crate::error::{PauseReason, PipelineError};
use crate::policy::{Budget, Policy, RetryRefused};

/// State of the feature pipeline; each step performs one external effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureState {
    SelectingBase,
    CreatingBranch {
        base: BranchName,
    },
    /// The branch exists; read its head to record as the expected remote head.
    RecordingHead {
        base: BranchName,
    },
    OpeningPullRequest {
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

/// Specification -> Feature: selects the base, creates the feature branch and opens the draft PR.
///
/// Every step that creates something first looks for what an earlier, possibly lost, attempt
/// already created, so a retry or restart never duplicates the branch or the pull request.
pub struct FeaturePipeline {
    specification: Specification,
    feature_branch: BranchName,
    repository: Arc<dyn Repository>,
    forge: Arc<dyn Forge>,
    policy: Arc<Policy>,
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
        }
    }

    async fn advance(&self, state: &FeatureState) -> Result<FeatureState, PortError> {
        match state {
            FeatureState::SelectingBase => Ok(FeatureState::CreatingBranch {
                base: self.repository.resolve_base_branch().await?,
            }),
            FeatureState::CreatingBranch { base } => {
                // A failed lookup means the branch is absent; an uncertain one is retried.
                match self.repository.remote_head(&self.feature_branch).await {
                    Ok(_) => {}
                    Err(error) if error.is_failed() => {
                        self.repository
                            .create_feature_branch(&self.feature_branch, base)
                            .await?;
                    }
                    Err(error) => return Err(error),
                }
                Ok(FeatureState::RecordingHead { base: base.clone() })
            }
            FeatureState::RecordingHead { base } => Ok(FeatureState::OpeningPullRequest {
                base: base.clone(),
                head: self.repository.remote_head(&self.feature_branch).await?,
            }),
            FeatureState::OpeningPullRequest { base, head } => {
                let draft_pull_request = match self
                    .forge
                    .find_open_pull_request(&self.feature_branch)
                    .await?
                {
                    Some(existing) => existing,
                    None => {
                        self.forge
                            .create_draft_pull_request(
                                &self.feature_branch,
                                base,
                                &format!("Implement {}", self.specification.issue),
                                &format!("Draft pull request for {}.", self.specification.issue),
                            )
                            .await?
                    }
                };
                Ok(FeatureState::Done(Feature {
                    specification: self.specification.issue.clone(),
                    base_branch: base.clone(),
                    feature_branch: self.feature_branch.clone(),
                    expected_remote_head: head.clone(),
                    draft_pull_request,
                }))
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

    /// Pauses instead of acting while the run is paused. A failed effect is retried (the state
    /// is returned unchanged) only while the policy permits; otherwise the pipeline pauses. A
    /// retry is always preceded by the reconciliation that opens each step.
    async fn step(&self, state: FeatureState) -> Result<FeatureState, PipelineError> {
        if let Err(reason) = self.policy.check_start() {
            return Ok(FeatureState::Paused {
                reason,
                resume_at: Box::new(state),
            });
        }
        match self.advance(&state).await {
            Ok(next) => Ok(next),
            Err(error) => match self.policy.permit_retry(Budget::GithubRetry, &error, true) {
                Ok(()) => Ok(state),
                Err(RetryRefused::Paused(reason)) => Ok(FeatureState::Paused {
                    reason,
                    resume_at: Box::new(state),
                }),
                Err(RetryRefused::NeedsReconciliation) => Err(error.into()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::{FakeRunStore, RunStore};
    use chimera_core::{IssueRef, Limits, RunId, TicketPlan};
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
    }

    #[async_trait]
    impl Repository for LostResponse {
        async fn resolve_base_branch(&self) -> Result<BranchName, PortError> {
            self.repository.resolve_base_branch().await
        }

        async fn create_feature_branch(
            &self,
            feature: &BranchName,
            base: &BranchName,
        ) -> Result<(), PortError> {
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

        async fn remote_head(&self, branch: &BranchName) -> Result<CommitId, PortError> {
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
            self.forge
                .create_draft_pull_request(head, base, title, body)
                .await?;
            if self.lose_pull_request.swap(false, Ordering::SeqCst) {
                return Err(PortError::uncertain("response lost"));
            }
            self.forge
                .find_open_pull_request(head)
                .await
                .map(|found| found.unwrap())
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

        async fn mark_pull_request_ready(&self, pull_request: &IssueRef) -> Result<(), PortError> {
            self.forge.mark_pull_request_ready(pull_request).await
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

    #[test]
    fn restart_from_every_saved_state_creates_nothing_twice() {
        let main = branch("main");
        let saved_states = [
            FeatureState::SelectingBase,
            FeatureState::CreatingBranch { base: main.clone() },
            FeatureState::RecordingHead { base: main.clone() },
            FeatureState::OpeningPullRequest {
                base: main.clone(),
                head: commit("c0"),
            },
        ];
        // Whether the effect of the saved step already happened before the crash.
        for (index, state) in saved_states.iter().enumerate() {
            for effect_happened in [false, true] {
                let world = World::new();
                if index >= 2 || (index == 1 && effect_happened) {
                    block_on(
                        world
                            .repository
                            .create_feature_branch(&feature_branch(), &main),
                    )
                    .unwrap();
                }
                if index == 3 && effect_happened {
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

                assert_eq!(feature.expected_remote_head, commit("c0"), "{state:?}");
                // Counts the setup's creation too: exactly one PR is ever created.
                assert_eq!(world.creations(), 1, "{state:?}");
                assert_eq!(
                    block_on(world.forge.find_open_pull_request(&feature_branch())).unwrap(),
                    Some(feature.draft_pull_request),
                    "{state:?}"
                );
            }
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
            resume_at: Box::new(FeatureState::OpeningPullRequest {
                base: branch("main"),
                head: commit("c0"),
            }),
        };
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!(serde_json::from_value::<FeatureState>(json).unwrap(), state);
    }
}
