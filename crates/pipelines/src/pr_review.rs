use std::future::Future;
use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::repository::Repository;
use chimera_core::run_store::RunStore;
use chimera_core::{
    AgentConfiguration, AgentId, Feature, IssueRef, Outcome, Role, RunId, TurnOutcome, TurnResult,
};
use serde::{Deserialize, Serialize};

use crate::agent_turn::{AgentTurns, TurnError, TurnRequest};
use crate::driver::{Pipeline, PipelineState, drive};
use crate::environment::{Environment, EnvironmentService, ProvisionSpec};
use crate::error::{PauseReason, PipelineError};
use crate::implementation::{ImplementationPipeline, ImplementationState, expected_head_key};
use crate::policy::{Budget, Policy, RetryRefused};

/// The pull request is out of draft and the specification is closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrReady {
    pub pull_request: IssueRef,
}

/// State of the PR review pipeline. `cycle` numbers the reviews, starting at 1, and names the
/// review environment, which is provisioned afresh for every review so that it starts at the
/// latest verified feature head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrReviewState {
    Provisioning {
        cycle: u32,
    },
    Reviewing {
        cycle: u32,
    },
    /// The review asked for changes; an Implementation pipeline works on `findings`.
    Fixing {
        cycle: u32,
        findings: String,
    },
    /// The fix was merged; removes the review environment of `cycle` before the next review.
    Refreshing {
        cycle: u32,
    },
    ClosingSpecification {
        cycle: u32,
    },
    MarkingReady {
        cycle: u32,
    },
    CleaningUp {
        cycle: u32,
    },
    Done(PrReady),
    Paused {
        reason: PauseReason,
        resume_at: Box<PrReviewState>,
    },
}

impl PrReviewState {
    /// The state a paused pipeline continues from; any other state is returned unchanged.
    pub fn resume(self) -> Self {
        match self {
            Self::Paused { resume_at, .. } => *resume_at,
            other => other,
        }
    }
}

impl PipelineState for PrReviewState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }
}

/// Runs the Implementation pipeline that fixes the findings of a final review.
pub trait Implement: Sync {
    /// Runs the fix of review `cycle` until it is merged or paused.
    fn implement(
        &self,
        cycle: u32,
        findings: &str,
    ) -> impl Future<Output = Result<ImplementationState, PipelineError>> + Send;
}

/// Drives the [`ImplementationPipeline`] that `build` makes for a review cycle and its findings.
/// `build` must give the pipeline the findings as work item and the final configuration.
pub struct DriveImplementation<B> {
    pub store: Arc<dyn RunStore>,
    pub build: B,
}

impl<B> Implement for DriveImplementation<B>
where
    B: Fn(u32, &str) -> ImplementationPipeline + Sync,
{
    async fn implement(
        &self,
        cycle: u32,
        findings: &str,
    ) -> Result<ImplementationState, PipelineError> {
        let pipeline = (self.build)(cycle, findings);
        drive(
            self.store.as_ref(),
            &pipeline.run,
            &pipeline.instance.clone(),
            &pipeline,
        )
        .await
    }
}

/// Feature -> PR ready: reviews the whole branch against the specification, fixes findings
/// through the Implementation pipeline, and on approval closes the specification and marks the
/// pull request ready.
pub struct PrReviewPipeline<I> {
    pub run: RunId,
    /// Names this pipeline in the store and its review agent in the history.
    pub instance: String,
    pub feature: Feature,
    /// The final configuration.
    pub configuration: AgentConfiguration,
    /// Reviews allowed, corrections included.
    pub review_limit: u32,
    /// The worktree on the feature branch and the single review agent.
    pub spec: ProvisionSpec,
    pub store: Arc<dyn RunStore>,
    pub repository: Arc<dyn Repository>,
    pub forge: Arc<dyn Forge>,
    pub policy: Arc<Policy>,
    pub environment: Arc<EnvironmentService>,
    pub turns: Arc<AgentTurns>,
    pub implementation: I,
}

/// What a step that was interrupted may have left half done, saved before the effect runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingTurn {
    cycle: u32,
    /// What the review agent's pane showed before the prompt was sent.
    output_before: String,
    /// Invalid results in the history when the turn started; later ones are its corrections.
    invalid_before: usize,
    /// Corrections known to be delivered.
    corrections: u32,
    phase: TurnPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TurnPhase {
    /// The assignment is being sent: it may or may not have been delivered.
    Sending,
    /// The assignment or a correction was delivered; the result is awaited.
    Awaiting,
    /// A correction is being sent: it may or may not have been delivered.
    Correcting,
}

fn paused(reason: PauseReason, resume_at: PrReviewState) -> PrReviewState {
    PrReviewState::Paused {
        reason,
        resume_at: Box::new(resume_at),
    }
}

impl<I: Implement> PrReviewPipeline<I> {
    fn environment_key(&self, cycle: u32) -> String {
        format!("{}/environment/{cycle}", self.instance)
    }

    fn pending_key(&self) -> String {
        format!("{}/pending", self.instance)
    }

    fn agent(&self) -> AgentId {
        AgentId::new(format!("{}-Review", self.instance)).expect("instance is not blank")
    }

    async fn load_environment(&self, cycle: u32) -> Result<Environment, PipelineError> {
        let saved = self
            .store
            .load_pipeline_state(&self.run, &self.environment_key(cycle))
            .await?
            .ok_or_else(|| PipelineError::Environment("review is not provisioned".into()))?;
        Ok(serde_json::from_value(saved)?)
    }

    /// The turns of the review agent, oldest first.
    async fn history(&self) -> Result<Vec<TurnResult>, PipelineError> {
        let agent = self.agent();
        Ok(self
            .store
            .load_history(&self.run)
            .await?
            .into_iter()
            .filter(|turn| turn.agent == agent)
            .collect())
    }

    async fn load_pending(&self) -> Result<Option<PendingTurn>, PipelineError> {
        Ok(
            match self
                .store
                .load_pipeline_state(&self.run, &self.pending_key())
                .await?
            {
                Some(saved) => serde_json::from_value(saved)?,
                None => None,
            },
        )
    }

    async fn save_pending(&self, pending: Option<&PendingTurn>) -> Result<(), PipelineError> {
        self.store
            .save_pipeline_state(
                &self.run,
                &self.pending_key(),
                serde_json::to_value(pending)?,
            )
            .await?;
        Ok(())
    }

    /// Makes sure the review starts from the latest verified feature head: the head Chimera last
    /// verified must still be the remote one. The environment of each review is created from
    /// the feature branch, so a fresh environment is on that head.
    async fn provision(&self, cycle: u32) -> Result<PrReviewState, PipelineError> {
        let expected = match self
            .store
            .load_pipeline_state(&self.run, &expected_head_key(&self.feature.feature_branch))
            .await?
        {
            Some(saved) => serde_json::from_value(saved)?,
            None => self.feature.expected_remote_head.clone(),
        };
        let actual = self
            .repository
            .remote_head(&self.feature.feature_branch)
            .await?;
        let reason = match actual {
            Some(actual) => self.policy.check_remote_head(&expected, &actual).err(),
            None => {
                self.policy.pause(PauseReason::UnexpectedRemoteChange);
                Some(PauseReason::UnexpectedRemoteChange)
            }
        };
        if let Some(reason) = reason {
            // The driver saves no policy, so the pause is saved here.
            self.policy.save(self.store.as_ref(), &self.run).await?;
            return Ok(paused(reason, PrReviewState::Provisioning { cycle }));
        }
        self.environment
            .provision(
                self.store.as_ref(),
                &self.run,
                &self.environment_key(cycle),
                &self.spec,
            )
            .await?;
        Ok(PrReviewState::Reviewing { cycle })
    }

    /// Runs the review of `cycle` unless its result was already saved. A turn that was
    /// interrupted is reconciled with the agent instead of being sent again.
    async fn review(
        &self,
        state: PrReviewState,
        cycle: u32,
    ) -> Result<PrReviewState, PipelineError> {
        let history = self.history().await?;
        let valid: Vec<&Outcome> = history
            .iter()
            .filter_map(|turn| match &turn.outcome {
                TurnOutcome::Valid(outcome) => Some(outcome),
                TurnOutcome::Invalid { .. } => None,
            })
            .collect();
        let outcome = match valid.get(cycle as usize - 1) {
            Some(outcome) => (*outcome).clone(),
            None => {
                let previous = (cycle > 1).then(|| valid[cycle as usize - 2]);
                match self.run_turn(&state, cycle, &history, previous).await? {
                    Ok(outcome) => outcome,
                    Err(next) => return Ok(next),
                }
            }
        };
        Ok(match outcome {
            Outcome::ReviewApproved(_) => PrReviewState::ClosingSpecification { cycle },
            // The next review would exceed the limit, so the fix is not started.
            Outcome::ChangesRequested(findings) => {
                let fixing = PrReviewState::Fixing { cycle, findings };
                if cycle >= self.review_limit {
                    paused(PauseReason::LimitExhausted, fixing)
                } else {
                    fixing
                }
            }
            other => unreachable!("{other:?} is not valid for the Review role"),
        })
    }

    /// Starts the review turn, or continues it after a restart, and returns its outcome. When
    /// the turn cannot finish now, returns the state to continue from instead.
    async fn run_turn(
        &self,
        state: &PrReviewState,
        cycle: u32,
        history: &[TurnResult],
        previous: Option<&Outcome>,
    ) -> Result<Result<Outcome, PrReviewState>, PipelineError> {
        let existing = self
            .load_pending()
            .await?
            .filter(|pending| pending.cycle == cycle);
        let invalid = |history: &[TurnResult]| {
            history
                .iter()
                .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                .count()
        };
        let invalid_before = existing
            .as_ref()
            .map_or_else(|| invalid(history), |pending| pending.invalid_before);
        // Every review and every correction counts toward the limit.
        let used = cycle as usize + invalid_before;
        if used > self.review_limit as usize {
            return Ok(Err(paused(PauseReason::LimitExhausted, state.clone())));
        }
        let max_corrections = (self.review_limit as usize - used) as u32;

        let environment = self.load_environment(cycle).await?;
        let pane = environment.pane(Role::Review).ok_or_else(|| {
            PipelineError::Environment("no review agent in the environment".into())
        })?;
        let description = previous.map(|outcome| match outcome {
            Outcome::ChangesRequested(findings) => {
                format!("Your previous findings were fixed and merged:\n{findings}")
            }
            _ => String::new(),
        });
        let agent = self.agent();
        let request = TurnRequest {
            run: &self.run,
            agent: &agent,
            pane,
            role: Role::Review,
            profile: self.configuration.profile(Role::Review),
            issue: &self.feature.specification,
            previous_description: description.as_deref(),
        };

        let created = existing.is_none();
        let mut turn = match existing {
            Some(turn) => turn,
            None => PendingTurn {
                cycle,
                output_before: self.turns.read_output(pane).await?,
                invalid_before,
                corrections: 0,
                phase: TurnPhase::Sending,
            },
        };
        if created {
            self.save_pending(Some(&turn)).await?;
        }
        if turn.phase == TurnPhase::Sending {
            if created {
                if let Err(error) = self.turns.send_assignment(&request).await {
                    // A definite failure means nothing was delivered.
                    if !matches!(&error, TurnError::Port(port) if port.is_uncertain()) {
                        self.save_pending(None).await?;
                    }
                    return self.turn_failed(error, state, &environment).await;
                }
            } else if let Err(error) = self.confirm_delivery(&request, &turn, "assignment").await {
                return self.turn_failed(error, state, &environment).await;
            }
            turn.phase = TurnPhase::Awaiting;
            self.save_pending(Some(&turn)).await?;
        }
        loop {
            if turn.phase == TurnPhase::Correcting {
                if let Err(error) = self.confirm_delivery(&request, &turn, "correction").await {
                    return self.turn_failed(error, state, &environment).await;
                }
                turn.corrections += 1;
                turn.phase = TurnPhase::Awaiting;
                self.save_pending(Some(&turn)).await?;
            }
            let collected = match self.turns.collect(&request).await {
                Ok(collected) => collected,
                Err(error) => return self.turn_failed(error, state, &environment).await,
            };
            // A crash after the result was saved but before the phase moved on leaves the
            // result in the history: saving it again would count it twice.
            let saved = invalid(&self.history().await?) - turn.invalid_before;
            if collected.parsed.is_ok() || saved <= turn.corrections as usize {
                self.turns.record(&request, &collected).await?;
            }
            let problem = match collected.parsed {
                Ok(outcome) => {
                    self.save_pending(None).await?;
                    return Ok(Ok(outcome));
                }
                Err(problem) => problem,
            };
            if turn.corrections >= max_corrections {
                self.save_pending(None).await?;
                return Ok(Err(paused(PauseReason::LimitExhausted, state.clone())));
            }
            turn.phase = TurnPhase::Correcting;
            turn.output_before = collected.output;
            self.save_pending(Some(&turn)).await?;
            if let Err(error) = self.turns.send_correction(&request, &problem).await {
                if !error.is_uncertain() {
                    // Not delivered: the saved result is corrected on the next attempt.
                    turn.phase = TurnPhase::Awaiting;
                    self.save_pending(Some(&turn)).await?;
                }
                return Err(error.into());
            }
            turn.corrections += 1;
            turn.phase = TurnPhase::Awaiting;
            self.save_pending(Some(&turn)).await?;
        }
    }

    /// A prompt sent before a restart is known to have arrived only if the agent is working or
    /// its output changed.
    async fn confirm_delivery(
        &self,
        request: &TurnRequest<'_>,
        turn: &PendingTurn,
        what: &str,
    ) -> Result<(), TurnError> {
        if self
            .turns
            .delivered(request.pane, &turn.output_before)
            .await?
        {
            Ok(())
        } else {
            let unknown = format!("the {what} may or may not have been delivered");
            Err(PortError::uncertain(unknown).into())
        }
    }

    /// Recovers a lost agent, or reports why the turn cannot go on.
    async fn turn_failed(
        &self,
        error: TurnError,
        state: &PrReviewState,
        environment: &Environment,
    ) -> Result<Result<Outcome, PrReviewState>, PipelineError> {
        match error {
            TurnError::AgentLost => {
                let command_line = &self
                    .spec
                    .agents
                    .iter()
                    .find(|launch| launch.role == Role::Review)
                    .ok_or_else(|| PipelineError::Environment("no review launch in spec".into()))?
                    .command_line;
                self.environment
                    .relaunch(environment, Role::Review, command_line)
                    .await?;
                self.save_pending(None).await?;
                Ok(Err(state.clone()))
            }
            TurnError::CorrectionsExhausted { .. } => {
                self.save_pending(None).await?;
                Ok(Err(paused(PauseReason::LimitExhausted, state.clone())))
            }
            TurnError::Paused(reason) => {
                // Nothing was sent.
                self.save_pending(None).await?;
                Err(PipelineError::Paused(reason))
            }
            other => Err(other.into()),
        }
    }

    /// Runs the fix; the review environment stays until the fix is merged.
    async fn fix(
        &self,
        state: &PrReviewState,
        cycle: u32,
        findings: &str,
    ) -> Result<PrReviewState, PipelineError> {
        Ok(
            match self.implementation.implement(cycle, findings).await? {
                ImplementationState::Done(_) => PrReviewState::Refreshing { cycle },
                ImplementationState::Paused { reason, .. } => paused(reason, state.clone()),
                other => unreachable!("the Implementation pipeline stopped in {other:?}"),
            },
        )
    }

    /// Closes the workspace and removes the worktree of `cycle`. What was removed is saved even
    /// if a later removal fails, so a retry continues where this one stopped.
    async fn clean_up(&self, cycle: u32) -> Result<(), PipelineError> {
        let mut environment = self.load_environment(cycle).await?;
        let cleaned = self.environment.cleanup(&mut environment).await;
        self.store
            .save_pipeline_state(
                &self.run,
                &self.environment_key(cycle),
                serde_json::to_value(&environment)?,
            )
            .await?;
        cleaned
    }

    /// Performs a forge effect once per run: an effect that is recorded as done is skipped. One
    /// whose outcome was not recorded is repeated, which is harmless for closing an issue and
    /// for marking a pull request ready.
    async fn once(
        &self,
        name: &str,
        effect: impl Future<Output = Result<(), PortError>>,
    ) -> Result<(), PortError> {
        let key = format!("{}/{name}", self.instance);
        let effects = self.store.load_effects(&self.run).await?;
        match effects.iter().find(|record| record.key == key) {
            Some(record) if record.outcome.is_some() => return Ok(()),
            Some(_) => {}
            None => {
                self.store
                    .record_effect_intent(&self.run, &key, name)
                    .await?
            }
        }
        effect.await?;
        self.store
            .record_effect_outcome(&self.run, &key, "done")
            .await
    }

    async fn advance(&self, state: &PrReviewState) -> Result<PrReviewState, PipelineError> {
        match state {
            PrReviewState::Provisioning { cycle } => self.provision(*cycle).await,
            PrReviewState::Reviewing { cycle } => self.review(state.clone(), *cycle).await,
            PrReviewState::Fixing { cycle, findings } => self.fix(state, *cycle, findings).await,
            PrReviewState::Refreshing { cycle } => {
                self.clean_up(*cycle).await?;
                Ok(PrReviewState::Provisioning { cycle: cycle + 1 })
            }
            PrReviewState::ClosingSpecification { cycle } => {
                self.once(
                    "close-specification",
                    self.forge.close_issue(&self.feature.specification),
                )
                .await?;
                Ok(PrReviewState::MarkingReady { cycle: *cycle })
            }
            PrReviewState::MarkingReady { cycle } => {
                self.once(
                    "mark-ready",
                    self.forge
                        .mark_pull_request_ready(&self.feature.draft_pull_request),
                )
                .await?;
                Ok(PrReviewState::CleaningUp { cycle: *cycle })
            }
            PrReviewState::CleaningUp { cycle } => {
                self.clean_up(*cycle).await?;
                Ok(PrReviewState::Done(PrReady {
                    pull_request: self.feature.draft_pull_request.clone(),
                }))
            }
            PrReviewState::Done(_) | PrReviewState::Paused { .. } => Ok(state.clone()),
        }
    }
}

impl<I: Implement> Pipeline for PrReviewPipeline<I> {
    type State = PrReviewState;

    fn initial_state(&self) -> PrReviewState {
        PrReviewState::Provisioning { cycle: 1 }
    }

    /// Pauses instead of acting while the run is paused. A failed forge effect is retried only
    /// while the policy permits, otherwise the pipeline pauses; the effects are safe to repeat,
    /// so an uncertain one counts as reconciled.
    async fn step(&self, state: PrReviewState) -> Result<PrReviewState, PipelineError> {
        if let Err(reason) = self.policy.check_start() {
            return Ok(paused(reason, state));
        }
        match self.advance(&state).await {
            Ok(next) => Ok(next),
            Err(PipelineError::Paused(reason)) => Ok(paused(reason, state)),
            Err(PipelineError::Port(error))
                if matches!(
                    state,
                    PrReviewState::ClosingSpecification { .. } | PrReviewState::MarkingReady { .. }
                ) =>
            {
                match self.policy.permit_retry(Budget::GithubRetry, &error, true) {
                    Ok(()) => Ok(state),
                    Err(RetryRefused::Paused(reason)) => Ok(paused(reason, state)),
                    Err(RetryRefused::NeedsReconciliation) => Err(error.into()),
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::terminal::{FakeTerminal, Terminal, TurnStatus};
    use chimera_core::{AgentProfile, BranchName, CommitId, Limits, MergedOk, PaneId, WorkspaceId};
    use futures_executor::block_on;

    use super::*;
    use crate::environment::AgentLaunch;

    const APPROVED: &str = r#"{"ReviewApproved":"fine"}"#;
    const FINDINGS: &str = r#"{"ChangesRequested":"fix it"}"#;

    /// Answers each prompt to a pane with that pane's next scripted reply and finishes the turn.
    struct ScriptedTerminal {
        inner: FakeTerminal,
        replies: Mutex<VecDeque<String>>,
        launches: Mutex<usize>,
        sent: Mutex<HashMap<PaneId, Vec<String>>>,
    }

    #[async_trait]
    impl Terminal for ScriptedTerminal {
        async fn create_workspace(
            &self,
            directory: &Path,
        ) -> Result<(WorkspaceId, PaneId), PortError> {
            self.inner.create_workspace(directory).await
        }
        async fn split_pane(
            &self,
            workspace: &WorkspaceId,
            pane: &PaneId,
        ) -> Result<PaneId, PortError> {
            self.inner.split_pane(workspace, pane).await
        }
        async fn launch_agent(&self, pane: &PaneId, command_line: &str) -> Result<(), PortError> {
            *self.launches.lock().unwrap() += 1;
            self.inner.launch_agent(pane, command_line).await
        }
        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            if let Some(reply) = self.replies.lock().unwrap().pop_front() {
                self.inner.script_output(pane, reply);
                self.inner.script_statuses(pane, [TurnStatus::Finished]);
            }
            self.sent
                .lock()
                .unwrap()
                .entry(pane.clone())
                .or_default()
                .push(prompt.to_string());
            self.inner.send_prompt(pane, prompt).await
        }
        async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
            self.inner.read_status(pane).await
        }
        async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
            self.inner.read_output(pane).await
        }
        async fn close_workspace(&self, workspace: &WorkspaceId) -> Result<(), PortError> {
            self.inner.close_workspace(workspace).await
        }
    }

    /// Stands in for the Implementation pipeline: records the findings and, like a merge, moves
    /// the verified feature head on.
    struct FakeImplement {
        store: Arc<FakeRunStore>,
        repository: Arc<FakeRepository>,
        calls: Mutex<Vec<(u32, String)>>,
        pause: Mutex<bool>,
    }

    impl Implement for FakeImplement {
        async fn implement(
            &self,
            cycle: u32,
            findings: &str,
        ) -> Result<ImplementationState, PipelineError> {
            let call = {
                let mut calls = self.calls.lock().unwrap();
                calls.push((cycle, findings.to_string()));
                calls.len()
            };
            if *self.pause.lock().unwrap() {
                return Ok(ImplementationState::Paused {
                    reason: PauseReason::LimitExhausted,
                    resume_at: Box::new(ImplementationState::Provisioning),
                });
            }
            let head = CommitId::new(format!("c{call}")).unwrap();
            self.repository
                .set_remote_head(branch("feat"), head.clone());
            self.store
                .save_pipeline_state(
                    &run(),
                    &expected_head_key(&branch("feat")),
                    serde_json::to_value(&head)?,
                )
                .await?;
            Ok(ImplementationState::Done(MergedOk { commit: head }))
        }
    }

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    fn run() -> RunId {
        RunId::new("run").unwrap()
    }

    fn specification() -> IssueRef {
        IssueRef::new("o", "r", 4).unwrap()
    }

    fn pull_request() -> IssueRef {
        IssueRef::new("o", "r", 1000).unwrap()
    }

    struct Fixture {
        terminal: Arc<ScriptedTerminal>,
        store: Arc<FakeRunStore>,
        repository: Arc<FakeRepository>,
        forge: Arc<FakeForge>,
        policy: Arc<Policy>,
        implement: Arc<FakeImplement>,
        limit: u32,
    }

    fn fixture(limit: u32, replies: &[&str]) -> Fixture {
        let repository = Arc::new(FakeRepository::new(
            branch("main"),
            CommitId::new("c0").unwrap(),
        ));
        repository.add_branch(branch("feat"), CommitId::new("c0").unwrap());
        let forge = Arc::new(FakeForge::new("o", "r"));
        forge.add_open_pull_request(pull_request(), branch("feat"));
        let store = Arc::new(FakeRunStore::new());
        Fixture {
            terminal: Arc::new(ScriptedTerminal {
                inner: FakeTerminal::new(),
                replies: Mutex::new(replies.iter().map(|r| r.to_string()).collect()),
                launches: Mutex::new(0),
                sent: Mutex::default(),
            }),
            implement: Arc::new(FakeImplement {
                store: store.clone(),
                repository: repository.clone(),
                calls: Mutex::default(),
                pause: Mutex::new(false),
            }),
            store,
            repository,
            forge,
            policy: Arc::new(Policy::new(&Limits::default())),
            limit,
        }
    }

    /// Wraps the shared fake [`FakeImplement`] so that a pipeline can be rebuilt after a restart.
    struct Shared(Arc<FakeImplement>);

    impl Implement for Shared {
        fn implement(
            &self,
            cycle: u32,
            findings: &str,
        ) -> impl Future<Output = Result<ImplementationState, PipelineError>> + Send {
            self.0.implement(cycle, findings)
        }
    }

    impl Fixture {
        /// A pipeline over the shared fakes; building another one simulates a restart.
        fn pipeline(&self) -> PrReviewPipeline<Shared> {
            let profile = |name: &str| AgentProfile::new("p", "m", format!("You are {name}."));
            let environment = Arc::new(EnvironmentService::new(
                self.repository.clone(),
                self.terminal.clone(),
                self.policy.clone(),
            ));
            let turns = Arc::new(AgentTurns::new(
                self.terminal.clone(),
                self.store.clone(),
                self.policy.clone(),
                Duration::from_millis(1),
            ));
            PrReviewPipeline {
                run: run(),
                instance: "review".into(),
                feature: Feature {
                    specification: specification(),
                    base_branch: branch("main"),
                    feature_branch: branch("feat"),
                    expected_remote_head: CommitId::new("c0").unwrap(),
                    draft_pull_request: pull_request(),
                },
                configuration: AgentConfiguration {
                    implementation: profile("implementer"),
                    review: profile("reviewer"),
                    merge: profile("merger"),
                },
                review_limit: self.limit,
                spec: ProvisionSpec {
                    worktree: "/wt/review".into(),
                    task_branch: branch("review-task"),
                    feature: branch("feat"),
                    agents: vec![AgentLaunch {
                        role: Role::Review,
                        command_line: "agent Review".into(),
                    }],
                },
                store: self.store.clone(),
                repository: self.repository.clone(),
                forge: self.forge.clone(),
                policy: self.policy.clone(),
                environment,
                turns,
                implementation: Shared(self.implement.clone()),
            }
        }

        async fn drive(&self) -> Result<PrReviewState, PipelineError> {
            let pipeline = self.pipeline();
            drive(self.store.as_ref(), &run(), "review", &pipeline).await
        }

        /// Drives to the end, rebuilding the pipeline after every saved state.
        async fn drive_with_restarts(&self) -> PrReviewState {
            loop {
                let pipeline = self.pipeline();
                let state = match self
                    .store
                    .load_pipeline_state(&run(), "review")
                    .await
                    .unwrap()
                {
                    Some(saved) => serde_json::from_value(saved).unwrap(),
                    None => pipeline.initial_state(),
                };
                if state.is_terminal() || state.is_paused() {
                    return state;
                }
                let next = pipeline.step(state).await.unwrap();
                self.store
                    .save_pipeline_state(&run(), "review", serde_json::to_value(&next).unwrap())
                    .await
                    .unwrap();
            }
        }

        fn review_prompts(&self) -> Vec<String> {
            self.terminal
                .sent
                .lock()
                .unwrap()
                .values()
                .flatten()
                .cloned()
                .collect()
        }

        fn forge_effects(&self) -> Vec<ForgeCall> {
            self.forge
                .calls()
                .into_iter()
                .filter(|call| {
                    matches!(
                        call,
                        ForgeCall::CloseIssue(_) | ForgeCall::MarkPullRequestReady(_)
                    )
                })
                .collect()
        }
    }

    fn ready() -> PrReviewState {
        PrReviewState::Done(PrReady {
            pull_request: pull_request(),
        })
    }

    #[test]
    fn approval_on_the_first_review() {
        let f = fixture(5, &[APPROVED]);

        assert_eq!(block_on(f.drive()).unwrap(), ready());

        let prompts = f.review_prompts();
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("You are reviewer.") && prompts[0].contains("o/r#4"));
        assert!(f.implement.calls.lock().unwrap().is_empty());
        assert_eq!(*f.terminal.launches.lock().unwrap(), 1);
        assert_eq!(f.forge.is_draft(&pull_request()), Some(false));
        assert!(f.forge.is_closed(&specification()));
        // The review environment is cleaned up.
        assert_eq!(f.repository.worktree_branch(Path::new("/wt/review")), None);
    }

    #[test]
    fn findings_run_an_implementation_and_are_reviewed_again() {
        let f = fixture(5, &[FINDINGS, FINDINGS, APPROVED]);

        assert_eq!(block_on(f.drive()).unwrap(), ready());

        assert_eq!(
            *f.implement.calls.lock().unwrap(),
            vec![(1, "fix it".to_string()), (2, "fix it".to_string())]
        );
        let prompts = f.review_prompts();
        assert_eq!(prompts.len(), 3);
        assert!(prompts[1].contains("fix it"));
        // One single review agent at a time, started again on the new head each review.
        assert_eq!(*f.terminal.launches.lock().unwrap(), 3);
        assert_eq!(f.forge_effects().len(), 2);
    }

    #[test]
    fn each_review_starts_on_the_latest_verified_head() {
        let f = fixture(5, &[FINDINGS, APPROVED]);

        block_on(f.drive()).unwrap();

        // The environment of the second review was provisioned after the fix moved the head.
        assert_eq!(
            block_on(f.repository.remote_head(&branch("feat"))).unwrap(),
            Some(CommitId::new("c1").unwrap())
        );
        assert!(
            block_on(f.store.load_pipeline_state(&run(), "review/environment/2"))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn an_unexpected_remote_head_pauses_before_the_review() {
        let f = fixture(5, &[APPROVED]);
        f.repository
            .set_remote_head(branch("feat"), CommitId::new("other").unwrap());

        assert_eq!(
            block_on(f.drive()).unwrap(),
            paused(
                PauseReason::UnexpectedRemoteChange,
                PrReviewState::Provisioning { cycle: 1 }
            )
        );
        assert!(f.review_prompts().is_empty());
    }

    #[test]
    fn limit_exhaustion_pauses_and_keeps_the_draft() {
        let f = fixture(2, &[FINDINGS, FINDINGS, APPROVED]);

        let end = block_on(f.drive()).unwrap();

        assert_eq!(
            end,
            paused(
                PauseReason::LimitExhausted,
                PrReviewState::Fixing {
                    cycle: 2,
                    findings: "fix it".into()
                }
            )
        );
        assert_eq!(f.implement.calls.lock().unwrap().len(), 1);
        assert_eq!(f.forge.is_draft(&pull_request()), Some(true));
        assert!(!f.forge.is_closed(&specification()));
        assert!(f.forge_effects().is_empty());
        // A paused pipeline stays paused after a restart.
        assert_eq!(block_on(f.drive()).unwrap(), end);
    }

    #[test]
    fn corrections_count_toward_the_limit() {
        let f = fixture(2, &["no outcome", "still none", APPROVED]);

        assert_eq!(
            block_on(f.drive()).unwrap(),
            paused(
                PauseReason::LimitExhausted,
                PrReviewState::Reviewing { cycle: 1 }
            )
        );
        assert_eq!(f.forge.is_draft(&pull_request()), Some(true));
    }

    #[test]
    fn a_paused_fix_pauses_the_review() {
        let f = fixture(5, &[FINDINGS]);
        *f.implement.pause.lock().unwrap() = true;

        assert_eq!(
            block_on(f.drive()).unwrap(),
            paused(
                PauseReason::LimitExhausted,
                PrReviewState::Fixing {
                    cycle: 1,
                    findings: "fix it".into()
                }
            )
        );
    }

    #[test]
    fn the_specification_is_closed_before_the_pull_request_is_marked_ready() {
        let f = fixture(5, &[APPROVED]);

        block_on(f.drive()).unwrap();

        assert_eq!(
            f.forge_effects(),
            vec![
                ForgeCall::CloseIssue(specification()),
                ForgeCall::MarkPullRequestReady(pull_request())
            ]
        );
    }

    #[test]
    fn a_failed_mark_ready_keeps_the_close_from_repeating() {
        let f = fixture(5, &[APPROVED]);
        // The close succeeds; the mark-ready fails once and is retried.
        let pipeline = f.pipeline();
        let drive_to = |state: PrReviewState| block_on(pipeline.step(state)).unwrap();
        let state = drive_to(drive_to(PrReviewState::Provisioning { cycle: 1 }));
        assert_eq!(state, PrReviewState::ClosingSpecification { cycle: 1 });
        let state = drive_to(state);
        assert_eq!(state, PrReviewState::MarkingReady { cycle: 1 });
        f.forge.fail_next(PortError::failed("rate limited"));
        assert_eq!(drive_to(state.clone()), state);

        assert_eq!(block_on(f.drive_resumed(state)), ready());

        let effects = f.forge_effects();
        assert_eq!(
            effects
                .iter()
                .filter(|call| matches!(call, ForgeCall::CloseIssue(_)))
                .count(),
            1
        );
    }

    impl Fixture {
        async fn drive_resumed(&self, state: PrReviewState) -> PrReviewState {
            self.store
                .save_pipeline_state(&run(), "review", serde_json::to_value(&state).unwrap())
                .await
                .unwrap();
            self.drive().await.unwrap()
        }
    }

    #[test]
    fn restart_after_every_state_repeats_nothing() {
        let f = fixture(5, &[FINDINGS, APPROVED]);

        assert_eq!(block_on(f.drive_with_restarts()), ready());

        assert_eq!(f.review_prompts().len(), 2);
        assert_eq!(f.implement.calls.lock().unwrap().len(), 1);
        assert_eq!(f.forge_effects().len(), 2);
        assert_eq!(*f.terminal.launches.lock().unwrap(), 2);
    }

    #[test]
    fn restart_with_a_recorded_close_does_not_close_again() {
        let f = fixture(5, &[APPROVED]);
        let state = PrReviewState::ClosingSpecification { cycle: 1 };
        // A previous process closed the issue but crashed before saving the next state.
        block_on(async {
            f.store
                .record_effect_intent(&run(), "review/close-specification", "close-specification")
                .await
                .unwrap();
            f.store
                .record_effect_outcome(&run(), "review/close-specification", "done")
                .await
                .unwrap();
            f.pipeline()
                .environment
                .provision(
                    f.store.as_ref(),
                    &run(),
                    "review/environment/1",
                    &f.pipeline().spec,
                )
                .await
                .unwrap();
        });

        assert_eq!(block_on(f.drive_resumed(state)), ready());

        assert_eq!(
            f.forge_effects(),
            vec![ForgeCall::MarkPullRequestReady(pull_request())]
        );
    }

    #[test]
    fn restart_while_awaiting_a_review_does_not_send_it_again() {
        let f = fixture(5, &[APPROVED]);
        let pipeline = f.pipeline();
        block_on(async {
            let state = pipeline
                .step(PrReviewState::Provisioning { cycle: 1 })
                .await
                .unwrap();
            assert_eq!(state, PrReviewState::Reviewing { cycle: 1 });
            let environment = pipeline.load_environment(1).await.unwrap();
            let pane = environment.pane(Role::Review).unwrap().clone();
            // The prompt was delivered and the agent answered while Chimera was down.
            f.terminal.inner.script_output(&pane, APPROVED);
            f.terminal
                .inner
                .script_statuses(&pane, [TurnStatus::Finished]);
            pipeline
                .save_pending(Some(&PendingTurn {
                    cycle: 1,
                    output_before: String::new(),
                    invalid_before: 0,
                    corrections: 0,
                    phase: TurnPhase::Awaiting,
                }))
                .await
                .unwrap();
        });

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );

        assert!(f.review_prompts().is_empty());
    }

    #[test]
    fn a_saved_review_result_is_not_asked_for_again() {
        let f = fixture(5, &[APPROVED]);
        block_on(f.drive()).unwrap();
        let prompts = f.review_prompts().len();

        // Restarting from the review state finds the result in the history.
        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );

        assert_eq!(f.review_prompts().len(), prompts);
    }
}
