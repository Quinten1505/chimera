use std::future::Future;
use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::repository::Repository;
use chimera_core::run_store::RunStore;
use chimera_core::{
    AgentConfiguration, AgentId, CommitId, Feature, IssueRef, IssueStatus, Outcome, Role, RunId,
    TurnOutcome, TurnResult,
};
use serde::{Deserialize, Serialize};

use crate::agent_turn::{AgentTurns, TurnError, TurnRequest};
use crate::driver::{Pipeline, PipelineState};
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
    /// Advances the fix of review `cycle` by one step and returns the state it is in: `Done`,
    /// `Paused`, or a state to continue from on the next call.
    fn implement(
        &self,
        cycle: u32,
        findings: &str,
    ) -> impl Future<Output = Result<ImplementationState, PipelineError>> + Send;
}

/// Steps the [`ImplementationPipeline`] that `build` makes for a review cycle and its findings,
/// saving its state after each step, so every step of the outer pipeline performs one effect
/// of the fix. `build` must give the pipeline the findings as work item and the final
/// configuration.
///
/// A fix that is saved as paused is continued from where it paused: the outer pipeline only
/// asks for it again after it was resumed, while a plain restart of a paused review never
/// reaches the fix.
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
        let state = match self
            .store
            .load_pipeline_state(&pipeline.run, &pipeline.instance)
            .await?
        {
            Some(saved) => match serde_json::from_value(saved)? {
                ImplementationState::Paused { resume_at, .. } => *resume_at,
                state => state,
            },
            None => pipeline.initial_state(),
        };
        if state.is_terminal() {
            return Ok(state);
        }
        let next = pipeline.step(state).await?;
        self.store
            .save_pipeline_state(
                &pipeline.run,
                &pipeline.instance,
                serde_json::to_value(&next)?,
            )
            .await?;
        Ok(next)
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
    /// The prompts the review agent's pane had received before the prompt being sent: the
    /// receipt that tells whether it arrived.
    received_before: u64,
    /// Invalid results in the history when the turn started; later ones are its corrections.
    invalid_before: usize,
    /// Corrections known to be delivered.
    corrections: u32,
    phase: TurnPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TurnPhase {
    /// The assignment is being sent: it may or may not have been delivered, which the pane's
    /// receipt tells.
    Sending,
    /// The assignment or a correction was delivered; the result is awaited.
    Awaiting,
    /// A correction is being sent: it may or may not have been delivered, which the pane's
    /// receipt tells.
    Correcting,
    /// The agent was found gone and is being launched again: whether the launch happened is
    /// read from the agent's status. The turn starts over with the new agent.
    Relaunching,
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

    /// The feature head Chimera last verified.
    async fn expected_head(&self) -> Result<CommitId, PipelineError> {
        Ok(
            match self
                .store
                .load_pipeline_state(&self.run, &expected_head_key(&self.feature.feature_branch))
                .await?
            {
                Some(saved) => serde_json::from_value(saved)?,
                None => self.feature.expected_remote_head.clone(),
            },
        )
    }

    /// Provisions the review environment one effect per step, its worktree moved onto the latest
    /// verified feature head. Once it is ready, the review starts only if that head is still the
    /// remote one.
    async fn provision(&self, cycle: u32) -> Result<PrReviewState, PipelineError> {
        let key = self.environment_key(cycle);
        let environment =
            EnvironmentService::load(self.store.as_ref(), &self.run, &key, &self.spec).await?;
        let expected = self.expected_head().await?;
        if !environment.is_provisioned(&self.spec) {
            self.environment
                .provision_step(self.store.as_ref(), &self.run, &key, &self.spec, &expected)
                .await?;
            return Ok(PrReviewState::Provisioning { cycle });
        }
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
        Ok(match reason {
            Some(reason) => paused(reason, PrReviewState::Provisioning { cycle }),
            None => PrReviewState::Reviewing { cycle },
        })
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
                // The turn may have saved corrections since `history` was read.
                let corrections = self
                    .history()
                    .await?
                    .iter()
                    .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                    .count();
                if cycle as usize + corrections >= self.review_limit as usize {
                    paused(PauseReason::LimitExhausted, fixing)
                } else {
                    fixing
                }
            }
            other => unreachable!("{other:?} is not valid for the Review role"),
        })
    }

    /// Starts the review turn, or continues it, by at most one prompt and returns its outcome
    /// once it has one. Until then, or when the turn cannot finish now, returns the state to
    /// continue from instead.
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
        if existing
            .as_ref()
            .is_some_and(|turn| turn.phase == TurnPhase::Relaunching)
        {
            self.relaunch(cycle).await?;
            return Ok(Err(state.clone()));
        }
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
                received_before: self.turns.prompts_received(pane).await?,
                invalid_before,
                corrections: 0,
                phase: TurnPhase::Sending,
            },
        };
        if created {
            self.save_pending(Some(&turn)).await?;
        }
        if turn.phase == TurnPhase::Sending {
            // An assignment that was being sent arrived if the pane's receipt moved on since;
            // otherwise it is sent now, whether the earlier send never ran or failed.
            let delivered = if created {
                false
            } else {
                match self.turns.delivered(pane, turn.received_before).await {
                    Ok(delivered) => delivered,
                    Err(error) => return self.turn_failed(error, state, cycle).await,
                }
            };
            if !delivered && let Err(error) = self.turns.send_assignment(&request).await {
                // A definite failure means nothing was delivered.
                if !matches!(&error, TurnError::Port(port) if port.is_uncertain()) {
                    self.save_pending(None).await?;
                }
                return self.turn_failed(error, state, cycle).await;
            }
            turn.phase = TurnPhase::Awaiting;
            self.save_pending(Some(&turn)).await?;
            if !delivered {
                // The prompt was this step's effect; the next step collects the result.
                return Ok(Err(state.clone()));
            }
        }
        if turn.phase == TurnPhase::Correcting {
            match self.turns.delivered(pane, turn.received_before).await {
                Ok(true) => turn.corrections += 1,
                // Never arrived: the result it corrects is collected again and corrected below.
                Ok(false) => {}
                Err(error) => return self.turn_failed(error, state, cycle).await,
            }
            turn.phase = TurnPhase::Awaiting;
            self.save_pending(Some(&turn)).await?;
        }
        let collected = match self.turns.collect(&request).await {
            Ok(collected) => collected,
            Err(error) => return self.turn_failed(error, state, cycle).await,
        };
        // A crash after the result was saved but before the phase moved on leaves the result in
        // the history: saving it again would count it twice.
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
        turn.received_before = self.turns.prompts_received(pane).await?;
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
        Ok(Err(state.clone()))
    }

    /// Launches the gone review agent of `cycle` again, unless the agent shows that an earlier
    /// launch happened, then drops the turn it lost: the new agent gets a fresh assignment.
    async fn relaunch(&self, cycle: u32) -> Result<(), PipelineError> {
        let environment = self.load_environment(cycle).await?;
        let command_line = &self
            .spec
            .agents
            .iter()
            .find(|launch| launch.role == Role::Review)
            .ok_or_else(|| PipelineError::Environment("no review launch in spec".into()))?
            .command_line;
        self.environment
            .relaunch(
                self.store.as_ref(),
                &self.run,
                &environment,
                Role::Review,
                command_line,
            )
            .await?;
        self.save_pending(None).await
    }

    /// Recovers a lost agent, or reports why the turn cannot go on.
    async fn turn_failed(
        &self,
        error: TurnError,
        state: &PrReviewState,
        cycle: u32,
    ) -> Result<Result<Outcome, PrReviewState>, PipelineError> {
        match error {
            TurnError::AgentLost => {
                // Saved before the launch, so that a restart reconciles the launch instead of
                // collecting the lost turn from whatever agent the pane holds.
                if let Some(mut turn) = self.load_pending().await? {
                    turn.phase = TurnPhase::Relaunching;
                    self.save_pending(Some(&turn)).await?;
                }
                self.relaunch(cycle).await?;
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

    /// Advances the fix by one step; the review environment stays until the fix is merged.
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
                _ => state.clone(),
            },
        )
    }

    /// Performs the next cleanup effect of the environment of `cycle`; `true` once it is all
    /// removed. What was removed is saved, so a retry continues where the last step stopped.
    async fn clean_up(&self, cycle: u32) -> Result<bool, PipelineError> {
        self.environment
            .cleanup_step(self.store.as_ref(), &self.run, &self.environment_key(cycle))
            .await
    }

    /// Performs a forge effect once per run: an effect that is recorded as done is skipped. One
    /// whose outcome was not recorded may have been performed, so `reconcile` reads whether it
    /// was before the effect is repeated.
    async fn once(
        &self,
        name: &str,
        reconcile: impl Future<Output = Result<bool, PortError>>,
        effect: impl Future<Output = Result<(), PortError>>,
    ) -> Result<(), PortError> {
        let key = format!("{}/{name}", self.instance);
        let effects = self.store.load_effects(&self.run).await?;
        match effects.iter().find(|record| record.key == key) {
            Some(record) if record.outcome.is_some() => return Ok(()),
            Some(_) => {
                if reconcile.await? {
                    return self
                        .store
                        .record_effect_outcome(&self.run, &key, "done")
                        .await;
                }
            }
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
            PrReviewState::Refreshing { cycle } => Ok(if self.clean_up(*cycle).await? {
                PrReviewState::Provisioning { cycle: cycle + 1 }
            } else {
                state.clone()
            }),
            PrReviewState::ClosingSpecification { cycle } => {
                self.once(
                    "close-specification",
                    async {
                        let status = self.forge.issue_status(&self.feature.specification).await?;
                        Ok(status == IssueStatus::Closed)
                    },
                    self.forge.close_issue(&self.feature.specification),
                )
                .await?;
                Ok(PrReviewState::MarkingReady { cycle: *cycle })
            }
            PrReviewState::MarkingReady { cycle } => {
                self.once(
                    "mark-ready",
                    async {
                        let draft = self
                            .forge
                            .pull_request_is_draft(&self.feature.draft_pull_request)
                            .await?;
                        Ok(!draft)
                    },
                    self.forge
                        .mark_pull_request_ready(&self.feature.draft_pull_request),
                )
                .await?;
                Ok(PrReviewState::CleaningUp { cycle: *cycle })
            }
            PrReviewState::CleaningUp { cycle } => Ok(if self.clean_up(*cycle).await? {
                PrReviewState::Done(PrReady {
                    pull_request: self.feature.draft_pull_request.clone(),
                })
            } else {
                state.clone()
            }),
            PrReviewState::Done(_) | PrReviewState::Paused { .. } => Ok(state.clone()),
        }
    }
}

impl<I: Implement> Pipeline for PrReviewPipeline<I> {
    type State = PrReviewState;

    fn initial_state(&self) -> PrReviewState {
        PrReviewState::Provisioning { cycle: 1 }
    }

    /// Runs the step and saves the policy if it changed: the driver saves no policy, and a
    /// restart must not reset the run-wide budgets or lose a pause.
    async fn step(&self, state: PrReviewState) -> Result<PrReviewState, PipelineError> {
        let before = self.policy.snapshot();
        let result = self.step_effect(state).await;
        if self.policy.snapshot() != before {
            self.policy.save(self.store.as_ref(), &self.run).await?;
        }
        result
    }
}

impl<I: Implement> PrReviewPipeline<I> {
    /// While the run is paused, a review turn that already started still finishes and its result
    /// is saved; the pipeline then pauses at the state the result leads to, so no fix or new
    /// review starts. Any other state pauses as it is.
    async fn finish_started_review(
        &self,
        reason: PauseReason,
        state: PrReviewState,
    ) -> Result<PrReviewState, PipelineError> {
        let PrReviewState::Reviewing { cycle } = state else {
            return Ok(paused(reason, state));
        };
        if self
            .load_pending()
            .await?
            .is_none_or(|pending| pending.cycle != cycle)
        {
            return Ok(paused(reason, state));
        }
        Ok(match self.review(state.clone(), cycle).await {
            // Still in flight: the next step goes on collecting it.
            Ok(next) if next == state => next,
            Ok(next) if next.is_paused() => next,
            Ok(next) => paused(reason, next),
            Err(PipelineError::Paused(_)) => paused(reason, state),
            Err(error) => return Err(error),
        })
    }

    /// Pauses instead of acting while the run is paused. A failed forge effect is retried only
    /// while the policy permits, otherwise the pipeline pauses; the retry reconciles the effect
    /// first, so an uncertain one counts as reconciled.
    async fn step_effect(&self, state: PrReviewState) -> Result<PrReviewState, PipelineError> {
        if let Err(reason) = self.policy.check_start() {
            return self.finish_started_review(reason, state).await;
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
    use std::collections::{HashSet, VecDeque};
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::terminal::{FakeTerminal, Terminal, TurnStatus};
    use chimera_core::{
        AgentProfile, BranchName, CommitId, Limits, MergedOk, PaneId, WorkItem, WorkspaceId,
    };
    use futures_executor::block_on;

    use super::*;
    use crate::driver::drive;
    use crate::environment::AgentLaunch;
    use crate::merge_lock::MergeLock;

    const APPROVED: &str = r#"{"ReviewApproved":"fine"}"#;
    const FINDINGS: &str = r#"{"ChangesRequested":"fix it"}"#;

    /// Answers each prompt to a pane with that pane's next scripted reply and finishes the turn.
    struct ScriptedTerminal {
        inner: FakeTerminal,
        replies: Mutex<VecDeque<String>>,
        launches: Mutex<usize>,
        /// Every prompt in the order it was sent.
        sent: Mutex<Vec<String>>,
        /// Fails the next launch, as a lost connection would.
        fail_launch: Mutex<Option<PortError>>,
        /// Fails the next launch after it started the agent, as a lost response would.
        fail_after_launch: Mutex<Option<PortError>>,
        /// Panes whose agent is gone until it is launched again.
        gone: Mutex<HashSet<PaneId>>,
    }

    #[async_trait]
    impl Terminal for ScriptedTerminal {
        async fn create_workspace(
            &self,
            directory: &Path,
        ) -> Result<(WorkspaceId, PaneId), PortError> {
            self.inner.create_workspace(directory).await
        }
        async fn find_workspace(
            &self,
            directory: &Path,
        ) -> Result<Option<(WorkspaceId, Vec<PaneId>)>, PortError> {
            self.inner.find_workspace(directory).await
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
            if let Some(error) = self.fail_launch.lock().unwrap().take() {
                return Err(error);
            }
            self.gone.lock().unwrap().remove(pane);
            self.inner.launch_agent(pane, command_line).await?;
            match self.fail_after_launch.lock().unwrap().take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            if let Some(reply) = self.replies.lock().unwrap().pop_front() {
                self.inner.script_output(pane, reply);
                self.inner.script_statuses(pane, [TurnStatus::Finished]);
            }
            self.sent.lock().unwrap().push(prompt.to_string());
            self.inner.send_prompt(pane, prompt).await
        }
        async fn prompts_received(&self, pane: &PaneId) -> Result<u64, PortError> {
            self.inner.prompts_received(pane).await
        }
        async fn read_status(&self, pane: &PaneId) -> Result<TurnStatus, PortError> {
            if self.gone.lock().unwrap().contains(pane) {
                return Ok(TurnStatus::Gone);
            }
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
                fail_launch: Mutex::default(),
                fail_after_launch: Mutex::default(),
                gone: Mutex::default(),
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
            self.terminal.sent.lock().unwrap().clone()
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
    fn a_consumed_recovery_is_saved_before_the_relaunch() {
        let f = fixture(5, &[]);
        let pipeline = f.pipeline();
        let reviewing = block_on(next_state(&pipeline, pipeline.initial_state()));
        assert_eq!(reviewing, PrReviewState::Reviewing { cycle: 1 });
        let environment = block_on(pipeline.load_environment(1)).unwrap();
        let pane = environment.pane(Role::Review).unwrap();
        f.terminal.inner.script_statuses(pane, [TurnStatus::Gone]);
        *f.terminal.fail_launch.lock().unwrap() = Some(PortError::uncertain("lost"));

        // The assignment, then the relaunch of the agent found gone.
        assert_eq!(
            block_on(pipeline.step(reviewing.clone())).unwrap(),
            reviewing
        );
        let error = block_on(pipeline.step(reviewing)).unwrap_err();

        assert!(error.is_uncertain());
        let restored = block_on(Policy::load_or_new(
            f.store.as_ref(),
            &run(),
            &Limits::default(),
        ))
        .unwrap();
        assert_eq!(
            restored.snapshot().agent_recovery_remaining,
            Limits::default().agent_recovery - 1
        );
    }

    /// Starts the first review, then finds its agent gone; the step that relaunches it is
    /// interrupted after the launch, its response lost. The idle new agent's pane still shows
    /// the old result.
    fn interrupted_relaunch(f: &Fixture) {
        let pipeline = f.pipeline();
        let reviewing = block_on(next_state(&pipeline, pipeline.initial_state()));
        assert_eq!(
            block_on(pipeline.step(reviewing.clone())).unwrap(),
            reviewing
        );
        let environment = block_on(pipeline.load_environment(1)).unwrap();
        let pane = environment.pane(Role::Review).unwrap().clone();
        f.terminal.gone.lock().unwrap().insert(pane);
        *f.terminal.fail_after_launch.lock().unwrap() = Some(PortError::uncertain("lost"));

        assert!(
            block_on(pipeline.step(reviewing))
                .unwrap_err()
                .is_uncertain()
        );
        let turn = block_on(pipeline.load_pending()).unwrap().unwrap();
        assert_eq!(turn.phase, TurnPhase::Relaunching);
    }

    #[test]
    fn a_relaunch_whose_response_was_lost_is_reconciled_and_assigned_afresh() {
        let f = fixture(5, &[FINDINGS, APPROVED]);
        interrupted_relaunch(&f);

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );

        // Launched once to provision and once to recover; the stale findings were not taken
        // for the new agent's review.
        assert_eq!(*f.terminal.launches.lock().unwrap(), 2);
        assert_eq!(f.review_prompts().len(), 2);
        assert!(f.implement.calls.lock().unwrap().is_empty());
        assert_eq!(
            block_on(saved_policy(&f))
                .snapshot()
                .agent_recovery_remaining,
            Limits::default().agent_recovery - 1
        );
    }

    #[test]
    fn a_crash_after_the_relaunch_before_the_turn_is_dropped_assigns_afresh() {
        let f = fixture(5, &[APPROVED]);
        let pipeline = f.pipeline();
        block_on(async {
            next_state(&pipeline, PrReviewState::Provisioning { cycle: 1 }).await;
            let environment = pipeline.load_environment(1).await.unwrap();
            let pane = environment.pane(Role::Review).unwrap().clone();
            // The relaunched agent is idle; the pane shows the lost turn's findings.
            f.terminal.inner.script_output(&pane, FINDINGS);
            f.terminal
                .inner
                .script_statuses(&pane, [TurnStatus::Finished]);
            pipeline
                .save_pending(Some(&PendingTurn {
                    cycle: 1,
                    received_before: 0,
                    invalid_before: 0,
                    corrections: 0,
                    phase: TurnPhase::Relaunching,
                }))
                .await
                .unwrap();
        });

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );

        assert_eq!(*f.terminal.launches.lock().unwrap(), 1);
        assert_eq!(f.review_prompts().len(), 1);
        assert!(f.implement.calls.lock().unwrap().is_empty());
        assert_eq!(
            block_on(saved_policy(&f))
                .snapshot()
                .agent_recovery_remaining,
            Limits::default().agent_recovery
        );
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
        let drive_to = |state: PrReviewState| block_on(next_state(&pipeline, state));
        let state = drive_to(drive_to(PrReviewState::Provisioning { cycle: 1 }));
        assert_eq!(state, PrReviewState::ClosingSpecification { cycle: 1 });
        let state = drive_to(state);
        assert_eq!(state, PrReviewState::MarkingReady { cycle: 1 });
        f.forge.fail_next(PortError::failed("rate limited"));
        assert_eq!(block_on(pipeline.step(state.clone())).unwrap(), state);

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
            provision_first_review(&f).await;
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
            let state = next_state(&pipeline, PrReviewState::Provisioning { cycle: 1 }).await;
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
                    received_before: 0,
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

    /// Provisions the first review and saves its turn as being sent; the idle pane already shows
    /// the approval the turn will end with. Returns the pane.
    async fn review_saved_as_sending(f: &Fixture) -> PaneId {
        let pipeline = f.pipeline();
        next_state(&pipeline, PrReviewState::Provisioning { cycle: 1 }).await;
        let environment = pipeline.load_environment(1).await.unwrap();
        let pane = environment.pane(Role::Review).unwrap().clone();
        f.terminal.inner.script_output(&pane, APPROVED);
        f.terminal
            .inner
            .script_statuses(&pane, [TurnStatus::Finished]);
        pipeline
            .save_pending(Some(&PendingTurn {
                cycle: 1,
                received_before: 0,
                invalid_before: 0,
                corrections: 0,
                phase: TurnPhase::Sending,
            }))
            .await
            .unwrap();
        pane
    }

    #[test]
    fn a_review_saved_as_sending_but_never_sent_is_sent_once_after_a_restart() {
        let f = fixture(5, &[APPROVED]);
        block_on(review_saved_as_sending(&f));

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );
        assert_eq!(f.review_prompts().len(), 1);
    }

    #[test]
    fn a_delivered_review_with_identical_output_is_not_resent_after_a_restart() {
        let f = fixture(5, &[]);
        let pane = block_on(review_saved_as_sending(&f));
        // The assignment arrived and the agent answered with what the pane showed before.
        block_on(f.terminal.inner.send_prompt(&pane, "You are reviewer.")).unwrap();

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );
        assert!(f.review_prompts().is_empty());
        assert_eq!(f.store.history(&run()).len(), 1);
    }

    #[test]
    fn a_correction_that_never_arrived_is_sent_once_after_a_restart() {
        let f = fixture(5, &[APPROVED]);
        let pane = block_on(review_saved_as_sending(&f));
        block_on(async {
            // The invalid result was saved; its correction was being sent when the process died.
            f.terminal.inner.script_output(&pane, "no outcome");
            f.store
                .append_turn(
                    &run(),
                    TurnResult {
                        agent: f.pipeline().agent(),
                        role: Role::Review,
                        outcome: TurnOutcome::Invalid {
                            output: "no outcome".into(),
                            problem: "no outcome found".into(),
                        },
                    },
                )
                .await
                .unwrap();
            f.pipeline()
                .save_pending(Some(&PendingTurn {
                    cycle: 1,
                    received_before: 0,
                    invalid_before: 0,
                    corrections: 0,
                    phase: TurnPhase::Correcting,
                }))
                .await
                .unwrap();
        });

        assert_eq!(
            block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 })),
            ready()
        );
        let prompts = f.review_prompts();
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("was rejected"));
        assert_eq!(f.store.history(&run()).len(), 2);
    }

    #[test]
    fn a_started_review_is_collected_while_paused_and_nothing_new_starts() {
        for (reply, after) in [
            (APPROVED, PrReviewState::ClosingSpecification { cycle: 1 }),
            (
                FINDINGS,
                PrReviewState::Fixing {
                    cycle: 1,
                    findings: "fix it".into(),
                },
            ),
        ] {
            let f = fixture(5, &[]);
            let pipeline = f.pipeline();
            block_on(async {
                next_state(&pipeline, PrReviewState::Provisioning { cycle: 1 }).await;
                let environment = pipeline.load_environment(1).await.unwrap();
                let pane = environment.pane(Role::Review).unwrap().clone();
                // The review was assigned before the restart; the agent answered meanwhile.
                f.terminal.inner.script_output(&pane, reply);
                f.terminal
                    .inner
                    .script_statuses(&pane, [TurnStatus::Finished]);
                pipeline
                    .save_pending(Some(&PendingTurn {
                        cycle: 1,
                        received_before: 0,
                        invalid_before: 0,
                        corrections: 0,
                        phase: TurnPhase::Awaiting,
                    }))
                    .await
                    .unwrap();
            });
            f.policy.pause(PauseReason::GlobalPause);

            let state = block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 }));

            assert_eq!(state, paused(PauseReason::GlobalPause, after), "{reply}");
            let history = f.store.history(&run());
            assert_eq!(history.len(), 1, "{reply}");
            assert!(f.review_prompts().is_empty(), "{reply}");
            assert!(f.implement.calls.lock().unwrap().is_empty(), "{reply}");
            assert!(f.forge_effects().is_empty(), "{reply}");
        }
    }

    #[test]
    fn a_review_that_has_not_started_stays_unsent_while_paused() {
        let f = fixture(5, &[APPROVED]);
        let pipeline = f.pipeline();
        block_on(next_state(
            &pipeline,
            PrReviewState::Provisioning { cycle: 1 },
        ));
        f.policy.pause(PauseReason::GlobalPause);

        let state = block_on(f.drive_resumed(PrReviewState::Reviewing { cycle: 1 }));

        assert_eq!(
            state,
            paused(
                PauseReason::GlobalPause,
                PrReviewState::Reviewing { cycle: 1 }
            )
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

    /// Provisions the review environment of cycle 1, as an earlier process did.
    async fn provision_first_review(f: &Fixture) {
        let pipeline = f.pipeline();
        pipeline
            .environment
            .provision(
                f.store.as_ref(),
                &run(),
                "review/environment/1",
                &pipeline.spec,
                &CommitId::new("c0").unwrap(),
            )
            .await
            .unwrap();
    }

    /// Steps until the state changes: provisioning and cleanup take one step per effect.
    async fn next_state<I: Implement>(
        pipeline: &PrReviewPipeline<I>,
        state: PrReviewState,
    ) -> PrReviewState {
        loop {
            let next = pipeline.step(state.clone()).await.unwrap();
            if next != state {
                return next;
            }
        }
    }

    #[test]
    fn the_worktree_is_on_the_verified_remote_head() {
        let f = fixture(5, &[APPROVED]);
        // The fix moved the remote and the verified head; the local feature branch is behind.
        f.repository
            .set_remote_head(branch("feat"), CommitId::new("c1").unwrap());
        block_on(f.store.save_pipeline_state(
            &run(),
            &expected_head_key(&branch("feat")),
            serde_json::to_value(CommitId::new("c1").unwrap()).unwrap(),
        ))
        .unwrap();
        let worktree = Path::new("/wt/review");
        let c1 = Some(CommitId::new("c1").unwrap());

        // A crash after the worktree was created and before it was moved.
        let created = block_on(f.pipeline().step(PrReviewState::Provisioning { cycle: 1 }));
        assert_eq!(created.unwrap(), PrReviewState::Provisioning { cycle: 1 });
        assert_eq!(
            f.repository.worktree_head(worktree),
            Some(CommitId::new("c0").unwrap())
        );
        let pipeline = f.pipeline();
        let state = block_on(next_state(
            &pipeline,
            PrReviewState::Provisioning { cycle: 1 },
        ));
        assert_eq!(state, PrReviewState::Reviewing { cycle: 1 });
        assert_eq!(f.repository.worktree_head(worktree), c1);

        // Provisioning again after a restart leaves it there.
        block_on(f.pipeline().step(PrReviewState::Provisioning { cycle: 1 })).unwrap();
        assert_eq!(f.repository.worktree_head(worktree), c1);
    }

    #[test]
    fn a_fix_moves_the_next_review_worktree_to_the_new_head() {
        let f = fixture(5, &[FINDINGS, APPROVED]);
        let pipeline = f.pipeline();
        let step = |state: PrReviewState| block_on(next_state(&pipeline, state));
        let state = step(step(step(PrReviewState::Provisioning { cycle: 1 })));
        let state = step(step(state));
        assert_eq!(state, PrReviewState::Reviewing { cycle: 2 });

        assert_eq!(
            f.repository.worktree_head(Path::new("/wt/review")),
            Some(CommitId::new("c1").unwrap())
        );
    }

    #[test]
    fn a_crash_after_closing_before_the_outcome_does_not_close_again() {
        let f = fixture(5, &[APPROVED]);
        block_on(async {
            provision_first_review(&f).await;
            f.forge.close_issue(&specification()).await.unwrap();
            f.store
                .record_effect_intent(&run(), "review/close-specification", "close-specification")
                .await
                .unwrap();
        });

        let state = PrReviewState::ClosingSpecification { cycle: 1 };
        assert_eq!(block_on(f.drive_resumed(state)), ready());

        assert_eq!(
            f.forge_effects(),
            vec![
                ForgeCall::CloseIssue(specification()),
                ForgeCall::MarkPullRequestReady(pull_request())
            ]
        );
    }

    #[test]
    fn a_crash_after_marking_ready_before_the_outcome_does_not_mark_again() {
        let f = fixture(5, &[APPROVED]);
        block_on(async {
            provision_first_review(&f).await;
            f.forge
                .mark_pull_request_ready(&pull_request())
                .await
                .unwrap();
            f.store
                .record_effect_intent(&run(), "review/mark-ready", "mark-ready")
                .await
                .unwrap();
        });

        let state = PrReviewState::MarkingReady { cycle: 1 };
        assert_eq!(block_on(f.drive_resumed(state)), ready());

        assert_eq!(
            f.forge_effects(),
            vec![ForgeCall::MarkPullRequestReady(pull_request())]
        );
    }

    #[test]
    fn an_effect_with_an_intent_that_never_ran_is_performed() {
        let f = fixture(5, &[APPROVED]);
        block_on(async {
            provision_first_review(&f).await;
            f.store
                .record_effect_intent(&run(), "review/close-specification", "close-specification")
                .await
                .unwrap();
        });

        let state = PrReviewState::ClosingSpecification { cycle: 1 };
        assert_eq!(block_on(f.drive_resumed(state)), ready());

        assert_eq!(f.forge_effects().len(), 2);
        assert!(f.forge.is_closed(&specification()));
    }

    #[test]
    fn corrections_count_before_a_fix_is_started() {
        let f = fixture(2, &["no outcome", FINDINGS, APPROVED]);

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
        assert!(f.implement.calls.lock().unwrap().is_empty());
        assert_eq!(f.forge.is_draft(&pull_request()), Some(true));
    }

    async fn saved_policy(f: &Fixture) -> Policy {
        Policy::load_or_new(f.store.as_ref(), &run(), &Limits::default())
            .await
            .unwrap()
    }

    #[test]
    fn a_github_retry_is_saved_with_the_policy() {
        let f = fixture(5, &[APPROVED]);
        block_on(provision_first_review(&f));
        f.forge.fail_next(PortError::failed("rate limited"));
        let state = PrReviewState::ClosingSpecification { cycle: 1 };

        assert_eq!(block_on(f.pipeline().step(state.clone())).unwrap(), state);

        // A restart restores the budget that the retry consumed.
        assert_eq!(
            block_on(saved_policy(&f))
                .snapshot()
                .github_retries_remaining,
            Limits::default().github_retries - 1
        );
    }

    #[test]
    fn an_exhausted_github_budget_pauses_across_a_restart() {
        let mut f = fixture(5, &[APPROVED]);
        f.policy = Arc::new(Policy::new(&Limits {
            github_retries: 0,
            ..Limits::default()
        }));
        block_on(provision_first_review(&f));
        f.forge.fail_next(PortError::failed("rate limited"));
        let state = PrReviewState::ClosingSpecification { cycle: 1 };

        assert_eq!(
            block_on(f.pipeline().step(state.clone())).unwrap(),
            paused(PauseReason::GithubRetriesExhausted, state)
        );

        assert_eq!(
            block_on(saved_policy(&f)).check_start(),
            Err(PauseReason::GithubRetriesExhausted)
        );
    }

    /// An outer pipeline whose fix is the real Implementation pipeline, saved as paused in the
    /// step that merges the fix: only the cleanup is left.
    fn drive_implementation_fixture(
        f: &Fixture,
    ) -> PrReviewPipeline<DriveImplementation<impl Fn(u32, &str) -> ImplementationPipeline + Sync>>
    {
        let pipeline = f.pipeline();
        let (store, repository) = (f.store.clone(), f.repository.clone());
        let (policy, environment, turns) = (
            f.policy.clone(),
            pipeline.environment.clone(),
            pipeline.turns.clone(),
        );
        let configuration = pipeline.configuration.clone();
        let lock = block_on(MergeLock::open(f.store.clone(), run(), &branch("feat"))).unwrap();
        let build = move |_cycle: u32, findings: &str| ImplementationPipeline {
            run: run(),
            instance: "fix".into(),
            work_item: WorkItem::Findings(findings.to_string()),
            issue: specification(),
            configuration: configuration.clone(),
            cycle_limit: 5,
            merge_limit: 5,
            spec: ProvisionSpec {
                worktree: "/wt/fix".into(),
                task_branch: branch("fix-task"),
                feature: branch("feat"),
                agents: vec![AgentLaunch {
                    role: Role::Implementation,
                    command_line: "agent Implementation".into(),
                }],
            },
            initial_remote_head: CommitId::new("c0").unwrap(),
            store: store.clone(),
            repository: repository.clone(),
            policy: policy.clone(),
            lock: lock.clone(),
            environment: environment.clone(),
            turns: turns.clone(),
        };
        PrReviewPipeline {
            run: pipeline.run,
            instance: pipeline.instance,
            feature: pipeline.feature,
            configuration: pipeline.configuration,
            review_limit: pipeline.review_limit,
            spec: pipeline.spec,
            store: pipeline.store.clone(),
            repository: pipeline.repository,
            forge: pipeline.forge,
            policy: pipeline.policy,
            environment: pipeline.environment,
            turns: pipeline.turns,
            implementation: DriveImplementation {
                store: pipeline.store,
                build,
            },
        }
    }

    #[test]
    fn each_fixing_step_advances_the_fix_by_one_effect() {
        let f = fixture(5, &[]);
        let outer = drive_implementation_fixture(&f);
        let fix = (outer.implementation.build)(1, "fix it");
        let fixing = PrReviewState::Fixing {
            cycle: 1,
            findings: "fix it".into(),
        };
        let cleaning = ImplementationState::CleaningUp {
            merged: CommitId::new("c0").unwrap(),
        };
        let fix_environment = |f: &Fixture| -> Environment {
            let saved = block_on(f.store.load_pipeline_state(&run(), "fix/environment"));
            serde_json::from_value(saved.unwrap().unwrap()).unwrap()
        };
        block_on(async {
            fix.environment
                .provision(
                    f.store.as_ref(),
                    &run(),
                    "fix/environment",
                    &fix.spec,
                    &CommitId::new("c0").unwrap(),
                )
                .await
                .unwrap();
            f.store
                .save_pipeline_state(&run(), "fix", serde_json::to_value(&cleaning).unwrap())
                .await
                .unwrap();
        });
        let load_fix = || block_on(f.store.load_pipeline_state(&run(), "fix")).unwrap();

        // Close the workspace.
        assert_eq!(block_on(outer.step(fixing.clone())).unwrap(), fixing);
        assert_eq!(load_fix(), Some(serde_json::to_value(&cleaning).unwrap()));
        let environment = fix_environment(&f);
        assert!(environment.workspace.is_none() && environment.worktree_created);

        // Remove the worktree.
        assert_eq!(block_on(outer.step(fixing.clone())).unwrap(), fixing);
        assert!(!fix_environment(&f).worktree_created);

        // Prune; the fix is done.
        assert_eq!(
            block_on(outer.step(fixing)).unwrap(),
            PrReviewState::Refreshing { cycle: 1 }
        );
    }

    #[test]
    fn a_resumed_review_resumes_its_paused_fix_and_a_restart_does_not() {
        let f = fixture(5, &[APPROVED]);
        let outer = drive_implementation_fixture(&f);
        let fix = (outer.implementation.build)(1, "fix it");
        let merged = CommitId::new("c0").unwrap();
        let fix_paused = ImplementationState::Paused {
            reason: PauseReason::LimitExhausted,
            resume_at: Box::new(ImplementationState::CleaningUp {
                merged: merged.clone(),
            }),
        };
        let fixing = PrReviewState::Fixing {
            cycle: 1,
            findings: "fix it".into(),
        };
        let outer_paused = paused(PauseReason::LimitExhausted, fixing);
        block_on(async {
            provision_first_review(&f).await;
            // The review that asked for the fix.
            f.store
                .append_turn(
                    &run(),
                    TurnResult {
                        agent: outer.agent(),
                        role: Role::Review,
                        outcome: TurnOutcome::Valid(Outcome::ChangesRequested("fix it".into())),
                    },
                )
                .await
                .unwrap();
            fix.environment
                .provision(
                    f.store.as_ref(),
                    &run(),
                    "fix/environment",
                    &fix.spec,
                    &CommitId::new("c0").unwrap(),
                )
                .await
                .unwrap();
            f.store
                .save_pipeline_state(&run(), "fix", serde_json::to_value(&fix_paused).unwrap())
                .await
                .unwrap();
            f.store
                .save_pipeline_state(
                    &run(),
                    "review",
                    serde_json::to_value(&outer_paused).unwrap(),
                )
                .await
                .unwrap();
        });
        let load = |instance: &str| {
            block_on(f.store.load_pipeline_state(&run(), instance))
                .unwrap()
                .unwrap()
        };

        // A plain restart of the paused run stays paused, child included.
        let restarted = drive_implementation_fixture(&f);
        let state = block_on(drive(f.store.as_ref(), &run(), "review", &restarted)).unwrap();
        assert_eq!(state, outer_paused);
        assert_eq!(load("fix"), serde_json::to_value(&fix_paused).unwrap());

        // Resuming the outer pipeline resumes the child from where it paused.
        block_on(f.store.save_pipeline_state(
            &run(),
            "review",
            serde_json::to_value(outer_paused.resume()).unwrap(),
        ))
        .unwrap();
        let resumed = drive_implementation_fixture(&f);
        let state = block_on(drive(f.store.as_ref(), &run(), "review", &resumed)).unwrap();

        assert_eq!(state, ready());
        assert_eq!(
            load("fix"),
            serde_json::to_value(ImplementationState::Done(MergedOk { commit: merged })).unwrap()
        );
        // The fix repeated no work: nothing but the review after it was asked of an agent.
        assert_eq!(f.review_prompts().len(), 1);
        assert_eq!(f.repository.worktree_branch(Path::new("/wt/fix")), None);
    }
}
