use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::repository::Repository;
use chimera_core::run_store::RunStore;
use chimera_core::{
    AgentConfiguration, AgentId, BranchName, CommitId, IssueRef, MergedOk, Outcome, Role, RunId,
    TurnOutcome, TurnResult, WorkItem,
};
use serde::{Deserialize, Serialize};

use crate::agent_turn::{AgentTurns, TurnError, TurnRequest, pushed_commit};
use crate::driver::{Pipeline, PipelineState};
use crate::environment::{Environment, EnvironmentService, ProvisionSpec};
use crate::error::{PauseReason, PipelineError};
use crate::merge_lock::MergeLock;
use crate::policy::Policy;

/// State of one implementation pipeline instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImplementationState {
    Provisioning,
    Implementing {
        cycle: u32,
    },
    Reviewing {
        cycle: u32,
    },
    WaitingForMerge,
    Merging {
        attempt: u32,
    },
    ConflictReview {
        attempt: u32,
    },
    Verifying {
        attempt: u32,
    },
    CleaningUp {
        merged: CommitId,
    },
    Done(MergedOk),
    Paused {
        reason: PauseReason,
        resume_at: Box<ImplementationState>,
    },
}

impl PipelineState for ImplementationState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }
}

/// `(WorkItem, AgentConfiguration)` to `MergedOk`: provisioning, the implementation/review loop,
/// the merge under the feature branch's merge lock, its verification and the cleanup.
///
/// The merge agent starts the explanation of `MergeSuccessful` with the commit it pushed; the
/// merge prompt and the outcome validation enforce it, and verification accepts only a remote
/// head equal to that commit.
pub struct ImplementationPipeline {
    pub run: RunId,
    /// Names this task in the store and its agents in the history.
    pub instance: String,
    pub work_item: WorkItem,
    /// The issue the agents are pointed at: the ticket, or the specification for findings.
    pub issue: IssueRef,
    pub configuration: AgentConfiguration,
    /// Implementation/review cycles allowed, corrections included.
    pub cycle_limit: u32,
    /// Merge attempts allowed, corrections included.
    pub merge_limit: u32,
    pub spec: ProvisionSpec,
    /// The remote head of the feature branch when no merge has updated the expected one yet.
    pub initial_remote_head: CommitId,
    pub store: Arc<dyn RunStore>,
    pub repository: Arc<dyn Repository>,
    pub policy: Arc<Policy>,
    pub lock: Arc<MergeLock>,
    pub environment: Arc<EnvironmentService>,
    pub turns: Arc<AgentTurns>,
}

impl ImplementationPipeline {
    fn environment_key(&self) -> String {
        format!("{}/environment", self.instance)
    }

    fn agent(&self, role: Role) -> AgentId {
        AgentId::new(format!("{}-{role:?}", self.instance)).expect("instance is not blank")
    }

    async fn load_environment(&self) -> Result<Environment, PipelineError> {
        let saved = self
            .store
            .load_pipeline_state(&self.run, &self.environment_key())
            .await?
            .ok_or_else(|| PipelineError::Environment("task is not provisioned".into()))?;
        Ok(serde_json::from_value(saved)?)
    }

    /// The turns of this task's agents, oldest first.
    async fn history(&self) -> Result<Vec<TurnResult>, PipelineError> {
        let agents = [Role::Implementation, Role::Review, Role::Merge].map(|role| self.agent(role));
        Ok(self
            .store
            .load_history(&self.run)
            .await?
            .into_iter()
            .filter(|turn| agents.contains(&turn.agent))
            .collect())
    }

    fn pending_key(&self) -> String {
        format!("{}/pending", self.instance)
    }

    async fn load_pending(&self) -> Result<Pending, PipelineError> {
        Ok(
            match self
                .store
                .load_pipeline_state(&self.run, &self.pending_key())
                .await?
            {
                Some(saved) => serde_json::from_value(saved)?,
                None => Pending::default(),
            },
        )
    }

    async fn save_pending(&self, pending: &Pending) -> Result<(), PipelineError> {
        self.store
            .save_pipeline_state(
                &self.run,
                &self.pending_key(),
                serde_json::to_value(pending)?,
            )
            .await?;
        Ok(())
    }

    /// Clears the sender whose reset is owed. Resetting twice is harmless, so an uncertain
    /// earlier reset is simply repeated.
    async fn reset_owed(&self, pending: &mut Pending) -> Result<(), PipelineError> {
        let Some(sender) = pending.reset else {
            return Ok(());
        };
        let environment = self.load_environment().await?;
        let pane = environment.pane(sender).ok_or_else(|| {
            PipelineError::Environment(format!("no {sender:?} agent in the environment"))
        })?;
        self.turns
            .reset(pane, self.configuration.profile(sender))
            .await?;
        pending.reset = None;
        self.save_pending(pending).await
    }

    /// Runs the turn described by `fresh` unless `pending` shows it already started, and
    /// continues it from the phase `pending` records. What a restart cannot tell is never
    /// repeated: a prompt that may have been delivered is reconciled instead of sent again, and
    /// the sender is reset only once the receiver is known to have its prompt.
    async fn execute_turn(
        &self,
        pending: &mut Pending,
        request: &TurnRequest<'_>,
        fresh: PendingTurn,
        sender: Option<Role>,
        max_corrections: u32,
        reset_failed: &mut Option<PipelineError>,
    ) -> Result<Result<Outcome, TurnError>, PipelineError> {
        let existing = pending
            .turn
            .clone()
            .filter(|turn| turn.role == fresh.role && turn.cycle == fresh.cycle);
        let created = existing.is_none();
        let mut turn = existing.unwrap_or(fresh);
        if created {
            pending.turn = Some(turn.clone());
            self.save_pending(pending).await?;
        }
        if turn.phase == TurnPhase::Sending {
            if created {
                if let Err(error) = self.turns.send_assignment(request).await {
                    // A definite failure means nothing was delivered.
                    if !matches!(&error, TurnError::Port(port) if port.is_uncertain()) {
                        pending.turn = None;
                        self.save_pending(pending).await?;
                    }
                    return Ok(Err(error));
                }
            } else {
                match self
                    .turns
                    .delivered(request.pane, &turn.output_before)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        let unknown = "the assignment may or may not have been delivered";
                        return Ok(Err(PortError::uncertain(unknown).into()));
                    }
                    Err(error) => return Ok(Err(error)),
                }
            }
            turn.phase = TurnPhase::Awaiting;
            pending.turn = Some(turn.clone());
            pending.reset = sender;
            self.save_pending(pending).await?;
            if let Err(error) = self.reset_owed(pending).await {
                *reset_failed = Some(error);
            }
        }
        loop {
            if turn.phase == TurnPhase::Correcting {
                // The correction was being sent: the pane shows what it showed before it.
                match self
                    .turns
                    .delivered(request.pane, &turn.output_before)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        let unknown = "the correction may or may not have been delivered";
                        return Ok(Err(PortError::uncertain(unknown).into()));
                    }
                    Err(error) => return Ok(Err(error)),
                }
                turn.corrections += 1;
                turn.phase = TurnPhase::Awaiting;
                pending.turn = Some(turn.clone());
                self.save_pending(pending).await?;
            }
            let collected = match self.turns.collect(request).await {
                Ok(collected) => collected,
                Err(error) => return Ok(Err(error)),
            };
            // A crash after the result was saved but before the phase moved on leaves the
            // result in the history: saving it again would count it twice.
            let saved = self
                .history()
                .await?
                .iter()
                .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                .count()
                - turn.invalid_before;
            if collected.parsed.is_ok() || saved <= turn.corrections as usize {
                self.turns.record(request, &collected).await?;
            }
            let problem = match collected.parsed {
                Ok(outcome) => return Ok(Ok(outcome)),
                Err(problem) => problem,
            };
            if turn.corrections >= max_corrections {
                return Ok(Err(TurnError::CorrectionsExhausted {
                    corrections: turn.corrections,
                    problem,
                }));
            }
            turn.phase = TurnPhase::Correcting;
            turn.output_before = collected.output;
            pending.turn = Some(turn.clone());
            self.save_pending(pending).await?;
            if let Err(error) = self.turns.send_correction(request, &problem).await {
                if !error.is_uncertain() {
                    // Not delivered: the saved result is corrected on the next attempt.
                    turn.phase = TurnPhase::Awaiting;
                    pending.turn = Some(turn.clone());
                    self.save_pending(pending).await?;
                }
                return Ok(Err(error.into()));
            }
            // Confirmed delivery: no reconciliation is needed on a restart.
            turn.corrections += 1;
            turn.phase = TurnPhase::Awaiting;
            pending.turn = Some(turn.clone());
            self.save_pending(pending).await?;
        }
    }

    /// Runs the turn of `role` in `cycle` unless its result was already saved, then moves on.
    /// A turn that was interrupted is reconciled with the agent instead of being sent again,
    /// and a sender reset still owed is done before anything else.
    async fn turn_step(
        &self,
        state: ImplementationState,
        role: Role,
        cycle: u32,
    ) -> Result<ImplementationState, PipelineError> {
        let mut pending = self.load_pending().await?;
        self.reset_owed(&mut pending).await?;
        let history = self.history().await?;
        let valid = |role: Role| {
            history.iter().filter_map(move |turn| match &turn.outcome {
                TurnOutcome::Valid(outcome) if turn.role == role => Some(outcome),
                _ => None,
            })
        };
        let saved = (valid(role).count() >= cycle as usize)
            .then(|| valid(role).next_back().cloned())
            .flatten();
        let outcome = match saved {
            Some(outcome) => outcome,
            None => {
                let invalid = history
                    .iter()
                    .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                    .count();
                // Corrections of a turn that already started stay counted by that turn.
                let invalid_before = pending
                    .turn
                    .as_ref()
                    .filter(|turn| turn.role == role && turn.cycle == cycle)
                    .map_or(invalid, |turn| turn.invalid_before);
                let other = if role == Role::Review {
                    Role::Implementation
                } else {
                    Role::Review
                };
                let plan = TurnPlan {
                    role,
                    cycle,
                    invalid_before,
                    used: cycle as usize + invalid_before,
                    limit: self.cycle_limit,
                    previous: valid(other)
                        .next_back()
                        .map(|outcome| (other, outcome_text(outcome))),
                    remember: false,
                };
                match self.run_turn(&mut pending, state, plan).await? {
                    Ok(outcome) => outcome,
                    Err(next) => return Ok(next),
                }
            }
        };
        Ok(match outcome {
            Outcome::ImplementationReady(_) => ImplementationState::Reviewing { cycle },
            Outcome::ReviewApproved(_) => ImplementationState::WaitingForMerge,
            Outcome::ChangesRequested(_) => ImplementationState::Implementing { cycle: cycle + 1 },
            other => unreachable!("{other:?} is not valid for the {role:?} role"),
        })
    }

    /// Starts the turn of `plan`, or continues it after a restart, and returns its outcome. When
    /// the turn cannot finish now, returns the state to continue from instead.
    async fn run_turn(
        &self,
        pending: &mut Pending,
        state: ImplementationState,
        plan: TurnPlan,
    ) -> Result<Result<Outcome, ImplementationState>, PipelineError> {
        let TurnPlan {
            role,
            cycle,
            invalid_before,
            used,
            limit,
            previous,
            remember,
        } = plan;
        if used > limit as usize {
            return Ok(Err(paused(PauseReason::LimitExhausted, state)));
        }
        let sender = previous
            .as_ref()
            .map(|(sender, _)| *sender)
            .filter(|sender| *sender != role);
        let previous = previous.map(|(_, text)| text);
        // Every assignment keeps the findings next to the latest description.
        let description = match (&self.work_item, previous) {
            (WorkItem::Findings(findings), Some(previous)) => Some(format!(
                "Findings:\n{findings}\n\nLatest description:\n{previous}"
            )),
            (WorkItem::Findings(findings), None) => Some(format!("Findings:\n{findings}")),
            (_, previous) => previous,
        };
        let agent = self.agent(role);
        let environment = self.load_environment().await?;
        let pane = environment.pane(role).ok_or_else(|| {
            PipelineError::Environment(format!("no {role:?} agent in the environment"))
        })?;
        let request = TurnRequest {
            run: &self.run,
            agent: &agent,
            pane,
            role,
            profile: self.configuration.profile(role),
            issue: &self.issue,
            previous_description: description.as_deref(),
        };
        let max_corrections = (limit as usize - used) as u32;
        let fresh = PendingTurn {
            role,
            cycle,
            output_before: self.turns.read_output(pane).await?,
            invalid_before,
            valid_before: valid_count(&self.history().await?),
            corrections: 0,
            phase: TurnPhase::Sending,
        };
        let mut reset_failed = None;
        let result = self
            .execute_turn(
                pending,
                &request,
                fresh,
                sender,
                max_corrections,
                &mut reset_failed,
            )
            .await?;
        match result {
            Ok(outcome) => {
                // A reset that failed stays owed: it is retried before the state moves on, and
                // the saved result of the turn is kept.
                pending.turn = None;
                if remember {
                    let valid = valid_count(&self.history().await?);
                    pending.finished = Some(Finished { state, valid });
                }
                self.save_pending(pending).await?;
                if let Some(error) = reset_failed {
                    return Err(error);
                }
                Ok(Ok(outcome))
            }
            Err(TurnError::AgentLost) => {
                let command_line = &self
                    .spec
                    .agents
                    .iter()
                    .find(|launch| launch.role == role)
                    .ok_or_else(|| {
                        PipelineError::Environment(format!("no {role:?} launch in spec"))
                    })?
                    .command_line;
                self.environment
                    .relaunch(&environment, role, command_line)
                    .await?;
                pending.turn = None;
                self.save_pending(pending).await?;
                Ok(Err(state))
            }
            Err(TurnError::CorrectionsExhausted { .. }) => {
                pending.turn = None;
                self.save_pending(pending).await?;
                Ok(Err(paused(PauseReason::LimitExhausted, state)))
            }
            Err(TurnError::Paused(reason)) => {
                // Nothing was sent.
                pending.turn = None;
                self.save_pending(pending).await?;
                Err(PipelineError::Paused(reason))
            }
            Err(other) => Err(other.into()),
        }
    }

    /// Runs the turn of `role` in the merge phase unless its result was already saved, then
    /// moves on. A merge attempt can span several turns (the merge agent, the conflict review and
    /// the merge agent again), so a saved result is recognised by the turn it ended, not by a
    /// count.
    async fn merge_turn_step(
        &self,
        state: ImplementationState,
        role: Role,
        attempt: u32,
    ) -> Result<ImplementationState, PipelineError> {
        let mut pending = self.load_pending().await?;
        self.reset_owed(&mut pending).await?;
        let history = self.history().await?;
        let valid = valid_turns(&history);
        let started = pending
            .turn
            .as_ref()
            .filter(|turn| turn.role == role && turn.cycle == attempt);
        let finished = pending
            .finished
            .as_ref()
            .is_some_and(|finished| finished.state == state && finished.valid == valid.len());
        let saved = finished || started.is_some_and(|turn| valid.len() > turn.valid_before);
        let outcome = if saved {
            if !finished {
                // The result was saved but the turn was not closed.
                pending.turn = None;
                pending.finished = Some(Finished {
                    state: state.clone(),
                    valid: valid.len(),
                });
                self.save_pending(&pending).await?;
            }
            valid.last().expect("a valid turn was saved").1.clone()
        } else {
            let total_invalid = history
                .iter()
                .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                .count();
            let invalid_before = started.map_or(total_invalid, |turn| turn.invalid_before);
            let plan = TurnPlan {
                role,
                cycle: attempt,
                invalid_before,
                used: attempt as usize + self.merge_phase_invalid(&history, invalid_before),
                limit: self.merge_limit,
                previous: valid
                    .last()
                    .map(|(sender, outcome)| (*sender, outcome_text(outcome))),
                remember: true,
            };
            match self.run_turn(&mut pending, state, plan).await? {
                Ok(outcome) => outcome,
                Err(next) => return Ok(next),
            }
        };
        Ok(match outcome {
            Outcome::MergeSuccessful(_) => ImplementationState::Verifying { attempt },
            Outcome::MergeReadyForConflictReview(_) => {
                ImplementationState::ConflictReview { attempt }
            }
            Outcome::MergeBlocked(_) => ImplementationState::Merging {
                attempt: attempt + 1,
            },
            // Every conflict review counts as a merge attempt, so the loop ends at the limit.
            Outcome::ReviewApproved(_) | Outcome::ChangesRequested(_) => {
                ImplementationState::Merging {
                    attempt: attempt + 1,
                }
            }
            other => unreachable!("{other:?} is not valid for the {role:?} role"),
        })
    }

    /// Invalid results of the merge agent and of the conflict reviews among the first
    /// `invalid_before` invalid results of the history.
    fn merge_phase_invalid(&self, history: &[TurnResult], invalid_before: usize) -> usize {
        let (merge, review) = (self.agent(Role::Merge), self.agent(Role::Review));
        let mut merging = false;
        history
            .iter()
            .filter_map(|turn| {
                merging |= turn.agent == merge;
                matches!(turn.outcome, TurnOutcome::Invalid { .. })
                    .then_some(turn.agent == merge || (merging && turn.agent == review))
            })
            .take(invalid_before)
            .filter(|counts| *counts)
            .count()
    }

    fn expected_head_key(&self) -> String {
        expected_head_key(&self.spec.feature)
    }

    /// The remote head of the feature branch as Chimera last knew it, shared by all instances.
    async fn expected_head(&self) -> Result<CommitId, PipelineError> {
        Ok(
            match self
                .store
                .load_pipeline_state(&self.run, &self.expected_head_key())
                .await?
            {
                Some(saved) => serde_json::from_value(saved)?,
                None => self.initial_remote_head.clone(),
            },
        )
    }

    /// Checks the pushed commit through the repository instead of trusting the merge agent, then
    /// records it as the expected remote head and releases the merge lock. What was verified is
    /// saved first, so a restart never merges again and only repeats the idempotent rest.
    async fn verify(&self, attempt: u32) -> Result<ImplementationState, PipelineError> {
        let mut pending = self.load_pending().await?;
        let merged = match pending.verified.clone() {
            Some(merged) => merged,
            None => {
                let history = self.history().await?;
                let reported =
                    valid_turns(&history)
                        .last()
                        .and_then(|(_, outcome)| match outcome {
                            Outcome::MergeSuccessful(text) => Some(text.as_str()),
                            _ => None,
                        });
                let expected = self.expected_head().await?;
                let actual = self.repository.remote_head(&self.spec.feature).await?;
                match actual {
                    // Nothing was pushed: the lock is kept and the merge is retried.
                    Some(actual) if actual == expected => {
                        return Ok(ImplementationState::Merging {
                            attempt: attempt + 1,
                        });
                    }
                    Some(actual) if reported.and_then(pushed_commit) == Some(actual.as_str()) => {
                        pending.verified = Some(actual.clone());
                        self.save_pending(&pending).await?;
                        actual
                    }
                    other => {
                        // Neither the expected head nor the verified push. The branch being gone
                        // is just as unexpected.
                        let reason = match other {
                            Some(actual) => self
                                .policy
                                .check_remote_head(&expected, &actual)
                                .expect_err("the heads differ"),
                            None => {
                                self.policy.pause(PauseReason::UnexpectedRemoteChange);
                                PauseReason::UnexpectedRemoteChange
                            }
                        };
                        // The driver saves no state on an error, so the pause is saved here.
                        self.policy.save(self.store.as_ref(), &self.run).await?;
                        return Err(PipelineError::Paused(reason));
                    }
                }
            }
        };
        // Recorded once, before the lock is released: after that another instance may have
        // moved the expected head on, and a replay must not move it back.
        if !pending.recorded {
            self.store
                .save_pipeline_state(
                    &self.run,
                    &self.expected_head_key(),
                    serde_json::to_value(&merged)?,
                )
                .await?;
            pending.recorded = true;
            self.save_pending(&pending).await?;
        }
        if self.lock.holder().as_deref() == Some(self.instance.as_str()) {
            self.lock.release(&self.instance).await?;
        }
        Ok(ImplementationState::CleaningUp { merged })
    }

    /// Closes the workspace and removes the worktree. What was removed is saved even if a later
    /// removal fails, so a retry continues where this one stopped.
    async fn clean_up(&self, merged: CommitId) -> Result<ImplementationState, PipelineError> {
        let mut environment = self.load_environment().await?;
        let cleaned = self.environment.cleanup(&mut environment).await;
        self.store
            .save_pipeline_state(
                &self.run,
                &self.environment_key(),
                serde_json::to_value(&environment)?,
            )
            .await?;
        cleaned?;
        Ok(ImplementationState::Done(MergedOk { commit: merged }))
    }
}

/// Where the verified remote head of `feature` is saved, shared by every pipeline that works on it.
pub(crate) fn expected_head_key(feature: &BranchName) -> String {
    format!("remote_head:{feature}")
}

/// The valid results of a history with the role that produced each, oldest first.
fn valid_turns(history: &[TurnResult]) -> Vec<(Role, &Outcome)> {
    history
        .iter()
        .filter_map(|turn| match &turn.outcome {
            TurnOutcome::Valid(outcome) => Some((turn.role, outcome)),
            TurnOutcome::Invalid { .. } => None,
        })
        .collect()
}

fn valid_count(history: &[TurnResult]) -> usize {
    valid_turns(history).len()
}

/// What a turn needs besides its role: how it counts against its limit and what it is told.
struct TurnPlan {
    role: Role,
    /// Identifies the turn in `Pending` while it is in flight.
    cycle: u32,
    /// Invalid results in the history when the turn started.
    invalid_before: usize,
    /// Cycles or attempts used, corrections included.
    used: usize,
    limit: u32,
    /// The previous agent's role and description.
    previous: Option<(Role, String)>,
    /// Keep a marker of the finished turn in `Pending` for the merge phase.
    remember: bool,
}

/// What a step that was interrupted may have left half done, saved before the effect runs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Pending {
    /// A turn that has started.
    turn: Option<PendingTurn>,
    /// The sender of a handoff whose context still has to be cleared. Set only once the
    /// receiver is known to have its prompt.
    reset: Option<Role>,
    /// The last merge-phase turn that finished, until the next one starts.
    #[serde(default)]
    finished: Option<Finished>,
    /// The pushed commit that was verified.
    #[serde(default)]
    verified: Option<CommitId>,
    /// The verified commit was recorded as the expected remote head.
    #[serde(default)]
    recorded: bool,
}

/// A merge-phase turn that finished in `state`, when `valid` valid results were in the history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Finished {
    state: ImplementationState,
    valid: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingTurn {
    role: Role,
    cycle: u32,
    /// What the agent's pane showed before the prompt was sent, to tell a new result from a
    /// stale one.
    output_before: String,
    /// Invalid results in the history when the turn started; later ones are its corrections.
    invalid_before: usize,
    /// Valid results in the history when the turn started; a later one is its result.
    #[serde(default)]
    valid_before: usize,
    /// Corrections known to be delivered.
    corrections: u32,
    phase: TurnPhase,
}

/// How far the turn got, saved before each effect that cannot be taken back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TurnPhase {
    /// The assignment is being sent: it may or may not have been delivered.
    Sending,
    /// The assignment or a correction was delivered; the agent's result is awaited. If the
    /// history already holds an invalid result beyond the delivered corrections, it still has
    /// to be corrected.
    Awaiting,
    /// A correction is being sent: it may or may not have been delivered.
    Correcting,
}

fn paused(reason: PauseReason, resume_at: ImplementationState) -> ImplementationState {
    ImplementationState::Paused {
        reason,
        resume_at: Box::new(resume_at),
    }
}

fn outcome_text(outcome: &Outcome) -> String {
    match outcome {
        Outcome::ImplementationReady(text)
        | Outcome::ReviewApproved(text)
        | Outcome::ChangesRequested(text)
        | Outcome::MergeReadyForConflictReview(text)
        | Outcome::MergeSuccessful(text)
        | Outcome::MergeBlocked(text) => text.clone(),
    }
}

impl Pipeline for ImplementationPipeline {
    type State = ImplementationState;

    fn initial_state(&self) -> ImplementationState {
        ImplementationState::Provisioning
    }

    async fn step(&self, state: ImplementationState) -> Result<ImplementationState, PipelineError> {
        match state {
            ImplementationState::Provisioning => {
                self.environment
                    .provision(
                        self.store.as_ref(),
                        &self.run,
                        &self.environment_key(),
                        &self.spec,
                    )
                    .await?;
                Ok(ImplementationState::Implementing { cycle: 1 })
            }
            ImplementationState::Implementing { cycle } => {
                self.turn_step(state, Role::Implementation, cycle).await
            }
            ImplementationState::Reviewing { cycle } => {
                self.turn_step(state, Role::Review, cycle).await
            }
            ImplementationState::WaitingForMerge => {
                self.policy.check_start().map_err(PipelineError::Paused)?;
                self.lock.acquire(&self.instance).await?;
                Ok(ImplementationState::Merging { attempt: 1 })
            }
            ImplementationState::Merging { attempt } => {
                self.merge_turn_step(state, Role::Merge, attempt).await
            }
            ImplementationState::ConflictReview { attempt } => {
                self.merge_turn_step(state, Role::Review, attempt).await
            }
            ImplementationState::Verifying { attempt } => self.verify(attempt).await,
            ImplementationState::CleaningUp { merged } => self.clean_up(merged).await,
            other @ (ImplementationState::Done(_) | ImplementationState::Paused { .. }) => {
                Ok(other)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use chimera_core::error::PortError;
    use chimera_core::repository::FakeRepository;
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::terminal::{FakeTerminal, Terminal, TurnStatus};
    use chimera_core::{
        AgentProfile, Blocker, BranchName, IssueStatus, Limits, PaneId, Ticket, WorkspaceId,
    };

    use super::*;
    use futures_executor::block_on;

    use crate::driver::drive;
    use crate::environment::AgentLaunch;
    use crate::policy::Policy;

    /// Answers each prompt to a pane with that pane's next scripted reply and finishes the turn.
    /// Prompts without a script left, and reset commands, get no reply.
    struct ScriptedTerminal {
        inner: FakeTerminal,
        replies: Mutex<HashMap<PaneId, VecDeque<String>>>,
        launches: Mutex<usize>,
        gone: Mutex<HashSet<PaneId>>,
        /// Fails the next reset command without delivering it.
        fail_reset: Mutex<Option<PortError>>,
        /// Fails the next assignment after delivering it, as a lost connection would.
        fail_after_send: Mutex<Option<PortError>>,
        /// Fails the next assignment without delivering it.
        fail_send: Mutex<Option<PortError>>,
        /// Fails the next correction after delivering it.
        fail_correction_after_send: Mutex<Option<PortError>>,
        /// Every prompt delivered, kept after the pane is closed.
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
            self.gone.lock().unwrap().remove(pane);
            self.inner.launch_agent(pane, command_line).await
        }
        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            if prompt == "/clear"
                && let Some(error) = self.fail_reset.lock().unwrap().take()
            {
                return Err(error);
            }
            if prompt != "/clear"
                && let Some(error) = self.fail_send.lock().unwrap().take()
            {
                return Err(error);
            }
            let reply = if prompt == "/clear" {
                None
            } else {
                self.replies
                    .lock()
                    .unwrap()
                    .get_mut(pane)
                    .and_then(VecDeque::pop_front)
            };
            if let Some(reply) = reply {
                self.inner.script_output(pane, reply);
                self.inner.script_statuses(pane, [TurnStatus::Finished]);
            }
            self.inner.send_prompt(pane, prompt).await?;
            self.sent
                .lock()
                .unwrap()
                .entry(pane.clone())
                .or_default()
                .push(prompt.to_string());
            if prompt.contains("was rejected")
                && let Some(error) = self.fail_correction_after_send.lock().unwrap().take()
            {
                return Err(error);
            }
            match self.fail_after_send.lock().unwrap().take() {
                Some(error) if prompt != "/clear" => Err(error),
                _ => Ok(()),
            }
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

    struct Fixture {
        terminal: Arc<ScriptedTerminal>,
        store: Arc<FakeRunStore>,
        repository: Arc<FakeRepository>,
        policy: Arc<Policy>,
        pipeline: ImplementationPipeline,
    }

    fn issue() -> IssueRef {
        IssueRef::new("o", "r", 31).unwrap()
    }

    fn ticket() -> Ticket {
        Ticket {
            issue: issue(),
            status: IssueStatus::Open,
            blockers: Vec::<Blocker>::new(),
        }
    }

    fn fixture(work_item: WorkItem, cycle_limit: u32) -> Fixture {
        fixture_for(work_item, cycle_limit, "t31", None)
    }

    /// A pipeline instance `instance`; with `other` it shares that fixture's run store,
    /// repository and policy, and so the merge lock.
    fn fixture_for(
        work_item: WorkItem,
        cycle_limit: u32,
        instance: &str,
        other: Option<&Fixture>,
    ) -> Fixture {
        let branch = |name: &str| BranchName::new(name).unwrap();
        let repository = match other {
            Some(other) => other.repository.clone(),
            None => {
                let repository = Arc::new(FakeRepository::new(
                    branch("main"),
                    CommitId::new("c0").unwrap(),
                ));
                repository.add_branch(branch("feat"), CommitId::new("c0").unwrap());
                repository
            }
        };
        let terminal = Arc::new(ScriptedTerminal {
            inner: FakeTerminal::new(),
            replies: Mutex::default(),
            launches: Mutex::new(0),
            gone: Mutex::default(),
            fail_reset: Mutex::default(),
            fail_after_send: Mutex::default(),
            fail_send: Mutex::default(),
            fail_correction_after_send: Mutex::default(),
            sent: Mutex::default(),
        });
        let store = other.map_or_else(|| Arc::new(FakeRunStore::new()), |f| f.store.clone());
        let policy = other.map_or_else(
            || Arc::new(Policy::new(&Limits::default())),
            |f| f.policy.clone(),
        );
        let lock = block_on(MergeLock::open(
            store.clone(),
            RunId::new("run").unwrap(),
            &branch("feat"),
        ))
        .unwrap();
        let environment = Arc::new(EnvironmentService::new(
            repository.clone(),
            terminal.clone(),
            policy.clone(),
        ));
        let turns = Arc::new(AgentTurns::new(
            terminal.clone(),
            store.clone(),
            policy.clone(),
            Duration::from_millis(1),
        ));
        let profile = |name: &str| AgentProfile::new("p", "m", format!("You are {name}."));
        let pipeline = ImplementationPipeline {
            run: RunId::new("run").unwrap(),
            instance: instance.into(),
            work_item,
            issue: issue(),
            configuration: AgentConfiguration {
                implementation: profile("implementer"),
                review: profile("reviewer"),
                merge: profile("merger"),
            },
            cycle_limit,
            merge_limit: 3,
            spec: ProvisionSpec {
                worktree: format!("/wt/{instance}").into(),
                task_branch: branch(&format!("task-{instance}")),
                feature: branch("feat"),
                agents: [Role::Implementation, Role::Review, Role::Merge]
                    .map(|role| AgentLaunch {
                        role,
                        command_line: format!("agent {role:?}"),
                    })
                    .to_vec(),
            },
            initial_remote_head: CommitId::new("c0").unwrap(),
            store: store.clone(),
            repository: repository.clone(),
            policy: policy.clone(),
            lock,
            environment,
            turns,
        };
        Fixture {
            terminal,
            store,
            repository,
            policy,
            pipeline,
        }
    }

    impl Fixture {
        /// Drives the task until its work is approved and it waits for the merge.
        async fn drive(&self) -> Result<ImplementationState, PipelineError> {
            let mut state = match self
                .store
                .load_pipeline_state(&self.pipeline.run, &self.pipeline.instance)
                .await?
            {
                Some(saved) => serde_json::from_value(saved)?,
                None => self.pipeline.initial_state(),
            };
            while !matches!(
                state,
                ImplementationState::WaitingForMerge | ImplementationState::Paused { .. }
            ) {
                state = self.pipeline.step(state).await?;
                self.store
                    .save_pipeline_state(
                        &self.pipeline.run,
                        &self.pipeline.instance,
                        serde_json::to_value(&state)?,
                    )
                    .await?;
            }
            Ok(state)
        }

        /// Drives the task to the end, through the merge.
        async fn drive_through(&self) -> Result<ImplementationState, PipelineError> {
            drive(
                self.store.as_ref(),
                &self.pipeline.run,
                &self.pipeline.instance,
                &self.pipeline,
            )
            .await
        }

        async fn pane(&self, role: Role) -> PaneId {
            self.pipeline
                .load_environment()
                .await
                .unwrap()
                .pane(role)
                .unwrap()
                .clone()
        }

        /// Scripts the replies of `role`'s agent, creating nothing: panes exist after
        /// provisioning, so this provisions first.
        async fn script(&self, role: Role, replies: &[&str]) {
            if self
                .store
                .load_pipeline_state(&self.pipeline.run, &self.pipeline.environment_key())
                .await
                .unwrap()
                .is_none()
            {
                let state = self.pipeline.step(ImplementationState::Provisioning).await;
                state.unwrap();
            }
            let pane = self.pane(role).await;
            self.terminal
                .replies
                .lock()
                .unwrap()
                .entry(pane)
                .or_default()
                .extend(replies.iter().map(|r| r.to_string()));
        }

        async fn prompts(&self, role: Role) -> Vec<String> {
            let pane = self.pane(role).await;
            self.terminal
                .sent
                .lock()
                .unwrap()
                .get(&pane)
                .cloned()
                .unwrap_or_default()
        }
    }

    const READY: &str = r#"{"ImplementationReady":"done"}"#;
    const APPROVED: &str = r#"{"ReviewApproved":"fine"}"#;
    const CHANGES: &str = r#"{"ChangesRequested":"fix it"}"#;

    #[tokio::test]
    async fn approved_first_time() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );

        let implementation = f.prompts(Role::Implementation).await;
        assert!(implementation[0].contains("You are implementer."));
        assert!(implementation[0].contains(&issue().to_string()));
        let review = f.prompts(Role::Review).await;
        assert!(review[0].contains("You are reviewer.") && review[0].contains("done"));
        // The implementer is reset once the reviewer has started.
        assert_eq!(implementation[1], "/clear");
        assert_eq!(*f.terminal.launches.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn change_requests_loop_back_to_implementing() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY, READY, READY]).await;
        f.script(Role::Review, &[CHANGES, CHANGES, APPROVED]).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );

        let implementation = f.prompts(Role::Implementation).await;
        assert!(implementation.iter().any(|p| p.contains("fix it")));
        let history = f.store.load_history(&f.pipeline.run).await.unwrap();
        assert_eq!(history.len(), 6);
    }

    #[tokio::test]
    async fn findings_are_given_to_the_first_implementation_turn() {
        let f = fixture(WorkItem::Findings("the findings".into()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;

        f.drive().await.unwrap();

        assert!(f.prompts(Role::Implementation).await[0].contains("the findings"));
    }

    #[tokio::test]
    async fn exhausted_cycles_pause_this_instance_only() {
        let f = fixture(WorkItem::Ticket(ticket()), 2);
        f.script(Role::Implementation, &[READY, READY]).await;
        f.script(Role::Review, &[CHANGES, CHANGES]).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Implementing { cycle: 3 }),
            }
        );
        assert_eq!(f.policy.check_start(), Ok(()));
    }

    #[tokio::test]
    async fn invalid_output_corrections_count_toward_the_limit() {
        let f = fixture(WorkItem::Ticket(ticket()), 2);
        f.script(Role::Implementation, &["nonsense", "still nonsense"])
            .await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Implementing { cycle: 1 }),
            }
        );
    }

    #[tokio::test]
    async fn a_confirmed_correction_with_identical_output_still_counts() {
        let f = fixture(WorkItem::Ticket(ticket()), 2);
        f.script(Role::Implementation, &["nonsense", "nonsense"])
            .await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Implementing { cycle: 1 }),
            }
        );
        let history = f.store.load_history(&f.pipeline.run).await.unwrap();
        let invalid = history
            .iter()
            .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
            .count();
        assert_eq!(invalid, 2);
    }

    #[tokio::test]
    async fn restart_from_a_confirmed_correction_does_not_resend_it() {
        let f = fixture(WorkItem::Ticket(ticket()), 3);
        f.script(Role::Implementation, &[]).await;
        f.script(Role::Review, &[APPROVED]).await;
        let agent = f.pipeline.agent(Role::Implementation);
        f.store
            .append_turn(
                &f.pipeline.run,
                TurnResult {
                    agent,
                    role: Role::Implementation,
                    outcome: TurnOutcome::Invalid {
                        output: "nonsense".into(),
                        problem: "no outcome found".into(),
                    },
                },
            )
            .await
            .unwrap();
        // The correction was confirmed and saved; the agent finished with the same output.
        show(&f, Role::Implementation, READY).await;
        save_pending_turn(&f, Role::Implementation, TurnPhase::Awaiting, "nonsense", 1).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );
        let sent = f.prompts(Role::Implementation).await;
        assert!(sent.iter().all(|p| !p.contains("was rejected")));
    }

    #[tokio::test]
    async fn a_correction_within_the_limit_recovers() {
        let f = fixture(WorkItem::Ticket(ticket()), 3);
        f.script(Role::Implementation, &["nonsense", READY]).await;
        f.script(Role::Review, &[APPROVED]).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );
    }

    #[tokio::test]
    async fn global_pause_stops_before_the_next_handoff() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        let step = f
            .pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        assert_eq!(step, ImplementationState::Reviewing { cycle: 1 });
        f.store
            .save_pipeline_state(
                &f.pipeline.run,
                &f.pipeline.instance,
                serde_json::to_value(&step).unwrap(),
            )
            .await
            .unwrap();

        f.policy.pause(PauseReason::GlobalPause);
        let error = f.drive().await.unwrap_err();

        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::GlobalPause)
        ));
        assert!(f.prompts(Role::Review).await.is_empty());
    }

    #[tokio::test]
    async fn restart_does_not_reprovision_or_resend_saved_turns() {
        for saved in [
            ImplementationState::Provisioning,
            ImplementationState::Implementing { cycle: 1 },
            ImplementationState::Reviewing { cycle: 1 },
        ] {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            f.script(Role::Implementation, &[READY]).await;
            f.script(Role::Review, &[APPROVED]).await;
            // The turns before `saved` completed and were saved, but the state was not.
            let completed = match saved {
                ImplementationState::Provisioning => 0,
                ImplementationState::Implementing { .. } => 1,
                _ => 2,
            };
            let mut next = ImplementationState::Implementing { cycle: 1 };
            for _ in 0..completed {
                next = f.pipeline.step(next).await.unwrap();
            }
            f.store
                .save_pipeline_state(
                    &f.pipeline.run,
                    &f.pipeline.instance,
                    serde_json::to_value(&saved).unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                f.drive().await.unwrap(),
                ImplementationState::WaitingForMerge,
                "from {saved:?}"
            );

            assert_eq!(*f.terminal.launches.lock().unwrap(), 3, "from {saved:?}");
            let sent = f.prompts(Role::Implementation).await;
            assert_eq!(
                sent.iter()
                    .filter(|p| p.contains("You are implementer."))
                    .count(),
                1,
                "from {saved:?}"
            );
            let reviews = f.prompts(Role::Review).await;
            assert_eq!(
                reviews
                    .iter()
                    .filter(|p| p.contains("You are reviewer."))
                    .count(),
                1,
                "from {saved:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_lost_agent_is_relaunched_before_continuing() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        let pane = f.pane(Role::Implementation).await;
        f.terminal.gone.lock().unwrap().insert(pane.clone());

        let state = f
            .pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        assert_eq!(state, ImplementationState::Implementing { cycle: 1 });
        assert_eq!(*f.terminal.launches.lock().unwrap(), 4);
        assert_eq!(
            f.terminal.inner.launched_command(&pane).as_deref(),
            Some("agent Implementation")
        );

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );
    }

    fn assignments(prompts: &[String], who: &str) -> Vec<String> {
        let marker = format!("You are {who}.");
        prompts
            .iter()
            .filter(|p| p.contains(&marker))
            .cloned()
            .collect()
    }

    async fn save_pending_turn(
        f: &Fixture,
        role: Role,
        phase: TurnPhase,
        output_before: &str,
        corrections: u32,
    ) {
        let pending = Pending {
            turn: Some(PendingTurn {
                role,
                cycle: 1,
                output_before: output_before.into(),
                invalid_before: 0,
                valid_before: 0,
                corrections,
                phase,
            }),
            ..Pending::default()
        };
        f.pipeline.save_pending(&pending).await.unwrap();
    }

    async fn show(f: &Fixture, role: Role, output: &str) {
        let pane = f.pane(role).await;
        f.terminal.inner.script_output(&pane, output);
        f.terminal
            .inner
            .script_statuses(&pane, [TurnStatus::Finished]);
    }

    async fn save_state(f: &Fixture, state: ImplementationState) {
        f.store
            .save_pipeline_state(
                &f.pipeline.run,
                &f.pipeline.instance,
                serde_json::to_value(state).unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn restart_during_a_turn_reconciles_instead_of_resending() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        // The prompt was delivered and the agent is working when the process dies.
        save_pending_turn(&f, Role::Implementation, TurnPhase::Sending, "", 0).await;
        let pane = f.pane(Role::Implementation).await;
        f.terminal
            .send_prompt(&pane, "You are implementer.")
            .await
            .unwrap();

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );

        let sent = f.prompts(Role::Implementation).await;
        assert_eq!(assignments(&sent, "implementer").len(), 1);
    }

    #[tokio::test]
    async fn a_delivered_turn_that_finished_with_identical_output_is_not_resent() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[]).await;
        f.script(Role::Review, &[APPROVED]).await;
        // The delivery was saved; the agent finished with exactly what the pane showed before.
        show(&f, Role::Implementation, READY).await;
        save_pending_turn(&f, Role::Implementation, TurnPhase::Awaiting, READY, 0).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );

        let sent = f.prompts(Role::Implementation).await;
        assert!(assignments(&sent, "implementer").is_empty());
    }

    #[tokio::test]
    async fn an_unsaved_delivery_with_identical_output_stays_uncertain() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[]).await;
        show(&f, Role::Implementation, READY).await;
        save_pending_turn(&f, Role::Implementation, TurnPhase::Sending, READY, 0).await;

        let error = f.drive().await.unwrap_err();

        assert!(error.is_uncertain());
        let sent = f.prompts(Role::Implementation).await;
        assert!(assignments(&sent, "implementer").is_empty());
    }

    #[tokio::test]
    async fn restart_before_the_handoff_send_does_not_reset_the_sender() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[]).await;
        f.pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        // The intent was saved but the process died before, or during, the send.
        save_pending_turn(&f, Role::Review, TurnPhase::Sending, "", 0).await;
        show(&f, Role::Review, "").await;
        save_state(&f, ImplementationState::Reviewing { cycle: 1 }).await;

        let error = f.drive().await.unwrap_err();

        assert!(error.is_uncertain());
        assert!(
            !f.prompts(Role::Implementation)
                .await
                .contains(&"/clear".to_string())
        );
        assert!(f.prompts(Role::Review).await.is_empty());
    }

    #[tokio::test]
    async fn a_failed_handoff_send_keeps_the_sender_even_if_paused_next() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        f.pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        *f.terminal.fail_send.lock().unwrap() = Some(PortError::failed("refused"));

        let step = f
            .pipeline
            .step(ImplementationState::Reviewing { cycle: 1 })
            .await
            .unwrap_err();
        assert!(step.is_failed());
        assert_eq!(f.pipeline.load_pending().await.unwrap(), Pending::default());

        f.policy.pause(PauseReason::GlobalPause);
        save_state(&f, ImplementationState::Reviewing { cycle: 1 }).await;
        assert!(matches!(
            f.drive().await.unwrap_err(),
            PipelineError::Paused(PauseReason::GlobalPause)
        ));
        assert!(
            !f.prompts(Role::Implementation)
                .await
                .contains(&"/clear".to_string())
        );
    }

    #[tokio::test]
    async fn restart_before_a_correction_is_delivered_keeps_the_correction() {
        let f = fixture(WorkItem::Ticket(ticket()), 2);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        // The first invalid result was saved; the process died before sending its correction.
        let agent = f.pipeline.agent(Role::Implementation);
        f.store
            .append_turn(
                &f.pipeline.run,
                TurnResult {
                    agent,
                    role: Role::Implementation,
                    outcome: TurnOutcome::Invalid {
                        output: "nonsense".into(),
                        problem: "no outcome found".into(),
                    },
                },
            )
            .await
            .unwrap();
        show(&f, Role::Implementation, "nonsense").await;
        save_pending_turn(&f, Role::Implementation, TurnPhase::Awaiting, "", 0).await;

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );

        let sent = f.prompts(Role::Implementation).await;
        assert_eq!(
            sent.iter().filter(|p| p.contains("was rejected")).count(),
            1
        );
        let history = f.store.load_history(&f.pipeline.run).await.unwrap();
        let invalid = history
            .iter()
            .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
            .count();
        assert_eq!(invalid, 1);
    }

    #[tokio::test]
    async fn an_uncertain_correction_send_is_reconciled_without_resending() {
        let f = fixture(WorkItem::Ticket(ticket()), 3);
        f.script(Role::Implementation, &["nonsense", READY]).await;
        f.script(Role::Review, &[APPROVED]).await;
        *f.terminal.fail_correction_after_send.lock().unwrap() = Some(PortError::uncertain("lost"));

        assert!(f.drive().await.unwrap_err().is_uncertain());
        assert_eq!(
            f.pipeline.load_pending().await.unwrap().turn.unwrap().phase,
            TurnPhase::Correcting
        );

        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );
        let sent = f.prompts(Role::Implementation).await;
        assert_eq!(
            sent.iter().filter(|p| p.contains("was rejected")).count(),
            1
        );
    }

    #[tokio::test]
    async fn a_correction_with_no_sign_of_delivery_stays_uncertain() {
        let f = fixture(WorkItem::Ticket(ticket()), 3);
        f.script(Role::Implementation, &[]).await;
        let agent = f.pipeline.agent(Role::Implementation);
        f.store
            .append_turn(
                &f.pipeline.run,
                TurnResult {
                    agent,
                    role: Role::Implementation,
                    outcome: TurnOutcome::Invalid {
                        output: "nonsense".into(),
                        problem: "no outcome found".into(),
                    },
                },
            )
            .await
            .unwrap();
        show(&f, Role::Implementation, "nonsense").await;
        save_pending_turn(
            &f,
            Role::Implementation,
            TurnPhase::Correcting,
            "nonsense",
            0,
        )
        .await;

        assert!(f.drive().await.unwrap_err().is_uncertain());
        assert!(f.prompts(Role::Implementation).await.is_empty());
    }

    #[tokio::test]
    async fn uncertain_send_is_reconciled_without_resending() {
        for role in [Role::Implementation, Role::Review] {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            f.script(Role::Implementation, &[READY]).await;
            f.script(Role::Review, &[APPROVED]).await;
            if role == Role::Review {
                f.pipeline
                    .step(ImplementationState::Implementing { cycle: 1 })
                    .await
                    .unwrap();
                f.store
                    .save_pipeline_state(
                        &f.pipeline.run,
                        &f.pipeline.instance,
                        serde_json::to_value(ImplementationState::Reviewing { cycle: 1 }).unwrap(),
                    )
                    .await
                    .unwrap();
            }
            *f.terminal.fail_after_send.lock().unwrap() = Some(PortError::uncertain("lost"));

            let error = f.drive().await.unwrap_err();
            assert!(error.is_uncertain(), "{role:?}");

            assert_eq!(
                f.drive().await.unwrap(),
                ImplementationState::WaitingForMerge,
                "{role:?}"
            );
            let sent = f.prompts(role).await;
            let who = if role == Role::Review {
                "reviewer"
            } else {
                "implementer"
            };
            assert_eq!(assignments(&sent, who).len(), 1, "{role:?}");
        }
    }

    #[tokio::test]
    async fn a_failed_sender_reset_is_retried_before_moving_on() {
        for error in [PortError::failed("refused"), PortError::uncertain("lost")] {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            f.script(Role::Implementation, &[READY]).await;
            f.script(Role::Review, &[APPROVED]).await;
            f.pipeline
                .step(ImplementationState::Implementing { cycle: 1 })
                .await
                .unwrap();
            *f.terminal.fail_reset.lock().unwrap() = Some(error.clone());

            let step = f
                .pipeline
                .step(ImplementationState::Reviewing { cycle: 1 })
                .await
                .unwrap_err();
            assert_eq!(step.is_uncertain(), error.is_uncertain());
            assert_eq!(
                f.pipeline.load_pending().await.unwrap().reset,
                Some(Role::Implementation)
            );
            let implementation = f.prompts(Role::Implementation).await;
            assert!(!implementation.contains(&"/clear".to_string()));

            // The receiver's saved result survives the restart and decides the next state.
            let next = f
                .pipeline
                .step(ImplementationState::Reviewing { cycle: 1 })
                .await
                .unwrap();
            assert_eq!(next, ImplementationState::WaitingForMerge);
            let implementation = f.prompts(Role::Implementation).await;
            assert_eq!(implementation.iter().filter(|p| *p == "/clear").count(), 1);
            let review = f.prompts(Role::Review).await;
            assert_eq!(assignments(&review, "reviewer").len(), 1);
            assert_eq!(f.pipeline.load_pending().await.unwrap(), Pending::default());
        }
    }

    #[tokio::test]
    async fn a_lost_receiver_is_relaunched_even_if_the_sender_reset_failed() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &[APPROVED, APPROVED]).await;
        f.pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        let reviewer = f.pane(Role::Review).await;
        f.terminal.gone.lock().unwrap().insert(reviewer.clone());
        *f.terminal.fail_reset.lock().unwrap() = Some(PortError::failed("refused"));

        let state = f
            .pipeline
            .step(ImplementationState::Reviewing { cycle: 1 })
            .await
            .unwrap();

        assert_eq!(state, ImplementationState::Reviewing { cycle: 1 });
        assert_eq!(*f.terminal.launches.lock().unwrap(), 4);
        let pending = f.pipeline.load_pending().await.unwrap();
        assert_eq!(pending.reset, Some(Role::Implementation));
        assert_eq!(pending.turn, None);

        assert_eq!(
            f.pipeline.step(state).await.unwrap(),
            ImplementationState::WaitingForMerge
        );
        // The owed reset, then the one of the repeated handoff.
        let implementation = f.prompts(Role::Implementation).await;
        assert_eq!(implementation.iter().filter(|p| *p == "/clear").count(), 2);
    }

    #[tokio::test]
    async fn exhausted_corrections_pause_even_if_the_sender_reset_failed() {
        let f = fixture(WorkItem::Ticket(ticket()), 2);
        f.script(Role::Implementation, &[READY]).await;
        f.script(Role::Review, &["nonsense", "more nonsense"]).await;
        f.pipeline
            .step(ImplementationState::Implementing { cycle: 1 })
            .await
            .unwrap();
        *f.terminal.fail_reset.lock().unwrap() = Some(PortError::uncertain("lost"));

        let state = f
            .pipeline
            .step(ImplementationState::Reviewing { cycle: 1 })
            .await
            .unwrap();

        assert_eq!(
            state,
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Reviewing { cycle: 1 }),
            }
        );
        assert_eq!(
            f.pipeline.load_pending().await.unwrap().reset,
            Some(Role::Implementation)
        );
    }

    #[tokio::test]
    async fn every_assignment_keeps_the_findings() {
        let f = fixture(WorkItem::Findings("the findings".into()), 5);
        f.script(Role::Implementation, &[READY, READY]).await;
        f.script(Role::Review, &[CHANGES, APPROVED]).await;

        f.drive().await.unwrap();

        let implementation = assignments(&f.prompts(Role::Implementation).await, "implementer");
        let review = assignments(&f.prompts(Role::Review).await, "reviewer");
        assert_eq!((implementation.len(), review.len()), (2, 2));
        for prompt in implementation.iter().chain(&review) {
            assert!(prompt.contains("the findings"), "{prompt}");
        }
        assert!(implementation[1].contains("fix it"));
        assert!(review[0].contains("done"));
    }

    const CONFLICTS: &str = r#"{"MergeReadyForConflictReview":"resolved"}"#;
    const MERGED: &str = r#"{"MergeSuccessful":"c1 merged and pushed"}"#;
    const BLOCKED: &str = r#"{"MergeBlocked":"tests fail"}"#;

    fn commit(id: &str) -> CommitId {
        CommitId::new(id).unwrap()
    }

    fn feature() -> BranchName {
        BranchName::new("feat").unwrap()
    }

    /// Approves the work, scripts the merge agent and the conflict reviews, and stops at
    /// `WaitingForMerge`.
    async fn ready_to_merge(f: &Fixture, merge: &[&str], conflict_reviews: &[&str]) {
        f.script(Role::Implementation, &[READY]).await;
        let reviews: Vec<&str> = [APPROVED]
            .into_iter()
            .chain(conflict_reviews.iter().copied())
            .collect();
        f.script(Role::Review, &reviews).await;
        f.script(Role::Merge, merge).await;
        assert_eq!(
            f.drive().await.unwrap(),
            ImplementationState::WaitingForMerge
        );
    }

    fn holder(f: &Fixture) -> Option<String> {
        f.pipeline.lock.holder()
    }

    #[tokio::test]
    async fn a_clean_merge_is_verified_without_another_review() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[MERGED], &[]).await;
        f.repository.set_remote_head(feature(), commit("c1"));

        assert_eq!(
            f.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c1")
            })
        );

        assert_eq!(holder(&f), None);
        assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c1"));
        assert_eq!(
            assignments(&f.prompts(Role::Review).await, "reviewer").len(),
            1
        );
        assert_eq!(
            assignments(&f.prompts(Role::Merge).await, "merger").len(),
            1
        );
        // The environment is cleaned up and the history is kept.
        let path = f.pipeline.spec.worktree.clone();
        assert_eq!(f.repository.worktree_branch(&path), None);
        assert!(
            f.pipeline
                .load_environment()
                .await
                .unwrap()
                .workspace
                .is_none()
        );
        assert_eq!(
            f.store.load_history(&f.pipeline.run).await.unwrap().len(),
            3
        );
    }

    #[tokio::test]
    async fn waiting_for_merge_holds_the_lock_before_merging() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[], &[]).await;

        let next = f
            .pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();

        assert_eq!(next, ImplementationState::Merging { attempt: 1 });
        assert_eq!(holder(&f), Some("t31".into()));
    }

    #[tokio::test]
    async fn a_global_pause_stops_before_the_lock_is_taken() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[], &[]).await;
        f.policy.pause(PauseReason::GlobalPause);

        let error = f
            .pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::GlobalPause)
        ));
        assert_eq!(holder(&f), None);
    }

    #[tokio::test]
    async fn conflict_review_corrections_return_to_the_merge_agent_which_keeps_the_lock() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[CONFLICTS, MERGED], &[CHANGES]).await;
        f.repository.set_remote_head(feature(), commit("c1"));

        let mut state = f
            .pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();
        let mut visited = vec![state.clone()];
        while !matches!(state, ImplementationState::CleaningUp { .. }) {
            state = f.pipeline.step(state).await.unwrap();
            if !matches!(state, ImplementationState::CleaningUp { .. }) {
                assert_eq!(holder(&f), Some("t31".into()), "{state:?}");
            }
            visited.push(state.clone());
        }

        assert_eq!(
            visited,
            [
                ImplementationState::Merging { attempt: 1 },
                ImplementationState::ConflictReview { attempt: 1 },
                ImplementationState::Merging { attempt: 2 },
                ImplementationState::Verifying { attempt: 2 },
                ImplementationState::CleaningUp {
                    merged: commit("c1")
                },
            ]
        );
        let merger = assignments(&f.prompts(Role::Merge).await, "merger");
        assert_eq!(merger.len(), 2);
        assert!(merger[1].contains("fix it"));
        // The conflict review is the reviewer's second assignment and sees the merge agent's
        // description.
        let reviewer = assignments(&f.prompts(Role::Review).await, "reviewer");
        assert_eq!(reviewer.len(), 2);
        assert!(reviewer[1].contains("resolved"));
        assert_eq!(holder(&f), None);
    }

    #[tokio::test]
    async fn a_failed_push_keeps_the_lock_and_retries_the_merge() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[MERGED, MERGED], &[]).await;
        let mut state = f
            .pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();

        // The agent reports a push, but the remote did not move.
        state = f.pipeline.step(state).await.unwrap();
        assert_eq!(state, ImplementationState::Verifying { attempt: 1 });
        state = f.pipeline.step(state).await.unwrap();
        assert_eq!(state, ImplementationState::Merging { attempt: 2 });
        assert_eq!(holder(&f), Some("t31".into()));
        assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c0"));

        f.repository.set_remote_head(feature(), commit("c1"));
        state = f.pipeline.step(state).await.unwrap();
        assert_eq!(state, ImplementationState::Verifying { attempt: 2 });
        state = f.pipeline.step(state).await.unwrap();
        assert_eq!(
            state,
            ImplementationState::CleaningUp {
                merged: commit("c1")
            }
        );
        assert_eq!(holder(&f), None);
        assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c1"));
    }

    #[tokio::test]
    async fn a_blocked_merge_counts_as_an_attempt() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[BLOCKED], &[]).await;
        let state = f
            .pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();

        assert_eq!(
            f.pipeline.step(state).await.unwrap(),
            ImplementationState::Merging { attempt: 2 }
        );
        assert_eq!(holder(&f), Some("t31".into()));
    }

    #[tokio::test]
    async fn an_unverified_remote_change_pauses_the_run_and_keeps_the_lock() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[MERGED], &[]).await;
        // Somebody else moved the branch; it is not the commit the agent reported.
        f.repository.set_remote_head(feature(), commit("c9"));

        let error = f.drive_through().await.unwrap_err();

        assert!(matches!(
            error,
            PipelineError::Paused(PauseReason::UnexpectedRemoteChange)
        ));
        assert_eq!(
            f.policy.check_start(),
            Err(PauseReason::UnexpectedRemoteChange)
        );
        assert_eq!(holder(&f), Some("t31".into()));
        assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c0"));
    }

    #[tokio::test]
    async fn the_merge_limit_pauses_while_keeping_the_lock_which_blocks_a_second_instance() {
        let a = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&a, &[BLOCKED, BLOCKED, BLOCKED], &[]).await;
        let b = fixture_for(WorkItem::Ticket(ticket()), 5, "t32", Some(&a));
        ready_to_merge(&b, &[MERGED], &[]).await;

        assert_eq!(
            a.drive_through().await.unwrap(),
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Merging { attempt: 4 }),
            }
        );

        assert_eq!(holder(&a), Some("t31".into()));
        // The pause is this instance's only.
        assert_eq!(a.policy.check_start(), Ok(()));
        let blocked = tokio::time::timeout(Duration::from_millis(50), b.drive_through()).await;
        assert!(blocked.is_err(), "the second instance must keep waiting");
        assert_eq!(holder(&b), Some("t31".into()));
        assert_eq!(b.pipeline.lock.waiting(), ["t32"]);
    }

    #[tokio::test]
    async fn instances_merge_in_the_order_they_queued() {
        let a = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&a, &[MERGED], &[]).await;
        let b = fixture_for(WorkItem::Ticket(ticket()), 5, "t32", Some(&a));
        let merged_c2 = r#"{"MergeSuccessful":"c2 pushed"}"#;
        ready_to_merge(&b, &[merged_c2], &[]).await;
        a.repository.set_remote_head(feature(), commit("c1"));

        // B queues behind A, which takes the lock.
        a.pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();
        let waiting = tokio::time::timeout(
            Duration::from_millis(20),
            b.pipeline.step(ImplementationState::WaitingForMerge),
        )
        .await;
        assert!(waiting.is_err());
        assert_eq!(a.pipeline.lock.waiting(), ["t32"]);
        save_state(&a, ImplementationState::Merging { attempt: 1 }).await;

        assert_eq!(
            a.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c1")
            })
        );
        // Releasing hands the lock straight to B.
        assert_eq!(holder(&a), Some("t32".into()));

        b.repository.set_remote_head(feature(), commit("c2"));
        assert_eq!(
            b.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c2")
            })
        );
        assert_eq!(holder(&b), None);
        assert_eq!(b.pipeline.expected_head().await.unwrap(), commit("c2"));
    }

    #[tokio::test]
    async fn restart_from_every_merge_state_keeps_the_lock_and_never_repeats_a_merge() {
        let sequence = [
            ImplementationState::WaitingForMerge,
            ImplementationState::Merging { attempt: 1 },
            ImplementationState::ConflictReview { attempt: 1 },
            ImplementationState::Merging { attempt: 2 },
            ImplementationState::Verifying { attempt: 2 },
            ImplementationState::CleaningUp {
                merged: commit("c1"),
            },
        ];
        for crashed in 0..sequence.len() {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            ready_to_merge(&f, &[CONFLICTS, MERGED], &[APPROVED]).await;
            f.repository.set_remote_head(feature(), commit("c1"));
            // Every step up to `crashed` ran, but the state it returned was not saved.
            for (state, next) in sequence[..=crashed].iter().zip(&sequence[1..]) {
                assert_eq!(&f.pipeline.step(state.clone()).await.unwrap(), next);
            }
            if crashed + 1 == sequence.len() {
                f.pipeline.step(sequence[crashed].clone()).await.unwrap();
            }
            save_state(&f, sequence[crashed].clone()).await;
            let context = format!("from {:?}", sequence[crashed]);
            if (1..=3).contains(&crashed) {
                assert_eq!(holder(&f), Some("t31".into()), "{context}");
            }

            assert_eq!(
                f.drive_through().await.unwrap(),
                ImplementationState::Done(MergedOk {
                    commit: commit("c1")
                }),
                "{context}"
            );

            let merger = assignments(&f.prompts(Role::Merge).await, "merger");
            assert_eq!(merger.len(), 2, "{context}");
            let reviewer = assignments(&f.prompts(Role::Review).await, "reviewer");
            assert_eq!(reviewer.len(), 2, "{context}");
            assert_eq!(holder(&f), None, "{context}");
        }
    }

    #[test]
    fn the_pushed_commit_is_the_first_word_of_the_explanation() {
        assert_eq!(pushed_commit("c1 merged and pushed"), Some("c1"));
        assert_eq!(pushed_commit("  c1\ttests passed"), Some("c1"));
        assert_eq!(pushed_commit("c1"), Some("c1"));
        assert_eq!(pushed_commit("pushed c1; tests passed"), None);
        assert_eq!(pushed_commit("(c1) merged"), None);
        assert_eq!(pushed_commit("c1; merged"), None);
        assert_eq!(pushed_commit(" "), None);
    }

    #[tokio::test]
    async fn an_explanation_may_continue_after_the_pushed_commit() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[r#"{"MergeSuccessful":"c1 tests passed"}"#], &[]).await;
        f.repository.set_remote_head(feature(), commit("c1"));

        assert_eq!(
            f.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c1")
            })
        );
        let prompt = &assignments(&f.prompts(Role::Merge).await, "merger")[0];
        assert!(prompt.contains("must start with the full SHA"), "{prompt}");
    }

    #[tokio::test]
    async fn an_unrelated_remote_head_mentioned_later_is_not_accepted() {
        for text in [
            "c1 pushed; compared against c9",
            "c1 pushed; c9 is the other mention",
        ] {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            let reply = format!(r#"{{"MergeSuccessful":"{text}"}}"#);
            ready_to_merge(&f, &[&reply], &[]).await;
            f.repository.set_remote_head(feature(), commit("c9"));

            let error = f.drive_through().await.unwrap_err();

            assert!(
                matches!(
                    error,
                    PipelineError::Paused(PauseReason::UnexpectedRemoteChange)
                ),
                "{text}"
            );
            assert_eq!(holder(&f), Some("t31".into()), "{text}");
            assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c0"));
        }
    }

    #[tokio::test]
    async fn the_pushed_commit_is_the_first_word_when_other_commits_are_mentioned() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(
            &f,
            &[r#"{"MergeSuccessful":"c1 pushed on top of c0, not c9"}"#],
            &[],
        )
        .await;
        f.repository.set_remote_head(feature(), commit("c1"));

        assert_eq!(
            f.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c1")
            })
        );
    }

    #[tokio::test]
    async fn a_success_without_a_commit_is_corrected_and_then_verified() {
        for missing in [
            "merged; tests passed",
            "pushed c1; tests passed",
            "c1; merged",
        ] {
            let f = fixture(WorkItem::Ticket(ticket()), 5);
            let invalid = format!(r#"{{"MergeSuccessful":"{missing}"}}"#);
            ready_to_merge(&f, &[&invalid, MERGED], &[]).await;
            f.repository.set_remote_head(feature(), commit("c1"));

            assert_eq!(
                f.drive_through().await.unwrap(),
                ImplementationState::Done(MergedOk {
                    commit: commit("c1")
                }),
                "{missing}"
            );
            let prompts = f.prompts(Role::Merge).await;
            assert!(
                prompts
                    .iter()
                    .any(|p| p.contains("rejected") && p.contains("full SHA")),
                "{missing}: {prompts:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_success_that_stays_without_a_commit_pauses_with_the_limit_and_keeps_the_lock() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[r#"{"MergeSuccessful":"merged"}"#; 6], &[]).await;
        f.repository.set_remote_head(feature(), commit("c1"));

        let state = f.drive_through().await.unwrap();

        assert!(
            matches!(
                state,
                ImplementationState::Paused {
                    reason: PauseReason::LimitExhausted,
                    ..
                }
            ),
            "{state:?}"
        );
        assert_eq!(holder(&f), Some("t31".into()));
        assert_eq!(f.pipeline.expected_head().await.unwrap(), commit("c0"));
    }

    #[tokio::test]
    async fn the_unexpected_change_pause_survives_a_restart() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[MERGED], &[]).await;
        f.repository.set_remote_head(feature(), commit("c9"));

        f.drive_through().await.unwrap_err();

        let restarted = Policy::load_or_new(f.store.as_ref(), &f.pipeline.run, &Limits::default())
            .await
            .unwrap();
        assert_eq!(
            restarted.check_start(),
            Err(PauseReason::UnexpectedRemoteChange)
        );
    }

    #[tokio::test]
    async fn conflict_review_loops_count_against_the_merge_limit_and_keep_the_lock() {
        let f = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&f, &[CONFLICTS; 3], &[CHANGES; 3]).await;

        assert_eq!(
            f.drive_through().await.unwrap(),
            ImplementationState::Paused {
                reason: PauseReason::LimitExhausted,
                resume_at: Box::new(ImplementationState::Merging { attempt: 4 }),
            }
        );

        assert_eq!(holder(&f), Some("t31".into()));
        assert_eq!(
            assignments(&f.prompts(Role::Merge).await, "merger").len(),
            3
        );
    }

    #[tokio::test]
    async fn a_replayed_verification_does_not_move_the_expected_head_back() {
        let a = fixture(WorkItem::Ticket(ticket()), 5);
        ready_to_merge(&a, &[MERGED], &[]).await;
        let b = fixture_for(WorkItem::Ticket(ticket()), 5, "t32", Some(&a));
        ready_to_merge(&b, &[r#"{"MergeSuccessful":"c2 pushed"}"#], &[]).await;
        a.repository.set_remote_head(feature(), commit("c1"));
        a.pipeline
            .step(ImplementationState::WaitingForMerge)
            .await
            .unwrap();
        let queued = tokio::time::timeout(
            Duration::from_millis(20),
            b.pipeline.step(ImplementationState::WaitingForMerge),
        )
        .await;
        assert!(queued.is_err());
        // A verifies c1 and releases the lock, but the CleaningUp state is not saved.
        let merging = ImplementationState::Merging { attempt: 1 };
        let verifying = a.pipeline.step(merging.clone()).await.unwrap();
        assert_eq!(verifying, ImplementationState::Verifying { attempt: 1 });
        a.pipeline.step(verifying.clone()).await.unwrap();
        assert_eq!(holder(&a), Some("t32".into()));

        // B merges c2 while A is down.
        save_state(&b, merging).await;
        b.repository.set_remote_head(feature(), commit("c2"));
        b.drive_through().await.unwrap();
        assert_eq!(b.pipeline.expected_head().await.unwrap(), commit("c2"));

        // A restarts from Verifying.
        save_state(&a, verifying).await;
        assert_eq!(
            a.drive_through().await.unwrap(),
            ImplementationState::Done(MergedOk {
                commit: commit("c1")
            })
        );
        assert_eq!(a.pipeline.expected_head().await.unwrap(), commit("c2"));
    }

    #[test]
    fn state_serde_round_trip() {
        let state = ImplementationState::Paused {
            reason: PauseReason::LimitExhausted,
            resume_at: Box::new(ImplementationState::Reviewing { cycle: 2 }),
        };
        let value = serde_json::to_value(&state).unwrap();
        assert_eq!(
            serde_json::from_value::<ImplementationState>(value).unwrap(),
            state
        );
    }
}
