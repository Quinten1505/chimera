use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::run_store::RunStore;
use chimera_core::{
    AgentConfiguration, AgentId, CommitId, IssueRef, MergedOk, Outcome, Role, RunId, TurnOutcome,
    TurnResult, WorkItem,
};
use serde::{Deserialize, Serialize};

use crate::agent_turn::{AgentTurns, TurnError, TurnRequest};
use crate::driver::{Pipeline, PipelineState};
use crate::environment::{Environment, EnvironmentService, ProvisionSpec};
use crate::error::{PauseReason, PipelineError};

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
    /// The steps from `WaitingForMerge` on are not implemented yet, so the driver stops there.
    fn is_terminal(&self) -> bool {
        matches!(self, Self::WaitingForMerge | Self::Done(_))
    }

    fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }
}

/// `(WorkItem, AgentConfiguration)` up to `WaitingForMerge`: provisioning and the
/// implementation/review loop.
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
    pub spec: ProvisionSpec,
    pub store: Arc<dyn RunStore>,
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
        let (implementation, review) = (self.agent(Role::Implementation), self.agent(Role::Review));
        Ok(self
            .store
            .load_history(&self.run)
            .await?
            .into_iter()
            .filter(|turn| turn.agent == implementation || turn.agent == review)
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
                let used = cycle as usize + invalid_before;
                if used > self.cycle_limit as usize {
                    return Ok(paused(PauseReason::LimitExhausted, state));
                }
                let other = if role == Role::Review {
                    Role::Implementation
                } else {
                    Role::Review
                };
                let previous = valid(other).next_back().map(outcome_text);
                let has_previous = previous.is_some();
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
                let max_corrections = (self.cycle_limit as usize - used) as u32;
                let fresh = PendingTurn {
                    role,
                    cycle,
                    output_before: self.turns.read_output(pane).await?,
                    invalid_before,
                    corrections: 0,
                    phase: TurnPhase::Sending,
                };
                let mut reset_failed = None;
                let result = self
                    .execute_turn(
                        &mut pending,
                        &request,
                        fresh,
                        has_previous.then_some(other),
                        max_corrections,
                        &mut reset_failed,
                    )
                    .await?;
                match result {
                    Ok(outcome) => {
                        // A reset that failed stays owed: it is retried before the state moves
                        // on, and the saved result of the turn is kept.
                        pending.turn = None;
                        self.save_pending(&pending).await?;
                        if let Some(error) = reset_failed {
                            return Err(error);
                        }
                        outcome
                    }
                    Err(error) => match error {
                        TurnError::AgentLost => {
                            let command_line = &self
                                .spec
                                .agents
                                .iter()
                                .find(|launch| launch.role == role)
                                .ok_or_else(|| {
                                    PipelineError::Environment(format!(
                                        "no {role:?} launch in spec"
                                    ))
                                })?
                                .command_line;
                            self.environment
                                .relaunch(&environment, role, command_line)
                                .await?;
                            pending.turn = None;
                            self.save_pending(&pending).await?;
                            return Ok(state);
                        }
                        TurnError::CorrectionsExhausted { .. } => {
                            pending.turn = None;
                            self.save_pending(&pending).await?;
                            return Ok(paused(PauseReason::LimitExhausted, state));
                        }
                        TurnError::Paused(reason) => {
                            // Nothing was sent.
                            self.save_pending(&Pending::default()).await?;
                            return Err(PipelineError::Paused(reason));
                        }
                        other => return Err(other.into()),
                    },
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
}

/// What a step that was interrupted may have left half done, saved before the effect runs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Pending {
    /// A turn that has started.
    turn: Option<PendingTurn>,
    /// The sender of a handoff whose context still has to be cleared. Set only once the
    /// receiver is known to have its prompt.
    reset: Option<Role>,
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
            other => Ok(other),
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
        let branch = |name: &str| BranchName::new(name).unwrap();
        let repository = Arc::new(FakeRepository::new(
            branch("main"),
            CommitId::new("c0").unwrap(),
        ));
        repository.add_branch(branch("feat"), CommitId::new("c0").unwrap());
        let terminal = Arc::new(ScriptedTerminal {
            inner: FakeTerminal::new(),
            replies: Mutex::default(),
            launches: Mutex::new(0),
            gone: Mutex::default(),
            fail_reset: Mutex::default(),
            fail_after_send: Mutex::default(),
            fail_send: Mutex::default(),
            fail_correction_after_send: Mutex::default(),
        });
        let store = Arc::new(FakeRunStore::new());
        let policy = Arc::new(Policy::new(&Limits::default()));
        let environment = Arc::new(EnvironmentService::new(
            repository,
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
            instance: "t31".into(),
            work_item,
            issue: issue(),
            configuration: AgentConfiguration {
                implementation: profile("implementer"),
                review: profile("reviewer"),
                merge: profile("merger"),
            },
            cycle_limit,
            spec: ProvisionSpec {
                worktree: "/wt".into(),
                task_branch: branch("task"),
                feature: branch("feat"),
                agents: [Role::Implementation, Role::Review, Role::Merge]
                    .map(|role| AgentLaunch {
                        role,
                        command_line: format!("agent {role:?}"),
                    })
                    .to_vec(),
            },
            store: store.clone(),
            environment,
            turns,
        };
        Fixture {
            terminal,
            store,
            policy,
            pipeline,
        }
    }

    impl Fixture {
        async fn drive(&self) -> Result<ImplementationState, PipelineError> {
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
            self.terminal.inner.prompts(&self.pane(role).await)
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
                corrections,
                phase,
            }),
            reset: None,
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
