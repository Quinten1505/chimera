use std::sync::Arc;

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

    /// Runs the turn of `role` in `cycle` unless its result was already saved, then moves on.
    async fn turn_step(
        &self,
        state: ImplementationState,
        role: Role,
        cycle: u32,
    ) -> Result<ImplementationState, PipelineError> {
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
                let corrections_used = history
                    .iter()
                    .filter(|turn| matches!(turn.outcome, TurnOutcome::Invalid { .. }))
                    .count();
                let used = cycle as usize + corrections_used;
                if used > self.cycle_limit as usize {
                    return Ok(paused(PauseReason::LimitExhausted, state));
                }
                let other = if role == Role::Review {
                    Role::Implementation
                } else {
                    Role::Review
                };
                let previous = valid(other).next_back().map(outcome_text);
                let findings = match &self.work_item {
                    WorkItem::Findings(findings) if cycle == 1 && role == Role::Implementation => {
                        Some(findings.as_str())
                    }
                    _ => None,
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
                    previous_description: previous.as_deref().or(findings),
                };
                let max_corrections = (self.cycle_limit as usize - used) as u32;
                let sender = environment.pane(other).filter(|_| previous.is_some());
                let result = match sender {
                    Some(sender) => {
                        self.turns
                            .handoff(
                                sender,
                                self.configuration.profile(other),
                                &request,
                                max_corrections,
                            )
                            .await
                    }
                    None => self.turns.run_turn(&request, max_corrections).await,
                };
                match result {
                    // A reset that failed leaves the sender with stale context; the turn's
                    // result is saved and decides the next state either way.
                    Ok(completed) => match completed.result.outcome {
                        TurnOutcome::Valid(outcome) => outcome,
                        TurnOutcome::Invalid { .. } => unreachable!("a completed turn is valid"),
                    },
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
                        return Ok(state);
                    }
                    Err(TurnError::CorrectionsExhausted { .. }) => {
                        return Ok(paused(PauseReason::LimitExhausted, state));
                    }
                    Err(TurnError::Paused(reason)) => return Err(PipelineError::Paused(reason)),
                    Err(error) => return Err(error.into()),
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
            self.inner.send_prompt(pane, prompt).await
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
