use std::sync::Arc;
use std::time::Duration;

use chimera_core::error::PortError;
use chimera_core::run_store::RunStore;
use chimera_core::terminal::{Terminal, TurnStatus};
use chimera_core::{
    AgentId, AgentProfile, IssueRef, Outcome, PaneId, Role, RunId, TurnOutcome, TurnResult,
};
use thiserror::Error;

use crate::error::{PauseReason, PipelineError};
use crate::policy::Policy;

/// Why a turn did not produce a result.
#[derive(Debug, Error)]
pub enum TurnError {
    /// The run is paused, so no turn or handoff was started.
    #[error("run is paused: {0:?}")]
    Paused(PauseReason),
    /// The agent is gone; the caller decides whether to recover it.
    #[error("agent is gone")]
    AgentLost,
    /// The agent kept producing an invalid outcome after every permitted correction.
    #[error("outcome still invalid after {corrections} corrections: {problem}")]
    CorrectionsExhausted { corrections: u32, problem: String },
    #[error(transparent)]
    Port(#[from] PortError),
    /// A handoff's receiver turn failed and the sender's reset had failed too. The sender still
    /// holds its old context; `sender_reset` keeps its Failed/Uncertain classification.
    #[error("{receiver}; sender reset also failed: {sender_reset}")]
    ReceiverFailedAfterResetFailure {
        receiver: Box<TurnError>,
        sender_reset: PortError,
    },
}

impl From<TurnError> for PipelineError {
    fn from(error: TurnError) -> Self {
        match error {
            TurnError::Port(error) => Self::Port(error),
            other if other.is_uncertain() => Self::Port(PortError::uncertain(other.to_string())),
            other => Self::Port(PortError::failed(other.to_string())),
        }
    }
}

impl TurnError {
    /// Some effect of the turn may have happened and must be reconciled before retrying.
    fn is_uncertain(&self) -> bool {
        match self {
            Self::Port(error) => error.is_uncertain(),
            Self::ReceiverFailedAfterResetFailure {
                receiver,
                sender_reset,
            } => sender_reset.is_uncertain() || receiver.is_uncertain(),
            _ => false,
        }
    }
}

/// What one agent is asked to do in a turn.
pub struct TurnRequest<'a> {
    pub run: &'a RunId,
    pub agent: &'a AgentId,
    pub pane: &'a PaneId,
    pub role: Role,
    pub profile: &'a AgentProfile,
    pub issue: &'a IssueRef,
    /// The latest relevant description from the previous agent, if there was one.
    pub previous_description: Option<&'a str>,
}

/// A valid result and the number of corrections it took to get it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTurn {
    pub result: TurnResult,
    pub corrections: u32,
    /// Set when a handoff's receiver turn completed but the sender's reset failed. The sender
    /// still holds its old context; the caller must reset it (or reconcile, if the error is
    /// uncertain) without re-running the receiver.
    pub sender_reset_failed: Option<PortError>,
}

/// What an agent reported at the end of a turn: its output and whether that is a valid outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collected {
    pub output: String,
    pub parsed: Result<Outcome, String>,
}

/// Runs agent turns: prompt in, validated [`TurnResult`] out.
pub struct AgentTurns {
    terminal: Arc<dyn Terminal>,
    store: Arc<dyn RunStore>,
    policy: Arc<Policy>,
    poll_interval: Duration,
}

impl AgentTurns {
    pub fn new(
        terminal: Arc<dyn Terminal>,
        store: Arc<dyn RunStore>,
        policy: Arc<Policy>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            terminal,
            store,
            policy,
            poll_interval,
        }
    }

    /// Sends the prompt and waits for a valid outcome, asking the same agent to correct an
    /// invalid one at most `max_corrections` times. The policy is checked only before the turn
    /// starts: once started, a turn (corrections included) finishes and is saved even if the
    /// run is paused meanwhile.
    pub async fn run_turn(
        &self,
        request: &TurnRequest<'_>,
        max_corrections: u32,
    ) -> Result<CompletedTurn, TurnError> {
        self.send_assignment(request).await?;
        self.finish_turn(request, max_corrections).await
    }

    /// Hands work over to the receiver. The sender's context is cleared with its profile's reset
    /// command only once the receiver's prompt was sent; if that fails the sender keeps its
    /// context. Then waits for the receiver's turn as [`Self::run_turn`] does.
    pub async fn handoff(
        &self,
        sender_pane: &PaneId,
        sender_profile: &AgentProfile,
        receiver: &TurnRequest<'_>,
        max_corrections: u32,
    ) -> Result<CompletedTurn, TurnError> {
        self.check_policy()?;
        self.terminal
            .send_prompt(receiver.pane, &prompt(receiver))
            .await?;
        // The receiver has started, so its turn is finished and saved even if the reset fails;
        // abandoning it would make a retry duplicate its work.
        let reset = self
            .terminal
            .send_prompt(sender_pane, &sender_profile.reset_command)
            .await;
        let finished = self.finish_turn(receiver, max_corrections).await;
        match (finished, reset) {
            (Ok(mut completed), reset) => {
                completed.sender_reset_failed = reset.err();
                Ok(completed)
            }
            (Err(error), Ok(())) => Err(error),
            (Err(receiver), Err(sender_reset)) => Err(TurnError::ReceiverFailedAfterResetFailure {
                receiver: Box::new(receiver),
                sender_reset,
            }),
        }
    }

    /// Clears the context of the agent in `pane` with its profile's reset command.
    pub async fn reset(&self, pane: &PaneId, profile: &AgentProfile) -> Result<(), PortError> {
        self.terminal
            .send_prompt(pane, &profile.reset_command)
            .await
    }

    /// The output currently shown by `pane`.
    pub async fn read_output(&self, pane: &PaneId) -> Result<String, PortError> {
        self.terminal.read_output(pane).await
    }

    /// Sends the prompt of a turn without waiting for its result, unless the run is paused.
    pub async fn send_assignment(&self, request: &TurnRequest<'_>) -> Result<(), TurnError> {
        self.check_policy()?;
        self.terminal
            .send_prompt(request.pane, &prompt(request))
            .await?;
        Ok(())
    }

    /// Whether the agent in `pane` shows evidence of having received a prompt sent while it
    /// showed `output_before`: it is working, or its output changed. An idle agent still showing
    /// `output_before` proves nothing either way, as a delivered turn can end with identical
    /// output, so the caller must not treat that as "never arrived".
    pub async fn delivered(&self, pane: &PaneId, output_before: &str) -> Result<bool, TurnError> {
        Ok(match self.terminal.read_status(pane).await? {
            TurnStatus::Gone => return Err(TurnError::AgentLost),
            TurnStatus::Running => true,
            TurnStatus::Finished => self.terminal.read_output(pane).await? != output_before,
        })
    }

    /// Waits for the agent's turn to end and parses what it reported, without saving it.
    pub async fn collect(&self, request: &TurnRequest<'_>) -> Result<Collected, TurnError> {
        self.wait_until_finished(request.pane).await?;
        let output = self.terminal.read_output(request.pane).await?;
        let parsed = parse_outcome(&output, request.role);
        Ok(Collected { output, parsed })
    }

    /// Saves what [`Self::collect`] found to the history.
    pub async fn record(
        &self,
        request: &TurnRequest<'_>,
        collected: &Collected,
    ) -> Result<TurnResult, PortError> {
        let outcome = match &collected.parsed {
            Ok(outcome) => TurnOutcome::Valid(outcome.clone()),
            Err(problem) => TurnOutcome::Invalid {
                output: collected.output.clone(),
                problem: problem.clone(),
            },
        };
        let result = TurnResult {
            agent: request.agent.clone(),
            role: request.role,
            outcome,
        };
        self.store.append_turn(request.run, result.clone()).await?;
        Ok(result)
    }

    /// Asks the agent to correct the invalid outcome described by `problem`.
    pub async fn send_correction(
        &self,
        request: &TurnRequest<'_>,
        problem: &str,
    ) -> Result<(), PortError> {
        let correction = correction_prompt(request.role, problem);
        self.terminal.send_prompt(request.pane, &correction).await
    }

    fn check_policy(&self) -> Result<(), TurnError> {
        self.policy.check_start().map_err(TurnError::Paused)
    }

    async fn finish_turn(
        &self,
        request: &TurnRequest<'_>,
        max_corrections: u32,
    ) -> Result<CompletedTurn, TurnError> {
        let mut corrections = 0;
        loop {
            let collected = self.collect(request).await?;
            let result = self.record(request, &collected).await?;
            match collected.parsed {
                Ok(_) => {
                    return Ok(CompletedTurn {
                        result,
                        corrections,
                        sender_reset_failed: None,
                    });
                }
                Err(problem) if corrections >= max_corrections => {
                    return Err(TurnError::CorrectionsExhausted {
                        corrections,
                        problem,
                    });
                }
                Err(problem) => {
                    corrections += 1;
                    self.send_correction(request, &problem).await?;
                }
            }
        }
    }

    async fn wait_until_finished(&self, pane: &PaneId) -> Result<(), TurnError> {
        loop {
            match self.terminal.read_status(pane).await? {
                TurnStatus::Finished => return Ok(()),
                TurnStatus::Gone => return Err(TurnError::AgentLost),
                TurnStatus::Running => tokio::time::sleep(self.poll_interval).await,
            }
        }
    }
}

/// Only the role's instructions, the issue reference, and the previous agent's description.
fn prompt(request: &TurnRequest<'_>) -> String {
    let mut prompt = format!(
        "{}\n\nIssue: {}",
        request.profile.prompt_template, request.issue
    );
    if let Some(description) = request.previous_description {
        prompt.push_str(&format!("\n\nPrevious agent:\n{description}"));
    }
    prompt
}

fn correction_prompt(role: Role, problem: &str) -> String {
    format!(
        "Your last result was rejected: {problem}. Report exactly one outcome as a single line \
         of JSON, e.g. {{\"{}\":\"explanation\"}}. Valid outcomes: {}.",
        valid_names(role)[0],
        valid_names(role).join(", ")
    )
}

fn valid_names(role: Role) -> &'static [&'static str] {
    match role {
        Role::Implementation => &["ImplementationReady"],
        Role::Review => &["ReviewApproved", "ChangesRequested"],
        Role::Merge => &[
            "MergeReadyForConflictReview",
            "MergeSuccessful",
            "MergeBlocked",
        ],
    }
}

fn is_valid_for(role: Role, outcome: &Outcome) -> bool {
    match role {
        Role::Implementation => matches!(outcome, Outcome::ImplementationReady(_)),
        Role::Review => matches!(
            outcome,
            Outcome::ReviewApproved(_) | Outcome::ChangesRequested(_)
        ),
        Role::Merge => matches!(
            outcome,
            Outcome::MergeReadyForConflictReview(_)
                | Outcome::MergeSuccessful(_)
                | Outcome::MergeBlocked(_)
        ),
    }
}

/// The outcome is the last line of the output that is a JSON object, e.g.
/// `{"ReviewApproved":"looks good"}`. On failure, describes what is wrong.
fn parse_outcome(output: &str, role: Role) -> Result<Outcome, String> {
    let line = output
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| line.starts_with('{'))
        .ok_or("no outcome found")?;
    let outcome: Outcome =
        serde_json::from_str(line).map_err(|error| format!("malformed outcome: {error}"))?;
    if is_valid_for(role, &outcome) {
        Ok(outcome)
    } else {
        Err(format!("outcome is not valid for the {role:?} role"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::path::Path;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::terminal::FakeTerminal;
    use chimera_core::{Limits, WorkspaceId};
    use tokio::time::Instant;

    use super::*;

    /// A [`FakeTerminal`] that answers each prompt with the next scripted output, logs every
    /// prompt in order, and can refuse prompts to chosen panes (with a chosen error) or pause the run on the first one.
    struct ScriptedTerminal {
        inner: FakeTerminal,
        replies: Mutex<VecDeque<String>>,
        log: Mutex<Vec<(PaneId, String)>>,
        refuse: Mutex<HashSet<PaneId>>,
        refuse_with: Mutex<PortError>,
        pause_on_first_prompt: Mutex<Option<Arc<Policy>>>,
    }

    impl ScriptedTerminal {
        fn reply_with(&self, replies: &[&str]) {
            self.replies
                .lock()
                .unwrap()
                .extend(replies.iter().map(|r| r.to_string()));
        }

        fn log(&self) -> Vec<(PaneId, String)> {
            self.log.lock().unwrap().clone()
        }
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
            self.inner.launch_agent(pane, command_line).await
        }

        async fn send_prompt(&self, pane: &PaneId, prompt: &str) -> Result<(), PortError> {
            if self.refuse.lock().unwrap().contains(pane) {
                return Err(self.refuse_with.lock().unwrap().clone());
            }
            self.log
                .lock()
                .unwrap()
                .push((pane.clone(), prompt.to_string()));
            if let Some(reply) = self.replies.lock().unwrap().pop_front() {
                self.inner.script_output(pane, reply);
            }
            if let Some(policy) = self.pause_on_first_prompt.lock().unwrap().take() {
                policy.pause(PauseReason::GlobalPause);
            }
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

    struct Fixture {
        terminal: Arc<ScriptedTerminal>,
        store: Arc<FakeRunStore>,
        policy: Arc<Policy>,
        turns: AgentTurns,
        run: RunId,
        issue: IssueRef,
        panes: [PaneId; 2],
        profile: AgentProfile,
    }

    const INTERVAL: Duration = Duration::from_secs(2);

    async fn fixture() -> Fixture {
        let terminal = Arc::new(ScriptedTerminal {
            inner: FakeTerminal::new(),
            replies: Mutex::default(),
            log: Mutex::default(),
            refuse: Mutex::default(),
            refuse_with: Mutex::new(PortError::failed("refused")),
            pause_on_first_prompt: Mutex::default(),
        });
        let (workspace, first) = terminal.create_workspace(Path::new("/w")).await.unwrap();
        let second = terminal.split_pane(&workspace, &first).await.unwrap();
        let store = Arc::new(FakeRunStore::new());
        let policy = Arc::new(Policy::new(&Limits::default()));
        let turns = AgentTurns::new(terminal.clone(), store.clone(), policy.clone(), INTERVAL);
        Fixture {
            terminal,
            store,
            policy,
            turns,
            run: RunId::new("run").unwrap(),
            issue: IssueRef::new("o", "r", 28).unwrap(),
            panes: [first, second],
            profile: AgentProfile::new("p", "m", "Do the work."),
        }
    }

    impl Fixture {
        fn all_finished(&self) {
            for pane in &self.panes {
                self.terminal
                    .inner
                    .script_statuses(pane, [TurnStatus::Finished]);
            }
        }

        fn request<'a>(
            &'a self,
            agent: &'a AgentId,
            index: usize,
            role: Role,
            previous: Option<&'a str>,
        ) -> TurnRequest<'a> {
            TurnRequest {
                run: &self.run,
                agent,
                pane: &self.panes[index],
                role,
                profile: &self.profile,
                issue: &self.issue,
                previous_description: previous,
            }
        }
    }

    fn agent(name: &str) -> AgentId {
        AgentId::new(name).unwrap()
    }

    const APPROVED: &str = "thinking...\n{\"ReviewApproved\":\"good\"}\n";

    #[tokio::test(start_paused = true)]
    async fn valid_outcome_after_polling() {
        let f = fixture().await;
        f.terminal.reply_with(&[APPROVED]);
        f.terminal.inner.script_statuses(
            &f.panes[0],
            [
                TurnStatus::Running,
                TurnStatus::Running,
                TurnStatus::Finished,
            ],
        );
        let id = agent("a1");
        let started = Instant::now();

        let completed = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Review, Some("prior notes")), 3)
            .await
            .unwrap();

        assert_eq!(started.elapsed(), INTERVAL * 2);
        assert_eq!(completed.corrections, 0);
        assert_eq!(
            completed.result,
            TurnResult {
                agent: id,
                role: Role::Review,
                outcome: TurnOutcome::Valid(Outcome::ReviewApproved("good".into())),
            }
        );
        assert_eq!(
            f.terminal.log(),
            [(
                f.panes[0].clone(),
                "Do the work.\n\nIssue: o/r#28\n\nPrevious agent:\nprior notes".to_string()
            )]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_outcomes_are_corrected_on_the_same_agent() {
        let f = fixture().await;
        f.all_finished();
        f.terminal
            .reply_with(&["no outcome here", "{\"ReviewApproved\":", APPROVED]);
        let id = agent("a1");

        let completed = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Review, None), 2)
            .await
            .unwrap();

        assert_eq!(completed.corrections, 2);
        let log = f.terminal.log();
        assert_eq!(log.len(), 3);
        assert!(log.iter().all(|(pane, _)| *pane == f.panes[0]));
        assert!(log[1].1.contains("no outcome found"));
        assert!(log[2].1.contains("malformed outcome"));
        assert!(log[1].1.contains("ReviewApproved, ChangesRequested"));
        let history = f.store.history(&f.run);
        assert_eq!(history.len(), 3);
        assert_eq!(
            history[0].outcome,
            TurnOutcome::Invalid {
                output: "no outcome here".into(),
                problem: "no outcome found".into(),
            }
        );
        assert!(matches!(
            &history[1].outcome,
            TurnOutcome::Invalid { output, problem }
                if output == "{\"ReviewApproved\":" && problem.starts_with("malformed outcome")
        ));
        assert_eq!(history[2], completed.result);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_corrections_are_reported_distinctly() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&["nope", "nope", "nope"]);
        let id = agent("a1");

        let error = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Review, None), 2)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TurnError::CorrectionsExhausted { corrections: 2, .. }
        ));
        assert_eq!(f.terminal.log().len(), 3);
        let history = f.store.history(&f.run);
        assert_eq!(history.len(), 3);
        assert!(history.iter().all(
            |turn| matches!(&turn.outcome, TurnOutcome::Invalid { output, .. } if output == "nope")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn outcome_invalid_for_the_role_is_corrected() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&[
            "{\"MergeSuccessful\":\"done\"}",
            "{\"ImplementationReady\":\"ok\"}",
        ]);
        let id = agent("a1");

        let completed = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Implementation, None), 1)
            .await
            .unwrap();

        assert_eq!(completed.corrections, 1);
        assert!(
            f.terminal.log()[1]
                .1
                .contains("not valid for the Implementation role")
        );
        assert!(matches!(
            &f.store.history(&f.run)[0].outcome,
            TurnOutcome::Invalid { output, .. } if output == "{\"MergeSuccessful\":\"done\"}"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn gone_agent_is_an_agent_lost_error() {
        let f = fixture().await;
        f.terminal
            .inner
            .script_statuses(&f.panes[0], [TurnStatus::Running, TurnStatus::Gone]);
        let id = agent("a1");

        let error = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Review, None), 3)
            .await
            .unwrap_err();

        assert!(matches!(error, TurnError::AgentLost));
        assert!(f.store.history(&f.run).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn history_keeps_turn_order() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&[
            "{\"ImplementationReady\":\"one\"}",
            "{\"ChangesRequested\":\"two\"}",
        ]);
        let (implementer, reviewer) = (agent("impl"), agent("rev"));

        f.turns
            .run_turn(&f.request(&implementer, 0, Role::Implementation, None), 0)
            .await
            .unwrap();
        f.turns
            .run_turn(&f.request(&reviewer, 1, Role::Review, Some("one")), 0)
            .await
            .unwrap();

        let outcomes: Vec<_> = f
            .store
            .history(&f.run)
            .into_iter()
            .map(|turn| (turn.agent, turn.outcome))
            .collect();
        assert_eq!(
            outcomes,
            [
                (
                    implementer,
                    TurnOutcome::Valid(Outcome::ImplementationReady("one".into()))
                ),
                (
                    reviewer,
                    TurnOutcome::Valid(Outcome::ChangesRequested("two".into()))
                ),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn handoff_resets_sender_after_receiver_prompt() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&["{\"ChangesRequested\":\"fix\"}"]);
        let id = agent("rev");

        f.turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, Some("ready")),
                0,
            )
            .await
            .unwrap();

        let log = f.terminal.log();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].0, f.panes[1]);
        assert_eq!(log[1], (f.panes[0].clone(), "/clear".to_string()));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_handoff_does_not_reset_sender() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.refuse.lock().unwrap().insert(f.panes[1].clone());
        let id = agent("rev");

        let error = f
            .turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, None),
                0,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TurnError::Port(_)));
        assert!(f.terminal.log().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_sender_reset_still_finishes_and_saves_the_receiver_turn() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&["{\"ChangesRequested\":\"fix\"}"]);
        f.terminal.refuse.lock().unwrap().insert(f.panes[0].clone());
        let id = agent("rev");

        let completed = f
            .turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, None),
                0,
            )
            .await
            .unwrap();

        assert_eq!(
            completed.sender_reset_failed,
            Some(PortError::failed("refused"))
        );
        assert_eq!(f.store.history(&f.run), [completed.result]);
        assert_eq!(f.terminal.log().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn uncertain_sender_reset_keeps_its_classification() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&["{\"ChangesRequested\":\"fix\"}"]);
        f.terminal.refuse.lock().unwrap().insert(f.panes[0].clone());
        *f.terminal.refuse_with.lock().unwrap() = PortError::uncertain("lost");
        let id = agent("rev");

        let completed = f
            .turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, None),
                0,
            )
            .await
            .unwrap();

        assert_eq!(
            completed.sender_reset_failed,
            Some(PortError::uncertain("lost"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sender_reset_failure_is_kept_when_the_receiver_exhausts_corrections() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&["nope"]);
        f.terminal.refuse.lock().unwrap().insert(f.panes[0].clone());
        *f.terminal.refuse_with.lock().unwrap() = PortError::uncertain("lost");
        let id = agent("rev");

        let error = f
            .turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, None),
                0,
            )
            .await
            .unwrap_err();

        let TurnError::ReceiverFailedAfterResetFailure {
            receiver,
            sender_reset,
        } = error
        else {
            panic!("expected the reset failure to be kept, got {error:?}");
        };
        assert!(matches!(
            *receiver,
            TurnError::CorrectionsExhausted { corrections: 0, .. }
        ));
        assert_eq!(sender_reset, PortError::uncertain("lost"));
        assert_eq!(f.store.history(&f.run).len(), 1);
    }

    fn combined(receiver: PortError, sender_reset: PortError) -> TurnError {
        TurnError::ReceiverFailedAfterResetFailure {
            receiver: Box::new(TurnError::Port(receiver)),
            sender_reset,
        }
    }

    #[test]
    fn uncertain_reset_with_exhausted_receiver_converts_to_uncertain() {
        let error = TurnError::ReceiverFailedAfterResetFailure {
            receiver: Box::new(TurnError::CorrectionsExhausted {
                corrections: 0,
                problem: "bad".into(),
            }),
            sender_reset: PortError::uncertain("lost"),
        };
        let error = PipelineError::from(error);
        assert!(error.is_uncertain() && !error.is_failed());
    }

    #[test]
    fn uncertain_receiver_with_failed_reset_converts_to_uncertain() {
        let error = PipelineError::from(combined(
            PortError::uncertain("lost"),
            PortError::failed("refused"),
        ));
        assert!(error.is_uncertain() && !error.is_failed());
    }

    #[test]
    fn failed_receiver_and_failed_reset_convert_to_failed() {
        let error = PipelineError::from(combined(PortError::failed("a"), PortError::failed("b")));
        assert!(error.is_failed() && !error.is_uncertain());
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_failure_without_reset_failure_is_unchanged() {
        let f = fixture().await;
        f.terminal
            .inner
            .script_statuses(&f.panes[1], [TurnStatus::Gone]);
        let id = agent("rev");

        let error = f
            .turns
            .handoff(
                &f.panes[0],
                &f.profile,
                &f.request(&id, 1, Role::Review, None),
                0,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TurnError::AgentLost));
    }

    #[tokio::test(start_paused = true)]
    async fn pause_lets_the_active_turn_finish_but_blocks_new_ones() {
        let f = fixture().await;
        f.all_finished();
        f.terminal.reply_with(&[APPROVED, APPROVED]);
        *f.terminal.pause_on_first_prompt.lock().unwrap() = Some(f.policy.clone());
        let id = agent("a1");

        let completed = f
            .turns
            .run_turn(&f.request(&id, 0, Role::Review, None), 0)
            .await
            .unwrap();
        assert_eq!(f.store.history(&f.run), [completed.result]);

        let request = f.request(&id, 0, Role::Review, None);
        let error = f.turns.run_turn(&request, 0).await.unwrap_err();
        assert!(matches!(error, TurnError::Paused(PauseReason::GlobalPause)));
        let error = f
            .turns
            .handoff(&f.panes[1], &f.profile, &request, 0)
            .await
            .unwrap_err();
        assert!(matches!(error, TurnError::Paused(PauseReason::GlobalPause)));
        assert_eq!(f.terminal.log().len(), 1);
    }
}
