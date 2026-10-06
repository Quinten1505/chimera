use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::run_store::RunStore;
use chimera_core::{Blocker, IssueRef, IssueStatus, MergedOk, RunId, Ticket, TicketPlan};
use futures_util::lock::Mutex as AsyncMutex;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::driver::{Pipeline, PipelineState, drive};
use crate::error::{PauseReason, PipelineError};
use crate::implementation::ImplementationState;
use crate::policy::{Budget, Policy, RetryRefused};

/// Name of the saved ticket state in the run store.
const STORE_INSTANCE: &str = "tickets";

/// Where one ticket stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TicketProgress {
    /// Open and not started; it starts once its blockers are closed.
    Waiting,
    /// Its Implementation task was started and has not ended.
    Running,
    /// Merged and verified; its issue is being closed. Close attempt `attempt` is recorded as
    /// the effect `close/<issue>/<attempt>`, so a restart knows whether it ran and how it ended.
    Closing {
        merged: MergedOk,
        attempt: u32,
    },
    Closed,
    /// Its Implementation task paused and stays paused.
    Paused(PauseReason),
}

/// A ticket of the plan with its progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketEntry {
    pub ticket: Ticket,
    pub progress: TicketProgress,
}

/// Why a ticket is not making progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketPause {
    /// An open blocker outside the plan keeps the ticket from starting.
    Blocker(IssueRef),
    /// Its Implementation task paused.
    Reason(PauseReason),
}

/// The ticket plan, fixed when it is read, and the progress of every ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketBoard {
    pub entries: Vec<TicketEntry>,
}

impl TicketBoard {
    fn from_plan(plan: TicketPlan) -> Self {
        let entries = plan
            .tickets
            .into_iter()
            .map(|ticket| TicketEntry {
                progress: match ticket.status {
                    IssueStatus::Closed => TicketProgress::Closed,
                    IssueStatus::Open => TicketProgress::Waiting,
                },
                ticket,
            })
            .collect();
        Self { entries }
    }

    /// Every ticket is closed.
    pub fn is_complete(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| entry.progress == TicketProgress::Closed)
    }

    /// The tickets that cannot continue without outside action, with the reason.
    pub fn paused(&self) -> Vec<(IssueRef, TicketPause)> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let pause = match &entry.progress {
                    TicketProgress::Paused(reason) => TicketPause::Reason(*reason),
                    TicketProgress::Waiting => TicketPause::Blocker(self.open_outside(entry)?),
                    _ => return None,
                };
                Some((entry.ticket.issue.clone(), pause))
            })
            .collect()
    }

    /// The first open blocker of `entry` that is not a ticket of the plan, as read with the plan.
    fn open_outside(&self, entry: &TicketEntry) -> Option<IssueRef> {
        entry
            .ticket
            .blockers
            .iter()
            .find(|blocker| {
                self.entry(&blocker.issue).is_none() && blocker.status == IssueStatus::Open
            })
            .map(|blocker| blocker.issue.clone())
    }

    fn entry(&self, issue: &IssueRef) -> Option<&TicketEntry> {
        self.entries
            .iter()
            .find(|entry| &entry.ticket.issue == issue)
    }

    fn entry_mut(&mut self, issue: &IssueRef) -> Option<&mut TicketEntry> {
        self.entries
            .iter_mut()
            .find(|entry| &entry.ticket.issue == issue)
    }

    fn is_unblocked(&self, ticket: &Ticket) -> bool {
        ticket
            .blockers
            .iter()
            .all(|Blocker { issue, status }| match self.entry(issue) {
                Some(entry) => entry.progress == TicketProgress::Closed,
                None => *status == IssueStatus::Closed,
            })
    }

    fn set(&mut self, issue: &IssueRef, progress: TicketProgress) {
        if let Some(entry) = self.entry_mut(issue) {
            entry.progress = progress;
        }
    }
}

/// State of the ticket pipeline, saved after every step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TicketState {
    /// The plan has not been read yet.
    ReadingPlan,
    /// Tickets are started, their tasks collected and their issues closed.
    Running(TicketBoard),
    /// Nothing can run and nothing is running, but tickets are still open: they are blocked or
    /// their tasks paused. This is waiting, not completion.
    Waiting(TicketBoard),
    /// Every ticket is closed.
    Done(TicketBoard),
    Paused {
        reason: PauseReason,
        resume_at: Box<TicketState>,
    },
}

impl TicketState {
    /// The plan and its progress, once the plan was read.
    pub fn board(&self) -> Option<&TicketBoard> {
        match self {
            Self::ReadingPlan => None,
            Self::Running(board) | Self::Waiting(board) | Self::Done(board) => Some(board),
            Self::Paused { resume_at, .. } => resume_at.board(),
        }
    }

    /// Every ticket is closed.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Done(_))
    }
}

impl PipelineState for TicketState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    fn is_paused(&self) -> bool {
        matches!(self, Self::Waiting(_) | Self::Paused { .. })
    }
}

type TaskEnd = (IssueRef, Result<ImplementationState, PipelineError>);

/// What only lives as long as the process: the running tasks.
#[derive(Default)]
struct Runtime {
    tasks: JoinSet<TaskEnd>,
    in_flight: HashSet<IssueRef>,
    /// Tasks that stopped because the run is paused; this process does not start them again.
    halted: HashSet<IssueRef>,
    /// The pause such a task stopped on.
    pause: Option<PauseReason>,
}

/// Feature -> all tickets merged: runs one Implementation task per ticket as soon as the ticket
/// is open, unblocked and not running, and closes each ticket issue after its verified merge.
///
/// `start` runs the Implementation task of a ticket (with the ticket configuration and
/// `WorkItem::Ticket`) through `drive`, which continues the task's saved state, and returns the
/// state it stopped in: `Done` or `Paused`. Tasks run in a `JoinSet` with no concurrency limit;
/// the set is runtime state, and a restart starts the tasks of running tickets again from their
/// saved states.
///
/// Each step reads the plan, makes one scheduling transition, takes one step of closing an
/// issue, starts tasks, or collects one task that ended.
pub struct TicketPipeline<F> {
    run: RunId,
    specification: IssueRef,
    store: Arc<dyn RunStore>,
    forge: Arc<dyn Forge>,
    policy: Arc<Policy>,
    start: F,
    runtime: AsyncMutex<Runtime>,
}

impl<F, Fut> TicketPipeline<F>
where
    F: Fn(Ticket) -> Fut + Sync,
    Fut: Future<Output = Result<ImplementationState, PipelineError>> + Send + 'static,
{
    pub fn new(
        run: RunId,
        specification: IssueRef,
        store: Arc<dyn RunStore>,
        forge: Arc<dyn Forge>,
        policy: Arc<Policy>,
        start: F,
    ) -> Self {
        Self {
            run,
            specification,
            store,
            forge,
            policy,
            start,
            runtime: AsyncMutex::default(),
        }
    }

    /// Drives the saved ticket state until every ticket is closed, nothing can run any more
    /// (see [`TicketState::Waiting`]), or the run is paused. A saved plan is continued without
    /// reading the plan again.
    pub async fn run(&self) -> Result<TicketState, PipelineError> {
        drive(self.store.as_ref(), &self.run, STORE_INSTANCE, self).await
    }

    /// Reads the plan once; a failed read is repeated by the next step while the policy permits.
    async fn read_plan(&self) -> Result<TicketState, PipelineError> {
        match self.forge.read_plan(&self.specification).await {
            Ok(plan) => Ok(TicketState::Running(TicketBoard::from_plan(plan))),
            Err(error) => match self.permit_github_retry(error).await {
                Ok(()) => Ok(TicketState::ReadingPlan),
                Err(PipelineError::Paused(reason)) => Ok(TicketState::Paused {
                    reason,
                    resume_at: Box::new(TicketState::ReadingPlan),
                }),
                Err(error) => Err(error),
            },
        }
    }

    /// One step of the running plan, in this order: a step of closing a merged ticket's issue,
    /// marking unblocked tickets as running, starting the tasks of running tickets, collecting
    /// a task that ended. While the run is paused no issue is closed and no ticket starts, but
    /// the tasks of running tickets still continue their turns and are collected.
    async fn schedule(&self, mut board: TicketBoard) -> Result<TicketState, PipelineError> {
        let mut runtime = self.runtime.lock().await;
        let pause = self.policy.check_start().err().or(runtime.pause);
        if pause.is_none() {
            let closing = board
                .entries
                .iter()
                .find_map(|entry| match &entry.progress {
                    TicketProgress::Closing { merged, attempt } => {
                        Some((entry.ticket.issue.clone(), merged.clone(), *attempt))
                    }
                    _ => None,
                });
            if let Some((issue, merged, attempt)) = closing {
                let progress = self.close_step(&issue, merged, attempt).await?;
                board.set(&issue, progress);
                return Ok(TicketState::Running(board));
            }
            let unblocked: Vec<IssueRef> = board
                .entries
                .iter()
                .filter(|entry| {
                    entry.progress == TicketProgress::Waiting && board.is_unblocked(&entry.ticket)
                })
                .map(|entry| entry.ticket.issue.clone())
                .collect();
            if !unblocked.is_empty() {
                for issue in &unblocked {
                    board.set(issue, TicketProgress::Running);
                }
                return Ok(TicketState::Running(board));
            }
        }
        let to_start: Vec<Ticket> = board
            .entries
            .iter()
            .filter(|entry| {
                let issue = &entry.ticket.issue;
                entry.progress == TicketProgress::Running
                    && !runtime.in_flight.contains(issue)
                    && !runtime.halted.contains(issue)
            })
            .map(|entry| entry.ticket.clone())
            .collect();
        if !to_start.is_empty() {
            for ticket in to_start {
                let issue = ticket.issue.clone();
                runtime.in_flight.insert(issue.clone());
                let task = (self.start)(ticket);
                runtime.tasks.spawn(async move { (issue, task.await) });
            }
            return Ok(TicketState::Running(board));
        }
        if let Some(joined) = runtime.tasks.join_next().await {
            let (issue, result) = joined.map_err(|error| {
                PipelineError::Environment(format!("ticket task failed: {error}"))
            })?;
            runtime.in_flight.remove(&issue);
            let progress = match result {
                Err(PipelineError::Paused(reason)) => {
                    runtime.halted.insert(issue);
                    runtime.pause.get_or_insert(reason);
                    return Ok(TicketState::Running(board));
                }
                Err(error) => return Err(error),
                Ok(ImplementationState::Done(merged)) => {
                    TicketProgress::Closing { merged, attempt: 1 }
                }
                Ok(ImplementationState::Paused { reason, .. }) => TicketProgress::Paused(reason),
                Ok(other) => {
                    return Err(PipelineError::Environment(format!(
                        "ticket {issue} stopped in {other:?}"
                    )));
                }
            };
            board.set(&issue, progress);
            return Ok(TicketState::Running(board));
        }
        Ok(if board.is_complete() {
            TicketState::Done(board)
        } else if let Some(reason) = pause {
            TicketState::Paused {
                reason,
                resume_at: Box::new(TicketState::Running(board)),
            }
        } else {
            TicketState::Waiting(board)
        })
    }

    /// Takes one step of closing `issue` with close attempt `attempt` and returns the ticket's
    /// progress after it. The attempt's intent is recorded before the close and its outcome
    /// after, so a completed close is never repeated: one that may have run is reconciled
    /// against the issue's status first, and one that did not close the issue is attempted
    /// again only with the policy's permission.
    async fn close_step(
        &self,
        issue: &IssueRef,
        merged: MergedOk,
        attempt: u32,
    ) -> Result<TicketProgress, PipelineError> {
        let key = format!("close/{issue}/{attempt}");
        let record = self
            .store
            .load_effects(&self.run)
            .await?
            .into_iter()
            .find(|record| record.key == key);
        let unchanged = TicketProgress::Closing {
            merged: merged.clone(),
            attempt,
        };
        let outcome = match record {
            Some(record) => match record.outcome.as_deref() {
                Some(CLOSED) => return Ok(TicketProgress::Closed),
                Some(_) => {
                    let failed = PortError::failed("the close did not close the issue");
                    return Ok(match self.permit_github_retry(failed).await {
                        Ok(()) => TicketProgress::Closing {
                            merged,
                            attempt: attempt + 1,
                        },
                        // The pause is saved; the next step drains the tasks and pauses.
                        Err(PipelineError::Paused(_)) => unchanged,
                        Err(error) => return Err(error),
                    });
                }
                // The close may have run: its outcome is read from the issue.
                None => match self.forge.issue_status(issue).await {
                    Ok(IssueStatus::Closed) => CLOSED,
                    Ok(IssueStatus::Open) => NOT_CLOSED,
                    Err(error) => return self.retry_later(error, unchanged).await,
                },
            },
            None => {
                self.store
                    .record_effect_intent(&self.run, &key, "close ticket issue")
                    .await?;
                match self.forge.close_issue(issue).await {
                    Ok(()) => CLOSED,
                    Err(error) if error.is_failed() => NOT_CLOSED,
                    // Reconciled by the next step.
                    Err(_) => return Ok(unchanged),
                }
            }
        };
        self.store
            .record_effect_outcome(&self.run, &key, outcome)
            .await?;
        Ok(if outcome == CLOSED {
            TicketProgress::Closed
        } else {
            unchanged
        })
    }

    /// A lookup that failed is repeated by the next step while the policy permits.
    async fn retry_later(
        &self,
        error: PortError,
        progress: TicketProgress,
    ) -> Result<TicketProgress, PipelineError> {
        match self.permit_github_retry(error).await {
            Ok(()) | Err(PipelineError::Paused(_)) => Ok(progress),
            Err(error) => Err(error),
        }
    }

    /// Asks permission to repeat a GitHub call that failed with `error`, which is reconciled or
    /// has no effect. The budget and pause outlive a restart, so they are saved before acting
    /// on them.
    async fn permit_github_retry(&self, error: PortError) -> Result<(), PipelineError> {
        let permit = self.policy.permit_retry(Budget::GithubRetry, &error, true);
        self.policy.save(self.store.as_ref(), &self.run).await?;
        permit.map_err(|refused| match refused {
            RetryRefused::Paused(reason) => PipelineError::Paused(reason),
            RetryRefused::NeedsReconciliation => PipelineError::from(error),
        })
    }
}

/// Outcomes of a close attempt's effect record.
const CLOSED: &str = "closed";
const NOT_CLOSED: &str = "not closed";

impl<F, Fut> Pipeline for TicketPipeline<F>
where
    F: Fn(Ticket) -> Fut + Sync,
    Fut: Future<Output = Result<ImplementationState, PipelineError>> + Send + 'static,
{
    type State = TicketState;

    fn initial_state(&self) -> TicketState {
        TicketState::ReadingPlan
    }

    async fn step(&self, state: TicketState) -> Result<TicketState, PipelineError> {
        match state {
            TicketState::ReadingPlan => self.read_plan().await,
            TicketState::Running(board) => self.schedule(board).await,
            other @ (TicketState::Waiting(_)
            | TicketState::Done(_)
            | TicketState::Paused { .. }) => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Mutex;

    use chimera_core::error::PortError;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::{CommitId, Limits};
    use tokio::sync::Barrier;

    use super::*;
    use crate::test_support::CrashingStore;

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("o", "r", number).unwrap()
    }

    fn run_id() -> RunId {
        RunId::new("run").unwrap()
    }

    fn ticket(number: u64, blockers: &[u64]) -> Ticket {
        Ticket {
            issue: issue(number),
            status: IssueStatus::Open,
            blockers: blockers
                .iter()
                .map(|&n| Blocker {
                    issue: issue(n),
                    status: IssueStatus::Open,
                })
                .collect(),
        }
    }

    fn closed(mut ticket: Ticket) -> Ticket {
        ticket.status = IssueStatus::Closed;
        ticket
    }

    fn merged() -> MergedOk {
        MergedOk {
            commit: CommitId::new("c1").unwrap(),
        }
    }

    /// Merged, with its issue still to be closed by close attempt `attempt`.
    fn closing(attempt: u32) -> TicketProgress {
        TicketProgress::Closing {
            merged: merged(),
            attempt,
        }
    }

    /// The reason the pipeline ended paused with, if it did.
    fn pause_reason(state: &TicketState) -> Option<PauseReason> {
        match state {
            TicketState::Paused { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    fn paused() -> ImplementationState {
        ImplementationState::Paused {
            reason: PauseReason::LimitExhausted,
            resume_at: Box::new(ImplementationState::Implementing { cycle: 1 }),
        }
    }

    /// What the fake Implementation task of a ticket ends in; merged unless listed.
    #[derive(Default)]
    struct Script {
        paused: Vec<u64>,
        /// Tickets whose task fails with the global pause, after the rendezvous.
        pause_error: Vec<u64>,
        /// Tickets that wait for each other before ending, to prove they run concurrently.
        rendezvous: Vec<u64>,
    }

    type Started = (u64, Option<ImplementationState>);

    type Task = Pin<Box<dyn Future<Output = Result<ImplementationState, PipelineError>> + Send>>;

    struct Fixture {
        store: Arc<CrashingStore>,
        forge: Arc<FakeForge>,
        policy: Arc<Policy>,
        /// Tickets started, in order, and the saved task state each found.
        started: Arc<Mutex<Vec<Started>>>,
    }

    impl Fixture {
        fn new(tickets: Vec<Ticket>) -> Self {
            let forge = Arc::new(FakeForge::new("o", "r"));
            forge.set_plan(issue(4), TicketPlan { tickets });
            Self {
                store: Arc::new(CrashingStore::default()),
                forge,
                policy: Arc::new(Policy::new(&Limits::default())),
                started: Arc::default(),
            }
        }

        async fn run(&self, script: Script) -> Result<TicketState, PipelineError> {
            self.pipeline(script).run().await
        }

        /// A pipeline whose fake tasks end as `script` says.
        fn pipeline(&self, script: Script) -> TicketPipeline<impl Fn(Ticket) -> Task + Sync> {
            let barrier = Arc::new(Barrier::new(script.rendezvous.len().max(1)));
            let script = Arc::new(script);
            let store = self.store.clone();
            let started = self.started.clone();
            TicketPipeline::new(
                run_id(),
                issue(4),
                self.store.clone(),
                self.forge.clone(),
                self.policy.clone(),
                move |ticket: Ticket| {
                    let (store, started, script, barrier) = (
                        store.clone(),
                        started.clone(),
                        script.clone(),
                        barrier.clone(),
                    );
                    Box::pin(async move {
                        let number = ticket.issue.number();
                        let instance = format!("t{number}");
                        let saved = store
                            .load_pipeline_state(&run_id(), &instance)
                            .await?
                            .map(serde_json::from_value)
                            .transpose()?;
                        started.lock().unwrap().push((number, saved));
                        if script.rendezvous.contains(&number) {
                            barrier.wait().await;
                        }
                        if script.pause_error.contains(&number) {
                            return Err(PipelineError::Paused(PauseReason::GlobalPause));
                        }
                        let end = if script.paused.contains(&number) {
                            paused()
                        } else {
                            ImplementationState::Done(merged())
                        };
                        store
                            .save_pipeline_state(&run_id(), &instance, serde_json::to_value(&end)?)
                            .await?;
                        Ok(end)
                    }) as Task
                },
            )
        }

        fn started(&self) -> Vec<u64> {
            self.started.lock().unwrap().iter().map(|s| s.0).collect()
        }

        fn closes(&self) -> Vec<u64> {
            self.forge
                .calls()
                .into_iter()
                .filter_map(|call| match call {
                    ForgeCall::CloseIssue(issue) => Some(issue.number()),
                    _ => None,
                })
                .collect()
        }

        fn reads(&self) -> usize {
            self.forge
                .calls()
                .iter()
                .filter(|call| matches!(call, ForgeCall::ReadPlan(_)))
                .count()
        }

        async fn saved_progress(&self) -> Vec<(u64, TicketProgress)> {
            let saved = self
                .store
                .load_pipeline_state(&run_id(), STORE_INSTANCE)
                .await
                .unwrap()
                .unwrap();
            serde_json::from_value::<TicketState>(saved)
                .unwrap()
                .board()
                .unwrap()
                .entries
                .clone()
                .into_iter()
                .map(|entry| (entry.ticket.issue.number(), entry.progress))
                .collect()
        }

        async fn saved_policy(&self, limits: &Limits) -> Policy {
            Policy::load_or_new(self.store.as_ref(), &run_id(), limits)
                .await
                .unwrap()
        }

        async fn save_state(&self, entries: Vec<(Ticket, TicketProgress)>) {
            let state = TicketState::Running(TicketBoard {
                entries: entries
                    .into_iter()
                    .map(|(ticket, progress)| TicketEntry { ticket, progress })
                    .collect(),
            });
            self.store
                .save_pipeline_state(
                    &run_id(),
                    STORE_INSTANCE,
                    serde_json::to_value(state).unwrap(),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_dependency_chain_runs_in_order_and_each_ticket_is_closed_before_the_next_starts() {
        let f = Fixture::new(vec![ticket(3, &[2]), ticket(2, &[1]), ticket(1, &[])]);
        let state = f.run(Script::default()).await.unwrap();
        assert!(state.is_complete());
        assert_eq!(f.started(), [1, 2, 3]);
        assert_eq!(f.closes(), [1, 2, 3]);
        assert_eq!(f.reads(), 1);
        let calls = f.forge.calls();
        assert!(matches!(calls[0], ForgeCall::ReadPlan(_)));
    }

    #[tokio::test]
    async fn a_diamond_starts_the_middle_concurrently_and_the_join_after_both_close() {
        let f = Fixture::new(vec![
            ticket(4, &[2, 3]),
            ticket(2, &[1]),
            ticket(3, &[1]),
            ticket(1, &[]),
        ]);
        // Both middle tickets wait for each other, so this only ends if they run concurrently.
        let script = Script {
            rendezvous: vec![2, 3],
            ..Script::default()
        };
        let state = f.run(script).await.unwrap();
        assert!(state.is_complete());
        let started = f.started();
        assert_eq!(started[0], 1);
        assert_eq!(started[3], 4);
        let closes = f.closes();
        assert_eq!(closes[0], 1);
        assert_eq!(closes[3], 4);
    }

    #[tokio::test]
    async fn closed_tickets_count_as_done_and_are_neither_started_nor_closed() {
        let f = Fixture::new(vec![closed(ticket(1, &[])), ticket(2, &[1])]);
        let state = f.run(Script::default()).await.unwrap();
        assert!(state.is_complete());
        assert_eq!(f.started(), [2]);
        assert_eq!(f.closes(), [2]);
    }

    #[tokio::test]
    async fn a_closed_outside_blocker_is_ignored() {
        let mut t = ticket(1, &[]);
        t.blockers.push(Blocker {
            issue: issue(99),
            status: IssueStatus::Closed,
        });
        let f = Fixture::new(vec![t]);
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        assert_eq!(f.started(), [1]);
    }

    #[tokio::test]
    async fn an_open_outside_blocker_pauses_only_that_ticket_and_names_the_blocker() {
        let mut blocked = ticket(1, &[]);
        blocked.blockers.push(Blocker {
            issue: issue(99),
            status: IssueStatus::Open,
        });
        let f = Fixture::new(vec![blocked, ticket(2, &[]), ticket(3, &[1])]);
        let state = f.run(Script::default()).await.unwrap();
        assert!(!state.is_complete());
        assert_eq!(f.started(), [2]);
        assert_eq!(f.closes(), [2]);
        assert_eq!(
            state.board().unwrap().paused(),
            [(issue(1), TicketPause::Blocker(issue(99)))]
        );
    }

    #[tokio::test]
    async fn a_paused_task_does_not_stop_independent_tickets() {
        let f = Fixture::new(vec![
            ticket(1, &[]),
            ticket(2, &[]),
            ticket(3, &[1]),
            ticket(4, &[2]),
        ]);
        let script = Script {
            paused: vec![1],
            ..Script::default()
        };
        let state = f.run(script).await.unwrap();
        assert!(!state.is_complete());
        let mut started = f.started();
        started.sort();
        assert_eq!(started, [1, 2, 4]);
        assert_eq!(f.closes(), [2, 4]);
        assert_eq!(
            state.board().unwrap().paused(),
            [(issue(1), TicketPause::Reason(PauseReason::LimitExhausted))]
        );
    }

    #[tokio::test]
    async fn nothing_runnable_with_tickets_blocked_is_waiting_not_completion() {
        let f = Fixture::new(vec![ticket(1, &[2]), ticket(2, &[1])]);
        let state = f.run(Script::default()).await.unwrap();
        assert!(!state.is_complete());
        assert!(f.started().is_empty());
    }

    #[tokio::test]
    async fn restart_continues_saved_tasks_and_never_rereads_the_plan_or_repeats_a_close() {
        let tickets = vec![
            ticket(1, &[]),
            ticket(2, &[]),
            ticket(3, &[]),
            ticket(4, &[2]),
        ];
        let f = Fixture::new(tickets.clone());
        f.save_state(vec![
            (tickets[0].clone(), closing(1)),
            (tickets[1].clone(), TicketProgress::Running),
            (tickets[2].clone(), TicketProgress::Closed),
            (tickets[3].clone(), TicketProgress::Waiting),
        ])
        .await;
        // Ticket 1's close completed before the crash; ticket 2's task had progressed.
        let key = format!("close/{}/1", issue(1));
        f.store
            .record_effect_intent(&run_id(), &key, "close")
            .await
            .unwrap();
        f.store
            .record_effect_outcome(&run_id(), &key, "closed")
            .await
            .unwrap();
        let saved = ImplementationState::Reviewing { cycle: 2 };
        f.store
            .save_pipeline_state(&run_id(), "t2", serde_json::to_value(&saved).unwrap())
            .await
            .unwrap();

        let state = f.run(Script::default()).await.unwrap();
        assert!(state.is_complete());
        assert_eq!(f.reads(), 0);
        assert_eq!(f.started.lock().unwrap()[0], (2, Some(saved)));
        assert_eq!(f.started(), [2, 4]);
        assert_eq!(f.closes(), [2, 4]);
    }

    #[tokio::test]
    async fn restart_after_an_uncertain_close_reconciles_before_closing_again() {
        let tickets = vec![ticket(1, &[])];
        let f = Fixture::new(tickets.clone());
        f.save_state(vec![(tickets[0].clone(), closing(1))]).await;
        let key = format!("close/{}/1", issue(1));
        f.store
            .record_effect_intent(&run_id(), &key, "close")
            .await
            .unwrap();

        // The issue turns out to be closed: it is not closed again.
        f.forge.set_plan(
            issue(4),
            TicketPlan {
                tickets: vec![closed(tickets[0].clone())],
            },
        );
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        assert_eq!(f.closes(), Vec::<u64>::new());
        assert_eq!(f.reads(), 0);
    }

    #[tokio::test]
    async fn restart_after_the_close_succeeded_but_before_its_outcome_was_recorded() {
        let tickets = vec![ticket(1, &[])];
        let f = Fixture::new(tickets.clone());
        f.save_state(vec![(tickets[0].clone(), closing(1))]).await;
        let key = format!("close/{}/1", issue(1));
        f.store
            .record_effect_intent(&run_id(), &key, "close")
            .await
            .unwrap();
        f.forge.close_issue(&issue(1)).await.unwrap();

        assert!(f.run(Script::default()).await.unwrap().is_complete());
        assert_eq!(f.closes(), [1]);
        assert_eq!(f.reads(), 0);
        let effects = f.store.load_effects(&run_id()).await.unwrap();
        assert_eq!(effects[0].outcome.as_deref(), Some("closed"));
    }

    /// A merged ticket 1 whose first close loses its response.
    async fn merged_with_a_lost_close(issue_is_closed: bool) -> Fixture {
        let t = ticket(1, &[]);
        let f = Fixture::new(vec![if issue_is_closed {
            closed(t.clone())
        } else {
            t.clone()
        }]);
        f.save_state(vec![(t, closing(1))]).await;
        f.forge.fail_next(PortError::uncertain("lost"));
        f
    }

    #[tokio::test]
    async fn an_uncertain_close_that_happened_is_not_repeated() {
        let f = merged_with_a_lost_close(true).await;
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        assert_eq!(f.closes(), [1]);
    }

    #[tokio::test]
    async fn an_uncertain_close_that_did_not_happen_is_repeated_after_reconciling() {
        let f = merged_with_a_lost_close(false).await;
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        assert_eq!(f.closes(), [1, 1]);
        assert!(f.forge.is_closed(&issue(1)));
    }

    #[tokio::test]
    async fn a_task_pausing_the_run_lets_the_other_active_tasks_finish_and_be_saved() {
        let f = Fixture::new(vec![ticket(1, &[]), ticket(2, &[])]);
        let script = Script {
            rendezvous: vec![1, 2],
            pause_error: vec![1],
            ..Script::default()
        };
        let state = f.run(script).await.unwrap();
        assert_eq!(pause_reason(&state), Some(PauseReason::GlobalPause));
        assert_eq!(f.closes(), [2]);
        assert_eq!(
            f.saved_progress().await,
            [(1, TicketProgress::Running), (2, TicketProgress::Closed)]
        );
    }

    #[tokio::test]
    async fn an_exhausted_close_retry_budget_pauses_after_active_tasks_are_saved() {
        let tickets = vec![ticket(1, &[]), ticket(2, &[]), ticket(3, &[2])];
        let mut f = Fixture::new(tickets.clone());
        let limits = Limits {
            github_retries: 0,
            ..Limits::default()
        };
        f.policy = Arc::new(Policy::new(&limits));
        f.save_state(vec![
            (tickets[0].clone(), closing(1)),
            (tickets[1].clone(), TicketProgress::Running),
            (tickets[2].clone(), TicketProgress::Waiting),
        ])
        .await;
        f.forge.fail_next(PortError::failed("down"));

        let state = f.run(Script::default()).await.unwrap();
        assert_eq!(
            pause_reason(&state),
            Some(PauseReason::GithubRetriesExhausted)
        );
        // The running task finished and was saved; nothing new started and no close was retried.
        assert_eq!(f.started(), [2]);
        assert_eq!(f.closes(), [1]);
        assert_eq!(
            f.saved_progress().await,
            [
                (1, closing(1)),
                (2, closing(1)),
                (3, TicketProgress::Waiting),
            ]
        );
        assert_eq!(
            f.saved_policy(&Limits::default()).await.check_start(),
            Err(PauseReason::GithubRetriesExhausted)
        );
    }

    #[tokio::test]
    async fn restart_keeps_the_remaining_close_retries() {
        let mut f = merged_with_a_lost_close(false).await;
        let limits = Limits {
            github_retries: 3,
            ..Limits::default()
        };
        f.policy = Arc::new(Policy::new(&limits));
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        let restored = f.saved_policy(&Limits::default()).await.snapshot();
        assert_eq!(restored.github_retries_remaining, 2);
        assert_eq!(restored.paused, None);
    }

    #[tokio::test]
    async fn restart_after_close_retries_were_exhausted_stays_paused_with_no_retries() {
        let t = ticket(1, &[]);
        let mut f = Fixture::new(vec![t.clone()]);
        let limits = Limits {
            github_retries: 1,
            ..Limits::default()
        };
        f.policy = Arc::new(Policy::new(&limits));
        f.save_state(vec![(t, closing(1))]).await;
        f.forge.fail_next(PortError::failed("down"));
        f.forge.fail_next(PortError::failed("down"));

        let state = f.run(Script::default()).await.unwrap();
        assert_eq!(
            pause_reason(&state),
            Some(PauseReason::GithubRetriesExhausted)
        );
        let restored = f.saved_policy(&Limits::default()).await;
        let snapshot = restored.snapshot();
        assert_eq!(snapshot.github_retries_remaining, 0);
        assert_eq!(snapshot.paused, Some(PauseReason::GithubRetriesExhausted));
        assert_eq!(f.closes(), [1, 1]);

        // The restart stays paused and does not close again.
        f.policy = Arc::new(restored);
        let state = f.run(Script::default()).await.unwrap();
        assert_eq!(
            pause_reason(&state),
            Some(PauseReason::GithubRetriesExhausted)
        );
        assert_eq!(f.closes(), [1, 1]);
        assert_eq!(f.saved_progress().await, [(1, closing(2))]);
    }

    #[tokio::test]
    async fn a_failed_plan_read_is_retried_with_a_saved_budget() {
        let f = Fixture::new(vec![ticket(1, &[])]);
        f.forge.fail_next(PortError::failed("rate limited"));

        let state = f.run(Script::default()).await.unwrap();

        assert!(state.is_complete());
        assert_eq!(f.reads(), 2);
        let limits = Limits::default();
        assert_eq!(
            f.saved_policy(&limits)
                .await
                .snapshot()
                .github_retries_remaining,
            limits.github_retries - 1
        );
    }

    #[tokio::test]
    async fn a_plan_read_that_keeps_failing_pauses_with_the_pause_saved() {
        let limits = Limits {
            github_retries: 0,
            ..Limits::default()
        };
        let mut f = Fixture::new(vec![ticket(1, &[])]);
        f.policy = Arc::new(Policy::new(&limits));
        f.forge.fail_next(PortError::uncertain("lost"));

        let state = f.run(Script::default()).await.unwrap();

        assert_eq!(
            state,
            TicketState::Paused {
                reason: PauseReason::GithubRetriesExhausted,
                resume_at: Box::new(TicketState::ReadingPlan),
            }
        );
        assert!(f.started().is_empty());
        assert_eq!(
            f.saved_policy(&limits).await.check_start(),
            Err(PauseReason::GithubRetriesExhausted)
        );
    }

    #[tokio::test]
    async fn a_failed_reconciliation_lookup_is_retried_alone() {
        let f = Fixture::new(vec![ticket(1, &[]), ticket(2, &[1])]);
        f.save_state(vec![
            (ticket(1, &[]), closing(1)),
            (ticket(2, &[1]), TicketProgress::Waiting),
        ])
        .await;
        // A close was started before the restart; its lookup fails once.
        f.store
            .record_effect_intent(&run_id(), "close/o/r#1/1", "close ticket issue")
            .await
            .unwrap();
        f.forge.fail_next(PortError::failed("rate limited"));

        let state = f.run(Script::default()).await.unwrap();

        assert!(state.is_complete());
        assert_eq!(f.reads(), 0);
        assert_eq!(f.closes(), [1, 2]);
        // The failed lookup, then the close found not to have happened, were each retried.
        let limits = Limits::default();
        assert_eq!(
            f.saved_policy(&limits)
                .await
                .snapshot()
                .github_retries_remaining,
            limits.github_retries - 2
        );
    }

    #[tokio::test]
    async fn an_exhausted_reconciliation_lookup_pauses_without_dropping_running_tasks() {
        let limits = Limits {
            github_retries: 0,
            ..Limits::default()
        };
        let mut f = Fixture::new(vec![ticket(1, &[]), ticket(2, &[])]);
        f.policy = Arc::new(Policy::new(&limits));
        f.save_state(vec![
            (ticket(1, &[]), closing(1)),
            (ticket(2, &[]), TicketProgress::Running),
        ])
        .await;
        f.store
            .record_effect_intent(&run_id(), "close/o/r#1/1", "close ticket issue")
            .await
            .unwrap();
        f.forge.fail_next(PortError::failed("rate limited"));

        let state = f.run(Script::default()).await.unwrap();

        assert_eq!(
            pause_reason(&state),
            Some(PauseReason::GithubRetriesExhausted)
        );
        // The running task finished and was saved; the plan and its progress are kept.
        assert_eq!(f.started(), [2]);
        assert_eq!(
            f.saved_progress().await,
            [(1, closing(1)), (2, closing(1)),]
        );
        assert!(f.closes().is_empty());
    }

    #[tokio::test]
    async fn a_crash_at_every_write_closes_each_ticket_once() {
        let tickets = || vec![ticket(1, &[]), ticket(2, &[1]), ticket(3, &[])];
        let total = {
            let f = Fixture::new(tickets());
            f.run(Script::default()).await.unwrap();
            f.store.writes()
        };
        for crash_at in 1..=total {
            let f = Fixture::new(tickets());
            f.store.crash_at(Some(crash_at));
            assert!(
                f.run(Script::default()).await.is_err(),
                "crash at {crash_at}"
            );
            f.store.crash_at(None);

            let state = f.run(Script::default()).await.unwrap();

            assert!(state.is_complete(), "crash at {crash_at}");
            let mut closes = f.closes();
            closes.sort();
            assert_eq!(closes, [1, 2, 3], "crash at {crash_at}");
            assert_eq!(
                f.reads(),
                if crash_at == 1 { 2 } else { 1 },
                "crash at {crash_at}"
            );
        }
    }

    #[tokio::test]
    async fn each_step_saves_one_transition_with_at_most_one_github_call() {
        let f = Fixture::new(vec![ticket(1, &[])]);
        let pipeline = f.pipeline(Script::default());
        let progress =
            |state: &TicketState| state.board().map(|board| board.entries[0].progress.clone());
        let mut state = pipeline.initial_state();
        let mut seen = Vec::new();
        while !state.is_terminal() {
            let calls = f.forge.calls().len();
            state = pipeline.step(state).await.unwrap();
            assert!(f.forge.calls().len() - calls <= 1, "{state:?}");
            seen.push(progress(&state));
        }
        assert_eq!(
            seen,
            [
                // The plan is read.
                Some(TicketProgress::Waiting),
                // The ticket is marked running, then its task is started.
                Some(TicketProgress::Running),
                Some(TicketProgress::Running),
                // The task ended merged; its issue is closed.
                Some(closing(1)),
                Some(TicketProgress::Closed),
                Some(TicketProgress::Closed),
            ]
        );
        assert!(state.is_complete());
        assert_eq!(f.closes(), [1]);
    }

    #[tokio::test]
    async fn a_close_that_keeps_failing_ends_paused_when_the_retry_budget_is_used_up() {
        let mut f = Fixture::new(vec![ticket(1, &[])]);
        let limits = Limits {
            github_retries: 2,
            ..Limits::default()
        };
        f.policy = Arc::new(Policy::new(&limits));
        f.save_state(vec![(ticket(1, &[]), closing(1))]).await;
        for _ in 0..10 {
            f.forge.fail_next(PortError::failed("down"));
        }

        let state = f.run(Script::default()).await.unwrap();

        assert_eq!(
            pause_reason(&state),
            Some(PauseReason::GithubRetriesExhausted)
        );
        // The first close and one retry per unit of the budget.
        assert_eq!(f.closes(), [1, 1, 1]);
        assert_eq!(f.saved_progress().await, [(1, closing(3))]);
    }

    #[tokio::test]
    async fn a_paused_run_stays_paused_on_restart_and_continues_once_resumed() {
        let f = Fixture::new(vec![ticket(1, &[]), ticket(2, &[])]);
        let script = || Script {
            rendezvous: vec![1, 2],
            pause_error: vec![1],
            ..Script::default()
        };
        let paused = f.run(script()).await.unwrap();
        assert_eq!(pause_reason(&paused), Some(PauseReason::GlobalPause));

        // A restart leaves the paused state alone.
        assert_eq!(f.run(Script::default()).await.unwrap(), paused);
        assert_eq!(f.started(), [1, 2]);

        // Resuming starts the halted task again from its saved state.
        let TicketState::Paused { resume_at, .. } = paused else {
            unreachable!()
        };
        f.store
            .save_pipeline_state(
                &run_id(),
                STORE_INSTANCE,
                serde_json::to_value(*resume_at).unwrap(),
            )
            .await
            .unwrap();
        assert!(f.run(Script::default()).await.unwrap().is_complete());
        let mut started = f.started();
        started.sort();
        assert_eq!(started, [1, 1, 2]);
        let mut closes = f.closes();
        closes.sort();
        assert_eq!(closes, [1, 2]);
    }
}
