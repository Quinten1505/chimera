use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use chimera_core::forge::Forge;
use chimera_core::run_store::RunStore;
use chimera_core::{Blocker, IssueRef, IssueStatus, MergedOk, RunId, Ticket, TicketPlan};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

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
    /// Merged and verified; the issue is not yet known to be closed.
    Merged(MergedOk),
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

/// The ticket plan, fixed when it is read, and the progress of every ticket. This is the saved
/// state of the ticket pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketState {
    pub entries: Vec<TicketEntry>,
}

impl TicketState {
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

/// Feature -> all tickets merged: runs one Implementation task per ticket as soon as the ticket
/// is open, unblocked and not running, and closes each ticket issue after its verified merge.
///
/// `start` runs the Implementation task of a ticket (with the ticket configuration and
/// `WorkItem::Ticket`) through `drive`, which continues the task's saved state, and returns the
/// state it stopped in: `Done` or `Paused`. Tasks run in a `JoinSet` with no concurrency limit.
pub struct TicketPipeline<F> {
    pub run: RunId,
    pub specification: IssueRef,
    pub store: Arc<dyn RunStore>,
    pub forge: Arc<dyn Forge>,
    pub policy: Arc<Policy>,
    pub start: F,
}

impl<F, Fut> TicketPipeline<F>
where
    F: Fn(Ticket) -> Fut + Sync,
    Fut: Future<Output = Result<ImplementationState, PipelineError>> + Send + 'static,
{
    /// Runs until every ticket is closed or nothing can run any more: the remaining tickets are
    /// blocked or paused, which is waiting and not completion (see [`TicketState::paused`]). A
    /// saved plan is continued without reading the plan again.
    pub async fn run(&self) -> Result<TicketState, PipelineError> {
        let mut state = match self
            .store
            .load_pipeline_state(&self.run, STORE_INSTANCE)
            .await?
        {
            Some(saved) => serde_json::from_value(saved)?,
            None => {
                let state =
                    TicketState::from_plan(self.forge.read_plan(&self.specification).await?);
                self.save(&state).await?;
                state
            }
        };
        let mut tasks = JoinSet::new();
        let mut in_flight = HashSet::new();
        // A pause lets the active tasks finish and be saved but starts nothing new; the run
        // then ends paused. `halted` tasks paused and are not started again by this run.
        let mut pause = None;
        let mut halted = HashSet::new();
        let mut close_paused = false;
        loop {
            if !close_paused {
                match self.close_merged(&mut state).await {
                    Ok(()) => {}
                    Err(PipelineError::Paused(reason)) => {
                        close_paused = true;
                        pause.get_or_insert(reason);
                    }
                    Err(error) => return Err(error),
                }
            }
            let may_start = pause.is_none() && self.policy.check_start().is_ok();
            let mut to_start = Vec::new();
            for entry in &state.entries {
                let issue = &entry.ticket.issue;
                if entry.progress == TicketProgress::Running
                    && !in_flight.contains(issue)
                    && !halted.contains(issue)
                {
                    to_start.push(entry.ticket.clone());
                }
            }
            if may_start {
                let unblocked: Vec<Ticket> = state
                    .entries
                    .iter()
                    .filter(|entry| {
                        entry.progress == TicketProgress::Waiting
                            && state.is_unblocked(&entry.ticket)
                    })
                    .map(|entry| entry.ticket.clone())
                    .collect();
                for ticket in unblocked {
                    state.set(&ticket.issue, TicketProgress::Running);
                    to_start.push(ticket);
                }
            }
            if !to_start.is_empty() {
                self.save(&state).await?;
            }
            for ticket in to_start {
                in_flight.insert(ticket.issue.clone());
                let issue = ticket.issue.clone();
                let task = (self.start)(ticket);
                tasks.spawn(async move { (issue, task.await) });
            }
            let Some(joined) = tasks.join_next().await else {
                return match pause {
                    Some(reason) => Err(PipelineError::Paused(reason)),
                    None => Ok(state),
                };
            };
            let (issue, result) = joined.map_err(|error| {
                PipelineError::Environment(format!("ticket task failed: {error}"))
            })?;
            in_flight.remove(&issue);
            let progress = match result {
                Err(PipelineError::Paused(reason)) => {
                    halted.insert(issue);
                    pause.get_or_insert(reason);
                    continue;
                }
                Err(error) => return Err(error),
                Ok(end) => end,
            };
            let progress = match progress {
                ImplementationState::Done(merged) => TicketProgress::Merged(merged),
                ImplementationState::Paused { reason, .. } => TicketProgress::Paused(reason),
                other => {
                    return Err(PipelineError::Environment(format!(
                        "ticket {issue} stopped in {other:?}"
                    )));
                }
            };
            state.set(&issue, progress);
            self.save(&state).await?;
        }
    }

    async fn save(&self, state: &TicketState) -> Result<(), PipelineError> {
        self.store
            .save_pipeline_state(&self.run, STORE_INSTANCE, serde_json::to_value(state)?)
            .await?;
        Ok(())
    }

    /// Closes every merged ticket whose close has not completed.
    async fn close_merged(&self, state: &mut TicketState) -> Result<(), PipelineError> {
        let merged: Vec<IssueRef> = state
            .entries
            .iter()
            .filter(|entry| matches!(entry.progress, TicketProgress::Merged(_)))
            .map(|entry| entry.ticket.issue.clone())
            .collect();
        for issue in merged {
            self.close(&issue).await?;
            state.set(&issue, TicketProgress::Closed);
            self.save(state).await?;
        }
        Ok(())
    }

    /// Closes `issue` once. The intent is recorded before the close and the outcome after, so a
    /// completed close is never repeated and an interrupted or uncertain one is reconciled
    /// against the issue's status first.
    async fn close(&self, issue: &IssueRef) -> Result<(), PipelineError> {
        let key = format!("close/{issue}");
        let record = self
            .store
            .load_effects(&self.run)
            .await?
            .into_iter()
            .find(|record| record.key == key);
        match record {
            Some(record) if record.outcome.is_some() => return Ok(()),
            Some(_) if self.is_closed(issue).await? => {}
            Some(_) => {
                self.permit_close()?;
                self.close_with_retries(issue).await?;
            }
            None => {
                self.permit_close()?;
                self.store
                    .record_effect_intent(&self.run, &key, "close ticket issue")
                    .await?;
                self.close_with_retries(issue).await?;
            }
        }
        self.store
            .record_effect_outcome(&self.run, &key, "closed")
            .await?;
        Ok(())
    }

    /// A restored pause forbids another close; only reconciliation against the issue's status
    /// is allowed while paused.
    fn permit_close(&self) -> Result<(), PipelineError> {
        self.policy.check_start().map_err(PipelineError::Paused)
    }

    async fn close_with_retries(&self, issue: &IssueRef) -> Result<(), PipelineError> {
        loop {
            let error = match self.forge.close_issue(issue).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if error.is_uncertain() && self.is_closed(issue).await? {
                return Ok(());
            }
            let permit = self.policy.permit_retry(Budget::GithubRetry, &error, true);
            // The budget and pause outlive a restart, so they are saved before acting on them.
            self.policy.save(self.store.as_ref(), &self.run).await?;
            permit.map_err(|refused| match refused {
                RetryRefused::Paused(reason) => PipelineError::Paused(reason),
                RetryRefused::NeedsReconciliation => PipelineError::from(error),
            })?;
        }
    }

    /// Reads the current status of `issue` for reconciliation; the saved plan is not read.
    async fn is_closed(&self, issue: &IssueRef) -> Result<bool, PipelineError> {
        Ok(self.forge.issue_status(issue).await? == IssueStatus::Closed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chimera_core::error::PortError;
    use chimera_core::forge::{FakeForge, ForgeCall};
    use chimera_core::run_store::FakeRunStore;
    use chimera_core::{CommitId, Limits};
    use tokio::sync::Barrier;

    use super::*;

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

    struct Fixture {
        store: Arc<FakeRunStore>,
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
                store: Arc::new(FakeRunStore::new()),
                forge,
                policy: Arc::new(Policy::new(&Limits::default())),
                started: Arc::default(),
            }
        }

        async fn run(&self, script: Script) -> Result<TicketState, PipelineError> {
            let barrier = Arc::new(Barrier::new(script.rendezvous.len().max(1)));
            let script = Arc::new(script);
            let store = self.store.clone();
            let started = self.started.clone();
            let pipeline = TicketPipeline {
                run: run_id(),
                specification: issue(4),
                store: self.store.clone(),
                forge: self.forge.clone(),
                policy: self.policy.clone(),
                start: move |ticket: Ticket| {
                    let (store, started, script, barrier) = (
                        store.clone(),
                        started.clone(),
                        script.clone(),
                        barrier.clone(),
                    );
                    async move {
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
                    }
                },
            };
            pipeline.run().await
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
                .entries
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
            let state = TicketState {
                entries: entries
                    .into_iter()
                    .map(|(ticket, progress)| TicketEntry { ticket, progress })
                    .collect(),
            };
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
            state.paused(),
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
            state.paused(),
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
            (tickets[0].clone(), TicketProgress::Merged(merged())),
            (tickets[1].clone(), TicketProgress::Running),
            (tickets[2].clone(), TicketProgress::Closed),
            (tickets[3].clone(), TicketProgress::Waiting),
        ])
        .await;
        // Ticket 1's close completed before the crash; ticket 2's task had progressed.
        let key = format!("close/{}", issue(1));
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
        f.save_state(vec![(tickets[0].clone(), TicketProgress::Merged(merged()))])
            .await;
        let key = format!("close/{}", issue(1));
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
        f.save_state(vec![(tickets[0].clone(), TicketProgress::Merged(merged()))])
            .await;
        let key = format!("close/{}", issue(1));
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
        f.save_state(vec![(t, TicketProgress::Merged(merged()))])
            .await;
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
        let result = f.run(script).await;
        assert!(matches!(
            result,
            Err(PipelineError::Paused(PauseReason::GlobalPause))
        ));
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
            (tickets[0].clone(), TicketProgress::Merged(merged())),
            (tickets[1].clone(), TicketProgress::Running),
            (tickets[2].clone(), TicketProgress::Waiting),
        ])
        .await;
        f.forge.fail_next(PortError::failed("down"));

        let result = f.run(Script::default()).await;
        assert!(matches!(
            result,
            Err(PipelineError::Paused(PauseReason::GithubRetriesExhausted))
        ));
        // The running task finished and was saved; nothing new started and no close was retried.
        assert_eq!(f.started(), [2]);
        assert_eq!(f.closes(), [1]);
        assert_eq!(
            f.saved_progress().await,
            [
                (1, TicketProgress::Merged(merged())),
                (2, TicketProgress::Merged(merged())),
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
        f.save_state(vec![(t, TicketProgress::Merged(merged()))])
            .await;
        f.forge.fail_next(PortError::failed("down"));
        f.forge.fail_next(PortError::failed("down"));

        let result = f.run(Script::default()).await;
        assert!(matches!(
            result,
            Err(PipelineError::Paused(PauseReason::GithubRetriesExhausted))
        ));
        let restored = f.saved_policy(&Limits::default()).await;
        let snapshot = restored.snapshot();
        assert_eq!(snapshot.github_retries_remaining, 0);
        assert_eq!(snapshot.paused, Some(PauseReason::GithubRetriesExhausted));
        assert_eq!(f.closes(), [1, 1]);

        // Rerunning with the restored policy reconciles but does not close again.
        f.policy = Arc::new(restored);
        let result = f.run(Script::default()).await;
        assert!(matches!(
            result,
            Err(PipelineError::Paused(PauseReason::GithubRetriesExhausted))
        ));
        assert_eq!(f.closes(), [1, 1]);
        assert_eq!(
            f.saved_progress().await,
            [(1, TicketProgress::Merged(merged()))]
        );
    }
}
