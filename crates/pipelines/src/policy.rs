use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use chimera_core::error::PortError;
use chimera_core::run_store::RunStore;
use chimera_core::{CommitId, Limits, RunId};
use futures_util::lock::Mutex as AsyncMutex;
use serde::{Deserialize, Serialize};

use crate::error::{PauseReason, PipelineError};

/// Instance name under which the policy is saved in the `RunStore`.
const STORE_INSTANCE: &str = "policy";

/// A run-wide budget that a retry consumes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    AgentRecovery,
    GithubRetry,
}

/// Why a retry was not permitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryRefused {
    /// The run is paused, possibly because this very request exhausted the budget.
    Paused(PauseReason),
    /// The effect is uncertain and must be reconciled before it may be retried.
    NeedsReconciliation,
}

/// The serializable part of the policy: the pause flag and the remaining budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyState {
    pub paused: Option<PauseReason>,
    pub agent_recovery_remaining: u32,
    pub github_retries_remaining: u32,
}

/// Global pause flag and run-wide budgets, shared by all pipeline tasks.
#[derive(Debug)]
pub struct Policy {
    state: Mutex<PolicyState>,
    /// Tasks waiting for the run to pause; registered and woken under the `state` lock.
    pause_wakers: Mutex<Vec<Waker>>,
    /// Serializes saving, so that a save started later never stores an older snapshot.
    writes: AsyncMutex<()>,
}

impl Policy {
    pub fn new(limits: &Limits) -> Self {
        Self::restore(PolicyState {
            paused: None,
            agent_recovery_remaining: limits.agent_recovery,
            github_retries_remaining: limits.github_retries,
        })
    }

    pub fn restore(state: PolicyState) -> Self {
        Self {
            state: Mutex::new(state),
            pause_wakers: Mutex::default(),
            writes: AsyncMutex::new(()),
        }
    }

    pub fn snapshot(&self) -> PolicyState {
        *self.state.lock().unwrap()
    }

    /// Consulted before starting any agent, handoff, or merge; refuses with the pause reason
    /// while the run is paused.
    pub fn check_start(&self) -> Result<(), PauseReason> {
        match self.state.lock().unwrap().paused {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }

    /// Sets the global pause unless the run is already paused; the first reason is kept.
    pub fn pause(&self, reason: PauseReason) {
        let mut state = self.state.lock().unwrap();
        if state.paused.is_none() {
            self.set_paused(&mut state, reason);
        }
    }

    /// Ready with the pause reason once the run is paused; until then `context` is woken when
    /// it pauses.
    pub fn poll_paused(&self, context: &mut Context<'_>) -> Poll<PauseReason> {
        let state = self.state.lock().unwrap();
        match state.paused {
            Some(reason) => Poll::Ready(reason),
            None => {
                self.pause_wakers
                    .lock()
                    .unwrap()
                    .push(context.waker().clone());
                Poll::Pending
            }
        }
    }

    fn set_paused(&self, state: &mut PolicyState, reason: PauseReason) {
        state.paused = Some(reason);
        self.pause_wakers
            .lock()
            .unwrap()
            .drain(..)
            .for_each(Waker::wake);
    }

    /// Pauses the run if the remote head of the feature branch is not the expected one.
    pub fn check_remote_head(
        &self,
        expected: &CommitId,
        actual: &CommitId,
    ) -> Result<(), PauseReason> {
        if expected != actual {
            self.pause(PauseReason::UnexpectedRemoteChange);
        }
        self.check_start()
    }

    /// Asks permission to retry the effect that failed with `error`. A failed effect is retried
    /// while the budget lasts, consuming one; a request on an empty budget pauses the run. An
    /// uncertain effect is refused unless the caller has `reconciled` it.
    pub fn permit_retry(
        &self,
        budget: Budget,
        error: &PortError,
        reconciled: bool,
    ) -> Result<(), RetryRefused> {
        let mut state = self.state.lock().unwrap();
        if let Some(reason) = state.paused {
            return Err(RetryRefused::Paused(reason));
        }
        if error.is_uncertain() && !reconciled {
            return Err(RetryRefused::NeedsReconciliation);
        }
        let (remaining, exhausted) = match budget {
            Budget::AgentRecovery => (
                &mut state.agent_recovery_remaining,
                PauseReason::AgentRecoveryExhausted,
            ),
            Budget::GithubRetry => (
                &mut state.github_retries_remaining,
                PauseReason::GithubRetriesExhausted,
            ),
        };
        if *remaining == 0 {
            self.set_paused(&mut state, exhausted);
            return Err(RetryRefused::Paused(exhausted));
        }
        *remaining -= 1;
        Ok(())
    }

    /// Saves the current state so it survives a restart. Saves are ordered: the snapshot is
    /// taken only once the previous save finished, so the stored state is never older than the
    /// one a completed save stored.
    pub async fn save(&self, store: &dyn RunStore, run: &RunId) -> Result<(), PipelineError> {
        let _write = self.writes.lock().await;
        let value = serde_json::to_value(self.snapshot())?;
        store
            .save_pipeline_state(run, STORE_INSTANCE, value)
            .await?;
        Ok(())
    }

    /// Restores the saved policy of `run`, or starts a fresh one from `limits`.
    pub async fn load_or_new(
        store: &dyn RunStore,
        run: &RunId,
        limits: &Limits,
    ) -> Result<Self, PipelineError> {
        match store.load_pipeline_state(run, STORE_INSTANCE).await? {
            Some(saved) => Ok(Self::restore(serde_json::from_value(saved)?)),
            None => Ok(Self::new(limits)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use chimera_core::TurnResult;
    use chimera_core::run_store::{EffectRecord, FakeRunStore};
    use futures_executor::block_on;
    use tokio::sync::Notify;

    use super::*;

    fn limits(agent_recovery: u32, github_retries: u32) -> Limits {
        Limits {
            agent_recovery,
            github_retries,
            ..Limits::default()
        }
    }

    fn failed() -> PortError {
        PortError::failed("boom")
    }

    fn commit(id: &str) -> CommitId {
        CommitId::new(id).unwrap()
    }

    #[test]
    fn budgets_start_from_limits() {
        let state = Policy::new(&limits(3, 4)).snapshot();
        assert_eq!(state.paused, None);
        assert_eq!(state.agent_recovery_remaining, 3);
        assert_eq!(state.github_retries_remaining, 4);
    }

    #[test]
    fn failed_effect_retries_consume_the_budget_then_pause() {
        for (budget, reason) in [
            (Budget::AgentRecovery, PauseReason::AgentRecoveryExhausted),
            (Budget::GithubRetry, PauseReason::GithubRetriesExhausted),
        ] {
            let policy = Policy::new(&limits(2, 2));
            assert_eq!(policy.permit_retry(budget, &failed(), false), Ok(()));
            assert_eq!(policy.permit_retry(budget, &failed(), false), Ok(()));
            assert_eq!(policy.check_start(), Ok(()));
            assert_eq!(
                policy.permit_retry(budget, &failed(), false),
                Err(RetryRefused::Paused(reason))
            );
            assert_eq!(policy.check_start(), Err(reason));
        }
    }

    #[test]
    fn budgets_are_independent() {
        let policy = Policy::new(&limits(1, 5));
        policy
            .permit_retry(Budget::AgentRecovery, &failed(), false)
            .unwrap();
        policy
            .permit_retry(Budget::GithubRetry, &failed(), false)
            .unwrap();
        let state = policy.snapshot();
        assert_eq!(state.agent_recovery_remaining, 0);
        assert_eq!(state.github_retries_remaining, 4);
    }

    #[test]
    fn unexpected_remote_change_pauses() {
        let policy = Policy::new(&Limits::default());
        assert_eq!(policy.check_remote_head(&commit("a"), &commit("a")), Ok(()));
        assert_eq!(
            policy.check_remote_head(&commit("a"), &commit("b")),
            Err(PauseReason::UnexpectedRemoteChange)
        );
        assert_eq!(
            policy.check_start(),
            Err(PauseReason::UnexpectedRemoteChange)
        );
    }

    #[test]
    fn paused_policy_refuses_new_starts_and_retries() {
        let policy = Policy::new(&Limits::default());
        policy.pause(PauseReason::GlobalPause);
        policy.pause(PauseReason::UnexpectedRemoteChange);
        assert_eq!(policy.check_start(), Err(PauseReason::GlobalPause));
        assert_eq!(
            policy.permit_retry(Budget::GithubRetry, &failed(), false),
            Err(RetryRefused::Paused(PauseReason::GlobalPause))
        );
        assert_eq!(
            policy.snapshot().github_retries_remaining,
            Limits::default().github_retries
        );
    }

    #[test]
    fn uncertain_effects_require_reconciliation() {
        let policy = Policy::new(&limits(1, 1));
        let uncertain = PortError::uncertain("lost");
        assert_eq!(
            policy.permit_retry(Budget::GithubRetry, &uncertain, false),
            Err(RetryRefused::NeedsReconciliation)
        );
        assert_eq!(policy.snapshot().github_retries_remaining, 1);
        assert_eq!(policy.check_start(), Ok(()));
        assert_eq!(
            policy.permit_retry(Budget::GithubRetry, &uncertain, true),
            Ok(())
        );
        assert_eq!(policy.snapshot().github_retries_remaining, 0);
    }

    #[test]
    fn state_round_trips_across_a_restart() {
        let store = FakeRunStore::new();
        let run = RunId::new("run-1").unwrap();
        let before = Policy::new(&limits(3, 3));
        before
            .permit_retry(Budget::AgentRecovery, &failed(), false)
            .unwrap();
        before.pause(PauseReason::UnexpectedRemoteChange);
        block_on(before.save(&store, &run)).unwrap();

        let after = block_on(Policy::load_or_new(&store, &run, &Limits::default())).unwrap();

        assert_eq!(after.snapshot(), before.snapshot());
        assert_eq!(
            after.check_start(),
            Err(PauseReason::UnexpectedRemoteChange)
        );
        assert_eq!(after.snapshot().agent_recovery_remaining, 2);
    }

    #[test]
    fn load_without_saved_state_starts_fresh() {
        let store = FakeRunStore::new();
        let run = RunId::new("run-1").unwrap();
        let policy = block_on(Policy::load_or_new(&store, &run, &limits(7, 8))).unwrap();
        assert_eq!(policy.snapshot().agent_recovery_remaining, 7);
        assert_eq!(policy.snapshot().github_retries_remaining, 8);
    }

    /// Holds the first pipeline state save back until a later one has been stored, or until a
    /// timeout passes if no later save can reach the store meanwhile.
    struct ReorderingStore {
        inner: FakeRunStore,
        saves: Mutex<usize>,
        later_stored: Notify,
    }

    #[async_trait]
    impl RunStore for ReorderingStore {
        async fn save_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
            state: serde_json::Value,
        ) -> Result<(), PortError> {
            let number = {
                let mut saves = self.saves.lock().unwrap();
                *saves += 1;
                *saves
            };
            if number == 1 {
                let later = self.later_stored.notified();
                let _ = tokio::time::timeout(Duration::from_secs(1), later).await;
                return self.inner.save_pipeline_state(run, pipeline, state).await;
            }
            let saved = self.inner.save_pipeline_state(run, pipeline, state).await;
            self.later_stored.notify_one();
            saved
        }
        async fn load_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
        ) -> Result<Option<serde_json::Value>, PortError> {
            self.inner.load_pipeline_state(run, pipeline).await
        }
        async fn save_run_data(
            &self,
            run: &RunId,
            data: serde_json::Value,
        ) -> Result<(), PortError> {
            self.inner.save_run_data(run, data).await
        }
        async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
            self.inner.load_run_data(run).await
        }
        async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
            self.inner.append_turn(run, turn).await
        }
        async fn load_history(&self, run: &RunId) -> Result<Vec<TurnResult>, PortError> {
            self.inner.load_history(run).await
        }
        async fn record_effect_intent(
            &self,
            run: &RunId,
            key: &str,
            intent: &str,
        ) -> Result<(), PortError> {
            self.inner.record_effect_intent(run, key, intent).await
        }
        async fn record_effect_outcome(
            &self,
            run: &RunId,
            key: &str,
            outcome: &str,
        ) -> Result<(), PortError> {
            self.inner.record_effect_outcome(run, key, outcome).await
        }
        async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
            self.inner.load_effects(run).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_delayed_older_save_never_overwrites_a_newer_one() {
        let store = ReorderingStore {
            inner: FakeRunStore::new(),
            saves: Mutex::new(0),
            later_stored: Notify::new(),
        };
        let run = RunId::new("run-1").unwrap();
        let policy = Policy::new(&limits(3, 3));

        let (first, second) = tokio::join!(policy.save(&store, &run), async {
            // The first save has taken its snapshot and is held back in the store.
            tokio::task::yield_now().await;
            policy
                .permit_retry(Budget::GithubRetry, &failed(), false)
                .unwrap();
            policy.pause(PauseReason::GlobalPause);
            policy.save(&store, &run).await
        });
        first.unwrap();
        second.unwrap();

        let restored = Policy::load_or_new(&store, &run, &Limits::default())
            .await
            .unwrap();
        assert_eq!(restored.snapshot(), policy.snapshot());
        assert_eq!(restored.check_start(), Err(PauseReason::GlobalPause));
        assert_eq!(restored.snapshot().github_retries_remaining, 2);
    }
}
