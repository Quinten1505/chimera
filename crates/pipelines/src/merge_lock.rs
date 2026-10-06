use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::task::{Poll, Waker};

use chimera_core::error::PortError;
use chimera_core::run_store::RunStore;
use chimera_core::{BranchName, RunId};
use futures_util::lock::Mutex as AsyncMutex;
use serde::{Deserialize, Serialize};

use crate::error::PipelineError;
use crate::policy::Policy;

/// Enqueue position of a waiter; assigned once and never reused.
pub type Sequence = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    owner: String,
    sequence: Sequence,
}

/// The persisted part of the lock: who holds it, who waits (in grant order), and the next
/// sequence number.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct LockState {
    holder: Option<Entry>,
    queue: VecDeque<Entry>,
    next_sequence: Sequence,
}

#[derive(Default)]
struct Shared {
    state: LockState,
    /// A save started and did not complete cleanly (failed, uncertain or cancelled), so the
    /// stored state may differ from `state` until it is reloaded.
    stale: bool,
    wakers: Vec<Waker>,
}

/// Live locks by store (address), run and instance, so every open of the same branch shares one
/// lock. A live lock keeps its store alive, so an address cannot be reused while it is listed.
type Registry = HashMap<(usize, RunId, String), Weak<MergeLock>>;

static REGISTRY: LazyLock<AsyncMutex<Registry>> = LazyLock::new(Default::default);

/// Fair FIFO async lock for one feature branch.
///
/// Owners are identified by a stable key (e.g. the implementation pipeline instance) so the
/// holder and the waiting order survive a restart. The lock is only ever released by
/// [`MergeLock::release`]; dropping or pausing the task of the holder leaves it held, and
/// dropping a pending [`MergeLock::acquire`] leaves its place in the queue.
///
/// There is one live lock per store, run and branch: [`MergeLock::open`] hands out the same
/// instance while any handle to it exists.
pub struct MergeLock {
    store: Arc<dyn RunStore>,
    run: RunId,
    instance: String,
    shared: Mutex<Shared>,
    /// Serializes persisting so the stored state is always the latest one.
    writes: AsyncMutex<()>,
}

impl MergeLock {
    /// Opens the lock of `branch`, restoring holder and queue if they were saved. Returns the
    /// already open lock if there is one.
    pub async fn open(
        store: Arc<dyn RunStore>,
        run: RunId,
        branch: &BranchName,
    ) -> Result<Arc<Self>, PipelineError> {
        let instance = format!("merge_lock:{branch}");
        let key = (
            Arc::as_ptr(&store).cast::<()>() as usize,
            run.clone(),
            instance.clone(),
        );
        // Held across the load so concurrent opens cannot create two locks.
        let mut registry = REGISTRY.lock().await;
        if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let state = match store.load_pipeline_state(&run, &instance).await? {
            Some(saved) => serde_json::from_value(saved)?,
            None => LockState::default(),
        };
        let lock = Arc::new(Self {
            store,
            run,
            instance,
            shared: Mutex::new(Shared {
                state,
                stale: false,
                wakers: Vec::new(),
            }),
            writes: AsyncMutex::new(()),
        });
        registry.retain(|_, weak| weak.strong_count() > 0);
        registry.insert(key, Arc::downgrade(&lock));
        Ok(lock)
    }

    /// Takes a place in the queue, or the lock itself if it is free. Idempotent: an owner that
    /// already holds or waits keeps its sequence number.
    pub async fn enqueue(&self, owner: &str) -> Result<Sequence, PipelineError> {
        let _write = self.writes.lock().await;
        let mut state = self.current().await?;
        if let Some(entry) = state
            .holder
            .iter()
            .chain(&state.queue)
            .find(|entry| entry.owner == owner)
        {
            return Ok(entry.sequence);
        }
        let entry = Entry {
            owner: owner.to_string(),
            sequence: state.next_sequence,
        };
        state.next_sequence += 1;
        if state.holder.is_none() {
            state.holder = Some(entry.clone());
        } else {
            state.queue.push_back(entry.clone());
        }
        self.commit(state).await?;
        Ok(entry.sequence)
    }

    /// Enqueues `owner` and waits until it holds the lock, or until the run is paused: then it
    /// returns the pause, and `owner` keeps its place in the queue.
    pub async fn acquire(&self, owner: &str, policy: &Policy) -> Result<Sequence, PipelineError> {
        let sequence = self.enqueue(owner).await?;
        std::future::poll_fn(|context| {
            let mut shared = self.shared.lock().unwrap();
            if shared
                .state
                .holder
                .as_ref()
                .is_some_and(|h| h.owner == owner)
            {
                return Poll::Ready(Ok(sequence));
            }
            shared.wakers.push(context.waker().clone());
            policy
                .poll_paused(context)
                .map(|reason| Err(PipelineError::Paused(reason)))
        })
        .await
    }

    /// Releases the lock after a verified push and grants it to the next waiter. Fails if
    /// `owner` is not the holder.
    pub async fn release(&self, owner: &str) -> Result<(), PipelineError> {
        let _write = self.writes.lock().await;
        let mut state = self.current().await?;
        if state
            .holder
            .as_ref()
            .is_none_or(|holder| holder.owner != owner)
        {
            return Err(PortError::failed(format!("{owner} does not hold the merge lock")).into());
        }
        state.holder = state.queue.pop_front();
        self.commit(state).await
    }

    pub fn holder(&self) -> Option<String> {
        self.shared
            .lock()
            .unwrap()
            .state
            .holder
            .as_ref()
            .map(|h| h.owner.clone())
    }

    /// Waiting owners in grant order, excluding the holder.
    pub fn waiting(&self) -> Vec<String> {
        let shared = self.shared.lock().unwrap();
        shared
            .state
            .queue
            .iter()
            .map(|entry| entry.owner.clone())
            .collect()
    }

    /// The state to mutate. If an earlier save may or may not have been persisted, the stored
    /// state is the truth, so reload it first. Callers hold `writes`.
    async fn current(&self) -> Result<LockState, PipelineError> {
        {
            let shared = self.shared.lock().unwrap();
            if !shared.stale {
                return Ok(shared.state.clone());
            }
        }
        let state = match self
            .store
            .load_pipeline_state(&self.run, &self.instance)
            .await?
        {
            Some(saved) => serde_json::from_value(saved)?,
            None => LockState::default(),
        };
        let mut shared = self.shared.lock().unwrap();
        shared.state = state.clone();
        shared.stale = false;
        shared.wakers.drain(..).for_each(Waker::wake);
        Ok(state)
    }

    /// Saves `state`, then makes it current and wakes the waiters. Callers hold `writes`.
    async fn commit(&self, state: LockState) -> Result<(), PipelineError> {
        let saved = serde_json::to_value(&state)?;
        // Stays set if the save errs or this future is dropped mid-save.
        self.shared.lock().unwrap().stale = true;
        self.store
            .save_pipeline_state(&self.run, &self.instance, saved)
            .await?;
        let mut shared = self.shared.lock().unwrap();
        shared.state = state;
        shared.stale = false;
        shared.wakers.drain(..).for_each(Waker::wake);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Waker};

    use async_trait::async_trait;
    use chimera_core::run_store::{EffectRecord, FakeRunStore};
    use chimera_core::{Limits, TurnResult};
    use futures_executor::{LocalPool, block_on};
    use futures_util::task::LocalSpawnExt;

    use super::*;
    use crate::error::PauseReason;

    /// The policy of a run that is never paused.
    static RUNNING: LazyLock<Policy> = LazyLock::new(|| Policy::new(&Limits::default()));

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn branch() -> BranchName {
        BranchName::new("spec/4-pipelines").unwrap()
    }

    async fn open(store: &Arc<dyn RunStore>) -> Arc<MergeLock> {
        MergeLock::open(store.clone(), run(), &branch())
            .await
            .unwrap()
    }

    #[derive(Clone, Copy)]
    enum Fault {
        None,
        /// Persists, then reports the outcome as unknown.
        Uncertain,
        /// Persists, then never completes.
        HangAfterSave,
    }

    /// Fake store whose next pipeline-state save persists and then misbehaves.
    struct FaultyStore {
        inner: FakeRunStore,
        fault: Mutex<Fault>,
    }

    impl FaultyStore {
        fn set(&self, fault: Fault) {
            *self.fault.lock().unwrap() = fault;
        }
    }

    #[async_trait]
    impl RunStore for FaultyStore {
        async fn save_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
            state: serde_json::Value,
        ) -> Result<(), PortError> {
            self.inner.save_pipeline_state(run, pipeline, state).await?;
            let fault = std::mem::replace(&mut *self.fault.lock().unwrap(), Fault::None);
            match fault {
                Fault::None => Ok(()),
                Fault::Uncertain => Err(PortError::uncertain("save outcome unknown")),
                Fault::HangAfterSave => std::future::pending().await,
            }
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

    fn faulty() -> (Arc<FaultyStore>, Arc<MergeLock>) {
        let faulty = Arc::new(FaultyStore {
            inner: FakeRunStore::new(),
            fault: Mutex::new(Fault::None),
        });
        let store: Arc<dyn RunStore> = faulty.clone();
        let lock = block_on(open(&store));
        (faulty, lock)
    }

    /// Polls `future` once, then drops it, as when its task is cancelled.
    fn poll_once_and_drop<F: Future>(future: F) {
        let mut future = Box::pin(future);
        let mut context = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn uncertain_enqueue_save_is_reconciled_before_the_next_mutation() {
        let (store, lock) = faulty();
        block_on(lock.acquire("a", &RUNNING)).unwrap();
        store.set(Fault::Uncertain);
        assert!(matches!(
            block_on(lock.enqueue("b")),
            Err(PipelineError::Port(PortError::Uncertain(_)))
        ));

        // b was persisted, so c queues behind it with a fresh sequence number.
        assert_eq!(block_on(lock.enqueue("c")).unwrap(), 2);
        assert_eq!(block_on(lock.enqueue("b")).unwrap(), 1);
        assert_eq!(lock.waiting(), ["b", "c"]);
    }

    #[test]
    fn cancelled_enqueue_after_save_is_reconciled_before_the_next_mutation() {
        let (store, lock) = faulty();
        block_on(lock.acquire("a", &RUNNING)).unwrap();
        store.set(Fault::HangAfterSave);
        poll_once_and_drop(lock.enqueue("b"));

        assert_eq!(block_on(lock.enqueue("c")).unwrap(), 2);
        assert_eq!(lock.waiting(), ["b", "c"]);
    }

    #[test]
    fn uncertain_release_save_is_reconciled_before_the_next_mutation() {
        let (store, lock) = faulty();
        block_on(lock.acquire("a", &RUNNING)).unwrap();
        block_on(lock.enqueue("b")).unwrap();
        store.set(Fault::Uncertain);
        assert!(matches!(
            block_on(lock.release("a")),
            Err(PipelineError::Port(PortError::Uncertain(_)))
        ));

        // The release was persisted: b holds, and a no longer does.
        assert!(block_on(lock.release("a")).is_err());
        assert_eq!(lock.holder().as_deref(), Some("b"));
        block_on(lock.release("b")).unwrap();
        assert_eq!(lock.holder(), None);
    }

    #[test]
    fn cancelled_release_after_save_is_reconciled_before_the_next_mutation() {
        let (store, lock) = faulty();
        block_on(lock.acquire("a", &RUNNING)).unwrap();
        block_on(lock.enqueue("b")).unwrap();
        store.set(Fault::HangAfterSave);
        poll_once_and_drop(lock.release("a"));

        assert!(block_on(lock.release("a")).is_err());
        assert_eq!(lock.holder().as_deref(), Some("b"));
        assert_eq!(block_on(lock.enqueue("c")).unwrap(), 2);
    }

    fn store() -> Arc<dyn RunStore> {
        Arc::new(FakeRunStore::new())
    }

    #[test]
    fn waiters_are_granted_in_fifo_order() {
        let store = store();
        let lock = block_on(open(&store));
        let granted = Arc::new(Mutex::new(Vec::new()));
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        // The first owner holds the lock; the others queue up in order.
        block_on(lock.acquire("w0", &RUNNING)).unwrap();
        for index in 1..=5 {
            block_on(lock.enqueue(&format!("w{index}"))).unwrap();
        }
        // Spawn the waiters in reverse so wake order cannot explain the grant order.
        for index in (1..=5).rev() {
            let (lock, granted) = (lock.clone(), granted.clone());
            spawner
                .spawn_local(async move {
                    let owner = format!("w{index}");
                    lock.acquire(&owner, &RUNNING).await.unwrap();
                    granted.lock().unwrap().push(owner.clone());
                    lock.release(&owner).await.unwrap();
                })
                .unwrap();
        }
        pool.run_until_stalled();
        assert!(granted.lock().unwrap().is_empty());

        block_on(lock.release("w0")).unwrap();
        pool.run();
        assert_eq!(*granted.lock().unwrap(), ["w1", "w2", "w3", "w4", "w5"]);
        assert_eq!(lock.holder(), None);
    }

    #[test]
    fn at_most_one_holder_and_release_grants_next() {
        let store = store();
        let lock = block_on(open(&store));
        assert_eq!(block_on(lock.enqueue("a")).unwrap(), 0);
        assert_eq!(block_on(lock.enqueue("b")).unwrap(), 1);
        assert_eq!(block_on(lock.enqueue("b")).unwrap(), 1);
        assert_eq!(lock.holder().as_deref(), Some("a"));
        assert_eq!(lock.waiting(), ["b"]);

        let mut waiting = pin!(lock.acquire("b", &RUNNING));
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());

        assert!(block_on(lock.release("b")).is_err());
        block_on(lock.release("a")).unwrap();
        assert_eq!(lock.holder().as_deref(), Some("b"));
        assert_eq!(block_on(waiting).unwrap(), 1);
    }

    #[test]
    fn holder_keeps_lock_when_its_task_is_dropped() {
        let store = store();
        let lock = block_on(open(&store));
        let mut context = Context::from_waker(Waker::noop());
        {
            let mut holding = Box::pin(lock.acquire("a", &RUNNING));
            assert!(holding.as_mut().poll(&mut context).is_ready());
            let mut waiting = Box::pin(lock.acquire("b", &RUNNING));
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            // Both futures are dropped, as when a paused task is torn down.
        }
        assert_eq!(lock.holder().as_deref(), Some("a"));
        assert_eq!(lock.waiting(), ["b"]);
    }

    #[test]
    fn restart_restores_holder_and_waiter_order() {
        let store = store();
        let lock = block_on(open(&store));
        for owner in ["a", "b", "c", "d"] {
            block_on(lock.enqueue(owner)).unwrap();
        }
        block_on(lock.release("a")).unwrap();
        drop(lock);

        let restored = block_on(open(&store));
        assert_eq!(restored.holder().as_deref(), Some("b"));
        assert_eq!(restored.waiting(), ["c", "d"]);
        // Sequence numbers continue where they left off.
        assert_eq!(block_on(restored.enqueue("e")).unwrap(), 4);
        block_on(restored.release("b")).unwrap();
        block_on(restored.release("c")).unwrap();
        assert_eq!(restored.holder().as_deref(), Some("d"));
        assert_eq!(restored.waiting(), ["e"]);
    }

    #[test]
    fn locks_of_different_branches_are_independent() {
        let store = store();
        let first = block_on(open(&store));
        let other = block_on(MergeLock::open(
            store.clone(),
            run(),
            &BranchName::new("spec/5-other").unwrap(),
        ))
        .unwrap();
        block_on(first.enqueue("a")).unwrap();
        block_on(other.enqueue("b")).unwrap();
        assert_eq!(first.holder().as_deref(), Some("a"));
        assert_eq!(other.holder().as_deref(), Some("b"));
    }

    #[test]
    fn handles_of_the_same_branch_share_one_lock() {
        let store = store();
        let first = block_on(open(&store));
        let second = block_on(open(&store));
        assert!(Arc::ptr_eq(&first, &second));

        assert_eq!(block_on(first.acquire("a", &RUNNING)).unwrap(), 0);
        let mut waiting = Box::pin(second.acquire("b", &RUNNING));
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        assert_eq!(first.holder().as_deref(), Some("a"));
        assert_eq!(second.holder().as_deref(), Some("a"));
        assert_eq!(first.waiting(), ["b"]);

        // The persisted state keeps both owners, in order.
        drop(waiting);
        drop((first, second));
        let restored = block_on(open(&store));
        assert_eq!(restored.holder().as_deref(), Some("a"));
        assert_eq!(restored.waiting(), ["b"]);
    }

    #[test]
    fn a_waiter_returns_on_a_pause_and_keeps_its_place() {
        let store = store();
        let lock = block_on(open(&store));
        let policy = Policy::new(&Limits::default());
        block_on(lock.acquire("a", &policy)).unwrap();
        let mut pool = LocalPool::new();
        let waiter = {
            let (lock, policy) = (lock.clone(), &policy);
            async move { lock.acquire("b", policy).await }
        };
        let mut waiter = Box::pin(waiter);
        let mut context = Context::from_waker(Waker::noop());
        assert!(waiter.as_mut().poll(&mut context).is_pending());

        policy.pause(PauseReason::GlobalPause);
        assert!(matches!(
            pool.run_until(waiter),
            Err(PipelineError::Paused(PauseReason::GlobalPause))
        ));
        assert_eq!(lock.holder().as_deref(), Some("a"));
        assert_eq!(lock.waiting(), ["b"]);

        // Resumed: the waiter keeps its sequence number and gets the lock after the holder.
        block_on(lock.release("a")).unwrap();
        assert_eq!(block_on(lock.acquire("b", &RUNNING)).unwrap(), 1);
    }
}
