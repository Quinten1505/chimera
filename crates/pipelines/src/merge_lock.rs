use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::task::{Poll, Waker};

use chimera_core::error::PortError;
use chimera_core::run_store::RunStore;
use chimera_core::{BranchName, RunId};
use futures_util::lock::Mutex as AsyncMutex;
use serde::{Deserialize, Serialize};

use crate::error::PipelineError;

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
        let mut state = self.snapshot();
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

    /// Enqueues `owner` and waits until it holds the lock.
    pub async fn acquire(&self, owner: &str) -> Result<Sequence, PipelineError> {
        let sequence = self.enqueue(owner).await?;
        std::future::poll_fn(|context| {
            let mut shared = self.shared.lock().unwrap();
            if shared
                .state
                .holder
                .as_ref()
                .is_some_and(|h| h.owner == owner)
            {
                Poll::Ready(())
            } else {
                shared.wakers.push(context.waker().clone());
                Poll::Pending
            }
        })
        .await;
        Ok(sequence)
    }

    /// Releases the lock after a verified push and grants it to the next waiter. Fails if
    /// `owner` is not the holder.
    pub async fn release(&self, owner: &str) -> Result<(), PipelineError> {
        let _write = self.writes.lock().await;
        let mut state = self.snapshot();
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

    fn snapshot(&self) -> LockState {
        self.shared.lock().unwrap().state.clone()
    }

    /// Saves `state`, then makes it current and wakes the waiters. Callers hold `writes`.
    async fn commit(&self, state: LockState) -> Result<(), PipelineError> {
        self.store
            .save_pipeline_state(&self.run, &self.instance, serde_json::to_value(&state)?)
            .await?;
        let mut shared = self.shared.lock().unwrap();
        shared.state = state;
        shared.wakers.drain(..).for_each(Waker::wake);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Waker};

    use chimera_core::run_store::FakeRunStore;
    use futures_executor::{LocalPool, block_on};
    use futures_util::task::LocalSpawnExt;

    use super::*;

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
        block_on(lock.acquire("w0")).unwrap();
        for index in 1..=5 {
            block_on(lock.enqueue(&format!("w{index}"))).unwrap();
        }
        // Spawn the waiters in reverse so wake order cannot explain the grant order.
        for index in (1..=5).rev() {
            let (lock, granted) = (lock.clone(), granted.clone());
            spawner
                .spawn_local(async move {
                    let owner = format!("w{index}");
                    lock.acquire(&owner).await.unwrap();
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

        let mut waiting = pin!(lock.acquire("b"));
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
            let mut holding = Box::pin(lock.acquire("a"));
            assert!(holding.as_mut().poll(&mut context).is_ready());
            let mut waiting = Box::pin(lock.acquire("b"));
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

        assert_eq!(block_on(first.acquire("a")).unwrap(), 0);
        let mut waiting = Box::pin(second.acquire("b"));
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
}
