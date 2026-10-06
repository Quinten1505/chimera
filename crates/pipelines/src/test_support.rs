//! Test doubles shared by the pipeline tests.

use std::sync::Mutex;

use async_trait::async_trait;
use chimera_core::error::PortError;
use chimera_core::run_store::{EffectRecord, FakeRunStore, RunStore};
use chimera_core::{RunId, TurnResult};

/// Wraps the fake store; the write (state save, history append or effect record) with the armed
/// number fails without storing anything, as if the process stopped right before it.
#[derive(Default)]
pub struct CrashingStore {
    inner: FakeRunStore,
    writes: Mutex<usize>,
    crash_at: Mutex<Option<usize>>,
}

#[async_trait]
impl RunStore for CrashingStore {
    async fn save_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
        state: serde_json::Value,
    ) -> Result<(), PortError> {
        self.write()?;
        self.inner.save_pipeline_state(run, pipeline, state).await
    }
    async fn load_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
    ) -> Result<Option<serde_json::Value>, PortError> {
        self.inner.load_pipeline_state(run, pipeline).await
    }
    async fn save_run_data(&self, run: &RunId, data: serde_json::Value) -> Result<(), PortError> {
        self.inner.save_run_data(run, data).await
    }
    async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
        self.inner.load_run_data(run).await
    }
    async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
        self.write()?;
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
        self.write()?;
        self.inner.record_effect_intent(run, key, intent).await
    }
    async fn record_effect_outcome(
        &self,
        run: &RunId,
        key: &str,
        outcome: &str,
    ) -> Result<(), PortError> {
        self.write()?;
        self.inner.record_effect_outcome(run, key, outcome).await
    }
    async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
        self.inner.load_effects(run).await
    }
}

impl CrashingStore {
    /// Counts a write; fails the armed one.
    fn write(&self) -> Result<(), PortError> {
        let number = {
            let mut writes = self.writes.lock().unwrap();
            *writes += 1;
            *writes
        };
        if *self.crash_at.lock().unwrap() == Some(number) {
            return Err(PortError::failed("process stopped"));
        }
        Ok(())
    }

    /// Arms the write with this number, counted from the first write, or disarms.
    pub fn crash_at(&self, save: Option<usize>) {
        *self.crash_at.lock().unwrap() = save;
    }

    /// Writes so far, failed ones included.
    pub fn writes(&self) -> usize {
        *self.writes.lock().unwrap()
    }
}
