use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::PortError;
use crate::{RunId, TurnResult};

/// An external effect, recorded as intent before it runs and as outcome after. An effect with an
/// intent but no outcome is uncertain on restart; one with an outcome is completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecord {
    /// Identifies the effect within its run.
    pub key: String,
    pub intent: String,
    pub outcome: Option<String>,
}

/// Durable storage for run state. Pipeline state and run data cross this port in serialized form
/// so the trait stays object-safe.
#[async_trait]
pub trait RunStore: Send + Sync {
    /// Replaces the state of the pipeline instance `pipeline`.
    async fn save_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
        state: serde_json::Value,
    ) -> Result<(), PortError>;

    async fn load_pipeline_state(
        &self,
        run: &RunId,
        pipeline: &str,
    ) -> Result<Option<serde_json::Value>, PortError>;

    /// Replaces the run data of `run`.
    async fn save_run_data(&self, run: &RunId, data: serde_json::Value) -> Result<(), PortError>;

    async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError>;

    /// Appends a completed turn to the history of `run`.
    async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError>;

    async fn load_history(&self, run: &RunId) -> Result<Vec<TurnResult>, PortError>;

    /// Records that the effect `key` is about to run.
    async fn record_effect_intent(
        &self,
        run: &RunId,
        key: &str,
        intent: &str,
    ) -> Result<(), PortError>;

    /// Records the outcome of an effect whose intent was recorded.
    async fn record_effect_outcome(
        &self,
        run: &RunId,
        key: &str,
        outcome: &str,
    ) -> Result<(), PortError>;

    /// All effects of `run` in the order their intents were recorded.
    async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError>;
}

#[cfg(any(test, feature = "testing"))]
pub use fake::FakeRunStore;

#[cfg(any(test, feature = "testing"))]
mod fake {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::{EffectRecord, RunStore};
    use crate::error::PortError;
    use crate::{RunId, TurnResult};

    #[derive(Default)]
    struct State {
        pipelines: HashMap<(RunId, String), serde_json::Value>,
        run_data: HashMap<RunId, serde_json::Value>,
        history: HashMap<RunId, Vec<TurnResult>>,
        effects: HashMap<RunId, Vec<EffectRecord>>,
    }

    /// In-memory [`RunStore`] that exposes what was saved for assertions.
    #[derive(Default)]
    pub struct FakeRunStore {
        state: Mutex<State>,
    }

    impl FakeRunStore {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn pipeline_states(&self) -> HashMap<(RunId, String), serde_json::Value> {
            self.state.lock().unwrap().pipelines.clone()
        }

        pub fn history(&self, run: &RunId) -> Vec<TurnResult> {
            self.state
                .lock()
                .unwrap()
                .history
                .get(run)
                .cloned()
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl RunStore for FakeRunStore {
        async fn save_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
            state: serde_json::Value,
        ) -> Result<(), PortError> {
            let key = (run.clone(), pipeline.to_string());
            self.state.lock().unwrap().pipelines.insert(key, state);
            Ok(())
        }

        async fn load_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
        ) -> Result<Option<serde_json::Value>, PortError> {
            let key = (run.clone(), pipeline.to_string());
            Ok(self.state.lock().unwrap().pipelines.get(&key).cloned())
        }

        async fn save_run_data(
            &self,
            run: &RunId,
            data: serde_json::Value,
        ) -> Result<(), PortError> {
            self.state
                .lock()
                .unwrap()
                .run_data
                .insert(run.clone(), data);
            Ok(())
        }

        async fn load_run_data(&self, run: &RunId) -> Result<Option<serde_json::Value>, PortError> {
            Ok(self.state.lock().unwrap().run_data.get(run).cloned())
        }

        async fn append_turn(&self, run: &RunId, turn: TurnResult) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.history.entry(run.clone()).or_default().push(turn);
            Ok(())
        }

        async fn load_history(&self, run: &RunId) -> Result<Vec<TurnResult>, PortError> {
            Ok(self.history(run))
        }

        async fn record_effect_intent(
            &self,
            run: &RunId,
            key: &str,
            intent: &str,
        ) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state
                .effects
                .entry(run.clone())
                .or_default()
                .push(EffectRecord {
                    key: key.to_string(),
                    intent: intent.to_string(),
                    outcome: None,
                });
            Ok(())
        }

        async fn record_effect_outcome(
            &self,
            run: &RunId,
            key: &str,
            outcome: &str,
        ) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            let record = state
                .effects
                .get_mut(run)
                .and_then(|effects| effects.iter_mut().find(|effect| effect.key == key))
                .ok_or_else(|| PortError::failed(format!("no intent recorded for effect {key}")))?;
            record.outcome = Some(outcome.to_string());
            Ok(())
        }

        async fn load_effects(&self, run: &RunId) -> Result<Vec<EffectRecord>, PortError> {
            let state = self.state.lock().unwrap();
            Ok(state.effects.get(run).cloned().unwrap_or_default())
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use futures_executor::block_on;

        use super::*;
        use crate::{AgentId, Outcome, Role};

        fn run() -> RunId {
            RunId::new("run-1").unwrap()
        }

        fn turn(explanation: &str) -> TurnResult {
            TurnResult {
                agent: AgentId::new("a1").unwrap(),
                role: Role::Review,
                outcome: Outcome::ReviewApproved(explanation.into()),
            }
        }

        #[test]
        fn state_and_run_data_round_trip() {
            let fake = FakeRunStore::new();
            let store: &dyn RunStore = &fake;
            assert_eq!(
                block_on(store.load_pipeline_state(&run(), "p1")).unwrap(),
                None
            );
            assert_eq!(block_on(store.load_run_data(&run())).unwrap(), None);

            let state = serde_json::json!({"step": 3});
            block_on(store.save_pipeline_state(&run(), "p1", state.clone())).unwrap();
            block_on(store.save_run_data(&run(), serde_json::json!({"k": "v"}))).unwrap();

            assert_eq!(
                block_on(store.load_pipeline_state(&run(), "p1")).unwrap(),
                Some(state.clone())
            );
            assert_eq!(
                block_on(store.load_pipeline_state(&run(), "p2")).unwrap(),
                None
            );
            assert_eq!(
                block_on(store.load_run_data(&run())).unwrap(),
                Some(serde_json::json!({"k": "v"}))
            );
            assert_eq!(fake.pipeline_states()[&(run(), "p1".to_string())], state);
        }

        #[test]
        fn history_keeps_append_order() {
            let store: Arc<dyn RunStore> = Arc::new(FakeRunStore::new());
            for name in ["first", "second", "third"] {
                block_on(store.append_turn(&run(), turn(name))).unwrap();
            }
            assert_eq!(
                block_on(store.load_history(&run())).unwrap(),
                vec![turn("first"), turn("second"), turn("third")]
            );
        }

        #[test]
        fn fake_exposes_history() {
            let fake = FakeRunStore::new();
            block_on(fake.append_turn(&run(), turn("only"))).unwrap();
            assert_eq!(fake.history(&run()), vec![turn("only")]);
        }

        #[test]
        fn effect_intent_is_uncertain_until_outcome_recorded() {
            let store: Arc<dyn RunStore> = Arc::new(FakeRunStore::new());
            block_on(store.record_effect_intent(&run(), "push", "push feat")).unwrap();
            block_on(store.record_effect_intent(&run(), "pr", "open pr")).unwrap();
            block_on(store.record_effect_outcome(&run(), "push", "pushed c1")).unwrap();

            let effects = block_on(store.load_effects(&run())).unwrap();
            assert_eq!(effects[0].outcome.as_deref(), Some("pushed c1"));
            assert_eq!(effects[1].key, "pr");
            assert_eq!(effects[1].outcome, None);
        }

        #[test]
        fn outcome_without_intent_fails() {
            let fake = FakeRunStore::new();
            let error = block_on(fake.record_effect_outcome(&run(), "x", "o")).unwrap_err();
            assert!(error.is_failed());
        }
    }
}
