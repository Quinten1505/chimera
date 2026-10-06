use std::future::Future;

use chimera_core::RunId;
use chimera_core::run_store::RunStore;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::PipelineError;

/// Serializable state of a pipeline instance.
pub trait PipelineState: Serialize + DeserializeOwned {
    /// No further step will run.
    fn is_terminal(&self) -> bool;
    /// The pipeline is waiting to be resumed; the driver does not step it.
    fn is_paused(&self) -> bool;
}

/// A pipeline expressed as a state machine.
pub trait Pipeline: Sync {
    type State: PipelineState + Send;

    /// State of an instance that has never been saved.
    fn initial_state(&self) -> Self::State;

    /// Performs one external effect and returns the next state.
    fn step(
        &self,
        state: Self::State,
    ) -> impl Future<Output = Result<Self::State, PipelineError>> + Send;
}

/// Runs the pipeline instance `instance` of `run` until it reaches a terminal or paused state.
///
/// Resumes from the saved state if there is one. Every state returned by a step is saved before
/// the next step runs; if saving fails the error is returned and no further step runs.
pub async fn drive<P: Pipeline>(
    store: &dyn RunStore,
    run: &RunId,
    instance: &str,
    pipeline: &P,
) -> Result<P::State, PipelineError> {
    let mut state = match store.load_pipeline_state(run, instance).await? {
        Some(saved) => serde_json::from_value(saved)?,
        None => pipeline.initial_state(),
    };
    while !state.is_terminal() && !state.is_paused() {
        state = pipeline.step(state).await?;
        store
            .save_pipeline_state(run, instance, serde_json::to_value(&state)?)
            .await?;
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chimera_core::TurnResult;
    use chimera_core::error::PortError;
    use chimera_core::run_store::FakeRunStore;
    use futures_executor::block_on;
    use serde::Deserialize;

    use super::*;
    use crate::PauseReason;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum Toy {
        A,
        B,
        C,
        Done,
        Paused {
            reason: PauseReason,
            resume_at: Box<Toy>,
        },
    }

    impl PipelineState for Toy {
        fn is_terminal(&self) -> bool {
            matches!(self, Toy::Done)
        }
        fn is_paused(&self) -> bool {
            matches!(self, Toy::Paused { .. })
        }
    }

    /// Steps A -> B -> C -> Done, optionally pausing instead of leaving B; records every step run.
    #[derive(Default)]
    struct ToyPipeline {
        pause_at_b: bool,
        steps: Mutex<Vec<Toy>>,
    }

    impl Pipeline for ToyPipeline {
        type State = Toy;

        fn initial_state(&self) -> Toy {
            Toy::A
        }

        async fn step(&self, state: Toy) -> Result<Toy, PipelineError> {
            self.steps.lock().unwrap().push(state.clone());
            Ok(match state {
                Toy::A => Toy::B,
                Toy::B if self.pause_at_b => Toy::Paused {
                    reason: PauseReason::LimitExhausted,
                    resume_at: Box::new(Toy::C),
                },
                Toy::B => Toy::C,
                Toy::C => Toy::Done,
                other => other,
            })
        }
    }

    /// Wraps the fake store, logging saves and failing the nth one.
    struct RecordingStore {
        inner: FakeRunStore,
        fail_save_number: Option<usize>,
        saves: Mutex<Vec<serde_json::Value>>,
    }

    impl RecordingStore {
        fn new(fail_save_number: Option<usize>) -> Self {
            Self {
                inner: FakeRunStore::new(),
                fail_save_number,
                saves: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RunStore for RecordingStore {
        async fn save_pipeline_state(
            &self,
            run: &RunId,
            pipeline: &str,
            state: serde_json::Value,
        ) -> Result<(), PortError> {
            let number = {
                let mut saves = self.saves.lock().unwrap();
                saves.push(state.clone());
                saves.len()
            };
            if self.fail_save_number == Some(number) {
                return Err(PortError::failed("disk full"));
            }
            self.inner.save_pipeline_state(run, pipeline, state).await
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

        async fn load_effects(
            &self,
            run: &RunId,
        ) -> Result<Vec<chimera_core::run_store::EffectRecord>, PortError> {
            self.inner.load_effects(run).await
        }
    }

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn json(state: &Toy) -> serde_json::Value {
        serde_json::to_value(state).unwrap()
    }

    #[test]
    fn every_state_is_saved_before_the_next_step() {
        let store = RecordingStore::new(None);
        let pipeline = ToyPipeline::default();
        let end = block_on(drive(&store, &run(), "p", &pipeline)).unwrap();

        assert_eq!(end, Toy::Done);
        assert_eq!(
            *store.saves.lock().unwrap(),
            vec![json(&Toy::B), json(&Toy::C), json(&Toy::Done)]
        );
        assert_eq!(
            *pipeline.steps.lock().unwrap(),
            vec![Toy::A, Toy::B, Toy::C]
        );
        assert_eq!(
            block_on(store.load_pipeline_state(&run(), "p")).unwrap(),
            Some(json(&Toy::Done))
        );
    }

    #[test]
    fn resume_continues_without_rerunning_completed_steps() {
        let store = FakeRunStore::new();
        block_on(store.save_pipeline_state(&run(), "p", json(&Toy::C))).unwrap();
        let pipeline = ToyPipeline::default();

        let end = block_on(drive(&store, &run(), "p", &pipeline)).unwrap();

        assert_eq!(end, Toy::Done);
        assert_eq!(*pipeline.steps.lock().unwrap(), vec![Toy::C]);
    }

    #[test]
    fn other_instances_start_from_the_initial_state() {
        let store = FakeRunStore::new();
        block_on(store.save_pipeline_state(&run(), "p", json(&Toy::C))).unwrap();
        let pipeline = ToyPipeline::default();

        block_on(drive(&store, &run(), "q", &pipeline)).unwrap();

        assert_eq!(
            *pipeline.steps.lock().unwrap(),
            vec![Toy::A, Toy::B, Toy::C]
        );
    }

    #[test]
    fn paused_state_stops_the_loop_and_stays_paused_after_reload() {
        let store = FakeRunStore::new();
        let pipeline = ToyPipeline {
            pause_at_b: true,
            ..Default::default()
        };
        let paused = Toy::Paused {
            reason: PauseReason::LimitExhausted,
            resume_at: Box::new(Toy::C),
        };

        assert_eq!(
            block_on(drive(&store, &run(), "p", &pipeline)).unwrap(),
            paused
        );
        assert_eq!(pipeline.steps.lock().unwrap().len(), 2);

        assert_eq!(
            block_on(drive(&store, &run(), "p", &pipeline)).unwrap(),
            paused
        );
        assert_eq!(pipeline.steps.lock().unwrap().len(), 2);
    }

    #[test]
    fn save_failure_stops_the_loop() {
        let store = RecordingStore::new(Some(2));
        let pipeline = ToyPipeline::default();

        let error = block_on(drive(&store, &run(), "p", &pipeline)).unwrap_err();

        assert!(error.is_failed());
        assert_eq!(*pipeline.steps.lock().unwrap(), vec![Toy::A, Toy::B]);
        assert_eq!(
            block_on(store.load_pipeline_state(&run(), "p")).unwrap(),
            Some(json(&Toy::B))
        );
    }
}
