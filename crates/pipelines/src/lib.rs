//! The four orchestration pipelines (feature, ticket, implementation and PR review) and the
//! shared services they use, expressed in terms of the core domain.
//!
//! Expose the module interface here; keep implementation submodules private.

mod agent_turn;
mod driver;
mod environment;
mod error;
mod feature;
mod implementation;
mod merge_lock;
mod policy;
mod ticket;

pub use agent_turn::{AgentTurns, CompletedTurn, TurnError, TurnRequest};
pub use driver::{Pipeline, PipelineState, drive};
pub use environment::{
    AgentLaunch, Environment, EnvironmentAgent, EnvironmentService, ProvisionSpec,
};
pub use error::{PauseReason, PipelineError};
pub use feature::{FeaturePipeline, FeatureState};
pub use implementation::{ImplementationPipeline, ImplementationState};
pub use merge_lock::{MergeLock, Sequence};
pub use policy::{Budget, Policy, PolicyState, RetryRefused};
pub use ticket::{TicketEntry, TicketPause, TicketPipeline, TicketProgress, TicketState};
