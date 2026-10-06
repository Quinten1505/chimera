//! The four orchestration pipelines (feature, ticket, implementation and PR review) and the
//! shared services they use, expressed in terms of the core domain.
//!
//! Expose the module interface here; keep implementation submodules private.

mod driver;
mod environment;
mod error;
mod merge_lock;
mod policy;

pub use driver::{Pipeline, PipelineState, drive};
pub use environment::{
    AgentLaunch, Environment, EnvironmentAgent, EnvironmentService, ProvisionSpec,
};
pub use error::{PauseReason, PipelineError};
pub use merge_lock::{MergeLock, Sequence};
pub use policy::{Budget, Policy, PolicyState, RetryRefused};
