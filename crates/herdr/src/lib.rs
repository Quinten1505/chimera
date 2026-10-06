//! Herdr client and session tracking.

mod agent;
mod herdr;
mod session;

pub use agent::{Agent, AgentSessionReference};

pub use herdr::{
    CreatedWorkspace, HerdrClient, HerdrError, Pane, PaneOptions, SplitDirection, Tab, Workspace,
    WorkspaceOptions,
};

pub use session::{Session, SessionTarget, WorkspacePanes};
