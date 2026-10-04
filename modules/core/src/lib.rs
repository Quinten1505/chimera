//! Shared application concepts and the Herdr integration.

mod agent;
mod herdr;
mod session;

pub use agent::{Agent, AgentSessionReference};

pub use herdr::{
    CreatedWorkspace, CreatedWorktree, HerdrClient, HerdrError, Pane, PaneOptions, SplitDirection,
    Tab, Workspace, WorkspaceOptions, Worktree, WorktreeOptions, WorktreeSource,
};

pub use session::{Session, SessionTarget, WorkspacePanes};
