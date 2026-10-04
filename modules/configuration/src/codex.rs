/// Launch settings for a Codex agent.
///
/// The composition root passes these settings to the agent launcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexOptions {
    /// Model identifier, for example "gpt-6-luna".
    pub model: String,
    /// Requested reasoning effort, for example "medium".
    pub reasoning_effort: String,
    /// Requested service tier, for example "fast".
    pub service_tier: String,
    /// Route approvals through automatic review with the workspace-write sandbox.
    pub approve_for_me: bool,
}
