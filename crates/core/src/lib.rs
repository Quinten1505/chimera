//! Domain types and ports only. Performs no I/O: no sockets, processes, or file system access.

pub mod error;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The part an agent plays within a triplet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    Implementation,
    Review,
    Merge,
}

fn default_reset_command() -> String {
    "/clear".to_string()
}

/// How one role's agent is launched and prompted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentProfile {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub settings: BTreeMap<String, serde_json::Value>,
    pub prompt_template: String,
    #[serde(default = "default_reset_command")]
    pub reset_command: String,
}

impl AgentProfile {
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        prompt_template: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            settings: BTreeMap::new(),
            prompt_template: prompt_template.into(),
            reset_command: default_reset_command(),
        }
    }
}

/// Exactly one profile per [`Role`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentConfiguration {
    pub implementation: AgentProfile,
    pub review: AgentProfile,
    pub merge: AgentProfile,
}

impl AgentConfiguration {
    pub fn profile(&self, role: Role) -> &AgentProfile {
        match role {
            Role::Implementation => &self.implementation,
            Role::Review => &self.review,
            Role::Merge => &self.merge,
        }
    }
}

/// Attempt limits; see functional.md for what happens when each is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub implementation_review_cycles: u32,
    pub merge_attempts: u32,
    pub final_review_fix_cycles: u32,
    pub agent_recovery: u32,
    pub github_retries: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            implementation_review_cycles: 100,
            merge_attempts: 100,
            final_review_fix_cycles: 100,
            agent_recovery: 5,
            github_retries: 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> AgentConfiguration {
        AgentConfiguration {
            implementation: AgentProfile::new("codex", "m1", "implement"),
            review: AgentProfile::new("codex", "m2", "review"),
            merge: AgentProfile::new("claude", "m3", "merge"),
        }
    }

    #[test]
    fn limits_default_matches_functional_spec() {
        let limits = Limits::default();
        assert_eq!(limits.implementation_review_cycles, 100);
        assert_eq!(limits.merge_attempts, 100);
        assert_eq!(limits.final_review_fix_cycles, 100);
        assert_eq!(limits.agent_recovery, 5);
        assert_eq!(limits.github_retries, 5);
    }

    #[test]
    fn reset_command_defaults_to_clear() {
        assert_eq!(AgentProfile::new("p", "m", "t").reset_command, "/clear");
        let profile: AgentProfile =
            serde_json::from_str(r#"{"provider":"p","model":"m","prompt_template":"t"}"#).unwrap();
        assert_eq!(profile.reset_command, "/clear");
    }

    #[test]
    fn lookup_by_role() {
        let configuration = configuration();
        assert_eq!(configuration.profile(Role::Implementation).model, "m1");
        assert_eq!(configuration.profile(Role::Review).model, "m2");
        assert_eq!(configuration.profile(Role::Merge).model, "m3");
    }

    #[test]
    fn serde_round_trip() {
        let mut configuration = configuration();
        configuration
            .review
            .settings
            .insert("effort".into(), serde_json::json!("high"));
        let json = serde_json::to_string(&configuration).unwrap();
        assert_eq!(
            serde_json::from_str::<AgentConfiguration>(&json).unwrap(),
            configuration
        );

        let limits = Limits::default();
        let json = serde_json::to_string(&limits).unwrap();
        assert_eq!(serde_json::from_str::<Limits>(&json).unwrap(), limits);
        let json = serde_json::to_string(&Role::Merge).unwrap();
        assert_eq!(serde_json::from_str::<Role>(&json).unwrap(), Role::Merge);
    }
}
