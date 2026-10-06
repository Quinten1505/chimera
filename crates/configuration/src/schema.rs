//! The Chimera YAML schema: provider sections, limits, and the ticket and final agent configurations.

use std::collections::BTreeMap;
use std::{fs, path::Path};

use chimera_core::{AgentConfiguration, AgentProfile, Limits};
use serde::Deserialize;

use crate::{ConfigurationError, codex_args::validate_settings};

const DEFAULT_RESET_COMMAND: &str = "/clear";

/// Resolved configuration: agents for ticket and final pipelines, plus attempt limits.
#[derive(Debug, Clone, PartialEq)]
pub struct Configuration {
    pub ticket: AgentConfiguration,
    pub final_review: AgentConfiguration,
    pub limits: Limits,
}

impl Configuration {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        Self::from_yaml(&fs::read_to_string(path)?)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigurationError> {
        let file: File = serde_yaml::from_str(yaml)?;
        let configuration = file.resolve();
        configuration.validate_codex_settings()?;
        Ok(configuration)
    }
}

impl Configuration {
    /// Rejects unknown or ill-typed Codex settings, naming the field path.
    fn validate_codex_settings(&self) -> Result<(), ConfigurationError> {
        for (section, agents) in [("ticket", &self.ticket), ("final", &self.final_review)] {
            for (role, profile) in [
                ("implementation", &agents.implementation),
                ("review", &agents.review),
                ("merge", &agents.merge),
            ] {
                validate_settings(profile, &format!("{section}.{role}"))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    providers: BTreeMap<String, ProviderSection>,
    #[serde(default)]
    limits: LimitsSection,
    ticket: AgentsSection,
    #[serde(rename = "final")]
    final_review: AgentsSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderSection {
    reset_command: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsSection {
    implementation_review_cycles: Option<u32>,
    merge_attempts: Option<u32>,
    final_review_fix_cycles: Option<u32>,
    agent_recovery: Option<u32>,
    github_retries: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentsSection {
    implementation: ProfileSection,
    review: ProfileSection,
    merge: ProfileSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileSection {
    provider: String,
    model: String,
    #[serde(default)]
    settings: BTreeMap<String, serde_json::Value>,
    prompt_template: String,
}

impl File {
    fn resolve(self) -> Configuration {
        let Self {
            providers,
            limits,
            ticket,
            final_review,
        } = self;
        Configuration {
            ticket: ticket.resolve(&providers),
            final_review: final_review.resolve(&providers),
            limits: limits.resolve(),
        }
    }
}

impl AgentsSection {
    fn resolve(self, providers: &BTreeMap<String, ProviderSection>) -> AgentConfiguration {
        AgentConfiguration {
            implementation: self.implementation.resolve(providers),
            review: self.review.resolve(providers),
            merge: self.merge.resolve(providers),
        }
    }
}

impl ProfileSection {
    fn resolve(self, providers: &BTreeMap<String, ProviderSection>) -> AgentProfile {
        let reset_command = providers
            .get(&self.provider)
            .and_then(|section| section.reset_command.clone())
            .unwrap_or_else(|| DEFAULT_RESET_COMMAND.to_string());
        AgentProfile {
            provider: self.provider,
            model: self.model,
            settings: self.settings,
            prompt_template: self.prompt_template,
            reset_command,
        }
    }
}

impl LimitsSection {
    fn resolve(self) -> Limits {
        let defaults = Limits::default();
        Limits {
            implementation_review_cycles: self
                .implementation_review_cycles
                .unwrap_or(defaults.implementation_review_cycles),
            merge_attempts: self.merge_attempts.unwrap_or(defaults.merge_attempts),
            final_review_fix_cycles: self
                .final_review_fix_cycles
                .unwrap_or(defaults.final_review_fix_cycles),
            agent_recovery: self.agent_recovery.unwrap_or(defaults.agent_recovery),
            github_retries: self.github_retries.unwrap_or(defaults.github_retries),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../../../chimera.example.yaml");

    const MINIMAL: &str = "
ticket:
  implementation: { provider: codex, model: m1, prompt_template: implement }
  review: { provider: codex, model: m2, prompt_template: review }
  merge: { provider: claude, model: m3, prompt_template: merge }
final:
  implementation: { provider: codex, model: m1, prompt_template: implement }
  review: { provider: codex, model: m2, prompt_template: review }
  merge: { provider: claude, model: m3, prompt_template: merge }
";

    #[test]
    fn parses_example_file() {
        let configuration = Configuration::from_yaml(EXAMPLE).unwrap();
        assert_eq!(configuration.ticket.implementation.provider, "codex");
        assert_eq!(configuration.ticket.merge.provider, "claude");
        assert_eq!(configuration.final_review.review.provider, "codex");
        assert_eq!(configuration.ticket.implementation.reset_command, "/clear");
        assert_eq!(configuration.ticket.merge.reset_command, "/clear");
        assert_eq!(configuration.limits, Limits::default());
        assert!(
            configuration
                .ticket
                .implementation
                .settings
                .contains_key("reasoning_effort")
        );
    }

    #[test]
    fn parses_full_file() {
        let yaml = format!(
            "providers:
  codex: {{ reset_command: /new }}
  claude: {{}}
limits:
  implementation_review_cycles: 1
  merge_attempts: 2
  final_review_fix_cycles: 3
  agent_recovery: 4
  github_retries: 6
{MINIMAL}"
        )
        .replace("model: m3,", "model: m3, settings: {effort: high, n: 2},");
        let configuration = Configuration::from_yaml(&yaml).unwrap();
        let profile = &configuration.final_review.merge;
        assert_eq!(profile.reset_command, "/clear");
        assert_eq!(configuration.ticket.implementation.reset_command, "/new");
        assert_eq!(profile.settings["effort"], "high");
        assert_eq!(profile.settings["n"], 2);
        assert_eq!(configuration.ticket.merge.reset_command, "/clear");
        assert_eq!(
            configuration.limits,
            Limits {
                implementation_review_cycles: 1,
                merge_attempts: 2,
                final_review_fix_cycles: 3,
                agent_recovery: 4,
                github_retries: 6,
            }
        );
    }

    #[test]
    fn applies_defaults() {
        let configuration = Configuration::from_yaml(MINIMAL).unwrap();
        assert_eq!(configuration.limits, Limits::default());
        assert_eq!(configuration.ticket.review.reset_command, "/clear");

        let partial = format!("limits:\n  merge_attempts: 7\n{MINIMAL}");
        let limits = Configuration::from_yaml(&partial).unwrap().limits;
        assert_eq!(limits.merge_attempts, 7);
        assert_eq!(
            limits,
            Limits {
                merge_attempts: 7,
                ..Limits::default()
            }
        );

        let empty_provider = format!("providers:\n  codex: {{}}\n{MINIMAL}");
        let configuration = Configuration::from_yaml(&empty_provider).unwrap();
        assert_eq!(configuration.ticket.implementation.reset_command, "/clear");
    }

    #[test]
    fn rejects_unknown_fields() {
        for yaml in [
            format!("typo: 1\n{MINIMAL}"),
            format!("limits:\n  typo: 1\n{MINIMAL}"),
            format!("providers:\n  codex:\n    typo: 1\n{MINIMAL}"),
            MINIMAL.replace("prompt_template: merge", "prompt_template: merge, typo: 1"),
            // test policy lives in prompt templates, not in configuration fields
            MINIMAL.replace("model: m1", "model: m1, test_requirements: all"),
        ] {
            assert!(Configuration::from_yaml(&yaml).is_err(), "{yaml}");
        }
    }

    #[test]
    fn rejects_missing_sections() {
        assert!(Configuration::from_yaml("limits: {}").is_err());
        assert!(
            Configuration::from_yaml(&MINIMAL.replace(
                "  merge: { provider: claude, model: m3, prompt_template: merge }\n",
                ""
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_bad_codex_settings_with_field_path() {
        for (settings, path) in [
            ("{service_tier: 3}", "ticket.review.settings.service_tier"),
            ("{typo: true}", "ticket.review.settings.typo"),
            (
                "{approve_for_me: maybe}",
                "ticket.review.settings.approve_for_me",
            ),
        ] {
            let yaml = MINIMAL.replacen(
                "model: m2,",
                &format!("model: m2, settings: {settings},"),
                1,
            );
            let message = Configuration::from_yaml(&yaml).unwrap_err().to_string();
            assert!(message.contains(path), "{message}");
        }
    }
}
