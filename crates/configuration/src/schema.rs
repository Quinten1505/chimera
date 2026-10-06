//! The Chimera YAML schema: provider sections, limits, and the ticket and final agent configurations.

use std::collections::BTreeMap;
use std::{fs, path::Path};

use chimera_core::{AgentConfiguration, AgentProfile, Limits};
use serde::Deserialize;

use crate::{
    ConfigurationError,
    codex_args::{CODEX_PROVIDER, validate_settings},
    error::is_plain_text,
};

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
        let configuration = file.resolve()?;
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
    ticket: Option<AgentsSection>,
    #[serde(rename = "final")]
    final_review: Option<AgentsSection>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderSection {
    reset_command: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsSection {
    #[serde(default, deserialize_with = "present")]
    implementation_review_cycles: Option<serde_yaml::Value>,
    #[serde(default, deserialize_with = "present")]
    merge_attempts: Option<serde_yaml::Value>,
    #[serde(default, deserialize_with = "present")]
    final_review_fix_cycles: Option<serde_yaml::Value>,
    #[serde(default, deserialize_with = "present")]
    agent_recovery: Option<serde_yaml::Value>,
    #[serde(default, deserialize_with = "present")]
    github_retries: Option<serde_yaml::Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentsSection {
    implementation: Option<ProfileSection>,
    review: Option<ProfileSection>,
    merge: Option<ProfileSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileSection {
    provider: Option<String>,
    model: Option<String>,
    #[serde(default)]
    settings: BTreeMap<String, serde_json::Value>,
    prompt_template: Option<String>,
}

/// Keeps an explicit `null` distinct from an absent key, so `null` is rejected as an invalid limit.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_yaml::Value>, D::Error> {
    serde_yaml::Value::deserialize(deserializer).map(Some)
}

fn invalid(field: &str) -> ConfigurationError {
    ConfigurationError::Invalid {
        field: field.to_string(),
    }
}

fn missing(field: &str) -> ConfigurationError {
    ConfigurationError::Missing {
        field: field.to_string(),
    }
}

/// A required single-line string: nonempty and free of control characters.
fn plain_string(value: Option<String>, field: &str) -> Result<String, ConfigurationError> {
    let value = value.ok_or_else(|| missing(field))?;
    if !is_plain_text(&value) {
        return Err(invalid(field));
    }
    Ok(value)
}

/// Every string inside a settings value, at any depth, must be plain text; `field` is the value's path.
fn validate_setting_strings(
    value: &serde_json::Value,
    field: &str,
) -> Result<(), ConfigurationError> {
    use serde_json::Value;
    match value {
        Value::String(text) if !is_plain_text(text) => Err(invalid(field)),
        Value::Array(items) => items.iter().enumerate().try_for_each(|(index, item)| {
            validate_setting_strings(item, &format!("{field}[{index}]"))
        }),
        Value::Object(map) => map
            .iter()
            .try_for_each(|(key, item)| validate_setting_strings(item, &format!("{field}.{key}"))),
        _ => Ok(()),
    }
}

/// A required prompt template: nonempty, and free of control characters except
/// line feed, carriage return and tab.
///
/// Per the clarified spec (issue #21), `prompt_template` is the only string
/// exempt from the "no control characters" rule, and only for `\n`, `\r` and
/// `\t`, because templates are multi-line. Every other control character
/// (e.g. NUL, ESC) is rejected with the field path.
fn template_string(value: Option<String>, field: &str) -> Result<String, ConfigurationError> {
    let value = value.ok_or_else(|| missing(field))?;
    if value.trim().is_empty()
        || value
            .chars()
            .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
    {
        return Err(invalid(field));
    }
    Ok(value)
}

impl File {
    fn resolve(self) -> Result<Configuration, ConfigurationError> {
        let Self {
            providers,
            limits,
            ticket,
            final_review,
        } = self;
        let mut reset_commands = BTreeMap::new();
        for (name, section) in providers {
            if name != CODEX_PROVIDER {
                return Err(ConfigurationError::UnsupportedProvider {
                    field: format!("providers.{name}"),
                    provider: name,
                });
            }
            if let Some(command) = section.reset_command {
                let field = format!("providers.{name}.reset_command");
                reset_commands.insert(name, plain_string(Some(command), &field)?);
            }
        }
        Ok(Configuration {
            ticket: ticket
                .unwrap_or_default()
                .resolve("ticket", &reset_commands)?,
            final_review: final_review
                .unwrap_or_default()
                .resolve("final", &reset_commands)?,
            limits: limits.resolve()?,
        })
    }
}

impl AgentsSection {
    fn resolve(
        self,
        prefix: &str,
        reset_commands: &BTreeMap<String, String>,
    ) -> Result<AgentConfiguration, ConfigurationError> {
        let role = |profile: Option<ProfileSection>, name: &str| {
            let field = format!("{prefix}.{name}");
            profile
                .ok_or_else(|| missing(&field))?
                .resolve(&field, reset_commands)
        };
        Ok(AgentConfiguration {
            implementation: role(self.implementation, "implementation")?,
            review: role(self.review, "review")?,
            merge: role(self.merge, "merge")?,
        })
    }
}

impl ProfileSection {
    fn resolve(
        self,
        field: &str,
        reset_commands: &BTreeMap<String, String>,
    ) -> Result<AgentProfile, ConfigurationError> {
        let provider = self
            .provider
            .ok_or_else(|| missing(&format!("{field}.provider")))?;
        if provider != CODEX_PROVIDER {
            return Err(ConfigurationError::UnsupportedProvider {
                field: format!("{field}.provider"),
                provider,
            });
        }
        let reset_command = reset_commands
            .get(&provider)
            .cloned()
            .unwrap_or_else(|| DEFAULT_RESET_COMMAND.to_string());
        for (key, value) in &self.settings {
            validate_setting_strings(value, &format!("{field}.settings.{key}"))?;
        }
        Ok(AgentProfile {
            model: plain_string(self.model, &format!("{field}.model"))?,
            settings: self.settings,
            prompt_template: template_string(
                self.prompt_template,
                &format!("{field}.prompt_template"),
            )?,
            provider,
            reset_command,
        })
    }
}

fn limit(
    value: Option<serde_yaml::Value>,
    name: &str,
    default: u32,
) -> Result<u32, ConfigurationError> {
    let Some(value) = value else {
        return Ok(default);
    };
    value
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .filter(|&n| n > 0)
        .ok_or_else(|| ConfigurationError::InvalidLimit {
            field: format!("limits.{name}"),
        })
}

impl LimitsSection {
    fn resolve(self) -> Result<Limits, ConfigurationError> {
        let d = Limits::default();
        Ok(Limits {
            implementation_review_cycles: limit(
                self.implementation_review_cycles,
                "implementation_review_cycles",
                d.implementation_review_cycles,
            )?,
            merge_attempts: limit(self.merge_attempts, "merge_attempts", d.merge_attempts)?,
            final_review_fix_cycles: limit(
                self.final_review_fix_cycles,
                "final_review_fix_cycles",
                d.final_review_fix_cycles,
            )?,
            agent_recovery: limit(self.agent_recovery, "agent_recovery", d.agent_recovery)?,
            github_retries: limit(self.github_retries, "github_retries", d.github_retries)?,
        })
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
  merge: { provider: codex, model: m3, prompt_template: merge }
final:
  implementation: { provider: codex, model: m1, prompt_template: implement }
  review: { provider: codex, model: m2, prompt_template: review }
  merge: { provider: codex, model: m3, prompt_template: merge }
";

    #[test]
    fn parses_example_file() {
        let configuration = Configuration::from_yaml(EXAMPLE).unwrap();
        assert_eq!(configuration.ticket.implementation.provider, "codex");
        assert_eq!(configuration.ticket.merge.provider, "codex");
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
limits:
  implementation_review_cycles: 1
  merge_attempts: 2
  final_review_fix_cycles: 3
  agent_recovery: 4
  github_retries: 6
{MINIMAL}"
        )
        .replace(
            "model: m3,",
            "model: m3, settings: {reasoning_effort: high, approve_for_me: true},",
        );
        let configuration = Configuration::from_yaml(&yaml).unwrap();
        let profile = &configuration.final_review.merge;
        assert_eq!(profile.reset_command, "/new");
        assert_eq!(configuration.ticket.implementation.reset_command, "/new");
        assert_eq!(profile.settings["reasoning_effort"], "high");
        assert_eq!(profile.settings["approve_for_me"], true);
        assert_eq!(configuration.ticket.merge.reset_command, "/new");
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
                "  merge: { provider: codex, model: m3, prompt_template: merge }\n",
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

    fn err(yaml: &str) -> String {
        Configuration::from_yaml(yaml).unwrap_err().to_string()
    }

    #[test]
    fn rejects_missing_role_naming_it() {
        let yaml = MINIMAL.replacen(
            "  merge: { provider: codex, model: m3, prompt_template: merge }\n",
            "",
            2,
        );
        assert!(err(&yaml).contains("ticket.merge"), "{}", err(&yaml));
        let last = MINIMAL.rsplit_once("  merge:").unwrap().0;
        assert!(err(last).contains("final.merge"), "{}", err(last));
        assert!(err("limits: {}").contains("ticket.implementation"));
    }

    #[test]
    fn rejects_missing_or_empty_prompt_template() {
        let missing = MINIMAL.replacen(", prompt_template: review", "", 1);
        assert!(err(&missing).contains("ticket.review.prompt_template"));
        let empty = MINIMAL.replacen("prompt_template: merge", "prompt_template: '  '", 1);
        assert!(err(&empty).contains("ticket.merge.prompt_template"));
    }

    #[test]
    fn rejects_unsupported_provider() {
        let yaml = MINIMAL.replacen("provider: codex", "provider: gemini", 1);
        let message = err(&yaml);
        assert!(
            message.contains("ticket.implementation.provider"),
            "{message}"
        );
        assert!(message.contains("gemini"));
        assert!(
            err(&format!("providers:\n  gemini: {{}}\n{MINIMAL}")).contains("providers.gemini")
        );
    }

    #[test]
    fn rejects_invalid_limits() {
        for value in ["0", "-1", "1.5", "many", "null", "[]", "4294967296"] {
            let yaml = format!("limits:\n  agent_recovery: {value}\n{MINIMAL}");
            assert!(err(&yaml).contains("limits.agent_recovery"), "{value}");
        }
    }

    #[test]
    fn rejects_invalid_string_fields() {
        let model = MINIMAL.replacen("model: m2", "model: ''", 1);
        assert!(err(&model).contains("ticket.review.model"));
        let control = MINIMAL.replacen("model: m1", "model: \"m\\x07\"", 1);
        assert!(err(&control).contains("ticket.implementation.model"));
        let reset = format!("providers:\n  codex: {{ reset_command: \"\" }}\n{MINIMAL}");
        assert!(err(&reset).contains("providers.codex.reset_command"));
        for bad in ["a\\x00b", "a\\x1bb"] {
            let template = MINIMAL.replacen(
                "prompt_template: implement",
                &format!("prompt_template: \"{bad}\""),
                1,
            );
            assert!(
                err(&template).contains("ticket.implementation.prompt_template"),
                "{bad}"
            );
        }
        for bad in ["m\\n", "m\\t", "m\\r"] {
            let model = MINIMAL.replacen("model: m1", &format!("model: \"{bad}\""), 1);
            assert!(err(&model).contains("ticket.implementation.model"), "{bad}");
            let reset = format!("providers:\n  codex: {{ reset_command: \"{bad}\" }}\n{MINIMAL}");
            assert!(
                err(&reset).contains("providers.codex.reset_command"),
                "{bad}"
            );
        }
    }

    #[test]
    fn multiline_prompt_templates_are_valid() {
        let yaml = MINIMAL.replacen(
            "prompt_template: implement",
            "prompt_template: \"a\\nb\\r\\nc\\td\"",
            1,
        );
        assert!(Configuration::from_yaml(&yaml).is_ok());
    }

    #[test]
    fn rejects_invalid_strings_in_settings_with_field_path() {
        for (settings, path) in [
            ("{service_tier: ''}", "ticket.review.settings.service_tier"),
            (
                "{service_tier: '  '}",
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{service_tier: "a\nb"}"#,
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{service_tier: "a\rb"}"#,
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{service_tier: "a\tb"}"#,
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{service_tier: "a\0b"}"#,
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{service_tier: "a\eb"}"#,
                "ticket.review.settings.service_tier",
            ),
            (
                r#"{other: {nested: ["ok", "a\nb"]}}"#,
                "ticket.review.settings.other.nested[1]",
            ),
            (
                r#"{other: {deep: ""}}"#,
                "ticket.review.settings.other.deep",
            ),
        ] {
            let yaml = MINIMAL.replacen(
                "model: m2,",
                &format!("model: m2, settings: {settings},"),
                1,
            );
            let message = err(&yaml);
            assert!(message.contains(path), "{settings}: {message}");
        }
    }

    #[test]
    fn accepts_plain_strings_in_nested_settings() {
        let value = serde_json::json!({"x": {"y": ["a b", 1, true, null]}});
        assert!(validate_setting_strings(&value, "settings").is_ok());
    }
}
