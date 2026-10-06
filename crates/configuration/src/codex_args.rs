//! Codex launch arguments built from an [`AgentProfile`], so the Herdr adapter receives a ready argv.

use std::collections::BTreeMap;

use chimera_core::AgentProfile;
use serde_json::Value;

use crate::{ConfigurationError, codex::is_plain_text};

pub const CODEX_PROVIDER: &str = "codex";

/// Typed view of the Codex-specific entries in a profile's `settings`.
#[derive(Debug, Default, PartialEq, Eq)]
struct CodexSettings {
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    approve_for_me: bool,
}

impl CodexSettings {
    /// `prefix` is the field path of the settings map, e.g. `ticket.review.settings`.
    fn parse(settings: &BTreeMap<String, Value>, prefix: &str) -> Result<Self, ConfigurationError> {
        let mut parsed = Self::default();
        for (key, value) in settings {
            let field = format!("{prefix}.{key}");
            match key.as_str() {
                "reasoning_effort" => parsed.reasoning_effort = Some(string(value, field)?),
                "service_tier" => parsed.service_tier = Some(string(value, field)?),
                "approve_for_me" => {
                    parsed.approve_for_me =
                        value.as_bool().ok_or(ConfigurationError::InvalidSetting {
                            field,
                            expected: "a boolean",
                        })?;
                }
                _ => return Err(ConfigurationError::UnknownSetting { field }),
            }
        }
        Ok(parsed)
    }
}

fn string(value: &Value, field: String) -> Result<String, ConfigurationError> {
    match value.as_str() {
        Some(text) if is_plain_text(text) => Ok(text.to_string()),
        _ => Err(ConfigurationError::InvalidSetting {
            field,
            expected: "a nonempty string without control characters",
        }),
    }
}

/// Rejects unknown or ill-typed Codex settings; `prefix` is the profile's field path.
/// Profiles of other providers are not inspected.
pub(crate) fn validate_settings(
    profile: &AgentProfile,
    prefix: &str,
) -> Result<(), ConfigurationError> {
    if profile.provider == CODEX_PROVIDER {
        CodexSettings::parse(&profile.settings, &format!("{prefix}.settings"))?;
    }
    Ok(())
}

/// Individual argv entries for launching Codex, passed on without constructing a shell command.
pub fn codex_launch_args(profile: &AgentProfile) -> Result<Vec<String>, ConfigurationError> {
    if profile.provider != CODEX_PROVIDER {
        return Err(ConfigurationError::NotCodex(profile.provider.clone()));
    }
    let settings = CodexSettings::parse(&profile.settings, "settings")?;
    let mut args = vec!["--model".to_string(), profile.model.clone()];
    if let Some(effort) = &settings.reasoning_effort {
        args.push("--config".into());
        args.push(format!("model_reasoning_effort={}", toml_string(effort)));
    }
    if let Some(tier) = &settings.service_tier {
        args.push("--config".into());
        args.push(format!("service_tier={}", toml_string(tier)));
    }
    if settings.approve_for_me {
        args.push("--approve-for-me".into());
    }
    Ok(args)
}

pub(crate) fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn profile(settings: Value) -> AgentProfile {
        let mut profile = AgentProfile::new("codex", "gpt-6-luna", "prompt");
        profile.settings = serde_json::from_value(settings).unwrap();
        profile
    }

    #[test]
    fn builds_full_argv() {
        let profile = profile(json!({
            "reasoning_effort": "medium", "service_tier": "fast", "approve_for_me": true
        }));
        assert_eq!(
            codex_launch_args(&profile).unwrap(),
            [
                "--model",
                "gpt-6-luna",
                "--config",
                "model_reasoning_effort=\"medium\"",
                "--config",
                "service_tier=\"fast\"",
                "--approve-for-me",
            ]
        );
    }

    #[test]
    fn approve_for_me_false_omits_flag() {
        let profile = profile(json!({"reasoning_effort": "high", "approve_for_me": false}));
        let args = codex_launch_args(&profile).unwrap();
        assert!(!args.contains(&"--approve-for-me".to_string()));
        assert_eq!(
            codex_launch_args(&self::profile(json!({}))).unwrap().len(),
            2
        );
    }

    #[test]
    fn quotes_values_as_toml_strings_in_single_entries() {
        assert_eq!(toml_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
        let args = codex_launch_args(&profile(json!({"service_tier": "x\" --y\\"}))).unwrap();
        assert_eq!(args[3], "service_tier=\"x\\\" --y\\\\\"");
    }

    #[test]
    fn rejects_unknown_and_ill_typed_settings_with_field_path() {
        let error = validate_settings(&profile(json!({"typo": 1})), "ticket.review").unwrap_err();
        assert!(matches!(error, ConfigurationError::UnknownSetting { .. }));
        assert!(error.to_string().contains("ticket.review.settings.typo"));
        for settings in [
            json!({"service_tier": 3}),
            json!({"service_tier": ""}),
            json!({"approve_for_me": "yes"}),
        ] {
            assert!(validate_settings(&profile(settings), "ticket.review").is_err());
        }
    }

    #[test]
    fn rejects_other_providers() {
        let claude = AgentProfile::new("claude", "m", "p");
        assert!(codex_launch_args(&claude).is_err());
        assert!(validate_settings(&claude, "x").is_ok());
    }
}
