use serde::Deserialize;
use std::{fmt, fs, io, path::Path};

/// Launch settings loaded from a Chimera YAML file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexOptions {
    pub model: String,
    pub reasoning_effort: String,
    pub service_tier: String,
    /// Route approvals through automatic review with workspace-write.
    pub approve_for_me: bool,
}

impl CodexOptions {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        Self::from_yaml(&fs::read_to_string(path).map_err(ConfigurationError::Io)?)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigurationError> {
        let options: Self = serde_yaml::from_str(yaml).map_err(ConfigurationError::Yaml)?;
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), ConfigurationError> {
        for (field, value) in [
            ("model", &self.model),
            ("reasoning_effort", &self.reasoning_effort),
            ("service_tier", &self.service_tier),
        ] {
            if value.trim().is_empty() || value.chars().any(char::is_control) {
                return Err(ConfigurationError::Invalid(field));
            }
        }
        Ok(())
    }

    /// Individual argv entries, passed to Herdr without constructing a shell command.
    pub fn launch_args(&self) -> Result<Vec<String>, ConfigurationError> {
        self.validate()?;
        let mut args = vec![
            "--model".into(),
            self.model.clone(),
            "--config".into(),
            format!(
                "model_reasoning_effort={}",
                toml_string(&self.reasoning_effort)
            ),
            "--config".into(),
            format!("service_tier={}", toml_string(&self.service_tier)),
        ];
        if self.approve_for_me {
            args.push("--approve-for-me".into());
        }
        Ok(args)
    }
}

fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[derive(Debug)]
pub enum ConfigurationError {
    Io(io::Error),
    Yaml(serde_yaml::Error),
    Invalid(&'static str),
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "Cannot read Codex configuration: {error}"),
            Self::Yaml(error) => write!(f, "Invalid Codex YAML: {error}"),
            Self::Invalid(field) => write!(
                f,
                "{field} must be nonempty and contain no control characters"
            ),
        }
    }
}

impl std::error::Error for ConfigurationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Yaml(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_luna_yaml_and_builds_argv() {
        let options =
            CodexOptions::load(concat!(env!("CARGO_MANIFEST_DIR"), "/../../codex.yaml")).unwrap();
        assert_eq!(
            options.launch_args().unwrap(),
            vec![
                "--model",
                "gpt-6-luna",
                "--config",
                "model_reasoning_effort=\"medium\"",
                "--config",
                "service_tier=\"fast\"",
                "--approve-for-me",
            ]
        );
        let mut manual = options;
        manual.approve_for_me = false;
        assert!(
            !manual
                .launch_args()
                .unwrap()
                .contains(&"--approve-for-me".into())
        );
    }

    #[test]
    fn rejects_incomplete_unknown_and_empty_settings() {
        for yaml in [
            "model: luna",
            "model: luna\nreasoning_effort: medium\nservice_tier: fast\napprove_for_me: true\ntypo: true",
            "model: ' '\nreasoning_effort: medium\nservice_tier: fast\napprove_for_me: true",
            "model: [invalid",
        ] {
            assert!(CodexOptions::from_yaml(yaml).is_err());
        }
    }

    #[test]
    fn quotes_config_values_as_toml_strings() {
        assert_eq!(toml_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
    }
}
