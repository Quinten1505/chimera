use std::path::Path;

use chimera_core::RunId;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::StoreError;
use crate::atomic::{create_dir_all_durable, read_json, write_json_atomic};
use crate::paths::run_directory;

const FILE_NAME: &str = "run.json";

/// Writes `run.json` in the run's directory atomically, creating the directory if needed. The
/// data is opaque: it is stored as given, including fields the store does not know.
pub(crate) fn save_run_data<T: Serialize>(
    root: &Path,
    run: &RunId,
    data: &T,
) -> Result<(), StoreError> {
    let directory = run_directory(root, run)?;
    create_dir_all_durable(&directory)?;
    write_json_atomic(&directory.join(FILE_NAME), data)
}

/// Reads `run.json` of `run`. A run without it is unknown: [`StoreError::RunNotFound`] names the
/// run directory that was looked up.
pub(crate) fn load_run_data<T: DeserializeOwned>(
    root: &Path,
    run: &RunId,
) -> Result<T, StoreError> {
    let directory = run_directory(root, run)?;
    read_json(&directory.join(FILE_NAME))?.ok_or(StoreError::RunNotFound { directory })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use chimera_core::{
        AgentConfiguration, AgentProfile, BranchName, CommitId, Feature, IssueRef, Ticket,
        TicketPlan,
    };
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    /// What a run was started with.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RunInput {
        pub repository: PathBuf,
        pub specification: IssueRef,
        pub configuration_file: PathBuf,
    }

    /// The contents of `run.json`: fixed for the run once the Configuration and Feature pipelines
    /// have produced them.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct RunData {
        pub input: RunInput,
        pub ticket_plan: TicketPlan,
        pub feature: Feature,
        pub configuration: AgentConfiguration,
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("octo", "repo", number).unwrap()
    }

    fn run() -> RunId {
        RunId::new("run-1").unwrap()
    }

    fn data() -> RunData {
        let mut review = AgentProfile::new("codex", "m2", "review");
        review
            .settings
            .insert("effort".into(), serde_json::json!("high"));
        RunData {
            input: RunInput {
                repository: PathBuf::from("/work/repo"),
                specification: issue(8),
                configuration_file: PathBuf::from("/work/chimera.toml"),
            },
            ticket_plan: TicketPlan {
                tickets: vec![Ticket {
                    issue: issue(54),
                    status: chimera_core::IssueStatus::Open,
                    blockers: vec![],
                }],
            },
            feature: Feature {
                specification: issue(8),
                base_branch: BranchName::new("main").unwrap(),
                feature_branch: BranchName::new("spec/8-store").unwrap(),
                expected_remote_head: CommitId::new("abc123").unwrap(),
                draft_pull_request: issue(9),
            },
            configuration: AgentConfiguration {
                implementation: AgentProfile::new("codex", "m1", "implement"),
                review,
                merge: AgentProfile::new("claude", "m3", "merge"),
            },
        }
    }

    #[test]
    fn run_data_round_trips_by_run_id() {
        let root = tempfile::tempdir().unwrap();

        save_run_data(root.path(), &run(), &data()).unwrap();

        let loaded = load_run_data::<RunData>(root.path(), &run()).unwrap();
        assert_eq!(loaded, data());
        assert_eq!(
            loaded.input.configuration_file,
            PathBuf::from("/work/chimera.toml")
        );
        assert!(root.path().join("run-1/run.json").is_file());
    }

    #[test]
    fn saving_again_replaces_the_data() {
        let root = tempfile::tempdir().unwrap();
        let mut changed = data();
        changed.feature.expected_remote_head = CommitId::new("def456").unwrap();

        save_run_data(root.path(), &run(), &data()).unwrap();
        save_run_data(root.path(), &run(), &changed).unwrap();

        assert_eq!(
            load_run_data::<RunData>(root.path(), &run()).unwrap(),
            changed
        );
    }

    #[test]
    fn runs_are_stored_separately() {
        let root = tempfile::tempdir().unwrap();
        let other = RunId::new("run-2").unwrap();
        let mut changed = data();
        changed.input.specification = issue(99);

        save_run_data(root.path(), &run(), &data()).unwrap();
        save_run_data(root.path(), &other, &changed).unwrap();

        assert_eq!(
            load_run_data::<RunData>(root.path(), &run()).unwrap(),
            data()
        );
        assert_eq!(
            load_run_data::<RunData>(root.path(), &other).unwrap(),
            changed
        );
    }

    #[test]
    fn unknown_run_is_a_failed_error_naming_the_run_directory() {
        let root = tempfile::tempdir().unwrap();

        let error =
            load_run_data::<RunData>(root.path(), &RunId::new("nope").unwrap()).unwrap_err();

        assert!(matches!(error, StoreError::RunNotFound { .. }));
        assert!(
            error
                .to_string()
                .contains(&root.path().join("nope").display().to_string())
        );
        let port: chimera_core::error::PortError = error.into();
        assert!(port.is_failed());
    }

    #[test]
    fn opaque_values_and_unknown_fields_round_trip_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let values = [
            json!({"x": 1}),
            json!(null),
            json!([1, "two", {"three": null}]),
            json!("text"),
            json!({"input": {"future_field": [1, 2]}, "extra": {"a": null}}),
        ];
        for value in values {
            save_run_data(root.path(), &run(), &value).unwrap();
            let loaded: serde_json::Value = load_run_data(root.path(), &run()).unwrap();
            assert_eq!(loaded, value);
        }
    }

    #[test]
    fn corrupt_run_data_is_a_deserialize_error() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("run-1")).unwrap();
        fs::write(root.path().join("run-1/run.json"), "{").unwrap();

        let error = load_run_data::<serde_json::Value>(root.path(), &run()).unwrap_err();

        assert!(matches!(error, StoreError::Deserialize { .. }));
    }
}
