//! Domain types and ports only. Performs no I/O: no sockets, processes, or file system access.

pub mod error;
pub mod repository;

use std::collections::BTreeMap;
use std::fmt;

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

/// Error returned when an identifier is constructed from an empty or whitespace-only value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind} must not be empty or whitespace-only")]
pub struct EmptyIdentifier {
    kind: &'static str,
}

fn non_blank(kind: &'static str, value: String) -> Result<String, EmptyIdentifier> {
    if value.trim().is_empty() {
        Err(EmptyIdentifier { kind })
    } else {
        Ok(value)
    }
}

macro_rules! string_identifier {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, EmptyIdentifier> {
                non_blank(stringify!($name), value.into()).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = EmptyIdentifier;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_identifier!(
    /// Identifies one run of the workflow.
    RunId
);
string_identifier!(
    /// Name of a git branch.
    BranchName
);
string_identifier!(
    /// Identifies a git commit.
    CommitId
);
string_identifier!(
    /// Identifies a terminal workspace.
    WorkspaceId
);
string_identifier!(
    /// Identifies a terminal pane.
    PaneId
);
string_identifier!(
    /// Identifies an agent.
    AgentId
);

/// Identifies a GitHub issue by owner, repository, and number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "IssueRefParts")]
pub struct IssueRef {
    owner: String,
    repository: String,
    number: u64,
}

#[derive(Deserialize)]
struct IssueRefParts {
    owner: String,
    repository: String,
    number: u64,
}

impl TryFrom<IssueRefParts> for IssueRef {
    type Error = EmptyIdentifier;

    fn try_from(parts: IssueRefParts) -> Result<Self, Self::Error> {
        Self::new(parts.owner, parts.repository, parts.number)
    }
}

impl IssueRef {
    pub fn new(
        owner: impl Into<String>,
        repository: impl Into<String>,
        number: u64,
    ) -> Result<Self, EmptyIdentifier> {
        Ok(Self {
            owner: non_blank("IssueRef owner", owner.into())?,
            repository: non_blank("IssueRef repository", repository.into())?,
            number,
        })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn repository(&self) -> &str {
        &self.repository
    }

    pub fn number(&self) -> u64 {
        self.number
    }
}

impl fmt::Display for IssueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}#{}", self.owner, self.repository, self.number)
    }
}

/// Whether a GitHub issue is open or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IssueStatus {
    Open,
    Closed,
}

/// A GitHub issue describing one feature; maps to one feature branch and one draft PR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Specification {
    pub issue: IssueRef,
}

/// The specification's feature branch and draft PR. The PR shares the issue number space,
/// so it is referenced by an [`IssueRef`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Feature {
    pub specification: IssueRef,
    pub base_branch: BranchName,
    pub feature_branch: BranchName,
    pub expected_remote_head: CommitId,
    pub draft_pull_request: IssueRef,
}

/// An issue blocking a ticket, with its status as read. It may lie outside the [`TicketPlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocker {
    pub issue: IssueRef,
    pub status: IssueStatus,
}

/// A sub-issue of a specification, with the issues blocking it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub issue: IssueRef,
    pub status: IssueStatus,
    pub blockers: Vec<Blocker>,
}

/// The tickets of a run and their blocked-by relationships, fixed for the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketPlan {
    pub tickets: Vec<Ticket>,
}

/// A unit of work for an implementation/review/merge triplet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItem {
    Ticket(Ticket),
    /// Findings from the final review, carrying its explanation.
    Findings(String),
}

/// A verified pushed commit on the feature branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergedOk {
    pub commit: CommitId,
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

    macro_rules! string_identifier_tests {
        ($($test:ident: $name:ident),* $(,)?) => {$(
            #[test]
            fn $test() {
                let id = $name::new("abc").unwrap();
                assert_eq!(id.to_string(), "abc");
                let json = serde_json::to_string(&id).unwrap();
                assert_eq!(json, "\"abc\"");
                assert_eq!(serde_json::from_str::<$name>(&json).unwrap(), id);
                assert!($name::new("").is_err());
                assert!($name::new(" \t\n").is_err());
                assert!(serde_json::from_str::<$name>("\"  \"").is_err());
            }
        )*};
    }

    string_identifier_tests! {
        run_id: RunId,
        branch_name: BranchName,
        commit_id: CommitId,
        workspace_id: WorkspaceId,
        pane_id: PaneId,
        agent_id: AgentId,
    }

    #[test]
    fn issue_ref() {
        let issue = IssueRef::new("octo", "repo", 9).unwrap();
        assert_eq!(issue.to_string(), "octo/repo#9");
        let json = serde_json::to_string(&issue).unwrap();
        assert_eq!(serde_json::from_str::<IssueRef>(&json).unwrap(), issue);
        assert!(IssueRef::new("", "repo", 9).is_err());
        assert!(IssueRef::new("octo", "  ", 9).is_err());
        assert!(
            serde_json::from_str::<IssueRef>(r#"{"owner":"","repository":"r","number":1}"#)
                .is_err()
        );
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("octo", "repo", number).unwrap()
    }

    fn ticket() -> Ticket {
        Ticket {
            issue: issue(11),
            status: IssueStatus::Open,
            blockers: vec![
                Blocker {
                    issue: issue(9),
                    status: IssueStatus::Closed,
                },
                Blocker {
                    issue: issue(500),
                    status: IssueStatus::Open,
                },
            ],
        }
    }

    fn assert_round_trip<T>(value: T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + fmt::Debug,
    {
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<T>(&json).unwrap(), value);
    }

    #[test]
    fn domain_types_round_trip() {
        assert_round_trip(IssueStatus::Closed);
        assert_round_trip(Specification { issue: issue(2) });
        assert_round_trip(Feature {
            specification: issue(2),
            base_branch: BranchName::new("main").unwrap(),
            feature_branch: BranchName::new("spec/2-core").unwrap(),
            expected_remote_head: CommitId::new("abc123").unwrap(),
            draft_pull_request: issue(3),
        });
        assert_round_trip(ticket());
        assert_round_trip(TicketPlan {
            tickets: vec![ticket()],
        });
        assert_round_trip(WorkItem::Ticket(ticket()));
        assert_round_trip(WorkItem::Findings("fix the thing".into()));
        assert_round_trip(MergedOk {
            commit: CommitId::new("def456").unwrap(),
        });
    }
}
