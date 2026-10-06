use async_trait::async_trait;

use crate::error::PortError;
use crate::{BranchName, IssueRef, TicketPlan};

/// The GitHub issues and pull requests the run works with. Pull requests share the issue number
/// space, so they are referenced by an [`IssueRef`].
#[async_trait]
pub trait Forge: Send + Sync {
    /// The sub-issues of `specification` with their blocked-by links.
    async fn read_plan(&self, specification: &IssueRef) -> Result<TicketPlan, PortError>;

    /// Opens a draft pull request merging `head` into `base`.
    async fn create_draft_pull_request(
        &self,
        head: &BranchName,
        base: &BranchName,
        title: &str,
        body: &str,
    ) -> Result<IssueRef, PortError>;

    /// The open pull request whose head is `head`, used to reconcile an uncertain creation.
    async fn find_open_pull_request(
        &self,
        head: &BranchName,
    ) -> Result<Option<IssueRef>, PortError>;

    async fn close_issue(&self, issue: &IssueRef) -> Result<(), PortError>;

    async fn mark_pull_request_ready(&self, pull_request: &IssueRef) -> Result<(), PortError>;
}

#[cfg(any(test, feature = "testing"))]
pub use fake::{FakeForge, ForgeCall};

#[cfg(any(test, feature = "testing"))]
mod fake {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::Forge;
    use crate::error::PortError;
    use crate::{BranchName, IssueRef, TicketPlan};

    /// One call made to a [`FakeForge`], recorded even when it fails.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ForgeCall {
        ReadPlan(IssueRef),
        CreateDraftPullRequest {
            head: BranchName,
            base: BranchName,
            title: String,
            body: String,
        },
        FindOpenPullRequest(BranchName),
        CloseIssue(IssueRef),
        MarkPullRequestReady(IssueRef),
    }

    struct PullRequest {
        number: IssueRef,
        head: BranchName,
        draft: bool,
    }

    struct State {
        plans: HashMap<IssueRef, TicketPlan>,
        pull_requests: Vec<PullRequest>,
        closed: Vec<IssueRef>,
        calls: Vec<ForgeCall>,
        failures: VecDeque<PortError>,
    }

    /// In-memory [`Forge`] for one repository. Created pull requests are numbered from 1000.
    pub struct FakeForge {
        owner: String,
        repository: String,
        state: Mutex<State>,
    }

    impl FakeForge {
        pub fn new(owner: impl Into<String>, repository: impl Into<String>) -> Self {
            Self {
                owner: owner.into(),
                repository: repository.into(),
                state: Mutex::new(State {
                    plans: HashMap::new(),
                    pull_requests: Vec::new(),
                    closed: Vec::new(),
                    calls: Vec::new(),
                    failures: VecDeque::new(),
                }),
            }
        }

        pub fn set_plan(&self, specification: IssueRef, plan: TicketPlan) {
            self.state.lock().unwrap().plans.insert(specification, plan);
        }

        /// Adds an already open pull request, as if created by an earlier uncertain call.
        pub fn add_open_pull_request(&self, number: IssueRef, head: BranchName) {
            self.state.lock().unwrap().pull_requests.push(PullRequest {
                number,
                head,
                draft: true,
            });
        }

        /// The next operation returns `error` instead of running.
        pub fn fail_next(&self, error: PortError) {
            self.state.lock().unwrap().failures.push_back(error);
        }

        pub fn calls(&self) -> Vec<ForgeCall> {
            self.state.lock().unwrap().calls.clone()
        }

        pub fn is_closed(&self, issue: &IssueRef) -> bool {
            self.state.lock().unwrap().closed.contains(issue)
        }

        /// Whether `pull_request` exists and is still a draft.
        pub fn is_draft(&self, pull_request: &IssueRef) -> Option<bool> {
            let state = self.state.lock().unwrap();
            state
                .pull_requests
                .iter()
                .find(|pr| &pr.number == pull_request)
                .map(|pr| pr.draft)
        }
    }

    impl State {
        fn begin(&mut self, call: ForgeCall) -> Result<(), PortError> {
            self.calls.push(call);
            self.failures.pop_front().map_or(Ok(()), Err)
        }

        fn pull_request_mut(&mut self, number: &IssueRef) -> Result<&mut PullRequest, PortError> {
            self.pull_requests
                .iter_mut()
                .find(|pr| &pr.number == number)
                .ok_or_else(|| PortError::failed(format!("pull request {number} does not exist")))
        }
    }

    #[async_trait]
    impl Forge for FakeForge {
        async fn read_plan(&self, specification: &IssueRef) -> Result<TicketPlan, PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin(ForgeCall::ReadPlan(specification.clone()))?;
            state
                .plans
                .get(specification)
                .cloned()
                .ok_or_else(|| PortError::failed(format!("issue {specification} does not exist")))
        }

        async fn create_draft_pull_request(
            &self,
            head: &BranchName,
            base: &BranchName,
            title: &str,
            body: &str,
        ) -> Result<IssueRef, PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin(ForgeCall::CreateDraftPullRequest {
                head: head.clone(),
                base: base.clone(),
                title: title.to_string(),
                body: body.to_string(),
            })?;
            let number = IssueRef::new(
                self.owner.clone(),
                self.repository.clone(),
                1000 + state.pull_requests.len() as u64,
            )
            .map_err(|e| PortError::failed(e.to_string()))?;
            state.pull_requests.push(PullRequest {
                number: number.clone(),
                head: head.clone(),
                draft: true,
            });
            Ok(number)
        }

        async fn find_open_pull_request(
            &self,
            head: &BranchName,
        ) -> Result<Option<IssueRef>, PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin(ForgeCall::FindOpenPullRequest(head.clone()))?;
            Ok(state
                .pull_requests
                .iter()
                .find(|pr| &pr.head == head && !state.closed.contains(&pr.number))
                .map(|pr| pr.number.clone()))
        }

        async fn close_issue(&self, issue: &IssueRef) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin(ForgeCall::CloseIssue(issue.clone()))?;
            state.closed.push(issue.clone());
            Ok(())
        }

        async fn mark_pull_request_ready(&self, pull_request: &IssueRef) -> Result<(), PortError> {
            let mut state = self.state.lock().unwrap();
            state.begin(ForgeCall::MarkPullRequestReady(pull_request.clone()))?;
            state.pull_request_mut(pull_request)?.draft = false;
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use futures_executor::block_on;

        use super::*;
        use crate::{IssueStatus, Ticket};

        fn branch(name: &str) -> BranchName {
            BranchName::new(name).unwrap()
        }

        fn issue(number: u64) -> IssueRef {
            IssueRef::new("octo", "repo", number).unwrap()
        }

        fn forge() -> FakeForge {
            FakeForge::new("octo", "repo")
        }

        #[test]
        fn usable_as_trait_object() {
            let forge: Arc<dyn Forge> = Arc::new(forge());
            assert_eq!(
                block_on(forge.find_open_pull_request(&branch("feat"))).unwrap(),
                None
            );
        }

        #[test]
        fn reads_plan() {
            let fake = forge();
            let plan = TicketPlan {
                tickets: vec![Ticket {
                    issue: issue(11),
                    status: IssueStatus::Open,
                    blockers: vec![],
                }],
            };
            fake.set_plan(issue(2), plan.clone());
            assert_eq!(block_on(fake.read_plan(&issue(2))).unwrap(), plan);
            assert!(block_on(fake.read_plan(&issue(3))).unwrap_err().is_failed());
            assert_eq!(
                fake.calls(),
                vec![ForgeCall::ReadPlan(issue(2)), ForgeCall::ReadPlan(issue(3))]
            );
        }

        #[test]
        fn creates_draft_pull_request() {
            let fake = forge();
            let number = block_on(fake.create_draft_pull_request(
                &branch("feat"),
                &branch("main"),
                "t",
                "b",
            ))
            .unwrap();
            assert_eq!(fake.is_draft(&number), Some(true));
            assert_eq!(
                fake.calls(),
                vec![ForgeCall::CreateDraftPullRequest {
                    head: branch("feat"),
                    base: branch("main"),
                    title: "t".into(),
                    body: "b".into(),
                }]
            );
        }

        #[test]
        fn finds_pull_request_by_branch() {
            let fake = forge();
            assert_eq!(
                block_on(fake.find_open_pull_request(&branch("feat"))).unwrap(),
                None
            );
            let number = block_on(fake.create_draft_pull_request(
                &branch("feat"),
                &branch("main"),
                "t",
                "b",
            ))
            .unwrap();
            assert_eq!(
                block_on(fake.find_open_pull_request(&branch("feat"))).unwrap(),
                Some(number.clone())
            );
            assert_eq!(
                block_on(fake.find_open_pull_request(&branch("other"))).unwrap(),
                None
            );
            block_on(fake.close_issue(&number)).unwrap();
            assert_eq!(
                block_on(fake.find_open_pull_request(&branch("feat"))).unwrap(),
                None
            );
        }

        #[test]
        fn closes_issue() {
            let fake = forge();
            assert!(!fake.is_closed(&issue(11)));
            block_on(fake.close_issue(&issue(11))).unwrap();
            assert!(fake.is_closed(&issue(11)));
        }

        #[test]
        fn marks_pull_request_ready() {
            let fake = forge();
            let number = block_on(fake.create_draft_pull_request(
                &branch("feat"),
                &branch("main"),
                "t",
                "b",
            ))
            .unwrap();
            block_on(fake.mark_pull_request_ready(&number)).unwrap();
            assert_eq!(fake.is_draft(&number), Some(false));
            assert!(
                block_on(fake.mark_pull_request_ready(&issue(5)))
                    .unwrap_err()
                    .is_failed()
            );
        }

        #[test]
        fn scripted_failure_applies_once() {
            let fake = forge();
            fake.fail_next(PortError::uncertain("lost"));
            assert!(
                block_on(fake.close_issue(&issue(11)))
                    .unwrap_err()
                    .is_uncertain()
            );
            assert!(!fake.is_closed(&issue(11)));
            block_on(fake.close_issue(&issue(11))).unwrap();
            assert!(fake.is_closed(&issue(11)));
        }

        #[test]
        fn uncertain_creation_can_be_reconciled() {
            let fake = forge();
            fake.add_open_pull_request(issue(7), branch("feat"));
            fake.fail_next(PortError::uncertain("lost"));
            assert!(
                block_on(fake.create_draft_pull_request(
                    &branch("feat"),
                    &branch("main"),
                    "t",
                    "b"
                ))
                .unwrap_err()
                .is_uncertain()
            );
            assert_eq!(
                block_on(fake.find_open_pull_request(&branch("feat"))).unwrap(),
                Some(issue(7))
            );
        }
    }
}
