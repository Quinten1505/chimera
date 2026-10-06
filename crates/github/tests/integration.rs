//! Opt-in tests against real GitHub. They create and close real issues, branches and pull
//! requests, so they are ignored by default. Run them with:
//!
//! ```text
//! CHIMERA_GITHUB_TOKEN=<token> CHIMERA_GITHUB_REPOSITORY=<owner>/<scratch-repo> \
//!     cargo test -p chimera-github --test integration -- --ignored
//! ```
//!
//! The token needs read and write access to issues, pull requests and contents of the scratch
//! repository, which must be a repository you do not mind cluttering with closed issues (GitHub
//! cannot delete them). Each test closes the issues and pull requests it created and deletes its
//! branch, even when an assertion fails.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chimera_core::forge::Forge;
use chimera_core::{BranchName, IssueRef, IssueStatus};
use chimera_github::GitHubForge;
use reqwest::Method;
use serde_json::{Value, json};

struct Scratch {
    token: String,
    owner: String,
    repository: String,
    http: reqwest::Client,
    nonce: String,
    issues: Mutex<Vec<u64>>,
    branches: Mutex<Vec<String>>,
}

impl Scratch {
    fn from_environment() -> Arc<Self> {
        let token = std::env::var("CHIMERA_GITHUB_TOKEN")
            .expect("set CHIMERA_GITHUB_TOKEN to run the integration tests");
        let repository = std::env::var("CHIMERA_GITHUB_REPOSITORY")
            .expect("set CHIMERA_GITHUB_REPOSITORY to <owner>/<repo> to run the integration tests");
        let (owner, repository) = repository
            .split_once('/')
            .expect("CHIMERA_GITHUB_REPOSITORY must be <owner>/<repo>");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Arc::new(Self {
            token,
            owner: owner.into(),
            repository: repository.into(),
            http: reqwest::Client::new(),
            nonce: format!("{}-{nanos}", std::process::id()),
            issues: Mutex::default(),
            branches: Mutex::default(),
        })
    }

    fn forge(&self) -> GitHubForge {
        GitHubForge::new(&self.token, &self.owner, &self.repository)
    }

    fn issue_ref(&self, number: u64) -> IssueRef {
        IssueRef::new(&self.owner, &self.repository, number).unwrap()
    }

    /// Call the REST API directly, panicking on failure; returns `Null` for an empty body.
    async fn api(&self, method: Method, path: &str, body: Option<Value>) -> Value {
        self.try_api(method, path, body)
            .await
            .unwrap_or_else(|error| panic!("{error}"))
    }

    async fn try_api(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let url = format!(
            "https://api.github.com/repos/{}/{}{path}",
            self.owner, self.repository
        );
        let mut request = self
            .http
            .request(method.clone(), &url)
            .bearer_auth(&self.token)
            .header("User-Agent", "chimera-integration-tests")
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();
        let text = response.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("{method} {path}: {status}: {text}"));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Create an issue, remembering it for cleanup. Returns its number and global id.
    async fn create_issue(&self, title: &str) -> (u64, u64) {
        let issue = self
            .api(
                Method::POST,
                "/issues",
                Some(json!({"title": format!("{title} {}", self.nonce)})),
            )
            .await;
        let number = issue["number"].as_u64().unwrap();
        self.issues.lock().unwrap().push(number);
        (number, issue["id"].as_u64().unwrap())
    }

    /// Create a branch off the default branch that differs from it, remembering it for cleanup.
    /// Returns the branch and the default branch.
    async fn create_branch(&self) -> (BranchName, BranchName) {
        let repository = self.api(Method::GET, "", None).await;
        let default = repository["default_branch"].as_str().unwrap().to_owned();
        let reference = self
            .api(Method::GET, &format!("/git/ref/heads/{default}"), None)
            .await;
        let sha = reference["object"]["sha"].as_str().unwrap();
        let name = format!("chimera-it-{}", self.nonce);
        self.api(
            Method::POST,
            "/git/refs",
            Some(json!({"ref": format!("refs/heads/{name}"), "sha": sha})),
        )
        .await;
        self.branches.lock().unwrap().push(name.clone());
        self.api(
            Method::PUT,
            &format!("/contents/chimera-it-{}.txt", self.nonce),
            Some(json!({"message": "integration test", "content": "dGVzdA==", "branch": name})),
        )
        .await;
        (
            BranchName::new(name).unwrap(),
            BranchName::new(default).unwrap(),
        )
    }

    /// Close everything created, then delete the branches. Pull requests and issues share the
    /// issue number space, so closing the issue closes the pull request.
    async fn clean_up(&self) {
        let issues = std::mem::take(&mut *self.issues.lock().unwrap());
        let branches = std::mem::take(&mut *self.branches.lock().unwrap());
        for number in issues {
            let result = self
                .try_api(
                    Method::PATCH,
                    &format!("/issues/{number}"),
                    Some(json!({"state": "closed", "state_reason": "not_planned"})),
                )
                .await;
            if let Err(error) = result {
                eprintln!("cleanup: could not close #{number}: {error}");
            }
        }
        for branch in branches {
            let result = self
                .try_api(Method::DELETE, &format!("/git/refs/heads/{branch}"), None)
                .await;
            if let Err(error) = result {
                eprintln!("cleanup: could not delete branch {branch}: {error}");
            }
        }
    }
}

/// Run `test`, then clean up whether it passed or panicked, and propagate the panic.
async fn with_scratch<F, Fut>(test: F)
where
    F: FnOnce(Arc<Scratch>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let scratch = Scratch::from_environment();
    let outcome = tokio::spawn(test(scratch.clone())).await;
    scratch.clean_up().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}

#[tokio::test]
#[ignore = "needs CHIMERA_GITHUB_TOKEN and CHIMERA_GITHUB_REPOSITORY"]
async fn reads_a_specification_with_sub_issues_and_a_blocked_by_link() {
    with_scratch(|scratch| async move {
        let (specification, _) = scratch.create_issue("chimera spec").await;
        let (first, first_id) = scratch.create_issue("chimera first").await;
        let (second, second_id) = scratch.create_issue("chimera second").await;
        for id in [first_id, second_id] {
            scratch
                .api(
                    Method::POST,
                    &format!("/issues/{specification}/sub_issues"),
                    Some(json!({"sub_issue_id": id})),
                )
                .await;
        }
        scratch
            .api(
                Method::POST,
                &format!("/issues/{second}/dependencies/blocked_by"),
                Some(json!({"issue_id": first_id})),
            )
            .await;

        let plan = scratch
            .forge()
            .read_plan(&scratch.issue_ref(specification))
            .await
            .unwrap();

        let blockers_of = |number: u64| {
            let ticket = plan
                .tickets
                .iter()
                .find(|ticket| ticket.issue == scratch.issue_ref(number))
                .unwrap_or_else(|| panic!("#{number} missing from {plan:?}"));
            ticket.blockers.clone()
        };
        assert_eq!(plan.tickets.len(), 2);
        assert!(blockers_of(first).is_empty());
        let blockers = blockers_of(second);
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].issue, scratch.issue_ref(first));
        assert_eq!(blockers[0].status, IssueStatus::Open);
    })
    .await;
}

#[tokio::test]
#[ignore = "needs CHIMERA_GITHUB_TOKEN and CHIMERA_GITHUB_REPOSITORY"]
async fn draft_pull_request_lifecycle_and_idempotent_close() {
    with_scratch(|scratch| async move {
        let forge = scratch.forge();
        let (head, base) = scratch.create_branch().await;

        assert_eq!(forge.find_open_pull_request(&head).await.unwrap(), None);
        let pull_request = forge
            .create_draft_pull_request(&head, &base, "Chimera integration test", "Safe to close.")
            .await
            .unwrap();
        scratch.issues.lock().unwrap().push(pull_request.number());

        assert_eq!(
            forge.find_open_pull_request(&head).await.unwrap(),
            Some(pull_request.clone())
        );
        assert!(forge.pull_request_is_draft(&pull_request).await.unwrap());

        forge.mark_pull_request_ready(&pull_request).await.unwrap();
        forge.mark_pull_request_ready(&pull_request).await.unwrap();
        assert!(!forge.pull_request_is_draft(&pull_request).await.unwrap());

        let (number, _) = scratch.create_issue("chimera close").await;
        let issue = scratch.issue_ref(number);
        assert_eq!(forge.issue_status(&issue).await.unwrap(), IssueStatus::Open);
        forge.close_issue(&issue).await.unwrap();
        forge.close_issue(&issue).await.unwrap();
        assert_eq!(
            forge.issue_status(&issue).await.unwrap(),
            IssueStatus::Closed
        );
    })
    .await;
}
