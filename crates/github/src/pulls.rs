use chimera_core::{BranchName, IssueRef};
use reqwest::Method;
use serde_json::{Value, json};

use crate::client::{Access, Client};
use crate::error::GitHubError;

const PULL_REQUEST_QUERY: &str = "query($owner: String!, $name: String!, $number: Int!) { \
     repository(owner: $owner, name: $name) { pullRequest(number: $number) { id isDraft } } }";

const MARK_READY_MUTATION: &str = "mutation($id: ID!) { \
     markPullRequestReadyForReview(input: {pullRequestId: $id}) { pullRequest { isDraft } } }";

impl Client {
    /// Open a draft pull request merging `head` into `base` in `owner/repository`.
    ///
    /// Fails when a pull request already exists for `head`; the existing one is not returned.
    pub async fn create_draft_pull_request(
        &self,
        owner: &str,
        repository: &str,
        head: &BranchName,
        base: &BranchName,
        title: &str,
        body: &str,
    ) -> Result<IssueRef, GitHubError> {
        let path = format!("/repos/{}/{}/pulls", encode(owner), encode(repository));
        let request = json!({
            "title": title,
            "body": body,
            "head": head.as_str(),
            "base": base.as_str(),
            "draft": true,
        });
        let response = self
            .rest(Access::Mutate, Method::POST, &path, Some(&request))
            .await?;
        // The pull request was created, so an unusable response leaves the outcome unknown.
        pull_request_ref(owner, repository, &response).ok_or_else(|| {
            GitHubError::Uncertain(
                "GitHub created a pull request without returning its number".into(),
            )
        })
    }

    /// The open pull request in `owner/repository` whose head is the branch `head`, if any.
    pub async fn find_open_pull_request(
        &self,
        owner: &str,
        repository: &str,
        head: &BranchName,
    ) -> Result<Option<IssueRef>, GitHubError> {
        let path = format!(
            "/repos/{}/{}/pulls?state=open&head={}",
            encode(owner),
            encode(repository),
            encode(&format!("{owner}:{head}")),
        );
        let response = self.rest(Access::Read, Method::GET, &path, None).await?;
        let Some(pulls) = response.as_array() else {
            return Err(GitHubError::Failed(
                "unexpected pull request listing".into(),
            ));
        };
        let Some(first) = pulls.first() else {
            return Ok(None);
        };
        pull_request_ref(owner, repository, first)
            .map(Some)
            .ok_or_else(|| GitHubError::Failed("pull request listing has no number".into()))
    }

    /// Mark `pull_request` ready for review. Succeeds if it already is.
    pub async fn mark_pull_request_ready(
        &self,
        pull_request: &IssueRef,
    ) -> Result<(), GitHubError> {
        let (id, is_draft) = self.pull_request_node(pull_request).await?;
        if !is_draft {
            return Ok(());
        }
        self.graphql(Access::Mutate, MARK_READY_MUTATION, json!({ "id": id }))
            .await
            .map(|_| ())
    }

    /// Whether `pull_request` is still a draft.
    pub async fn pull_request_is_draft(
        &self,
        pull_request: &IssueRef,
    ) -> Result<bool, GitHubError> {
        self.pull_request_node(pull_request)
            .await
            .map(|(_, is_draft)| is_draft)
    }

    /// The GraphQL node id and draft flag of `pull_request`.
    async fn pull_request_node(
        &self,
        pull_request: &IssueRef,
    ) -> Result<(String, bool), GitHubError> {
        let variables = json!({
            "owner": pull_request.owner(),
            "name": pull_request.repository(),
            "number": pull_request.number(),
        });
        let data = self
            .graphql(Access::Read, PULL_REQUEST_QUERY, variables)
            .await?;
        let node = &data["repository"]["pullRequest"];
        match (node["id"].as_str(), node["isDraft"].as_bool()) {
            (Some(id), Some(is_draft)) => Ok((id.to_owned(), is_draft)),
            _ => Err(GitHubError::Failed(format!(
                "pull request {pull_request} not found"
            ))),
        }
    }
}

fn pull_request_ref(owner: &str, repository: &str, pull: &Value) -> Option<IssueRef> {
    IssueRef::new(owner, repository, pull.get("number")?.as_u64()?).ok()
}

/// Percent-encode everything but unreserved characters.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chimera_core::error::PortError;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Serve one canned response per connection, in order; return the URL and the raw requests.
    async fn serve(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buf = vec![0; 8192];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text
                            .to_lowercase()
                            .split("content-length: ")
                            .nth(1)
                            .and_then(|r| r.split("\r\n").next()?.parse::<usize>().ok())
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                    assert!(n > 0, "connection closed early");
                }
                requests.push(String::from_utf8_lossy(&request).into_owned());
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (url, handle)
    }

    fn client(url: &str) -> Client {
        Client::with_base_url("tok", url).with_timeout(Duration::from_secs(2))
    }

    fn branch(name: &str) -> BranchName {
        BranchName::new(name).unwrap()
    }

    fn pr(number: u64) -> IssueRef {
        IssueRef::new("octo", "repo", number).unwrap()
    }

    async fn create(url: &str) -> Result<IssueRef, GitHubError> {
        client(url)
            .create_draft_pull_request(
                "octo",
                "repo",
                &branch("feat/x"),
                &branch("main"),
                "Title",
                "Body",
            )
            .await
    }

    #[tokio::test]
    async fn create_posts_a_draft_and_returns_the_reference() {
        let (url, requests) = serve(vec![("201 Created", r#"{"number":12}"#)]).await;
        assert_eq!(create(&url).await.unwrap(), pr(12));
        let request = requests.await.unwrap().remove(0);
        assert!(request.starts_with("POST /repos/octo/repo/pulls HTTP/1.1"));
        let body: Value = serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"title": "Title", "body": "Body", "head": "feat/x", "base": "main", "draft": true})
        );
    }

    #[tokio::test]
    async fn create_when_a_pull_request_exists_is_failed_and_says_so() {
        let body = r#"{"message":"Validation Failed","errors":[{"resource":"PullRequest","code":"custom","message":"A pull request already exists for octo:feat/x."}]}"#;
        let (url, _) = serve(vec![("422 Unprocessable Entity", body)]).await;
        let error = create(&url).await.unwrap_err();
        assert!(matches!(error, GitHubError::Failed(_)), "{error}");
        assert!(error.to_string().contains("already exists"), "{error}");
    }

    #[tokio::test]
    async fn create_server_error_is_uncertain() {
        let (url, _) = serve(vec![("502 Bad Gateway", "")]).await;
        let error = create(&url).await.unwrap_err();
        assert!(matches!(error, GitHubError::Uncertain(_)), "{error}");
        assert!(matches!(PortError::from(error), PortError::Uncertain(_)));
    }

    async fn find(url: &str) -> Result<Option<IssueRef>, GitHubError> {
        client(url)
            .find_open_pull_request("octo", "repo", &branch("feat/x"))
            .await
    }

    #[tokio::test]
    async fn find_returns_the_open_pull_request() {
        let (url, requests) = serve(vec![("200 OK", r#"[{"number":7},{"number":8}]"#)]).await;
        assert_eq!(find(&url).await.unwrap(), Some(pr(7)));
        let request = requests.await.unwrap().remove(0);
        assert!(
            request
                .starts_with("GET /repos/octo/repo/pulls?state=open&head=octo%3Afeat%2Fx HTTP/1.1"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn find_returns_nothing_when_there_is_none() {
        let (url, _) = serve(vec![("200 OK", "[]")]).await;
        assert_eq!(find(&url).await.unwrap(), None);
    }

    #[tokio::test]
    async fn find_server_error_is_failed() {
        let (url, _) = serve(vec![("500 Internal Server Error", "")]).await;
        assert!(matches!(find(&url).await, Err(GitHubError::Failed(_))));
    }

    const DRAFT: &str = r#"{"data":{"repository":{"pullRequest":{"id":"PR_1","isDraft":true}}}}"#;
    const READY: &str = r#"{"data":{"repository":{"pullRequest":{"id":"PR_1","isDraft":false}}}}"#;
    const MARKED: &str =
        r#"{"data":{"markPullRequestReadyForReview":{"pullRequest":{"isDraft":false}}}}"#;

    #[tokio::test]
    async fn mark_ready_converts_a_draft() {
        let (url, requests) = serve(vec![("200 OK", DRAFT), ("200 OK", MARKED)]).await;
        client(&url).mark_pull_request_ready(&pr(5)).await.unwrap();
        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("POST /graphql"));
        assert!(requests[1].contains("markPullRequestReadyForReview"));
        assert!(requests[1].contains("PR_1"));
    }

    #[tokio::test]
    async fn mark_ready_succeeds_when_already_ready() {
        let (url, requests) = serve(vec![("200 OK", READY)]).await;
        client(&url).mark_pull_request_ready(&pr(5)).await.unwrap();
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn mark_ready_for_a_missing_pull_request_is_failed() {
        let body = r#"{"data":{"repository":{"pullRequest":null}}}"#;
        let (url, _) = serve(vec![("200 OK", body)]).await;
        let result = client(&url).mark_pull_request_ready(&pr(5)).await;
        assert!(matches!(result, Err(GitHubError::Failed(_))));
    }

    #[tokio::test]
    async fn mark_ready_server_error_is_uncertain() {
        let (url, _) = serve(vec![("200 OK", DRAFT), ("502 Bad Gateway", "")]).await;
        let result = client(&url).mark_pull_request_ready(&pr(5)).await;
        assert!(matches!(result, Err(GitHubError::Uncertain(_))));
    }
}
