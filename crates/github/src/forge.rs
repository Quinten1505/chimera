use async_trait::async_trait;
use chimera_core::error::PortError;
use chimera_core::forge::Forge;
use chimera_core::{BranchName, IssueRef, IssueStatus, TicketPlan};

use crate::client::Client;

/// The [`Forge`] port backed by GitHub. Pull requests are opened and found in the repository the
/// forge was created for; every other operation addresses the repository of its [`IssueRef`].
#[derive(Debug, Clone)]
pub struct GitHubForge {
    client: Client,
    owner: String,
    repository: String,
}

impl GitHubForge {
    /// A forge authenticating with `token` that opens pull requests in `owner/repository`.
    pub fn new(
        token: impl Into<String>,
        owner: impl Into<String>,
        repository: impl Into<String>,
    ) -> Self {
        Self::with_client(Client::new(token), owner, repository)
    }

    fn with_client(
        client: Client,
        owner: impl Into<String>,
        repository: impl Into<String>,
    ) -> Self {
        Self {
            client,
            owner: owner.into(),
            repository: repository.into(),
        }
    }
}

#[async_trait]
impl Forge for GitHubForge {
    async fn read_plan(&self, specification: &IssueRef) -> Result<TicketPlan, PortError> {
        Ok(self.client.read_plan(specification).await?)
    }

    async fn create_draft_pull_request(
        &self,
        head: &BranchName,
        base: &BranchName,
        title: &str,
        body: &str,
    ) -> Result<IssueRef, PortError> {
        Ok(self
            .client
            .create_draft_pull_request(&self.owner, &self.repository, head, base, title, body)
            .await?)
    }

    async fn find_open_pull_request(
        &self,
        head: &BranchName,
    ) -> Result<Option<IssueRef>, PortError> {
        Ok(self
            .client
            .find_open_pull_request(&self.owner, &self.repository, head)
            .await?)
    }

    async fn close_issue(&self, issue: &IssueRef) -> Result<(), PortError> {
        Ok(self.client.close_issue(issue).await?)
    }

    async fn issue_status(&self, issue: &IssueRef) -> Result<IssueStatus, PortError> {
        Ok(self.client.issue_status(issue).await?)
    }

    async fn mark_pull_request_ready(&self, pull_request: &IssueRef) -> Result<(), PortError> {
        Ok(self.client.mark_pull_request_ready(pull_request).await?)
    }

    async fn pull_request_is_draft(&self, pull_request: &IssueRef) -> Result<bool, PortError> {
        Ok(self.client.pull_request_is_draft(pull_request).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Answer one request with `body` and return the base URL and the request line.
    async fn serve(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf[..n])
                .lines()
                .next()
                .unwrap()
                .to_owned()
        });
        (url, handle)
    }

    #[tokio::test]
    async fn delegates_through_a_trait_object_to_the_configured_repository() {
        let (url, request) = serve(r#"[{"number":7}]"#).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_secs(2));
        let forge: Arc<dyn Forge> = Arc::new(GitHubForge::with_client(client, "octo", "repo"));
        let head = BranchName::new("feat/x").unwrap();
        assert_eq!(
            forge.find_open_pull_request(&head).await.unwrap(),
            Some(IssueRef::new("octo", "repo", 7).unwrap())
        );
        assert!(
            request
                .await
                .unwrap()
                .starts_with("GET /repos/octo/repo/pulls?")
        );
    }

    #[tokio::test]
    async fn issue_status_reads_the_state() {
        let (url, _) = serve(r#"{"state":"closed"}"#).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_secs(2));
        let forge = GitHubForge::with_client(client, "octo", "repo");
        let issue = IssueRef::new("octo", "repo", 3).unwrap();
        assert_eq!(
            forge.issue_status(&issue).await.unwrap(),
            IssueStatus::Closed
        );
    }

    #[tokio::test]
    async fn pull_request_is_draft_reads_the_flag() {
        let (url, _) =
            serve(r#"{"data":{"repository":{"pullRequest":{"id":"PR_1","isDraft":true}}}}"#).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_secs(2));
        let forge = GitHubForge::with_client(client, "octo", "repo");
        let pr = IssueRef::new("octo", "repo", 3).unwrap();
        assert!(forge.pull_request_is_draft(&pr).await.unwrap());
    }
}
