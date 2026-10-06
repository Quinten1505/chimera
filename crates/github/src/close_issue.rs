use chimera_core::{IssueRef, IssueStatus};
use reqwest::Method;
use serde_json::json;

use crate::client::{Access, Client};
use crate::error::GitHubError;

impl Client {
    /// Close `issue` as completed. Closing an already closed issue succeeds.
    pub async fn close_issue(&self, issue: &IssueRef) -> Result<(), GitHubError> {
        let path = format!(
            "/repos/{}/{}/issues/{}",
            issue.owner(),
            issue.repository(),
            issue.number()
        );
        let body = json!({ "state": "closed", "state_reason": "completed" });
        self.rest(Access::Mutate, Method::PATCH, &path, Some(&body))
            .await
            .map(|_| ())
    }

    /// Whether `issue` is open or closed.
    pub async fn issue_status(&self, issue: &IssueRef) -> Result<IssueStatus, GitHubError> {
        let path = format!(
            "/repos/{}/{}/issues/{}",
            issue.owner(),
            issue.repository(),
            issue.number()
        );
        let response = self.rest(Access::Read, Method::GET, &path, None).await?;
        match response["state"].as_str() {
            Some("open") => Ok(IssueStatus::Open),
            Some("closed") => Ok(IssueStatus::Closed),
            _ => Err(GitHubError::Failed(format!(
                "issue {issue} has no recognizable state"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Answer one request and return the base URL and a handle yielding the raw request.
    async fn serve(
        status: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = vec![0; 8192];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request);
                if n == 0 || text.contains("\"state_reason\"") || !text.starts_with("PATCH") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (url, handle)
    }

    async fn close(status: &'static str, body: &'static str) -> (Result<(), GitHubError>, String) {
        let (url, handle) = serve(status, body).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_millis(500));
        let issue = IssueRef::new("octo", "repo", 7).unwrap();
        let result = client.close_issue(&issue).await;
        (result, handle.await.unwrap())
    }

    #[tokio::test]
    async fn closes_open_issue_as_completed() {
        let (result, request) = close("200 OK", "{\"state\":\"closed\"}").await;
        result.unwrap();
        assert!(request.starts_with("PATCH /repos/octo/repo/issues/7 HTTP/1.1"));
        assert!(request.contains("\"state\":\"closed\""));
        assert!(request.contains("\"state_reason\":\"completed\""));
    }

    #[tokio::test]
    async fn closing_closed_issue_succeeds() {
        let body = "{\"state\":\"closed\",\"state_reason\":\"not_planned\"}";
        let (result, _) = close("200 OK", body).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn missing_issue_is_failed() {
        let (result, _) = close("404 Not Found", "{\"message\":\"Not Found\"}").await;
        assert!(matches!(result, Err(GitHubError::Failed(_))), "{result:?}");
    }

    #[tokio::test]
    async fn server_error_is_uncertain() {
        let (result, _) = close("502 Bad Gateway", "").await;
        assert!(
            matches!(result, Err(GitHubError::Uncertain(_))),
            "{result:?}"
        );
    }
}
