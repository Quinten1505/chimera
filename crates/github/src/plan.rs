use chimera_core::{Blocker, IssueRef, IssueStatus, Ticket, TicketPlan};
use serde_json::{Value, json};

use crate::client::{Access, Client};
use crate::error::GitHubError;

const PAGE_SIZE: u64 = 100;

const ISSUE_FIELDS: &str = "number state repository { name owner { login } }";

fn plan_query() -> String {
    format!(
        "query($owner: String!, $name: String!, $number: Int!, $after: String) {{
          repository(owner: $owner, name: $name) {{
            issue(number: $number) {{
              subIssues(first: {PAGE_SIZE}, after: $after) {{
                nodes {{
                  id {ISSUE_FIELDS}
                  blockedBy(first: {PAGE_SIZE}) {{
                    nodes {{ {ISSUE_FIELDS} }}
                    pageInfo {{ hasNextPage endCursor }}
                  }}
                }}
                pageInfo {{ hasNextPage endCursor }}
              }}
            }}
          }}
        }}"
    )
}

fn blockers_query() -> String {
    format!(
        "query($id: ID!, $after: String) {{
          node(id: $id) {{
            ... on Issue {{
              blockedBy(first: {PAGE_SIZE}, after: $after) {{
                nodes {{ {ISSUE_FIELDS} }}
                pageInfo {{ hasNextPage endCursor }}
              }}
            }}
          }}
        }}"
    )
}

impl Client {
    /// Read the sub-issues of `specification` and their blocked-by links, following every page.
    /// Blockers outside the specification, including in other repositories, are returned as given.
    pub async fn read_plan(&self, specification: &IssueRef) -> Result<TicketPlan, GitHubError> {
        let query = plan_query();
        let mut tickets = Vec::new();
        let mut after = Value::Null;
        loop {
            let data = self
                .graphql(
                    Access::Read,
                    &query,
                    json!({
                        "owner": specification.owner(),
                        "name": specification.repository(),
                        "number": specification.number(),
                        "after": after,
                    }),
                )
                .await?;
            let Some(issue) = data.pointer("/repository/issue").filter(|i| !i.is_null()) else {
                return Err(GitHubError::Failed(format!(
                    "specification issue {specification} was not found"
                )));
            };
            let sub_issues = &issue["subIssues"];
            for node in array(sub_issues, "nodes")? {
                let mut blockers = blockers(&node["blockedBy"])?;
                let mut page = page_info(&node["blockedBy"])?;
                while let Some(cursor) = page {
                    let id = string(node, "id")?;
                    let data = self
                        .graphql(
                            Access::Read,
                            &blockers_query(),
                            json!({ "id": id, "after": cursor }),
                        )
                        .await?;
                    let more = &data["node"]["blockedBy"];
                    blockers.extend(self::blockers(more)?);
                    page = page_info(more)?;
                }
                let (issue, status) = issue_and_status(node)?;
                tickets.push(Ticket {
                    issue,
                    status,
                    blockers,
                });
            }
            match page_info(sub_issues)? {
                Some(cursor) => after = Value::String(cursor),
                None => return Ok(TicketPlan { tickets }),
            }
        }
    }
}

fn blockers(connection: &Value) -> Result<Vec<Blocker>, GitHubError> {
    array(connection, "nodes")?
        .iter()
        .map(|node| {
            let (issue, status) = issue_and_status(node)?;
            Ok(Blocker { issue, status })
        })
        .collect()
}

/// The cursor of the next page, if there is one.
fn page_info(connection: &Value) -> Result<Option<String>, GitHubError> {
    let info = &connection["pageInfo"];
    if info["hasNextPage"]
        .as_bool()
        .ok_or_else(|| malformed("pageInfo"))?
    {
        Ok(Some(string(info, "endCursor")?))
    } else {
        Ok(None)
    }
}

fn issue_and_status(node: &Value) -> Result<(IssueRef, IssueStatus), GitHubError> {
    let repository = &node["repository"];
    let issue = IssueRef::new(
        string(&repository["owner"], "login")?,
        string(repository, "name")?,
        node["number"]
            .as_u64()
            .ok_or_else(|| malformed("issue number"))?,
    )
    .map_err(|e| GitHubError::Failed(format!("malformed GitHub response: {e}")))?;
    let status = match string(node, "state")?.as_str() {
        "OPEN" => IssueStatus::Open,
        "CLOSED" => IssueStatus::Closed,
        other => return Err(malformed(&format!("issue state {other}"))),
    };
    Ok((issue, status))
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>, GitHubError> {
    value[key].as_array().ok_or_else(|| malformed(key))
}

fn string(value: &Value, key: &str) -> Result<String, GitHubError> {
    value[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| malformed(key))
}

fn malformed(what: &str) -> GitHubError {
    GitHubError::Failed(format!(
        "malformed GitHub response: missing or invalid {what}"
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Serve one JSON response per connection, in order, and return the base URL and a handle
    /// yielding the request bodies received.
    async fn serve(responses: Vec<Value>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buf = vec![0; 8192];
                let body_start = loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    request.extend_from_slice(&buf[..n]);
                    if let Some(i) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&request[..body_start]).to_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                while request.len() < body_start + length {
                    let n = stream.read(&mut buf).await.unwrap();
                    request.extend_from_slice(&buf[..n]);
                }
                requests.push(serde_json::from_slice(&request[body_start..]).unwrap());
                let body = response.to_string();
                let http = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(http.as_bytes()).await.unwrap();
            }
            requests
        });
        (url, handle)
    }

    /// A recorded GraphQL response, captured with `gh api graphql` against Quinten1505/chimera
    /// using these queries (page sizes lowered in the recordings that exercise pagination).
    fn fixture(name: &str) -> Value {
        let path = format!("{}/tests/fixtures/{name}.json", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn spec() -> IssueRef {
        IssueRef::new("Quinten1505", "chimera", 5).unwrap()
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("Quinten1505", "chimera", number).unwrap()
    }

    async fn read_spec(
        specification: &IssueRef,
        responses: Vec<Value>,
    ) -> (Result<TicketPlan, GitHubError>, Vec<Value>) {
        let (url, requests) = serve(responses).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_secs(5));
        let result = client.read_plan(specification).await;
        (result, requests.await.unwrap())
    }

    async fn read(responses: Vec<Value>) -> (Result<TicketPlan, GitHubError>, Vec<Value>) {
        read_spec(&spec(), responses).await
    }

    fn ticket(number: u64, status: IssueStatus, blockers: &[(u64, IssueStatus)]) -> Ticket {
        Ticket {
            issue: issue(number),
            status,
            blockers: blockers
                .iter()
                .map(|&(n, status)| Blocker {
                    issue: issue(n),
                    status,
                })
                .collect(),
        }
    }

    use IssueStatus::{Closed, Open};

    /// The plan of #5 at the time of recording.
    fn recorded_tickets() -> Vec<Ticket> {
        vec![
            ticket(35, Closed, &[(13, Closed)]),
            ticket(36, Open, &[(11, Closed), (35, Closed)]),
            ticket(37, Closed, &[(9, Closed), (35, Closed)]),
            ticket(38, Closed, &[(9, Closed), (35, Closed)]),
            ticket(
                39,
                Open,
                &[(38, Closed), (37, Closed), (36, Open), (14, Closed)],
            ),
        ]
    }

    #[tokio::test]
    async fn no_sub_issues_is_an_empty_plan() {
        let specification = IssueRef::new("Quinten1505", "chimera", 36).unwrap();
        let (plan, requests) = read_spec(&specification, vec![fixture("plan_no_sub_issues")]).await;
        assert_eq!(plan.unwrap(), TicketPlan { tickets: vec![] });
        assert_eq!(requests[0]["variables"]["owner"], "Quinten1505");
        assert_eq!(requests[0]["variables"]["name"], "chimera");
        assert_eq!(requests[0]["variables"]["number"], 36);
    }

    #[tokio::test]
    async fn maps_open_and_closed_sub_issues_with_blockers() {
        let plan = read(vec![fixture("plan_all")]).await.0.unwrap();
        assert_eq!(plan.tickets, recorded_tickets());
    }

    /// No cross-repository blocker exists in public data to record, so this one response is
    /// hand-written in the recorded shape.
    #[tokio::test]
    async fn cross_repository_blockers_are_returned_with_their_status() {
        let mut response = fixture("plan_sub_issues_page3");
        response["data"]["repository"]["issue"]["subIssues"]["nodes"][0]["blockedBy"]["nodes"] = json!([
            { "number": 7, "state": "OPEN",
              "repository": { "name": "elsewhere", "owner": { "login": "other" } } }
        ]);
        let plan = read(vec![response]).await.0.unwrap();
        assert_eq!(
            plan.tickets[0].blockers,
            vec![Blocker {
                issue: IssueRef::new("other", "elsewhere", 7).unwrap(),
                status: Open,
            }]
        );
    }

    #[tokio::test]
    async fn reads_every_page_of_sub_issues() {
        let pages = [
            "plan_sub_issues_page1",
            "plan_sub_issues_page2",
            "plan_sub_issues_page3",
        ]
        .map(fixture)
        .to_vec();
        let (plan, requests) = read(pages).await;
        assert_eq!(plan.unwrap().tickets, recorded_tickets());
        assert_eq!(requests[0]["variables"]["after"], Value::Null);
        assert_eq!(requests[1]["variables"]["after"], "Mg");
        assert_eq!(requests[2]["variables"]["after"], "NA");
    }

    #[tokio::test]
    async fn reads_every_page_of_blocked_by() {
        let first = fixture("plan_blocked_by_page1");
        let nodes = &first["data"]["repository"]["issue"]["subIssues"]["nodes"];
        let id = nodes[4]["id"].as_str().unwrap().to_owned();
        let cursor = nodes[4]["blockedBy"]["pageInfo"]["endCursor"]
            .as_str()
            .unwrap()
            .to_owned();
        let (plan, requests) = read(vec![first, fixture("blockers_page2")]).await;
        assert_eq!(plan.unwrap().tickets, recorded_tickets());
        assert_eq!(requests[1]["variables"]["id"], id);
        assert_eq!(requests[1]["variables"]["after"], cursor);
    }

    #[tokio::test]
    async fn missing_specification_is_failed_with_clear_cause() {
        let (plan, _) = read(vec![json!({ "data": { "repository": { "issue": null } } })]).await;
        assert_eq!(
            plan,
            Err(GitHubError::Failed(
                "specification issue Quinten1505/chimera#5 was not found".to_owned()
            ))
        );
    }

    #[tokio::test]
    async fn missing_repository_is_failed() {
        let (plan, _) = read(vec![json!({ "data": { "repository": null } })]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(c)) if c.contains("not found")));
    }

    #[tokio::test]
    async fn recorded_missing_issue_is_failed_with_cause() {
        let (plan, _) = read(vec![fixture("plan_missing_issue")]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(c)) if c.contains("Could not resolve")));
    }

    #[tokio::test]
    async fn malformed_response_is_failed() {
        let mut page = fixture("plan_all");
        page["data"]["repository"]["issue"]["subIssues"]["nodes"][0]["state"] = json!("WEIRD");
        let (plan, _) = read(vec![page]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(_))));
    }
}
