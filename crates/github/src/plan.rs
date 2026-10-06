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

    fn spec() -> IssueRef {
        IssueRef::new("octo", "repo", 5).unwrap()
    }

    fn issue(number: u64) -> IssueRef {
        IssueRef::new("octo", "repo", number).unwrap()
    }

    fn node(number: u64, state: &str) -> Value {
        node_in("octo", "repo", number, state)
    }

    fn node_in(owner: &str, repo: &str, number: u64, state: &str) -> Value {
        json!({ "number": number, "state": state,
                "repository": { "name": repo, "owner": { "login": owner } } })
    }

    fn sub_issue(number: u64, state: &str, blockers: Vec<Value>, next: Option<&str>) -> Value {
        let mut n = node(number, state);
        n["id"] = json!(format!("id{number}"));
        n["blockedBy"] = connection(blockers, next);
        n
    }

    fn connection(nodes: Vec<Value>, next: Option<&str>) -> Value {
        json!({ "nodes": nodes,
                "pageInfo": { "hasNextPage": next.is_some(), "endCursor": next } })
    }

    fn plan_page(sub_issues: Vec<Value>, next: Option<&str>) -> Value {
        json!({ "data": { "repository": { "issue": {
            "subIssues": connection(sub_issues, next) } } } })
    }

    async fn read(responses: Vec<Value>) -> (Result<TicketPlan, GitHubError>, Vec<Value>) {
        let (url, requests) = serve(responses).await;
        let client = Client::with_base_url("tok", url).with_timeout(Duration::from_secs(5));
        let result = client.read_plan(&spec()).await;
        (result, requests.await.unwrap())
    }

    fn ticket(number: u64, status: IssueStatus, blockers: Vec<Blocker>) -> Ticket {
        Ticket {
            issue: issue(number),
            status,
            blockers,
        }
    }

    fn blocker(issue: IssueRef, status: IssueStatus) -> Blocker {
        Blocker { issue, status }
    }

    #[tokio::test]
    async fn no_sub_issues_is_an_empty_plan() {
        let (plan, requests) = read(vec![plan_page(vec![], None)]).await;
        assert_eq!(plan.unwrap(), TicketPlan { tickets: vec![] });
        assert_eq!(requests[0]["variables"]["owner"], "octo");
        assert_eq!(requests[0]["variables"]["name"], "repo");
        assert_eq!(requests[0]["variables"]["number"], 5);
    }

    #[tokio::test]
    async fn maps_open_and_closed_sub_issues_with_blockers() {
        let page = plan_page(
            vec![
                sub_issue(10, "CLOSED", vec![], None),
                sub_issue(11, "OPEN", vec![node(10, "CLOSED"), node(12, "OPEN")], None),
                sub_issue(12, "OPEN", vec![], None),
            ],
            None,
        );
        let plan = read(vec![page]).await.0.unwrap();
        assert_eq!(
            plan.tickets,
            vec![
                ticket(10, IssueStatus::Closed, vec![]),
                ticket(
                    11,
                    IssueStatus::Open,
                    vec![
                        blocker(issue(10), IssueStatus::Closed),
                        blocker(issue(12), IssueStatus::Open)
                    ]
                ),
                ticket(12, IssueStatus::Open, vec![]),
            ]
        );
    }

    #[tokio::test]
    async fn external_blockers_are_returned_with_their_status() {
        let page = plan_page(
            vec![sub_issue(
                10,
                "OPEN",
                vec![
                    node_in("other", "elsewhere", 7, "OPEN"),
                    node_in("octo", "repo", 99, "CLOSED"),
                ],
                None,
            )],
            None,
        );
        let plan = read(vec![page]).await.0.unwrap();
        assert_eq!(
            plan.tickets[0].blockers,
            vec![
                blocker(
                    IssueRef::new("other", "elsewhere", 7).unwrap(),
                    IssueStatus::Open
                ),
                blocker(issue(99), IssueStatus::Closed),
            ]
        );
    }

    #[tokio::test]
    async fn reads_every_page_of_sub_issues() {
        let pages = vec![
            plan_page(vec![sub_issue(10, "OPEN", vec![], None)], Some("c1")),
            plan_page(vec![sub_issue(11, "CLOSED", vec![], None)], Some("c2")),
            plan_page(vec![sub_issue(12, "OPEN", vec![], None)], None),
        ];
        let (plan, requests) = read(pages).await;
        let numbers: Vec<u64> = plan
            .unwrap()
            .tickets
            .iter()
            .map(|t| t.issue.number())
            .collect();
        assert_eq!(numbers, [10, 11, 12]);
        assert_eq!(requests[0]["variables"]["after"], Value::Null);
        assert_eq!(requests[1]["variables"]["after"], "c1");
        assert_eq!(requests[2]["variables"]["after"], "c2");
    }

    #[tokio::test]
    async fn reads_every_page_of_blocked_by() {
        let first = plan_page(
            vec![
                sub_issue(10, "OPEN", vec![node(1, "OPEN")], Some("b1")),
                sub_issue(11, "OPEN", vec![node(3, "OPEN")], None),
            ],
            None,
        );
        let more =
            |nodes, next| json!({ "data": { "node": { "blockedBy": connection(nodes, next) } } });
        let responses = vec![
            first,
            more(vec![node(2, "CLOSED")], Some("b2")),
            more(vec![node(4, "OPEN")], None),
        ];
        let (plan, requests) = read(responses).await;
        let plan = plan.unwrap();
        let blocked_by = |t: &Ticket| {
            t.blockers
                .iter()
                .map(|b| b.issue.number())
                .collect::<Vec<_>>()
        };
        assert_eq!(blocked_by(&plan.tickets[0]), [1, 2, 4]);
        assert_eq!(plan.tickets[0].blockers[1].status, IssueStatus::Closed);
        assert_eq!(blocked_by(&plan.tickets[1]), [3]);
        assert_eq!(requests[1]["variables"]["id"], "id10");
        assert_eq!(requests[1]["variables"]["after"], "b1");
        assert_eq!(requests[2]["variables"]["after"], "b2");
    }

    #[tokio::test]
    async fn missing_specification_is_failed_with_clear_cause() {
        let (plan, _) = read(vec![json!({ "data": { "repository": { "issue": null } } })]).await;
        assert_eq!(
            plan,
            Err(GitHubError::Failed(
                "specification issue octo/repo#5 was not found".to_owned()
            ))
        );
    }

    #[tokio::test]
    async fn missing_repository_is_failed() {
        let (plan, _) = read(vec![json!({ "data": { "repository": null } })]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(c)) if c.contains("not found")));
    }

    #[tokio::test]
    async fn not_found_graphql_error_is_failed_with_cause() {
        let response = json!({
            "data": { "repository": { "issue": null } },
            "errors": [{ "type": "NOT_FOUND", "message": "Could not resolve to an issue" }]
        });
        let (plan, _) = read(vec![response]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(c)) if c.contains("Could not resolve")));
    }

    #[tokio::test]
    async fn malformed_response_is_failed() {
        let page = plan_page(vec![json!({ "number": 10, "state": "WEIRD" })], None);
        let (plan, _) = read(vec![page]).await;
        assert!(matches!(plan, Err(GitHubError::Failed(_))));
    }
}
