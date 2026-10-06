# chimera

Rust modular monolith (edition 2024). One executable composes four library
modules in a Cargo workspace. All modules run in the same process.

## Structure

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | Composition root: load configuration, construct modules, and start the application. |
| `crates/core` | Shared application concepts. |
| `crates/herdr` | Herdr client: connection, workspace, pane, session, and agent operations. |
| `crates/pipelines` | The four orchestration pipelines and the shared services. |
| `crates/github` | `Forge` adapter: specification plans, issues, and pull requests through the GitHub API. |
| `crates/git` | `Repository` adapter: branches, worktrees, and remote heads through the `git` CLI. |
| `crates/configuration` | Loading and validating application settings. |

Package names use the `chimera-` prefix (for example, `chimera-core`) to
avoid colliding with Rust's built-in `core` crate.

## Module interfaces and dependencies

Each module exposes its interface from `src/lib.rs`. Keep implementation
submodules private and explicitly re-export only what callers need. Cargo
dependencies declare which module interfaces a crate may use; Rust visibility
protects each module's implementation.

The initial dependency graph is:

```text
chimera (executable)
  -> core
  -> pipelines -> core
  -> github -> core
  -> git -> core
  -> configuration
```

Core and configuration have no internal dependencies. Pipelines, GitHub, and Git
may use core, but do not depend on each other. The executable owns module assembly
and passes configuration into the modules that need it. Introduce interfaces
for cross-module behavior when concrete use cases require them, keeping this
dependency direction and avoiding cycles.

Pipelines remains a scaffold. Configuration loads Codex settings from YAML. The `crates/herdr` crate provides the Herdr
client below. The executable retains its initial hello-world output.

## Development

Install Rust and Cargo with support for edition 2024, then run these commands
from the repository root:

```sh
cargo run
cargo build --workspace
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

All workspace packages are default members, so plain `cargo build` and
`cargo test` also include every module. Run a focused module check with, for
example, `cargo test -p chimera-pipelines`. The deployable binary is
`target/release/chimera` after `cargo build --release -p chimera`.

## Herdr integration

The `crates/herdr` crate (`chimera-herdr`) exposes a synchronous client for Herdr's local Unix socket API (Linux,
macOS, and WSL). Start Herdr first and supply its socket path explicitly, for
example from HERDR_SOCKET_PATH or ~/.config/herdr/herdr.sock. Native Windows
named pipes are not implemented.

```rust
use chimera_herdr::{
    HerdrClient, PaneOptions, SplitDirection, WorkspaceOptions,
};

fn provision() -> Result<(), Box<dyn std::error::Error>> {
    let client = HerdrClient::connect(std::env::var("HERDR_SOCKET_PATH")?)?;
    let workspace = client.create_workspace(&WorkspaceOptions::new(std::env::current_dir()?))?;
    let pane = client.add_pane(&PaneOptions::new(
        workspace.root_pane.pane_id,
        SplitDirection::Right,
    ))?;
    println!("Created pane {}", pane.pane_id);
    Ok(())
}
```

Pane options accept an absolute working directory and focus
flag; call add_pane repeatedly to add more panes, targeting returned pane IDs.
Both operations default to leaving focus unchanged.

Connections are verified with ping. Requests use a fresh connection and a
30-second read/write timeout, configurable with connect_with_timeout. Transport,
JSON, protocol, validation, and server errors are returned as HerdrError.
A timeout can happen after a mutation succeeds; requests are not retried
automatically. Repository trust policy remains Herdr-owned.

Tests use isolated fake Unix sockets. An optional read-only live connection test:
```sh
HERDR_SOCKET_PATH="$HOME/.config/herdr/herdr.sock" cargo test -p chimera-herdr live_connection -- --ignored
```

The request shapes were checked against the installed Herdr 0.9.3 schema
(protocol 22); see the [Herdr socket API](https://herdr.dev/docs/socket-api/).

## Starting an application session

Call HerdrClient::start_session with an application-owned ID and
SessionTarget::Workspace for an existing directory.

```rust
use chimera_herdr::{HerdrClient, Session, SessionTarget, WorkspaceOptions};

fn start(client: &HerdrClient) -> Result<Session, chimera_herdr::HerdrError> {
    client.start_session(
        "my-session",
        &SessionTarget::Workspace(WorkspaceOptions::new("/home/me/git/project")),
    )
}
```

The returned Session records the workspace ID, initial pane ID, and known
checkout path. It is populated only when Herdr reports checkout
metadata; a pane's working directory alone does not establish a Git checkout.
State is held in the returned struct and supports Serde serialization. Startup
does not write a session file or launch an AI agent. Additional panes created
with add_pane must be recorded in the session by the caller.

## Starting Codex agents in an existing session

Load the checked-in `codex.yaml` and launch one Codex agent per tracked pane:

```rust,no_run
use chimera::start_session_agents;
use chimera_herdr::{HerdrClient, Session};

fn launch(client: &HerdrClient, session: &mut Session) -> Result<(), Box<dyn std::error::Error>> {
    start_session_agents(client, session, "codex.yaml")
}
```

The YAML defines named profiles under `agents`. Each requires `kind: codex`,
`model`, `reasoning_effort`, `service_tier`, and
`approve_for_me`. The example selects `gpt-6-luna`, medium reasoning, fast
service, and automatic approval review for `builder`; `reviewer` uses high reasoning.
`start_session_agents` currently selects `builder` for every pane in Rust.
Other callers can select profiles with `CodexConfiguration::load(path)?.profile(name)?`. Unknown fields and empty string
settings are rejected before launching. Codex validates model-specific setting
support. The mapping uses Codex's
[configuration overrides](https://learn.chatgpt.com/docs/config-file/config-reference)
and the installed CLI's `--approve-for-me` option.

Herdr's `agent.start` launches into an existing shell pane and waits for
interactive readiness. Each successful launch is appended to
`WorkspacePanes.agents`, including its provider session reference when available.
Already tracked panes are skipped. Launching stops at the first error and retains
earlier successes; it is not a transaction and does not roll them back. A timeout
can leave an agent running without a recorded success, so inspect Herdr before
retrying. Startup allows Herdr 30 seconds for readiness and at least 35 seconds
for the socket response.

This helper consumes an already populated session. It does not create the layout,
save session files, or change the executable's hello-world entry point.

## Git integration

The `crates/git` crate (`chimera-git`) implements core's `Repository` port as
`GitRepository` by running the `git` CLI; there is no libgit2 dependency, so
`git` must be installed and on `PATH` at runtime. Construct it with the
repository's working directory, which must have an `origin` remote:

```rust,no_run
use chimera_core::repository::Repository;
use chimera_git::GitRepository;

async fn base() -> Result<(), chimera_core::error::PortError> {
    let repo = GitRepository::new("/home/me/git/project");
    let base = repo.resolve_base_branch().await?;
    println!("Base branch: {}", base.as_str());
    Ok(())
}
```

The base branch is `develop` if `origin` has one, otherwise the remote's
default branch. Feature branches are created from the fetched base and pushed
with upstream tracking; task worktrees are created from the feature branch.
`remote_head` queries `origin` directly with `git ls-remote`, so it reflects
pushes that were never fetched locally; it returns `None` only when the branch
is absent, and an unreachable remote is an error.

Each `git` invocation runs once, without retries, on a blocking thread, with
interactive prompts disabled (credential, SSH, editor, and pager). Failures are
reported as `PortError`s classified as failed (the effect did not happen) or
uncertain (a push whose outcome is unknown, such as a dropped connection).
Operations tolerate already-applied state where it is safe, for example
removing a missing worktree or recreating an existing branch at the expected
commit, so they can be repeated during reconciliation after a restart.

Tests run against real repositories with a bare `origin` in temporary
directories and need only `git`:

```sh
cargo test -p chimera-git
```

## GitHub integration

The `crates/github` crate (`chimera-github`) implements core's `Forge` port as
`GitHubForge` over the GitHub REST and GraphQL APIs. Construct it with an API
token and the repository in which it opens pull requests:

```rust,no_run
use chimera_core::IssueRef;
use chimera_core::forge::Forge;
use chimera_github::GitHubForge;

async fn plan() -> Result<(), Box<dyn std::error::Error>> {
    let forge = GitHubForge::new(std::env::var("GITHUB_TOKEN")?, "octo", "project");
    let plan = forge.read_plan(&IssueRef::new("octo", "project", 5)?).await?;
    println!("{} tickets", plan.tickets.len());
    Ok(())
}
```

The token is sent as a bearer token; the crate does not read it from the
environment or configuration itself. Requests always go to
`https://api.github.com`; a different base API URL (such as GitHub Enterprise
Server) is not configurable. Opening and finding pull requests use the
repository given to `new`; every other operation addresses the repository of
its `IssueRef`.

`read_plan` returns the specification issue's sub-issues with their open or
closed status and blocked-by links, following every page. Blockers outside the
specification, including those in other repositories, are returned as given.
Draft pull requests are created through REST; creating one fails if a pull
request already exists for the head branch. Closing an issue (as completed)
and marking a pull request ready for review both succeed when already applied.

Each request is sent once, without retries, with a 30-second timeout.
Failures are reported as `PortError`s classified as failed (the effect did not
happen) or uncertain (a mutation whose outcome is unknown, such as a timeout or
an unconfirmed GraphQL result). Reads are never uncertain. Reconcile an
uncertain result with `find_open_pull_request`, `issue_status`, or
`pull_request_is_draft`.

Unit tests use local fake HTTP servers and need no network:

```sh
cargo test -p chimera-github
```

Opt-in integration tests run against real GitHub. They create and close real
issues, branches, and pull requests, so they are ignored by default. Use a
scratch repository you do not mind cluttering with closed issues (GitHub cannot
delete them) and a token with read and write access to its issues, pull
requests, and contents:

```sh
CHIMERA_GITHUB_TOKEN=<token> CHIMERA_GITHUB_REPOSITORY=<owner>/<scratch-repo> \
    cargo test -p chimera-github --test integration -- --ignored
```

Each test closes the issues and pull requests it created and deletes its branch,
even when an assertion fails.
