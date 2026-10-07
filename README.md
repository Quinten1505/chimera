# chimera

Rust modular monolith (edition 2024). One executable composes four library
modules in a Cargo workspace. All modules run in the same process.

## Structure

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | Composition root: load configuration, construct modules, and start the application. |
| `crates/core` | Shared application concepts. |
| `crates/herdr` | `Terminal` adapter (`HerdrTerminal`) over Herdr's Unix socket. |
| `crates/pipelines` | The four orchestration pipelines and the shared services. |
| `crates/github` | `Forge` adapter: specification plans, issues, and pull requests through the GitHub API. |
| `crates/git` | `Repository` adapter: branches, worktrees, and remote heads through the `git` CLI. |
| `crates/configuration` | Loading and validating the YAML configuration: ticket and final agent profiles, limits, and Codex launch arguments. |

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

Pipelines remains a scaffold. Configuration loads and validates the YAML settings (see below). The `crates/herdr` crate provides the `Terminal` adapter below. The executable retains its initial hello-world output.

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

The `crates/herdr` crate (`chimera-herdr`) implements core's `Terminal` port as
`HerdrTerminal` over Herdr's local Unix socket API (Linux, macOS, and WSL; native
Windows named pipes are not implemented). Start Herdr first and supply its socket
path explicitly, for example from `HERDR_SOCKET_PATH` or `~/.config/herdr/herdr.sock`.
The public API is only `HerdrTerminal::connect`, `HerdrTerminal`, and `HerdrError`.

```rust
use std::sync::Arc;
use chimera_core::terminal::Terminal;
use chimera_herdr::HerdrTerminal;

async fn provision() -> Result<(), Box<dyn std::error::Error>> {
    let terminal: Arc<dyn Terminal> =
        Arc::new(HerdrTerminal::connect(std::env::var("HERDR_SOCKET_PATH")?).await?);
    let (workspace, pane) = terminal.create_workspace(&std::env::current_dir()?).await?;
    let second = terminal.split_pane(&workspace, &pane).await?;
    terminal.launch_agent(&second, "codex --model gpt-6").await?;
    terminal.send_prompt(&second, "hello").await?;
    Ok(())
}
```

The adapter knows nothing about roles, prompts, results, or providers. `launch_agent`
takes a ready command line (built by `chimera-configuration`), types it into the pane's
shell and returns once Herdr reports an agent on the pane (up to 30 seconds). Herdr's
statuses map to `TurnStatus`: working, blocked and unknown are *running*; idle and done are
*finished*; a missing agent, pane or workspace is *gone*. There is no inactivity timeout.
`read_output` returns the recent output as plain text, capped at 500 lines and 64 KiB (the
end is kept). `close_workspace` succeeds when the workspace is already gone.

**Reconnecting.** `HerdrTerminal` keeps no workspace, pane, or agent state in memory; every
operation addresses Herdr by the IDs it is given. After a Chimera restart, build a new
instance from the socket path alone and use the IDs saved earlier. The count behind
`prompts_received` is kept in Herdr itself: each send records its own pane metadata token
(`chimera_p_<nonce>`) before sending, so concurrent sends and restarts never overwrite one
another. The token is confirmed on delivery and removed on a certain failure; a send whose
outcome is unknown leaves it pending, and `prompts_received` then reports *uncertain* instead
of a count. Herdr allows 32 tokens per pane, which bounds the prompts one pane can record (a
send past that is *failed* and sends nothing). `create_workspace` labels the workspace
`chimera:<root key>` in the creating request, and `find_workspace` finds a workspace by that
root token, not by any pane's working directory, so it works after a lost reply or a restart.
Renaming the workspace in Herdr discards its identity.

**Errors.** Every `HerdrError` is *failed* (known not to have happened) or *uncertain*
(a state-changing request was written and its outcome is unknown, so inspect Herdr before
retrying); both convert into `PortError`. Requests use a fresh connection and a 30-second
timeout, and nothing is retried automatically.

Unit tests use isolated fake Unix sockets. Integration tests (`crates/herdr/tests`) drive a
real Herdr through `dyn Terminal` and are skipped when `HERDR_SOCKET_PATH` is unset or
unreachable:
```sh
HERDR_SOCKET_PATH="$HOME/.config/herdr/herdr.sock" cargo test -p chimera-herdr --test terminal
```

The request shapes were checked against the installed Herdr 0.9.3 schema
(protocol 22); see the [Herdr socket API](https://herdr.dev/docs/socket-api/).

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

## Configuration

The `crates/configuration` crate (`chimera-configuration`) turns a YAML file into
validated agent configurations or an error that stops startup. It depends only on
`chimera-core`. `chimera.example.yaml` is a complete, documented example; the
`chimera` binary reads `chimera.yaml` by default (or the path given as its first
argument).

```rust,no_run
use chimera_configuration::load;

fn settings() -> Result<(), Box<dyn std::error::Error>> {
    let configuration = load("chimera.yaml")?;
    let review = &configuration.ticket.review;
    println!("{} {}", review.provider, review.model);
    println!("{} merge attempts", configuration.limits.merge_attempts);
    Ok(())
}
```

`load` returns a `Configuration` with two `AgentConfiguration`s, `ticket` (the
per-ticket pipeline) and `final_review` (the `final:` key, the pipeline over the
integrated feature branch), plus the `Limits`. Each set has `implementation`,
`review`, and `merge` profiles with a required `provider`, `model`, and
`prompt_template`, and optional provider-specific `settings`. Every role in both
sets must be present. Test requirements, TDD, review scope, and commands belong
in the prompt templates; the configuration encodes no test policy.

Optional values default as follows:

| Setting | Default |
| --- | --- |
| `providers.<name>.reset_command` (context reset command) | `/clear` |
| `limits.implementation_review_cycles` | 100 |
| `limits.merge_attempts` | 100 |
| `limits.final_review_fix_cycles` | 100 |
| `limits.agent_recovery` | 5 |
| `limits.github_retries` | 5 |

Limits must be positive integers. Only `codex` is a supported provider. Unknown
keys, missing roles or prompt templates, unsupported providers, invalid limits,
and strings that are empty or contain control characters (prompt templates may
span lines) are rejected. Errors are `ConfigurationError`s that name the
offending field path (for example `ticket.review.settings.typo`) and, for YAML
syntax errors, the line and column.

`codex_launch_args(&profile)` builds the Codex argv from a profile so the Herdr
adapter stays provider-agnostic: `--model <model>`, plus `--config
model_reasoning_effort="…"` for `reasoning_effort`, `--config service_tier="…"`
for `service_tier`, and `--approve-for-me` when `approve_for_me` is true. These
are the only Codex settings; others are rejected.

The earlier Codex-only `codex.yaml` format has been removed; migrate to the
format above.

```sh
cargo test -p chimera-configuration
```
