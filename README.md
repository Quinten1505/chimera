# chimera

Rust modular monolith (edition 2024). One executable composes four library
modules in a Cargo workspace. All modules run in the same process.

## Structure

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | Composition root: load configuration, construct modules, and start the application. |
| `modules/core` | Shared application concepts and Herdr connection, worktree, and pane operations. |
| `modules/workflow` | Workflow use cases and orchestration. |
| `modules/github` | GitHub integration and translation between GitHub data and domain concepts. |
| `modules/configuration` | Loading and validating application settings. |

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
  -> workflow -> core
  -> github -> core
  -> configuration
```

Core and configuration have no internal dependencies. Workflow and GitHub may
use core, but do not depend on each other. The executable owns module assembly
and passes configuration into the modules that need it. Introduce interfaces
for cross-module behavior when concrete use cases require them, keeping this
dependency direction and avoiding cycles.

Workflow and GitHub remain scaffolds. Configuration loads Codex settings from YAML. Core provides the Herdr
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

All five packages are default workspace members, so plain `cargo build` and
`cargo test` also include every module. Run a focused module check with, for
example, `cargo test -p chimera-workflow`. The deployable binary is
`target/release/chimera` after `cargo build --release -p chimera`.

## Herdr integration

Core exposes a synchronous client for Herdr's local Unix socket API (Linux,
macOS, and WSL). Start Herdr first and supply its socket path explicitly, for
example from HERDR_SOCKET_PATH or ~/.config/herdr/herdr.sock. Native Windows
named pipes are not implemented.

```rust
use chimera_core::{
    HerdrClient, PaneOptions, SplitDirection, WorktreeOptions, WorktreeSource,
};

fn provision() -> Result<(), Box<dyn std::error::Error>> {
    let client = HerdrClient::connect(std::env::var("HERDR_SOCKET_PATH")?)?;
    let options = WorktreeOptions::new(
        WorktreeSource::Directory { cwd: std::env::current_dir()? },
        "feature/my-task",
    );
    let worktree = client.create_worktree(&options)?;
    let pane = client.add_pane(&PaneOptions::new(
        worktree.root_pane.pane_id,
        SplitDirection::Right,
    ))?;
    println!("Created pane {}", pane.pane_id);
    Ok(())
}
```

Use WorktreeSource::Workspace to target an existing Herdr workspace instead.
Worktree options also accept a base revision, absolute checkout path, label, and
focus flag. Herdr checks out an existing branch or creates one from the base
(default HEAD). Pane options accept an absolute working directory and focus
flag; call add_pane repeatedly to add more panes, targeting returned pane IDs.
Both operations default to leaving focus unchanged.

Connections are verified with ping. Requests use a fresh connection and a
30-second read/write timeout, configurable with connect_with_timeout. Transport,
JSON, protocol, validation, and server errors are returned as HerdrError.
A timeout can happen after a mutation succeeds; requests are not retried
automatically. Repository trust and worktree location policies remain Herdr-owned.

Tests use isolated fake Unix sockets. An optional read-only live connection test:
```sh
HERDR_SOCKET_PATH="$HOME/.config/herdr/herdr.sock" cargo test -p chimera-core live_connection -- --ignored
```

The request shapes were checked against the installed Herdr 0.9.3 schema
(protocol 22); see the [Herdr socket API](https://herdr.dev/docs/socket-api/).

## Starting an application session

Call HerdrClient::start_session with an application-owned ID and either
SessionTarget::Workspace for an existing directory or SessionTarget::Worktree
with WorktreeOptions for a new checkout.

```rust
use chimera_core::{HerdrClient, Session, SessionTarget, WorkspaceOptions};

fn start(client: &HerdrClient) -> Result<Session, chimera_core::HerdrError> {
    client.start_session(
        "my-session",
        &SessionTarget::Workspace(WorkspaceOptions::new("/home/me/git/project")),
    )
}
```

The returned Session records the workspace ID, initial pane ID, and known
checkout path. For new worktrees, the path comes from Herdr's creation result.
For ordinary workspaces, it is populated only when Herdr reports checkout
metadata; a pane's working directory alone does not establish a Git checkout.
State is held in the returned struct and supports Serde serialization. Startup
does not write a session file or launch an AI agent. Additional panes created
with add_pane must be recorded in the session by the caller.

## Starting Codex agents in an existing session

Load the checked-in `codex.yaml` and launch one Codex agent per tracked pane:

```rust,no_run
use chimera::start_session_agents;
use chimera_core::{HerdrClient, Session};

fn launch(client: &HerdrClient, session: &mut Session) -> Result<(), Box<dyn std::error::Error>> {
    start_session_agents(client, session, "codex.yaml")
}
```

The YAML requires `model`, `reasoning_effort`, `service_tier`, and
`approve_for_me`. The example selects `gpt-6-luna`, medium reasoning, fast
service, and automatic approval review. Unknown fields and empty string
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
