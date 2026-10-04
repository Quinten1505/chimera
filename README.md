# chimera

Rust modular monolith (edition 2024). One executable composes four library
modules in a Cargo workspace. All modules run in the same process.

## Structure

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | Composition root: load configuration, construct modules, and start the application. |
| `modules/core` | Shared domain concepts and rules, independent of configuration and external systems. |
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

This is an architectural scaffold: module interfaces are intentionally empty
until behavior is added. The executable retains its initial hello-world output.

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
