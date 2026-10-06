//! `Repository` adapter that invokes the `git` CLI.
//!
//! Expose the module interface here; keep implementation submodules private.

// Unused until the Repository adapter builds on the operations.
#[allow(dead_code)]
mod branch;
mod error;
// Unused until the operation tickets build on the runner.
#[allow(dead_code)]
mod remote_head;
#[allow(dead_code)]
mod runner;
#[cfg(test)]
mod testing;
// Unused until the adapter wires the operations into `Repository`.
#[allow(dead_code)]
mod worktree;

pub use error::GitError;
