//! `Repository` adapter that invokes the `git` CLI.
//!
//! Expose the module interface here; keep implementation submodules private.

mod error;
// Unused until the operation tickets build on the runner.
#[allow(dead_code)]
mod runner;
#[cfg(test)]
mod testing;

pub use error::GitError;
