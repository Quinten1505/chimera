//! `Repository` adapter that invokes the `git` CLI.
//!
//! Expose the module interface here; keep implementation submodules private.

mod branch;
mod error;
mod remote_head;
mod repository;
mod runner;
#[cfg(test)]
mod testing;
mod worktree;

pub use repository::GitRepository;
