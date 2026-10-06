//! File-backed run store: persists run state as JSON files under the XDG state directory.
//!
//! Expose the module interface here; keep implementation submodules private.

mod atomic;
mod error;
mod file_run_store;
mod history;
mod merge_lock;
mod paths;
mod pipeline;
mod run_data;

pub use error::StoreError;
pub use file_run_store::FileRunStore;
