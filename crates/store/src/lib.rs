//! File-backed run store: persists run state as JSON files under the XDG state directory.
//!
//! Expose the module interface here; keep implementation submodules private.

mod atomic;
mod error;
mod history;
mod paths;

pub use error::StoreError;
pub use history::{HistoryEntry, TurnKind, append_history};
pub use paths::{run_directory, state_root};
