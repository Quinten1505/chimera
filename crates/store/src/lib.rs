//! File-backed run store: persists run state as JSON files under the XDG state directory.
//!
//! Expose the module interface here; keep implementation submodules private.

mod atomic;
mod error;
mod history;
mod merge_lock;
mod paths;
mod run_data;

pub use error::StoreError;
pub use history::{HistoryEntry, TurnKind, append_history};
pub use merge_lock::{MergeLockEntry, MergeLockState, load_merge_lock, save_merge_lock};
pub use paths::{run_directory, state_root};
pub use run_data::{RunData, RunInput, load_run_data, save_run_data};
