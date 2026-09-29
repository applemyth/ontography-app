//! OS processes that cannot outlive their owner.
//!
//! A supervisor shell leads each process group. It starts its program only
//! after the owner has saved a lease naming it and sent a permit over the
//! supervisor's lifetime pipe. When that pipe closes, even because the owner
//! died without running destructors, the supervisor stops its whole group.
//! Before starting a replacement, the next owner calls [`recover_process`],
//! which verifies what the lease names before signalling it.
//!
//! A directory holds one lease, so it hosts one supervised process at a time.
//! The piped supervisor here serves command tasks and headless agents.
//! Terminal agents have their own PTY supervisor and share the lease and its
//! recovery.

mod lease;
mod piped;

pub(crate) use lease::lease_process;
pub use lease::recover_process;
pub use piped::{Stdin, SupervisedProcess, spawn_supervised};

use crate::AppError;

/// Supervision failures keep the class command workers have always reported
/// them under.
fn failure(error: impl std::fmt::Display) -> AppError {
    AppError::new("workflow_worker", error.to_string())
}
