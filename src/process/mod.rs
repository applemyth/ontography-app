//! OS processes that cannot outlive their owner.
//!
//! A supervisor shell leads each process group. It starts its program only
//! after the owner has saved a lease naming it and sent a permit over the
//! supervisor's lifetime pipe. When that pipe closes, even because the owner
//! died without running destructors, the supervisor stops its whole group.
//! Before starting a replacement, the next owner calls [`recover_process`],
//! which verifies what the lease names before signalling it, or
//! [`recover_patiently`], which also waits while a previous worker that no
//! signal may reach is still exiting.
//!
//! A directory holds one lease, so it hosts one supervised process at a time.
//! The piped supervisor here serves command tasks and headless agents.
//! Terminal agents have their own PTY supervisor and share the lease and its
//! recovery.

mod lease;
mod piped;

pub(crate) use lease::lease_process;
pub use lease::{recover_patiently, recover_process};
pub use piped::{Stdin, SupervisedProcess, spawn_supervised};

use crate::AppError;

/// Whether this server's child `pid` has exited, reaped or not. It waits
/// without reaping, so the ID stays reserved for whoever reaps it; a child
/// already reaped is no longer ours to wait for. A stopped child still runs.
pub(crate) fn exited(pid: i32) -> bool {
    use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
    let Some(pid) = Pid::from_raw(pid) else {
        return false;
    };
    match waitid(
        WaitId::Pid(pid),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
    ) {
        Ok(status) => status.is_some(),
        Err(error) => error == rustix::io::Errno::CHILD,
    }
}

/// Supervision failures keep the class command workers have always reported
/// them under.
fn failure(error: impl std::fmt::Display) -> AppError {
    AppError::new("workflow_worker", error.to_string())
}
