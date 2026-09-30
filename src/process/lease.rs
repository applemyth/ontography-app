//! The lease that names a live supervisor, and the recovery that verifies it
//! before signalling the process group it names.

use super::failure;
use crate::{Result, persistence::write_json};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessLease {
    pub(super) pid: i32,
    pub(super) token: String,
    pub(super) identity: String,
}

pub(super) fn lease_path(directory: &Path) -> PathBuf {
    directory.join("worker-process.json")
}

/// Record an owned terminal supervisor before permitting it to launch user code.
/// Terminal workers use the same verified recovery as command-task workers.
pub(crate) async fn lease_process(directory: &Path, pid: u32, token: &str) -> Result<()> {
    let pid = i32::try_from(pid).map_err(failure)?;
    let identity = startup_identity(
        || inspect_process_identity(pid, token),
        Duration::from_secs(3),
    )
    .await?;
    write_json(
        &lease_path(directory),
        &ProcessLease {
            pid,
            token: token.into(),
            identity,
        },
    )
    .map_err(failure)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ProcessIdentity {
    Gone,
    ArgumentsUnavailable,
    Verified(String),
}

fn classify_identity(pid: i32, token: &str, output: &[u8]) -> Result<ProcessIdentity> {
    use sha2::{Digest, Sha256};
    let text = String::from_utf8_lossy(output);
    let mut fields = text.split_whitespace().skip(5); // lstart has five fields.
    if fields.next().and_then(|value| value.parse::<i32>().ok()) != Some(pid) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    let command = fields.collect::<Vec<_>>().join(" ");
    if command == "(sh)" {
        // macOS can report the process name before argv becomes readable.
        return Ok(ProcessIdentity::ArgumentsUnavailable);
    }
    if !command.contains(&format!("workflow-worker-{token}")) {
        return Err(failure(
            "Saved worker process ownership is uncertain; no process was signalled",
        ));
    }
    Ok(ProcessIdentity::Verified(format!(
        "{:x}",
        Sha256::digest(output)
    )))
}

pub(super) async fn inspect_process_identity(pid: i32, token: &str) -> Result<ProcessIdentity> {
    use nix::{errno::Errno, sys::signal::kill, unistd::getpgid};
    match kill(Pid::from_raw(pid), None) {
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        Err(error) => {
            return Err(failure(format!(
                "Cannot inspect saved worker process: {error}"
            )));
        }
        Ok(()) => {}
    }
    let observed = Command::new("/bin/ps")
        .args([
            "-ww",
            "-p",
            &pid.to_string(),
            "-o",
            "lstart=",
            "-o",
            "pgid=",
            "-o",
            "command=",
        ])
        .kill_on_drop(true)
        .output()
        .await
        .map_err(failure)?;
    if !observed.status.success() {
        if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH) {
            return Ok(ProcessIdentity::Gone);
        }
        return Err(failure("Cannot verify the saved worker process identity"));
    }
    match getpgid(Some(Pid::from_raw(pid))) {
        Ok(group) if group == Pid::from_raw(pid) => {}
        Err(Errno::ESRCH) => return Ok(ProcessIdentity::Gone),
        _ => {
            return Err(failure(
                "Saved worker process ownership is uncertain; no process was signalled",
            ));
        }
    }
    classify_identity(pid, token, &observed.stdout)
}

pub(super) async fn startup_identity<F, Fut>(mut inspect: F, timeout: Duration) -> Result<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ProcessIdentity>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let identity = tokio::time::timeout_at(deadline, inspect())
            .await
            .map_err(|_| failure("Worker supervisor arguments were unavailable at startup"))??;
        match identity {
            ProcessIdentity::Verified(identity) => return Ok(identity),
            ProcessIdentity::Gone => {
                return Err(failure(
                    "Worker supervisor exited before its lease was saved",
                ));
            }
            ProcessIdentity::ArgumentsUnavailable => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(failure(
                        "Worker supervisor arguments were unavailable at startup",
                    ));
                }
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
                )
                .await;
            }
        }
    }
}

/// How long a verified supervisor has to clean up after SIGTERM, and a
/// force-stopped group has to vanish, before recovery reports the previous
/// worker as still exiting.
const GRACE: Duration = Duration::from_secs(3);

/// The code of a recovery that found the node's previous worker still
/// running. Starting a replacement waits and tries again; nothing overlaps.
pub(crate) const STILL_EXITING: &str = "worker_still_exiting";

fn still_exiting(why: &str) -> crate::AppError {
    crate::AppError::new(
        STILL_EXITING,
        format!(
            "The node's previous worker is still exiting ({why}); its replacement starts once it has gone"
        ),
    )
}

/// Whether the lease still names its own supervisor. A supervisor that is
/// gone, changed, or cannot be inspected is not proven: nothing is signalled.
async fn owned(lease: &ProcessLease) -> bool {
    matches!(
        inspect_process_identity(lease.pid, &lease.token).await,
        Ok(ProcessIdentity::Verified(identity)) if identity == lease.identity
    )
}

/// Whether the process group `pid` is gone within `bound`.
async fn group_gone_within(pid: i32, bound: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        match killpg(Pid::from_raw(pid), None) {
            Err(nix::errno::Errno::ESRCH) => return Ok(true),
            // macOS can briefly deny this probe while the group's members
            // are exiting. Only ESRCH establishes that cleanup finished.
            Ok(()) | Err(nix::errno::Errno::EPERM) => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(false);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => {
                return Err(failure(format!(
                    "Cannot verify worker group cleanup: {error}"
                )));
            }
        }
    }
}

/// Force-stops a verified supervisor's group. Jobs its session runs in other
/// process groups go first, while the supervisor, still alive, keeps its
/// session ID from naming anyone else; then the group itself.
async fn force_stop(pid: i32) -> Result<()> {
    let owner = Pid::from_raw(pid);
    tokio::task::spawn_blocking(move || crate::terminal::signal_session_groups(owner))
        .await
        .map_err(failure)?
        .map_err(failure)?;
    match killpg(owner, Signal::SIGKILL) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(error) => Err(failure(error)),
    }
}

/// Called before giving any replacement worker inputs. Never signal a saved PID
/// until its process group, random marker and complete start/command fingerprint
/// have been verified. The supervisor's lifetime pipe normally cleans it first.
///
/// A worker that outlives its grace, as one frozen or starved by a loaded
/// machine does, is force-stopped if its supervisor still proves itself. If
/// ownership is uncertain, or the group outlives the force-stop, recovery
/// reports [`STILL_EXITING`] and signals nothing more: one worker runs per node.
pub async fn recover_process(directory: &Path) -> Result<()> {
    let path = lease_path(directory);
    if !path.exists() {
        return Ok(());
    }
    let lease: ProcessLease = crate::persistence::read_json(&path).map_err(failure)?;
    uuid::Uuid::parse_str(&lease.token).map_err(failure)?;
    if lease.pid <= 1 {
        return Err(failure("Invalid saved worker process identity"));
    }
    if owned(&lease).await {
        // Let the verified supervisor finish its own cleanup before removing
        // the owner group. PTY agents may have jobs in other process groups;
        // killing the supervisor first would interrupt their cleanup helper.
        match nix::sys::signal::kill(Pid::from_raw(lease.pid), Signal::SIGTERM) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => return Err(failure(error)),
        }
    }
    if !group_gone_within(lease.pid, GRACE).await? {
        if !owned(&lease).await {
            return Err(still_exiting(
                "its group runs on, and its supervisor can no longer prove it owns it, so nothing is signalled",
            ));
        }
        force_stop(lease.pid).await?;
        if !group_gone_within(lease.pid, GRACE).await? {
            return Err(still_exiting("its group runs on after being force-stopped"));
        }
    }
    for suffix in ["input", "status"] {
        match tokio::fs::remove_file(directory.join(format!("{}.{}", lease.token, suffix))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(failure(error)),
        }
    }
    tokio::fs::remove_file(path).await.map_err(failure)?;
    Ok(())
}

/// Recovers as [`recover_process`] does before a node starts, but waits while
/// the previous worker is still exiting: it tries again with backoff until the
/// old group is gone, telling `waiting` why each time. Returns false if
/// `stopped` completes first, so the node starts nothing. Other failures end
/// the wait.
pub async fn recover_patiently(
    directory: &Path,
    mut waiting: impl FnMut(&crate::AppError),
    stopped: impl std::future::Future<Output = ()>,
) -> Result<bool> {
    tokio::pin!(stopped);
    let mut pause = Duration::from_millis(250);
    loop {
        let recovered = tokio::select! {
            biased;
            () = &mut stopped => return Ok(false),
            recovered = recover_process(directory) => recovered,
        };
        match recovered {
            Ok(()) => return Ok(true),
            Err(error) if error.code == STILL_EXITING => {
                waiting(&error);
                tokio::select! {
                    biased;
                    () = &mut stopped => return Ok(false),
                    () = tokio::time::sleep(pause) => {}
                }
                pause = (pause * 2).min(Duration::from_secs(5));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::{errno::Errno, sys::signal::kill, unistd::getpgid};
    use std::io::BufRead;
    use std::process::Stdio;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn wait_file(path: &Path) -> String {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(value) = std::fs::read_to_string(path)
                    && !value.is_empty()
                {
                    return value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the fixture did not start")
    }

    async fn wait_gone(pid: Pid) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while kill(pid, None) != Err(Errno::ESRCH) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the process was left running");
    }

    /// A worker as a crashed server leaves it: a supervisor leading its own
    /// session, with a job in another process group, and the lease naming
    /// it. Nothing of this test reaps it but a thread, as launchd would.
    async fn leased_worker(directory: &Path) -> (Pid, Pid) {
        let token = uuid::Uuid::new_v4().to_string();
        let job = directory.join("job.pid");
        let script = format!(
            "trap '' HUP TERM; set -m; sleep 60 & printf '%s' \"$!\" > '{}'; wait",
            job.display()
        );
        let mut supervisor = std::process::Command::new("perl")
            .args([
                "-e",
                r#"use POSIX; POSIX::setsid() or die; exec "/bin/sh", "-c", $ARGV[0], $ARGV[1]"#,
                &script,
                &format!("workflow-worker-{token}"),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let owner = Pid::from_raw(supervisor.id() as i32);
        std::thread::spawn(move || supervisor.wait());
        let job = Pid::from_raw(wait_file(&job).await.parse().unwrap());
        lease_process(directory, owner.as_raw() as u32, &token)
            .await
            .unwrap();
        (owner, job)
    }

    /// Ends the frozen groups if the test fails before recovery does.
    struct Frozen(Pid, Pid);

    impl Drop for Frozen {
        fn drop(&mut self) {
            for group in [self.0, self.1] {
                if getpgid(Some(group)) == Ok(group) {
                    let _ = killpg(group, Signal::SIGKILL);
                }
            }
        }
    }

    #[tokio::test]
    async fn recovery_force_stops_a_frozen_worker_and_its_session_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let (owner, job) = leased_worker(directory.path()).await;
        let job_group = getpgid(Some(job)).unwrap();
        assert_ne!(job_group, owner);
        let _frozen = Frozen(owner, job_group);
        // A loaded machine can keep a crashed server's worker from exiting;
        // stopped outright, it never runs its own cleanup.
        killpg(owner, Signal::SIGSTOP).unwrap();
        killpg(job_group, Signal::SIGSTOP).unwrap();
        recover_process(directory.path()).await.unwrap();
        wait_gone(job).await;
        wait_gone(owner).await;
        assert!(!lease_path(directory.path()).exists());
    }

    /// A group whose leader is gone while a member runs on. Removing `hold`
    /// ends the member; nothing else does.
    fn orphaned_group(hold: &Path) -> (Pid, Pid) {
        std::fs::write(hold, b"").unwrap();
        let mut leader = std::process::Command::new("perl")
            .args([
                "-e",
                r#"setpgrp(0, 0); my $pid = fork() // die; if ($pid) { print "$pid\n"; exit 0 } close STDOUT; select(undef, undef, undef, 0.05) while -e $ARGV[0]"#,
            ])
            .arg(hold)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(leader.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let group = Pid::from_raw(leader.id() as i32);
        assert!(leader.wait().unwrap().success());
        (group, Pid::from_raw(line.trim().parse().unwrap()))
    }

    #[tokio::test]
    async fn recovery_never_signals_a_group_it_cannot_prove_it_owns_and_waits_for_it() {
        let directory = tempfile::tempdir().unwrap();
        let hold = directory.path().join("hold");
        let (group, member) = orphaned_group(&hold);
        write_json(
            &lease_path(directory.path()),
            &ProcessLease {
                pid: group.as_raw(),
                token: uuid::Uuid::new_v4().to_string(),
                identity: "unproven".into(),
            },
        )
        .unwrap();
        // A stop ends the wait, and the lease stays for the next start.
        let stopped = recover_patiently(
            directory.path(),
            |_| {},
            tokio::time::sleep(Duration::from_millis(100)),
        )
        .await
        .unwrap();
        assert!(!stopped);
        assert!(lease_path(directory.path()).exists());
        // Otherwise it waits, saying why, until the group ends by itself.
        let waits = AtomicUsize::new(0);
        let wait = recover_patiently(
            directory.path(),
            |error| {
                assert_eq!(error.code, STILL_EXITING);
                waits.fetch_add(1, Ordering::Relaxed);
            },
            std::future::pending(),
        );
        let release = async {
            while waits.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // Nothing signalled it while ownership was unproven.
            assert!(kill(member, None).is_ok());
            std::fs::remove_file(&hold).unwrap();
        };
        let (recovered, ()) = tokio::join!(wait, release);
        assert!(recovered.unwrap());
        assert!(waits.load(Ordering::Relaxed) >= 1);
        wait_gone(member).await;
        assert!(!lease_path(directory.path()).exists());
    }

    #[tokio::test]
    async fn startup_retries_only_unavailable_arguments_with_a_deadline() {
        let pid = 123;
        let token = "fixture-token";
        let bare = format!("Sat Sep 26 12:00:00 2026 {pid} (sh)\n");
        let full =
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh -c script workflow-worker-{token}\n");
        let mut attempts = 0;
        let identity = startup_identity(
            || {
                attempts += 1;
                std::future::ready(classify_identity(
                    pid,
                    token,
                    if attempts < 3 {
                        bare.as_bytes()
                    } else {
                        full.as_bytes()
                    },
                ))
            },
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            classify_identity(pid, token, full.as_bytes()).unwrap(),
            ProcessIdentity::Verified(identity),
        );
        // A readable but different command or a different group is not the
        // macOS metadata race, even if an expected value might appear later.
        for output in [
            format!("Sat Sep 26 12:00:00 2026 {pid} /bin/sh unrelated\n"),
            "Sat Sep 26 12:00:00 2026 456 (sh)\n".into(),
        ] {
            let mut attempts = 0;
            let result = startup_identity(
                || {
                    attempts += 1;
                    std::future::ready(classify_identity(pid, token, output.as_bytes()))
                },
                Duration::from_millis(100),
            )
            .await;
            assert!(
                result
                    .unwrap_err()
                    .message
                    .contains("ownership is uncertain")
            );
            assert_eq!(attempts, 1);
        }
        let result = startup_identity(
            || std::future::ready(classify_identity(pid, token, bare.as_bytes())),
            Duration::from_millis(20),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .message
                .contains("unavailable at startup")
        );
    }
}
