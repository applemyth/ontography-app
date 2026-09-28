//! The supervisor for programs without a terminal: command tasks, and headless
//! agents that read input as their owner writes it. The owner reads the
//! program's output from pipes while it runs.

use super::{
    failure,
    lease::{
        ProcessIdentity, ProcessLease, inspect_process_identity, lease_path, startup_identity,
    },
};
use crate::{AppError, Result, persistence::write_json};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use std::{
    fs::OpenOptions,
    os::unix::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::unix::pipe, process::Command};

// The supervisor does not launch user code until its durable lease is saved.
// Its stdin is a lifetime pipe: parent death closes it and terminates the entire
// group. A nonce-bound result file preserves the command's exit code while the
// supervisor kills itself and any remaining descendants, even on normal exit.
const SUPERVISOR: &str = r#"
owner=$$
token=$1
input=$2
result=$3
shift 3
cleanup() {
    trap '' HUP INT TERM
    kill -TERM -"$owner" 2>/dev/null
    sleep 0.05
    kill -KILL -"$owner" 2>/dev/null
    exit 125
}
trap cleanup HUP INT TERM
IFS= read -r permit || cleanup
[ "$permit" = "$token" ] || cleanup
exec 3<&0
( IFS= read -r ignored <&3; kill -TERM -"$owner" 2>/dev/null ) &
"$@" <"$input" 3<&- &
task=$!
wait "$task"
code=$?
printf '%s %s\n' "$token" "$code" >"$result"
cleanup
"#;

/// What the supervised program reads on stdin.
#[derive(Clone, Copy, Debug)]
pub enum Stdin<'a> {
    /// These bytes, then end of input.
    Bytes(&'a [u8]),
    /// What the owner writes through [`SupervisedProcess::stdin`] while the
    /// program runs. Dropping that writer ends the input, provided the program
    /// has opened its end by then: a FIFO that nothing holds open discards
    /// unread bytes, and the program would block opening it. Input that is
    /// complete up front belongs in `Bytes`. The owner's end also counts as a
    /// reader, so writes never fail when the program exits; they wait once the
    /// pipe is full. Watch the process rather than write errors.
    Stream,
}

struct ProcessGroup(Option<Pid>);
impl ProcessGroup {
    fn disarm(&mut self) {
        self.0 = None;
    }

    fn terminate(&mut self) {
        self.terminate_with(|pid| {
            let _ = killpg(pid, Signal::SIGKILL);
        });
    }

    fn terminate_with(&mut self, signal: impl FnOnce(Pid)) {
        if let Some(pid) = self.0.take() {
            signal(pid);
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

struct ProcessFiles {
    directory: PathBuf,
    token: String,
    leased: bool,
}

impl ProcessFiles {
    fn path(&self, suffix: &str) -> PathBuf {
        self.directory.join(format!("{}.{}", self.token, suffix))
    }
}

impl Drop for ProcessFiles {
    fn drop(&mut self) {
        for suffix in ["input", "status"] {
            let _ = std::fs::remove_file(self.path(suffix));
        }
        if self.leased
            && crate::persistence::read_json::<ProcessLease>(&lease_path(&self.directory))
                .is_ok_and(|lease| lease.token == self.token)
        {
            let _ = std::fs::remove_file(lease_path(&self.directory));
        }
    }
}

/// A supervised program. Its owner takes the pipes it needs, permits the
/// program to start, and reads its output as it arrives. Dropping this kills
/// whatever still runs in the group, as does losing the owner process, which
/// closes the lifetime pipe.
pub struct SupervisedProcess {
    /// The writer for [`Stdin::Stream`] input.
    pub stdin: Option<pipe::Sender>,
    pub stdout: Option<tokio::process::ChildStdout>,
    pub stderr: Option<tokio::process::ChildStderr>,
    child: Option<tokio::process::Child>,
    group: ProcessGroup,
    lifetime: Option<tokio::process::ChildStdin>,
    token: String,
    files: Option<ProcessFiles>,
}

impl SupervisedProcess {
    fn child(&mut self) -> &mut tokio::process::Child {
        self.child.as_mut().expect("owned supervisor")
    }

    /// Lets the supervisor start the program. Permit only once: the supervisor
    /// treats anything more on its lifetime pipe like the pipe closing.
    pub async fn permit(&mut self) -> Result<()> {
        let permit = format!("{}\n", self.token);
        self.lifetime
            .as_mut()
            .expect("owned lifetime pipe")
            .write_all(permit.as_bytes())
            .await
            .map_err(failure)
    }

    /// Waits for the supervisor to exit, and reaps it.
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let result = self.child().wait().await;
        if result.is_ok() {
            // Reaping releases the PID. This must precede any other await or
            // output error, because that number can now belong to a new group.
            self.group.disarm();
        }
        result
    }

    /// Kills the whole group, unless the supervisor has already exited.
    pub fn terminate(&mut self) {
        if matches!(self.child().try_wait(), Ok(Some(_))) {
            self.group.disarm();
        } else {
            // The unreaped child still reserves its PID while we signal.
            self.group.terminate();
        }
    }

    /// Stops and reaps the supervisor if it still runs, removes the temporary
    /// files and lease, and returns the program's exit code. Fails if the
    /// program did not run to completion.
    pub async fn finish(mut self) -> Result<u8> {
        self.terminate();
        self.wait().await.map_err(failure)?;
        let files = self.files.take().expect("owned process files");
        // Read the exit record before dropping this process's temporary files.
        let record = tokio::fs::read_to_string(files.path("status")).await;
        drop(files);
        let record =
            record.map_err(|_| failure("Worker supervisor exited without a completed command"))?;
        record
            .strip_prefix(&format!("{} ", self.token))
            .and_then(|value| value.trim().parse::<u8>().ok())
            .ok_or_else(|| failure("Invalid worker completion record"))
    }
}

impl Drop for SupervisedProcess {
    fn drop(&mut self) {
        if self.child.is_none() {
            return;
        }
        self.terminate();
        self.lifetime.take();
        let mut child = self.child.take().expect("owned supervisor");
        let files = self.files.take();
        // A cancelled startup still owns an unreaped child. Keep its files
        // until reaping finishes, so a late supervisor write cannot recreate
        // the status file after cleanup.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
                drop(files);
            });
        }
    }
}

/// Starts `argv` in `cwd` under a new supervisor, leased in `directory`, which
/// also holds its temporary files. The program waits for its permit.
pub async fn spawn_supervised(
    argv: &[String],
    cwd: &Path,
    directory: &Path,
    stdin: Stdin<'_>,
) -> Result<SupervisedProcess> {
    spawn_supervised_using(
        argv,
        cwd,
        directory,
        stdin,
        |pid, token| async move { inspect_process_identity(pid, &token).await },
        Duration::from_secs(1),
    )
    .await
}

async fn spawn_supervised_using<F, Fut>(
    argv: &[String],
    cwd: &Path,
    directory: &Path,
    stdin: Stdin<'_>,
    mut inspect: F,
    timeout: Duration,
) -> Result<SupervisedProcess>
where
    F: FnMut(i32, String) -> Fut,
    Fut: std::future::Future<Output = Result<ProcessIdentity>>,
{
    if argv.is_empty() {
        return Err(failure("Worker argv is empty"));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let files = ProcessFiles {
        directory: directory.to_owned(),
        token: token.clone(),
        leased: false,
    };
    // Keep creation synchronous with guard ownership: cancelling tokio's
    // blocking file write could otherwise recreate a file after guard cleanup.
    let writer = match stdin {
        Stdin::Bytes(bytes) => {
            std::fs::write(files.path("input"), bytes).map_err(failure)?;
            None
        }
        Stdin::Stream => Some(open_stream(&files.path("input"))?),
    };
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(SUPERVISOR)
        .arg(format!("workflow-worker-{token}"))
        .arg(&token)
        .arg(files.path("input"))
        .arg(files.path("status"))
        .args(argv)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command.spawn().map_err(failure)?;
    let pid = child
        .id()
        .ok_or_else(|| failure("Worker supervisor has no identity"))? as i32;
    let lifetime = child.stdin.take().expect("piped supervisor lifetime");
    let mut process = SupervisedProcess {
        stdin: writer,
        stdout: child.stdout.take(),
        stderr: child.stderr.take(),
        child: Some(child),
        group: ProcessGroup(Some(Pid::from_raw(pid))),
        lifetime: Some(lifetime),
        token: token.clone(),
        files: Some(files),
    };
    let leased = async {
        let identity = startup_identity(|| inspect(pid, token.clone()), timeout).await?;
        write_json(
            &lease_path(directory),
            &ProcessLease {
                pid,
                token,
                identity,
            },
        )
        .map_err(failure)?;
        process.files.as_mut().expect("owned process files").leased = true;
        Ok::<_, AppError>(())
    }
    .await;
    if let Err(error) = leased {
        process.terminate();
        let _ = process.wait().await;
        process.files.take();
        return Err(error);
    }
    Ok(process)
}

/// Creates the input FIFO and opens the owner's end of it.
fn open_stream(path: &Path) -> Result<pipe::Sender> {
    nix::unistd::mkfifo(
        path,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .map_err(failure)?;
    // O_RDWR opens without waiting for the program; the program's own
    // read-only descriptor sees EOF when this last writer closes. The child
    // cannot inherit the writer (Rust opens it CLOEXEC).
    let fifo = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(nix::libc::O_NONBLOCK)
        .open(path)
        .map_err(failure)?;
    pipe::Sender::from_file(fifo).map_err(failure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::recover_process;
    use nix::{errno::Errno, sys::signal::kill};
    use tokio::io::{AsyncBufReadExt, BufReader};

    #[tokio::test]
    async fn rejected_timed_out_and_cancelled_startups_reap_and_remove_temporary_files() {
        for timeout in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut pid = 0;
            let result = spawn_supervised_using(
                &["/bin/sh".into(), "-c".into(), "touch ran".into()],
                directory.path(),
                directory.path(),
                Stdin::Bytes(b"input"),
                |observed, _| {
                    pid = observed;
                    std::future::ready(if timeout {
                        Ok(ProcessIdentity::ArgumentsUnavailable)
                    } else {
                        Err(failure("ownership mismatch"))
                    })
                },
                Duration::from_millis(20),
            )
            .await;
            assert!(result.is_err());
            assert_eq!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH));
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_owned();
        let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut sender = Some(pid_sender);
            spawn_supervised_using(
                &["/bin/sh".into(), "-c".into(), "touch ran".into()],
                &path,
                &path,
                Stdin::Bytes(b"input"),
                |pid, _| {
                    sender.take().unwrap().send(pid).unwrap();
                    std::future::pending::<Result<ProcessIdentity>>()
                },
                Duration::from_secs(30),
            )
            .await
        });
        let pid = pid_receiver.await.unwrap();
        task.abort();
        assert!(task.await.err().unwrap().is_cancelled());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH)
                    && std::fs::read_dir(directory.path()).unwrap().count() == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        // Even an OS spawn error occurs after the input file was created.
        assert!(
            spawn_supervised(
                &["/bin/true".into()],
                &directory.path().join("missing-working-directory"),
                directory.path(),
                Stdin::Bytes(b"input"),
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn output_error_after_reaping_cannot_signal_a_reused_group() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = spawn_supervised(
            &["/bin/true".into()],
            directory.path(),
            directory.path(),
            Stdin::Bytes(b""),
        )
        .await
        .unwrap();
        process.permit().await.unwrap();
        let (reaped, completion) = tokio::sync::oneshot::channel();
        let result = tokio::try_join!(
            async {
                process.wait().await.map_err(failure)?;
                reaped.send(()).unwrap();
                Ok(())
            },
            async {
                completion.await.unwrap();
                Err::<(), _>(failure("pipe failed after child exit"))
            },
        );
        assert!(result.unwrap_err().message.contains("pipe failed"));
        process
            .group
            .terminate_with(|_| panic!("A reaped child's group ID must never be signalled"));
        assert!(process.group.0.is_none());
    }

    #[tokio::test]
    async fn lifetime_pipe_terminates_orphans_before_and_after_start_permission() {
        for permitted in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut process = spawn_supervised(
                &[
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo started > started; sleep 30".into(),
                ],
                directory.path(),
                directory.path(),
                Stdin::Bytes(b""),
            )
            .await
            .unwrap();
            if permitted {
                process.permit().await.unwrap();
                tokio::time::timeout(Duration::from_secs(3), async {
                    while !directory.path().join("started").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
            }
            // Model abrupt parent loss: no Rust Drop signal is available;
            // the OS closes only the parent's lifetime-pipe write end.
            let mut child = process.child.take().unwrap();
            let lifetime = process.lifetime.take().unwrap();
            process.group.disarm();
            std::mem::forget(process.files.take());
            drop(lifetime);
            tokio::time::timeout(Duration::from_secs(3), child.wait())
                .await
                .unwrap()
                .unwrap();
            recover_process(directory.path()).await.unwrap();
            assert_eq!(directory.path().join("started").exists(), permitted);
            assert!(!lease_path(directory.path()).exists());
        }
    }

    #[tokio::test]
    async fn orphan_recovery_verifies_identity_before_signalling() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = spawn_supervised(
            &["/bin/sleep".into(), "30".into()],
            directory.path(),
            directory.path(),
            Stdin::Bytes(b""),
        )
        .await
        .unwrap();
        process.permit().await.unwrap();
        let mut lease: ProcessLease =
            crate::persistence::read_json(&lease_path(directory.path())).unwrap();
        let original = lease.identity.clone();
        lease.identity = "different process".into();
        write_json(&lease_path(directory.path()), &lease).unwrap();
        assert!(
            recover_process(directory.path())
                .await
                .unwrap_err()
                .message
                .contains("identity changed")
        );
        assert!(
            process.child().try_wait().unwrap().is_none(),
            "mismatch must not signal the process"
        );
        lease.identity = original;
        write_json(&lease_path(directory.path()), &lease).unwrap();
        let (recovered, exited) = tokio::join!(recover_process(directory.path()), process.wait());
        recovered.unwrap();
        exited.unwrap();
        assert!(!lease_path(directory.path()).exists());
    }

    #[tokio::test]
    async fn streamed_input_is_read_as_written_and_ends_when_the_writer_closes() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = spawn_supervised(
            &["/bin/cat".into()],
            directory.path(),
            directory.path(),
            Stdin::Stream,
        )
        .await
        .unwrap();
        let mut input = process.stdin.take().unwrap();
        let mut output = BufReader::new(process.stdout.take().unwrap()).lines();
        process.permit().await.unwrap();
        for line in ["first", "second"] {
            input
                .write_all(format!("{line}\n").as_bytes())
                .await
                .unwrap();
            let echoed = tokio::time::timeout(Duration::from_secs(3), output.next_line())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(echoed.as_deref(), Some(line));
        }
        // The program is waiting for more: only closing the writer ends input.
        assert!(process.child().try_wait().unwrap().is_none());
        drop(input);
        tokio::time::timeout(Duration::from_secs(3), process.wait())
            .await
            .unwrap()
            .unwrap();
        let rest = tokio::time::timeout(Duration::from_secs(3), output.next_line())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rest, None);
        assert_eq!(process.finish().await.unwrap(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn a_streaming_group_stops_with_its_owner_while_the_writer_stays_open() {
        for abrupt in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut process = spawn_supervised(
                &[
                    "/bin/sh".into(),
                    "-c".into(),
                    "printf %s \"$$\" > program.pid; exec /bin/cat".into(),
                ],
                directory.path(),
                directory.path(),
                Stdin::Stream,
            )
            .await
            .unwrap();
            let mut input = process.stdin.take().unwrap();
            let mut output = BufReader::new(process.stdout.take().unwrap()).lines();
            process.permit().await.unwrap();
            input.write_all(b"running\n").await.unwrap();
            let echoed = tokio::time::timeout(Duration::from_secs(3), output.next_line())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(echoed.as_deref(), Some("running"));
            let program = Pid::from_raw(
                std::fs::read_to_string(directory.path().join("program.pid"))
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
            if abrupt {
                // As above: the OS closes only the lifetime pipe.
                let mut child = process.child.take().unwrap();
                let lifetime = process.lifetime.take().unwrap();
                process.group.disarm();
                std::mem::forget(process.files.take());
                drop(lifetime);
                tokio::time::timeout(Duration::from_secs(3), child.wait())
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                drop(process);
            }
            // A dropped handle removes its own files once its supervisor is
            // reaped; a lost owner leaves them to recovery.
            tokio::time::timeout(Duration::from_secs(3), async {
                while kill(program, None) != Err(Errno::ESRCH)
                    || (!abrupt && lease_path(directory.path()).exists())
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            recover_process(directory.path()).await.unwrap();
            let remaining: Vec<_> = std::fs::read_dir(directory.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(remaining, ["program.pid"]);
            drop(input);
        }
    }

    #[tokio::test]
    async fn finish_returns_the_recorded_exit_code_or_reports_an_unfinished_program() {
        for (script, recorded) in [("exit 7", Some(7)), ("sleep 30", None)] {
            let directory = tempfile::tempdir().unwrap();
            let mut process = spawn_supervised(
                &["/bin/sh".into(), "-c".into(), script.into()],
                directory.path(),
                directory.path(),
                Stdin::Bytes(b""),
            )
            .await
            .unwrap();
            process.permit().await.unwrap();
            if recorded.is_some() {
                tokio::time::timeout(Duration::from_secs(3), process.wait())
                    .await
                    .unwrap()
                    .unwrap();
            }
            let finished = process.finish().await;
            match recorded {
                Some(code) => assert_eq!(finished.unwrap(), code),
                None => assert!(
                    finished
                        .unwrap_err()
                        .message
                        .contains("without a completed command")
                ),
            }
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
    }
}
