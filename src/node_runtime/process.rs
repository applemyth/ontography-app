//! PTY supervisor with a separate lifetime pipe. Closing a UI never closes this
//! pipe; losing the owning server does, even when no Rust destructors run.

use crate::{
    AppError, Result,
    process::lease_process,
    terminal::{LaunchSpec, Terminal},
};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

const SUPERVISOR: &str = r#"
owner=$$
token=$1
pipe=$2
result=$3
cleanup_program=$4
shift 4
cleanup() {
    trap '' HUP INT TERM
    # Native getsid checks also find jobs that created their own process group.
    # The helper is in this session and deliberately excludes our owner group.
    "$cleanup_program" internal-node-cleanup --owner "$owner" </dev/null 3<&- 4<&- || :
    kill -TERM -"$owner" 2>/dev/null
    sleep 0.05
    kill -KILL -"$owner" 2>/dev/null
    exit 125
}
trap cleanup HUP INT TERM
# Preserve the PTY slave: macOS kqueue rejects a reopened /dev/tty descriptor.
exec 4<&0
exec 3<"$pipe"
IFS= read -r permit <&3 || cleanup
[ "$permit" = "$token" ] || cleanup
( IFS= read -r ignored <&3; kill -TERM -"$owner" 2>/dev/null ) &
"$@" <&4 3<&- 4<&- &
task=$!
wait "$task"
code=$?
printf '%s %s\n' "$token" "$code" >"$result"
cleanup
"#;

pub(super) struct Lifetime {
    directory: PathBuf,
    token: String,
    pipe: Option<File>,
    leased: bool,
    cleanup_program: PathBuf,
}

impl Lifetime {
    pub fn new(directory: &Path) -> Result<Self> {
        let mut lifetime = Self {
            directory: directory.into(),
            token: uuid::Uuid::new_v4().to_string(),
            pipe: None,
            leased: false,
            cleanup_program: crate::launcher::application_executable()?,
        };
        nix::unistd::mkfifo(
            &lifetime.path("input"),
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .map_err(|error| AppError::new("node_lifetime", error.to_string()))?;
        // O_RDWR opens without waiting for the supervisor; its own read-only
        // descriptor sees EOF when this last writer closes. The child cannot
        // inherit the writer (Rust opens it CLOEXEC).
        lifetime.pipe = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(nix::libc::O_NONBLOCK)
                .open(lifetime.path("input"))?,
        );
        Ok(lifetime)
    }

    fn path(&self, suffix: &str) -> PathBuf {
        self.directory.join(format!("{}.{}", self.token, suffix))
    }

    pub fn supervise(&self, mut spec: LaunchSpec) -> LaunchSpec {
        let mut args = vec![
            "-c".into(),
            SUPERVISOR.into(),
            format!("workflow-worker-{}", self.token),
            self.token.clone(),
            self.path("input").to_string_lossy().into_owned(),
            self.path("status").to_string_lossy().into_owned(),
            self.cleanup_program.to_string_lossy().into_owned(),
            spec.program.to_string_lossy().into_owned(),
        ];
        args.extend(spec.args);
        spec.program = "/bin/sh".into();
        spec.args = args;
        spec
    }

    pub async fn permit(&mut self, terminal: &Terminal) -> Result<()> {
        let pid = terminal
            .status()
            .pid
            .ok_or_else(|| AppError::new("node_lifetime", "Terminal supervisor has no PID"))?;
        lease_process(&self.directory, pid, &self.token).await?;
        self.leased = true;
        writeln!(
            self.pipe.as_mut().expect("open lifetime pipe"),
            "{}",
            self.token
        )?;
        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.pipe.take();
    }

    pub fn exit_code(&self) -> Result<u32> {
        let record = std::fs::read_to_string(self.path("status")).map_err(|_| {
            AppError::new(
                "node_process_exit",
                "Agent supervisor stopped without a completed exit record",
            )
        })?;
        record
            .strip_prefix(&format!("{} ", self.token))
            .and_then(|value| value.trim().parse().ok())
            .ok_or_else(|| AppError::new("node_process_exit", "Invalid agent exit record"))
    }
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.disconnect();
        // A saved lease stays until recovery verifies that the whole previous
        // group is gone. In particular, abort must not erase recovery evidence.
        if !self.leased {
            let _ = std::fs::remove_file(self.path("input"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::recover_process;
    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
    use std::{os::unix::fs::PermissionsExt, time::Duration};

    fn spec(directory: &Path, script: &str) -> LaunchSpec {
        LaunchSpec {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            env: crate::environment::Environment::current().vars().clone(),
            cwd: directory.into(),
            rows: 24,
            cols: 80,
            server_id: "server".into(),
            session_id: "node".into(),
        }
    }

    fn directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }

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
        .expect("supervised process did not become ready")
    }

    async fn wait_gone(pid: Pid) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while kill(pid, None) != Err(Errno::ESRCH) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervised process was left running");
    }

    #[tokio::test]
    async fn lifetime_eof_stops_permitted_process_with_terminal_handles_retained() {
        let directory = directory();
        let mut lifetime = Lifetime::new(directory.path()).unwrap();
        let terminal = Terminal::launch(
            lifetime.supervise(spec(
                directory.path(),
                "trap '' HUP TERM; set -m; sleep 60 & printf '%s' \"$!\" > job.pid; printf '%s' \"$$\" > agent.pid; wait",
            )),
            directory.path().join("terminal.sock"),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!directory.path().join("agent.pid").exists());
        lifetime.permit(&terminal).await.unwrap();
        assert!(directory.path().join("worker-process.json").exists());
        let pid = Pid::from_raw(
            wait_file(&directory.path().join("agent.pid"))
                .await
                .parse()
                .unwrap(),
        );
        let job = Pid::from_raw(
            wait_file(&directory.path().join("job.pid"))
                .await
                .parse()
                .unwrap(),
        );
        let owner = Pid::from_raw(terminal.status().pid.unwrap() as i32);
        assert_eq!(nix::unistd::getsid(Some(job)).unwrap(), owner);
        assert_ne!(nix::unistd::getpgid(Some(job)).unwrap(), owner);
        lifetime.disconnect();
        wait_gone(pid).await;
        wait_gone(job).await;
        wait_gone(Pid::from_raw(terminal.status().pid.unwrap() as i32)).await;
        assert!(!terminal.status().running);
        recover_process(directory.path()).await.unwrap();
        assert!(!directory.path().join("worker-process.json").exists());
        assert!(!lifetime.path("input").exists());
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn natural_exit_records_agent_code_before_supervisor_cleanup() {
        let directory = directory();
        let mut lifetime = Lifetime::new(directory.path()).unwrap();
        let terminal = Terminal::launch(
            lifetime.supervise(spec(directory.path(), "exit 7")),
            directory.path().join("terminal.sock"),
        )
        .await
        .unwrap();
        lifetime.permit(&terminal).await.unwrap();
        wait_gone(Pid::from_raw(terminal.status().pid.unwrap() as i32)).await;
        assert_eq!(lifetime.exit_code().unwrap(), 7);
        lifetime.disconnect();
        recover_process(directory.path()).await.unwrap();
        assert!(!lifetime.path("status").exists());
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_of_a_live_supervisor_waits_for_other_session_groups_to_stop() {
        let directory = directory();
        let mut lifetime = Lifetime::new(directory.path()).unwrap();
        let terminal = Terminal::launch(
            lifetime.supervise(spec(
                directory.path(),
                "trap '' HUP TERM; set -m; sleep 60 & printf '%s' \"$!\" > job.pid; wait",
            )),
            directory.path().join("terminal.sock"),
        )
        .await
        .unwrap();
        lifetime.permit(&terminal).await.unwrap();
        let job = Pid::from_raw(
            wait_file(&directory.path().join("job.pid"))
                .await
                .parse()
                .unwrap(),
        );
        let owner = Pid::from_raw(terminal.status().pid.unwrap() as i32);
        assert_eq!(nix::unistd::getsid(Some(job)).unwrap(), owner);
        assert_ne!(nix::unistd::getpgid(Some(job)).unwrap(), owner);
        // Recovery must let the still-live guardian clean other process groups;
        // killing only its owner group would leave this job behind.
        recover_process(directory.path()).await.unwrap();
        wait_gone(job).await;
        wait_gone(owner).await;
        assert!(!directory.path().join("worker-process.json").exists());
        lifetime.disconnect();
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn event_reader_fixture() {
        let Some(path) = std::env::var_os("ONTOGRAPHY_TEST_EVENT_READER") else {
            return;
        };
        // On macOS kqueue cannot register a reopened /dev/tty descriptor even
        // though isatty succeeds. Agent stdin must retain the PTY slave itself.
        let _reader = crossterm::event::EventStream::new();
        std::fs::write(path, "ready").unwrap();
    }

    #[tokio::test]
    async fn supervised_agent_keeps_stdin_compatible_with_terminal_event_readers() {
        let directory = directory();
        let mut lifetime = Lifetime::new(directory.path()).unwrap();
        let mut launch = spec(directory.path(), "");
        launch.program = std::env::current_exe().unwrap();
        launch.args = vec![
            "--exact".into(),
            "node_runtime::process::tests::event_reader_fixture".into(),
            "--nocapture".into(),
        ];
        launch.env.insert(
            "ONTOGRAPHY_TEST_EVENT_READER".into(),
            directory
                .path()
                .join("reader-ready")
                .to_string_lossy()
                .into(),
        );
        let terminal = Terminal::launch(
            lifetime.supervise(launch),
            directory.path().join("terminal.sock"),
        )
        .await
        .unwrap();
        lifetime.permit(&terminal).await.unwrap();
        wait_gone(Pid::from_raw(terminal.status().pid.unwrap() as i32)).await;
        assert_eq!(
            lifetime.exit_code().unwrap(),
            0,
            "{}",
            terminal.snapshot().screen
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("reader-ready")).unwrap(),
            "ready"
        );
        lifetime.disconnect();
        recover_process(directory.path()).await.unwrap();
        terminal.shutdown().await.unwrap();
    }

    /// Runs only as the child of the abrupt-owner-loss test below. Its owner is
    /// killed without running Rust drops, leaving the FIFO/PTY to stop the group.
    #[tokio::test]
    async fn crash_owner_fixture() {
        let Some(directory) = std::env::var_os("ONTOGRAPHY_TEST_LIFETIME_DIRECTORY") else {
            return;
        };
        let directory = PathBuf::from(directory);
        let mut lifetime = Lifetime::new(&directory).unwrap();
        let terminal = Terminal::launch(
            lifetime.supervise(spec(
                &directory,
                "trap '' HUP TERM; set -m; sleep 60 & printf '%s' \"$!\" > job.pid; printf '%s' \"$$\" > agent.pid; wait",
            )),
            directory.join("terminal.sock"),
        )
        .await
        .unwrap();
        if std::env::var_os("ONTOGRAPHY_TEST_LIFETIME_PERMIT").is_some() {
            lifetime.permit(&terminal).await.unwrap();
            wait_file(&directory.join("agent.pid")).await;
        }
        std::fs::write(
            directory.join("owner-ready"),
            terminal.status().pid.unwrap().to_string(),
        )
        .unwrap();
        std::future::pending::<()>().await;
    }

    #[tokio::test]
    async fn abrupt_owner_loss_stops_supervisor_before_and_after_start_permission() {
        for permitted in [false, true] {
            let directory = directory();
            let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "node_runtime::process::tests::crash_owner_fixture",
                    "--nocapture",
                ])
                .env("ONTOGRAPHY_TEST_LIFETIME_DIRECTORY", directory.path())
                .env_remove("ONTOGRAPHY_TEST_LIFETIME_PERMIT")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .kill_on_drop(true);
            if permitted {
                command.env("ONTOGRAPHY_TEST_LIFETIME_PERMIT", "1");
            }
            let mut owner = command.spawn().unwrap();
            let supervisor = Pid::from_raw(
                wait_file(&directory.path().join("owner-ready"))
                    .await
                    .parse()
                    .unwrap(),
            );
            let job = if permitted {
                let job = Pid::from_raw(
                    wait_file(&directory.path().join("job.pid"))
                        .await
                        .parse()
                        .unwrap(),
                );
                assert_eq!(nix::unistd::getsid(Some(job)).unwrap(), supervisor);
                assert_ne!(nix::unistd::getpgid(Some(job)).unwrap(), supervisor);
                Some(job)
            } else {
                None
            };
            owner.kill().await.unwrap();
            wait_gone(supervisor).await;
            if permitted {
                let agent = Pid::from_raw(
                    wait_file(&directory.path().join("agent.pid"))
                        .await
                        .parse()
                        .unwrap(),
                );
                wait_gone(agent).await;
                wait_gone(job.unwrap()).await;
                recover_process(directory.path()).await.unwrap();
            } else {
                assert!(!directory.path().join("agent.pid").exists());
            }
        }
    }
}
