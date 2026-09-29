//! One persistent interactive shell, with native Pi as an ordinary foreground job.
//!
//! A private generation-bound socket lends the shell's Pi launcher a live lease.
//! Mode and process ownership follow that lease, never terminal output. Every
//! invocation reads the current conversation from durable session state.

use crate::{
    AppError, Result,
    launcher::{self, PiSessionLaunch},
    persistence::Paths,
    protocol,
    sessions::SessionStatus,
    state::Service,
    terminal::{LaunchSpec, Terminal},
};
use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::{Pid, getpgid, getsid},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    signal::unix::{SignalKind, signal},
    sync::watch,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PiCommand {
    program: PathBuf,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum LeaseRequest {
    Open {
        session_id: String,
        generation: String,
        launcher_pid: u32,
    },
    Started {
        pid: u32,
    },
    Exited {
        code: Option<i32>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LeaseResponse {
    Launch { command: PiCommand },
    Accepted,
    Finished,
    Error { error: AppError },
}

struct Lease {
    id: String,
    group: Pid,
    manager_pid: Option<u32>,
    termios: Option<String>,
}

struct Mode {
    name: &'static str,
    lease: Option<Lease>,
    error: Option<String>,
}

pub struct ManagedShell {
    pub terminal: Arc<Terminal>,
    session_id: String,
    generation: String,
    pi: PathBuf,
    mode: Mutex<Mode>,
    stop: watch::Sender<bool>,
    socket: PathBuf,
    rc: PathBuf,
}

fn lease_socket(paths: &Paths, generation: &str) -> Result<PathBuf> {
    uuid::Uuid::parse_str(generation).map_err(|_| AppError::invalid("invalid shell generation"))?;
    Ok(paths
        .socket
        .parent()
        .expect("server socket parent")
        .join(format!("pi-{generation}.sock")))
}

fn quote(value: impl AsRef<std::ffi::OsStr>) -> String {
    format!(
        "'{}'",
        value.as_ref().to_string_lossy().replace('\'', "'\\''")
    )
}

impl ManagedShell {
    pub async fn launch(
        service: Arc<Service>,
        session_id: &str,
        pi: PathBuf,
        rows: u16,
        cols: u16,
    ) -> Result<Arc<Self>> {
        let record = service.sessions.get(session_id).await?.lock().await.clone();
        if record.status != SessionStatus::Active {
            return Err(AppError::new(
                "session_inactive",
                "resume the Ontography session before starting its shell",
            ));
        }
        // The shell, and Pi within it, start with the session's environment.
        let environment = service.session_environment(session_id).await;
        // Check the configured executable before leaving a durable shell running.
        launcher::pi_command(&service.paths, &record.project, &pi, &environment, false).await?;
        let generation = uuid::Uuid::new_v4().to_string();
        let socket = lease_socket(&service.paths, &generation)?;
        let listener = UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        let shell_dir = service
            .sessions
            .conversations_dir(session_id)?
            .parent()
            .expect("session directory")
            .join("shell");
        std::fs::create_dir_all(&shell_dir)?;
        std::fs::set_permissions(&shell_dir, std::fs::Permissions::from_mode(0o700))?;
        let rc = shell_dir.join(format!("{generation}.bashrc"));
        let executable = launcher::application_executable()?;
        let source = format!(
            "set -m\nHISTFILE={}\nHISTCONTROL=ignoredups\nshopt -s histappend\nPROMPT_COMMAND='history -a'\nPS1='ontography:{} \\w $ '\n_ontography_exit() {{\n  trap - EXIT HUP TERM\n  local p\n  for p in $(jobs -p); do kill -TERM -- -\"$p\" 2>/dev/null; kill -KILL -- -\"$p\" 2>/dev/null; done\n}}\ntrap _ontography_exit EXIT\ntrap 'exit 0' HUP TERM\npi() {{\n  if [ \"$#\" -ne 0 ]; then printf '%s\\n' 'Use pi without arguments to resume this session; change model/settings inside Pi.' >&2; return 2; fi\n  {} --data-dir {} internal-pi --session-id {} --generation {}\n}}\npi\n",
            quote(shell_dir.join("history")),
            &session_id[..8],
            quote(executable),
            quote(&service.paths.root),
            quote(session_id),
            quote(&generation)
        );
        std::fs::write(&rc, source)?;
        std::fs::set_permissions(&rc, std::fs::Permissions::from_mode(0o600))?;
        let spec = LaunchSpec {
            program: "/bin/bash".into(),
            args: vec![
                "--noprofile".into(),
                "--rcfile".into(),
                rc.to_string_lossy().into_owned(),
                "-i".into(),
            ],
            env: environment
                .vars()
                .clone()
                .into_iter()
                .chain([
                    ("BASH_SILENCE_DEPRECATION_WARNING".into(), "1".into()),
                    // Commands typed in the shell reach this server's store.
                    (
                        "ONTOGRAPHY_DATA_DIR".into(),
                        service.paths.root.to_string_lossy().into_owned(),
                    ),
                    ("ONTOGRAPHY_SESSION_ID".into(), session_id.into()),
                    (
                        "ONTOGRAPHY_SOCKET".into(),
                        service.paths.socket.to_string_lossy().into_owned(),
                    ),
                ])
                .collect(),
            cwd: record.project,
            rows,
            cols,
            server_id: service.server_id.clone(),
            session_id: session_id.into(),
        };
        let terminal_socket = service
            .paths
            .socket
            .parent()
            .expect("server socket parent")
            .join(format!("pty-{session_id}.sock"));
        let terminal = match Terminal::launch(spec, terminal_socket).await {
            Ok(terminal) => terminal,
            Err(error) => {
                let _ = std::fs::remove_file(&socket);
                let _ = std::fs::remove_file(&rc);
                return Err(error);
            }
        };
        let (stop, _) = watch::channel(false);
        let shell = Arc::new(Self {
            terminal,
            session_id: session_id.into(),
            generation,
            pi,
            mode: Mutex::new(Mode {
                name: "starting",
                lease: None,
                error: None,
            }),
            stop,
            socket,
            rc,
        });
        let weak = Arc::downgrade(&shell);
        let mut stopping = shell.stop.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=stopping.changed()=>break,
                    accepted=listener.accept()=>match accepted {Ok((stream,_))=>{
                        let weak=weak.clone();let service=service.clone();
                        tokio::spawn(async move {if let Some(shell)=weak.upgrade(){shell.serve(service,stream).await;}});
                    },Err(_)=>break}
                }
            }
        });
        Ok(shell)
    }

    pub fn status(&self) -> Result<Value> {
        let mut result = serde_json::to_value(self.terminal.status())?;
        let mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
        result["manager_mode"] = json!(mode.name);
        result["manager_pid"] = json!(mode.lease.as_ref().and_then(|l| l.manager_pid));
        result["manager_error"] = json!(mode.error);
        Ok(result)
    }

    async fn pi_command(&self, service: &Service) -> Result<PiCommand> {
        let record = service
            .sessions
            .get(&self.session_id)
            .await?
            .lock()
            .await
            .clone();
        if record.status != SessionStatus::Active {
            return Err(AppError::new(
                "session_inactive",
                "this session is no longer active",
            ));
        }
        let active = record
            .pi
            .conversations
            .get(&record.pi.active_conversation_id)
            .ok_or_else(|| AppError::new("invalid_session", "active Pi conversation is missing"))?;
        let path = match &active.path {
            Some(path) if path.is_file() => Some(path.clone()),
            _ if active.materialized => {
                return Err(AppError::new(
                    "conversation_missing",
                    "saved Pi history is missing; restore it before resuming",
                ));
            }
            _ => None,
        };
        let command = launcher::pi_session_command(
            &service.paths,
            &record.project,
            &self.pi,
            &service.session_environment(&self.session_id).await,
            &PiSessionLaunch {
                session_id: self.session_id.clone(),
                conversations_dir: service.sessions.conversations_dir(&self.session_id)?,
                conversation_id: active.conversation_id.clone(),
                conversation_path: path,
            },
        )
        .await?;
        let command = command.as_std();
        Ok(PiCommand {
            program: command.get_program().into(),
            args: command
                .get_args()
                .map(|s| s.to_string_lossy().into_owned())
                .collect(),
            env: command
                .get_envs()
                .filter_map(|(k, v)| {
                    v.map(|v| {
                        (
                            k.to_string_lossy().into_owned(),
                            v.to_string_lossy().into_owned(),
                        )
                    })
                })
                .collect(),
            cwd: record.project,
        })
    }

    async fn serve(self: Arc<Self>, service: Arc<Service>, stream: UnixStream) {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut stop = self.stop.subscribe();
        let already_stopping = *stop.borrow();
        let mut guard = None;
        let mut owned_handshake = false;
        let outcome = async {
            let Some(LeaseRequest::Open {
                session_id,
                generation,
                launcher_pid,
            }) = receive(&mut read).await?
            else {
                return Err(AppError::invalid("expected launcher lease"));
            };
            if session_id != self.session_id
                || generation != self.generation
                || already_stopping
                || !self.terminal.status().running
            {
                return Err(AppError::new(
                    "stale_terminal",
                    "shell generation is no longer live",
                ));
            }
            owned_handshake = true;
            let launcher = Pid::from_raw(
                i32::try_from(launcher_pid)
                    .map_err(|_| AppError::invalid("invalid launcher PID"))?,
            );
            let shell = Pid::from_raw(
                self.terminal
                    .status()
                    .pid
                    .ok_or_else(|| AppError::new("terminal_error", "shell PID missing"))?
                    as i32,
            );
            if launcher_pid <= 1
                || self.terminal.foreground_process_group() != Some(launcher.as_raw())
                || getpgid(Some(launcher)).map_err(AppError::core)? != launcher
                || getsid(Some(launcher)).map_err(AppError::core)?
                    != getsid(Some(shell)).map_err(AppError::core)?
            {
                return Err(AppError::new(
                    "invalid_launcher",
                    "Pi must be launched as this shell's foreground job",
                ));
            }
            let lease_id = uuid::Uuid::new_v4().to_string();
            let termios = self.terminal_settings(None);
            {
                let mut mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
                if mode.lease.is_some() {
                    return Err(AppError::new(
                        "manager_busy",
                        "a Pi process already owns this session terminal",
                    ));
                }
                mode.name = "starting";
                mode.error = None;
                mode.lease = Some(Lease {
                    id: lease_id.clone(),
                    group: launcher,
                    manager_pid: None,
                    termios,
                });
            }
            guard = Some(LeaseGuard {
                shell: Arc::downgrade(&self),
                id: lease_id.clone(),
                completed: false,
            });
            let command = self.pi_command(&service).await?;
            send(&mut write, &LeaseResponse::Launch { command }).await?;
            let Some(LeaseRequest::Started { pid }) = receive(&mut read).await? else {
                return Err(AppError::new(
                    "launcher_disconnected",
                    "Pi launcher ended before reporting its process",
                ));
            };
            if getpgid(Some(Pid::from_raw(pid as i32))).map_err(AppError::core)? != launcher {
                return Err(AppError::new(
                    "invalid_launcher",
                    "Pi process left its owned job group",
                ));
            }
            {
                let mut mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(lease) = mode.lease.as_mut().filter(|l| l.id == lease_id) {
                    lease.manager_pid = Some(pid);
                    mode.name = "pi";
                }
            }
            send(&mut write, &LeaseResponse::Accepted).await?;
            match receive(&mut read).await? {
                Some(LeaseRequest::Exited { code }) => {
                    if code.is_some_and(|c| c != 0) {
                        self.mode.lock().unwrap_or_else(|p| p.into_inner()).error =
                            Some(format!("Pi exited with status {}", code.unwrap()));
                    }
                    if let Some(mut finished) = guard.take() {
                        finished.completed = true;
                        drop(finished);
                    }
                    send(&mut write, &LeaseResponse::Finished).await?;
                    Ok(())
                }
                _ => Err(AppError::new(
                    "launcher_disconnected",
                    "Pi launcher disconnected before its process completed",
                )),
            }
        };
        let result = tokio::select! {result=outcome=>result,_=stop.changed()=>Err(AppError::new("session_stopping","session terminal is stopping"))};
        if let Err(error) = result {
            // Failure before acquiring a lease must not disturb another launcher.
            {
                let mut mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
                if guard.is_some() || (owned_handshake && mode.lease.is_none()) {
                    mode.error = Some(error.message.clone());
                    if mode.lease.is_none() {
                        mode.name = "shell";
                    }
                }
            }
            let _ = send(&mut write, &LeaseResponse::Error { error }).await;
        }
        drop(guard);
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.stop.send_replace(true);
        {
            let mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(lease) = &mode.lease {
                let _ = killpg(lease.group, Signal::SIGTERM);
            }
        }
        if let Some(pid) = self.terminal.status().pid {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
        // Job-control groups share the PTY's OS session. Include shell jobs in
        // other groups, even when the shell died before running its exit trap.
        self.signal_owned_groups(Signal::SIGTERM).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.signal_owned_groups(Signal::SIGKILL).await?;
        self.terminal.shutdown().await?;
        {
            let mut mode = self.mode.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(lease) = mode.lease.take() {
                let _ = killpg(lease.group, Signal::SIGKILL);
            }
            mode.name = "shell";
        }
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_file(&self.rc);
        Ok(())
    }

    pub fn request_stop(&self) {
        self.stop.send_replace(true);
        if let Ok(mode) = self.mode.lock()
            && let Some(lease) = &mode.lease
        {
            let _ = killpg(lease.group, Signal::SIGKILL);
        }
        if let Some(pid) = self.terminal.status().pid {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGHUP);
        }
    }

    fn terminal_settings(&self, restore: Option<&str>) -> Option<String> {
        let tty = self.terminal.tty_name()?;
        let mut command = std::process::Command::new("/bin/stty");
        command
            .arg(if cfg!(target_os = "macos") {
                "-f"
            } else {
                "-F"
            })
            .arg(tty)
            .arg(restore.unwrap_or("-g"))
            .stdin(Stdio::null())
            .stderr(Stdio::null());
        let output = command.output().ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    async fn signal_owned_groups(&self, signal: Signal) -> Result<()> {
        let Some(owner) = self.terminal.status().pid else {
            return Ok(());
        };
        let output = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::process::Command::new("/bin/ps")
                .args(["-axo", "pid="])
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let output = output.map_err(|_| {
            AppError::new(
                "terminal_cleanup_timeout",
                "process enumeration timed out while cleaning session jobs",
            )
        })??;
        if !output.status.success() {
            return Err(AppError::new(
                "terminal_cleanup_failed",
                "could not enumerate the session's remaining process groups",
            ));
        }
        let owner = Pid::from_raw(owner as i32);
        let mut groups = std::collections::BTreeSet::new();
        for pid in String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .filter_map(|s| s.parse::<i32>().ok())
            .filter(|pid| *pid > 1)
            .map(Pid::from_raw)
        {
            if getsid(Some(pid)).ok() == Some(owner)
                && let Ok(group) = getpgid(Some(pid))
            {
                groups.insert(group);
            }
        }
        for group in groups {
            if group != nix::unistd::getpgrp() {
                match killpg(group, signal) {
                    Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                    Err(error) => {
                        return Err(AppError::new("terminal_cleanup_failed", error.to_string()));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for ManagedShell {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_file(&self.rc);
        if let Ok(mode) = self.mode.lock()
            && let Some(lease) = &mode.lease
        {
            let _ = killpg(lease.group, Signal::SIGKILL);
        }
    }
}

struct LeaseGuard {
    shell: Weak<ManagedShell>,
    id: String,
    completed: bool,
}
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(shell) = self.shell.upgrade() {
            let mut mode = shell.mode.lock().unwrap_or_else(|p| p.into_inner());
            if mode.lease.as_ref().is_some_and(|lease| lease.id == self.id) {
                if let Some(lease) = mode.lease.take() {
                    let _ = killpg(
                        lease.group,
                        if self.completed {
                            Signal::SIGTERM
                        } else {
                            Signal::SIGKILL
                        },
                    );
                    if !self.completed {
                        if let Some(settings) = &lease.termios {
                            shell.terminal_settings(Some(settings));
                        }
                        shell.terminal.reset_program_modes();
                    }
                }
                mode.name = "shell";
            }
        }
    }
}

async fn send(
    write: &mut (impl tokio::io::AsyncWrite + Unpin),
    message: &impl Serialize,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    write.write_all(&bytes).await?;
    Ok(())
}
async fn receive<T: serde::de::DeserializeOwned>(
    read: &mut (impl tokio::io::AsyncBufRead + Unpin),
) -> Result<Option<T>> {
    protocol::read_frame(read)
        .await?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(AppError::from))
        .transpose()
}

fn reset_terminal(saved: Option<String>) {
    if let Some(saved) = saved {
        let _ = std::process::Command::new("/bin/stty")
            .arg(saved.trim())
            .stdin(Stdio::inherit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    print!(
        "\x1b[?1049l\x1b[?25h\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l\x1b[?2004l\x1b[<99u\x1b[0m\x1b[2J\x1b[H"
    );
    let _ = std::io::stdout().flush();
}

/// Hidden child entry point. The interactive shell supplies job control; this
/// helper supervises Pi and keeps a live lease until its child has been reaped.
pub async fn run_pi(paths: &Paths, session_id: &str, generation: &str) -> Result<()> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    let mut quit = signal(SignalKind::quit())?;
    let stream = UnixStream::connect(lease_socket(paths, generation)?).await?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    send(
        &mut write,
        &LeaseRequest::Open {
            session_id: session_id.into(),
            generation: generation.into(),
            launcher_pid: std::process::id(),
        },
    )
    .await?;
    let command = match receive(&mut read).await? {
        Some(LeaseResponse::Launch { command }) => command,
        Some(LeaseResponse::Error { error }) => return Err(error),
        _ => {
            return Err(AppError::new(
                "launcher_protocol",
                "server did not supply a Pi launch",
            ));
        }
    };
    let saved = std::process::Command::new("/bin/stty")
        .arg("-g")
        .stdin(Stdio::inherit())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok());
    let mut child = tokio::process::Command::new(command.program)
        .args(command.args)
        .envs(command.env)
        .current_dir(command.cwd)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| AppError::new("launcher_error", "Pi process has no identity"))?;
    let result=async {
        send(&mut write,&LeaseRequest::Started{pid}).await?;
        match receive(&mut read).await? {Some(LeaseResponse::Accepted)=>{},Some(LeaseResponse::Error{error})=>return Err(error),_=>return Err(AppError::new("launcher_protocol","server did not accept the Pi process"))}
        let status=loop {tokio::select! {
            result=child.wait()=>break result?,
            _=interrupt.recv()=>{}, // The terminal sends SIGINT to this whole job.
            _=terminate.recv()=>{let _=kill(Pid::from_raw(pid as i32),Signal::SIGTERM);},
            _=hangup.recv()=>{let _=kill(Pid::from_raw(pid as i32),Signal::SIGHUP);},
            _=quit.recv()=>{let _=kill(Pid::from_raw(pid as i32),Signal::SIGQUIT);},
            message=receive::<LeaseResponse>(&mut read)=>{return match message {Ok(Some(LeaseResponse::Error{error}))=>Err(error),Err(error)=>Err(error),_=>Err(AppError::new("launcher_disconnected","session server disconnected"))};}
        }};
        send(&mut write,&LeaseRequest::Exited{code:status.code()}).await?;
        match receive(&mut read).await? {Some(LeaseResponse::Finished)=>Ok(()),Some(LeaseResponse::Error{error})=>Err(error),_=>Err(AppError::new("launcher_disconnected","session server did not release Pi lease"))}
    }.await;
    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    reset_terminal(saved);
    result
}
