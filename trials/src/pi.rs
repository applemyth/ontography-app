//! Pi mode: this binary as a session's manager program, standing in for
//! native Pi as plainly as a trial needs. The server checks its version, then
//! launches it through the session's shell with Pi's arguments and
//! environment; like Pi's extension, it registers the conversation it was
//! given. Then it answers what the trial types, a line at a time: `/quit`
//! exits to the shell, `/size` reports the terminal's size as a size change
//! does, `/child` and `/child-hup` start a program in its own process group
//! that keeps running (the second ignores SIGHUP), `/graph` asks the server
//! to show the graph, and any other line comes back as `ECHO <line>`.
//! Everything it sees goes to its session's witness file: the outside record
//! of what reached Pi.

use anyhow::{Context, Result, bail};
use ontography_app::client::Client;
use rustix::termios::{LocalModes, OptionalActions, Termios, tcgetattr, tcgetwinsize, tcsetattr};
use serde_json::{Map, Value, json};
use std::io::{BufRead, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};

/// The Pi version the server requires.
const VERSION: &str = "0.85.1";
/// A child exits by itself after this long, so a harness killed outright
/// leaves nothing running for long.
const CHILD_LIFETIME: Duration = Duration::from_secs(15 * 60);
/// Variables Pi's witness records: what the app promises its programs.
const RECORDED: [&str; 6] = [
    "ONTOGRAPHY_SESSION_ID",
    "ONTOGRAPHY_DATA_DIR",
    "ONTOGRAPHY_SOCKET",
    "ONTOGRAPHY_TERMINAL",
    "TRIALS_ACTIVATION",
    "TERM",
];

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The directory of witness files, one per session.
    #[arg(long)]
    witness: PathBuf,
    /// Print the version, as `pi --version` does.
    #[arg(long)]
    version: bool,
    /// Run as a program Pi started, rather than as Pi.
    #[arg(long, value_enum)]
    child: Option<Hangup>,
    /// The session whose Pi started this child.
    #[arg(long)]
    owner: Option<String>,
    // Pi's own arguments, as the server launches it.
    #[arg(long)]
    extension: Option<PathBuf>,
    #[arg(long, allow_hyphen_values = true)]
    append_system_prompt: Option<String>,
    #[arg(long)]
    session_dir: Option<PathBuf>,
    /// A saved conversation to resume.
    #[arg(long)]
    session: Option<PathBuf>,
    /// The reserved identity of a conversation not yet saved.
    #[arg(long)]
    session_id: Option<String>,
}

/// What a child does with SIGHUP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Hangup {
    /// Dies of it, as programs do unless they say otherwise.
    Default,
    /// Ignores it.
    Ignore,
}

impl Hangup {
    pub fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Ignore => "ignore",
        }
    }
}

/// One session's witness file. Each record is one write, so the records of
/// Pi and its children never interleave within a line.
struct Witness {
    path: PathBuf,
}

impl Witness {
    fn new(directory: &Path, session: &str) -> Self {
        Self {
            path: directory.join(format!("{session}.jsonl")),
        }
    }

    fn record(&self, mut event: Value) {
        event["pid"] = json!(std::process::id());
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            let _ = file.write_all(format!("{event}\n").as_bytes());
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    if args.version {
        println!("{VERSION}");
        return Ok(());
    }
    match args.child {
        Some(hangup) => child(&args, hangup).await,
        None => pi(&args).await,
    }
}

async fn pi(args: &Args) -> Result<()> {
    let session = std::env::var("ONTOGRAPHY_SESSION_ID").unwrap_or_else(|_| "unknown".into());
    let witness = Witness::new(&args.witness, &session);
    let (conversation, history) = match (&args.session, &args.session_id, &args.session_dir) {
        (Some(path), None, _) => (history_id(path)?, path.clone()),
        (None, Some(id), Some(directory)) => (id.clone(), directory.join(format!("{id}.jsonl"))),
        _ => bail!("Pi takes --session, or --session-id with --session-dir"),
    };
    // Pi saves a history once something is said; this one is said at once.
    if !history.exists() {
        let header = json!({"type": "session", "id": conversation});
        std::fs::write(&history, format!("{header}\n"))?;
    }
    let registered = call(
        &session,
        "session.conversation",
        json!({"session_id": session, "action": "activate", "conversation_id": conversation, "path": history}),
    )
    .await;
    // Full-screen Pi reads keys itself: the terminal echoes nothing.
    let stdin = std::io::stdin();
    let saved = tcgetattr(&stdin).ok();
    if let Some(saved) = &saved {
        let mut quiet = saved.clone();
        quiet.local_modes.remove(LocalModes::ECHO);
        let _ = tcsetattr(&stdin, OptionalActions::Now, &quiet);
    }
    let (rows, cols) = size();
    let environment: Map<String, Value> = RECORDED
        .iter()
        .filter_map(|name| Some((name.to_string(), json!(std::env::var(name).ok()?))))
        .collect();
    witness.record(json!({
        "event": "start",
        "conversation": conversation,
        "resumed": args.session,
        "reserved": args.session_id,
        "session_dir": args.session_dir,
        "registration_error": registered.err(),
        "environment": environment,
        "rows": rows,
        "cols": cols,
    }));
    // Ready for signals before saying so.
    let mut resized = signal(SignalKind::window_change())?;
    let mut hangup = signal(SignalKind::hangup())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut screen = Screen::default();
    screen.write("\x1b[?1049h\x1b[2J\x1b[H");
    screen.say(format!("READY {} {conversation}", std::process::id()));
    // Lines arrive from a thread: a blocked read of the terminal must never
    // hold up an exit.
    let (sender, mut lines) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let report_size = |witness: &Witness, screen: &mut Screen, redraw: bool| {
        let (rows, cols) = size();
        witness.record(json!({"event": "size", "rows": rows, "cols": cols}));
        if redraw {
            screen.redraw(rows);
        }
        screen.say(format!("SIZE {rows} {cols}"));
    };
    loop {
        tokio::select! {
            line = lines.recv() => {
                let Some(line) = line else {
                    leave(&witness, saved.as_ref(), "end of input", 0)
                };
                let line = line.trim_end_matches('\r').to_owned();
                witness.record(json!({"event": "input", "line": line}));
                match line.as_str() {
                    "/quit" => leave(&witness, saved.as_ref(), "quit", 0),
                    "/size" => report_size(&witness, &mut screen, false),
                    "/child" | "/child-hup" => {
                        let hangup = if line == "/child" { Hangup::Default } else { Hangup::Ignore };
                        match spawn_child(&args.witness, &session, hangup) {
                            Ok(child) => {
                                witness.record(json!({"event": "spawned", "child": child, "hangup": hangup.name()}));
                                screen.say(format!("CHILD {child} {}", hangup.name()));
                            }
                            Err(error) => screen.say(format!("CHILD failed: {error:#}")),
                        }
                    }
                    "/graph" => {
                        let outcome = call(&session, "terminal.graph", json!({"session_id": session})).await;
                        let outcome = outcome.err().unwrap_or_else(|| "ok".into());
                        witness.record(json!({"event": "graph", "outcome": outcome}));
                        screen.say(format!("GRAPH {outcome}"));
                    }
                    _ => screen.say(format!("ECHO {line}")),
                }
            }
            _ = resized.recv() => report_size(&witness, &mut screen, true),
            _ = hangup.recv() => leave(&witness, saved.as_ref(), "hangup", 129),
            _ = terminate.recv() => leave(&witness, saved.as_ref(), "terminate", 143),
        }
    }
}

/// Leaves the full screen and restores the terminal, as Pi does on exit.
fn leave(witness: &Witness, saved: Option<&Termios>, why: &str, code: i32) -> ! {
    witness.record(json!({"event": "exit", "why": why}));
    print!("\x1b[?1049l");
    let _ = std::io::stdout().flush();
    if let Some(saved) = saved {
        let _ = tcsetattr(std::io::stdin(), OptionalActions::Now, saved);
    }
    std::process::exit(code)
}

/// What Pi has printed, so that a size change redraws it, as full-screen
/// Pi does; a terminal that shrinks otherwise cuts rows off.
#[derive(Default)]
struct Screen {
    lines: Vec<String>,
}

impl Screen {
    fn write(&self, text: &str) {
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    }

    fn say(&mut self, line: String) {
        self.write(&format!("{line}\r\n"));
        self.lines.push(line);
        if self.lines.len() > 500 {
            self.lines.drain(..100);
        }
    }

    /// Clears the screen and prints the latest lines that fit, leaving a
    /// row for what comes next.
    fn redraw(&self, rows: u16) {
        let keep = usize::from(rows).saturating_sub(2);
        let from = self.lines.len().saturating_sub(keep);
        let mut text = String::from("\x1b[2J\x1b[H");
        for line in &self.lines[from..] {
            text.push_str(line);
            text.push_str("\r\n");
        }
        self.write(&text);
    }
}

fn size() -> (u16, u16) {
    tcgetwinsize(std::io::stdin()).map_or((0, 0), |size| (size.ws_row, size.ws_col))
}

/// The conversation a saved history belongs to: its header's `id`.
fn history_id(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let header: Value = serde_json::from_str(text.lines().next().unwrap_or_default())?;
    header["id"]
        .as_str()
        .map(String::from)
        .context("a history without a session id")
}

/// Calls the server on the session's behalf, as Pi's extension does.
async fn call(session: &str, operation: &str, args: Value) -> Result<(), String> {
    let request = async {
        let socket = std::env::var("ONTOGRAPHY_SOCKET").context("no ONTOGRAPHY_SOCKET")?;
        let client = Client::connect(&socket).await?.for_session(session);
        client.call(operation, args).await?;
        anyhow::Ok(())
    };
    match tokio::time::timeout(Duration::from_secs(10), request).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("{error:#}")),
        Err(_) => Err("timed out".into()),
    }
}

/// Starts a child in its own process group, detached from the terminal's
/// input and output, as a tool starting a background server would.
fn spawn_child(witness: &Path, session: &str, hangup: Hangup) -> Result<u32> {
    let child = std::process::Command::new(std::env::current_exe()?)
        .arg("pi")
        .arg("--witness")
        .arg(witness)
        .args(["--child", hangup.name(), "--owner", session])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    Ok(child.id())
}

/// A child: it records itself and keeps running; one that ignores SIGHUP
/// records each it gets.
async fn child(args: &Args, hangup: Hangup) -> Result<()> {
    let session = args.owner.clone().unwrap_or_else(|| "unknown".into());
    let witness = Witness::new(&args.witness, &session);
    witness.record(json!({
        "event": "child",
        "hangup": hangup.name(),
        "group": nix::unistd::getpgrp().as_raw(),
        "parent": nix::unistd::getppid().as_raw(),
    }));
    let lifetime = tokio::time::sleep(CHILD_LIFETIME);
    tokio::pin!(lifetime);
    match hangup {
        Hangup::Default => lifetime.await,
        Hangup::Ignore => {
            let mut hangups = signal(SignalKind::hangup())?;
            loop {
                tokio::select! {
                    _ = hangups.recv() => witness.record(json!({"event": "hangup ignored"})),
                    _ = &mut lifetime => break,
                }
            }
        }
    }
    std::process::exit(0)
}
