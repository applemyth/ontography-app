//! Server-owned native terminals. Attachments are views, never process owners.
//!
//! `vt100` owns terminal interpretation and screen serialization. Its callbacks
//! answer terminal queries locally, including while no client is attached.
use crate::{AppError, Result, protocol};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::{UnixListener, UnixStream},
    sync::{broadcast, watch},
};

pub const VERSION: u32 = 2;
const INPUT_LIMIT: usize = 64 * 1024;
const SCROLLBACK: usize = 1_000;

#[derive(Clone, Debug)]
pub struct LaunchSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub rows: u16,
    pub cols: u16,
    pub server_id: String,
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachRequest {
    pub version: u32,
    pub server_id: String,
    pub session_id: String,
    pub terminal_id: String,
    pub rows: u16,
    pub cols: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalStatus {
    pub terminal_id: String,
    pub socket: PathBuf,
    pub pid: Option<u32>,
    pub running: bool,
    pub exit_code: Option<u32>,
    pub rows: u16,
    pub cols: u16,
    pub attached: bool,
    pub sequence: u64,
    pub fault: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub terminal_id: String,
    pub sequence: u64,
    pub rows: u16,
    pub cols: u16,
    /// A complete vt100-formatted visible screen, including cursor state.
    pub screen: String,
    pub application_cursor: bool,
    pub bracketed_paste: bool,
    pub mouse: bool,
    pub keyboard_flags: u16,
    pub exit_code: Option<u32>,
    pub output_closed: bool,
    pub fault: Option<String>,
    /// Present only while this attachment browses a frozen copy of history.
    pub history: Option<HistoryPosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryPosition {
    pub offset: usize,
    pub total: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryAction {
    Enter,
    /// Positive rows move toward older output; negative rows toward newer.
    Move {
        rows: i32,
    },
    Oldest,
    Newest,
    Exit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    Attached { terminal_id: String },
    Snapshot { snapshot: Snapshot },
    Graph,
    Detach,
    Error { error: AppError },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientFrame {
    Input {
        terminal_id: String,
        bytes: Vec<u8>,
    },
    Resize {
        terminal_id: String,
        rows: u16,
        cols: u16,
    },
    Detach {
        terminal_id: String,
    },
    History {
        terminal_id: String,
        action: HistoryAction,
    },
}

/// Owned by an attachment, never installed in the live parser. Freezing the
/// bounded screen/history on entry keeps navigation stable as output continues.
struct History {
    screen: vt100::Screen,
    total: usize,
}

impl History {
    fn new(screen: &vt100::Screen) -> Self {
        let mut screen = screen.clone();
        screen.set_scrollback(SCROLLBACK);
        let total = screen.scrollback();
        screen.set_scrollback(0);
        Self { screen, total }
    }

    fn navigate(&mut self, action: HistoryAction) {
        let offset = match action {
            HistoryAction::Move { rows } => (self.screen.scrollback() as i64 + i64::from(rows))
                .clamp(0, self.total as i64) as usize,
            HistoryAction::Oldest => self.total,
            HistoryAction::Newest => 0,
            HistoryAction::Enter | HistoryAction::Exit => return,
        };
        self.screen.set_scrollback(offset);
    }

    fn position(&self) -> HistoryPosition {
        HistoryPosition {
            offset: self.screen.scrollback(),
            total: self.total,
        }
    }
}

/// Extra protocol behavior; cell layout, escape parsing and Unicode live in vt100.
#[derive(Default)]
struct Queries {
    replies: Vec<u8>,
    keyboard_stack: Vec<u16>,
    synchronized_since: Option<std::time::Instant>,
}

impl Queries {
    fn reply(&mut self, bytes: &[u8]) {
        if self.replies.len() + bytes.len() <= INPUT_LIMIT {
            self.replies.extend_from_slice(bytes);
        }
    }
    fn keyboard_flags(&self) -> u16 {
        self.keyboard_stack.last().copied().unwrap_or(0)
    }
}

impl vt100::Callbacks for Queries {
    fn unhandled_csi(
        &mut self,
        screen: &mut vt100::Screen,
        i1: Option<u8>,
        i2: Option<u8>,
        params: &[&[u16]],
        c: char,
    ) {
        let p = |index: usize| {
            params
                .get(index)
                .and_then(|p| p.first())
                .copied()
                .unwrap_or(0)
        };
        match (i1, i2, c) {
            (None, None, 'n') if p(0) == 5 => self.reply(b"\x1b[0n"),
            (None | Some(b'?'), None, 'n') if p(0) == 6 => {
                let (row, col) = screen.cursor_position();
                self.reply(
                    format!(
                        "\x1b[{}{};{}R",
                        if i1.is_some() { "?" } else { "" },
                        row + 1,
                        col + 1
                    )
                    .as_bytes(),
                );
            }
            (None, None, 'c') => self.reply(b"\x1b[?1;2c"),
            (Some(b'>'), None, 'c') => self.reply(b"\x1b[>0;1;0c"),
            (None, None, 't') if p(0) == 18 || p(0) == 19 => {
                let (rows, cols) = screen.size();
                self.reply(
                    format!("\x1b[{};{rows};{cols}t", if p(0) == 18 { 8 } else { 9 }).as_bytes(),
                );
            }
            // Keyboard events are encoded by our client, so this capability is
            // available regardless of the outer terminal's own negotiation.
            (Some(b'>'), None, 'u') => {
                if self.keyboard_stack.len() < 16 {
                    self.keyboard_stack.push(p(0) & 7);
                }
            }
            (Some(b'<'), None, 'u') => {
                for _ in 0..p(0).clamp(1, 16) {
                    self.keyboard_stack.pop();
                }
            }
            (Some(b'='), None, 'u') => {
                let flags = p(0) & 7;
                let old = self.keyboard_flags();
                let flags = match p(1) {
                    2 => old | flags,
                    3 => old & !flags,
                    _ => flags,
                };
                if let Some(last) = self.keyboard_stack.last_mut() {
                    *last = flags;
                } else {
                    self.keyboard_stack.push(flags);
                }
            }
            (Some(b'?'), None, 'u') => {
                self.reply(format!("\x1b[?{}u", self.keyboard_flags()).as_bytes())
            }
            (Some(b'?'), None, 'h') if p(0) == 2026 => {
                self.synchronized_since
                    .get_or_insert_with(std::time::Instant::now);
            }
            (Some(b'?'), None, 'l') if p(0) == 2026 => self.synchronized_since = None,
            (Some(b'?'), Some(b'$'), 'p') if p(0) == 2026 => self.reply(
                format!(
                    "\x1b[?2026;{}$y",
                    if self.synchronized_since.is_some() {
                        1
                    } else {
                        2
                    }
                )
                .as_bytes(),
            ),
            // Unknown DEC modes are reported as unsupported, not forwarded to
            // whichever physical terminal happens to be attached.
            (Some(b'?'), Some(b'$'), 'p') => self.reply(format!("\x1b[?{};0$y", p(0)).as_bytes()),
            _ => {}
        }
    }

    fn unhandled_osc(&mut self, _: &mut vt100::Screen, params: &[&[u8]]) {
        match params {
            [b"10", b"?"] => self.reply(b"\x1b]10;rgb:dddd/dddd/dddd\x1b\\"),
            [b"11", b"?"] => self.reply(b"\x1b]11;rgb:0000/0000/0000\x1b\\"),
            _ => {}
        }
    }
}

struct State {
    parser: vt100::Parser<Queries>,
    sequence: u64,
    exit_code: Option<u32>,
    output_closed: bool,
    fault: Option<String>,
}

struct Shared {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
    stopping: AtomicBool,
}

impl Shared {
    fn update(&self, action: impl FnOnce(&mut State)) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        action(&mut state);
        state.sequence += 1;
        self.changed.send_replace(state.sequence);
    }
    fn fault(&self, error: impl std::fmt::Display) {
        self.update(|s| s.fault = Some(error.to_string()));
    }
}

struct SocketPath {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl SocketPath {
    fn remove(&self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        self.remove();
    }
}

pub struct Terminal {
    id: String,
    socket: SocketPath,
    server_id: String,
    session_id: String,
    pid: Option<u32>,
    shared: Arc<Shared>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    child: Arc<Mutex<Box<dyn Child + Send + Sync>>>,
    input: mpsc::SyncSender<Vec<u8>>,
    attached: AtomicBool,
    graph: broadcast::Sender<Control>,
    shutdown: watch::Sender<bool>,
}

#[derive(Clone, Copy)]
enum Control {
    Graph,
    Detach,
}

impl Terminal {
    pub async fn launch(spec: LaunchSpec, socket_path: PathBuf) -> Result<Arc<Self>> {
        validate_size(spec.rows, spec.cols)?;
        prepare_socket(&socket_path).await?;
        let listener = UnixListener::bind(&socket_path)?;
        let metadata = std::fs::symlink_metadata(&socket_path)?;
        let socket = SocketPath {
            path: socket_path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        std::fs::set_permissions(&socket.path, std::fs::Permissions::from_mode(0o600))?;
        let spawn_spec = spec.clone();
        let (master, child, mut reader, mut writer) = tokio::task::spawn_blocking(move || {
            let pair = native_pty_system()
                .openpty(PtySize {
                    rows: spawn_spec.rows,
                    cols: spawn_spec.cols,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .map_err(pty_error)?;
            let mut command = CommandBuilder::new(&spawn_spec.program);
            command.args(&spawn_spec.args);
            command.cwd(&spawn_spec.cwd);
            // Advertise the virtual terminal, rather than unsupported capabilities
            // inherited from a GUI terminal which launched the background server.
            for name in [
                "TERM_PROGRAM",
                "TERM_PROGRAM_VERSION",
                "KITTY_WINDOW_ID",
                "WEZTERM_PANE",
                "ITERM_SESSION_ID",
                "TMUX",
                "STY",
            ] {
                command.env_remove(name);
            }
            command.env("TERM", "xterm-256color");
            command.env("COLORTERM", "truecolor");
            command.env("PI_IMAGE_PROTOCOL", "none");
            command.env("PI_HYPERLINKS", "0");
            for (key, value) in spawn_spec.env {
                command.env(key, value);
            }
            let reader = pair.master.try_clone_reader().map_err(pty_error)?;
            let writer = pair.master.take_writer().map_err(pty_error)?;
            if let Some(fd) = pair.master.as_raw_fd() {
                use nix::fcntl::{FcntlArg, OFlag, fcntl};
                let flags = fcntl(fd, FcntlArg::F_GETFL).map_err(pty_error)?;
                fcntl(
                    fd,
                    FcntlArg::F_SETFL(OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK),
                )
                .map_err(pty_error)?;
            }
            let child = pair.slave.spawn_command(command).map_err(pty_error)?;
            drop(pair.slave);
            Ok::<_, AppError>((pair.master, child, reader, writer))
        })
        .await
        .map_err(pty_error)??;
        let pid = child.process_id();
        let (changed, _) = watch::channel(0);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                parser: vt100::Parser::new_with_callbacks(
                    spec.rows,
                    spec.cols,
                    SCROLLBACK,
                    Queries::default(),
                ),
                sequence: 0,
                exit_code: None,
                output_closed: false,
                fault: None,
            }),
            changed,
            stopping: AtomicBool::new(false),
        });
        let (input, input_rx) = mpsc::sync_channel::<Vec<u8>>(64);
        let writer_shared = shared.clone();
        std::thread::spawn(move || {
            while let Ok(bytes) = input_rx.recv() {
                let mut rest = bytes.as_slice();
                while !rest.is_empty() && !writer_shared.stopping.load(Ordering::Acquire) {
                    match writer.write(rest) {
                        Ok(0) => {
                            writer_shared.fault("PTY input closed");
                            return;
                        }
                        Ok(count) => rest = &rest[count..],
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5))
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(error) => {
                            writer_shared.fault(error);
                            return;
                        }
                    }
                }
                if writer_shared.stopping.load(Ordering::Acquire) {
                    return;
                }
            }
        });
        let reader_shared = shared.clone();
        let replies = input.clone();
        std::thread::spawn(move || {
            let mut bytes = [0u8; 8192];
            loop {
                match reader.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(count) => {
                        let mut responses = Vec::new();
                        reader_shared.update(|state| {
                            state.parser.process(&bytes[..count]);
                            responses = std::mem::take(&mut state.parser.callbacks_mut().replies);
                        });
                        if !responses.is_empty() && replies.try_send(responses).is_err() {
                            reader_shared.fault("PTY reply queue is full");
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if reader_shared.stopping.load(Ordering::Acquire) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) if error.raw_os_error() == Some(nix::libc::EIO) => break,
                    Err(error) => {
                        reader_shared.fault(error);
                        break;
                    }
                }
            }
            reader_shared.update(|state| state.output_closed = true);
        });
        let child = Arc::new(Mutex::new(child));
        let reaper_child = child.clone();
        let reaper_shared = shared.clone();
        std::thread::spawn(move || {
            loop {
                let result = reaper_child
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .try_wait();
                match result {
                    Ok(Some(status)) => {
                        reaper_shared.update(|state| state.exit_code = Some(status.exit_code()));
                        break;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                    Err(error) => {
                        reaper_shared.fault(error);
                        break;
                    }
                }
            }
        });
        let (graph, _) = broadcast::channel(8);
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let terminal = Arc::new(Self {
            id: uuid::Uuid::new_v4().to_string(),
            socket,
            server_id: spec.server_id,
            session_id: spec.session_id,
            pid,
            shared,
            master: Mutex::new(master),
            child,
            input,
            attached: AtomicBool::new(false),
            graph,
            shutdown,
        });
        let weak = Arc::downgrade(&terminal);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => if let Some(terminal) = weak.upgrade() {
                            tokio::spawn(async move { let _ = terminal.accept(stream).await; });
                        } else { break; },
                        Err(_) => break,
                    }
                }
            }
        });
        Ok(terminal)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn status(&self) -> TerminalStatus {
        let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        let (rows, cols) = state.parser.screen().size();
        TerminalStatus {
            terminal_id: self.id.clone(),
            socket: self.socket.path.clone(),
            pid: self.pid,
            running: state.exit_code.is_none() && !self.shared.stopping.load(Ordering::Acquire),
            exit_code: state.exit_code,
            rows,
            cols,
            attached: self.attached.load(Ordering::Acquire),
            sequence: state.sequence,
            fault: state.fault.clone(),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_for(None)
    }

    fn snapshot_for(&self, history: Option<&History>) -> Snapshot {
        let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        let live = state.parser.screen();
        let screen = history.map_or(live, |history| &history.screen);
        let (rows, cols) = screen.size();
        Snapshot {
            terminal_id: self.id.clone(),
            sequence: state.sequence,
            rows,
            cols,
            screen: String::from_utf8_lossy(&screen.contents_formatted()).into_owned(),
            application_cursor: live.application_cursor(),
            bracketed_paste: live.bracketed_paste(),
            mouse: live.mouse_protocol_mode() != vt100::MouseProtocolMode::None,
            keyboard_flags: state.parser.callbacks().keyboard_flags(),
            exit_code: state.exit_code,
            output_closed: state.output_closed,
            fault: state.fault.clone(),
            history: history.map(History::position),
        }
    }

    fn snapshot_ready(&self) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .parser
            .callbacks()
            .synchronized_since
            .is_none_or(|start| start.elapsed() >= Duration::from_millis(200))
    }

    pub fn request_graph_view(&self) -> Result<()> {
        self.graph.send(Control::Graph).map(|_| ()).map_err(|_| {
            AppError::new(
                "no_attachment",
                "Attach to this session before opening its graph",
            )
        })
    }

    pub fn request_detach(&self) -> Result<()> {
        self.graph.send(Control::Detach).map(|_| ()).map_err(|_| {
            AppError::new(
                "no_attachment",
                "This session has no attached terminal client",
            )
        })
    }

    fn resize(&self, rows: u16, cols: u16) -> Result<()> {
        validate_size(rows, cols)?;
        let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        self.master
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(pty_error)?;
        state.parser.screen_mut().set_size(rows, cols);
        state.sequence += 1;
        self.shared.changed.send_replace(state.sequence);
        Ok(())
    }

    fn input(&self, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > INPUT_LIMIT {
            return Err(AppError::new(
                "input_too_large",
                "Terminal input frame exceeds 64 KiB",
            ));
        }
        if *self.shutdown.borrow() {
            return Err(AppError::new(
                "terminal_stopping",
                "This terminal is stopping",
            ));
        }
        if !self.status().running {
            return Err(AppError::new(
                "terminal_exited",
                "This terminal process has exited",
            ));
        }
        self.input.try_send(bytes).map_err(|_| {
            AppError::new(
                "terminal_busy",
                "Terminal input queue is full or closed; input was not accepted",
            )
        })
    }

    async fn accept(self: &Arc<Self>, stream: UnixStream) -> Result<()> {
        let mut reader = BufReader::new(stream);
        let request =
            tokio::time::timeout(Duration::from_secs(3), protocol::read_frame(&mut reader))
                .await
                .map_err(|_| AppError::new("handshake_timeout", "Terminal handshake timed out"))??
                .ok_or_else(|| AppError::invalid("Missing terminal handshake"))?;
        let request: AttachRequest = serde_json::from_slice(&request)?;
        let validation = if request.version != VERSION
            || request.server_id != self.server_id
            || request.session_id != self.session_id
            || request.terminal_id != self.id
        {
            Err(AppError::new(
                "stale_terminal",
                "Resolve the session's current terminal before attaching",
            ))
        } else if self.shared.stopping.load(Ordering::Acquire) || *self.shutdown.borrow() {
            Err(AppError::new(
                "terminal_stopping",
                "This terminal is stopping",
            ))
        } else {
            validate_size(request.rows, request.cols)
        };
        if let Err(error) = validation {
            protocol::write_frame(reader.get_mut(), &ServerFrame::Error { error }).await?;
            return Ok(());
        }
        if self
            .attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            protocol::write_frame(
                reader.get_mut(),
                &ServerFrame::Error {
                    error: AppError::new(
                        "terminal_attached",
                        "This terminal already has a controlling client; detach it first",
                    ),
                },
            )
            .await?;
            return Ok(());
        }
        struct Lease<'a>(&'a AtomicBool);
        impl Drop for Lease<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _lease = Lease(&self.attached);
        self.resize(request.rows, request.cols)?;
        let (read, mut write) = reader.into_inner().into_split();
        let mut read = BufReader::new(read);
        let mut changed = self.shared.changed.subscribe();
        let mut graphs = self.graph.subscribe();
        let mut shutdown = self.shutdown.subscribe();
        send(
            &mut write,
            &ServerFrame::Attached {
                terminal_id: self.id.clone(),
            },
        )
        .await?;
        let snapshot = self.snapshot();
        let exited = snapshot.exit_code.is_some() && snapshot.output_closed;
        send(&mut write, &ServerFrame::Snapshot { snapshot }).await?;
        if exited {
            return Ok(());
        }
        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(8);
        let input_task = tokio::spawn(async move {
            loop {
                let result = protocol::read_frame(&mut read).await.and_then(|frame| {
                    frame
                        .map(|bytes| {
                            serde_json::from_slice::<ClientFrame>(&bytes).map_err(AppError::from)
                        })
                        .transpose()
                });
                let done = !matches!(result, Ok(Some(_)));
                if input_tx.send(result).await.is_err() || done {
                    break;
                }
            }
        });
        let outcome = async {
            let mut history: Option<History> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(33));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    event = graphs.recv() => match event {
                        Ok(Control::Graph) => send(&mut write, &ServerFrame::Graph).await?,
                        Ok(Control::Detach) => { send(&mut write, &ServerFrame::Detach).await?; break; },
                        _ => {},
                    },
                    _ = tick.tick() => if changed.has_changed().unwrap_or(false) && self.snapshot_ready() {
                        changed.borrow_and_update();
                        let snapshot = self.snapshot_for(history.as_ref());
                        let exited = snapshot.exit_code.is_some() && snapshot.output_closed;
                        send(&mut write, &ServerFrame::Snapshot { snapshot }).await?;
                        if exited { break; }
                    },
                    frame = input_rx.recv() => match frame {
                        Some(Ok(Some(frame))) => {
                            let id = match &frame { ClientFrame::Input { terminal_id, .. } | ClientFrame::Resize { terminal_id, .. } | ClientFrame::Detach { terminal_id } | ClientFrame::History { terminal_id, .. } => terminal_id };
                            if id != &self.id { return Err(AppError::new("stale_terminal", "Terminal input belongs to another execution")); }
                            match frame {
                                ClientFrame::Input { bytes, .. } => self.input(bytes)?,
                                ClientFrame::Resize { rows, cols, .. } => {
                                    // A resized live terminal has different geometry. Return to
                                    // it without reflowing or mutating the frozen history.
                                    history = None;
                                    self.resize(rows, cols)?;
                                },
                                ClientFrame::Detach { .. } => break,
                                ClientFrame::History { action, .. } => {
                                    match action {
                                        HistoryAction::Exit => history = None,
                                        HistoryAction::Enter => {
                                            if history.is_none() {
                                                let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
                                                history = Some(History::new(state.parser.screen()));
                                            }
                                        },
                                        action => if let Some(history) = history.as_mut() { history.navigate(action); },
                                    }
                                    send(&mut write, &ServerFrame::Snapshot { snapshot: self.snapshot_for(history.as_ref()) }).await?;
                                },
                            }
                        },
                        Some(Err(error)) => return Err(error),
                        _ => break,
                    }
                }
            }
            Ok::<_, AppError>(())
        }.await;
        if let Err(error) = &outcome {
            let _ = send(
                &mut write,
                &ServerFrame::Error {
                    error: error.clone(),
                },
            )
            .await;
        }
        input_task.abort();
        outcome
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown.send_replace(true);
        let child = self.child.clone();
        let pid = self.pid;
        let reaping = tokio::task::spawn_blocking(move || {
            let mut child = child.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(status) = child.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            if let Some(pid) = pid {
                let _ = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGHUP,
                );
            }
            child.kill()?;
            if let Some(pid) = pid {
                let _ = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            child.wait()
        });
        let status = tokio::time::timeout(Duration::from_secs(3), reaping)
            .await
            .map_err(|_| AppError::new("terminal_stop_timeout", "Terminal termination did not finish within three seconds; inspect its process before retrying"))?
            .map_err(pty_error)??;
        self.shared
            .update(|state| state.exit_code = Some(status.exit_code()));
        self.shared.stopping.store(true, Ordering::Release);
        let _ = self.input.try_send(Vec::new());
        self.socket.remove();
        Ok(())
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.shared.stopping.store(true, Ordering::Release);
        let _ = self.input.try_send(Vec::new());
        if let Ok(mut child) = self.child.lock()
            && matches!(child.try_wait(), Ok(None))
        {
            let _ = child.kill();
            if let Some(pid) = self.pid {
                let _ = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            let _ = child.wait();
        }
    }
}

fn pty_error(error: impl std::fmt::Display) -> AppError {
    AppError::new("terminal_error", error.to_string())
}

pub fn validate_size(rows: u16, cols: u16) -> Result<()> {
    if rows == 0
        || cols == 0
        || rows > 200
        || cols > 500
        || u32::from(rows) * u32::from(cols) > 30_000
    {
        return Err(AppError::invalid(
            "Terminal dimensions must fit within 200 rows, 500 columns, and 30,000 cells",
        ));
    }
    Ok(())
}

async fn prepare_socket(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| AppError::invalid("Terminal socket requires a parent directory"))?;
    let metadata = std::fs::metadata(parent)?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::Uid::effective().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(AppError::new(
            "insecure_socket_directory",
            "Terminal sockets require a directory owned by this user with mode 0700",
        ));
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket()
                || metadata.uid() != nix::unistd::Uid::effective().as_raw()
            {
                return Err(AppError::new(
                    "socket_conflict",
                    "Terminal socket path is occupied by another file",
                ));
            }
            match UnixStream::connect(path).await {
                Ok(_) => {
                    return Err(AppError::new(
                        "terminal_owned",
                        "A live terminal service already owns this socket",
                    ));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path)?
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn send(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &ServerFrame,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), protocol::write_frame(writer, frame))
        .await
        .map_err(|_| {
            AppError::new(
                "slow_attachment",
                "Terminal client stopped receiving; reconnect for the current screen",
            )
        })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_use_emulated_cursor_and_work_across_chunks() {
        let mut parser = vt100::Parser::new_with_callbacks(24, 80, 0, Queries::default());
        parser.process("α\x1b[5;9H\x1b[".as_bytes());
        parser.process(b"6n\x1b[18t\x1b[>7u\x1b[?u\x1b[c\x1b]11;?\x1b\\");
        assert_eq!(
            parser.callbacks().replies,
            b"\x1b[5;9R\x1b[8;24;80t\x1b[?7u\x1b[?1;2c\x1b]11;rgb:0000/0000/0000\x1b\\"
        );
        assert_eq!(parser.screen().cell(0, 0).unwrap().contents(), "α");
    }

    #[test]
    fn screen_snapshot_preserves_unicode_styles_and_cursor() {
        let mut parser = vt100::Parser::new_with_callbacks(24, 80, 100, Queries::default());
        parser.process("\x1b[?1049h\x1b[31m你😀\x1b[5;9Hdone".as_bytes());
        let mut restored = vt100::Parser::new(24, 80, 0);
        restored.process(&parser.screen().contents_formatted());
        assert_eq!(restored.screen().contents(), parser.screen().contents());
        assert_eq!(
            restored.screen().cursor_position(),
            parser.screen().cursor_position()
        );
        assert_eq!(
            restored.screen().cell(0, 0).unwrap().fgcolor(),
            vt100::Color::Idx(1)
        );
    }

    #[test]
    fn history_navigation_never_mutates_live_parser_or_query_state() {
        let mut parser = vt100::Parser::new_with_callbacks(3, 20, 4, Queries::default());
        for line in 0..9 {
            parser.process(format!("line-{line}\r\n").as_bytes());
        }
        parser.process(b"\x1b[?1h\x1b[?2004h\x1b[>7u");
        let live = parser.screen().contents_formatted();
        let cursor = parser.screen().cursor_position();
        let mut history = History::new(parser.screen());
        assert_eq!(history.total, 4);
        history.navigate(HistoryAction::Move { rows: i32::MAX });
        assert_eq!(history.position().offset, 4);
        assert!(history.screen.contents().contains("line-3"));
        assert_eq!(parser.screen().contents_formatted(), live);
        assert_eq!(parser.screen().cursor_position(), cursor);
        assert_eq!(parser.screen().scrollback(), 0);
        let frozen = history.screen.contents_formatted();

        parser.process(b"\x1b[2;4H\x1b[6n\x1b[?u\x1b[18tnew output");
        assert_eq!(parser.callbacks().replies, b"\x1b[2;4R\x1b[?7u\x1b[8;3;20t");
        assert!(parser.screen().application_cursor());
        assert!(parser.screen().bracketed_paste());
        assert_eq!(history.screen.contents_formatted(), frozen);
        history.navigate(HistoryAction::Move { rows: i32::MIN });
        assert_eq!(history.position().offset, 0);
        assert_eq!(history.screen.contents_formatted(), live);
    }

    fn fixture(directory: &Path, command: &str) -> (LaunchSpec, PathBuf) {
        (
            LaunchSpec {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), command.into()],
                env: BTreeMap::new(),
                cwd: directory.into(),
                rows: 24,
                cols: 80,
                server_id: "server".into(),
                session_id: "session".into(),
            },
            directory.join("terminal.sock"),
        )
    }

    async fn attach(terminal: &Terminal) -> BufReader<UnixStream> {
        let mut stream = UnixStream::connect(&terminal.socket.path).await.unwrap();
        protocol::write_frame(
            &mut stream,
            &AttachRequest {
                version: VERSION,
                server_id: "server".into(),
                session_id: "session".into(),
                terminal_id: terminal.id.clone(),
                rows: 24,
                cols: 80,
            },
        )
        .await
        .unwrap();
        let mut reader = BufReader::new(stream);
        let frame = protocol::read_frame(&mut reader).await.unwrap().unwrap();
        assert!(matches!(
            serde_json::from_slice::<ServerFrame>(&frame).unwrap(),
            ServerFrame::Attached { .. }
        ));
        reader
    }

    async fn read_snapshot(reader: &mut BufReader<UnixStream>) -> Snapshot {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let frame = protocol::read_frame(reader).await.unwrap().unwrap();
                if let ServerFrame::Snapshot { snapshot } = serde_json::from_slice(&frame).unwrap()
                {
                    break snapshot;
                }
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn history_is_attachment_local_while_output_queries_and_input_continue() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(
            directory.path(),
            r"stty -echo -icanon min 1; i=0; while [ $i -lt 60 ]; do printf 'line-%02d\n' $i; i=$((i+1)); done; printf ready; while [ ! -f go ]; do sleep 0.01; done; printf '\033[4;9H\033[6n'; dd bs=1 count=6 of=reply 2>/dev/null; dd bs=1 count=3 of=input 2>/dev/null; printf 'new-output'; read line",
        );
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !terminal.snapshot().screen.contains("ready") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut reader = attach(&terminal).await;
        assert!(read_snapshot(&mut reader).await.history.is_none());
        for action in [HistoryAction::Enter, HistoryAction::Oldest] {
            protocol::write_frame(
                reader.get_mut(),
                &ClientFrame::History {
                    terminal_id: terminal.id().into(),
                    action,
                },
            )
            .await
            .unwrap();
        }
        let frozen = loop {
            let snapshot = read_snapshot(&mut reader).await;
            if snapshot
                .history
                .as_ref()
                .is_some_and(|h| h.offset > 0 && h.offset == h.total)
            {
                break snapshot;
            }
        };
        assert!(frozen.screen.contains("line-00"));
        assert!(frozen.screen.len() < 64 * 1024);
        std::fs::write(directory.path().join("go"), b"").unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while std::fs::read(directory.path().join("reply"))
                .unwrap_or_default()
                .len()
                != 6
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(directory.path().join("reply")).unwrap(),
            b"\x1b[4;9R"
        );
        protocol::write_frame(
            reader.get_mut(),
            &ClientFrame::Input {
                terminal_id: terminal.id().into(),
                bytes: b"ok\n".to_vec(),
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !terminal.snapshot().screen.contains("new-output") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(directory.path().join("input")).unwrap(),
            b"ok\n"
        );
        let updated = loop {
            let snapshot = read_snapshot(&mut reader).await;
            if snapshot.sequence > frozen.sequence {
                break snapshot;
            }
        };
        assert_eq!(updated.screen, frozen.screen);
        assert!(updated.history.is_some());
        assert_eq!(
            terminal
                .shared
                .state
                .lock()
                .unwrap()
                .parser
                .screen()
                .scrollback(),
            0
        );

        protocol::write_frame(
            reader.get_mut(),
            &ClientFrame::Detach {
                terminal_id: terminal.id().into(),
            },
        )
        .await
        .unwrap();
        drop(reader);
        tokio::time::timeout(Duration::from_secs(3), async {
            while terminal.status().attached {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut reattached = attach(&terminal).await;
        let snapshot = read_snapshot(&mut reattached).await;
        assert!(snapshot.history.is_none());
        assert!(snapshot.screen.contains("new-output"));
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn detach_keeps_process_and_collects_output_for_reconnect() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(
            directory.path(),
            "printf ready; read line; printf 'received:%s' \"$line\"; read line",
        );
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        let first_pid = terminal.status().pid;
        let mut first = attach(&terminal).await;
        protocol::write_frame(
            first.get_mut(),
            &ClientFrame::Detach {
                terminal_id: terminal.id.clone(),
            },
        )
        .await
        .unwrap();
        drop(first);
        tokio::time::timeout(Duration::from_secs(2), async {
            while terminal.status().attached {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        terminal.input(b"hello\n".to_vec()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !terminal.snapshot().screen.contains("received:hello") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut second = attach(&terminal).await;
        let frame = protocol::read_frame(&mut second).await.unwrap().unwrap();
        let ServerFrame::Snapshot { snapshot } = serde_json::from_slice(&frame).unwrap() else {
            panic!("missing screen")
        };
        assert!(snapshot.screen.contains("received:hello"));
        assert_eq!(terminal.status().pid, first_pid);
        terminal.shutdown().await.unwrap();
        assert!(!terminal.status().running);
    }

    #[tokio::test]
    async fn detached_process_receives_terminal_query_response() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(
            directory.path(),
            r"stty -echo -icanon min 1; printf '\033[4;9H\033[6n'; dd bs=1 count=6 of=reply 2>/dev/null",
        );
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while terminal.status().running {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(directory.path().join("reply")).unwrap(),
            b"\x1b[4;9R"
        );
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn controller_is_exclusive_and_external_detach_retains_process() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(directory.path(), "read line");
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        let mut first = attach(&terminal).await;
        let mut second = UnixStream::connect(&terminal.socket.path).await.unwrap();
        protocol::write_frame(
            &mut second,
            &AttachRequest {
                version: VERSION,
                server_id: "server".into(),
                session_id: "session".into(),
                terminal_id: terminal.id.clone(),
                rows: 24,
                cols: 80,
            },
        )
        .await
        .unwrap();
        let response = protocol::read_frame(&mut BufReader::new(second))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(serde_json::from_slice::<ServerFrame>(&response).unwrap(), ServerFrame::Error { error } if error.code == "terminal_attached")
        );
        terminal.request_graph_view().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let response = protocol::read_frame(&mut first).await.unwrap().unwrap();
                if matches!(
                    serde_json::from_slice::<ServerFrame>(&response).unwrap(),
                    ServerFrame::Graph
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        terminal.request_detach().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while terminal.status().attached {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(terminal.status().running);
        let _new_attachment = attach(&terminal).await;
        terminal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_identity_is_rejected_and_new_socket_survives_old_owner_drop() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(directory.path(), "read line");
        let old = Terminal::launch(spec.clone(), socket.clone())
            .await
            .unwrap();
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        protocol::write_frame(
            &mut stream,
            &AttachRequest {
                version: VERSION,
                server_id: "server".into(),
                session_id: "session".into(),
                terminal_id: "old-instance".into(),
                rows: 24,
                cols: 80,
            },
        )
        .await
        .unwrap();
        let response = protocol::read_frame(&mut BufReader::new(stream))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(serde_json::from_slice::<ServerFrame>(&response).unwrap(), ServerFrame::Error { error } if error.code == "stale_terminal")
        );
        old.shutdown().await.unwrap();
        let new = Terminal::launch(spec, socket.clone()).await.unwrap();
        drop(old);
        assert!(socket.exists());
        let _attachment = attach(&new).await;
        new.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_reaps_a_process_which_ignores_hangup() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(
            directory.path(),
            "trap '' HUP; printf ready; while :; do sleep 1; done",
        );
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !terminal.snapshot().screen.contains("ready") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let pid = terminal.status().pid.unwrap();
        tokio::time::timeout(Duration::from_secs(2), terminal.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(!terminal.status().running);
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
            Err(nix::errno::Errno::ESRCH)
        );
    }

    #[tokio::test]
    async fn disconnected_output_is_drained_with_bounded_screen_storage() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let (spec, socket) = fixture(
            directory.path(),
            r#"i=0; while [ "$i" -lt 4000 ]; do printf '%080d\n' "$i"; i=$((i+1)); done; printf COMPLETE; read line"#,
        );
        let terminal = Terminal::launch(spec, socket).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !terminal.snapshot().screen.contains("COMPLETE") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(terminal.snapshot().screen.len() < 10_000);
        let _attachment = attach(&terminal).await;
        terminal.shutdown().await.unwrap();
    }
}
