//! A view onto an existing server-owned native terminal.
use crate::{
    AppError, Result,
    client::Client,
    protocol,
    terminal::{AttachRequest, ClientFrame, HistoryAction, ServerFrame, Snapshot},
};

#[path = "terminal_panel.rs"]
mod panel;
use crossterm::{
    event::{
        self, Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
        KeyboardEnhancementFlags, MouseButton, MouseEvent, MouseEventKind,
        PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use std::{
    future::Future,
    io::{self, IsTerminal, Write},
    path::Path,
    time::Duration,
};
use tokio::{
    io::BufReader,
    net::UnixStream,
    sync::{mpsc, watch},
};

struct TerminalGuard {
    active: bool,
}
impl TerminalGuard {
    fn enter() -> Result<Self> {
        let mut guard = Self { active: false };
        guard.resume()?;
        Ok(guard)
    }
    fn resume(&mut self) -> Result<()> {
        terminal::enable_raw_mode()?;
        self.active = true;
        execute!(
            io::stdout(),
            EnterAlternateScreen,
            event::EnableBracketedPaste,
            PushKeyboardEnhancementFlags(
                KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                    | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                    | KeyboardEnhancementFlags::REPORT_ALTERNATE_KEYS
            )
        )?;
        Ok(())
    }
    fn pause(&mut self) {
        if self.active {
            let _ = execute!(
                io::stdout(),
                event::DisableMouseCapture,
                PopKeyboardEnhancementFlags,
                event::DisableBracketedPaste,
                crossterm::cursor::Show,
                LeaveAlternateScreen
            );
            let _ = terminal::disable_raw_mode();
            self.active = false;
        }
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.pause();
    }
}

enum Notice {
    Graph,
    Closed,
    Error(AppError),
}

#[derive(Default)]
struct InputState {
    prefix: bool,
    history: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum InputAction {
    Ignore,
    Detach,
    Graph,
    History(HistoryAction),
    Input(Vec<u8>),
}

impl InputState {
    fn handle(&mut self, event: Event, snapshot: Option<&Snapshot>) -> InputAction {
        let mut prefix_bytes = Vec::new();
        if let Event::Key(key) = &event {
            if key.kind == KeyEventKind::Release {
                return InputAction::Ignore;
            }
            if self.prefix {
                self.prefix = false;
                match key.code {
                    KeyCode::Char('d' | 'D') => return InputAction::Detach,
                    KeyCode::Char('g' | 'G') => return InputAction::Graph,
                    KeyCode::Char('[') => {
                        self.history = true;
                        return InputAction::History(HistoryAction::Enter);
                    }
                    _ => prefix_bytes.push(2),
                }
            } else if matches!(key.code, KeyCode::Char('b' | 'B'))
                && key.modifiers.contains(KeyModifiers::CONTROL)
            {
                self.prefix = true;
                return InputAction::Ignore;
            }
        }
        if self.history {
            let page = snapshot.map_or(23, |s| i32::from(s.rows.saturating_sub(1).max(1)));
            let action = match event {
                Event::Key(key) => match key.code {
                    KeyCode::Esc | KeyCode::Char('q') => {
                        self.history = false;
                        HistoryAction::Exit
                    }
                    KeyCode::Up => HistoryAction::Move { rows: 1 },
                    KeyCode::Down => HistoryAction::Move { rows: -1 },
                    KeyCode::PageUp => HistoryAction::Move { rows: page },
                    KeyCode::PageDown => HistoryAction::Move { rows: -page },
                    KeyCode::Home => HistoryAction::Oldest,
                    KeyCode::End => HistoryAction::Newest,
                    _ => return InputAction::Ignore,
                },
                Event::Mouse(mouse) => match mouse.kind {
                    MouseEventKind::ScrollUp => HistoryAction::Move { rows: 3 },
                    MouseEventKind::ScrollDown => HistoryAction::Move { rows: -3 },
                    _ => return InputAction::Ignore,
                },
                _ => return InputAction::Ignore,
            };
            return InputAction::History(action);
        }
        prefix_bytes.extend(encode(event, snapshot));
        if prefix_bytes.is_empty() {
            InputAction::Ignore
        } else {
            InputAction::Input(prefix_bytes)
        }
    }
}

/// Attach to a process which is already owned by the server. Ctrl-B, D detaches;
/// Ctrl-B, [ browses history without sending navigation to the child. In live
/// mode other terminal input belongs to the child. A graph callback may own the
/// physical terminal until it returns; its absence never pauses the child.
pub async fn run<F, Fut>(socket: &Path, request: AttachRequest, mut graph: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    run_with_initial_graph(socket, request, false, &mut graph).await
}

/// The initial graph view uses the same controlling attachment and cancellable
/// callback as `/graph`; detaching remotely closes either view.
pub async fn run_with_initial_graph<F, Fut>(
    socket: &Path,
    request: AttachRequest,
    initial_graph: bool,
    graph: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    run_inner(socket, request, initial_graph, None, graph).await
}

/// Display native Pi at full size, and the persistent shell beside live session
/// status after Pi exits. The backend's explicit mode determines the layout.
pub async fn run_with_session_panel<F, Fut>(
    socket: &Path,
    request: AttachRequest,
    initial_graph: bool,
    client: Client,
    graph: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let monitor = panel::Monitor::spawn(
        client,
        request.session_id.clone(),
        request.terminal_id.clone(),
    );
    run_inner(socket, request, initial_graph, Some(monitor), graph).await
}

async fn run_inner<F, Fut>(
    socket: &Path,
    request: AttachRequest,
    initial_graph: bool,
    mut monitor: Option<panel::Monitor>,
    mut graph: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(AppError::new(
            "terminal_required",
            "Attaching requires an interactive terminal",
        ));
    }
    let mut stream = UnixStream::connect(socket).await?;
    protocol::write_frame(&mut stream, &request).await?;
    let mut reader = BufReader::new(stream);
    let frame = tokio::time::timeout(Duration::from_secs(5), protocol::read_frame(&mut reader))
        .await
        .map_err(|_| {
            AppError::new(
                "handshake_timeout",
                "Terminal attachment handshake timed out",
            )
        })??
        .ok_or_else(|| AppError::new("disconnected", "Terminal closed before attachment"))?;
    match serde_json::from_slice::<ServerFrame>(&frame)? {
        ServerFrame::Attached { terminal_id } if terminal_id == request.terminal_id => {}
        ServerFrame::Error { error } => return Err(error),
        _ => {
            return Err(AppError::new(
                "protocol_error",
                "Invalid terminal attachment response",
            ));
        }
    }
    // Preserve any read-ahead snapshot bytes already buffered after Attached.
    let buffered = reader.buffer().to_vec();
    let (read, mut write) = reader.into_inner().into_split();
    let mut read = BufReader::new(read);
    let (screen_tx, mut screen_rx) = watch::channel::<Option<Snapshot>>(None);
    let (notice_tx, mut notice_rx) = mpsc::channel(8);
    let local_notices = notice_tx.clone();
    if initial_graph {
        notice_tx
            .try_send(Notice::Graph)
            .expect("new notice queue has capacity");
    }
    let expected = request.terminal_id.clone();
    let reader_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let prefix = std::io::Cursor::new(buffered);
        let mut frames = BufReader::new(prefix.chain(&mut read));
        let mut last_sequence = None;
        loop {
            let result = protocol::read_frame(&mut frames).await.and_then(|bytes| {
                bytes
                    .map(|bytes| {
                        serde_json::from_slice::<ServerFrame>(&bytes).map_err(AppError::from)
                    })
                    .transpose()
            });
            match result {
                Ok(Some(ServerFrame::Snapshot { snapshot })) => {
                    if snapshot.terminal_id != expected
                        || last_sequence.is_some_and(|last| snapshot.sequence < last)
                    {
                        let _ = notice_tx
                            .send(Notice::Error(AppError::new(
                                "protocol_error",
                                "Terminal snapshot identity or order changed",
                            )))
                            .await;
                        break;
                    }
                    last_sequence = Some(snapshot.sequence);
                    screen_tx.send_replace(Some(snapshot));
                }
                Ok(Some(ServerFrame::Graph)) => {
                    let _ = notice_tx.try_send(Notice::Graph);
                }
                Ok(Some(ServerFrame::Detach)) => {
                    let _ = notice_tx.send(Notice::Closed).await;
                    break;
                }
                Ok(Some(ServerFrame::Error { error })) => {
                    let _ = notice_tx.send(Notice::Error(error)).await;
                    break;
                }
                Ok(None) => {
                    let _ = notice_tx.send(Notice::Closed).await;
                    break;
                }
                Err(error) => {
                    let _ = notice_tx.send(Notice::Error(error)).await;
                    break;
                }
                _ => {
                    let _ = notice_tx
                        .send(Notice::Error(AppError::new(
                            "protocol_error",
                            "Unexpected terminal frame",
                        )))
                        .await;
                    break;
                }
            }
        }
    });
    let result = async {
        let mut guard = TerminalGuard::enter()?;
        let mut events = Some(EventStream::new());
        let mut input = InputState::default();
        let mut presentation = monitor.as_ref().map(|monitor| panel::Presentation::new(monitor.updates.borrow().clone(), request.cols, request.rows));
        let initial_dimensions = presentation.as_ref().map_or((request.rows, request.cols), panel::Presentation::dimensions);
        if initial_dimensions != (request.rows, request.cols) {
            send_resize(&mut write, &request.terminal_id, initial_dimensions).await?;
        }
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        loop {
            tokio::select! {
                _ = terminate.recv() => break,
                _ = hangup.recv() => break,
                changed = panel_changed(&mut monitor) => {
                    if changed {
                        let status = monitor.as_mut().expect("panel poller exists").updates.borrow_and_update().clone();
                        let view = presentation.as_mut().expect("panel presentation exists");
                        let previous = view.dimensions();
                        let mode_changed = view.update(status);
                        let dimensions = view.dimensions();
                        if previous != dimensions || mode_changed {
                            input = InputState::default();
                            view.invalidate();
                            send_resize(&mut write, &request.terminal_id, dimensions).await?;
                        }
                        if let Some(snapshot) = screen_rx.borrow().as_ref() { render_view(snapshot, &mut presentation)?; }
                    } else {
                        monitor = None;
                    }
                },
                notice = notice_rx.recv() => match notice {
                    Some(Notice::Graph) => {
                        if input.history {
                            protocol::write_frame(&mut write, &ClientFrame::History { terminal_id: request.terminal_id.clone(), action: HistoryAction::Exit }).await?;
                        }
                        input = InputState::default();
                        events.take();
                        guard.pause();
                        let outcome = {
                            let graph_future = graph();
                            tokio::pin!(graph_future);
                            loop {
                                tokio::select! {
                                    outcome = &mut graph_future => break Some(outcome),
                                    _ = terminate.recv() => break None,
                                    _ = hangup.recv() => break None,
                                    notice = notice_rx.recv() => match notice {
                                        Some(Notice::Graph) => {},
                                        Some(Notice::Error(error)) => return Err(error),
                                        Some(Notice::Closed) | None => break None,
                                    }
                                }
                            }
                        };
                        let Some(outcome) = outcome else { break; };
                        guard.resume()?;
                        events = Some(EventStream::new());
                        let (cols, rows) = terminal::size()?;
                        if let (Some(monitor), Some(view)) = (&mut monitor, &mut presentation) {
                            view.update(monitor.updates.borrow_and_update().clone());
                        }
                        let dimensions = resize_view(&mut presentation, cols, rows);
                        send_resize(&mut write, &request.terminal_id, dimensions).await?;
                        if let Some(snapshot) = screen_rx.borrow().as_ref() { render_view(snapshot, &mut presentation)?; }
                        outcome?;
                    },
                    Some(Notice::Error(error)) => return Err(error),
                    Some(Notice::Closed) | None => {
                        if let Some(snapshot) = screen_rx.borrow().as_ref() { render_view(snapshot, &mut presentation)?; }
                        break;
                    },
                },
                changed = screen_rx.changed() => {
                    if changed.is_err() { break; }
                    let snapshot = screen_rx.borrow_and_update().clone();
                    if let Some(snapshot) = snapshot { render_view(&snapshot, &mut presentation)?; }
                },
                event = events.as_mut().expect("input stream exists outside graph callback").next() => match event {
                    Some(Ok(Event::Resize(cols, rows))) => {
                        input = InputState::default();
                        let dimensions = resize_view(&mut presentation, cols, rows);
                        send_resize(&mut write, &request.terminal_id, dimensions).await?;
                    },
                    Some(Ok(event)) => {
                        let event = match &presentation {
                            Some(view) => match view.input(event) { Some(event) => event, None => continue },
                            None => event,
                        };
                        let snapshot = screen_rx.borrow().clone();
                        match input.handle(event, snapshot.as_ref()) {
                            InputAction::Ignore => {},
                            InputAction::Detach => break,
                            InputAction::Graph => { let _ = local_notices.try_send(Notice::Graph); },
                            InputAction::Input(bytes) => send_input(&mut write, &request.terminal_id, bytes).await?,
                            InputAction::History(action) => protocol::write_frame(&mut write, &ClientFrame::History { terminal_id: request.terminal_id.clone(), action }).await?,
                        }
                    },
                    Some(Err(error)) => return Err(error.into()),
                    None => break,
                }
            }
        }
        let _ = protocol::write_frame(&mut write, &ClientFrame::Detach { terminal_id: request.terminal_id }).await;
        Ok(())
    }.await;
    reader_task.abort();
    result
}

async fn panel_changed(monitor: &mut Option<panel::Monitor>) -> bool {
    match monitor {
        Some(monitor) => monitor.updates.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}

fn resize_view(presentation: &mut Option<panel::Presentation>, cols: u16, rows: u16) -> (u16, u16) {
    match presentation {
        Some(view) => {
            view.resize(cols, rows);
            view.dimensions()
        }
        None => (rows, cols),
    }
}

fn render_view(snapshot: &Snapshot, presentation: &mut Option<panel::Presentation>) -> Result<()> {
    match presentation {
        Some(view) => view.render(snapshot),
        None => render(snapshot),
    }
}

async fn send_resize(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    id: &str,
    (rows, cols): (u16, u16),
) -> Result<()> {
    protocol::write_frame(
        writer,
        &ClientFrame::Resize {
            terminal_id: id.into(),
            rows,
            cols,
        },
    )
    .await
}

async fn send_input(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    id: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    // Bytes, rather than strings, preserve UTF-8 and escape sequences across
    // chunk boundaries; the PTY consumes their original ordered byte stream.
    for chunk in bytes.chunks(4096) {
        protocol::write_frame(
            writer,
            &ClientFrame::Input {
                terminal_id: id.into(),
                bytes: chunk.to_vec(),
            },
        )
        .await?;
    }
    Ok(())
}

fn render(snapshot: &Snapshot) -> Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(b"\x1b[?2026h")?;
    stdout.write_all(snapshot.screen.as_bytes())?;
    if let Some(history) = &snapshot.history {
        // Like tmux's copy-mode indicator, this labels the frozen view while
        // leaving the complete underlying viewport available on return to live.
        let label = format!(
            " History {}/{} · q/Esc live ",
            history.offset, history.total
        );
        let label: String = label.chars().take(usize::from(snapshot.cols)).collect();
        let column = usize::from(snapshot.cols).saturating_sub(label.chars().count()) + 1;
        write!(stdout, "\x1b[1;{column}H\x1b[0;7m{label}\x1b[0m\x1b[?25l")?;
    }
    write_mouse_capture(&mut stdout, snapshot)?;
    stdout.write_all(b"\x1b[?2026l")?;
    stdout.flush()?;
    Ok(())
}

fn write_mouse_capture(writer: &mut impl Write, snapshot: &Snapshot) -> Result<()> {
    if snapshot.mouse || snapshot.history.is_some() {
        writer.write_all(b"\x1b[?1000h\x1b[?1002h\x1b[?1006h")?;
    } else {
        writer.write_all(b"\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l")?;
    }
    Ok(())
}

fn mouse_capture(snapshot: &Snapshot) -> Result<()> {
    let mut stdout = io::stdout().lock();
    write_mouse_capture(&mut stdout, snapshot)?;
    stdout.flush()?;
    Ok(())
}

fn encode(event: Event, snapshot: Option<&Snapshot>) -> Vec<u8> {
    match event {
        Event::Key(key) => encode_key(key, snapshot),
        Event::Paste(text) => {
            if snapshot.is_some_and(|s| s.bracketed_paste) {
                format!("\x1b[200~{text}\x1b[201~").into_bytes()
            } else {
                text.into_bytes()
            }
        }
        Event::Mouse(mouse) if snapshot.is_some_and(|s| s.mouse) => encode_mouse(mouse),
        _ => vec![],
    }
}

fn encode_key(key: KeyEvent, snapshot: Option<&Snapshot>) -> Vec<u8> {
    if key.kind == KeyEventKind::Release {
        return vec![];
    }
    let mut modifiers = 1;
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        modifiers += 1;
    }
    if key.modifiers.contains(KeyModifiers::ALT) {
        modifiers += 2;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        modifiers += 4;
    }
    if key.modifiers.contains(KeyModifiers::SUPER) {
        modifiers += 8;
    }
    let kitty = snapshot.is_some_and(|s| s.keyboard_flags != 0);
    let kitty_code = match key.code {
        KeyCode::Char(c) => Some(c as u32),
        KeyCode::Enter => Some(13),
        KeyCode::Tab | KeyCode::BackTab => Some(9),
        KeyCode::Backspace => Some(127),
        KeyCode::Esc => Some(27),
        _ => None,
    };
    let printable = matches!(key.code, KeyCode::Char(_))
        && !key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER);
    if kitty
        && !printable
        && let Some(code) = kitty_code
    {
        if key.code == KeyCode::BackTab {
            modifiers = 1 + ((modifiers - 1) | 1);
        }
        return format!("\x1b[{code};{modifiers}u").into_bytes();
    }
    let sequence = match key.code {
        KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let lower = c.to_ascii_lowercase();
            let code = match lower {
                'a'..='z' => lower as u8 - b'a' + 1,
                ' ' | '@' => 0,
                '[' => 27,
                '\\' => 28,
                ']' => 29,
                '^' => 30,
                '_' => 31,
                _ => return vec![],
            };
            return if key.modifiers.contains(KeyModifiers::ALT) {
                vec![27, code]
            } else {
                vec![code]
            };
        }
        KeyCode::Char(c) => c.to_string(),
        KeyCode::Enter => "\r".into(),
        KeyCode::Tab => "\t".into(),
        KeyCode::BackTab => "\x1b[Z".into(),
        KeyCode::Backspace => "\x7f".into(),
        KeyCode::Esc => "\x1b".into(),
        KeyCode::Up
        | KeyCode::Down
        | KeyCode::Right
        | KeyCode::Left
        | KeyCode::Home
        | KeyCode::End => {
            let suffix = match key.code {
                KeyCode::Up => 'A',
                KeyCode::Down => 'B',
                KeyCode::Right => 'C',
                KeyCode::Left => 'D',
                KeyCode::Home => 'H',
                _ => 'F',
            };
            if modifiers != 1 {
                format!("\x1b[1;{modifiers}{suffix}")
            } else if snapshot.is_some_and(|s| s.application_cursor) {
                format!("\x1bO{suffix}")
            } else {
                format!("\x1b[{suffix}")
            }
        }
        KeyCode::Insert | KeyCode::Delete | KeyCode::PageUp | KeyCode::PageDown => {
            let number = match key.code {
                KeyCode::Insert => 2,
                KeyCode::Delete => 3,
                KeyCode::PageUp => 5,
                _ => 6,
            };
            if modifiers == 1 {
                format!("\x1b[{number}~")
            } else {
                format!("\x1b[{number};{modifiers}~")
            }
        }
        KeyCode::F(number @ 1..=4) if modifiers == 1 => {
            format!("\x1bO{}", char::from(b'P' + number - 1))
        }
        KeyCode::F(number @ 1..=4) => {
            format!("\x1b[1;{modifiers}{}", char::from(b'P' + number - 1))
        }
        KeyCode::F(number @ 5..=12) => {
            let code = [15, 17, 18, 19, 20, 21, 23, 24][usize::from(number - 5)];
            if modifiers == 1 {
                format!("\x1b[{code}~")
            } else {
                format!("\x1b[{code};{modifiers}~")
            }
        }
        _ => return vec![],
    };
    if key.modifiers.contains(KeyModifiers::ALT) && !sequence.starts_with('\x1b') {
        format!("\x1b{sequence}").into_bytes()
    } else {
        sequence.into_bytes()
    }
}

fn encode_mouse(mouse: MouseEvent) -> Vec<u8> {
    let button = |button: MouseButton| match button {
        MouseButton::Left => 0,
        MouseButton::Middle => 1,
        MouseButton::Right => 2,
    };
    let (mut code, suffix) = match mouse.kind {
        MouseEventKind::Down(b) => (button(b), 'M'),
        MouseEventKind::Up(b) => (button(b), 'm'),
        MouseEventKind::Drag(b) => (button(b) + 32, 'M'),
        MouseEventKind::Moved => (35, 'M'),
        MouseEventKind::ScrollUp => (64, 'M'),
        MouseEventKind::ScrollDown => (65, 'M'),
        MouseEventKind::ScrollLeft => (66, 'M'),
        MouseEventKind::ScrollRight => (67, 'M'),
    };
    if mouse.modifiers.contains(KeyModifiers::SHIFT) {
        code += 4;
    }
    if mouse.modifiers.contains(KeyModifiers::ALT) {
        code += 8;
    }
    if mouse.modifiers.contains(KeyModifiers::CONTROL) {
        code += 16;
    }
    format!(
        "\x1b[<{code};{};{}{suffix}",
        mouse.column + 1,
        mouse.row + 1
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> Snapshot {
        Snapshot {
            terminal_id: "t".into(),
            sequence: 1,
            rows: 24,
            cols: 80,
            screen: String::new(),
            application_cursor: false,
            bracketed_paste: true,
            mouse: false,
            keyboard_flags: 7,
            exit_code: None,
            output_closed: false,
            fault: None,
            history: None,
        }
    }
    #[test]
    fn native_keyboard_paste_and_unicode_preserve_agent_input() {
        let state = snapshot();
        assert_eq!(
            encode(
                Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT)),
                Some(&state)
            ),
            b"\x1b[13;2u"
        );
        assert_eq!(
            encode(
                Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
                Some(&state)
            ),
            b"\x1b[99;5u"
        );
        assert_eq!(
            encode(
                Event::Key(KeyEvent::new(KeyCode::Char('你'), KeyModifiers::NONE)),
                Some(&state)
            ),
            "你".as_bytes()
        );
        assert_eq!(
            encode(Event::Paste("a\nb".into()), Some(&state)),
            b"\x1b[200~a\nb\x1b[201~"
        );
        assert_eq!(
            encode(
                Event::Key(KeyEvent::new(KeyCode::Up, KeyModifiers::CONTROL)),
                Some(&state)
            ),
            b"\x1b[1;5A"
        );
    }

    #[test]
    fn history_controls_consume_navigation_but_leave_live_agent_keys_alone() {
        let mut state = InputState::default();
        let snapshot = snapshot();
        let key = |code| Event::Key(KeyEvent::new(code, KeyModifiers::NONE));
        let prefix = || Event::Key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL));
        assert_eq!(
            state.handle(key(KeyCode::PageUp), Some(&snapshot)),
            InputAction::Input(b"\x1b[5~".to_vec())
        );
        assert_eq!(state.handle(prefix(), Some(&snapshot)), InputAction::Ignore);
        assert_eq!(
            state.handle(key(KeyCode::Char('[')), Some(&snapshot)),
            InputAction::History(HistoryAction::Enter)
        );
        assert_eq!(
            state.handle(key(KeyCode::PageUp), Some(&snapshot)),
            InputAction::History(HistoryAction::Move { rows: 23 })
        );
        assert_eq!(
            state.handle(key(KeyCode::Down), Some(&snapshot)),
            InputAction::History(HistoryAction::Move { rows: -1 })
        );
        assert_eq!(
            state.handle(key(KeyCode::Home), Some(&snapshot)),
            InputAction::History(HistoryAction::Oldest)
        );
        assert_eq!(
            state.handle(Event::Paste("must not reach Pi".into()), Some(&snapshot)),
            InputAction::Ignore
        );
        assert_eq!(
            state.handle(key(KeyCode::Enter), Some(&snapshot)),
            InputAction::Ignore
        );
        let mouse = Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 1,
            row: 1,
            modifiers: KeyModifiers::NONE,
        });
        assert_eq!(
            state.handle(mouse, Some(&snapshot)),
            InputAction::History(HistoryAction::Move { rows: 3 })
        );
        assert_eq!(
            state.handle(key(KeyCode::Esc), Some(&snapshot)),
            InputAction::History(HistoryAction::Exit)
        );
        assert_eq!(
            state.handle(key(KeyCode::Char('q')), Some(&snapshot)),
            InputAction::Input(b"q".to_vec())
        );
        assert_eq!(state.handle(prefix(), Some(&snapshot)), InputAction::Ignore);
        assert_eq!(
            state.handle(key(KeyCode::Char('x')), Some(&snapshot)),
            InputAction::Input(vec![2, b'x'])
        );
        assert_eq!(state.handle(prefix(), Some(&snapshot)), InputAction::Ignore);
        assert_eq!(
            state.handle(key(KeyCode::Char('g')), Some(&snapshot)),
            InputAction::Graph
        );
        for detach in ['d', 'D'] {
            assert_eq!(state.handle(prefix(), Some(&snapshot)), InputAction::Ignore);
            assert_eq!(
                state.handle(key(KeyCode::Char('[')), Some(&snapshot)),
                InputAction::History(HistoryAction::Enter)
            );
            assert_eq!(state.handle(prefix(), Some(&snapshot)), InputAction::Ignore);
            assert_eq!(
                state.handle(key(KeyCode::Char(detach)), Some(&snapshot)),
                InputAction::Detach
            );
        }
    }

    // Invoked only by the enclosing PTY regression. Running the normal suite
    // does not open an interactive terminal in the test runner.
    #[tokio::test]
    async fn client_fixture_entry() {
        let Ok(socket) = std::env::var("ONTOGRAPHY_TEST_TERMINAL_SOCKET") else {
            return;
        };
        let request: AttachRequest =
            serde_json::from_str(&std::env::var("ONTOGRAPHY_TEST_TERMINAL_REQUEST").unwrap())
                .unwrap();
        run(Path::new(&socket), request, || async { Ok(()) })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn burst_input_through_real_client_preserves_every_byte_once() {
        use crate::terminal::{LaunchSpec, Terminal, VERSION};
        use portable_pty::{CommandBuilder, PtySize, native_pty_system};
        use std::{
            collections::BTreeMap,
            io::Read,
            os::unix::fs::PermissionsExt,
            sync::{Arc, Mutex},
        };
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let spec = LaunchSpec {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                r"stty raw -echo; printf '\033[>7u'; dd bs=1 count=13 of=input 2>/dev/null".into(),
            ],
            env: BTreeMap::new(),
            cwd: directory.path().into(),
            rows: 24,
            cols: 80,
            server_id: "server".into(),
            session_id: "session".into(),
        };
        let terminal = Terminal::launch(spec, directory.path().join("terminal.sock"))
            .await
            .unwrap();
        let request = AttachRequest {
            version: VERSION,
            server_id: "server".into(),
            session_id: "session".into(),
            terminal_id: terminal.id().into(),
            rows: 24,
            cols: 80,
        };
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "terminal_client::tests::client_fixture_entry",
            "--nocapture",
        ]);
        command.env("ONTOGRAPHY_TEST_TERMINAL_SOCKET", terminal.status().socket);
        command.env(
            "ONTOGRAPHY_TEST_TERMINAL_REQUEST",
            serde_json::to_string(&request).unwrap(),
        );
        let mut child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let output_copy = output.clone();
        let reader_task = std::thread::spawn(move || {
            let mut bytes = [0u8; 4096];
            while let Ok(count) = reader.read(&mut bytes) {
                if count == 0 {
                    break;
                }
                let mut output = output_copy.lock().unwrap();
                if output.len() < 64 * 1024 {
                    output.extend_from_slice(&bytes[..count]);
                }
            }
        });
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !terminal.status().attached
                || terminal.snapshot().keyboard_flags != 7
                || !output
                    .lock()
                    .unwrap()
                    .windows(8)
                    .any(|bytes| bytes == b"\x1b[?1049h")
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if ready.is_err() {
            let _ = child.kill();
            let _ = child.wait();
            terminal.shutdown().await.unwrap();
            panic!(
                "client did not attach: {}",
                String::from_utf8_lossy(&output.lock().unwrap())
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut writer = pair.master.take_writer().unwrap();
        // Exercise real crossterm decoding and wire ordering as a single burst:
        // history navigation/paste must never appear in the child's input.
        writer
            .write_all(b"\x02[\x1b[5~\x1b[200~not input\x1b[201~q/graph\r")
            .unwrap();
        writer.flush().unwrap();
        let completion = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if completion.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        terminal.shutdown().await.unwrap();
        drop(writer);
        drop(pair.master);
        reader_task.join().unwrap();
        assert!(
            completion.is_ok(),
            "client did not finish: {}",
            String::from_utf8_lossy(&output.lock().unwrap())
        );
        assert_eq!(
            std::fs::read(directory.path().join("input")).unwrap(),
            b"/graph\x1b[13;1u"
        );
    }
}
