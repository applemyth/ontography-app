//! Attachment-only session status and a clipped view of the persistent shell.
use crate::{Result, client::Client, terminal::Snapshot};
use crossterm::event::{Event, MouseEvent};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Paragraph, Widget, Wrap},
};
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ManagerMode {
    #[default]
    Starting,
    Pi,
    Shell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Status {
    session_id: String,
    name: String,
    session_state: String,
    mode: ManagerMode,
    manager_available: bool,
    manager_error: Option<String>,
    graph: String,
    agents: String,
    received: String,
    outbound: String,
    context_error: Option<String>,
}

impl Status {
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            name: "Session".into(),
            session_state: "loading".into(),
            mode: ManagerMode::Starting,
            manager_available: false,
            manager_error: None,
            graph: "Loading".into(),
            agents: "Unknown".into(),
            received: "?".into(),
            outbound: "?".into(),
            context_error: None,
        }
    }

    fn manager(&mut self, result: std::result::Result<Value, String>, terminal_id: &str) {
        match result {
            Ok(value) if value["terminal_id"] == terminal_id => {
                let mode = match value["manager_mode"].as_str() {
                    Some("pi") => ManagerMode::Pi,
                    Some("shell") => ManagerMode::Shell,
                    Some("starting") => ManagerMode::Starting,
                    _ => {
                        self.manager_available = false;
                        self.manager_error = Some("Manager status unavailable".into());
                        return;
                    }
                };
                self.mode = mode;
                self.manager_available = true;
                self.manager_error = value["manager_error"].as_str().map(str::to_owned);
            }
            Ok(_) => {
                self.manager_available = false;
                self.manager_error = Some("Terminal replaced; reconnect".into());
            }
            Err(_) => {
                self.manager_available = false;
                self.manager_error = Some("Manager status unavailable".into());
            }
        }
    }

    fn context(&mut self, result: std::result::Result<Value, String>) {
        let value = match result {
            Ok(value) if value["session"]["session_id"] == self.session_id => value,
            _ => {
                self.context_error = Some("Session status unavailable; retrying".into());
                return;
            }
        };
        self.context_error = None;
        self.name = value["session"]["name"]
            .as_str()
            .unwrap_or("Session")
            .into();
        self.session_state = match value["session"]["status"].as_str() {
            Some("suspended") => "inactive",
            Some(status) => status,
            None => "unknown",
        }
        .into();
        let graph = &value["graph"];
        if graph.is_null() {
            self.graph = "No graph".into();
            self.agents = "No agents".into();
            self.received = "0".into();
            self.outbound = "0".into();
            return;
        }
        self.graph = match graph["admission"].as_str() {
            Some("faulted") => "Faulted",
            Some("closed") => "Closed",
            _ => match graph["status"].as_str() {
                Some("active") => "Active",
                Some("suspended") => "Inactive",
                Some("closed") => "Closed",
                Some("recoverable") => "Awaiting resume",
                Some("suspension_failed") => "Suspend failed",
                _ => "Unavailable",
            },
        }
        .into();
        self.agents = match graph["executions"].as_array() {
            Some(executions) if !executions.is_empty() => format!(
                "{} running / {} total",
                executions
                    .iter()
                    .filter(|entry| entry["status"] == "running")
                    .count(),
                executions.len()
            ),
            Some(_) => "No agents".into(),
            None if matches!(
                graph["status"].as_str(),
                Some("suspended" | "closed" | "recoverable")
            ) =>
            {
                "No agents running".into()
            }
            None => "Unavailable".into(),
        };
        self.received = pending_total(graph, "received");
        self.outbound = pending_total(graph, "outbound");
    }

    fn lines(&self, compact: bool) -> Vec<Line<'static>> {
        let short = self.session_id.chars().take(8).collect::<String>();
        let manager = if !self.manager_available {
            "unknown"
        } else {
            match self.mode {
                ManagerMode::Pi => "running",
                ManagerMode::Shell => "stopped",
                ManagerMode::Starting => "starting",
            }
        };
        let (session, graph, agents, received, outbound) = if self.context_error.is_some() {
            ("unavailable", "Unavailable", "Unavailable", "?", "?")
        } else {
            (
                self.session_state.as_str(),
                self.graph.as_str(),
                self.agents.as_str(),
                self.received.as_str(),
                self.outbound.as_str(),
            )
        };
        let mut lines = if compact {
            vec![
                Line::from(format!(
                    "{} [{short}] · {session} · Pi {manager}",
                    self.name
                )),
                Line::from(format!("Graph: {graph} · Agents: {agents}")),
                Line::from(format!(
                    "Pending: {received} received / {outbound} outbound"
                )),
                Line::from("pi: manager · Ctrl-B g: graph · Ctrl-B d: detach"),
            ]
        } else {
            vec![
                Line::from(self.name.clone()).style(Style::default().add_modifier(Modifier::BOLD)),
                Line::from(self.session_id.clone()),
                Line::from(format!("Session: {session}")),
                Line::from(format!("Pi manager: {manager}")),
                Line::from(""),
                Line::from(format!("Graph: {graph}")),
                Line::from(format!("Agents: {agents}")),
                Line::from(format!("Received: {received}")),
                Line::from(format!("Outbound: {outbound}")),
                Line::from(""),
                Line::from("pi          Open manager"),
                Line::from("Ctrl-B g    Graph"),
                Line::from("Ctrl-B [    History"),
                Line::from("Ctrl-B d    Detach"),
            ]
        };
        if let Some(error) = self.manager_error.as_ref().or(self.context_error.as_ref()) {
            let line = Line::from(error.clone()).style(Style::default().fg(Color::Yellow));
            if compact {
                lines.push(line);
            } else {
                lines[4] = line;
            }
        }
        lines
    }
}

fn pending_total(graph: &Value, phase: &str) -> String {
    graph["frontier"]["counts"]
        .as_object()
        .and_then(|counts| {
            counts.values().try_fold(0u128, |sum, entry| {
                let value = entry[phase].as_u64().map(u128::from).or_else(|| {
                    entry[phase]
                        .as_str()
                        .and_then(|value| value.parse::<u128>().ok())
                })?;
                sum.checked_add(value)
            })
        })
        .map_or_else(|| "?".into(), |count| count.to_string())
}

pub(super) struct Monitor {
    pub updates: watch::Receiver<Status>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Monitor {
    pub fn spawn(client: Client, session_id: String, terminal_id: String) -> Self {
        let (updates, receiver) = watch::channel(Status::new(session_id.clone()));
        let mut tasks = Vec::new();
        // Independent requests keep a blocked graph observation from delaying
        // mode changes. Neither poller is awaited by terminal input or detach.
        for manager in [true, false] {
            let client = client.clone();
            let session_id = session_id.clone();
            let terminal_id = terminal_id.clone();
            let updates = updates.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let operation = if manager {
                        "terminal.status"
                    } else {
                        "session.context"
                    };
                    let request = client.call(operation, json!({"session_id":session_id}));
                    tokio::pin!(request);
                    let result = tokio::select! {
                        result = &mut request => result,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {
                            publish(&updates, manager, &terminal_id, Err("Status request timed out".into()));
                            // A timed-out connection does not revoke server work.
                            // Keep this single observation rather than enqueueing
                            // more reads behind the same busy graph lock.
                            request.await
                        }
                    };
                    publish(&updates, manager, &terminal_id, result.map_err(|error| error.to_string()));
                    tokio::time::sleep(Duration::from_millis(if manager { 300 } else { 750 }))
                        .await;
                }
            }));
        }
        Self {
            updates: receiver,
            tasks,
        }
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn publish(
    updates: &watch::Sender<Status>,
    manager: bool,
    terminal_id: &str,
    result: std::result::Result<Value, String>,
) {
    updates.send_if_modified(|current| {
        let previous = current.clone();
        if manager {
            current.manager(result, terminal_id);
        } else {
            current.context(result);
        }
        *current != previous
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Split {
    shell: Rect,
    panel: Rect,
    compact: bool,
}

impl Split {
    fn new(cols: u16, rows: u16) -> Self {
        if cols >= 110 && rows >= 12 {
            let width = 38;
            Self {
                shell: Rect::new(0, 0, cols - width, rows),
                panel: Rect::new(cols - width, 0, width, rows),
                compact: false,
            }
        } else {
            let height = 6.min(rows.saturating_sub(2));
            Self {
                shell: Rect::new(0, 0, cols, rows - height),
                panel: Rect::new(0, rows - height, cols, height),
                compact: true,
            }
        }
    }
}

pub(super) struct Presentation {
    status: Status,
    cols: u16,
    rows: u16,
    terminal: Option<Terminal<CrosstermBackend<io::Stdout>>>,
    clear: bool,
}

impl Presentation {
    pub fn new(status: Status, cols: u16, rows: u16) -> Self {
        Self {
            status,
            cols,
            rows,
            terminal: None,
            clear: true,
        }
    }

    pub fn update(&mut self, status: Status) -> bool {
        let changed = self.status.mode != status.mode;
        self.status = status;
        if changed {
            self.invalidate();
        }
        changed
    }

    pub fn resize(&mut self, cols: u16, rows: u16) {
        self.cols = cols;
        self.rows = rows;
        self.invalidate();
    }

    pub fn invalidate(&mut self) {
        self.clear = true;
    }

    fn shell_area(&self) -> Rect {
        if self.status.mode == ManagerMode::Shell {
            Split::new(self.cols, self.rows).shell
        } else {
            Rect::new(0, 0, self.cols, self.rows)
        }
    }

    pub fn dimensions(&self) -> (u16, u16) {
        let area = self.shell_area();
        let cols = area.width.clamp(10, 500);
        let rows = area.height.clamp(2, 200).min(30_000 / cols);
        (rows, cols)
    }

    pub fn input(&self, event: Event) -> Option<Event> {
        if let Event::Mouse(MouseEvent { column, row, .. }) = event {
            let shell = self.shell_area();
            if column >= shell.width || row >= shell.height {
                return None;
            }
        }
        Some(event)
    }

    pub fn render(&mut self, snapshot: &Snapshot) -> Result<()> {
        if self.status.mode != ManagerMode::Shell {
            if self.terminal.take().is_some() || self.clear {
                use std::io::Write;
                io::stdout().write_all(b"\x1b[0m\x1b[2J\x1b[H")?;
            }
            self.clear = false;
            return super::render(snapshot);
        }
        if self.terminal.is_none() || self.clear {
            // Terminal::clear() synchronously reads a cursor-position reply,
            // racing our EventStream. A new buffer plus explicit full clear
            // invalidates the renderer without consuming terminal input.
            use std::io::Write;
            self.terminal.take();
            io::stdout().write_all(b"\x1b[0m\x1b[2J\x1b[H")?;
            self.terminal = Some(Terminal::new(CrosstermBackend::new(io::stdout()))?);
            self.clear = false;
        }
        let terminal = self.terminal.as_mut().expect("created shell renderer");
        let mut parser = vt100::Parser::new(snapshot.rows, snapshot.cols, 0);
        parser.process(snapshot.screen.as_bytes());
        terminal.draw(|frame| {
            let area = frame.area();
            let split = Split::new(area.width, area.height);
            frame.render_widget(Screen(parser.screen()), split.shell);
            let borders = if split.compact {
                Borders::TOP
            } else {
                Borders::LEFT
            };
            let block = Block::default()
                .borders(borders)
                .title(" Ontography session ")
                .border_style(Style::default().fg(Color::Cyan));
            let panel = block.inner(split.panel);
            frame.render_widget(block, split.panel);
            frame.render_widget(
                Paragraph::new(self.status.lines(split.compact)).wrap(Wrap { trim: false }),
                panel,
            );
            let (row, col) = parser.screen().cursor_position();
            if let Some(history) = &snapshot.history {
                frame.render_widget(
                    Paragraph::new(format!(
                        " History {}/{} · q/Esc live ",
                        history.offset, history.total
                    ))
                    .style(Style::default().add_modifier(Modifier::REVERSED)),
                    Rect::new(0, 0, split.shell.width, 1.min(split.shell.height)),
                );
            } else if !parser.screen().hide_cursor()
                && row < split.shell.height
                && col < split.shell.width
            {
                frame.set_cursor_position((col, row));
            }
        })?;
        super::mouse_capture(snapshot)?;
        Ok(())
    }
}

struct Screen<'a>(&'a vt100::Screen);
impl Widget for Screen<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let (rows, cols) = self.0.size();
        for row in 0..area.height.min(rows) {
            for col in 0..area.width.min(cols) {
                let Some(cell) = self.0.cell(row, col) else {
                    continue;
                };
                if cell.is_wide_continuation() || (cell.is_wide() && col + 1 >= area.width) {
                    continue;
                }
                let mut style = Style::default()
                    .fg(color(cell.fgcolor()))
                    .bg(color(cell.bgcolor()));
                for (enabled, modifier) in [
                    (cell.bold(), Modifier::BOLD),
                    (cell.dim(), Modifier::DIM),
                    (cell.italic(), Modifier::ITALIC),
                    (cell.underline(), Modifier::UNDERLINED),
                    (cell.inverse(), Modifier::REVERSED),
                ] {
                    if enabled {
                        style = style.add_modifier(modifier);
                    }
                }
                buffer[(area.x + col, area.y + row)]
                    .set_symbol(if cell.contents().is_empty() {
                        " "
                    } else {
                        cell.contents()
                    })
                    .set_style(style);
            }
        }
    }
}

fn color(color: vt100::Color) -> Color {
    match color {
        vt100::Color::Default => Color::Reset,
        vt100::Color::Idx(index) => Color::Indexed(index),
        vt100::Color::Rgb(r, g, b) => Color::Rgb(r, g, b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stalled_context_is_unavailable_without_delaying_mode_or_accumulating_requests() {
        use crate::protocol::{self, Request, Response};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tokio::{io::BufReader, net::UnixListener, task::JoinSet};
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("status.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let observations = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        let count = observations.clone();
        let closed = released.clone();
        let server = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let count = count.clone();
                        let closed = closed.clone();
                        clients.spawn(async move {
                            let mut reader = BufReader::new(stream);
                            let request: Request = serde_json::from_slice(&protocol::read_frame(&mut reader).await.unwrap().unwrap()).unwrap();
                            if request.operation == "session.context" {
                                count.fetch_add(1, Ordering::SeqCst);
                                assert!(protocol::read_frame(&mut reader).await.unwrap().is_none());
                                closed.store(true, Ordering::SeqCst);
                                return;
                            }
                            let result = if request.operation == "system.hello" {
                                json!({"protocol_version":protocol::VERSION,"server_id":"server","app_version":env!("CARGO_PKG_VERSION"),"core_version":ontography::VERSION,"app_build":crate::APP_BUILD,"core_build":crate::CORE_BUILD})
                            } else {
                                json!({"terminal_id":"terminal","manager_mode":"shell","manager_pid":null})
                            };
                            protocol::write_frame(reader.get_mut(), &Response::new("server", &request.request_id, Ok(result))).await.unwrap();
                        });
                    }
                    _ = clients.join_next(), if !clients.is_empty() => {},
                }
            }
        });
        let client = Client::connect(&socket).await.unwrap();
        let mut monitor = Monitor::spawn(client, "session".into(), "terminal".into());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                monitor.updates.changed().await.unwrap();
                let status = monitor.updates.borrow_and_update().clone();
                if status.mode == ManagerMode::Shell && status.context_error.is_some() {
                    break;
                }
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            observations.load(Ordering::SeqCst),
            1,
            "busy graph must have only one outstanding observation"
        );
        drop(monitor);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !released.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[test]
    fn explicit_mode_and_context_distinguish_idle_graphs_agents_and_unavailable_observations() {
        let mut status = Status::new("session".into());
        status.manager(
            Ok(json!({"terminal_id":"terminal","manager_mode":"shell","manager_pid":null})),
            "terminal",
        );
        status.context(Ok(json!({"session":{"session_id":"session","name":"My session","status":"active"},"graph":null})));
        assert_eq!(status.mode, ManagerMode::Shell);
        assert_eq!(status.graph, "No graph");
        assert_eq!(status.agents, "No agents");
        status.context(Ok(json!({"session":{"session_id":"session","status":"active"},"graph":{"status":"active","admission":"open","executions":[],"frontier":{"counts":{"a":{"received":"18446744073709551615","outbound":0},"b":{"received":1,"outbound":2}}}}})));
        assert_eq!(status.graph, "Active");
        assert_eq!(status.agents, "No agents");
        assert_eq!(status.received, "18446744073709551616");
        assert_eq!(status.outbound, "2");
        status.context(Err("busy".into()));
        let text = status
            .lines(false)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Graph: Unavailable"));
        assert!(!text.contains("Graph: Active"));
        status.manager(
            Ok(json!({"terminal_id":"terminal","manager_mode":"shell","manager_error":"Pi launch failed: executable missing"})),
            "terminal",
        );
        let text = status
            .lines(false)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("Pi manager: stopped"));
        assert!(text.contains("Pi launch failed: executable missing"));
        status.manager(
            Ok(json!({"terminal_id":"terminal","manager_mode":"shell"})),
            "terminal",
        );
        assert!(status.manager_error.is_none());
        status.manager(
            Ok(json!({"terminal_id":"different","manager_mode":"pi"})),
            "terminal",
        );
        assert_eq!(status.mode, ManagerMode::Shell);
        assert!(status.manager_error.is_some());
    }

    #[test]
    fn shell_layout_tracks_mode_and_resize_without_accepting_panel_mouse_input() {
        let mut status = Status::new("s".into());
        let mut view = Presentation::new(status.clone(), 120, 35);
        assert_eq!(view.dimensions(), (35, 120));
        status.mode = ManagerMode::Shell;
        view.update(status.clone());
        assert_eq!(view.dimensions(), (35, 82));
        let mouse = |column, row| {
            Event::Mouse(MouseEvent {
                kind: crossterm::event::MouseEventKind::ScrollUp,
                column,
                row,
                modifiers: crossterm::event::KeyModifiers::NONE,
            })
        };
        assert!(view.input(mouse(81, 0)).is_some());
        assert!(view.input(mouse(82, 0)).is_none());
        view.resize(80, 24);
        assert_eq!(view.dimensions(), (18, 80));
        assert!(view.input(mouse(0, 18)).is_none());
        status.mode = ManagerMode::Pi;
        view.update(status);
        assert_eq!(view.dimensions(), (24, 80));
        for (cols, rows) in [(1, 1), (10, 2), (500, 200), (1000, 1000)] {
            view.resize(cols, rows);
            let (rows, cols) = view.dimensions();
            crate::terminal::validate_size(rows, cols).unwrap();
        }
    }

    #[test]
    fn shell_cells_clip_escape_output_and_preserve_unicode_and_style() {
        let mut parser = vt100::Parser::new(5, 20, 0);
        parser.process("\x1b[2J\x1b[H\x1b[1;38;2;12;34;56m你e\u{301}\x1b[5;20H!".as_bytes());
        let mut buffer = Buffer::empty(Rect::new(0, 0, 20, 5));
        buffer[(8, 0)].set_symbol("P");
        Screen(parser.screen()).render(Rect::new(0, 0, 8, 4), &mut buffer);
        assert_eq!(buffer[(0, 0)].symbol(), "你");
        assert_eq!(buffer[(2, 0)].symbol(), "e\u{301}");
        assert_eq!(buffer[(0, 0)].fg, Color::Rgb(12, 34, 56));
        assert!(buffer[(0, 0)].modifier.contains(Modifier::BOLD));
        assert_eq!(buffer[(8, 0)].symbol(), "P");
        assert_eq!(buffer[(19, 4)].symbol(), " ");
    }
}
