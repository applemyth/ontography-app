//! The graph view for an existing native Pi session. This view owns no agent.
use super::{
    TerminalGuard,
    graph::{GraphCanvas, GraphView},
};
use crate::{AppError, Result, client::Client};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind};
use futures_util::StreamExt;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::sync::watch;

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub async fn run(client: Client, session_id: String) -> Result<()> {
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut events = EventStream::new();
    let (updates, mut latest) = watch::channel(None::<std::result::Result<Value, String>>);
    let _poller = AbortOnDrop(tokio::spawn(async move {
        loop {
            let result = match tokio::time::timeout(
                Duration::from_secs(5),
                client.call("session.context", json!({"session_id":session_id})),
            )
            .await
            {
                Ok(result) => result.map_err(|e| e.to_string()),
                Err(_) => Err("Graph inspection timed out; reconnecting…".into()),
            };
            if updates.send(Some(result)).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }));
    let mut graph = GraphView::default();
    let mut name = String::from("Ontography");
    let mut message = String::from("Loading session graph…");
    let mut selected = None;
    let mut pan_x = 0;
    async {
        loop {
            terminal.draw(|frame| {
                let areas = Layout::vertical([Constraint::Length(2), Constraint::Min(3), Constraint::Length(5), Constraint::Length(1)]).split(frame.area());
                frame.render_widget(Paragraph::new(format!("{name} · Graph\n{message}")).style(Style::default().fg(Color::Cyan)), areas[0]);
                let block = Block::default().borders(Borders::ALL);
                let canvas = block.inner(areas[1]);
                frame.render_widget(block, areas[1]);
                let pan_y = selected.as_deref().map(|id| (graph.node_y(id) - i32::from(canvas.height) / 2).max(0)).unwrap_or(0);
                frame.render_widget(GraphCanvas { graph: &graph, selected: selected.as_deref(), pan_x, pan_y }, canvas);
                let detail = if let Some(node) = graph.nodes.iter().find(|node| Some(node.id.as_str()) == selected.as_deref()) {
                    format!("Node {} · received {} · outbound {}\n{}", node.id, node.received, node.outbound, graph.incident_edges(&node.id).join("\n"))
                } else { "Pi remains running. Create and start the graph through its management tools.".into() };
                frame.render_widget(Paragraph::new(detail).wrap(Wrap { trim: false }), areas[2]);
                frame.render_widget(Paragraph::new("↑/↓ select · ←/→ pan · q/Esc return to terminal"), areas[3]);
            })?;
            tokio::select! {
                changed = latest.changed() => {
                    if changed.is_err() { break; }
                    let update = latest.borrow_and_update().clone();
                    match update {
                        Some(Ok(context)) => {
                            name = context["session"]["name"].as_str().unwrap_or("Ontography").into();
                            let snapshot = &context["graph"];
                            if snapshot.is_null() {
                                graph = GraphView::default(); selected = None;
                                message = "Awaiting graph initialization".into();
                            } else if snapshot["graph"].is_object() {
                                graph = GraphView::from_snapshot(snapshot).map_err(AppError::from)?;
                                selected = graph.selected_after_refresh(selected.as_deref());
                                message = format!("Run {} · revision {} · {} nodes · {} edges", snapshot["run_id"].as_str().unwrap_or("?"), snapshot["revision"].as_str().unwrap_or("?"), graph.nodes.len(), graph.edges.len());
                            } else {
                                graph = GraphView::default(); selected = None;
                                message = format!("Graph is {}", snapshot["status"].as_str().unwrap_or("unavailable"));
                            }
                        }
                        Some(Err(error)) => message = error,
                        None => {},
                    }
                }
                event = events.next() => match event {
                    Some(Ok(Event::Key(key))) if key.kind != KeyEventKind::Release => match key.code {
                        KeyCode::Esc | KeyCode::Char('q') => break,
                        KeyCode::Up => selected = graph.navigate(selected.as_deref(), -1),
                        KeyCode::Down | KeyCode::Tab => selected = graph.navigate(selected.as_deref(), 1),
                        KeyCode::Left => pan_x = (pan_x - 8).max(0),
                        KeyCode::Right => pan_x += 8,
                        _ => {},
                    },
                    Some(Err(error)) => return Err(error.into()),
                    None => break,
                    _ => {},
                },
            }
        }
        Ok(())
    }.await
}
