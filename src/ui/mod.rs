//! Rust terminal client. Pi owns conversation execution; the server owns graphs.

mod frontier;
pub mod graph;
pub mod pi_rpc;
mod session_graph;
pub use session_graph::run as run_graph_view;

use crate::{client::Client, error::AppError};
use crossterm::{
    event::{self, Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use graph::{GraphCanvas, GraphView};
use pi_rpc::{Conversation, PiEvent, PiRpc};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use serde_json::{Value, json};
use std::{
    io::{self, IsTerminal},
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{mpsc, watch},
};

pub struct UiOptions {
    /// Fully configured by the launcher, including `--mode rpc` and extension.
    pub pi_command: Command,
}

struct TerminalGuard;
impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        if let Err(error) = execute!(
            io::stdout(),
            EnterAlternateScreen,
            event::EnableBracketedPaste
        ) {
            let _ = terminal::disable_raw_mode();
            return Err(error);
        }
        Ok(Self)
    }
}
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            event::DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Focus {
    Prompt,
    Graph,
    Conversation,
}

struct View {
    server_id: String,
    graph: GraphView,
    snapshot: Value,
    runs: Vec<String>,
    run_id: Option<String>,
    selected: Option<String>,
    pan_x: i32,
    pan_y: i32,
    chat: Conversation,
    input: String,
    focus: Focus,
    status: String,
    chat_scroll: u16,
    dialog_choice: usize,
    dialog_saved_input: Option<String>,
    pi_alive: bool,
    preview: Option<RewritePreview>,
    show_preview: bool,
    node_pages: frontier::State,
}
struct RewritePreview {
    run_id: String,
    plan_id: String,
    base_revision: String,
    graph: GraphView,
    retirements: Value,
}
struct CallResult {
    operation: String,
    server_id: String,
    result: Result<Value, AppError>,
    selection: Option<frontier::Selection>,
}
impl Default for View {
    fn default() -> Self {
        Self {
            server_id: String::new(),
            graph: GraphView::default(),
            snapshot: Value::Null,
            runs: Vec::new(),
            run_id: None,
            selected: None,
            pan_x: 0,
            pan_y: 0,
            chat: Conversation::default(),
            input: String::new(),
            focus: Focus::Prompt,
            status: "Connecting to graph server…".into(),
            chat_scroll: 0,
            dialog_choice: 0,
            dialog_saved_input: None,
            pi_alive: true,
            preview: None,
            show_preview: false,
            node_pages: frontier::State::default(),
        }
    }
}

impl View {
    fn connected_to(&mut self, server_id: &str) {
        if self.server_id != server_id {
            self.server_id = server_id.into();
            self.preview = None;
            self.show_preview = false;
            self.selected = self.graph.selected_after_refresh(self.selected.as_deref());
            self.node_pages.sync(None);
        }
    }

    fn frontier_selection(&self) -> Option<frontier::Selection> {
        if self.active_preview().is_some() {
            return None;
        }
        let node_id = self.selected.clone()?;
        if !self.graph.nodes.iter().any(|node| node.id == node_id) {
            return None;
        }
        Some(frontier::Selection {
            server_id: self.server_id.clone(),
            run_id: self.run_id.clone()?,
            node_id,
        })
    }

    fn request_frontier(
        &mut self,
        client: &Client,
        tx: &mpsc::Sender<frontier::Response>,
        next: bool,
        open_history: bool,
    ) -> Result<(), AppError> {
        self.node_pages.sync(self.frontier_selection());
        if let Some(request) = self.node_pages.request(next, open_history)? {
            frontier::submit(client, tx, request);
        }
        Ok(())
    }

    fn remember_tool_preview(&mut self, details: &Value) {
        if details["receipt"]["server_id"]
            .as_str()
            .is_some_and(|id| id != self.server_id)
        {
            return;
        }
        self.remember_preview(details);
        self.remember_preview(&details["result"]);
    }

    fn active_preview(&self) -> Option<&RewritePreview> {
        self.preview.as_ref().filter(|preview| {
            self.show_preview && Some(preview.run_id.as_str()) == self.run_id.as_deref()
        })
    }

    fn visible_graph(&self) -> &GraphView {
        self.active_preview()
            .map_or(&self.graph, |preview| &preview.graph)
    }

    fn remember_preview(&mut self, value: &Value) {
        if let (Some(plan), Some(run)) = (value["plan_id"].as_str(), value["run_id"].as_str())
            && let Ok(graph) = GraphView::from_snapshot(value)
        {
            self.preview = Some(RewritePreview {
                run_id: run.into(),
                plan_id: plan.into(),
                base_revision: value["base_revision"]
                    .as_str()
                    .or_else(|| value["revision"].as_str())
                    .unwrap_or("unknown")
                    .into(),
                graph,
                retirements: value["retirements"].clone(),
            });
            self.show_preview = Some(run) == self.run_id.as_deref();
            self.selected = self
                .visible_graph()
                .selected_after_refresh(self.selected.as_deref());
        }
    }

    fn selected_detail(&self) -> Value {
        let node = self.selected.as_deref().unwrap_or("");
        if let Some(preview) = self.active_preview() {
            return json!({"preview_plan":preview.plan_id,"base_revision":preview.base_revision,"node_id":node,"proposed_edges":preview.graph.incident_edges(node),"retirements":preview.retirements});
        }
        let at_node = |value: &Value| {
            value
                .as_array()
                .into_iter()
                .flatten()
                .filter(|entry| entry["node_id"].as_str() == Some(node))
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut detail = json!({"run_id":self.run_id,"node_id":node,"revision":self.snapshot["revision"],"edges":self.graph.incident_edges(node),"executions":at_node(&self.snapshot["executions"])});
        detail.as_object_mut().expect("object").extend(
            self.node_pages
                .details(&self.snapshot["revision"])
                .as_object()
                .expect("object")
                .clone(),
        );
        detail
    }
}

struct Refresh {
    client: Option<Client>,
    runs: Vec<String>,
    run_id: Option<String>,
    snapshot: Result<Value, String>,
    more_runs: bool,
}

/// Run the unified graph/conversation frontend. Detaching only stops its Pi child.
pub async fn run(client: Client, options: UiOptions) -> Result<(), AppError> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(AppError::new(
            "terminal_required",
            "The graph UI needs an interactive terminal",
        ));
    }
    let (mut pi, mut pi_events) = PiRpc::spawn(options.pi_command)?;
    // Initialize protocol before enabling the terminal, and continuously drain it.
    pi.send(json!({"type":"get_state"})).await?;
    pi.send(json!({"type":"get_messages"})).await?;
    let result = run_terminal(client, &mut pi, &mut pi_events).await;
    pi.shutdown().await;
    result
}

async fn run_terminal(
    mut client: Client,
    pi: &mut PiRpc,
    pi_events: &mut mpsc::Receiver<PiEvent>,
) -> Result<(), AppError> {
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut input_events = EventStream::new();
    let (selected_tx, selected_rx) = watch::channel(None::<String>);
    let (refresh_tx, mut refresh_rx) = mpsc::channel(2);
    let (call_tx, mut call_rx) = mpsc::channel::<CallResult>(8);
    let (node_tx, mut node_rx) = mpsc::channel::<frontier::Response>(2);
    let poll_client = client.clone();
    let poller = tokio::spawn(refresh_loop(poll_client, selected_rx, refresh_tx));
    let mut view = View::default();
    view.connected_to(client.server_id());
    let mut render_tick = tokio::time::interval(Duration::from_millis(50));
    render_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let outcome:Result<(),AppError>=async {
        loop {
            tokio::select!{
                _=render_tick.tick()=>{terminal.draw(|frame|render(frame,&view))?;},
                _=terminate.recv()=>break,
                _=hangup.recv()=>break,
                event=input_events.next()=>match event {
                    Some(Ok(Event::Key(key))) if key.kind!=KeyEventKind::Release=>{
                        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code,KeyCode::Char('q')|KeyCode::Char('d')){break;}
                        let draft=view.input.clone();
                        if let Err(error)=handle_key(key,&mut view,pi,&client,&selected_tx,&call_tx,&node_tx).await{view.input=draft;view.chat.push("Error",error.to_string());}
                    },
                    Some(Ok(Event::Paste(text)))=>view.input.push_str(&text),
                    Some(Err(error))=>return Err(error.into()),
                    None=>break,
                    _=>{},
                },
                event=pi_events.recv(),if view.pi_alive=>match event {
                    Some(PiEvent::Record(record))=>{
                        if record["type"]=="tool_execution_end" {
                            view.remember_tool_preview(&record["result"]["details"]);
                        }
                        if record["type"]=="extension_ui_request" {
                            let opens_dialog=view.chat.dialog.is_none() && matches!(record["method"].as_str(),Some("select"|"confirm"|"input"|"editor"));
                            if opens_dialog {
                                view.dialog_choice=0;
                                if view.dialog_saved_input.is_none(){view.dialog_saved_input=Some(std::mem::take(&mut view.input));}
                                view.input.clear();
                            }
                            match record["method"].as_str().unwrap_or("") {
                                "editor" if opens_dialog=>view.input=record["prefill"].as_str().unwrap_or("").into(),
                                "input" if opens_dialog=>view.input=String::new(),
                                "set_editor_text"=>view.input=record["text"].as_str().unwrap_or("").into(),
                                _=>{},
                            }
                        }
                        if record["type"]=="response" && record["success"]==true && matches!(record["command"].as_str(),Some("new_session"|"switch_session"|"fork"|"clone")) && record["data"]["cancelled"]!=true {
                            pi.send(json!({"type":"get_messages"})).await?;
                            pi.send(json!({"type":"get_state"})).await?;
                        }
                        view.chat.apply(&record);
                    },
                    Some(PiEvent::Diagnostic(text))=>view.chat.push("Pi",text),
                    Some(PiEvent::Closed)|None=>{view.pi_alive=false;view.chat.busy=false;view.chat.push("Pi exited","Graph runs remain in the server. Ctrl-Q detaches; reopen to start Pi again.");},
                },
                response=node_rx.recv()=>if let Some(response)=response {
                    if selected_tx.borrow().as_ref().is_some_and(|selected|Some(selected)!=view.run_id.as_ref()){continue;}
                    view.node_pages.sync(view.frontier_selection());
                    if let Some(open_history)=view.node_pages.accept(response) {
                        if open_history && let Some(args)=view.node_pages.history_args() {
                            submit_call_scoped(&client,&call_tx,"inspect.package".into(),args,view.frontier_selection());
                        }else {
                            view.chat.push("Node package inspection",serde_json::to_string_pretty(&view.selected_detail())?);
                            view.focus=Focus::Conversation;
                        }
                    }
                },
                result=call_rx.recv()=>if let Some(CallResult{operation,server_id,result,selection})=result {
                    if selection.is_some() && (selection!=view.frontier_selection() || selected_tx.borrow().as_ref().is_some_and(|selected|Some(selected)!=view.run_id.as_ref())){continue;}
                    match result {
                        Ok(value)=>{
                            if server_id==view.server_id {
                                view.remember_preview(&value);
                                if operation=="rewrite.commit"{view.show_preview=false;}
                            }
                            view.chat.push(operation,serde_json::to_string_pretty(&value)?);
                        },
                        Err(error)=>view.chat.push(format!("{operation} failed"),error.to_string()),
                    }
                },
                update=refresh_rx.recv()=>if let Some(update)=update {
                    if let Some(connected)=update.client{view.connected_to(connected.server_id());client=connected;}
                    // A response for the previous selection may arrive after a key press.
                    // Preserve the requested run instead of resetting it to that response.
                    if selected_tx.borrow().as_ref().is_some_and(|selected|Some(selected)!=update.run_id.as_ref()){continue;}
                    view.runs=update.runs;
                    let changed=view.run_id!=update.run_id;
                    view.run_id=update.run_id;
                    if view.run_id.is_some() && *selected_tx.borrow()!=view.run_id {let _=selected_tx.send(view.run_id.clone());}
                    match update.snapshot {
                        Ok(snapshot)=>{
                            view.status=if update.more_runs{"Connected · first 100 runs listed; :run ID selects another".into()}else{"Connected · graph runs continue after detach".into()};
                            if snapshot.is_null(){view.graph=GraphView::default();view.snapshot=snapshot;}
                            else if snapshot.get("graph").is_none(){view.graph=GraphView::default();view.selected=None;view.status=format!("Run {} · resume to inspect live graph state",snapshot["status"].as_str().unwrap_or("unavailable"));view.snapshot=snapshot;}
                            else {match GraphView::from_snapshot(&snapshot){
                                Ok(graph)=>{view.graph=graph;view.snapshot=snapshot;view.selected=view.visible_graph().selected_after_refresh(if changed{None}else{view.selected.as_deref()});if changed{view.pan_x=0;view.pan_y=0;}},
                                Err(error)=>view.status=format!("Invalid graph snapshot: {error}"),
                            }}
                        },
                        Err(error)=>{view.status=format!("Snapshot unavailable: {error}");if changed{view.graph=GraphView::default();view.snapshot=Value::Null;}},
                    }
                },
            }
            let target_matches=selected_tx.borrow().as_ref().is_none_or(|selected|Some(selected)==view.run_id.as_ref());
            view.node_pages.sync(if target_matches {view.frontier_selection()}else{None});
        }
        Ok(())
    }.await;
    poller.abort();
    let _ = poller.await;
    outcome
}

async fn refresh_loop(
    mut client: Client,
    mut selected: watch::Receiver<Option<String>>,
    tx: mpsc::Sender<Refresh>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {_=ticker.tick()=>{},result=selected.changed()=>if result.is_err(){break;}}
        let target = selected.borrow().clone();
        let refresh = async {
            let list = match client.call("run.list", json!({})).await {
                Ok(value) => value,
                Err(error) if error.code == "server_restarted" => {
                    client = Client::connect(client.socket())
                        .await
                        .map_err(|error| error.to_string())?;
                    client
                        .call("run.list", json!({}))
                        .await
                        .map_err(|error| error.to_string())?
                }
                Err(error) => return Err(error.to_string()),
            };
            let runs: Vec<String> = list["runs"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|run| run["run_id"].as_str().map(str::to_owned))
                .collect();
            let run_id = target.or_else(|| runs.first().cloned());
            let more_runs = ["next", "next_after", "next_cursor"]
                .iter()
                .any(|key| list.get(key).is_some_and(|value| !value.is_null()));
            let snapshot = match &run_id {
                Some(id) => client
                    .call("run.inspect", json!({"run_id":id}))
                    .await
                    .map_err(|error| error.to_string()),
                None => Ok(Value::Null),
            };
            Ok::<_, String>(Refresh {
                client: Some(client.clone()),
                runs,
                run_id,
                snapshot,
                more_runs,
            })
        };
        let update = match tokio::time::timeout(Duration::from_secs(3), refresh).await {
            Ok(Ok(update)) => update,
            Ok(Err(error)) => Refresh {
                client: None,
                runs: vec![],
                run_id: selected.borrow().clone(),
                snapshot: Err(error),
                more_runs: false,
            },
            Err(_) => Refresh {
                client: None,
                runs: vec![],
                run_id: selected.borrow().clone(),
                snapshot: Err("Server request timed out; reconnecting".into()),
                more_runs: false,
            },
        };
        if tx.send(update).await.is_err() {
            break;
        }
    }
}

async fn handle_key(
    key: KeyEvent,
    view: &mut View,
    pi: &mut PiRpc,
    client: &Client,
    selected: &watch::Sender<Option<String>>,
    calls: &mpsc::Sender<CallResult>,
    nodes: &mpsc::Sender<frontier::Response>,
) -> Result<(), AppError> {
    if view.chat.dialog.is_some() {
        return handle_dialog(key, view, pi).await;
    }
    if key.code == KeyCode::Esc
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
    {
        pi.send(json!({"type":"clear_queue"})).await?;
        pi.send(json!({"type":"abort"})).await?;
        return Ok(());
    }
    if key.code == KeyCode::Tab {
        view.focus = match view.focus {
            Focus::Prompt => Focus::Graph,
            Focus::Graph => Focus::Conversation,
            Focus::Conversation => Focus::Prompt,
        };
        return Ok(());
    }
    match view.focus {
        Focus::Graph => match key.code {
            KeyCode::Up | KeyCode::Down if !key.modifiers.contains(KeyModifiers::SHIFT) => {
                view.selected = view.visible_graph().navigate(
                    view.selected.as_deref(),
                    if key.code == KeyCode::Up { -1 } else { 1 },
                );
                if let Some(id) = &view.selected {
                    view.pan_y = view.visible_graph().node_y(id).max(0);
                }
            }
            KeyCode::Up => view.pan_y = (view.pan_y - 3).max(0),
            KeyCode::Down => view.pan_y += 3,
            KeyCode::Left => view.pan_x = (view.pan_x - 4).max(0),
            KeyCode::Right => view.pan_x += 4,
            KeyCode::Char('p') => {
                view.show_preview = !view.show_preview;
                view.selected = view
                    .visible_graph()
                    .selected_after_refresh(view.selected.as_deref());
            }
            KeyCode::Char('o') if view.active_preview().is_none() => {
                view.node_pages.sync(view.frontier_selection());
                if !view.node_pages.loaded() {
                    view.request_frontier(client, nodes, false, true)?;
                } else if let Some(args) = view.node_pages.history_args() {
                    submit_call_scoped(
                        client,
                        calls,
                        "inspect.package".into(),
                        args,
                        view.frontier_selection(),
                    );
                } else {
                    view.chat.push(
                        "Node package inspection",
                        serde_json::to_string_pretty(&view.selected_detail())?,
                    );
                    view.focus = Focus::Conversation;
                }
            }
            KeyCode::Char('n') if view.active_preview().is_none() => {
                view.request_frontier(client, nodes, true, false)?
            }
            KeyCode::Char('r') if view.active_preview().is_none() => {
                view.request_frontier(client, nodes, false, false)?
            }
            KeyCode::Char('[') | KeyCode::Char(']') if !view.runs.is_empty() => {
                let index = view
                    .run_id
                    .as_ref()
                    .and_then(|id| view.runs.iter().position(|run| run == id))
                    .unwrap_or(0);
                let delta = if key.code == KeyCode::Char('[') {
                    -1
                } else {
                    1
                };
                let next = (index as isize + delta).rem_euclid(view.runs.len() as isize) as usize;
                let _ = selected.send(Some(view.runs[next].clone()));
            }
            KeyCode::Enter => {
                if view.active_preview().is_none() {
                    view.request_frontier(client, nodes, false, false)?;
                }
                let detail = view.selected_detail();
                view.chat
                    .push("Graph inspection", serde_json::to_string_pretty(&detail)?);
                view.focus = Focus::Conversation;
            }
            _ => {}
        },
        Focus::Conversation => match key.code {
            KeyCode::Up | KeyCode::PageUp => view.chat_scroll = view.chat_scroll.saturating_add(5),
            KeyCode::Down | KeyCode::PageDown => {
                view.chat_scroll = view.chat_scroll.saturating_sub(5)
            }
            KeyCode::End => view.chat_scroll = 0,
            _ => {}
        },
        Focus::Prompt => match key.code {
            KeyCode::Backspace => {
                view.input.pop();
            }
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => view.input.push('\n'),
            KeyCode::Enter => {
                let text = std::mem::take(&mut view.input);
                if text.trim().is_empty() {
                    return Ok(());
                }
                view.chat_scroll = 0;
                if text == "/new" {
                    pi.send(json!({"type":"new_session"})).await?;
                } else if let Some(path) = text.strip_prefix("/session ") {
                    pi.send(json!({"type":"switch_session","sessionPath":path.trim()}))
                        .await?;
                } else if text == "/clone" {
                    pi.send(json!({"type":"clone"})).await?;
                } else if let Some(entry) = text.strip_prefix("/fork ") {
                    pi.send(json!({"type":"fork","entryId":entry.trim()}))
                        .await?;
                } else if let Some(id) = text.strip_prefix(":run ") {
                    if id.trim().is_empty() {
                        return Err(AppError::invalid("Supply a run ID"));
                    }
                    let _ = selected.send(Some(id.trim().to_owned()));
                } else if let Some(call) = text.strip_prefix(":call ") {
                    let (operation, args) = call.split_once(' ').unwrap_or((call, "{}"));
                    let args: Value = serde_json::from_str(args)?;
                    // Explicit raw actions share the exact core binding used by Pi.
                    let operation = operation.to_owned();
                    view.chat
                        .push(&operation, "Requested; the server owns accepted work.");
                    submit_call(client, calls, operation, args);
                } else if let Some(package) = text.strip_prefix(":package ") {
                    let run = view.run_id.as_ref().ok_or_else(|| {
                        AppError::new(
                            "no_run_selected",
                            "Select a run before inspecting its package",
                        )
                    })?;
                    submit_call(
                        client,
                        calls,
                        "inspect.package".into(),
                        json!({"run_id":run,"package_id":package.trim()}),
                    );
                } else if text == "/help" {
                    view.chat.push("Controls","Tab changes pane. Graph: ↑/↓ select node; ←/→ pan; Shift+↑/↓ pan vertically; [/] select run; Enter loads node package pages; o cycles histories; n loads next pages; r refreshes first pages; p toggles rewrite preview. Esc stops Pi; Ctrl-Q detaches. /new, /session PATH, /clone, /fork ENTRY manage Pi sessions. :package PACKAGE_ID shows package history; :call OPERATION JSON invokes an Ontography binding.");
                } else {
                    if !view.pi_alive {
                        view.input = text;
                        return Err(AppError::new(
                            "pi_exited",
                            "Pi exited; reopen the client to continue",
                        ));
                    }
                    let selection = json!({"run_id":view.run_id,"node_id":view.selected,"observed_revision":view.snapshot["revision"]});
                    let text = if view.run_id.is_some() {
                        format!("Current Ontography UI selection: {selection}\n\n{text}")
                    } else {
                        text
                    };
                    let mut prompt = json!({"type":"prompt","message":text});
                    if view.chat.busy {
                        prompt["streamingBehavior"] = json!("steer");
                    }
                    pi.send(prompt).await?;
                }
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                view.input.push(c)
            }
            _ => {}
        },
    }
    Ok(())
}

fn submit_call(client: &Client, calls: &mpsc::Sender<CallResult>, operation: String, args: Value) {
    submit_call_scoped(client, calls, operation, args, None);
}

fn submit_call_scoped(
    client: &Client,
    calls: &mpsc::Sender<CallResult>,
    operation: String,
    args: Value,
    selection: Option<frontier::Selection>,
) {
    let client = client.clone();
    let calls = calls.clone();
    tokio::spawn(async move {
        let result = client.call(&operation, args).await;
        let _ = calls
            .send(CallResult {
                operation,
                server_id: client.server_id().into(),
                result,
                selection,
            })
            .await;
    });
}

async fn handle_dialog(key: KeyEvent, view: &mut View, pi: &mut PiRpc) -> Result<(), AppError> {
    let request = view.chat.dialog.as_ref().expect("dialog checked");
    let method = request["method"].as_str().unwrap_or("");
    let mut response = json!({"type":"extension_ui_response","id":request["id"]});
    match key.code {
        KeyCode::Esc => response["cancelled"] = json!(true),
        KeyCode::Up if method == "select" => {
            view.dialog_choice = view.dialog_choice.saturating_sub(1);
            return Ok(());
        }
        KeyCode::Down if method == "select" => {
            view.dialog_choice = (view.dialog_choice + 1).min(
                request["options"]
                    .as_array()
                    .map_or(0, |options| options.len().saturating_sub(1)),
            );
            return Ok(());
        }
        KeyCode::Enter if method == "editor" && key.modifiers.contains(KeyModifiers::SHIFT) => {
            view.input.push('\n');
            return Ok(());
        }
        KeyCode::Enter => match method {
            "select" => response["value"] = request["options"][view.dialog_choice].clone(),
            "confirm" => {
                response["confirmed"] = json!(
                    view.input.trim().eq_ignore_ascii_case("y")
                        || view.input.trim().eq_ignore_ascii_case("yes")
                )
            }
            _ => response["value"] = json!(std::mem::take(&mut view.input)),
        },
        KeyCode::Backspace => {
            view.input.pop();
            return Ok(());
        }
        KeyCode::Char(c) => {
            view.input.push(c);
            return Ok(());
        }
        _ => return Ok(()),
    }
    pi.send(response).await?;
    view.chat.dialog = view.chat.pending_dialogs.pop_front();
    view.dialog_choice = 0;
    view.input = match &view.chat.dialog {
        Some(next) => next["prefill"].as_str().unwrap_or("").to_owned(),
        None => view.dialog_saved_input.take().unwrap_or_default(),
    };
    Ok(())
}

fn render(frame: &mut Frame, view: &View) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(3),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let run_label = view
        .run_id
        .as_ref()
        .map(|id| id.chars().take(8).collect::<String>())
        .unwrap_or_else(|| "no run".into());
    let title = format!(
        " Ontography  ·  {}  ·  {}  ·  {} executions · revision {}",
        run_label,
        view.snapshot["admission"].as_str().unwrap_or("—"),
        view.snapshot["executions"].as_array().map_or(0, Vec::len),
        view.snapshot["revision"].as_str().unwrap_or("—")
    );
    frame.render_widget(
        Paragraph::new(title).style(Style::default().fg(Color::Cyan)),
        areas[0],
    );
    let panes = Layout::default()
        .direction(if frame.area().width >= 85 {
            Direction::Horizontal
        } else {
            Direction::Vertical
        })
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(areas[1]);
    let graph_title = if let Some(preview) = view.active_preview() {
        format!(
            " Preview {} · base {}{} · p returns ",
            preview.plan_id,
            preview.base_revision,
            if view.snapshot["revision"].as_str() == Some(preview.base_revision.as_str()) {
                ""
            } else {
                " · stale"
            }
        )
    } else {
        " Graph · ↑↓ / [ ] / p preview ".into()
    };
    let graph_block = panel(&graph_title, view.focus == Focus::Graph);
    let graph_inner = graph_block.inner(panes[0]);
    frame.render_widget(graph_block, panes[0]);
    let graph_areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(5.min(graph_inner.height / 3)),
        ])
        .split(graph_inner);
    frame.render_widget(
        GraphCanvas {
            graph: view.visible_graph(),
            selected: view.selected.as_deref(),
            pan_x: view.pan_x,
            pan_y: view.pan_y,
        },
        graph_areas[0],
    );
    let edges = view
        .selected
        .as_deref()
        .map(|id| view.visible_graph().incident_edges(id))
        .unwrap_or_default();
    frame.render_widget(
        Paragraph::new(edges.join("\n")).style(Style::default().fg(Color::Gray)),
        graph_areas[1],
    );
    let mut lines = Vec::new();
    for message in &view.chat.messages {
        lines.push(Line::from(Span::styled(
            format!("{}:", message.role),
            Style::default().fg(Color::Cyan),
        )));
        lines.extend(message.text.lines().map(|text| Line::from(text.to_owned())));
        lines.push(Line::default());
    }
    let chat_block = panel(
        if view.chat.busy {
            " Pi · working "
        } else {
            " Pi "
        },
        view.focus == Focus::Conversation,
    );
    let inner = chat_block.inner(panes[1]);
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let total = paragraph.line_count(inner.width).min(usize::from(u16::MAX)) as u16;
    let scroll = total
        .saturating_sub(inner.height)
        .saturating_sub(view.chat_scroll);
    frame.render_widget(paragraph.scroll((scroll, 0)).block(chat_block), panes[1]);
    frame.render_widget(
        Paragraph::new(view.input.as_str())
            .wrap(Wrap { trim: false })
            .block(panel(
                " Message · Enter send · /help ",
                view.focus == Focus::Prompt,
            )),
        areas[2],
    );
    frame.render_widget(
        Paragraph::new(format!("{} · Ctrl-Q detach", view.status))
            .style(Style::default().fg(Color::DarkGray)),
        areas[3],
    );
    if let Some(dialog) = &view.chat.dialog {
        let width = frame.area().width.saturating_sub(6).min(75);
        let height = frame.area().height.saturating_sub(4).min(15);
        let area = Rect::new(
            frame.area().x + (frame.area().width - width) / 2,
            frame.area().y + (frame.area().height - height) / 2,
            width,
            height,
        );
        let mut lines = vec![Line::from(
            dialog["message"].as_str().unwrap_or("").to_owned(),
        )];
        if let Some(options) = dialog["options"].as_array() {
            for (index, option) in options.iter().enumerate() {
                lines.push(Line::from(format!(
                    "{} {}",
                    if index == view.dialog_choice {
                        "›"
                    } else {
                        " "
                    },
                    option.as_str().unwrap_or("")
                )));
            }
        }
        if dialog["method"] == "confirm" {
            lines.push(Line::from(
                "Type yes to confirm; Enter submits, Esc cancels.",
            ));
        }
        lines.push(Line::from(view.input.clone()));
        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(panel(
                    dialog["title"].as_str().unwrap_or("Pi extension"),
                    true,
                )),
            area,
        );
    }
}

fn panel(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn unified_screen_renders_at_small_and_large_sizes() {
        for (width, height) in [(12, 6), (80, 24), (140, 45)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            let mut view = View::default();
            view.chat
                .push("Pi", "A graph run exists independently of this client.");
            terminal.draw(|frame| render(frame, &view)).unwrap();
        }
    }

    #[test]
    fn preview_never_replaces_current_graph_or_reports_future_frontier() {
        let mut view = View {
            run_id: Some("run-a".into()),
            snapshot: json!({"revision":"2","graph":{"nodes":[{"id":"old"}],"edges":[]},"frontier":{"received":[{"node_id":"old","package_id":"p1"},{"node_id":"other","package_id":"p2"}]}}),
            ..View::default()
        };
        view.graph = GraphView::from_snapshot(&view.snapshot).unwrap();
        view.selected = Some("old".into());
        assert!(view.selected_detail()["received"].is_null());
        view.remember_preview(&json!({"run_id":"run-a","plan_id":"plan-1","base_revision":"2","graph":{"nodes":[{"id":"new"}],"edges":[]},"retirements":[{"package_id":"p1"}]}));
        assert_eq!(view.visible_graph().nodes[0].id, "new");
        assert_eq!(view.graph.nodes[0].id, "old");
        assert!(view.selected_detail().get("received").is_none());
        view.show_preview = false;
        assert_eq!(view.visible_graph().nodes[0].id, "old");
    }

    #[test]
    fn server_restart_expires_preview_and_rejects_late_tool_results() {
        let mut view = View {
            run_id: Some("run-a".into()),
            ..View::default()
        };
        view.graph =
            GraphView::from_snapshot(&json!({"graph":{"nodes":[{"id":"live"}],"edges":[]}}))
                .unwrap();
        view.connected_to("server-1");
        let details = json!({"receipt":{"server_id":"server-1"},"result":{"run_id":"run-a","plan_id":"p1","base_revision":"2","graph":{"nodes":[{"id":"future"}],"edges":[]}}});
        view.remember_tool_preview(&details);
        view.connected_to("server-1");
        assert!(view.preview.is_some());
        assert_eq!(view.selected.as_deref(), Some("future"));
        view.connected_to("server-2");
        assert!(view.preview.is_none());
        assert!(!view.show_preview);
        assert_eq!(view.selected.as_deref(), Some("live"));
        view.remember_tool_preview(&details);
        assert!(view.preview.is_none());
    }

    #[test]
    fn inactive_preview_flags_do_not_block_live_node_package_queries() {
        let mut view = View {
            run_id: Some("run-a".into()),
            selected: Some("live".into()),
            show_preview: true,
            ..View::default()
        };
        view.graph =
            GraphView::from_snapshot(&json!({"graph":{"nodes":[{"id":"live"}],"edges":[]}}))
                .unwrap();
        assert!(view.frontier_selection().is_some());
        view.remember_preview(&json!({"run_id":"another-run","plan_id":"plan","base_revision":"1","graph":{"nodes":[{"id":"future"}],"edges":[]}}));
        view.show_preview = true;
        assert!(view.frontier_selection().is_some());
    }
}
