use clap::{Args, Parser, Subcommand};
use ontography_app::{
    AppError, Result,
    client::Client,
    declarations::parse_json,
    launcher, migration,
    persistence::{self, Paths},
    server, terminal, terminal_client, ui,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Parser)]
#[command(
    name = "ontography",
    version,
    about = "Persistent agent sessions and Ontography graphs",
    after_help = "With no command, create a new session in Pi. Use `attach NAME_OR_ID` to return to an existing session.\nCtrl-B D detaches; Pi /quit opens the session shell; shell exit suspends the session; `close NAME_OR_ID` closes it permanently."
)]
struct Cli {
    #[command(flatten)]
    options: Options,
    #[command(subcommand)]
    action: Option<Action>,
}
#[derive(Args)]
struct Options {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    project: Option<PathBuf>,
    #[arg(long, global = true, default_value = "pi")]
    pi: PathBuf,
    /// Explicit session name or ID for attachment or a scoped call.
    #[arg(long, global = true)]
    session: Option<String>,
    /// Open the session's graph before displaying its native Pi terminal.
    #[arg(long, global = true)]
    ui: bool,
}
#[derive(Subcommand)]
enum Action {
    /// Internal launcher used by the managed session shell.
    #[command(hide = true)]
    InternalPi {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        generation: String,
    },
    #[command(flatten)]
    Manage(SessionAction),
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },
    /// Session administration (compatible nested command forms).
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    Call {
        operation: String,
        #[arg(long, default_value = "{}", conflicts_with = "file")]
        args: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Archive the legacy home and move the stopped store into ~/.ontography.
    Migrate {
        #[arg(long)]
        from: Option<PathBuf>,
        #[arg(long)]
        to: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
enum ServerAction {
    Start,
    Status,
    Stop,
    Run {
        #[arg(long, hide = true)]
        detach: bool,
    },
}
#[derive(Subcommand)]
enum SessionAction {
    /// Create independent Pi state and attach; the graph starts uninitialized.
    #[command(alias = "create")]
    New {
        name: Option<String>,
        #[arg(long)]
        no_attach: bool,
    },
    /// List session, manager terminal, and graph status.
    #[command(visible_alias = "ls")]
    List {
        #[arg(long)]
        json: bool,
    },
    /// Inspect a session by exact ID or unique exact name.
    Show { id: String },
    /// Attach to an existing session by exact ID or unique exact name.
    Attach { id: String },
    /// Disconnect the controlling terminal; Pi and the graph keep running.
    Detach { id: String },
    /// Resume a session and graph without starting or attaching Pi.
    Resume { id: String },
    /// Stop Pi and suspend the graph, preserving saved state.
    Suspend { id: String },
    /// Stop Pi and permanently close the session and graph; keep history.
    Close { id: String },
    /// Associate an existing, unowned graph run with this app session.
    Adopt { id: String, run_id: String },
}
#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
fn project(path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(std::fs::canonicalize(
        path.unwrap_or(std::env::current_dir()?),
    )?)
}
async fn run(cli: Cli) -> Result<()> {
    let options = cli.options;
    if options.session.is_some() && !matches!(cli.action, None | Some(Action::Call { .. })) {
        return Err(AppError::invalid(
            "--session is only supported with a bare attachment or call; pass a name or ID to session commands",
        ));
    }
    if let Some(Action::Migrate { from, to }) = &cli.action {
        let home = std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
            AppError::new("configuration", "HOME is required for migration defaults")
        })?;
        return print(serde_json::to_value(migration::migrate(
            &from
                .clone()
                .unwrap_or_else(|| home.join(".local/share/ontography")),
            &to.clone().unwrap_or_else(|| home.join(".ontography")),
        )?)?);
    }
    let paths = Paths::initialize(match options.data_dir.clone() {
        Some(path) => path,
        None => persistence::default_data_dir()?,
    })?;
    match cli.action {
        Some(Action::InternalPi {
            session_id,
            generation,
        }) => ontography_app::managed_shell::run_pi(&paths, &session_id, &generation).await,
        Some(Action::Server {
            action: ServerAction::Run { detach },
        }) => {
            ontography_app::logging::install_panic_hook(&paths.root);
            if detach {
                nix::unistd::setsid().map_err(|e| AppError::new("detach_failed", e.to_string()))?;
            }
            let root = paths.root.clone();
            let result = server::serve(paths).await;
            if let Err(error) = &result {
                ontography_app::logging::record(&root, &error.to_string());
            }
            result
        }
        Some(Action::Server { action }) => {
            let client = if matches!(action, ServerAction::Start) {
                launcher::ensure_server(&paths).await?
            } else {
                Client::connect(&paths.socket).await?
            };
            print(
                client
                    .call(
                        if matches!(action, ServerAction::Stop) {
                            "server.stop"
                        } else {
                            "system.status"
                        },
                        json!({}),
                    )
                    .await?,
            )
        }
        Some(Action::Call {
            operation,
            args,
            file,
        }) => {
            let source = match file {
                Some(file) => std::fs::read_to_string(file)?,
                None => args,
            };
            let args = parse_json(&source).map_err(|e| AppError::invalid(e.to_string()))?;
            let client = launcher::ensure_server(&paths).await?;
            let client = if let Some(target) = &options.session {
                client.for_session(&resolve_session(&client, target).await?)
            } else {
                client
            };
            print(client.call(&operation, args).await?)
        }
        Some(Action::Session { action }) => {
            let client = launcher::ensure_server(&paths).await?;
            run_session(&client, &options, action, false).await
        }
        Some(Action::Manage(action)) => {
            let client = launcher::ensure_server(&paths).await?;
            run_session(&client, &options, action, true).await
        }
        None => {
            let client = launcher::ensure_server(&paths).await?;
            if let Some(target) = &options.session {
                let id = resolve_session(&client, target).await?;
                attach(&client, &id, &options.pi, options.ui).await
            } else {
                run_session(
                    &client,
                    &options,
                    SessionAction::New {
                        name: None,
                        no_attach: false,
                    },
                    true,
                )
                .await
            }
        }
        Some(Action::Migrate { .. }) => {
            unreachable!("migration handled before path initialization")
        }
    }
}

async fn run_session(
    client: &Client,
    options: &Options,
    action: SessionAction,
    top_level: bool,
) -> Result<()> {
    let (operation, target, mut args) = match action {
        SessionAction::New { name, no_attach } => {
            if !no_attach {
                require_terminal()?;
            }
            let mut args = json!({"project":project(options.project.clone())?});
            if let Some(name) = name {
                args["name"] = json!(name);
            }
            let value = client.call("session.create", args).await?;
            return if no_attach {
                print(value)
            } else {
                attach(client, value_id(&value)?, &options.pi, options.ui).await
            };
        }
        SessionAction::List { json } => {
            return if top_level {
                list_sessions(client, json).await
            } else {
                print(client.call("session.list", json!({})).await?)
            };
        }
        SessionAction::Attach { id } => {
            let id = resolve_session(client, &id).await?;
            return attach(client, &id, &options.pi, options.ui).await;
        }
        SessionAction::Show { id } => ("session.inspect", id, json!({})),
        SessionAction::Detach { id } => ("terminal.detach", id, json!({})),
        SessionAction::Resume { id } => ("session.resume", id, json!({})),
        SessionAction::Suspend { id } => ("session.suspend", id, json!({})),
        SessionAction::Close { id } => ("session.close", id, json!({})),
        SessionAction::Adopt { id, run_id } => ("session.adopt", id, json!({"run_id":run_id})),
    };
    args["session_id"] = json!(resolve_session(client, &target).await?);
    print(client.call(operation, args).await?)
}

// IDs take precedence over display names. Ambiguous names never choose a target.
async fn resolve_session(client: &Client, target: &str) -> Result<String> {
    let value = client.call("session.list", json!({})).await?;
    let sessions = array(&value, "sessions")?;
    if sessions
        .iter()
        .any(|session| session["session_id"] == target)
    {
        return Ok(target.to_owned());
    }
    let matches = sessions
        .iter()
        .filter(|session| session["name"] == target)
        .map(value_id)
        .collect::<Result<Vec<_>>>()?;
    match matches.as_slice() {
        [id] => Ok((*id).to_owned()),
        [] => Err(AppError::new(
            "session_not_found",
            format!("No session named or identified by {target:?}"),
        )),
        _ => Err(AppError::new(
            "ambiguous_session",
            format!(
                "Multiple sessions named {target:?}; use an ID: {}",
                matches.join(", ")
            ),
        )),
    }
}

async fn list_sessions(client: &Client, as_json: bool) -> Result<()> {
    let mut value = client.call("session.list", json!({})).await?;
    let mut sessions = array(&value, "sessions")?.to_vec();
    let mut runs = BTreeMap::new();
    let mut after = Value::Null;
    loop {
        let page = client
            .call(
                "run.list",
                if after.is_null() {
                    json!({})
                } else {
                    json!({"after":after})
                },
            )
            .await?;
        for run in array(&page, "runs")? {
            let id = run["run_id"]
                .as_str()
                .ok_or_else(|| AppError::new("protocol_error", "run has no identity"))?;
            runs.insert(id.to_owned(), run.clone());
        }
        value["run_recovery_errors"] = page["recovery_errors"].clone();
        if page["next_after"].is_null() {
            break;
        }
        after = page["next_after"].clone();
    }
    for session in &mut sessions {
        let terminal = client
            .call("terminal.status", json!({"session_id":value_id(session)?}))
            .await?;
        session["terminal"] = json!(if terminal["running"] != true {
            "stopped"
        } else if terminal["attached"] == true {
            "attached"
        } else {
            "detached"
        });
        session["program"] = if terminal["running"] == true {
            terminal["manager_mode"].clone()
        } else {
            json!("stopped")
        };
        session["graph"] = session["run_id"].as_str().map_or(Value::Null, |id| {
            runs.get(id)
                .cloned()
                .unwrap_or_else(|| json!({"run_id":id,"status":"unavailable"}))
        });
    }
    value["sessions"] = json!(sessions);
    if as_json {
        return print(value);
    }
    if sessions.is_empty() {
        println!("No sessions. Run `ontography` to create one.");
    } else {
        let name_width = sessions
            .iter()
            .filter_map(|s| s["name"].as_str())
            .map(|s| s.chars().count())
            .max()
            .unwrap_or(4)
            .max(4);
        println!(
            "{:<36}  {:<name_width$}  {:<9}  {:<8}  {:<8}  GRAPH",
            "SESSION", "NAME", "STATE", "TERMINAL", "PROGRAM"
        );
        for session in &sessions {
            println!(
                "{:<36}  {:<name_width$}  {:<9}  {:<8}  {:<8}  {}",
                value_id(session)?,
                session["name"].as_str().unwrap_or("?"),
                match session["status"].as_str() {
                    Some("suspended") => "inactive",
                    Some(status) => status,
                    None => "?",
                },
                session["terminal"].as_str().unwrap_or("?"),
                session["program"].as_str().unwrap_or("unknown"),
                session["graph"]["status"]
                    .as_str()
                    .unwrap_or("uninitialized")
            );
        }
    }
    for key in ["recovery_errors", "run_recovery_errors"] {
        if value[key]
            .as_object()
            .is_some_and(|errors| !errors.is_empty())
        {
            eprintln!("{key}: {}", value[key]);
        }
    }
    Ok(())
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value[key]
        .as_array()
        .ok_or_else(|| AppError::new("protocol_error", format!("response has no {key} array")))
}
fn value_id(value: &Value) -> Result<&str> {
    value["session_id"]
        .as_str()
        .ok_or_else(|| AppError::new("protocol_error", "session operation returned no identity"))
}
fn require_terminal() -> Result<()> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(AppError::new(
            "terminal_required",
            "attach needs an interactive terminal; use new --no-attach or call for scripts",
        ));
    }
    Ok(())
}
async fn attach(client: &Client, id: &str, pi: &Path, graph_first: bool) -> Result<()> {
    require_terminal()?;
    client
        .call("session.resume", json!({"session_id":id}))
        .await?;
    client
        .call("session.select", json!({"session_id":id}))
        .await?;
    let client = client.for_session(id);
    let (cols, rows) = crossterm::terminal::size()?;
    let status = client
        .call(
            "terminal.ensure",
            json!({"pi":pi,"rows":rows.max(2),"cols":cols.max(10)}),
        )
        .await?;
    let socket = PathBuf::from(
        status["socket"]
            .as_str()
            .ok_or_else(|| AppError::new("protocol_error", "terminal socket missing"))?,
    );
    let terminal_id = status["terminal_id"]
        .as_str()
        .ok_or_else(|| AppError::new("protocol_error", "terminal identity missing"))?
        .into();
    let request = terminal::AttachRequest {
        version: terminal::VERSION,
        server_id: client.server_id().into(),
        session_id: id.into(),
        terminal_id,
        rows: rows.max(2),
        cols: cols.max(10),
    };
    terminal_client::run_with_session_panel(&socket, request, graph_first, client.clone(), || {
        ui::run_graph_view(client.clone(), id.to_owned())
    })
    .await
}
fn print(value: Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
