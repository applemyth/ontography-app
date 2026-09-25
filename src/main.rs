use clap::{Parser, Subcommand};
use ontography_app::{
    AppError, Result,
    client::Client,
    declarations::parse_json,
    launcher, migration,
    persistence::{self, Paths},
    server, terminal, terminal_client, ui,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "ontography",
    version,
    about = "Persistent agent sessions and Ontography graphs"
)]
struct Cli {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    project: Option<PathBuf>,
    #[arg(long, global = true, default_value = "pi")]
    pi: PathBuf,
    /// Attach to an exact app session; defaults to the last selected session.
    #[arg(long, global = true)]
    session: Option<String>,
    /// Open the session's graph before displaying its native Pi terminal.
    #[arg(long)]
    ui: bool,
    #[command(subcommand)]
    action: Option<Action>,
}
#[derive(Subcommand)]
enum Action {
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },
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
    #[command(alias = "create")]
    New {
        name: Option<String>,
        #[arg(long)]
        no_attach: bool,
    },
    List,
    Show {
        id: String,
    },
    Attach {
        id: String,
    },
    Detach {
        id: String,
    },
    Resume {
        id: String,
    },
    Suspend {
        id: String,
    },
    Close {
        id: String,
    },
    /// Associate an existing, unowned graph run with this app session.
    Adopt {
        id: String,
        run_id: String,
    },
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
    let paths = Paths::initialize(match cli.data_dir {
        Some(path) => path,
        None => persistence::default_data_dir()?,
    })?;
    match cli.action {
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
            let client = cli
                .session
                .as_deref()
                .map_or_else(|| client.clone(), |id| client.for_session(id));
            print(client.call(&operation, args).await?)
        }
        Some(Action::Session { action }) => {
            let client = launcher::ensure_server(&paths).await?;
            match action {
                SessionAction::New { name, no_attach } => {
                    let mut args = json!({"project":project(cli.project)?});
                    if let Some(name) = name {
                        args["name"] = json!(name);
                    }
                    let value = client.call("session.create", args).await?;
                    if no_attach {
                        print(value)
                    } else {
                        attach(&client, value_id(&value)?, &cli.pi, cli.ui).await
                    }
                }
                SessionAction::List => print(client.call("session.list", json!({})).await?),
                SessionAction::Attach { id } => attach(&client, &id, &cli.pi, cli.ui).await,
                SessionAction::Show { id } => print(
                    client
                        .call("session.inspect", json!({"session_id":id}))
                        .await?,
                ),
                SessionAction::Detach { id } => print(
                    client
                        .call("terminal.detach", json!({"session_id":id}))
                        .await?,
                ),
                SessionAction::Resume { id } => print(
                    client
                        .call("session.resume", json!({"session_id":id}))
                        .await?,
                ),
                SessionAction::Suspend { id } => print(
                    client
                        .call("session.suspend", json!({"session_id":id}))
                        .await?,
                ),
                SessionAction::Close { id } => print(
                    client
                        .call("session.close", json!({"session_id":id}))
                        .await?,
                ),
                SessionAction::Adopt { id, run_id } => print(
                    client
                        .call("session.adopt", json!({"session_id":id,"run_id":run_id}))
                        .await?,
                ),
            }
        }
        None => {
            let client = launcher::ensure_server(&paths).await?;
            let id = if let Some(id) = cli.session {
                id
            } else {
                let sessions = client.call("session.list", json!({})).await?;
                if let Some(id) = sessions["selected_session_id"].as_str() {
                    id.into()
                } else {
                    let value = client
                        .call("session.create", json!({"project":project(cli.project)?}))
                        .await?;
                    value_id(&value)?.into()
                }
            };
            attach(&client, &id, &cli.pi, cli.ui).await
        }
        Some(Action::Migrate { .. }) => {
            unreachable!("migration handled before path initialization")
        }
    }
}
fn value_id(value: &Value) -> Result<&str> {
    value["session_id"]
        .as_str()
        .ok_or_else(|| AppError::new("protocol_error", "session operation returned no identity"))
}
async fn attach(client: &Client, id: &str, pi: &Path, graph_first: bool) -> Result<()> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(AppError::new(
            "terminal_required",
            "attach needs an interactive terminal; use session new --no-attach or call for scripts",
        ));
    }
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
    terminal_client::run_with_initial_graph(&socket, request, graph_first, || {
        ui::run_graph_view(client.clone(), id.to_owned())
    })
    .await
}
fn print(value: Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
