use clap::{Parser, Subcommand};
use ontography_app::{
    AppError, Result,
    client::Client,
    declarations::parse_json,
    launcher,
    persistence::{self, Paths},
    server, ui,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "ontography",
    version,
    about = "Manage persistent Ontography graphs with Pi"
)]
struct Cli {
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    project: Option<PathBuf>,
    #[arg(long, global = true, default_value = "pi")]
    pi: PathBuf,
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
    Call {
        operation: String,
        #[arg(long, default_value = "{}", conflicts_with = "file")]
        args: String,
        #[arg(long)]
        file: Option<PathBuf>,
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

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let paths = Paths::initialize(match cli.data_dir {
        Some(path) => path,
        None => persistence::default_data_dir()?,
    })?;
    match cli.action {
        Some(Action::Server {
            action: ServerAction::Run { detach },
        }) => {
            let root = paths.root.clone();
            ontography_app::logging::install_panic_hook(&root);
            if detach {
                nix::unistd::setsid().map_err(|e| AppError::new("detach_failed", e.to_string()))?;
            }
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
                        serde_json::json!({}),
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
            print(
                launcher::ensure_server(&paths)
                    .await?
                    .call(&operation, args)
                    .await?,
            )
        }
        None => {
            let project = std::fs::canonicalize(match cli.project {
                Some(path) => path,
                None => std::env::current_dir()?,
            })?;
            let client = launcher::ensure_server(&paths).await?;
            let mut command = launcher::pi_command(&paths, &project, &cli.pi, cli.ui).await?;
            if cli.ui {
                ui::run(
                    client,
                    ui::UiOptions {
                        pi_command: command,
                    },
                )
                .await
            } else {
                let status = command.status().await?;
                if status.success() {
                    Ok(())
                } else {
                    Err(AppError::new(
                        "pi_exited",
                        format!("Pi exited {status}; the graph server remains available"),
                    ))
                }
            }
        }
    }
}

fn print(value: serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
