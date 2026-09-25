//! Explicit iroh endpoints and transfers, independent of workflow delivery.
use crate::{
    AppError, Result,
    catalog::{Operation, schema},
    state::{ManagedRun, Service},
    views,
};
use iroh::{Endpoint, EndpointId, SecretKey, endpoint::presets};
use ontography::{
    ContentId, PackageStore,
    content::network::{
        BlobTicket, ContentDownload, ContentProvider, DownloadProgress, DownloadState,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

pub struct Provider {
    pub handle: ContentProvider,
    pub contents: Vec<ContentId>,
    pub package: Option<ContentId>,
}
pub struct Download {
    pub handle: ContentDownload,
    pub endpoint: Endpoint,
}

#[derive(Default, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct EndpointConfig {
    #[serde(default)]
    network: Network,
    #[serde(default)]
    bind_addresses: Vec<String>,
    secret_key_file: Option<PathBuf>,
}
#[derive(Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Network {
    #[default]
    Direct,
    N0,
}

async fn endpoint(value: Option<&Value>, project: &Path) -> Result<Endpoint> {
    let config: EndpointConfig =
        serde_json::from_value(value.cloned().unwrap_or_else(|| json!({})))?;
    let mut builder = match config.network {
        Network::Direct => Endpoint::builder(presets::Minimal),
        Network::N0 => Endpoint::builder(presets::N0),
    };
    if !config.bind_addresses.is_empty() {
        builder = builder.clear_ip_transports();
        for address in config.bind_addresses {
            builder = builder
                .bind_addr(address.as_str())
                .map_err(AppError::core)?;
        }
    }
    if let Some(path) = config.secret_key_file {
        let path = if path.is_absolute() {
            path
        } else {
            project.join(path)
        };
        let key: SecretKey = std::fs::read_to_string(path)?
            .trim()
            .parse()
            .map_err(AppError::core)?;
        builder = builder.secret_key(key);
    }
    builder.bind().await.map_err(AppError::core)
}

pub fn progress(value: DownloadProgress) -> Value {
    let (state, content, error) = match value.state {
        DownloadState::Connecting => ("connecting", None, None),
        DownloadState::Downloading => ("downloading", None, None),
        DownloadState::Verifying => ("verifying", None, None),
        DownloadState::Complete(id) => ("complete", Some(id), None),
        DownloadState::Cancelled => ("cancelled", None, None),
        DownloadState::Failed(error) => ("failed", None, Some(error)),
    };
    json!({"state":state,"content":content,"error":error,"local_bytes":value.local_bytes.to_string(),"received_bytes":value.received_bytes.to_string()})
}

fn ticket(args: &Value) -> Result<BlobTicket> {
    views::field(args, "ticket")?
        .parse()
        .map_err(AppError::core)
}
fn content(args: &Value, field: &str) -> Result<ContentId> {
    let mut value = args
        .get(field)
        .cloned()
        .ok_or_else(|| AppError::invalid(format!("{field} is required")))?;
    super::content::normalize_content_ids_input(&mut value)?;
    serde_json::from_value(value).map_err(Into::into)
}
fn absent() -> AppError {
    AppError::new(
        "unknown_handle",
        "network resource is absent, expired, or belongs to another run",
    )
}

pub async fn dispatch(run: &mut ManagedRun, operation: &str, args: &Value) -> Result<Value> {
    let mut normalized = args.clone();
    super::content::normalize_content_ids_input(&mut normalized)?;
    let args = &normalized;
    match operation {
        "network.serve" | "network.serve_package" => {
            let store = run
                .live()?
                .session
                .content_store()
                .await
                .map_err(AppError::core)?;
            let package = if operation == "network.serve_package" {
                Some(content(args, "package")?)
            } else {
                None
            };
            let contents: Vec<ContentId> = if let Some(root) = package {
                let dependencies = PackageStore::new(store.clone())
                    .dependencies(root)
                    .await
                    .map_err(AppError::core)?;
                vec![
                    store
                        .import_hash_sequence(&dependencies)
                        .await
                        .map_err(AppError::core)?,
                ]
            } else {
                serde_json::from_value(
                    args.get("contents")
                        .cloned()
                        .ok_or_else(|| AppError::invalid("contents is required"))?,
                )?
            };
            if contents.is_empty() {
                return Err(AppError::invalid("select at least one content root"));
            }
            let peers: Option<Vec<String>> = args
                .get("allowed_peers")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?;
            let peers: Option<BTreeSet<EndpointId>> = peers
                .map(|values| {
                    values
                        .into_iter()
                        .map(|v| v.parse().map_err(AppError::core))
                        .collect()
                })
                .transpose()?;
            let endpoint = endpoint(args.get("endpoint"), &run.manifest.project).await?;
            let handle = match store.serve(endpoint.clone(), contents.clone(), peers).await {
                Ok(handle) => handle,
                Err(error) => {
                    endpoint.close().await;
                    return Err(AppError::core(error));
                }
            };
            let id = uuid::Uuid::new_v4().to_string();
            let tickets = contents
                .iter()
                .map(|&content| {
                    handle
                        .ticket(content)
                        .map(|t| json!({"content":content,"ticket":t.to_string()}))
                        .map_err(AppError::core)
                })
                .collect::<Result<Vec<_>>>()?;
            let result = json!({"provider_id":id,"address":handle.address(),"contents":contents,"package":package,"tickets":tickets});
            run.live_mut()?.providers.insert(
                id,
                Provider {
                    handle,
                    contents,
                    package,
                },
            );
            Ok(result)
        }
        "network.providers" => Ok(
            json!({"providers":run.live()?.providers.iter().map(|(id,p)|json!({"provider_id":id,"address":p.handle.address(),"contents":p.contents,"package":p.package})).collect::<Vec<_>>()}),
        ),
        "network.ticket" => {
            let provider = run
                .live()?
                .providers
                .get(views::field(args, "provider_id")?)
                .ok_or_else(absent)?;
            let ticket = provider
                .handle
                .ticket(content(args, "content")?)
                .map_err(AppError::core)?;
            Ok(json!({"ticket":ticket.to_string()}))
        }
        "network.stop" => {
            run.live_mut()?
                .providers
                .remove(views::field(args, "provider_id")?)
                .ok_or_else(absent)?
                .handle
                .shutdown()
                .await
                .map_err(AppError::core)?;
            Ok(json!({"stopped":true}))
        }
        "network.download" => {
            let ticket = ticket(args)?;
            let store = run
                .live()?
                .session
                .content_store()
                .await
                .map_err(AppError::core)?;
            let endpoint = endpoint(args.get("endpoint"), &run.manifest.project).await?;
            let handle = match store.download(endpoint.clone(), ticket).await {
                Ok(handle) => handle,
                Err(error) => {
                    endpoint.close().await;
                    return Err(AppError::core(error));
                }
            };
            let id = uuid::Uuid::new_v4().to_string();
            let result = json!({"download_id":id,"ticket":handle.ticket().to_string(),"progress":progress(handle.progress())});
            run.live_mut()?
                .downloads
                .insert(id, Download { handle, endpoint });
            Ok(result)
        }
        "network.downloads" => Ok(
            json!({"downloads":run.live()?.downloads.iter().map(|(id,d)|json!({"download_id":id,"ticket":d.handle.ticket().to_string(),"progress":progress(d.handle.progress())})).collect::<Vec<_>>()}),
        ),
        "network.progress" | "network.cancel" => {
            let download = run
                .live()?
                .downloads
                .get(views::field(args, "download_id")?)
                .ok_or_else(absent)?;
            if operation == "network.cancel" {
                download.handle.cancel();
            }
            Ok(
                json!({"download_id":views::field(args,"download_id")?,"progress":progress(download.handle.progress())}),
            )
        }
        "network.release" => {
            let download = run
                .live_mut()?
                .downloads
                .remove(views::field(args, "download_id")?)
                .ok_or_else(absent)?;
            download.handle.cancel();
            let result = download.handle.finish().await;
            download.endpoint.close().await;
            Ok(
                json!({"released":true,"content":result.as_ref().ok(),"terminal_error":result.err().map(|e|e.to_string()),"partial_bytes_preserved":true}),
            )
        }
        "network.status" | "network.discard" => {
            let ticket = ticket(args)?;
            let store = run
                .live()?
                .session
                .content_store()
                .await
                .map_err(AppError::core)?;
            if operation == "network.discard" {
                // Transfer tags and artifact tags are separate in core. Checkpoints retain artifact tags.
                store
                    .discard_download(&ticket)
                    .await
                    .map_err(AppError::core)?;
            }
            let available = store
                .download_status(&ticket)
                .await
                .map_err(AppError::core)?;
            Ok(
                json!({"complete":available.complete,"local_bytes":available.local_bytes.to_string(),"transfer_pin_released":operation=="network.discard"}),
            )
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

/// Observe without holding the run mutex, so another client can cancel or suspend.
pub async fn wait(service: &Service, args: &Value) -> Result<Value> {
    let timeout = views::integer(args, "timeout_ms", 30_000)?;
    if !(1..=30_000).contains(&timeout) {
        return Err(AppError::invalid("timeout_ms must be 1..30000"));
    }
    let run = service.run(views::field(args, "run_id")?).await?;
    let mut receiver = {
        let run = run.lock().await;
        run.live()?
            .downloads
            .get(views::field(args, "download_id")?)
            .ok_or_else(absent)?
            .handle
            .subscribe()
    };
    let completed = tokio::time::timeout(Duration::from_millis(timeout), async {
        loop {
            if matches!(
                receiver.borrow().state,
                DownloadState::Complete(_) | DownloadState::Cancelled | DownloadState::Failed(_)
            ) {
                break;
            }
            if receiver.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .is_ok();
    Ok(json!({"settled":completed,"progress":progress(receiver.borrow().clone())}))
}

pub fn operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let id = super::workflow::content_id_schema();
    let endpoint = schema::<EndpointConfig>();
    let mut serve = json!({"contents":{"type":"array","minItems":1,"items":id},"endpoint":endpoint,"allowed_peers":{"type":"array","items":text}});
    let mut package_serve = serve.clone();
    package_serve.as_object_mut().unwrap().remove("contents");
    package_serve["package"] = id.clone();
    let rows = [
        (
            "network.serve",
            "Serve explicitly selected roots using a dedicated iroh endpoint. Direct networking is the default; n0 enables public relay/discovery.",
            &mut serve,
            vec!["contents"],
            true,
        ),
        (
            "network.serve_package",
            "Bundle and serve a semantic package's full dependency closure. Downloaded content still requires a separate governed workflow submission.",
            &mut package_serve,
            vec!["package"],
            true,
        ),
    ];
    let mut operations = rows
        .into_iter()
        .map(|(name, description, props, required, mutating)| {
            op(name, description, props.clone(), &required, mutating)
        })
        .collect::<Vec<_>>();
    for (name, description, props, required, mutating) in [
        (
            "network.providers",
            "List retained providers and their scoped roots.",
            json!({}),
            vec![],
            false,
        ),
        (
            "network.ticket",
            "Issue a ticket for an explicitly published content root.",
            json!({"provider_id":text,"content":id}),
            vec!["provider_id", "content"],
            false,
        ),
        (
            "network.stop",
            "Shut down a provider and its dedicated endpoint.",
            json!({"provider_id":text}),
            vec!["provider_id"],
            true,
        ),
        (
            "network.download",
            "Start or resume a verified transfer; retain its handle across client disconnects.",
            json!({"ticket":text,"endpoint":endpoint}),
            vec!["ticket"],
            true,
        ),
        (
            "network.downloads",
            "List retained downloads, tickets, and progress.",
            json!({}),
            vec![],
            false,
        ),
        (
            "network.progress",
            "Inspect a retained download.",
            json!({"download_id":text}),
            vec!["download_id"],
            false,
        ),
        (
            "network.wait",
            "Wait up to 30 seconds for transfer completion without blocking cancellation.",
            json!({"download_id":text,"timeout_ms":{"type":"integer","minimum":1,"maximum":30000}}),
            vec!["download_id"],
            false,
        ),
        (
            "network.cancel",
            "Request transfer cancellation; retain verified partial bytes.",
            json!({"download_id":text}),
            vec!["download_id"],
            true,
        ),
        (
            "network.release",
            "Settle and release a download and its endpoint; preserve its resumable partial bytes.",
            json!({"download_id":text}),
            vec!["download_id"],
            true,
        ),
        (
            "network.status",
            "Inspect local verified availability without connecting to a peer.",
            json!({"ticket":text}),
            vec!["ticket"],
            false,
        ),
        (
            "network.discard",
            "Release a download's persistent transfer pin. Separate retained artifacts and ledger dependencies remain pinned.",
            json!({"ticket":text}),
            vec!["ticket"],
            true,
        ),
    ] {
        operations.push(op(name, description, props, &required, mutating));
    }
    operations
}
fn op(
    name: &str,
    description: &str,
    mut props: Value,
    required: &[&str],
    mutating: bool,
) -> Operation {
    props["run_id"] = json!({"type":"string"});
    let mut required = required.to_vec();
    required.push("run_id");
    Operation::new(name, description, props, &required, mutating)
}
