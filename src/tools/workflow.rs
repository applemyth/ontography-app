//! Governed workflow mutations and observations through the owned core session.

use crate::catalog::Operation;
use crate::state::{ManagedRun, Service};
use crate::{AppError, Result, persistence, views};
use ontography::{
    Activation, ActivationId, ActivationProposal, ContentId, Emission, OutputAuthority,
    PackageRecord, PendingFrontier, Retirement, SessionSnapshot, Trigger,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};

/// The result and emissions of a single root or package-triggered activation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalInput {
    pub trigger: TriggerInput,
    pub result: Value,
    #[serde(default)]
    pub emissions: Vec<EmissionInput>,
    #[serde(default)]
    pub contents: Vec<ContentId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TriggerInput {
    Root {
        node_id: String,
        authority: Vec<String>,
    },
    Packages {
        package_ids: Vec<String>,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthorityInput {
    #[default]
    Carry,
    Transition {
        tags: Vec<String>,
    },
}

/// Select exactly one destination: immediate edge delivery or outbound type.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmissionInput {
    #[serde(default)]
    pub edge_id: Option<String>,
    #[serde(default)]
    pub object_type: Option<String>,
    pub payload: Value,
    #[serde(default)]
    pub authority: AuthorityInput,
}

impl EmissionInput {
    pub fn compile(&self) -> Result<Emission> {
        let payload = views::payload(&self.payload)?;
        let authority = match &self.authority {
            AuthorityInput::Carry => OutputAuthority::Carry,
            AuthorityInput::Transition { tags } => {
                OutputAuthority::Transition(views::authority(tags)?)
            }
        };
        match (&self.edge_id, &self.object_type) {
            (Some(edge), None) => Ok(Emission::new(edge.as_str(), authority, payload)),
            (None, Some(object_type)) => {
                Ok(Emission::outbound(object_type.as_str(), authority, payload))
            }
            _ => Err(AppError::invalid(
                "an emission requires exactly one of edge_id and object_type",
            )),
        }
    }
}

impl ProposalInput {
    pub fn compile(&self) -> Result<ActivationProposal> {
        let result = views::payload(&self.result)?;
        let mut proposal = match &self.trigger {
            TriggerInput::Root { node_id, authority } => {
                ActivationProposal::root(node_id.as_str(), views::authority(authority)?, result)
            }
            TriggerInput::Packages { package_ids } => ActivationProposal::join(
                package_ids
                    .iter()
                    .map(|id| views::package_id(id))
                    .collect::<Result<Vec<_>>>()?,
                result,
            ),
        };
        for emission in &self.emissions {
            proposal.emit(emission.compile()?);
        }
        Ok(proposal)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferInput {
    package_id: String,
    edge_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetireInput {
    package_id: String,
    #[serde(default)]
    evidence_activation_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementQuery {
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FrontierPhase {
    #[default]
    Received,
    Outbound,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontierInput {
    #[serde(default)]
    phase: FrontierPhase,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerQuery {
    node_id: String,
    #[serde(default)]
    edge_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageQuery {
    package_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationQuery {
    activation_id: String,
    #[serde(default = "default_preview")]
    preview_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationContentQuery {
    activation_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportInput {
    path: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitInput {
    #[serde(default)]
    after_revision: Option<String>,
    #[serde(default = "wait_timeout")]
    timeout_ms: u64,
}
fn wait_timeout() -> u64 {
    30_000
}

/// A frontier subscription owns only a watch receiver, never the storage engine.
/// Retaining a SessionHandle here would delay suspend/reopen until every waiter
/// finished. Admission is sampled under the run lock, without monopolizing it.
pub async fn dispatch_wait(service: &Service, args: &Value) -> Result<Value> {
    let run = service.run(views::field(args, "run_id")?).await?;
    let (input, mut receiver, initial_admission) = {
        let run = run.lock().await;
        let input: WaitInput = parse_args(&run, args)?;
        if !(1..=30_000).contains(&input.timeout_ms) {
            return Err(AppError::invalid("timeout_ms must be between 1 and 30000"));
        }
        let session = &run.live()?.session;
        (input, session.frontier(), session.status())
    };
    let after = input
        .after_revision
        .as_deref()
        .map(|revision| {
            revision
                .parse::<u64>()
                .map_err(|_| AppError::invalid("after_revision must be an unsigned decimal string"))
        })
        .transpose()?;
    let mut timed_out = false;
    let mut reason = "snapshot";
    if after == Some(receiver.revision()) && initial_admission == ontography::SessionStatus::Open {
        let deadline = tokio::time::sleep(Duration::from_millis(input.timeout_ms));
        tokio::pin!(deadline);
        let mut poll = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = &mut deadline => { timed_out = true; reason = "timeout"; break; }
                revision = receiver.changed() => {
                    reason = if after == Some(revision) { "observation_ended" } else { "frontier_notification" };
                    break;
                }
                _ = poll.tick() => {
                    if let Ok(run) = run.try_lock()
                        && run.live.as_ref().is_none_or(|live|live.session.status()!=ontography::SessionStatus::Open)
                    { reason = "lifecycle"; break; }
                }
            }
        }
    }
    // Never extend the bounded wait behind a long capture/import. A busy run has
    // explicitly unavailable admission; callers refresh after any notification.
    let (admission, run_status) = match run.try_lock() {
        Ok(run) => (
            run.live
                .as_ref()
                .map(|live| views::status(live.session.status())),
            if run.live.is_some() {
                "active".to_owned()
            } else {
                run.manifest.status.clone()
            },
        ),
        Err(_) => (None, "busy".to_owned()),
    };
    Ok(
        json!({"revision":receiver.revision().to_string(),"admission":admission,"run_status":run_status,"timed_out":timed_out,"reason":reason}),
    )
}

pub async fn dispatch(run: &mut ManagedRun, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        "workflow.submit" => {
            let input: ProposalInput = parse_args(run, args)?;
            let proposal = input.compile()?;
            let session = &run.live()?.session;
            let decision = session
                .submit_with_content(proposal, input.contents)
                .await
                .map_err(AppError::core)?;
            views::decision(decision)
        }
        "workflow.transfer" => {
            let input: TransferInput = parse_args(run, args)?;
            let package_id = views::package_id(&input.package_id)?;
            let delivery = run
                .live()?
                .session
                .transfer(package_id, &input.edge_id)
                .await
                .map_err(AppError::core)?
                .map_err(|error| AppError::new("rejected", error.to_string()))?;
            Ok(
                json!({"package_id":package_id.to_string(),"edge_id":delivery.edge_id(),"receiver":delivery.receiver()}),
            )
        }
        "workflow.retire" => {
            let input: RetireInput = parse_args(run, args)?;
            let id = views::package_id(&input.package_id)?;
            let evidence = input
                .evidence_activation_id
                .as_deref()
                .map(views::activation_id)
                .transpose()?;
            let retirement = run
                .live()?
                .session
                .retire(id, evidence)
                .await
                .map_err(AppError::core)?
                .map_err(|error| AppError::new("rejected", error.to_string()))?;
            let history = run
                .live()?
                .session
                .package_history(id)
                .await
                .map_err(AppError::core)?
                .ok_or_else(|| {
                    AppError::new("invalid_state", "Retired package history is missing")
                })?;
            Ok(
                json!({"package_id":id.to_string(),"disposition":"retired","revision":retirement.revision().to_string(),"retirement":retirement_view(&retirement,history.package())}),
            )
        }
        "inspect.frontier" => {
            let input: FrontierInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            let after = input.after.as_deref().map(views::package_id).transpose()?;
            let session = &run.live()?.session;
            let frontier = match input.phase {
                FrontierPhase::Received => match input.node_id.as_deref() {
                    Some(node) => session.pending_page_at(node, after, input.limit).await,
                    None => session.pending_page(after, input.limit).await,
                },
                FrontierPhase::Outbound => {
                    session
                        .outbound_page(input.node_id.as_deref(), after, input.limit)
                        .await
                }
            }
            .map_err(AppError::core)?;
            let mut value = frontier_view(&frontier);
            value["phase"] = json!(match input.phase {
                FrontierPhase::Received => "received",
                FrontierPhase::Outbound => "outbound",
            });
            value["next_after"] = json!(if frontier.packages().len() == input.limit {
                frontier.packages().last().map(|(id, _)| id.to_string())
            } else {
                None
            });
            Ok(value)
        }
        "inspect.trigger" => {
            let input: TriggerQuery = parse_args(run, args)?;
            let session = &run.live()?.session;
            let frontier = match input.edge_id {
                Some(edge) => {
                    session
                        .next_pending_on_edge_at(input.node_id.as_str(), edge.as_str())
                        .await
                }
                None => session.next_trigger_at(input.node_id.as_str()).await,
            }
            .map_err(AppError::core)?;
            Ok(frontier_view(&frontier))
        }
        "inspect.package" => {
            let input: PackageQuery = parse_args(run, args)?;
            let id = views::package_id(&input.package_id)?;
            let snapshot = run
                .live()?
                .session
                .try_snapshot()
                .await
                .map_err(AppError::core)?;
            let state = snapshot.state();
            let package = state
                .package(id)
                .ok_or_else(|| AppError::new("not_found", "package occurrence does not exist"))?;
            let producer = state
                .activation(id.producer())
                .ok_or_else(|| AppError::new("invalid_state", "package producer does not exist"))?;
            let inputs = match producer.trigger() {
                Trigger::Orig { .. } => Vec::new(),
                Trigger::Pkgs { package_ids } => {
                    package_ids.iter().map(ToString::to_string).collect()
                }
            };
            let mut value = views::package_state(state, id, package)?;
            value["inputs"] = json!(inputs);
            value["revision"] = json!(snapshot.revision().to_string());
            Ok(value)
        }
        "inspect.retirements" => {
            let input: RetirementQuery = parse_args(run, args)?;
            check_limit(input.limit)?;
            let after = input.after.as_deref().map(views::package_id).transpose()?;
            let snapshot = run
                .live()?
                .session
                .try_snapshot()
                .await
                .map_err(AppError::core)?;
            let all_retirements = snapshot.state().retirements();
            let mut records = all_retirements
                .iter()
                .filter(|(id, _)| after.is_none_or(|after| **id > after));
            let retirements = records
                .by_ref()
                .take(input.limit)
                .map(|(id, record)| {
                    let package = snapshot.state().package(*id).ok_or_else(|| {
                        AppError::new("invalid_state", "Retired package history is missing")
                    })?;
                    let mut value = retirement_view(record, package);
                    value["package_id"] = json!(id.to_string());
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;
            let next_after = if records.next().is_some() {
                retirements
                    .last()
                    .map(|record| record["package_id"].clone())
            } else {
                None
            };
            Ok(
                json!({"revision":snapshot.revision().to_string(),"retirements":retirements,"next_after":next_after}),
            )
        }
        "inspect.activation" => {
            let input: ActivationQuery = parse_args(run, args)?;
            check_preview(input.preview_bytes)?;
            let id = views::activation_id(&input.activation_id)?;
            let snapshot = run
                .live()?
                .session
                .try_snapshot()
                .await
                .map_err(AppError::core)?;
            let activation = snapshot
                .state()
                .activation(id)
                .ok_or_else(|| AppError::new("not_found", "activation does not exist"))?;
            let mut value = activation_view(id, activation, input.preview_bytes);
            value["revision"] = json!(snapshot.revision().to_string());
            value["contents"] = json!(
                snapshot
                    .activation_content()
                    .get(&id)
                    .cloned()
                    .unwrap_or_default()
            );
            Ok(value)
        }
        "inspect.activation_content" => {
            let input: ActivationContentQuery = parse_args(run, args)?;
            let id = views::activation_id(&input.activation_id)?;
            let contents = run
                .live()?
                .session
                .activation_content(id)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"activation_id":id.to_string(),"contents":contents}))
        }
        "inspect.export" => {
            let input: ExportInput = parse_args(run, args)?;
            let path = if input.path.is_absolute() {
                input.path
            } else {
                run.manifest.project.join(input.path)
            };
            let snapshot = run
                .live()?
                .session
                .try_snapshot()
                .await
                .map_err(AppError::core)?;
            let value = snapshot_view(run, &snapshot)?;
            persistence::write_json(&path, &value)?;
            Ok(
                json!({"path":path,"revision":snapshot.revision().to_string(),"kind":"workflow_snapshot","artifact_bytes_included":false,"context_records_included":false}),
            )
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn frontier_view(frontier: &PendingFrontier) -> Value {
    json!({"revision":frontier.revision().to_string(),"packages":frontier.packages().iter().map(|(id,p)|views::package(*id,p)).collect::<Vec<_>>()})
}

fn retirement_view(retirement: &Retirement, package: &PackageRecord) -> Value {
    let mut view = views::retirement(retirement);
    view["holder"] = json!(package.holder());
    view["phase"] = json!(views::phase(package.phase()));
    view
}

fn activation_view(id: ActivationId, activation: &Activation, preview_bytes: usize) -> Value {
    let trigger = match activation.trigger() {
        Trigger::Orig { node_id, authority } => {
            json!({"kind":"root","node_id":node_id,"authority":authority.tags().map(|tag|tag.id()).collect::<Vec<_>>()})
        }
        Trigger::Pkgs { package_ids } => {
            json!({"kind":"packages","package_ids":package_ids.iter().map(ToString::to_string).collect::<Vec<_>>()})
        }
    };
    let result = activation.result();
    json!({"activation_id":id.to_string(),"trigger":trigger,
        "result":views::bytes(&result[..result.len().min(preview_bytes)]),"result_bytes":result.len().to_string(),"result_truncated":result.len()>preview_bytes,
        "outputs":activation.package_outputs().iter().map(|(id,output)|json!({"package_id":id.to_string(),"edge_id":output.edge_id(),"object_type":output.object_type(),"authority":output.authority().tags().map(|tag|tag.id()).collect::<Vec<_>>(),"content_digest":output.content_digest().to_string()})).collect::<Vec<_>>()})
}

fn snapshot_view(run: &ManagedRun, snapshot: &SessionSnapshot) -> Result<Value> {
    let state = snapshot.state();
    let kernel = snapshot.kernel();
    let packages = state
        .packages()
        .iter()
        .map(|(id, p)| views::package_state(state, *id, p))
        .collect::<Result<Vec<_>>>()?;
    let retirements = state
        .retirements()
        .iter()
        .map(|(id, record)| {
            let package = state.package(*id).ok_or_else(|| {
                AppError::new("invalid_state", "Retired package history is missing")
            })?;
            let mut value = retirement_view(record, package);
            value["package_id"] = json!(id.to_string());
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"format":"ontography.workflow_snapshot","version":1,"run_id":run.manifest.run_id,
        "declaration":run.manifest.declaration,"revision":snapshot.revision().to_string(),"admission":views::status(snapshot.status()),
        "current_graph":views::graph(kernel),
        "current_node_definitions":kernel.node_definitions().iter().map(|n|json!({"node_id":n.node_id(),"types":n.types(),"result_contract":n.result_contract(),"ingress_mode":match n.ingress_mode(){ontography::IngressMode::Any=>"any",ontography::IngressMode::All=>"all"}})).collect::<Vec<_>>(),
        "current_edge_definitions":kernel.edge_definitions().iter().map(|e|json!({"edge_id":e.edge_id(),"types":e.types(),"source_requirements":e.source_requirements(),"target_requirements":e.target_requirements(),"package_contract":e.package_contract(),"authority_tags":e.authority_tags().iter().map(|t|t.id()).collect::<Vec<_>>(),"authority_match":match e.authority_match(){ontography::AuthorityMatch::AnyOf=>"any_of",ontography::AuthorityMatch::AllOf=>"all_of"}})).collect::<Vec<_>>(),
        "current_roots":kernel.roots().iter().map(|r|json!({"node_id":r.node_id(),"ceiling":r.ceiling().tags().map(|t|t.id()).collect::<Vec<_>>()})).collect::<Vec<_>>(),
        "current_authority_transitions":kernel.authority_transitions().iter().map(|r|json!({"node_id":r.node_id(),"from":r.from().tags().map(|t|t.id()).collect::<Vec<_>>(),"to":r.to().tags().map(|t|t.id()).collect::<Vec<_>>()})).collect::<Vec<_>>(),
        "current_fingerprint":kernel.fingerprint().to_string(),
        "activations":state.activations().iter().map(|(id,a)|activation_view(*id,a,usize::MAX)).collect::<Vec<_>>(),
        "packages":packages,
        "retirements":retirements,
        "activation_content":snapshot.activation_content().iter().map(|(id,contents)|json!({"activation_id":id.to_string(),"contents":contents})).collect::<Vec<_>>(),
        "artifact_bytes_included":false,"context_records_included":false}),
    )
}

pub(crate) fn parse_args<T: DeserializeOwned>(run: &ManagedRun, args: &Value) -> Result<T> {
    if views::field(args, "run_id")? != run.manifest.run_id {
        return Err(AppError::new(
            "foreign_handle",
            "arguments name a different run",
        ));
    }
    let mut args = args.clone();
    super::content::normalize_content_ids_input(&mut args)?;
    args.as_object_mut()
        .ok_or_else(|| AppError::invalid("args must be an object"))?
        .remove("run_id");
    Ok(serde_json::from_value(args)?)
}

pub(crate) fn default_limit() -> usize {
    100
}
pub(crate) fn default_preview() -> usize {
    4096
}
pub(crate) fn check_limit(limit: usize) -> Result<()> {
    if !(1..=1000).contains(&limit) {
        return Err(AppError::invalid("limit must be between 1 and 1000"));
    }
    Ok(())
}
pub(crate) fn check_preview(limit: usize) -> Result<()> {
    if limit > 65536 {
        return Err(AppError::invalid("preview_bytes must not exceed 65536"));
    }
    Ok(())
}

pub(crate) fn operation(
    name: &str,
    description: &str,
    mut properties: Value,
    required: &[&str],
    mutating: bool,
) -> Operation {
    properties["run_id"] =
        json!({"type":"string","description":"Exact run UUID from run.start or run.list"});
    let mut required = required.to_vec();
    required.push("run_id");
    Operation::new(name, description, properties, &required, mutating)
}

pub(crate) fn payload_schema() -> Value {
    json!({"anyOf":[{"type":"string"},{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}}]})
}

pub(crate) fn content_id_schema() -> Value {
    json!({"type":"object","properties":{"hash":{"type":"string"},"format":{"type":"string","enum":["Raw","HashSeq"]},"size":{"anyOf":[{"type":"integer","minimum":0,"maximum":9007199254740991u64},{"type":"string","pattern":"^[0-9]+$"}]}},"required":["hash","format","size"]})
}

pub(crate) fn emissions_schema() -> Value {
    json!({"type":"array","items":{"type":"object","additionalProperties":false,
        "properties":{"edge_id":{"type":"string"},"object_type":{"type":"string"},"payload":payload_schema(),"authority":{"oneOf":[{"type":"object","additionalProperties":false,"properties":{"kind":{"const":"carry"}},"required":["kind"]},{"type":"object","additionalProperties":false,"properties":{"kind":{"const":"transition"},"tags":{"type":"array","items":{"type":"string"}}},"required":["kind","tags"]}]}},
        "required":["payload"],"oneOf":[{"required":["edge_id"],"not":{"required":["object_type"]}},{"required":["object_type"],"not":{"required":["edge_id"]}}]}})
}

pub fn operations() -> Vec<Operation> {
    let trigger = json!({"oneOf":[
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"root"},"node_id":{"type":"string"},"authority":{"type":"array","items":{"type":"string"}}},"required":["kind","node_id","authority"]},
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"packages"},"package_ids":{"type":"array","items":{"type":"string"}}},"required":["kind","package_ids"]}]});
    vec![
        operation(
            "workflow.submit",
            "Submit one root or package-triggered activation with explicit result, emissions, and content dependencies.",
            json!({"trigger":trigger,"result":payload_schema(),"emissions":emissions_schema(),"contents":{"type":"array","items":content_id_schema()}}),
            &["trigger", "result"],
            true,
        ),
        operation(
            "workflow.transfer",
            "Deliver a live outbound occurrence through an accepting edge. A package can be delivered only once.",
            json!({"package_id":{"type":"string"},"edge_id":{"type":"string"}}),
            &["package_id", "edge_id"],
            true,
        ),
        operation(
            "workflow.retire",
            "Retire one live received or outbound package without consuming it. Optional evidence must identify an accepted activation; retirement rejects if the package is already consumed or retired.",
            json!({"package_id":{"type":"string"},"evidence_activation_id":{"type":"string"}}),
            &["package_id"],
            true,
        ),
        operation(
            "inspect.wait_frontier",
            "Observe the current frontier revision, or wait up to 30s after a decimal revision. Notifications are coalesced hints: refresh state after waking. Admission is null while suspended or another run operation is busy.",
            json!({"after_revision":{"type":"string","pattern":"^[0-9]+$"},"timeout_ms":{"type":"integer","minimum":1,"maximum":30000}}),
            &[],
            false,
        ),
        operation(
            "inspect.frontier",
            "Page current received or outbound packages. Restart pagination if revision changes.",
            json!({"phase":{"type":"string","enum":["received","outbound"]},"node_id":{"type":"string"},"after":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &[],
            false,
        ),
        operation(
            "inspect.trigger",
            "Observe the next complete node trigger, or the next package on an incoming edge. This reserves no work.",
            json!({"node_id":{"type":"string"},"edge_id":{"type":"string"}}),
            &["node_id"],
            false,
        ),
        operation(
            "inspect.package",
            "Read retained occurrence metadata, producer inputs, current live/consumed/retired disposition, custody, and retirement evidence at one revision. Core currently materializes complete history for this query.",
            json!({"package_id":{"type":"string"}}),
            &["package_id"],
            false,
        ),
        operation(
            "inspect.retirements",
            "Page retirement records in package_id order with bounded response size. Restart pagination if revision changes. Core currently materializes complete history for this query.",
            json!({"after":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &[],
            false,
        ),
        operation(
            "inspect.activation",
            "Read one activation and a bounded result preview. Core currently materializes the complete history for this query.",
            json!({"activation_id":{"type":"string"},"preview_bytes":{"type":"integer","minimum":0,"maximum":65536}}),
            &["activation_id"],
            false,
        ),
        operation(
            "inspect.activation_content",
            "Read retained artifact references for an activation. Unknown activations and no dependencies both return an empty list.",
            json!({"activation_id":{"type":"string"}}),
            &["activation_id"],
            false,
        ),
        operation(
            "inspect.export",
            "Export the complete graph/occurrence snapshot to a file. Artifact bytes and context records are excluded; this is not a complete backup.",
            json!({"path":{"type":"string"}}),
            &["path"],
            true,
        ),
    ]
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::declarations::GraphDeclaration;
    use crate::state::{LiveRun, RunManifest};
    use ontography::ProposalRuntime;
    use std::collections::BTreeMap;

    pub(crate) fn test_run(directory: &std::path::Path) -> ManagedRun {
        let declaration =
            GraphDeclaration::parse(include_str!("../../examples/flow.json")).unwrap();
        let compiled = declaration.compile().unwrap();
        let runtime = ProposalRuntime::with_policy(compiled.kernel, compiled.policy);
        let session = runtime.create_persistent(directory.join("core")).unwrap();
        ManagedRun {
            manifest: RunManifest {
                version: 1,
                run_id: uuid::Uuid::new_v4().to_string(),
                core_version: ontography::VERSION.into(),
                core_build: crate::CORE_BUILD.into(),
                declaration_revision: declaration.fingerprint().unwrap(),
                declaration: declaration.into(),
                project: directory.to_owned(),
                core_path: "core".into(),
                status: "active".into(),
                created_at: 0,
                checkpoints: BTreeMap::new(),
                workflow: None,
            },
            directory: directory.to_owned(),
            live: Some(LiveRun::new(runtime, session)),
            recovery_checkouts: BTreeMap::new(),
            registry: std::sync::Arc::new(crate::registry::ImplementationRegistry::default()),
            environment: crate::environment::Environment::current(),
        }
    }

    #[tokio::test]
    async fn outbound_transfer_and_consumption_preserve_occurrence_identity() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let accepted = dispatch(&mut run,"workflow.submit", &json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"sent","emissions":[{"object_type":"Text","payload":"hello"}]})).await.unwrap();
        let outbound = dispatch(
            &mut run,
            "inspect.frontier",
            &json!({"run_id":run_id,"phase":"outbound"}),
        )
        .await
        .unwrap();
        let package_id = outbound["packages"][0]["package_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let transfer = json!({"run_id":run_id,"package_id":package_id,"edge_id":"A_to_B"});
        assert_eq!(
            dispatch(&mut run, "workflow.transfer", &transfer)
                .await
                .unwrap()["receiver"],
            "B"
        );
        assert_eq!(
            dispatch(&mut run, "workflow.transfer", &transfer)
                .await
                .unwrap_err()
                .code,
            "rejected"
        );
        let received = dispatch(
            &mut run,
            "inspect.frontier",
            &json!({"run_id":run_id,"node_id":"B"}),
        )
        .await
        .unwrap();
        assert_eq!(received["packages"][0]["package_id"], package_id);
        dispatch(&mut run,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"packages","package_ids":[package_id]},"result":"received"})).await.unwrap();
        let frontier = dispatch(&mut run, "inspect.frontier", &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert!(frontier["packages"].as_array().unwrap().is_empty());
        let history = dispatch(
            &mut run,
            "inspect.package",
            &json!({"run_id":run_id,"package_id":package_id}),
        )
        .await
        .unwrap();
        assert_eq!(history["package"]["producer"], accepted["activation_id"]);
    }

    #[tokio::test]
    async fn rejected_payload_and_wrong_run_leave_state_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let request = json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":[]},"result":[255]});
        assert_eq!(
            dispatch(&mut run, "workflow.submit", &request)
                .await
                .unwrap_err()
                .code,
            "rejected"
        );
        assert_eq!(
            run.live()
                .unwrap()
                .session
                .try_snapshot()
                .await
                .unwrap()
                .revision(),
            0
        );
        assert_eq!(
            dispatch(
                &mut run,
                "inspect.frontier",
                &json!({"run_id":"another-run"})
            )
            .await
            .unwrap_err()
            .code,
            "foreign_handle"
        );
    }

    #[test]
    fn emission_destination_must_be_unambiguous() {
        let emission: EmissionInput =
            serde_json::from_value(json!({"edge_id":"e","object_type":"Text","payload":"hello"}))
                .unwrap();
        assert_eq!(emission.compile().unwrap_err().code, "invalid_arguments");
    }

    #[tokio::test]
    async fn frontier_wait_does_not_block_commits_and_reports_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let service =
            Service::new(crate::persistence::Paths::initialize(directory.path()).unwrap()).unwrap();
        let run = test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let run = std::sync::Arc::new(tokio::sync::Mutex::new(run));
        service
            .runs
            .lock()
            .await
            .insert(run_id.clone(), run.clone());
        let initial = dispatch_wait(&service, &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_eq!(initial["revision"], "0");
        let args = json!({"run_id":run_id,"after_revision":"0","timeout_ms":1000});
        let (observed, ()) = tokio::join!(dispatch_wait(&service, &args), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut run = run.lock().await;
            dispatch(&mut run,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":[]},"result":"done"})).await.unwrap();
        });
        let observed = observed.unwrap();
        assert_eq!(observed["revision"], "1");
        assert_eq!(observed["timed_out"], false);
        let timeout = dispatch_wait(
            &service,
            &json!({"run_id":run_id,"after_revision":"1","timeout_ms":1}),
        )
        .await
        .unwrap();
        assert_eq!(timeout["timed_out"], true);
        assert_eq!(timeout["admission"], "open");
    }

    #[tokio::test]
    async fn frontier_wait_survives_owner_release_and_observes_close_without_a_revision() {
        let directory = tempfile::tempdir().unwrap();
        let service =
            Service::new(crate::persistence::Paths::initialize(directory.path()).unwrap()).unwrap();
        let run = test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let run = std::sync::Arc::new(tokio::sync::Mutex::new(run));
        service
            .runs
            .lock()
            .await
            .insert(run_id.clone(), run.clone());
        let args = json!({"run_id":run_id,"after_revision":"0","timeout_ms":1000});
        let (observed, ()) = tokio::join!(dispatch_wait(&service, &args), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut run = run.lock().await;
            run.suspend(false).await.unwrap();
            run.resume().await.unwrap();
        });
        assert_eq!(observed.unwrap()["timed_out"], false);
        let (observed, ()) = tokio::join!(dispatch_wait(&service, &args), async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            run.lock().await.live().unwrap().session.close().await;
        });
        let observed = observed.unwrap();
        assert_eq!(observed["revision"], "0");
        assert_eq!(observed["admission"], "closed");
        assert_eq!(observed["timed_out"], false);
    }
}
