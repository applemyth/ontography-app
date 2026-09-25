//! Invocation capabilities and prepared evidence, retained by the run owner.

use super::workflow::{
    EmissionInput, check_limit, check_preview, content_id_schema, default_limit, default_preview,
    emissions_schema, operation, parse_args, payload_schema,
};
use crate::catalog::Operation;
use crate::state::ManagedRun;
use crate::{AppError, Result, views};
use ontography::{
    ContentId, ContextError, ContextEvent, ContextPolicy, ContextResponse, InvocationHandle,
    InvocationId, InvocationRecord, InvocationTrigger, Payload,
};
use serde::Deserialize;
use serde_json::{Value, json};

const RESPONSE_PREVIEW_BUDGET: usize = 192 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BeginInput {
    node_id: String,
    trigger: InvocationTriggerInput,
    #[serde(default)]
    policy: ContextPolicy,
    #[serde(default)]
    contents: Vec<ContentId>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum InvocationTriggerInput {
    Root {
        authority: Vec<String>,
        input: Value,
    },
    Packages {
        package_ids: Vec<String>,
    },
}

impl InvocationTriggerInput {
    fn compile(self) -> Result<InvocationTrigger> {
        match self {
            Self::Root { authority, input } => Ok(InvocationTrigger::Root {
                authority: views::authority(&authority)?,
                input: views::payload(&input)?,
            }),
            Self::Packages { package_ids } => Ok(InvocationTrigger::Packages(
                package_ids
                    .iter()
                    .map(|id| views::package_id(id))
                    .collect::<Result<_>>()?,
            )),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListInput {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandleInput {
    invocation_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectInput {
    invocation_id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndInput {
    invocation_id: String,
    reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitInput {
    invocation_id: String,
    result: Value,
    #[serde(default)]
    emissions: Vec<EmissionInput>,
    #[serde(default)]
    contents: Vec<ContentId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareInput {
    invocation_id: String,
    #[serde(default = "default_preview")]
    preview_bytes: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CallInput {
    invocation_id: String,
    operation: String,
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    invocation_id: String,
    payload: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum UnsignedInput {
    Decimal(String),
    Number(u64),
}

impl Default for UnsignedInput {
    fn default() -> Self {
        Self::Number(0)
    }
}

impl UnsignedInput {
    fn value(&self) -> Result<u64> {
        match self {
            Self::Decimal(value) => value
                .parse()
                .map_err(|_| AppError::invalid("counter must be an unsigned decimal integer")),
            Self::Number(value) => Ok(*value),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsInput {
    invocation_id: String,
    #[serde(default)]
    after: UnsignedInput,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    invocation_id: String,
    receipt_sequence: UnsignedInput,
    #[serde(default)]
    start: UnsignedInput,
    #[serde(default = "default_read_length")]
    length: usize,
}

fn default_read_length() -> usize {
    65536
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceInput {
    invocation_id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidateInput {
    invocation_id: String,
    payload: Value,
}

pub async fn dispatch(run: &mut ManagedRun, operation: &str, args: &Value) -> Result<Value> {
    match operation {
        "invocation.begin" => {
            let input: BeginInput = parse_args(run, args)?;
            let trigger = input.trigger.compile()?;
            let handle = run
                .live()?
                .session
                .begin_invocation_with_content(
                    input.node_id.as_str(),
                    trigger,
                    input.policy,
                    input.contents,
                )
                .await
                .map_err(context_error)?;
            let value = handle_view(&handle, 0, 100);
            run.live_mut()?
                .invocations
                .insert(handle.id().to_string(), handle);
            Ok(value)
        }
        "invocation.list" => {
            let input: ListInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            let records = match &run.live {
                Some(live) => live
                    .session
                    .invocations_page(
                        input.node_id.as_deref(),
                        input.after.as_deref(),
                        input.limit,
                    )
                    .await
                    .map_err(context_error)?,
                None => ontography::context::read_invocations(
                    run.core_path()?,
                    input.node_id.as_deref(),
                    input.after.as_deref(),
                    input.limit,
                )
                .map_err(context_error)?,
            };
            Ok(
                json!({"invocations":records.iter().map(record_view).collect::<Vec<_>>(),"next_after":records.last().map(|r|r.id.to_string())}),
            )
        }
        "invocation.inspect" => {
            let input: InspectInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            Ok(handle_view(
                handle(run, &input.invocation_id)?,
                input.offset,
                input.limit,
            ))
        }
        "invocation.submit" => {
            let input: SubmitInput = parse_args(run, args)?;
            let result = views::payload(&input.result)?;
            let emissions = input
                .emissions
                .iter()
                .map(EmissionInput::compile)
                .collect::<Result<_>>()?;
            let decision = handle(run, &input.invocation_id)?
                .submit(result, emissions, input.contents)
                .await
                .map_err(context_error)?;
            views::decision(decision)
        }
        "invocation.interrupt" | "invocation.fail" => {
            let input: EndInput = parse_args(run, args)?;
            let invocation = handle(run, &input.invocation_id)?;
            if operation == "invocation.interrupt" {
                invocation
                    .interrupt(&input.reason)
                    .await
                    .map_err(context_error)?;
            } else {
                invocation
                    .fail(&input.reason)
                    .await
                    .map_err(context_error)?;
            }
            Ok(json!({"invocation_id":input.invocation_id,"operation":operation}))
        }
        "invocation.release" => {
            let input: HandleInput = parse_args(run, args)?;
            handle(run, &input.invocation_id)?
                .interrupt("management capability explicitly released")
                .await
                .map_err(context_error)?;
            run.live_mut()?.invocations.remove(&input.invocation_id);
            Ok(json!({"invocation_id":input.invocation_id,"released":true}))
        }
        "context.prepare" => {
            let input: PrepareInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            check_preview(input.preview_bytes)?;
            let contributions = handle(run, &input.invocation_id)?
                .prepare_context()
                .await
                .map_err(context_error)?;
            let mut budget = RESPONSE_PREVIEW_BUDGET;
            let previews=contributions.iter().take(input.limit).map(|contribution|{
                let len=contribution.content.len();
                let shown=len.min(input.preview_bytes).min(budget); budget-=shown;
                json!({"package_id":contribution.package_id.map(|id|id.to_string()),"content_digest":contribution.content_digest.to_string(),"bytes":len.to_string(),"preview":views::bytes(&contribution.content[..shown]),"truncated":shown<len})
            }).collect::<Vec<_>>();
            Ok(
                json!({"invocation_id":input.invocation_id,"contributions":previews,"total_contributions":contributions.len(),"truncated":contributions.len()>input.limit,"receipt_state":"prepared"}),
            )
        }
        "context.call" => {
            let mut input: CallInput = parse_args(run, args)?;
            validate_context_arguments(&input.operation, &mut input.arguments)?;
            let response = handle(run, &input.invocation_id)?
                .call(&input.operation, input.arguments)
                .await
                .map_err(context_error)?;
            response_view(response)
        }
        "context.record" => {
            let input: RecordInput = parse_args(run, args)?;
            let bytes = views::payload(&input.payload)?;
            let response = handle(run, &input.invocation_id)?
                .record_initial_input(bytes)
                .await
                .map_err(context_error)?;
            response_view(response)
        }
        "context.events" => {
            let input: EventsInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            let id = invocation_id(&input.invocation_id)?;
            let after = input.after.value()?;
            let events = match &run.live {
                Some(live) => live
                    .session
                    .invocation_events(id, after, input.limit)
                    .await
                    .map_err(context_error)?,
                None => ontography::context::read_events(run.core_path()?, id, after, input.limit)
                    .map_err(context_error)?,
            };
            let mut shown = Vec::new();
            let mut budget = RESPONSE_PREVIEW_BUDGET;
            let mut next_after = None;
            for event in &events {
                let value = event_view(event);
                let size = serde_json::to_vec(&value)?.len();
                if size > budget && !shown.is_empty() {
                    break;
                }
                budget = budget.saturating_sub(size);
                next_after = Some(event.sequence.to_string());
                shown.push(value);
            }
            Ok(
                json!({"invocation_id":input.invocation_id,"truncated":shown.len()<events.len(),"events":shown,"next_after":next_after}),
            )
        }
        "context.read" => {
            let input: ReadInput = parse_args(run, args)?;
            if input.length > 65536 {
                return Err(AppError::invalid("length must not exceed 65536"));
            }
            let id = invocation_id(&input.invocation_id)?;
            let sequence = input.receipt_sequence.value()?;
            if sequence == 0 {
                return Err(AppError::invalid("receipt_sequence must be positive"));
            }
            let session = &run.live()?.session;
            let event = session
                .invocation_events(id, sequence - 1, 1)
                .await
                .map_err(context_error)?
                .into_iter()
                .next()
                .filter(|event| event.sequence == sequence && event.receipt_sequence == sequence)
                .ok_or_else(|| AppError::new("not_found", "prepared receipt does not exist"))?;
            let start = input.start.value()?;
            if start > event.bytes {
                return Err(AppError::invalid(
                    "start is beyond the receipt's byte length",
                ));
            }
            let end = start.saturating_add(input.length as u64).min(event.bytes);
            let bytes = session
                .invocation_content(id, sequence, start..end)
                .await
                .map_err(context_error)?
                .ok_or_else(|| AppError::new("not_found", "receipt content is unavailable"))?;
            Ok(
                json!({"invocation_id":input.invocation_id,"receipt_sequence":sequence.to_string(),"start":start.to_string(),"end":end.to_string(),"total_bytes":event.bytes.to_string(),"content":views::bytes(&bytes)}),
            )
        }
        "context.workspace" => {
            let input: WorkspaceInput = parse_args(run, args)?;
            check_limit(input.limit)?;
            let (capability, package) = handle(run, &input.invocation_id)?
                .workspace_package()
                .await
                .map_err(context_error)?;
            Ok(
                json!({"invocation_id":input.invocation_id,"handle":capability,"root":package.root(),"entries":package.entries().iter().skip(input.offset).take(input.limit).collect::<Vec<_>>(),"total_entries":package.entries().len(),"offset":input.offset,"checkout_prepared":false}),
            )
        }
        "context.validate_output" => {
            let input: ValidateInput = parse_args(run, args)?;
            let payload = views::payload(&input.payload)?;
            let dependencies = handle(run, &input.invocation_id)?
                .validate_worker_output(&payload)
                .await
                .map_err(context_error)?;
            Ok(json!({"invocation_id":input.invocation_id,"contents":dependencies}))
        }
        _ => Err(AppError::new("unknown_operation", operation)),
    }
}

fn handle<'a>(run: &'a ManagedRun, id: &str) -> Result<&'a InvocationHandle> {
    run.live()?.invocations.get(id).ok_or_else(||AppError::new("unknown_handle","invocation capability is absent from this run or expired; inspect persisted records with invocation.list"))
}

fn invocation_id(value: &str) -> Result<InvocationId> {
    value
        .parse()
        .map_err(|_| AppError::invalid("invocation_id must be a UUID"))
}

fn handle_view(handle: &InvocationHandle, offset: usize, limit: usize) -> Value {
    let descriptors = handle.tool_descriptors();
    let packages = descriptors["packages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let members = descriptors["members"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    json!({"invocation_id":handle.id().to_string(),"node_id":handle.node_id(),"policy":handle.policy(),"offset":offset,
        "capabilities":{"packages":packages.iter().skip(offset).take(limit).collect::<Vec<_>>(),"members":members.iter().skip(offset).take(limit).collect::<Vec<_>>()},
        "total_packages":packages.len(),"total_members":members.len(),
        "package_occurrences":handle.packages().iter().skip(offset).take(limit).map(|grant|json!({"handle":grant.handle,"package_id":grant.package_id.to_string(),"content_digest":grant.content_digest.to_string(),"received":grant.received})).collect::<Vec<_>>()})
}

fn record_view(record: &InvocationRecord) -> Value {
    let activation = record.activation_id.as_deref().map(|id| {
        u128::from_str_radix(id, 16)
            .map(|id| ontography::ActivationId::from_u128(id).to_string())
            .unwrap_or_else(|_| id.to_owned())
    });
    json!({"invocation_id":record.id.to_string(),"node_id":record.node_id,"status":record.status,"policy":record.policy,
        "activation_id":activation,"detail":record.detail,"returned_bytes":record.returned_bytes.to_string(),"package_count":record.packages.len(),"member_count":record.members.len()})
}

fn event_view(event: &ContextEvent) -> Value {
    json!({"invocation_id":event.invocation_id.to_string(),"sequence":event.sequence.to_string(),"receipt_sequence":event.receipt_sequence.to_string(),"state":event.state,
        "operation":event.operation,"content_digest":event.content_digest.to_string(),"bytes":event.bytes.to_string(),"source":event.source})
}

fn response_view(response: ContextResponse) -> Result<Value> {
    let fits = serde_json::to_vec(&response.value)?.len() <= RESPONSE_PREVIEW_BUDGET;
    Ok(
        json!({"receipt_sequence":response.sequence.to_string(),"receipt_state":"prepared","value":if fits{Some(response.value)}else{None},"truncated":!fits}),
    )
}

fn validate_context_arguments(operation: &str, args: &mut Value) -> Result<()> {
    let fields = args
        .as_object_mut()
        .ok_or_else(|| AppError::invalid("context arguments must be an object"))?;
    let allowed: &[&str] = match operation {
        "package.describe" | "package.parents" | "package.list" => &["handle"],
        "package.read" => &["handle", "start", "end"],
        _ => return Err(AppError::invalid("unknown context operation")),
    };
    if fields.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(AppError::invalid("unknown context argument"));
    }
    if !fields.get("handle").is_some_and(Value::is_string) {
        return Err(AppError::invalid("context handle must be a string"));
    }
    for key in ["start", "end"] {
        if let Some(value) = fields.get_mut(key) {
            let parsed: UnsignedInput = serde_json::from_value(value.clone())?;
            *value = json!(parsed.value()?);
        }
    }
    Ok(())
}

fn context_error(error: ContextError) -> AppError {
    let code = match &error {
        ContextError::Denied(_) => "context_denied",
        ContextError::Budget(_) => "context_budget",
        ContextError::Closed => "invocation_closed",
        ContextError::NotFound => "not_found",
        ContextError::Storage(_) => "storage_error",
        ContextError::Submit(_) => "core_error",
    };
    AppError::new(code, error.to_string())
}

/// A delivery boundary observed by a trusted worker transport, never asserted
/// by a model tool. Client management traffic is not a worker acknowledgement.
pub enum ObservedDelivery {
    Sent,
    Acknowledged,
}

/// Call only after the trusted worker transport observes the corresponding
/// boundary. Intentionally absent from the agent operation catalog.
pub async fn record_delivery(
    handle: &InvocationHandle,
    sequence: u64,
    event: ObservedDelivery,
) -> Result<()> {
    match event {
        ObservedDelivery::Sent => handle.mark_sent(sequence).await,
        ObservedDelivery::Acknowledged => handle.acknowledge(sequence).await,
    }
    .map_err(context_error)
}

/// Record an actual response observed by a trusted tool adapter. A management
/// agent cannot claim that an arbitrary named worker tool produced these bytes.
pub async fn record_tool_response(
    handle: &InvocationHandle,
    tool: &str,
    payload: Payload,
) -> Result<ContextResponse> {
    handle
        .record_tool_response(tool, payload)
        .await
        .map_err(context_error)
}

fn counter_schema() -> Value {
    json!({"anyOf":[{"type":"string","pattern":"^[0-9]+$"},{"type":"integer","minimum":0,"maximum":9007199254740991u64}]})
}

fn policy_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "mode":{"type":"string","enum":["prepared","explorable"]},"initial":{"type":"string","enum":["none","received","ancestry"]},
        "ancestor_metadata":{"type":"boolean"},"ancestor_payloads":{"type":"boolean"},
        "max_packages":{"type":"integer","minimum":1},"max_members":{"type":"integer","minimum":1},"max_bytes":{"type":"integer","minimum":1},"max_events":{"type":"integer","minimum":1},
        "workspace":{"anyOf":[{"type":"null"},{"type":"object","additionalProperties":false,"properties":{"input_edge":{"type":["string","null"]},"writable":{"type":"boolean"},"output_edge":{"type":["string","null"]}}}]}}})
}

pub fn operations() -> Vec<Operation> {
    let id = json!({"type":"string"});
    let limit = json!({"type":"integer","minimum":1,"maximum":1000});
    let trigger = json!({"oneOf":[
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"root"},"authority":{"type":"array","items":{"type":"string"}},"input":payload_schema()},"required":["kind","authority","input"]},
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"packages"},"package_ids":{"type":"array","items":{"type":"string"}}},"required":["kind","package_ids"]}]});
    vec![
        operation(
            "invocation.begin",
            "Issue and retain a capability bound to a node and exact root input or received packages. Does not consume its trigger.",
            json!({"node_id":id,"trigger":trigger,"policy":policy_schema(),"contents":{"type":"array","items":content_id_schema()}}),
            &["node_id", "trigger"],
            true,
        ),
        operation(
            "invocation.list",
            "Page durable invocation summaries, including suspended runs. Grant bodies are exposed through retained capability inspection.",
            json!({"node_id":id,"after":id,"limit":limit}),
            &[],
            false,
        ),
        operation(
            "invocation.inspect",
            "Describe a retained capability and page its package/member catalogs. Use invocation.list for durable lifecycle status.",
            json!({"invocation_id":id,"offset":{"type":"integer","minimum":0},"limit":limit}),
            &["invocation_id"],
            false,
        ),
        operation(
            "invocation.submit",
            "Publish using the invocation's bound trigger. Identical accepted retries return the original activation; changed retries reject.",
            json!({"invocation_id":id,"result":payload_schema(),"emissions":emissions_schema(),"contents":{"type":"array","items":content_id_schema()}}),
            &["invocation_id", "result"],
            true,
        ),
        operation(
            "invocation.interrupt",
            "End unfinished invocation work while retaining its context evidence.",
            json!({"invocation_id":id,"reason":id}),
            &["invocation_id", "reason"],
            true,
        ),
        operation(
            "invocation.fail",
            "Record an operational failure for unfinished invocation work.",
            json!({"invocation_id":id,"reason":id}),
            &["invocation_id", "reason"],
            true,
        ),
        operation(
            "invocation.release",
            "Release the server's capability; explicitly interrupt it first if unfinished. Persistent history remains.",
            json!({"invocation_id":id}),
            &["invocation_id"],
            true,
        ),
        operation(
            "context.prepare",
            "Prepare initial payload receipts and return bounded contribution previews. Read remaining receipt bytes with context.events/context.read.",
            json!({"invocation_id":id,"preview_bytes":{"type":"integer","minimum":0,"maximum":65536},"limit":limit}),
            &["invocation_id"],
            true,
        ),
        operation(
            "context.call",
            "Perform a granted package.describe, package.parents, package.list, or package.read operation. Records prepared evidence and enforces context budgets.",
            json!({"invocation_id":id,"operation":{"type":"string","enum":["package.describe","package.parents","package.list","package.read"]},"arguments":{"type":"object","additionalProperties":false,"properties":{"handle":id,"start":counter_schema(),"end":counter_schema()},"required":["handle"]}}),
            &["invocation_id", "operation", "arguments"],
            true,
        ),
        operation(
            "context.record",
            "Retain supplied dispatch bytes as a prepared receipt. This does not claim they were sent to or acknowledged by a worker.",
            json!({"invocation_id":id,"payload":payload_schema()}),
            &["invocation_id", "payload"],
            true,
        ),
        operation(
            "context.events",
            "Page durable receipt metadata by invocation-local sequence, including suspended runs. Sequences are decimal strings.",
            json!({"invocation_id":id,"after":counter_schema(),"limit":limit}),
            &["invocation_id"],
            false,
        ),
        operation(
            "context.read",
            "Read a bounded byte range from a prepared receipt. This is trusted inspection and issues no worker read grant.",
            json!({"invocation_id":id,"receipt_sequence":counter_schema(),"start":counter_schema(),"length":{"type":"integer","minimum":0,"maximum":65536}}),
            &["invocation_id", "receipt_sequence"],
            false,
        ),
        operation(
            "context.workspace",
            "Resolve the invocation's selected collection and page its entries; this neither creates a checkout nor claims filesystem exposure.",
            json!({"invocation_id":id,"offset":{"type":"integer","minimum":0},"limit":limit}),
            &["invocation_id"],
            false,
        ),
        operation(
            "context.validate_output",
            "Check whether a worker output names only granted package content; return its required dependencies. Denials create audit evidence.",
            json!({"invocation_id":id,"payload":payload_schema()}),
            &["invocation_id", "payload"],
            true,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::workflow;

    #[tokio::test]
    async fn bound_publication_retries_once_and_enforces_capabilities() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        workflow::dispatch(&mut run,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":["work"]},"result":"sent","emissions":[{"edge_id":"A_to_B","payload":"hello"}]})).await.unwrap();
        let frontier = workflow::dispatch(&mut run, "inspect.frontier", &json!({"run_id":run_id}))
            .await
            .unwrap();
        let package = frontier["packages"][0]["package_id"].clone();
        let issued=dispatch(&mut run,"invocation.begin",&json!({"run_id":run_id,"node_id":"B","trigger":{"kind":"packages","package_ids":[package]},"policy":{"mode":"explorable"}})).await.unwrap();
        let invocation = issued["invocation_id"].clone();
        let capability = issued["capabilities"]["packages"][0]["handle"].clone();
        let response=dispatch(&mut run,"context.call",&json!({"run_id":run_id,"invocation_id":invocation,"operation":"package.read","arguments":{"handle":capability,"start":"0","end":"5"}})).await.unwrap();
        assert_eq!(response["value"]["text"], "hello");
        assert_eq!(response["receipt_state"], "prepared");
        let unknown=dispatch(&mut run,"context.call",&json!({"run_id":run_id,"invocation_id":invocation,"operation":"package.read","arguments":{"handle":"other-invocation-handle"}})).await.unwrap_err();
        assert_eq!(unknown.code, "not_found");
        let submit = json!({"run_id":run_id,"invocation_id":invocation,"result":"done"});
        let first = dispatch(&mut run, "invocation.submit", &submit)
            .await
            .unwrap();
        let retry = dispatch(&mut run, "invocation.submit", &submit)
            .await
            .unwrap();
        assert_eq!(first, retry);
        let mut changed = submit;
        changed["result"] = json!("different");
        assert!(
            dispatch(&mut run, "invocation.submit", &changed)
                .await
                .is_err()
        );
        assert_eq!(
            run.live()
                .unwrap()
                .session
                .try_snapshot()
                .await
                .unwrap()
                .state()
                .activations()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn receipt_advancement_requires_trusted_observation_and_preserves_exact_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let issued=dispatch(&mut run,"invocation.begin",&json!({"run_id":run_id,"node_id":"A","trigger":{"kind":"root","authority":[],"input":"task"}})).await.unwrap();
        let invocation = issued["invocation_id"].as_str().unwrap().to_owned();
        let prepared = dispatch(
            &mut run,
            "context.record",
            &json!({"run_id":run_id,"invocation_id":invocation,"payload":"exact dispatch"}),
        )
        .await
        .unwrap();
        let sequence: u64 = prepared["receipt_sequence"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            record_delivery(
                handle(&run, &invocation).unwrap(),
                sequence,
                ObservedDelivery::Acknowledged
            )
            .await
            .is_err()
        );
        record_delivery(
            handle(&run, &invocation).unwrap(),
            sequence,
            ObservedDelivery::Sent,
        )
        .await
        .unwrap();
        record_delivery(
            handle(&run, &invocation).unwrap(),
            sequence,
            ObservedDelivery::Acknowledged,
        )
        .await
        .unwrap();
        let read=dispatch(&mut run,"context.read",&json!({"run_id":run_id,"invocation_id":invocation,"receipt_sequence":sequence.to_string()})).await.unwrap();
        assert_eq!(read["content"]["text"], "exact dispatch");
        let events = dispatch(
            &mut run,
            "context.events",
            &json!({"run_id":run_id,"invocation_id":invocation}),
        )
        .await
        .unwrap();
        assert_eq!(events["events"].as_array().unwrap().len(), 3);
        assert_eq!(events["events"][2]["state"], "acknowledged");
        let observed = record_tool_response(
            handle(&run, &invocation).unwrap(),
            "native.read",
            views::payload(&json!("exact tool response")).unwrap(),
        )
        .await
        .unwrap();
        let read = dispatch(&mut run,"context.read",&json!({"run_id":run_id,"invocation_id":invocation,"receipt_sequence":observed.sequence.to_string()})).await.unwrap();
        assert_eq!(read["content"]["text"], "exact tool response");
        let last = dispatch(&mut run,"context.events",&json!({"run_id":run_id,"invocation_id":invocation,"after":(observed.sequence-1).to_string()})).await.unwrap();
        assert_eq!(last["events"][0]["state"], "prepared");
        assert_eq!(last["events"][0]["operation"], "tool_response");
        assert_eq!(last["events"][0]["source"]["tool"], "native.read");
        assert!(
            operations()
                .iter()
                .all(|op| !op.name.contains("acknowledge") && !op.name.contains("mark_sent"))
        );
    }

    #[tokio::test]
    async fn context_budget_and_suspended_metadata_are_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let run_id = run.manifest.run_id.clone();
        let issued=dispatch(&mut run,"invocation.begin",&json!({"run_id":run_id,"node_id":"A","trigger":{"kind":"root","authority":[],"input":"hello"},"policy":{"mode":"explorable","max_bytes":5}})).await.unwrap();
        let invocation = issued["invocation_id"].clone();
        let capability = issued["capabilities"]["packages"][0]["handle"].clone();
        let error=dispatch(&mut run,"context.call",&json!({"run_id":run_id,"invocation_id":invocation,"operation":"package.read","arguments":{"handle":capability}})).await.unwrap_err();
        assert_eq!(error.code, "context_budget");
        run.suspend(false).await.unwrap();
        let history = dispatch(&mut run, "invocation.list", &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_eq!(history["invocations"][0]["status"], "interrupted");
        let events = dispatch(
            &mut run,
            "context.events",
            &json!({"run_id":run_id,"invocation_id":invocation}),
        )
        .await
        .unwrap();
        assert!(!events["events"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_real_capability_cannot_cross_invocation_or_run_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let other_directory = tempfile::tempdir().unwrap();
        let mut run = workflow::tests::test_run(directory.path());
        let mut other_run = workflow::tests::test_run(other_directory.path());
        let run_id = run.manifest.run_id.clone();
        let first = dispatch(&mut run,"invocation.begin",&json!({"run_id":run_id,"node_id":"A","trigger":{"kind":"root","authority":[],"input":"private first input"},"policy":{"mode":"explorable"}})).await.unwrap();
        let second = dispatch(&mut run,"invocation.begin",&json!({"run_id":run_id,"node_id":"A","trigger":{"kind":"root","authority":[],"input":"second input"},"policy":{"mode":"explorable"}})).await.unwrap();
        let capability = first["capabilities"]["packages"][0]["handle"].clone();
        assert!(capability.is_string());
        let request = json!({"run_id":run_id,"invocation_id":second["invocation_id"],"operation":"package.read","arguments":{"handle":capability}});
        assert_eq!(
            dispatch(&mut run, "context.call", &request)
                .await
                .unwrap_err()
                .code,
            "not_found"
        );
        let mut request = request;
        request["run_id"] = json!(other_run.manifest.run_id);
        request["invocation_id"] = first["invocation_id"].clone();
        let error = dispatch(&mut other_run, "context.call", &request)
            .await
            .unwrap_err();
        assert_eq!(error.code, "unknown_handle");
        let valid = dispatch(&mut run,"context.call",&json!({"run_id":run_id,"invocation_id":first["invocation_id"],"operation":"package.read","arguments":{"handle":capability}})).await.unwrap();
        assert_eq!(valid["value"]["text"], "private first input");
    }
}
