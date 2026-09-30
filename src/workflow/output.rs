//! Select task inputs and node results without confusing scheduling order with
//! result order. Inbox items are paged by stable identity, never called latest.

use super::{Implementation, WorkflowPayload, edit::WorkflowState, runtime, tasks, tools};
use crate::{AppError, Result, persistence, state::ManagedRun, views};
use ontography::{PackageId, PackageRecord};
use serde_json::{Value, json};

pub async fn inspect(run: &ManagedRun, state: &WorkflowState, args: &Value) -> Result<Value> {
    let mut record = read(run, state, args).await?;
    if let Some(object) = record.as_object_mut() {
        object.remove("invocation_id");
        object.remove("activation_id");
    }
    for field in ["result", "input"] {
        if let Some(value) = record.get(field) {
            record[field] = tools::payload_view(value);
        }
    }
    if let Some(items) = record.get_mut("items").and_then(Value::as_array_mut) {
        for item in items {
            item["input"] = tools::payload_view(&item["input"]);
        }
    }
    for field in ["stdout", "stderr"] {
        if let Some(value) = record.get(field).and_then(Value::as_str) {
            let preview: String = value.chars().take(8192).collect();
            if preview.len() < value.len() {
                record["truncated"] = json!(true);
            }
            record[field] = json!(preview);
        }
    }
    Ok(record)
}

pub async fn payload(
    run: &ManagedRun,
    state: &WorkflowState,
    args: &Value,
    workspace_only: bool,
) -> Result<WorkflowPayload> {
    let record = read(run, state, args).await?;
    if record.get("result").is_some() && record["publication_status"] != "committed" {
        return Err(AppError::new(
            "output_not_committed",
            "This worker output has not been accepted; inspect or resume the task",
        ));
    }
    let value = record
        .get("result")
        .or_else(|| record.get("input"))
        .filter(|value| !value.is_null());
    let Some(value) = value else {
        if record["items"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
        {
            return Err(AppError::new(
                "selection_required",
                "Choose a work_id from flow.output before opening or exporting an inbox item",
            ));
        }
        return Err(AppError::new(
            "no_output",
            "This node has no selected output or input",
        ));
    };
    if let Some(values) = value.as_array() {
        // Opening the one workspace in a joined task does not discard its
        // accompanying messages. Export of an individual input needs its ID.
        if workspace_only {
            let mut workspaces = values
                .iter()
                .filter(|value| value.get("ontography_package").is_some());
            if let Some(workspace) = workspaces.next()
                && workspaces.next().is_none()
            {
                return WorkflowPayload::from_value(workspace);
            }
        }
        return Err(AppError::new(
            "selection_required",
            "Choose a work_id from the task's work_ids to select one joined input",
        ));
    }
    WorkflowPayload::from_value(value)
}

async fn read(run: &ManagedRun, state: &WorkflowState, args: &Value) -> Result<Value> {
    let node = views::field(args, "node")?;
    let implementation = &state.binding(node)?.implementation;
    let human = matches!(implementation, Implementation::Human(_));
    let inbox = matches!(
        implementation,
        Implementation::Inbox(_) | Implementation::External(_)
    );
    let id = &state.identities.nodes[node];
    let source = tools::optional_str(args, "source")?.unwrap_or("auto");
    if !matches!(source, "auto" | "output" | "pending") {
        return Err(AppError::invalid("source must be auto, output, or pending"));
    }
    let task_id = tools::optional_str(args, "task_id")?;
    let work_id = tools::optional_str(args, "work_id")?;
    let paging = ["after", "revision", "limit"]
        .iter()
        .any(|name| args.get(name).is_some());
    if (source == "output" && (task_id.is_some() || work_id.is_some() || paging))
        || (paging && (task_id.is_some() || work_id.is_some()))
    {
        return Err(AppError::invalid(
            "Select a result, a task/item, or a pending page; do not combine those selectors",
        ));
    }
    if source == "auto" && (human || inbox) {
        // Without current core custody, a cached decision cannot be presented
        // as the input of a possibly newer review task.
        run.live()?;
    }
    if let Some(expected) = task_id {
        let candidate = tools::node_tasks(run, state, node)
            .await?
            .into_iter()
            .find(|candidate| candidate.task.key.as_str() == expected)
            .ok_or_else(|| {
                AppError::new(
                    "stale_task",
                    "This task is no longer ready; read status again",
                )
            })?;
        let task = tools::task_view(run, candidate).await?;
        if let Some(work) = work_id {
            let index = task
                .work_ids
                .iter()
                .position(|id| id == work)
                .ok_or_else(|| {
                    AppError::new(
                        "stale_work",
                        "This work_id is not an input of the selected task",
                    )
                })?;
            let input = if task.work_ids.len() == 1 {
                task.raw_input.clone()
            } else {
                task.raw_input[index].clone()
            };
            return Ok(
                json!({"node":node,"task_id":expected,"work_id":work,"input":input,"publication_status":"waiting"}),
            );
        }
        return Ok(task_record(task));
    }
    if let Some(work) = work_id {
        let (_, package, _) = find_work(run, id, work, None).await?;
        return Ok(
            json!({"node":node,"work_id":work,"input":input(run, &package).await?,"publication_status":"waiting"}),
        );
    }
    if source != "output" {
        // A worker's pending initial input shows even while it fails; it is
        // not a package, so the pending page cannot list it.
        if !paging
            && (human || source == "pending")
            && let Some(candidate) = tools::node_tasks(run, state, node)
                .await?
                .into_iter()
                .find(|candidate| human || candidate.task.is_initial())
        {
            return Ok(task_record(tools::task_view(run, candidate).await?));
        }
        if inbox || source == "pending" || paging {
            let page = pending(run, node, id, args).await?;
            if source == "pending"
                || paging
                || page["items"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty())
            {
                return Ok(page);
            }
        }
    }
    if let Some(record) = cached(run, node, id).await? {
        return Ok(record);
    }
    if source != "output"
        && let Some(candidate) = tools::node_tasks(run, state, node)
            .await?
            .into_iter()
            .next()
    {
        return Ok(task_record(tools::task_view(run, candidate).await?));
    }
    Ok(json!({"node":node,"input":null,"publication_status":"waiting"}))
}

fn task_record(task: tools::TaskView) -> Value {
    json!({"node":task.node,"task_id":task.task_id,"work_ids":task.work_ids,"input":task.raw_input,"publication_status":"waiting"})
}

pub(super) async fn input(run: &ManagedRun, package: &PackageRecord) -> Result<Value> {
    let bytes = run
        .live()?
        .session
        .content(package.content_digest())
        .await
        .map_err(AppError::core)?
        .ok_or_else(|| AppError::new("missing_content", "Task input is unavailable"))?;
    Ok(WorkflowPayload::read(&bytes)?.to_value())
}

fn require_revision(actual: u64, expected: Option<u64>) -> Result<()> {
    if expected.is_some_and(|expected| actual != expected) {
        return Err(AppError::new(
            "stale_page",
            "Pending work changed; restart listing from the first page",
        ));
    }
    Ok(())
}

/// Hash handles never expose package identities. Resolve them with bounded
/// core pages, fencing the scan so a changing frontier cannot silently skip work.
async fn find_work(
    run: &ManagedRun,
    node: &str,
    work: &str,
    mut revision: Option<u64>,
) -> Result<(PackageId, PackageRecord, u64)> {
    tasks::parse_work_id(work)?;
    let session = &run.live()?.session;
    let mut after = None;
    loop {
        let page = session
            .pending_page_at(node, after, 100)
            .await
            .map_err(AppError::core)?;
        require_revision(page.revision(), revision)?;
        revision = Some(page.revision());
        if let Some((id, package)) = page
            .packages()
            .iter()
            .find(|(id, _)| tasks::work_id(id) == work)
        {
            return Ok((*id, package.clone(), page.revision()));
        }
        if page.packages().len() < 100 {
            return Err(AppError::new(
                "stale_work",
                "This item is no longer pending at this node; read flow.output again",
            ));
        }
        after = page.packages().last().map(|(id, _)| *id);
    }
}

async fn pending(run: &ManagedRun, node: &str, id: &str, args: &Value) -> Result<Value> {
    let after = tools::optional_str(args, "after")?;
    let revision = tools::optional_str(args, "revision")?
        .map(|text| {
            text.parse::<u64>()
                .map_err(|_| AppError::invalid("revision must be an unsigned decimal string"))
        })
        .transpose()?;
    if after.is_some() && revision.is_none() {
        return Err(AppError::invalid("Pass the page revision with after"));
    }
    let limit = match args.get("limit") {
        None => 20,
        Some(value) => value
            .as_u64()
            .filter(|limit| (1..=100).contains(limit))
            .ok_or_else(|| AppError::invalid("limit must be between 1 and 100"))?
            as usize,
    };
    let cursor = match after {
        Some(work) => Some(find_work(run, id, work, revision).await?.0),
        None => None,
    };
    let page = run
        .live()?
        .session
        .pending_page_at(id, cursor, limit + 1)
        .await
        .map_err(AppError::core)?;
    require_revision(page.revision(), revision)?;
    let more = page.packages().len() > limit;
    let single = after.is_none() && !more && page.packages().len() == 1;
    let mut items = Vec::new();
    for (package_id, package) in page.packages().iter().take(limit) {
        let input = input(run, package).await?;
        let input = if single {
            input
        } else {
            tools::payload_view(&input)
        };
        items.push(json!({"work_id":tasks::work_id(package_id),"input":input}));
    }
    let next_after = more.then(|| items.last().expect("nonempty page")["work_id"].clone());
    let mut result = json!({"node":node,"publication_status":"waiting","revision":page.revision().to_string(),"items":items,"next_after":next_after});
    // Preserve the single-item convenience, but never implicitly choose one
    // from a larger inbox or from a later page.
    if single {
        result["input"] = result["items"][0]["input"].clone();
        result["work_id"] = result["items"][0]["work_id"].clone();
    }
    Ok(result)
}

async fn cached(run: &ManagedRun, node: &str, id: &str) -> Result<Option<Value>> {
    let path = runtime::node_directory(run, id).join("output.json");
    if !path.exists() {
        return Ok(None);
    }
    let mut output: Value = persistence::read_json(&path)?;
    output["node"] = json!(node);
    if output["publication_status"] == "prepared" && run.live.is_some() {
        let mut after = None;
        loop {
            let page = run
                .live()?
                .session
                .invocations_page(Some(id), after.as_deref(), 100)
                .await
                .map_err(AppError::core)?;
            if let Some(record) = page
                .iter()
                .find(|record| output["invocation_id"] == record.id.to_string())
            {
                output["publication_status"] = json!(match record.status {
                    ontography::InvocationStatus::Accepted => "committed",
                    ontography::InvocationStatus::Open => "prepared",
                    _ => "failed",
                });
                break;
            }
            if page.len() < 100 {
                break;
            }
            after = page.last().map(|record| record.id.to_string());
        }
    }
    Ok(Some(output))
}
