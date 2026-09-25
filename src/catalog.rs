use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone, Debug, Serialize)]
pub struct Operation {
    pub name: String,
    pub group: String,
    pub description: String,
    pub parameters: Value,
    pub mutating: bool,
}

impl Operation {
    pub fn new(
        name: &str,
        description: &str,
        properties: Value,
        required: &[&str],
        mutating: bool,
    ) -> Self {
        Self {
            name: name.into(),
            group: name.split('.').next().unwrap_or(name).into(),
            description: description.into(),
            parameters: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
            mutating,
        }
    }
}

pub fn operations() -> &'static [Operation] {
    static OPERATIONS: std::sync::OnceLock<Vec<Operation>> = std::sync::OnceLock::new();
    OPERATIONS.get_or_init(build_operations)
}

fn build_operations() -> Vec<Operation> {
    let text = json!({"type":"string"});
    let declaration = schema::<crate::declarations::GraphDeclaration>();
    let rewrite = schema::<crate::declarations::RewriteRequestDeclaration>();
    let mut ops = vec![
        Operation::new(
            "system.hello",
            "Inspect server identity, versions, and available tool schemas.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "system.status",
            "Inspect server state and retained runs.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "catalog.list",
            "Discover trusted validators and available executable implementations.",
            json!({}),
            &[],
            false,
        ),
        Operation::new(
            "operation.get",
            "Recover an accepted request outcome in this server instance; never resubmit an uncertain mutation.",
            json!({"client_id":text,"request_id":text}),
            &["client_id", "request_id"],
            false,
        ),
        Operation::new(
            "graph.validate",
            "Validate a logical graph and configured rewrite grammar using core admission.",
            json!({"declaration":declaration}),
            &["declaration"],
            false,
        ),
        Operation::new(
            "graph.save",
            "Save a versioned declaration draft. Admission happens through graph.validate or run.start.",
            json!({"declaration":declaration}),
            &["declaration"],
            true,
        ),
        Operation::new(
            "graph.list",
            "Page saved graph declaration revisions in revision order.",
            json!({"after":text,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &[],
            false,
        ),
        Operation::new(
            "graph.get",
            "Read a saved graph declaration by its revision hash.",
            json!({"revision":text}),
            &["revision"],
            false,
        ),
        Operation::new(
            "graph.import",
            "Import a declaration file as a saved draft. Relative paths use the explicitly named absolute project directory.",
            json!({"path":text,"project":text}),
            &["path", "project"],
            true,
        ),
        Operation::new(
            "graph.export",
            "Export an exact saved declaration revision to a file.",
            json!({"revision":text,"path":text,"project":text}),
            &["revision", "path", "project"],
            true,
        ),
        Operation::new(
            "run.start",
            "Create a persistent core run from a declaration or saved revision. An unbound logical graph starts idle. Supply the absolute project directory.",
            json!({"declaration":declaration,"revision":text,"project":text}),
            &["project"],
            true,
        ),
        Operation::new(
            "run.list",
            "Page live, suspended, and recoverable durable runs in run identity order.",
            json!({"after":text,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &[],
            false,
        ),
        Operation::new(
            "run.inspect",
            "Read current graph, bounded frontier, actual execution state, and owned resources.",
            json!({"run_id":text,"limit":{"type":"integer","minimum":1,"maximum":1000}}),
            &["run_id"],
            false,
        ),
        Operation::new(
            "run.resume",
            "Reopen the stored current graph under its exact original definition and grammar.",
            json!({"run_id":text}),
            &["run_id"],
            true,
        ),
        Operation::new(
            "run.suspend",
            "Stop executables, checkpoint workspaces, and release the run while preserving resumability.",
            json!({"run_id":text}),
            &["run_id"],
            true,
        ),
        Operation::new(
            "run.close",
            "Terminally close only the selected run, preserving readable durable history.",
            json!({"run_id":text}),
            &["run_id"],
            true,
        ),
        Operation::new(
            "rewrite.list",
            "List the fixed rewrite productions configured for this run.",
            json!({"run_id":text}),
            &["run_id"],
            false,
        ),
        Operation::new(
            "rewrite.prepare",
            "Prepare a configured rewrite and report its exact graph and package retirements without changing the run.",
            json!({"run_id":text,"request":rewrite}),
            &["run_id", "request"],
            true,
        ),
        Operation::new(
            "rewrite.inspect",
            "Read a retained prepared rewrite and its predecessor revision.",
            json!({"run_id":text,"plan_id":text}),
            &["run_id", "plan_id"],
            false,
        ),
        Operation::new(
            "rewrite.commit",
            "Commit a retained rewrite through core; stale or foreign plans reject.",
            json!({"run_id":text,"plan_id":text}),
            &["run_id", "plan_id"],
            true,
        ),
        Operation::new(
            "rewrite.discard",
            "Release a prepared rewrite without applying it.",
            json!({"run_id":text,"plan_id":text}),
            &["run_id", "plan_id"],
            true,
        ),
    ];
    ops.extend(crate::tools::operations());
    ops
}

pub fn schema<T: schemars::JsonSchema>() -> Value {
    let mut settings = schemars::generate::SchemaSettings::draft2020_12();
    settings.inline_subschemas = true;
    let mut value = settings
        .into_generator()
        .into_root_schema_for::<T>()
        .to_value();
    if let Some(object) = value.as_object_mut() {
        object.remove("$schema");
    }
    value
}
