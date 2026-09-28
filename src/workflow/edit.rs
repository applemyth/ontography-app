//! Edits to a running workflow, each one explicit core graph edit.
//!
//! The caller serializes edits for a run; workers keep submitting throughout.
//! An accepted edit is saved with every fresh identity before core changes,
//! and core's graph records whether it applied: recovery computes the edit
//! again and finds either the whole edit or nothing left to do.

use super::components::{Binding, Bindings, BoundNode, Catalog};
use super::document::{
    AUTHORITY, CONTRACT, Document, EDGE_TYPE, IdentityMap, Typing, edge_key, root,
};
use crate::declarations::GraphFragmentDeclaration;
use crate::{AppError, Result, persistence};
use ontography::{
    AuthorityMatch, AuthorityTag, EdgeDefinition, EditContext, EditPolicy, GraphEdit, IngressMode,
    Kernel, PolicyDenial, Principal, RetirementReason, RewriteError, RewriteRequest, RootRule,
    SessionHandle,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

const MAX_STALE_RETRIES: usize = 8;

/// The principal the workflow editor edits a run's graph as.
const EDITOR: &str = "workflow";

/// Which edits a workflow run accepts: only its editor's, and only ones that
/// keep it a workflow, with workflow nodes and connections, one entry, and no
/// authority changes.
pub fn policy() -> Arc<dyn EditPolicy> {
    Arc::new(workflow_edit)
}

fn workflow_edit(context: &EditContext<'_>) -> std::result::Result<(), PolicyDenial> {
    let deny = |reason: &str| Err(PolicyDenial::new(reason));
    if context.principal.name() != EDITOR {
        return deny("change this run through its workflow document");
    }
    let add = context.edit.add();
    if !add.authority_transitions().is_empty() {
        return deny("workflow edits cannot change authority");
    }
    if add
        .node_definitions()
        .iter()
        .any(|node| node.result_contract() != CONTRACT)
    {
        return deny("workflow nodes produce workflow payloads");
    }
    if !add.edge_definitions().iter().all(is_connection) {
        return deny("workflow connections carry workflow payloads under workflow authority");
    }
    let workflow_authority =
        |root: &RootRule| root.ceiling().tags().map(AuthorityTag::id).eq([AUTHORITY]);
    if !add.roots().iter().all(workflow_authority) || context.after.roots().len() != 1 {
        return deny("a workflow has exactly one entry, with workflow authority");
    }
    Ok(())
}

fn is_connection(edge: &EdgeDefinition) -> bool {
    edge.types().iter().map(AsRef::as_ref).eq([EDGE_TYPE])
        && edge.package_contract() == CONTRACT
        && edge
            .authority_tags()
            .iter()
            .map(AuthorityTag::id)
            .eq([AUTHORITY])
        && edge.authority_match() == AuthorityMatch::AnyOf
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowState {
    pub version: u64,
    pub current: Document,
    /// What each node of `current` is bound to.
    #[serde(default)]
    pub bindings: Bindings,
    pub identities: IdentityMap,
    pub pending: Option<Plan>,
}

impl WorkflowState {
    pub fn new(current: Document, bindings: Bindings, identities: IdentityMap) -> Result<Self> {
        Ok(Self {
            version: 1,
            current: current.canonicalized()?,
            bindings,
            identities,
            pending: None,
        })
    }

    /// State saved before components had no bindings. Its documents could
    /// only place built-in components, which bind the same way today.
    pub fn with_bindings(mut self) -> Result<Self> {
        if self.bindings.is_empty() {
            self.bindings = Catalog::builtin().bind(&self.current)?;
        }
        if let Some(plan) = self
            .pending
            .as_mut()
            .filter(|plan| plan.bindings.is_empty())
        {
            plan.bindings = Catalog::builtin().bind(&plan.document)?;
        }
        Ok(self)
    }

    /// A new run's state for a document that places only built-in components.
    #[cfg(test)]
    pub(crate) fn builtin(current: Document, identities: IdentityMap) -> Result<Self> {
        let bindings = Catalog::builtin().bind(&current)?;
        Self::new(current, bindings, identities)
    }

    /// The binding of the node named `name` in the current document.
    pub fn binding(&self, name: &str) -> Result<&Binding> {
        self.bindings
            .get(name)
            .ok_or_else(|| AppError::invalid(format!("Unknown workflow node {name:?}")))
    }

    /// The node named `name` with its binding.
    pub fn bound_node(&self, name: &str) -> Result<BoundNode> {
        let node = self
            .current
            .nodes
            .iter()
            .find(|node| node.id == name)
            .ok_or_else(|| AppError::invalid(format!("Unknown workflow node {name:?}")))?;
        Ok(BoundNode {
            node: node.clone(),
            binding: self.binding(name)?.clone(),
        })
    }

    /// Workflow names of core nodes, by core identity. While an accepted edit
    /// is pending, a replacement already in `kernel` takes its name and the
    /// node it replaces is labeled "(previous)".
    pub fn node_names(&self, kernel: &Kernel) -> BTreeMap<String, String> {
        let mut names: BTreeMap<_, _> = self
            .identities
            .nodes
            .iter()
            .map(|(name, id)| (id.clone(), name.clone()))
            .collect();
        for (name, id) in self.pending.iter().flat_map(|plan| &plan.identities.nodes) {
            if kernel.graph().node(id).is_some() {
                if let Some(old) = self.identities.nodes.get(name).filter(|old| *old != id) {
                    names.insert(old.clone(), format!("{name} (previous)"));
                }
                names.insert(id.clone(), name.clone());
            }
        }
        names
    }
}

/// A core node's workflow name from `WorkflowState::node_names`.
pub fn label(names: &BTreeMap<String, String>, core_id: &str) -> String {
    names
        .get(core_id)
        .cloned()
        .unwrap_or_else(|| "unknown node".into())
}

/// Store server-side. A client supplies its ID, never its approval contents.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub id: String,
    pub base_version: u64,
    pub base_core_revision: u64,
    pub document: Document,
    /// Bindings made when the plan was previewed; a later library change
    /// cannot alter an accepted edit.
    #[serde(default)]
    pub bindings: Bindings,
    pub identities: IdentityMap,
    pub retirements: BTreeMap<String, String>,
    /// Nodes and connections the edit adds to or removes from core's graph;
    /// none when only settings change.
    #[serde(alias = "steps")]
    pub changes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct EditOutcome {
    pub version: u64,
    pub core_revision: u64,
    /// Whether this call changed core's graph.
    pub changed_graph: bool,
}

pub fn load(path: &Path) -> Result<WorkflowState> {
    persistence::read_json::<WorkflowState>(path)?.with_bindings()
}

pub fn store(path: &Path, state: &WorkflowState) -> Result<()> {
    // Reject an unreadable successor before changing either store.
    if serde_json::to_vec_pretty(state)?.len().saturating_add(1) as u64
        > persistence::MAX_JSON_BYTES
    {
        return Err(AppError::new(
            "workflow_too_large",
            "Workflow state exceeds the storage limit",
        ));
    }
    persistence::write_json(path, state)
}

/// Prepare the edit to `next` against the current graph and report the work
/// it would retire, without changing anything. A stopped edit can be
/// re-previewed only with its original target, identities, and bindings.
pub async fn preview(
    session: &SessionHandle,
    state: &WorkflowState,
    next: Document,
    bindings: Bindings,
) -> Result<Plan> {
    let document = next.canonicalized()?;
    let kernel = session.kernel().await.map_err(AppError::core)?;
    let (bindings, identities) = if let Some(pending) = &state.pending {
        if pending.document != document {
            return Err(pending_edit());
        }
        (pending.bindings.clone(), pending.identities.clone())
    } else {
        if graph_edit(&kernel, &state.current, &state.bindings, &state.identities)?.is_some() {
            return Err(AppError::new(
                "workflow_drift",
                "Core's graph differs from the saved workflow",
            ));
        }
        let identities = target_identities(state, &document, &bindings, Typing::of(&kernel));
        (bindings, identities)
    };
    let (base_core_revision, retirements, changes) =
        match graph_edit(&kernel, &document, &bindings, &identities)? {
            None => (session.frontier().revision(), BTreeMap::new(), 0),
            Some(edit) => {
                let changes = edit.remove_nodes().len()
                    + edit.remove_edges().len()
                    + edit.add().nodes().len()
                    + edit.add().edges().len();
                let prepared = session
                    .prepare_rewrite(&request(edit))
                    .await
                    .map_err(AppError::core)?
                    .map_err(AppError::core)?;
                (
                    prepared.revision(),
                    retirement_report(prepared.retirements()),
                    changes,
                )
            }
        };
    Ok(Plan {
        id: uuid::Uuid::new_v4().to_string(),
        base_version: state.version,
        base_core_revision,
        document,
        bindings,
        identities,
        retirements,
        changes,
    })
}

/// Accept a server-owned preview, durably save intent, and apply it.
pub async fn commit(
    session: &SessionHandle,
    state: &mut WorkflowState,
    path: &Path,
    plan: Plan,
) -> Result<EditOutcome> {
    if let Some(pending) = &state.pending
        && (pending.document != plan.document
            || pending.bindings != plan.bindings
            || pending.identities != plan.identities)
    {
        return Err(pending_edit());
    }
    if state
        .pending
        .as_ref()
        .is_some_and(|pending| pending.id == plan.id)
    {
        // Retrying an accepted plan resumes its saved intention. Its original
        // revision is necessarily stale once its edit has committed.
        return recover(session, state, path).await;
    }
    if state.pending.is_none()
        && plan.base_version.checked_add(1) == Some(state.version)
        && state.current == plan.document
        && state.bindings == plan.bindings
        && state.identities == plan.identities
    {
        // The final metadata publication can succeed before its response is
        // delivered. An identical retry must not create another edit version.
        return recover(session, state, path).await;
    }
    let revision = session.frontier().revision();
    if state.version != plan.base_version
        || (plan.changes > 0 && revision != plan.base_core_revision)
    {
        return Err(AppError::new(
            "stale_preview",
            "The workflow changed after this preview; preview it again",
        ));
    }
    if plan.changes == 0 {
        // Worker progress cannot stale a settings edit. Its graph must still
        // match, so skipping the work revision never authorizes graph repair.
        let kernel = session.kernel().await.map_err(AppError::core)?;
        if graph_edit(&kernel, &plan.document, &plan.bindings, &plan.identities)?.is_some() {
            return Err(AppError::new(
                "workflow_drift",
                "Core's graph changed after this settings preview",
            ));
        }
    }
    let mut accepted = state.clone();
    accepted.pending = Some(plan);
    // Check the completed representation, too, before any core changes occur.
    completed_state(&accepted)?;
    store(path, &accepted)?;
    *state = accepted;
    recover(session, state, path).await
}

/// Finish a saved intention; never infer success from an app progress record.
/// New retirements leave the intention saved for another preview of this target.
pub async fn recover(
    session: &SessionHandle,
    state: &mut WorkflowState,
    path: &Path,
) -> Result<EditOutcome> {
    let Some(plan) = state.pending.clone() else {
        return Ok(EditOutcome {
            version: state.version,
            core_revision: session.frontier().revision(),
            changed_graph: false,
        });
    };
    let mut changed_graph = false;
    let mut stale_retries = 0;
    loop {
        let kernel = session.kernel().await.map_err(AppError::core)?;
        let Some(edit) = graph_edit(&kernel, &plan.document, &plan.bindings, &plan.identities)?
        else {
            let completed = completed_state(state)?;
            store(path, &completed)?;
            *state = completed;
            return Ok(EditOutcome {
                version: state.version,
                core_revision: session.frontier().revision(),
                changed_graph,
            });
        };
        if plan.changes == 0 || changed_graph {
            return Err(AppError::new(
                "workflow_drift",
                "Core's graph changed in a way this edit cannot repair",
            ));
        }
        let prepared = session
            .prepare_rewrite(&request(edit))
            .await
            .map_err(AppError::core)?
            .map_err(AppError::core)?;
        let additional: BTreeMap<_, _> = retirement_report(prepared.retirements())
            .into_iter()
            .filter(|(id, reason)| plan.retirements.get(id) != Some(reason))
            .collect();
        if !additional.is_empty() {
            return Err(AppError::new("retirement_preview_required", "Completing the pending edit would discard additional work; preview this target again")
                .details(serde_json::json!({"additional_retirements":additional,"pending_edit":plan.id})));
        }
        match session
            .commit_rewrite(prepared)
            .await
            .map_err(AppError::core)?
        {
            Ok(_) => changed_graph = true,
            Err(RewriteError::Stale) => {
                stale_retries += 1;
                if stale_retries == MAX_STALE_RETRIES {
                    return Err(AppError::new(
                        "workflow_busy",
                        "Work kept changing during the edit; its saved intention can be resumed",
                    ));
                }
            }
            Err(error) => return Err(AppError::core(error)),
        }
    }
}

fn completed_state(state: &WorkflowState) -> Result<WorkflowState> {
    let plan = state
        .pending
        .as_ref()
        .ok_or_else(|| AppError::invalid("No pending workflow edit"))?;
    Ok(WorkflowState {
        version: state
            .version
            .checked_add(1)
            .ok_or_else(|| AppError::new("version_exhausted", "Workflow version is exhausted"))?,
        current: plan.document.clone(),
        bindings: plan.bindings.clone(),
        identities: plan.identities.clone(),
        pending: None,
    })
}

fn pending_edit() -> AppError {
    AppError::new(
        "pending_edit",
        "Complete or re-preview the pending target before starting another edit",
    )
}

fn retirement_report(
    retirements: &BTreeMap<ontography::PackageId, RetirementReason>,
) -> BTreeMap<String, String> {
    retirements
        .iter()
        .map(|(id, reason)| {
            let reason = match reason {
                RetirementReason::HolderRemoved => "holder_removed",
                RetirementReason::NoAcceptingEdge => "no_accepting_edge",
                RetirementReason::RouteRemoved => "route_removed",
                RetirementReason::Explicit => "explicit",
            };
            (id.to_string(), reason.to_owned())
        })
        .collect()
}

/// Keep a node's core identity unless core must replace it: its join, entry
/// status, or core node types changed. A connection keeps its identity while
/// both of its nodes keep theirs.
fn target_identities(
    state: &WorkflowState,
    document: &Document,
    bindings: &Bindings,
    typing: Typing,
) -> IdentityMap {
    let node_types = |bindings: &Bindings, name: &str| {
        bindings.get(name).map(|binding| typing.node_types(binding))
    };
    let mut identities = IdentityMap::fresh(document);
    for node in &document.nodes {
        if state.current.nodes.iter().any(|old| {
            old.id == node.id
                && old.join == node.join
                && (old.id == state.current.entry) == (node.id == document.entry)
                && node_types(&state.bindings, &old.id) == node_types(bindings, &node.id)
        }) && let Some(id) = state.identities.nodes.get(&node.id)
        {
            identities.nodes.insert(node.id.clone(), id.clone());
        }
    }
    for edge in &document.edges {
        let key = edge_key(&edge.from, &edge.to);
        if identities.nodes.get(&edge.from) == state.identities.nodes.get(&edge.from)
            && identities.nodes.get(&edge.to) == state.identities.nodes.get(&edge.to)
            && let Some(id) = state.identities.edges.get(&key)
        {
            identities.edges.insert(key, id.clone());
        }
    }
    identities
}

/// The one core edit that makes `kernel`'s graph the target's: it adds the
/// target's nodes and connections that core lacks and removes those the target
/// lacks. `None` when the graph already matches.
fn graph_edit(
    kernel: &Kernel,
    document: &Document,
    bindings: &Bindings,
    ids: &IdentityMap,
) -> Result<Option<GraphEdit>> {
    let drift = |message: &str| AppError::new("workflow_drift", message);
    let typing = Typing::of(kernel);
    let mut add = GraphFragmentDeclaration::default();
    for node in &document.nodes {
        let id = ids
            .nodes
            .get(&node.id)
            .ok_or_else(|| AppError::invalid("Incomplete workflow node identities"))?;
        let binding = bindings
            .get(&node.id)
            .ok_or_else(|| AppError::invalid("Incomplete workflow node bindings"))?;
        let wanted = typing.node(id, binding, node.join);
        let entry = node.id == document.entry;
        let Some(actual) = kernel.node_definition(id) else {
            if let Some(unknown) = wanted
                .types
                .iter()
                .find(|name| !kernel.schema().node_types().any(|known| known == *name))
            {
                return Err(AppError::new(
                    "unknown_node_type",
                    format!(
                        "Node {:?} has type {unknown:?}, which this run does not declare; start a new run to use it",
                        node.id
                    ),
                ));
            }
            add.nodes.push(wanted);
            if entry {
                add.roots.push(root(id));
            }
            continue;
        };
        if !actual
            .types()
            .iter()
            .map(AsRef::as_ref)
            .eq(wanted.types.iter().map(String::as_str))
            || actual.ingress_mode() != IngressMode::from(wanted.ingress_mode)
            || kernel.root_ceiling(id).is_some() != entry
        {
            return Err(drift("A retained node has different core properties"));
        }
    }
    for edge in &document.edges {
        let id = ids
            .edges
            .get(&edge_key(&edge.from, &edge.to))
            .ok_or_else(|| AppError::invalid("Incomplete workflow edge identities"))?;
        let source = &ids.nodes[&edge.from];
        let target = &ids.nodes[&edge.to];
        match kernel.graph().edge(id) {
            None => add.edges.push(typing.edge(id, source, target)),
            Some(actual) if actual.source() != source || actual.target() != target => {
                return Err(drift("A retained connection has different endpoints"));
            }
            Some(_) => {}
        }
    }
    let kept =
        |ids: &BTreeMap<String, String>| -> BTreeSet<String> { ids.values().cloned().collect() };
    let (kept_nodes, kept_edges) = (kept(&ids.nodes), kept(&ids.edges));
    let remove_nodes: BTreeSet<Arc<str>> = kernel
        .graph()
        .nodes()
        .iter()
        .filter(|node| !kept_nodes.contains(node.id()))
        .map(|node| Arc::from(node.id()))
        .collect();
    let remove_edges: BTreeSet<Arc<str>> = kernel
        .graph()
        .edges()
        .iter()
        .filter(|edge| !kept_edges.contains(edge.id()))
        .map(|edge| Arc::from(edge.id()))
        .collect();
    if remove_nodes.is_empty()
        && remove_edges.is_empty()
        && add.nodes.is_empty()
        && add.edges.is_empty()
    {
        return Ok(None);
    }
    Ok(Some(GraphEdit::new(
        remove_nodes,
        remove_edges,
        add.compile().map_err(AppError::core)?,
    )))
}

fn request(edit: GraphEdit) -> RewriteRequest {
    RewriteRequest::new(Principal::new(EDITOR), edit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::declarations::{GraphEditDeclaration, IngressDeclaration};
    use crate::workflow::NodeType;
    use crate::workflow::document::{SHARED_NODE_TYPE, expand_builtin as expand};
    use ontography::{
        ActivationProposal, Emission, OutputAuthority, ProposalDecision, ProposalRuntime,
    };
    use serde_json::json;

    fn document() -> Document {
        serde_json::from_value(json!({
            "name":"edit-test", "entry":"writer",
            "nodes":[
                {"id":"writer","component":"agent","config":{"prompt":"before"}},
                {"id":"review","component":"inbox"}
            ],
            "edges":[{"from":"writer","to":"review"}]
        }))
        .unwrap()
    }

    /// Preview `next` bound by the built-in components.
    async fn preview(
        session: &SessionHandle,
        state: &WorkflowState,
        next: Document,
    ) -> Result<Plan> {
        let bindings = Catalog::builtin().bind(&next)?;
        super::preview(session, state, next, bindings).await
    }

    /// Apply the edit to `document` directly, as a writer bypassing the
    /// saved workflow would.
    async fn apply(session: &SessionHandle, document: &Document, ids: &IdentityMap) {
        let bindings = Catalog::builtin().bind(document).unwrap();
        let kernel = session.kernel().await.unwrap();
        let edit = graph_edit(&kernel, document, &bindings, ids)
            .unwrap()
            .unwrap();
        let prepared = session
            .prepare_rewrite(&request(edit))
            .await
            .unwrap()
            .unwrap();
        session.commit_rewrite(prepared).await.unwrap().unwrap();
    }

    fn fixture() -> (
        tempfile::TempDir,
        ProposalRuntime,
        SessionHandle,
        WorkflowState,
    ) {
        fixture_typed(Typing::Component)
    }

    /// A run created with `typing`. Runs created before node types have one
    /// shared type.
    fn fixture_typed(
        typing: Typing,
    ) -> (
        tempfile::TempDir,
        ProposalRuntime,
        SessionHandle,
        WorkflowState,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let doc = document().canonicalized().unwrap();
        let ids = IdentityMap::fresh(&doc);
        let mut declaration = expand(&doc, "edit-test", &ids).unwrap();
        if typing == Typing::Shared {
            declaration.schema.node_types = vec![SHARED_NODE_TYPE.into()];
            for node in &mut declaration.nodes {
                node.types = vec![SHARED_NODE_TYPE.into()];
            }
            for edge in &mut declaration.edges {
                edge.source_requirements = vec![SHARED_NODE_TYPE.into()];
                edge.target_requirements = vec![SHARED_NODE_TYPE.into()];
            }
        }
        let compiled = declaration.compile().unwrap();
        let runtime = ProposalRuntime::with_policy(compiled.kernel, policy());
        let session = runtime
            .create_persistent(directory.path().join("core"))
            .unwrap();
        let state = WorkflowState::builtin(doc, ids).unwrap();
        store(&directory.path().join("workflow.json"), &state).unwrap();
        (directory, runtime, session, state)
    }

    async fn produce(
        session: &SessionHandle,
        state: &WorkflowState,
        outbound: bool,
    ) -> Option<ontography::PackageId> {
        let kernel = session.kernel().await.unwrap();
        let node_id = &state.identities.nodes["writer"];
        let bytes: Arc<[u8]> = Arc::from(br#"{"message":"work"}"#.as_slice());
        let mut proposal = ActivationProposal::root(
            node_id.as_str(),
            kernel.root_ceiling(node_id).unwrap().clone(),
            bytes.clone(),
        );
        if outbound {
            let contract = kernel
                .contract(kernel.node_definition(node_id).unwrap().result_contract())
                .unwrap();
            proposal.emit(Emission::outbound(
                contract.object_type(),
                OutputAuthority::Carry,
                bytes,
            ));
        }
        let ProposalDecision::Committed(id) = session.submit(proposal).await.unwrap() else {
            panic!("fixture root proposal must be accepted");
        };
        session
            .try_snapshot()
            .await
            .unwrap()
            .state()
            .activation(id)
            .unwrap()
            .outputs()
            .next()
    }

    #[tokio::test]
    async fn a_node_keeps_its_identity_within_its_node_type_and_is_replaced_across_types() {
        let (_directory, _runtime, session, state) = fixture();
        let original = state.identities.clone();
        let mut next = state.current.clone();
        for node in &mut next.nodes {
            // Codex to Claude keeps the Agent type; inbox to human does not.
            if node.id == "writer" {
                node.component = "claude".into();
            } else {
                node.component = "human".into();
            }
        }
        let plan = preview(&session, &state, next).await.unwrap();
        assert_eq!(plan.identities.nodes["writer"], original.nodes["writer"]);
        assert_ne!(plan.identities.nodes["review"], original.nodes["review"]);
        assert_eq!(plan.bindings["writer"].implementation.kind(), "claude");
        assert_eq!(plan.bindings["review"].node_type, NodeType::Human);
        // One edit adds the human and its connection and removes the inbox
        // and its connection.
        assert_eq!(plan.changes, 4);
    }

    #[tokio::test]
    async fn runs_created_before_node_types_keep_one_shared_type() {
        let (directory, _runtime, session, mut state) = fixture_typed(Typing::Shared);
        let path = directory.path().join("workflow.json");
        let original = state.identities.clone();
        let mut next = state.current.clone();
        // Core never recorded roles here, so a new role replaces nothing.
        next.nodes
            .iter_mut()
            .find(|node| node.id == "review")
            .unwrap()
            .component = "human".into();
        next.nodes
            .push(serde_json::from_value(json!({"id":"archive","component":"inbox"})).unwrap());
        next.edges
            .push(serde_json::from_value(json!({"from":"review","to":"archive"})).unwrap());
        let plan = preview(&session, &state, next).await.unwrap();
        assert_eq!(plan.identities.nodes["review"], original.nodes["review"]);
        assert_eq!(plan.changes, 2);
        commit(&session, &mut state, &path, plan).await.unwrap();
        let kernel = session.kernel().await.unwrap();
        let archive = kernel
            .node_definition(&state.identities.nodes["archive"])
            .unwrap();
        assert_eq!(
            archive
                .types()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>(),
            [SHARED_NODE_TYPE]
        );
    }

    #[tokio::test]
    async fn only_the_editor_may_edit_and_only_into_a_workflow() {
        let (_directory, _runtime, session, state) = fixture();
        let writer = &state.identities.nodes["writer"];
        let review = &state.identities.nodes["review"];
        let connection = &state.identities.edges[&edge_key("writer", "review")];
        let denied = |principal: &str, edit: serde_json::Value| {
            let edit = serde_json::from_value::<GraphEditDeclaration>(edit)
                .unwrap()
                .compile()
                .unwrap();
            RewriteRequest::new(Principal::new(principal), edit)
        };
        let remove_review = json!({"remove_nodes":[review],"remove_edges":[connection]});
        let foreign_connection = json!({"add":{"edges":[{
            "id":"other","source":writer,"target":review,"types":["Other"],
            "package_contract":CONTRACT,"authority_tags":[AUTHORITY]}]}});
        let no_entry = json!({"remove_nodes":[writer],"remove_edges":[connection]});
        for (principal, edit, reason) in [
            ("operator", remove_review.clone(), "workflow document"),
            (EDITOR, foreign_connection, "connections"),
            (EDITOR, no_entry, "one entry"),
        ] {
            let error = session
                .prepare_rewrite(&denied(principal, edit))
                .await
                .unwrap()
                .unwrap_err();
            let RewriteError::Denied(message) = error else {
                panic!("{error}");
            };
            assert!(message.contains(reason), "{message}");
        }
        session
            .prepare_rewrite(&denied(EDITOR, remove_review))
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn state_saved_before_components_binds_its_built_in_kinds() {
        let (_directory, _runtime, _session, state) = fixture();
        let mut saved = serde_json::to_value(&state).unwrap();
        saved.as_object_mut().unwrap().remove("bindings");
        for node in saved["current"]["nodes"].as_array_mut().unwrap() {
            let component = node.as_object_mut().unwrap().remove("component").unwrap();
            node["kind"] = component;
        }
        let loaded: WorkflowState = serde_json::from_value(saved).unwrap();
        assert!(loaded.bindings.is_empty());
        let loaded = loaded.with_bindings().unwrap();
        assert_eq!(loaded.bindings, state.bindings);
        assert_eq!(loaded.current, state.current);
    }

    #[tokio::test]
    async fn plans_saved_with_steps_still_load() {
        let (directory, _runtime, session, mut state) = fixture();
        let mut next = state.current.clone();
        next.edges.clear();
        state.pending = Some(preview(&session, &state, next).await.unwrap());
        let mut saved = serde_json::to_value(&state).unwrap();
        let pending = saved["pending"].as_object_mut().unwrap();
        let changes = pending.remove("changes").unwrap();
        pending.insert("steps".into(), changes);
        let path = directory.path().join("workflow.json");
        persistence::write_json(&path, &saved).unwrap();
        assert_eq!(load(&path).unwrap().pending.unwrap().changes, 1);
    }

    #[tokio::test]
    async fn prompt_only_edit_preserves_core_revision_and_incarnations() {
        let (directory, _runtime, session, mut state) = fixture();
        let original_ids = state.identities.clone();
        let mut next = state.current.clone();
        next.nodes
            .iter_mut()
            .find(|node| node.id == "writer")
            .unwrap()
            .config = json!({"prompt":"after"});
        let plan = preview(&session, &state, next.clone()).await.unwrap();
        assert_eq!(plan.changes, 0);
        let result = commit(
            &session,
            &mut state,
            &directory.path().join("workflow.json"),
            plan,
        )
        .await
        .unwrap();
        assert_eq!(result.core_revision, 0);
        assert_eq!(result.version, 2);
        assert!(!result.changed_graph);
        assert_eq!(state.identities, original_ids);
        assert_eq!(
            load(&directory.path().join("workflow.json"))
                .unwrap()
                .current,
            next
        );
    }

    #[tokio::test]
    async fn prompt_edit_survives_unrelated_work_after_preview() {
        let (directory, _runtime, session, mut state) = fixture();
        let original_ids = state.identities.clone();
        let mut next = state.current.clone();
        next.nodes
            .iter_mut()
            .find(|node| node.id == "writer")
            .unwrap()
            .config = json!({"prompt":"after"});
        let plan = preview(&session, &state, next.clone()).await.unwrap();
        assert_eq!(plan.changes, 0);
        let package = produce(&session, &state, true).await.unwrap();
        let revision = session.frontier().revision();
        assert!(revision > plan.base_core_revision);
        let result = commit(
            &session,
            &mut state,
            &directory.path().join("workflow.json"),
            plan,
        )
        .await
        .unwrap();
        assert_eq!(result.version, 2);
        assert_eq!(result.core_revision, revision);
        assert!(!result.changed_graph);
        assert_eq!(state.current, next);
        assert_eq!(state.identities, original_ids);
        assert!(session.try_snapshot().await.unwrap().state().packages()[&package].is_live());
    }

    #[tokio::test]
    async fn prompt_edit_still_rejects_a_conflicting_document_version() {
        let (directory, _runtime, session, mut state) = fixture();
        let path = directory.path().join("workflow.json");
        let mut first = state.current.clone();
        first
            .nodes
            .iter_mut()
            .find(|node| node.id == "writer")
            .unwrap()
            .config = json!({"prompt":"first"});
        let first = preview(&session, &state, first).await.unwrap();
        let mut second = state.current.clone();
        second
            .nodes
            .iter_mut()
            .find(|node| node.id == "writer")
            .unwrap()
            .config = json!({"prompt":"second"});
        let accepted = preview(&session, &state, second.clone()).await.unwrap();
        commit(&session, &mut state, &path, accepted).await.unwrap();
        let error = commit(&session, &mut state, &path, first)
            .await
            .unwrap_err();
        assert_eq!(error.code, "stale_preview");
        assert_eq!(state.current, second);
        assert_eq!(state.version, 2);
        assert!(state.pending.is_none());
        assert_eq!(load(&path).unwrap().current, second);
    }

    #[tokio::test]
    async fn settings_plan_cannot_replace_an_incompatible_pending_target() {
        let (directory, _runtime, session, mut state) = fixture();
        let path = directory.path().join("workflow.json");
        let mut next = state.current.clone();
        next.nodes
            .iter_mut()
            .find(|node| node.id == "writer")
            .unwrap()
            .config = json!({"prompt":"after"});
        let settings = preview(&session, &state, next).await.unwrap();
        let mut topology = state.current.clone();
        topology.edges.clear();
        let pending = preview(&session, &state, topology).await.unwrap();
        let pending_id = pending.id.clone();
        state.pending = Some(pending);
        store(&path, &state).unwrap();
        let error = commit(&session, &mut state, &path, settings)
            .await
            .unwrap_err();
        assert_eq!(error.code, "pending_edit");
        assert_eq!(load(&path).unwrap().pending.unwrap().id, pending_id);
        assert_eq!(session.frontier().revision(), 0);
    }

    #[tokio::test]
    async fn settings_edits_never_repair_unexpected_topology_drift() {
        for accepted_before_drift in [false, true] {
            let (directory, _runtime, session, mut state) = fixture();
            let path = directory.path().join("workflow.json");
            let mut next = state.current.clone();
            next.nodes
                .iter_mut()
                .find(|node| node.id == "writer")
                .unwrap()
                .config = json!({"prompt":"after"});
            let plan = preview(&session, &state, next).await.unwrap();
            if accepted_before_drift {
                state.pending = Some(plan.clone());
                store(&path, &state).unwrap();
            }
            // Simulate a bypassing topology writer. A settings edit must not
            // remove its new node, before acceptance or during recovery.
            let mut changed = state.current.clone();
            changed.nodes.push(
                serde_json::from_value(json!({"id":"unexpected","component":"inbox"})).unwrap(),
            );
            let bindings = Catalog::builtin().bind(&changed).unwrap();
            let kernel = session.kernel().await.unwrap();
            let ids = target_identities(&state, &changed, &bindings, Typing::of(&kernel));
            apply(&session, &changed, &ids).await;
            let revision = session.frontier().revision();
            let error = commit(&session, &mut state, &path, plan).await.unwrap_err();
            assert_eq!(error.code, "workflow_drift");
            assert_eq!(session.frontier().revision(), revision);
            assert!(
                session
                    .kernel()
                    .await
                    .unwrap()
                    .graph()
                    .node(&ids.nodes["unexpected"])
                    .is_some()
            );
            assert_eq!(state.version, 1);
            assert_eq!(state.pending.is_some(), accepted_before_drift);
        }
    }

    #[tokio::test]
    async fn stale_preview_never_saves_or_applies_an_edit() {
        let (directory, _runtime, session, mut state) = fixture();
        let mut next = state.current.clone();
        next.edges.clear();
        let plan = preview(&session, &state, next).await.unwrap();
        produce(&session, &state, false).await;
        let error = commit(
            &session,
            &mut state,
            &directory.path().join("workflow.json"),
            plan,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "stale_preview");
        assert!(state.pending.is_none());
        assert_eq!(session.kernel().await.unwrap().graph().edges().len(), 1);
    }

    #[tokio::test]
    async fn extra_retirement_stops_then_same_target_can_be_repreviewed() {
        let (directory, _runtime, session, mut state) = fixture();
        let path = directory.path().join("workflow.json");
        let mut next = state.current.clone();
        next.edges.clear();
        let plan = preview(&session, &state, next.clone()).await.unwrap();
        assert!(plan.retirements.is_empty());
        // Intent was accepted, then a worker produced more work before removal.
        state.pending = Some(plan);
        store(&path, &state).unwrap();
        let package = produce(&session, &state, true).await.unwrap();
        let error = recover(&session, &mut state, &path).await.unwrap_err();
        assert_eq!(error.code, "retirement_preview_required");
        assert!(load(&path).unwrap().pending.is_some());
        assert!(session.try_snapshot().await.unwrap().state().packages()[&package].is_live());
        let refreshed = preview(&session, &state, next).await.unwrap();
        assert_eq!(
            refreshed.retirements[&package.to_string()],
            "no_accepting_edge"
        );
        commit(&session, &mut state, &path, refreshed)
            .await
            .unwrap();
        assert_eq!(
            session
                .try_snapshot()
                .await
                .unwrap()
                .state()
                .retirement(package)
                .unwrap()
                .reason(),
            RetirementReason::NoAcceptingEdge
        );
    }

    #[tokio::test]
    async fn a_replaced_connection_keeps_output_routable() {
        let (directory, _runtime, session, mut state) = fixture();
        let old_node = state.identities.nodes["review"].clone();
        let old_edge = state.identities.edges[&edge_key("writer", "review")].clone();
        let package = produce(&session, &state, true).await.unwrap();
        let mut next = state.current.clone();
        next.nodes
            .iter_mut()
            .find(|node| node.id == "review")
            .unwrap()
            .join = IngressDeclaration::All;
        let plan = preview(&session, &state, next).await.unwrap();
        assert_ne!(plan.identities.nodes["review"], old_node);
        assert_ne!(
            plan.identities.edges[&edge_key("writer", "review")],
            old_edge
        );
        assert!(plan.retirements.is_empty());
        commit(
            &session,
            &mut state,
            &directory.path().join("workflow.json"),
            plan,
        )
        .await
        .unwrap();
        let snapshot = session.try_snapshot().await.unwrap();
        assert!(snapshot.kernel().graph().node(&old_node).is_none());
        assert!(snapshot.state().used_node_ids().contains(old_node.as_str()));
        assert!(snapshot.state().used_edge_ids().contains(old_edge.as_str()));
        assert!(snapshot.state().packages()[&package].is_live());
    }

    #[tokio::test]
    async fn restart_recovers_after_core_commit_before_any_app_progress_write() {
        // Cover an accepted edit that core never applied and one whose final
        // document publication was interrupted after core applied it.
        for applied in [false, true] {
            let (directory, runtime, session, mut state) = fixture();
            let path = directory.path().join("workflow.json");
            let declaration = expand(&state.current, "edit-test", &state.identities).unwrap();
            let mut next = state.current.clone();
            next.nodes
                .push(serde_json::from_value(json!({"id":"archive","component":"inbox"})).unwrap());
            next.edges
                .push(serde_json::from_value(json!({"from":"review","to":"archive"})).unwrap());
            let plan = preview(&session, &state, next).await.unwrap();
            assert_eq!(plan.changes, 2);
            let target_ids = plan.identities.clone();
            state.pending = Some(plan.clone());
            store(&path, &state).unwrap();
            if applied {
                apply(&session, &plan.document, &plan.identities).await;
            }
            drop(session);
            drop(runtime);
            let compiled = declaration.compile().unwrap();
            let runtime = ProposalRuntime::with_policy(compiled.kernel, policy());
            let session = runtime
                .open_persistent(directory.path().join("core"))
                .unwrap();
            let mut state = load(&path).unwrap();
            let result = commit(&session, &mut state, &path, plan.clone())
                .await
                .unwrap();
            assert_eq!(result.changed_graph, !applied);
            assert_eq!(state.identities, target_ids);
            assert!(state.pending.is_none());
            assert_eq!(state.current, plan.document);
            assert_eq!(session.kernel().await.unwrap().graph().nodes().len(), 3);
            let repeated = commit(&session, &mut state, &path, plan).await.unwrap();
            assert_eq!(repeated.version, result.version);
            assert!(!repeated.changed_graph);
        }
    }
}
