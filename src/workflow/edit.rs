//! Durable intent over core's individually atomic rewrites.
//!
//! The caller serializes topology edits for a run. Workers may keep submitting
//! between steps: this is a recoverable live edit, not one atomic graph change.
//! The graph itself records progress; a saved target fixes all fresh identities.

use super::document::{Document, IdentityMap, JoinMode, edge_key};
use super::grammar::{self, Variant};
use crate::{AppError, Result, persistence};
use ontography::{
    IngressMode, Kernel, RetirementReason, RewriteError, RewriteGrammar, RewriteMatch,
    RewriteRequest, SessionHandle,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, sync::Arc};

const MAX_STALE_RETRIES: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowState {
    pub version: u64,
    pub current: Document,
    pub identities: IdentityMap,
    pub pending: Option<Plan>,
}

impl WorkflowState {
    pub fn new(current: Document, identities: IdentityMap) -> Result<Self> {
        Ok(Self {
            version: 1,
            current: current.canonicalized()?,
            identities,
            pending: None,
        })
    }
}

/// Store server-side. A client supplies its ID, never its approval contents.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub id: String,
    pub base_version: u64,
    pub base_core_revision: u64,
    pub document: Document,
    pub identities: IdentityMap,
    pub retirements: BTreeMap<String, String>,
    pub steps: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct EditOutcome {
    pub version: u64,
    pub core_revision: u64,
    pub steps_applied: usize,
}

pub fn load(path: &Path) -> Result<WorkflowState> {
    persistence::read_json(path)
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

/// Simulate the remaining changes using the same kernel rules and exact bytes.
/// A stopped edit can be re-previewed only with its original target and IDs.
pub async fn preview(
    session: &SessionHandle,
    state: &WorkflowState,
    next: Document,
) -> Result<Plan> {
    let document = next.canonicalized()?;
    let identities = if let Some(pending) = &state.pending {
        if pending.document != document {
            return Err(pending_edit());
        }
        pending.identities.clone()
    } else {
        target_identities(state, &document)
    };
    let snapshot = session.try_snapshot().await.map_err(AppError::core)?;
    let mut kernel = snapshot.kernel().clone();
    let mut facts = snapshot.state().clone();
    if state.pending.is_none() && next_step(&kernel, &state.current, &state.identities)?.is_some() {
        return Err(AppError::new(
            "workflow_drift",
            "Core's graph differs from the saved workflow",
        ));
    }
    let contracts = kernel.contracts().to_vec();
    let grammar = RewriteGrammar::new(
        grammar::universal()
            .iter()
            .map(|rule| rule.compile(kernel.id(), kernel.schema(), &contracts))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(AppError::core)?,
    )
    .map_err(AppError::core)?;
    let mut evidence = BTreeMap::new();
    let mut retirements = BTreeMap::new();
    let mut steps = 0;
    while let Some(request) = next_step(&kernel, &document, &identities)? {
        let prepared = loop {
            match kernel.prepare_rewrite(&facts, &grammar, &request, &evidence) {
                Ok(prepared) => break prepared,
                Err(RewriteError::MissingEvidence(package)) => {
                    let digest = facts.packages()[&package].content_digest();
                    let bytes = session
                        .content(digest)
                        .await
                        .map_err(AppError::core)?
                        .ok_or_else(|| {
                            AppError::new(
                                "missing_content",
                                format!("Missing bytes for pending package {package}"),
                            )
                        })?;
                    evidence.insert(digest, bytes);
                }
                Err(error) => return Err(AppError::core(error)),
            }
        };
        retirements.extend(retirement_report(&prepared.retirements()));
        kernel = kernel
            .commit_rewrite(&mut facts, prepared)
            .map_err(AppError::core)?;
        steps += 1;
    }
    Ok(Plan {
        id: uuid::Uuid::new_v4().to_string(),
        base_version: state.version,
        base_core_revision: snapshot.revision(),
        document,
        identities,
        retirements,
        steps,
    })
}

/// Accept a server-owned preview, durably save intent, and begin reconciling.
pub async fn commit(
    session: &SessionHandle,
    state: &mut WorkflowState,
    path: &Path,
    plan: Plan,
) -> Result<EditOutcome> {
    if let Some(pending) = &state.pending
        && (pending.document != plan.document || pending.identities != plan.identities)
    {
        return Err(pending_edit());
    }
    if state
        .pending
        .as_ref()
        .is_some_and(|pending| pending.id == plan.id)
    {
        // Retrying an accepted plan resumes its saved intention. Its original
        // revision is necessarily stale once any of its steps have committed.
        return recover(session, state, path).await;
    }
    if state.pending.is_none()
        && plan.base_version.checked_add(1) == Some(state.version)
        && state.current == plan.document
        && state.identities == plan.identities
    {
        // The final metadata publication can succeed before its response is
        // delivered. An identical retry must not create another edit version.
        return recover(session, state, path).await;
    }
    let revision = session.frontier().revision();
    if state.version != plan.base_version || (plan.steps > 0 && revision != plan.base_core_revision)
    {
        return Err(AppError::new(
            "stale_preview",
            "The workflow changed after this preview; preview it again",
        ));
    }
    if plan.steps == 0 {
        // Worker progress cannot stale a settings edit. Its graph must still
        // match, so skipping the work revision never authorizes graph repair.
        let kernel = session.kernel().await.map_err(AppError::core)?;
        if next_step(&kernel, &plan.document, &plan.identities)?.is_some() {
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

/// Continue a saved intention; never infer success from an app progress counter.
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
            steps_applied: 0,
        });
    };
    let mut steps_applied = 0;
    let mut stale_retries = 0;
    loop {
        let kernel = session.kernel().await.map_err(AppError::core)?;
        let Some(request) = next_step(&kernel, &plan.document, &plan.identities)? else {
            let completed = completed_state(state)?;
            store(path, &completed)?;
            *state = completed;
            return Ok(EditOutcome {
                version: state.version,
                core_revision: session.frontier().revision(),
                steps_applied,
            });
        };
        if plan.steps == 0 {
            return Err(AppError::new(
                "workflow_drift",
                "A settings edit cannot repair an unexpected graph change",
            ));
        }
        let prepared = session
            .prepare_rewrite(&request)
            .await
            .map_err(AppError::core)?
            .map_err(AppError::core)?;
        let retirements = retirement_report(prepared.retirements());
        let additional: BTreeMap<_, _> = retirements
            .into_iter()
            .filter(|(id, reason)| plan.retirements.get(id) != Some(reason))
            .collect();
        if !additional.is_empty() {
            return Err(AppError::new("retirement_preview_required", "Completing the pending edit would discard additional work; preview this target again")
                .details(serde_json::json!({"additional_retirements":additional,"steps_applied":steps_applied,"pending_edit":plan.id})));
        }
        match session
            .commit_rewrite(prepared)
            .await
            .map_err(AppError::core)?
        {
            Ok(_) => {
                steps_applied += 1;
                stale_retries = 0;
            }
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

fn target_identities(state: &WorkflowState, document: &Document) -> IdentityMap {
    let mut identities = IdentityMap::fresh(document);
    for node in &document.nodes {
        if state.current.nodes.iter().any(|old| {
            old.id == node.id
                && old.join == node.join
                && (old.id == state.current.entry) == (node.id == document.entry)
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

fn variant(kernel: &Kernel, node_id: &str) -> Result<Variant> {
    let node = kernel
        .node_definition(node_id)
        .ok_or_else(|| AppError::new("workflow_drift", format!("Missing node {node_id}")))?;
    Ok(Variant {
        join: match node.ingress_mode() {
            IngressMode::Any => JoinMode::Any,
            IngressMode::All => JoinMode::All,
        },
        root: kernel.root_ceiling(node_id).is_some(),
    })
}

/// Monotone reconciliation: add target nodes/routes before deleting old ones.
/// Replacements use new IDs, so their old and new incarnations can coexist.
fn next_step(
    kernel: &Kernel,
    document: &Document,
    ids: &IdentityMap,
) -> Result<Option<RewriteRequest>> {
    for node in &document.nodes {
        let id = ids
            .nodes
            .get(&node.id)
            .ok_or_else(|| AppError::invalid("Incomplete workflow node identities"))?;
        let wanted = Variant {
            join: node.join,
            root: node.id == document.entry,
        };
        if kernel.graph().node(id).is_none() {
            return Ok(Some(RewriteRequest::new(
                grammar::node_rule(true, wanted),
                RewriteMatch::new(
                    BTreeMap::new(),
                    BTreeMap::new(),
                    bindings(&[("n", id)]),
                    BTreeMap::new(),
                ),
            )));
        }
        if variant(kernel, id)? != wanted {
            return Err(AppError::new(
                "workflow_drift",
                "A retained node has different core properties",
            ));
        }
    }
    for edge in &document.edges {
        let id = ids
            .edges
            .get(&edge_key(&edge.from, &edge.to))
            .ok_or_else(|| AppError::invalid("Incomplete workflow edge identities"))?;
        let source = &ids.nodes[&edge.from];
        let target = &ids.nodes[&edge.to];
        if let Some(actual) = kernel.graph().edge(id) {
            if actual.source() != source || actual.target() != target {
                return Err(AppError::new(
                    "workflow_drift",
                    "A retained connection has different endpoints",
                ));
            }
        } else {
            return Ok(Some(edge_request(kernel, true, id, source, target)?));
        }
    }
    for edge in kernel.graph().edges() {
        if !ids.edges.values().any(|id| id == edge.id()) {
            return Ok(Some(edge_request(
                kernel,
                false,
                edge.id(),
                edge.source(),
                edge.target(),
            )?));
        }
    }
    for node in kernel.graph().nodes() {
        if !ids.nodes.values().any(|id| id == node.id()) {
            return Ok(Some(RewriteRequest::new(
                grammar::node_rule(false, variant(kernel, node.id())?),
                RewriteMatch::new(
                    bindings(&[("n", node.id())]),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                ),
            )));
        }
    }
    Ok(None)
}

fn edge_request(
    kernel: &Kernel,
    add: bool,
    id: &str,
    source: &str,
    target: &str,
) -> Result<RewriteRequest> {
    let is_loop = source == target;
    Ok(RewriteRequest::new(
        grammar::edge_rule(
            add,
            variant(kernel, source)?,
            variant(kernel, target)?,
            is_loop,
        ),
        RewriteMatch::new(
            if is_loop {
                bindings(&[("n", source)])
            } else {
                bindings(&[("a", source), ("b", target)])
            },
            if add {
                BTreeMap::new()
            } else {
                bindings(&[("e", id)])
            },
            BTreeMap::new(),
            if add {
                bindings(&[("e", id)])
            } else {
                BTreeMap::new()
            },
        ),
    ))
}

fn bindings(entries: &[(&str, &str)]) -> BTreeMap<Arc<str>, Arc<str>> {
    entries
        .iter()
        .map(|(key, value)| (Arc::from(*key), Arc::from(*value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::document::expand;
    use ontography::{
        ActivationProposal, Emission, OutputAuthority, ProposalDecision, ProposalRuntime,
    };
    use serde_json::json;

    fn document() -> Document {
        serde_json::from_value(json!({
            "name":"edit-test", "entry":"writer",
            "nodes":[
                {"id":"writer","kind":"agent","config":{"prompt":"before"}},
                {"id":"review","kind":"inbox"}
            ],
            "edges":[{"from":"writer","to":"review"}]
        }))
        .unwrap()
    }

    fn fixture() -> (
        tempfile::TempDir,
        ProposalRuntime,
        SessionHandle,
        WorkflowState,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let doc = document().canonicalized().unwrap();
        let ids = IdentityMap::fresh(&doc);
        let declaration = expand(&doc, "edit-test", &ids).unwrap();
        let compiled = declaration.compile().unwrap();
        let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
        let session = runtime
            .create_persistent(directory.path().join("core"))
            .unwrap();
        let state = WorkflowState::new(doc, ids).unwrap();
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
        assert_eq!(plan.steps, 0);
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
        assert_eq!(plan.steps, 0);
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
        assert_eq!(result.steps_applied, 0);
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
            changed
                .nodes
                .push(serde_json::from_value(json!({"id":"unexpected","kind":"inbox"})).unwrap());
            let ids = target_identities(&state, &changed);
            let kernel = session.kernel().await.unwrap();
            let request = next_step(&kernel, &changed, &ids).unwrap().unwrap();
            let prepared = session.prepare_rewrite(&request).await.unwrap().unwrap();
            session.commit_rewrite(prepared).await.unwrap().unwrap();
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
    async fn replacement_adds_new_route_before_removing_old_and_preserves_output() {
        let (directory, _runtime, session, mut state) = fixture();
        let old_node = state.identities.nodes["review"].clone();
        let old_edge = state.identities.edges[&edge_key("writer", "review")].clone();
        let package = produce(&session, &state, true).await.unwrap();
        let mut next = state.current.clone();
        next.nodes
            .iter_mut()
            .find(|node| node.id == "review")
            .unwrap()
            .join = JoinMode::All;
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
        // Cover both a partially applied edit and a fully applied graph whose
        // final document publication was interrupted.
        for committed_steps in 1..=2 {
            let (directory, runtime, session, mut state) = fixture();
            let path = directory.path().join("workflow.json");
            let declaration = expand(&state.current, "edit-test", &state.identities).unwrap();
            let mut next = state.current.clone();
            next.nodes
                .push(serde_json::from_value(json!({"id":"archive","kind":"inbox"})).unwrap());
            next.edges
                .push(serde_json::from_value(json!({"from":"review","to":"archive"})).unwrap());
            let plan = preview(&session, &state, next).await.unwrap();
            assert_eq!(plan.steps, 2);
            let target_ids = plan.identities.clone();
            state.pending = Some(plan.clone());
            store(&path, &state).unwrap();
            for _ in 0..committed_steps {
                let kernel = session.kernel().await.unwrap();
                let request = next_step(&kernel, &plan.document, &plan.identities)
                    .unwrap()
                    .unwrap();
                let prepared = session.prepare_rewrite(&request).await.unwrap().unwrap();
                session.commit_rewrite(prepared).await.unwrap().unwrap();
            }
            drop(session);
            drop(runtime);
            let compiled = declaration.compile().unwrap();
            let runtime = ProposalRuntime::with_grammar(compiled.kernel, compiled.grammar);
            let session = runtime
                .open_persistent(directory.path().join("core"))
                .unwrap();
            let mut state = load(&path).unwrap();
            let result = commit(&session, &mut state, &path, plan.clone())
                .await
                .unwrap();
            assert_eq!(result.steps_applied, 2 - committed_steps);
            assert_eq!(state.identities, target_ids);
            assert!(state.pending.is_none());
            assert_eq!(state.current, plan.document);
            assert_eq!(session.kernel().await.unwrap().graph().nodes().len(), 3);
            let repeated = commit(&session, &mut state, &path, plan).await.unwrap();
            assert_eq!(repeated.version, result.version);
            assert_eq!(repeated.steps_applied, 0);
        }
    }
}
