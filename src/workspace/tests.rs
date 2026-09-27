//! Workspace store guarantees, checked against a real core session.

use crate::workspace::{WorkspaceError, WorkspaceStore};
use ontography::{
    ActivationProposal, Authority, AuthorityTag, ContentError, Contract, DefinitionId, Edge,
    EdgeDefinition, Emission, Graph, Kernel, Node, NodeDefinition, OutputAuthority,
    PackageDocument, PackageEnvelope, PackageId, PackageStore, ProposalDecision, ProposalRuntime,
    RootRule, Schema, SessionHandle,
};
use std::{collections::BTreeMap, sync::Arc};

/// A fresh session over a two-node graph: `producer` may start work under
/// `route` authority and emit it along `flow` to `consumer`.
fn open_session() -> SessionHandle {
    let route = AuthorityTag::new("route").unwrap();
    let kernel = Kernel::admit(
        DefinitionId::new("workspace-tests").unwrap(),
        Schema::new(["Node"], ["Result", "Artifact"], [route.clone()]).unwrap(),
        Graph::new(
            [
                Node::new("producer").unwrap(),
                Node::new("consumer").unwrap(),
            ],
            [Edge::new("flow", "producer", "consumer").unwrap()],
        )
        .unwrap(),
        [
            Contract::new("result", "Result", |_| Ok(())).unwrap(),
            Contract::new("artifact", "Artifact", |_| Ok(())).unwrap(),
        ],
        [
            NodeDefinition::new("producer", ["Node"], "result").unwrap(),
            NodeDefinition::new("consumer", ["Node"], "result").unwrap(),
        ],
        [EdgeDefinition::new(
            "flow",
            ["Flow"],
            ["Node"],
            ["Node"],
            "artifact",
            [route.clone()],
        )
        .unwrap()],
        [],
        [RootRule::new("producer", Authority::new([route])).unwrap()],
    )
    .unwrap();
    ProposalRuntime::new(Arc::new(kernel)).open().unwrap()
}

/// A filesystem may treat names that differ only by case or Unicode
/// normalization as one path, so opening a package with such names is
/// rejected. The package itself is left as stored: its identity stays its
/// exact bytes.
#[tokio::test]
async fn unicode_aliases_are_rejected_without_changing_package_identity() {
    let session = open_session();
    let content = session.content_store().await.unwrap();
    let packages = PackageStore::new(content.clone());
    let empty = PackageDocument::Collection {
        entries: BTreeMap::new(),
    };
    let canonical = packages.put(&empty).await.unwrap();
    let alternate = content
        .import_bytes(br#"{ "entries": {}, "kind": "collection" }"#.to_vec())
        .await
        .unwrap();
    assert_eq!(packages.get(alternate).await.unwrap(), empty);
    assert_ne!(alternate, canonical);
    assert_eq!(
        packages
            .put(&packages.get(alternate).await.unwrap())
            .await
            .unwrap(),
        canonical
    );
    let directory = tempfile::tempdir().unwrap();
    let workspace = WorkspaceStore::new(content, directory.path());
    for (a, b) in [("é", "e\u{301}"), ("É", "e\u{301}")] {
        let id = packages
            .put(&PackageDocument::Collection {
                entries: BTreeMap::from([(a.into(), canonical), (b.into(), canonical)]),
            })
            .await
            .unwrap();
        assert!(matches!(
            workspace.open(id).await,
            Err(WorkspaceError::Invalid(message))
                if message == "case- or Unicode-normalization-colliding paths"
        ));
    }
    let distinct = packages
        .put(&PackageDocument::Collection {
            entries: BTreeMap::from([("é".into(), canonical), ("ø".into(), canonical)]),
        })
        .await
        .unwrap();
    workspace.open(distinct).await.unwrap();
}

/// A capture pins the content it adds until the capture is retained. If it
/// fails or is dropped, that content is released; content held by another
/// stage, or by the capture's base, stays.
#[tokio::test]
async fn failed_or_dropped_captures_release_only_their_own_content() {
    let session = open_session();
    let content = session.content_store().await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let workspace = WorkspaceStore::new(content.clone(), directory.path().join("cache"));
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let data = b"unique failed-capture bytes";
    // Learn the identity, then remove its preexisting retention.
    let id = content.import_bytes(data.to_vec()).await.unwrap();
    content.release(id).await.unwrap();
    content.collect_garbage().await.unwrap();
    std::fs::write(source.join("data"), data).unwrap();
    std::os::unix::fs::symlink("b", source.join("a")).unwrap();
    std::os::unix::fs::symlink("a", source.join("b")).unwrap();
    assert!(workspace.import_directory(&source).await.is_err());
    content.collect_garbage().await.unwrap();
    assert!(matches!(
        content.metadata(id).await,
        Err(ContentError::Missing(_))
    ));

    // Stages pin independently: dropping one keeps what another holds.
    let first = content.stage_imports();
    let second = content.stage_imports();
    let id = first.store().import_bytes(data.to_vec()).await.unwrap();
    assert_eq!(
        id,
        second.store().import_bytes(data.to_vec()).await.unwrap()
    );
    drop(first);
    content.collect_garbage().await.unwrap();
    assert!(content.metadata(id).await.unwrap().complete);
    second.retain().await.unwrap();
    content.collect_garbage().await.unwrap();
    assert!(content.metadata(id).await.unwrap().complete);
    content.release(id).await.unwrap();
    content.collect_garbage().await.unwrap();
    assert!(matches!(
        content.metadata(id).await,
        Err(ContentError::Missing(_))
    ));

    // Dropping a staged capture, as on cancellation or rejection, also
    // releases the packages it created.
    let packages = PackageStore::new(content.clone());
    let empty = packages
        .put(&PackageDocument::Collection {
            entries: BTreeMap::new(),
        })
        .await
        .unwrap();
    std::fs::remove_file(source.join("a")).unwrap();
    std::fs::remove_file(source.join("b")).unwrap();
    let capture = workspace.capture_staged(&source, empty).await.unwrap();
    let root = capture.package().root();
    content.collect_garbage().await.unwrap();
    workspace.open(root).await.unwrap();
    drop(capture);
    content.collect_garbage().await.unwrap();
    assert!(matches!(
        content.metadata(root).await,
        Err(ContentError::Missing(_))
    ));
    assert!(matches!(
        content.metadata(id).await,
        Err(ContentError::Missing(_))
    ));
    workspace.open(empty).await.unwrap();
}

/// A captured edit records only the changed path and reuses the rest of its
/// base. Published as a workflow output, it travels as a small envelope, and
/// the activation links exactly the capture's content.
#[tokio::test]
async fn changed_workspace_reuses_prior_content_and_emits_only_its_envelope() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("unchanged.bin"), vec![b'x'; 64 * 1024]).unwrap();
    std::fs::write(source.join("changed.txt"), b"before").unwrap();

    let session = open_session();
    let content = session.content_store().await.unwrap();
    let workspace = WorkspaceStore::new(content.clone(), directory.path().join("cache"));
    let packages = PackageStore::new(content);

    let base = workspace.import_directory(&source).await.unwrap();
    std::fs::write(source.join("changed.txt"), b"after").unwrap();
    let changed = workspace.capture(&source, base.root()).await.unwrap();

    let PackageDocument::Changes {
        base: referenced_base,
        changes,
    } = packages.get(changed.root()).await.unwrap()
    else {
        panic!("capture should create a Changes package");
    };
    assert_eq!(referenced_base, base.root());
    assert_eq!(changes.len(), 1);
    assert!(changes.contains_key("changed.txt"));
    assert_eq!(
        base.entries()
            .iter()
            .find(|entry| entry.path == "unchanged.bin")
            .unwrap()
            .package,
        changed
            .entries()
            .iter()
            .find(|entry| entry.path == "unchanged.bin")
            .unwrap()
            .package,
    );

    let envelope = PackageEnvelope::new(changed.root()).to_payload().unwrap();
    assert!(envelope.len() < 512);
    let mut proposal = ActivationProposal::root(
        "producer",
        Authority::new([AuthorityTag::new("route").unwrap()]),
        Arc::from(b"result".as_slice()),
    );
    proposal.emit(Emission::new(
        "flow",
        OutputAuthority::Carry,
        envelope.clone(),
    ));
    let activation = match session
        .submit_with_content(proposal, changed.dependencies())
        .await
        .unwrap()
    {
        ProposalDecision::Committed(id) => id,
        ProposalDecision::Rejected(error) => panic!("composition was rejected: {error}"),
    };
    let package = session
        .snapshot()
        .await
        .state()
        .package(PackageId::from_parts(activation, 0))
        .unwrap()
        .clone();
    let stored = session
        .content(package.content_digest())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, envelope);
    assert_eq!(
        PackageEnvelope::from_payload(&stored)
            .unwrap()
            .unwrap()
            .ontography_package,
        changed.root(),
    );
    assert_eq!(
        session.activation_content(activation).await.unwrap(),
        changed.dependencies()
    );
}
