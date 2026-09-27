//! Content an attempt creates without a checkout: imported files and composed
//! directories. Each becomes an output that submit_invocation can send.

use super::context::{Attempt, AttemptState, OutputRef};
use super::{NodeToolContext, Reply, Tool};
use crate::tools::workspace::workspace_error;
use crate::{AppError, Result};
use ontography::{ContentId, PackageDocument, PackageMemberGrant, PackageStore, ResolvedEntryKind};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;

/// Largest text one import stores; larger files belong in a checkout.
const MAX_IMPORT: usize = 8 * 1024 * 1024;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Import {
    attempt_id: String,
    /// The file's contents, at most 8 MiB.
    text: String,
    /// Whether the file is meant to be executable.
    #[serde(default)]
    executable: bool,
}

pub(super) struct ImportContent;

impl Tool for ImportContent {
    const NAME: &'static str = "import_content";
    const DESCRIPTION: &'static str = "Store text as a new file for this attempt. Returns an output_id to compose into a directory.";
    const MUTATING: bool = true;
    type Input = Import;

    async fn run(context: &NodeToolContext, import: Import) -> Result<Reply> {
        if import.text.len() > MAX_IMPORT {
            return Err(AppError::invalid(format!(
                "Import at most {MAX_IMPORT} bytes per file; write larger files in a checkout and capture it"
            )));
        }
        context
            .with_attempt(&import.attempt_id, async |attempt, state| {
                let store = state.staging();
                let content = store
                    .import_bytes(import.text.into_bytes())
                    .await
                    .map_err(AppError::core)?;
                let root = PackageStore::new(store)
                    .put(&PackageDocument::File {
                        content,
                        executable: import.executable,
                    })
                    .await
                    .map_err(AppError::core)?;
                let output = state.handle("out");
                let reply = json!({"output_id": output, "kind": "file", "bytes": content.size()});
                state.outputs.insert(
                    output,
                    OutputRef {
                        root,
                        directory: false,
                        // A file's representation is its document and its bytes.
                        dependencies: vec![root, content],
                        capture: None,
                    },
                );
                Reply::record(attempt, Self::NAME, &reply).await
            })
            .await
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Compose {
    attempt_id: String,
    /// A new directory from named entries, each an output_id or the handle of
    /// an input's file, link, or workspace.
    #[serde(default)]
    entries: Option<BTreeMap<String, String>>,
    /// Or: a directory output or workspace input whose view the changes apply to.
    #[serde(default)]
    base: Option<String>,
    /// Paths in base to replace with a handle, or to delete with null.
    #[serde(default)]
    changes: Option<BTreeMap<String, Option<String>>>,
}

pub(super) struct ComposePackage;

impl Tool for ComposePackage {
    const NAME: &'static str = "compose_package";
    const DESCRIPTION: &'static str = "Build a directory for this attempt, either from named entries or by changing paths in an existing directory. Returns an output_id.";
    const MUTATING: bool = true;
    type Input = Compose;

    async fn run(context: &NodeToolContext, compose: Compose) -> Result<Reply> {
        let id = compose.attempt_id.clone();
        context
            .with_attempt(&id, async |attempt, state| {
                let document = compose.document(attempt, state)?;
                let root = PackageStore::new(state.staging())
                    .put(&document)
                    .await
                    .map_err(workspace_error)?;
                // Core checks names and paths as it stores them; opening the
                // view also checks that it is safe to check out.
                let view = context
                    .workspaces
                    .open(root)
                    .await
                    .map_err(workspace_error)?;
                let output = state.handle("out");
                let reply = json!({"output_id": output, "kind": "directory",
                    "entries": view.entries().len().saturating_sub(1)});
                state.outputs.insert(
                    output,
                    OutputRef {
                        root,
                        directory: true,
                        dependencies: view.dependencies(),
                        capture: None,
                    },
                );
                Reply::record(attempt, Self::NAME, &reply).await
            })
            .await
    }
}

impl Compose {
    /// The package this composition describes, over the content its handles name.
    fn document(self, attempt: &Attempt, state: &AttemptState) -> Result<PackageDocument> {
        let content = |handle: &str| resolve(attempt, state, handle).map(|source| source.root());
        match (self.entries, self.base, self.changes) {
            (Some(entries), None, None) => Ok(PackageDocument::Collection {
                entries: entries
                    .into_iter()
                    .map(|(name, handle)| Ok((name, content(&handle)?)))
                    .collect::<Result<_>>()?,
            }),
            (None, Some(base), Some(changes)) => {
                let source = resolve(attempt, state, &base)?;
                if !source.is_directory() {
                    return Err(AppError::invalid(format!(
                        "Base {base:?} is not a directory"
                    )));
                }
                Ok(PackageDocument::Changes {
                    base: source.root(),
                    changes: changes
                        .into_iter()
                        .map(|(path, handle)| {
                            Ok((path, handle.as_deref().map(content).transpose()?))
                        })
                        .collect::<Result<_>>()?,
                })
            }
            _ => Err(AppError::invalid(
                "Supply either entries, or base and changes",
            )),
        }
    }
}

/// What a handle names for an attempt to build on: one of its outputs, or a
/// member of its inputs.
pub(super) enum Source<'a> {
    Output(&'a OutputRef),
    Input(&'a PackageMemberGrant),
}

impl Source<'_> {
    pub(super) fn root(&self) -> ContentId {
        match self {
            Self::Output(output) => output.root,
            Self::Input(member) => member.package,
        }
    }

    pub(super) fn is_directory(&self) -> bool {
        match self {
            Self::Output(output) => output.directory,
            Self::Input(member) => matches!(member.kind, ResolvedEntryKind::Directory),
        }
    }
}

/// Resolves a handle for this attempt to build on. Only a view's root is
/// reusable as a directory: a nested directory's identity is meaningful only
/// at its path, since a changes view gives it the whole changes package's ID.
/// Files and links carry packages of their own.
pub(super) fn resolve<'a>(
    attempt: &'a Attempt,
    state: &'a AttemptState,
    handle: &str,
) -> Result<Source<'a>> {
    if let Some(output) = state.outputs.get(handle) {
        return Ok(Source::Output(output));
    }
    let member = attempt.member(handle).ok_or_else(|| {
        AppError::new(
            "unknown_handle",
            format!("{handle:?} is neither an output of this attempt nor a member of its inputs"),
        )
    })?;
    if matches!(member.kind, ResolvedEntryKind::Directory) && !member.path.is_empty() {
        return Err(AppError::invalid(format!(
            "{handle:?} is a directory inside an input and can't be reused on its own; compose its files, or open the whole input as a workspace"
        )));
    }
    Ok(Source::Input(member))
}

#[cfg(test)]
mod tests {
    use super::MAX_IMPORT;
    use crate::node_tool::tests::{
        Fixture, begin, deliver_workspace, document, files, recorded, sink_files,
    };
    use serde_json::{Value, json};

    /// The handle of the member at `path` in one of an attempt's input directories.
    async fn member(fixture: &Fixture, attempt: &Value, directory: &Value, path: &str) -> Value {
        let listed = fixture
            .ok(
                "list_package",
                json!({"attempt_id": attempt, "handle": directory}),
            )
            .await;
        listed["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["path"] == path)
            .unwrap()["handle"]
            .clone()
    }

    #[tokio::test]
    async fn imported_files_compose_into_a_published_directory() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        fixture.deliver("build it").await;
        let attempt = begin(&fixture).await["attempt_id"].clone();
        let import = |text: &str, executable: bool| json!({"attempt_id": attempt, "text": text, "executable": executable});
        let readme = recorded(&fixture, "import_content", import("hello", false)).await;
        assert_eq!(readme["kind"], "file");
        assert_eq!(readme["bytes"], 5);
        let script = recorded(&fixture, "import_content", import("#!/bin/sh\n", true)).await;
        let bin = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt, "entries": {"run": script["output_id"]}}),
        )
        .await;
        let root = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt,
                "entries": {"README.md": readme["output_id"], "bin": bin["output_id"]}}),
        )
        .await;
        assert_eq!(root["kind"], "directory");
        assert_eq!(root["entries"], 3);
        // A file is composed into a directory, never sent as one.
        let file = fixture
            .error(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": readme["output_id"]}}),
            )
            .await;
        assert_eq!(file.code, "invalid_arguments");
        let accepted = fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": root["output_id"]}}),
            )
            .await;
        assert_eq!(accepted["status"], "accepted");
        assert_eq!(
            sink_files(&fixture).await,
            files(&[("README.md", "hello"), ("bin/run*", "#!/bin/sh\n")])
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn changes_replace_and_delete_paths_of_a_received_workspace() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        deliver_workspace(
            &fixture,
            &[
                ("keep.txt", "kept"),
                ("old.txt", "stale"),
                ("src/main.rs", "fn main() {}"),
            ],
        )
        .await;
        let begun = begin(&fixture).await;
        let (attempt, input) = (&begun["attempt_id"], &begun["inputs"][0]);
        assert_eq!(input["workspace"], true);
        let main = recorded(
            &fixture,
            "import_content",
            json!({"attempt_id": attempt, "text": "fn main() { run() }"}),
        )
        .await;
        let notes = recorded(
            &fixture,
            "import_content",
            json!({"attempt_id": attempt, "text": "notes"}),
        )
        .await;
        let replaced = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt, "base": input["handle"],
                "changes": {"src/main.rs": main["output_id"], "docs/notes.md": notes["output_id"]}}),
        )
        .await;
        // Changes stack on the attempt's own directories too.
        let pruned = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt, "base": replaced["output_id"], "changes": {"old.txt": null}}),
        )
        .await;
        let accepted = fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": pruned["output_id"]}}),
            )
            .await;
        assert_eq!(accepted["status"], "accepted");
        assert_eq!(
            sink_files(&fixture).await,
            files(&[
                ("docs/notes.md", "notes"),
                ("keep.txt", "kept"),
                ("src/main.rs", "fn main() { run() }"),
            ])
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn directories_inside_an_input_are_refused_but_its_files_are_reused() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        deliver_workspace(&fixture, &[("src/lib.rs", "pub fn lib() {}")]).await;
        let begun = begin(&fixture).await;
        let attempt = &begun["attempt_id"];
        let src = member(&fixture, attempt, &begun["inputs"][0]["handle"], "src").await;
        for (tool, args) in [
            ("compose_package", json!({"entries": {"src": src}})),
            ("compose_package", json!({"base": src, "changes": {}})),
            ("open_workspace", json!({"handle": src})),
        ] {
            let mut args = args;
            args["attempt_id"] = attempt.clone();
            let refused = fixture.error(tool, args).await;
            assert_eq!(refused.code, "invalid_arguments", "{tool}");
            assert!(
                refused.message.contains("directory inside an input"),
                "{tool}: {}",
                refused.message
            );
        }
        let lib = member(&fixture, attempt, &src, "src/lib.rs").await;
        let copied = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt, "entries": {"lib.rs": lib}}),
        )
        .await;
        fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": copied["output_id"]}}),
            )
            .await;
        assert_eq!(
            sink_files(&fixture).await,
            files(&[("lib.rs", "pub fn lib() {}")])
        );
        fixture.stop().await;
    }

    #[tokio::test]
    async fn invalid_imports_and_compositions_keep_the_attempt_open() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        fixture.deliver("input").await;
        let begun = begin(&fixture).await;
        let attempt = &begun["attempt_id"];
        let file = recorded(
            &fixture,
            "import_content",
            json!({"attempt_id": attempt, "text": "x"}),
        )
        .await["output_id"]
            .clone();
        for (tool, args, code) in [
            (
                "import_content",
                json!({"text": "x".repeat(MAX_IMPORT + 1)}),
                "invalid_arguments",
            ),
            (
                "compose_package",
                json!({"entries": {"a": file}, "base": file, "changes": {}}),
                "invalid_arguments",
            ),
            (
                "compose_package",
                json!({"base": file}),
                "invalid_arguments",
            ),
            (
                "compose_package",
                json!({"base": file, "changes": {}}),
                "invalid_arguments",
            ),
            (
                "compose_package",
                json!({"entries": {"a": "out_99"}}),
                "unknown_handle",
            ),
            // A message input names no content to compose.
            (
                "compose_package",
                json!({"entries": {"a": begun["inputs"][0]["handle"]}}),
                "unknown_handle",
            ),
            (
                "open_workspace",
                json!({"handle": file}),
                "invalid_arguments",
            ),
            // Core checks names as it stores them, and the view as a filesystem.
            (
                "compose_package",
                json!({"entries": {"a/b": file}}),
                "invalid_workspace",
            ),
            (
                "compose_package",
                json!({"entries": {"README": file, "readme": file}}),
                "invalid_workspace",
            ),
        ] {
            let mut args = args;
            args["attempt_id"] = attempt.clone();
            assert_eq!(fixture.error(tool, args).await.code, code, "{tool}");
        }
        let accepted = fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"message": "fine"}}),
            )
            .await;
        assert_eq!(accepted["status"], "accepted");
        fixture.stop().await;
    }
}
