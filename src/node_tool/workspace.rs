//! Private filesystem checkouts of an attempt's directories, opened on demand.

use super::context::OutputRef;
use super::outputs::{Source, resolve};
use super::{NodeToolContext, Reply, Tool};
use crate::tools::workspace::workspace_error;
use crate::workspace::{AttemptCheckout, WorkspaceError};
use crate::{AppError, Result};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Open {
    attempt_id: String,
    /// A workspace input of this attempt, or one of its directory outputs.
    handle: String,
    /// Whether edits may be captured; a read-only checkout rejects changes.
    #[serde(default = "writable")]
    writable: bool,
}

const fn writable() -> bool {
    true
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkout {
    attempt_id: String,
    /// A checkout from open_workspace.
    workspace_id: String,
}

pub(super) struct OpenWorkspace;

impl Tool for OpenWorkspace {
    const NAME: &'static str = "open_workspace";
    const DESCRIPTION: &'static str = "Check out a workspace input or directory output as a private directory. Returns its path and a workspace_id.";
    const MUTATING: bool = true;
    type Input = Open;

    async fn run(context: &NodeToolContext, open: Open) -> Result<Reply> {
        context
            .with_attempt(&open.attempt_id, async |attempt, state| {
                let source = resolve(attempt, state, &open.handle)?;
                if !source.is_directory() {
                    return Err(AppError::invalid(format!(
                        "{:?} is not a directory; only a directory opens as a workspace",
                        open.handle
                    )));
                }
                // A capture holds its validated view already; others are resolved here.
                let view = match source {
                    Source::Output(OutputRef {
                        capture: Some(capture),
                        ..
                    }) => capture.package().clone(),
                    _ => context
                        .workspaces
                        .open(source.root())
                        .await
                        .map_err(workspace_error)?,
                };
                let workspace = state.handle("ws");
                let checkout = AttemptCheckout::open(
                    &context.workspaces,
                    &view,
                    &format!("{}-{workspace}", attempt.id),
                    open.writable,
                )
                .await
                .map_err(workspace_error)?;
                let mut reply = checkout.exposure();
                reply["workspace_id"] = json!(workspace);
                state.workspaces.insert(workspace, checkout);
                Reply::record(attempt, Self::NAME, &reply).await
            })
            .await
    }
}

pub(super) struct CaptureWorkspace;

impl Tool for CaptureWorkspace {
    const NAME: &'static str = "capture_workspace";
    const DESCRIPTION: &'static str = "Capture a checkout's current files as a directory output. Stop anything writing to it first.";
    const MUTATING: bool = true;
    type Input = Checkout;

    async fn run(context: &NodeToolContext, checkout: Checkout) -> Result<Reply> {
        context
            .with_attempt(&checkout.attempt_id, async |attempt, state| {
                let workspace = state
                    .workspaces
                    .get(&checkout.workspace_id)
                    .ok_or_else(|| not_open(&checkout.workspace_id))?;
                let capture = match workspace.capture(&context.workspaces).await {
                    Ok(capture) => capture,
                    Err(WorkspaceError::ReadOnly(_)) => {
                        return Err(AppError::new(
                            "workspace_read_only",
                            format!(
                                "{:?} is read-only but its files changed; open the directory writable to capture edits",
                                checkout.workspace_id
                            ),
                        ));
                    }
                    Err(error) => return Err(workspace_error(error)),
                };
                let root = capture.package().root();
                let changed = root != workspace.base();
                let output = state.handle("out");
                let reply = json!({"output_id": output, "kind": "directory",
                    "entries": capture.package().entry_count().saturating_sub(1),
                    "changed": changed});
                state.outputs.insert(
                    output,
                    OutputRef {
                        root,
                        directory: true,
                        dependencies: capture.package().dependencies(),
                        capture: Some(capture),
                    },
                );
                Reply::record(attempt, Self::NAME, &reply).await
            })
            .await
    }
}

pub(super) struct ReleaseWorkspace;

impl Tool for ReleaseWorkspace {
    const NAME: &'static str = "release_workspace";
    const DESCRIPTION: &'static str =
        "Remove a checkout early. Checkouts are also removed when their attempt ends.";
    const MUTATING: bool = true;
    type Input = Checkout;

    async fn run(context: &NodeToolContext, checkout: Checkout) -> Result<Reply> {
        context
            .with_attempt(&checkout.attempt_id, async |attempt, state| {
                let workspace = state
                    .workspaces
                    .remove(&checkout.workspace_id)
                    .ok_or_else(|| not_open(&checkout.workspace_id))?;
                // Outputs captured from it keep their own pins.
                workspace.remove().await.map_err(workspace_error)?;
                Reply::record(
                    attempt,
                    Self::NAME,
                    &json!({"status": "released", "workspace_id": checkout.workspace_id}),
                )
                .await
            })
            .await
    }
}

fn not_open(workspace: &str) -> AppError {
    AppError::new(
        "unknown_handle",
        format!("{workspace:?} is not an open checkout of this attempt"),
    )
}

#[cfg(test)]
mod tests {
    use crate::node_tool::tests::{
        Fixture, begin, deliver_workspace, document, files, recorded, sink_files,
    };
    use serde_json::{Value, json};
    use std::{os::unix::fs::PermissionsExt, path::PathBuf};

    fn path(opened: &Value) -> PathBuf {
        PathBuf::from(opened["path"].as_str().unwrap())
    }

    #[tokio::test]
    async fn edits_in_a_checkout_are_captured_and_published() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        let root = deliver_workspace(&fixture, &[("a.txt", "one"), ("b.txt", "two")]).await;
        let begun = begin(&fixture).await;
        let attempt = &begun["attempt_id"];
        let opened = recorded(
            &fixture,
            "open_workspace",
            json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"]}),
        )
        .await;
        // The receipt names exactly the view exposed at the path.
        assert_eq!(opened["root"], json!(root));
        assert_eq!(opened["writable"], true);
        let checkout = path(&opened);
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one"
        );
        std::fs::write(checkout.join("a.txt"), "edited").unwrap();
        std::fs::remove_file(checkout.join("b.txt")).unwrap();
        std::fs::create_dir(checkout.join("c")).unwrap();
        std::fs::write(checkout.join("c/d.txt"), "new").unwrap();
        let captured = recorded(
            &fixture,
            "capture_workspace",
            json!({"attempt_id": attempt, "workspace_id": opened["workspace_id"]}),
        )
        .await;
        assert_eq!(captured["changed"], true);
        let accepted = fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": captured["output_id"]}}),
            )
            .await;
        assert_eq!(accepted["status"], "accepted");
        assert_eq!(
            sink_files(&fixture).await,
            files(&[("a.txt", "edited"), ("c/d.txt", "new")])
        );
        // Ending the attempt removed its checkout.
        assert!(!checkout.exists());
        fixture.stop().await;
    }

    #[tokio::test]
    async fn a_read_only_checkout_refuses_edits_at_capture() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        deliver_workspace(&fixture, &[("a.txt", "one")]).await;
        let begun = begin(&fixture).await;
        let attempt = &begun["attempt_id"];
        let opened = recorded(
            &fixture,
            "open_workspace",
            json!({"attempt_id": attempt, "handle": begun["inputs"][0]["handle"], "writable": false}),
        )
        .await;
        assert_eq!(opened["writable"], false);
        let file = path(&opened).join("a.txt");
        assert!(std::fs::metadata(&file).unwrap().permissions().readonly());
        let capture = json!({"attempt_id": attempt, "workspace_id": opened["workspace_id"]});
        let unchanged = recorded(&fixture, "capture_workspace", capture.clone()).await;
        assert_eq!(unchanged["changed"], false);
        // Permissions are advisory: the owner can lift them, so capture checks.
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::write(&file, "edited").unwrap();
        assert_eq!(
            fixture.error("capture_workspace", capture).await.code,
            "workspace_read_only"
        );
        fixture
            .ok(
                "fail_invocation",
                json!({"attempt_id": attempt, "reason": "edited a read-only checkout"}),
            )
            .await;
        assert!(!path(&opened).exists());
        fixture.stop().await;
    }

    #[tokio::test]
    async fn a_released_checkout_is_removed_and_its_capture_stays_valid() {
        let fixture = Fixture::new(document(json!({})), "worker", None).await;
        fixture.deliver("draft it").await;
        let attempt = begin(&fixture).await["attempt_id"].clone();
        let draft = recorded(
            &fixture,
            "import_content",
            json!({"attempt_id": attempt, "text": "draft"}),
        )
        .await;
        let directory = recorded(
            &fixture,
            "compose_package",
            json!({"attempt_id": attempt, "entries": {"draft.txt": draft["output_id"]}}),
        )
        .await;
        let open = json!({"attempt_id": attempt, "handle": directory["output_id"]});
        let first = recorded(&fixture, "open_workspace", open.clone()).await;
        let second = recorded(&fixture, "open_workspace", open).await;
        assert_ne!(first["path"], second["path"]);
        std::fs::write(path(&first).join("draft.txt"), "final").unwrap();
        let captured = recorded(
            &fixture,
            "capture_workspace",
            json!({"attempt_id": attempt, "workspace_id": first["workspace_id"]}),
        )
        .await;
        let release = json!({"attempt_id": attempt, "workspace_id": first["workspace_id"]});
        let released = recorded(&fixture, "release_workspace", release.clone()).await;
        assert_eq!(released["status"], "released");
        assert!(!path(&first).exists());
        assert!(path(&second).exists());
        assert_eq!(
            fixture.error("release_workspace", release).await.code,
            "unknown_handle"
        );
        // A capture reopens from its own validated view.
        let reopened = recorded(
            &fixture,
            "open_workspace",
            json!({"attempt_id": attempt, "handle": captured["output_id"]}),
        )
        .await;
        assert_eq!(
            std::fs::read_to_string(path(&reopened).join("draft.txt")).unwrap(),
            "final"
        );
        fixture
            .ok(
                "submit_invocation",
                json!({"attempt_id": attempt, "result": {"workspace": captured["output_id"]}}),
            )
            .await;
        assert_eq!(sink_files(&fixture).await, files(&[("draft.txt", "final")]));
        assert!(!path(&second).exists());
        assert!(!path(&reopened).exists());
        fixture.stop().await;
    }
}
