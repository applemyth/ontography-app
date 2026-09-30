//! Reading an attempt's inputs through their grants. Core checks reads and
//! listings against the attempt's budget and records their replies itself.

use super::context::{Attempt, context_error};
use super::{NodeToolContext, Reply, Tool};
use crate::{AppError, Result};
use ontography::ResolvedEntryKind;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

/// Largest byte range one read returns; read larger payloads in ranges.
const MAX_READ: u64 = 256 * 1024;
/// Largest collection one listing returns; open larger ones as a workspace.
const MAX_LISTED: usize = 1000;
/// Most text of member paths and link targets one listing returns. Core's
/// listing names each path twice, and JSON escaping at most doubles text,
/// both in the listing and in the MCP result carrying it: with its other
/// fields, a listing within this fits one 4 MiB MCP frame.
const MAX_LISTED_TEXT: usize = 256 * 1024;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Target {
    attempt_id: String,
    /// An input, collection member, or root input handle from this attempt.
    handle: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Range {
    attempt_id: String,
    /// An input, file member, or root input handle from this attempt.
    handle: String,
    /// First byte to read; defaults to 0.
    #[serde(default)]
    start: Option<u64>,
    /// End of the range, exclusive; defaults to at most 262144 bytes after start.
    #[serde(default)]
    end: Option<u64>,
}

pub(super) struct DescribePackage;

impl Tool for DescribePackage {
    const NAME: &'static str = "describe_package";
    const DESCRIPTION: &'static str = "Describe one of this attempt's inputs or collection members: whether it is a message, workspace, directory, file, or link, and its size or path.";
    const MUTATING: bool = false;
    type Input = Target;

    async fn run(context: &NodeToolContext, target: Target) -> Result<Reply> {
        context
            .with_attempt(&target.attempt_id, async |attempt, _| {
                let described = describe(context, attempt, &target.handle).await?;
                Reply::record(attempt, Self::NAME, &described).await
            })
            .await
    }
}

pub(super) struct ListPackage;

impl Tool for ListPackage {
    const NAME: &'static str = "list_package";
    const DESCRIPTION: &'static str = "List the immediate members of a collection among this attempt's inputs. Collections of more than 1000 entries, or whose paths total more than 262144 bytes, are refused; open them as a workspace.";
    const MUTATING: bool = false;
    type Input = Target;

    async fn run(context: &NodeToolContext, target: Target) -> Result<Reply> {
        context
            .with_attempt(&target.attempt_id, async |attempt, state| {
                if let Some(children) = state.children(attempt, &target.handle).await? {
                    let count = children.len();
                    if count > MAX_LISTED {
                        return Err(AppError::new(
                            "too_many_members",
                            format!(
                                "This collection has {count} entries; open it as a workspace instead"
                            ),
                        ));
                    }
                    // Checked before core records the listing, which must
                    // then fit one frame to reach the worker.
                    let text: usize = children
                        .iter()
                        .map(|child| match &child.kind {
                            ResolvedEntryKind::Symlink { target } => child.path.len() + target.len(),
                            _ => child.path.len(),
                        })
                        .sum();
                    if text > MAX_LISTED_TEXT {
                        return Err(AppError::new(
                            "too_large",
                            format!(
                                "This collection's paths total {text} bytes; open it as a workspace instead"
                            ),
                        ));
                    }
                }
                core_call(attempt, "package.list", json!({"handle": target.handle})).await
            })
            .await
    }
}

pub(super) struct ReadPackage;

impl Tool for ReadPackage {
    const NAME: &'static str = "read_package";
    const DESCRIPTION: &'static str = "Read bytes of a message, file, or other payload among this attempt's inputs, at most 262144 per call; pass start and end to read further.";
    const MUTATING: bool = false;
    type Input = Range;

    async fn run(context: &NodeToolContext, range: Range) -> Result<Reply> {
        context
            .with_attempt(&range.attempt_id, async |attempt, _| {
                let start = range.start.unwrap_or(0);
                let end = match range.end {
                    Some(end) => end,
                    None => payload_size(context, attempt, &range.handle)
                        .await?
                        .unwrap_or(0)
                        .min(start.saturating_add(MAX_READ)),
                };
                if end.saturating_sub(start) > MAX_READ {
                    return Err(AppError::invalid(format!(
                        "Read at most {MAX_READ} bytes per call"
                    )));
                }
                core_call(
                    attempt,
                    "package.read",
                    json!({"handle": range.handle, "start": start, "end": end}),
                )
                .await
            })
            .await
    }
}

async fn core_call(attempt: &Attempt, operation: &str, args: Value) -> Result<Reply> {
    let response = attempt
        .invocation
        .call(operation, args)
        .await
        .map_err(context_error)?;
    Reply::recorded_by_core(attempt, response)
}

/// What a handle names, in workflow terms: never the core identities that
/// core's own description carries.
async fn describe(context: &NodeToolContext, attempt: &Attempt, handle: &str) -> Result<Value> {
    if let Some(member) = attempt.member(handle) {
        // A root member is a whole input view: a workspace.
        let mut described = json!({"handle": handle, "path": member.path});
        match &member.kind {
            ResolvedEntryKind::Directory => {
                described["kind"] = json!(if member.path.is_empty() {
                    "workspace"
                } else {
                    "directory"
                });
            }
            ResolvedEntryKind::File {
                content,
                executable,
            } => {
                described["kind"] = json!("file");
                described["bytes"] = json!(content.size());
                described["executable"] = json!(executable);
            }
            ResolvedEntryKind::Symlink { target } => {
                described["kind"] = json!("link");
                described["target"] = json!(target);
            }
        }
        return Ok(described);
    }
    let bytes = payload_size(context, attempt, handle)
        .await?
        .ok_or_else(|| {
            AppError::new(
                "unknown_handle",
                format!("{handle:?} is not an input or member of this attempt"),
            )
        })?;
    Ok(json!({"handle": handle, "kind": "message", "bytes": bytes}))
}

/// The payload size behind an input, file, or root input handle.
async fn payload_size(
    context: &NodeToolContext,
    attempt: &Attempt,
    handle: &str,
) -> Result<Option<u64>> {
    if let Some(ResolvedEntryKind::File { content, .. }) =
        attempt.member(handle).map(|member| member.kind)
    {
        return Ok(Some(content.size()));
    }
    let digest = match (attempt.grant(handle), &attempt.root_input) {
        (Some(grant), _) => grant.content_digest,
        (None, Some((root, digest))) if root == handle => *digest,
        _ => return Ok(None),
    };
    context
        .execution
        .content_size(digest)
        .await
        .map_err(AppError::core)
}
