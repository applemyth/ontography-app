//! Tools that move or discard work, available only with the matching grant.

use super::context::successor_edge;
use super::node::{Page, find, stale_work};
use super::{NodeToolContext, Reply, Tool};
use crate::workflow::Grant;
use crate::workflow::tasks::{Held, TaskKey, work_id};
use crate::{AppError, Result, views};
use ontography::{RetireError, RewriteError, TransferError, TransferRejection};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

pub(super) struct ListOutbound;

impl Tool for ListOutbound {
    const NAME: &'static str = "list_outbound";
    const DESCRIPTION: &'static str =
        "Page through packages this node created to send later, in stable order.";
    const GRANT: Option<Grant> = Some(Grant::SendLater);
    const MUTATING: bool = false;
    type Input = Page;

    async fn run(context: &NodeToolContext, page: Page) -> Result<Reply> {
        let found = page.read(context, Held::Outbound).await?;
        let mut packages = Vec::with_capacity(found.packages.len());
        for (id, record) in &found.packages {
            let bytes = context
                .execution
                .content_size(record.content_digest())
                .await
                .map_err(AppError::core)?;
            packages.push(json!({"work_id": work_id(id), "bytes": bytes}));
        }
        Reply::plain(&json!({"packages": packages, "next_after": found.next_after}))
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Transfer {
    /// An outbound package from list_outbound.
    work_id: String,
    /// The successor to deliver it to.
    to: String,
}

pub(super) struct TransferPackage;

impl Tool for TransferPackage {
    const NAME: &'static str = "transfer_package";
    const DESCRIPTION: &'static str =
        "Deliver one of this node's outbound packages to a successor.";
    const GRANT: Option<Grant> = Some(Grant::SendLater);
    const MUTATING: bool = true;
    type Input = Transfer;

    async fn run(context: &NodeToolContext, transfer: Transfer) -> Result<Reply> {
        // Finding the package and moving it happen with no other move between.
        let _exclusive = context.exclusive().await?;
        let (package, _) = find(context, Held::Outbound, &transfer.work_id)
            .await?
            .ok_or_else(stale_work)?;
        let (kernel, names) = context.graph().await?;
        let successors = context.successors(&kernel, &names);
        context
            .session
            .transfer(package, successor_edge(&successors, &transfer.to)?)
            .await
            .map_err(AppError::core)?
            .map_err(|error| transfer_refusal(&error, &transfer.to))?;
        Reply::plain(
            &json!({"status": "delivered", "work_id": transfer.work_id, "to": transfer.to}),
        )
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct Retire {
    /// An input waiting here, or one of this node's outbound packages.
    work_id: String,
}

pub(super) struct RetirePackage;

impl Tool for RetirePackage {
    const NAME: &'static str = "retire_package";
    const DESCRIPTION: &'static str = "Discard an input waiting at this node, or one of its outbound packages, without processing it. Core keeps the retirement on record.";
    const GRANT: Option<Grant> = Some(Grant::Retire);
    const MUTATING: bool = true;
    type Input = Retire;

    async fn run(context: &NodeToolContext, retire: Retire) -> Result<Reply> {
        // Core retires any live package, wherever it is: find it here and
        // retire it with no transfer between.
        let _exclusive = context.exclusive().await?;
        let held = match find(context, Held::Received, &retire.work_id).await? {
            Some(input) => input,
            None => find(context, Held::Outbound, &retire.work_id)
                .await?
                .ok_or_else(stale_work)?,
        };
        let package = held.0;
        context
            .session
            .retire(package, None)
            .await
            .map_err(AppError::core)?
            .map_err(|error| match error {
                RetireError::NotLive(_) => stale_work(),
                error => views::retire_refusal(&error),
            })?;
        // A discarded input needs no further retries.
        context
            .ledger
            .clear(&TaskKey::new(context.node_id(), &[package]))?;
        Reply::plain(&json!({"status": "retired", "work_id": retire.work_id}))
    }
}

/// Why core refused a transfer, in workflow terms: core's own message names
/// the package and the connection.
fn transfer_refusal(error: &TransferError, to: &str) -> AppError {
    match error {
        TransferError::NotLive(_) | TransferError::NotOutbound(_) => stale_work(),
        TransferError::InvalidEdge(_) => {
            AppError::invalid(format!("{to:?} is no longer a successor of this node"))
        }
        TransferError::Rejected { reason, .. } => {
            let reason = match reason {
                TransferRejection::ObjectType { expected, actual } => {
                    format!("it takes {expected}, not {actual}")
                }
                TransferRejection::Authority => "the package's authority does not match".into(),
                TransferRejection::Contract { source, .. } => source.to_string(),
            };
            AppError::new(
                "rejected",
                format!("The connection to {to:?} refused the package: {reason}"),
            )
        }
        TransferError::Admission(RewriteError::Stale) => views::stale(),
        TransferError::Admission(_) => AppError::new("rejected", "Core refused the transfer"),
    }
}
