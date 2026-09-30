//! Bounded content reads and helpers shared by workflow artifact handling.

use super::workflow::{check_limit, content_id_schema, default_limit, operation, parse_args};
use crate::catalog::Operation;
use crate::state::ManagedRun;
use crate::{AppError, Result, views};
use ontography::{ContentId, ContentStore, package::ResolvedPackage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

/// The core serializes u64 lengths as numbers. JavaScript callers can instead
/// carry an identity's size as a decimal string without rounding its assertion.
pub fn normalize_content_ids_input(value: &mut Value) -> Result<()> {
    match value {
        Value::Array(items) => {
            for item in items {
                normalize_content_ids_input(item)?;
            }
        }
        Value::Object(fields) => {
            if fields.get("hash").is_some_and(Value::is_string)
                && matches!(
                    fields.get("format").and_then(Value::as_str),
                    Some("Raw" | "HashSeq")
                )
                && let Some(Value::String(size)) = fields.get("size")
            {
                let size: u64 = size.parse().map_err(|_| {
                    AppError::invalid("content size must be an unsigned decimal integer")
                })?;
                fields.insert("size".into(), json!(size));
            }
            for item in fields.values_mut() {
                normalize_content_ids_input(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Apply once at the response boundary; embedded payload strings stay exact.
pub fn normalize_content_ids_output(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                normalize_content_ids_output(item);
            }
        }
        Value::Object(fields) => {
            if fields.get("hash").is_some_and(Value::is_string)
                && matches!(
                    fields.get("format").and_then(Value::as_str),
                    Some("Raw" | "HashSeq")
                )
                && let Some(size) = fields.get("size").and_then(Value::as_u64)
                && size > 9_007_199_254_740_991
            {
                fields.insert("size".into(), json!(size.to_string()));
            }
            for item in fields.values_mut() {
                normalize_content_ids_output(item);
            }
        }
        _ => {}
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdInput {
    content_id: ContentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    content_id: ContentId,
    #[serde(default)]
    start: Offset,
    #[serde(default = "read_length")]
    length: u64,
}
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Offset {
    Decimal(String),
    Number(u64),
}
impl Default for Offset {
    fn default() -> Self {
        Self::Number(0)
    }
}
impl Offset {
    pub(crate) fn value(self) -> Result<u64> {
        match self {
            Self::Number(n) => Ok(n),
            Self::Decimal(s) => s
                .parse()
                .map_err(|_| AppError::invalid("offset must be an unsigned decimal integer")),
        }
    }
}
fn read_length() -> u64 {
    65536
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RootPage {
    pub root: ContentId,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}
/// All caller paths resolve against the run's project, never the daemon cwd.
pub(crate) fn local_path(run: &ManagedRun, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        run.manifest.project.join(path)
    }
}

/// A page is bounded by count and by serialized size; a single oversized record
/// reports an export/read alternative instead of silently truncating its fields.
pub(crate) fn page<T: Serialize>(items: &[T], offset: usize, limit: usize) -> Result<Value> {
    check_limit(limit)?;
    if offset > items.len() {
        return Err(AppError::invalid("offset exceeds result length"));
    }
    let mut out = Vec::new();
    let mut bytes = 0;
    for item in items.iter().skip(offset).take(limit) {
        let item = serde_json::to_value(item)?;
        let size = serde_json::to_vec(&item)?.len();
        if bytes + size > 1024 * 1024 {
            if out.is_empty() {
                return Err(AppError::new(
                    "result_too_large",
                    "one record exceeds the page budget; export its content and read it locally",
                ));
            }
            break;
        }
        bytes += size;
        out.push(item);
    }
    let end = offset + out.len();
    Ok(
        json!({"items":out,"total":items.len(),"offset":offset,"next_offset":(end < items.len()).then_some(end)}),
    )
}

pub(crate) fn resolved_view(
    package: &ResolvedPackage,
    offset: usize,
    limit: usize,
) -> Result<Value> {
    let mut result = page(&package.entries(), offset, limit)?;
    result["root"] = json!(package.root());
    result["dependency_count"] = json!(package.dependencies().len());
    Ok(result)
}

/// Retain ordinary artifacts without using core's protected ledger namespace.
pub(crate) async fn retain(store: &ContentStore, ids: &[ContentId]) -> Result<()> {
    let imports = store.stage_imports();
    imports.protect(ids).await.map_err(AppError::core)?;
    imports.retain().await.map_err(AppError::core)
}

pub async fn dispatch(run: &mut ManagedRun, name: &str, args: &Value) -> Result<Value> {
    let store = run
        .live()?
        .session
        .content_store()
        .await
        .map_err(AppError::core)?;
    match name {
        "content.metadata" => {
            let input: IdInput = parse_args(run, args)?;
            let metadata = store
                .metadata(input.content_id)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":metadata.id,"complete":metadata.complete}))
        }
        "content.read" => {
            let input: ReadInput = parse_args(run, args)?;
            if input.length > 65536 {
                return Err(AppError::invalid("length must not exceed 65536"));
            }
            let start = input.start.value()?;
            if start > input.content_id.size() {
                return Err(AppError::invalid("start exceeds content size"));
            }
            let end = start
                .saturating_add(input.length)
                .min(input.content_id.size());
            let bytes = store
                .read_range(input.content_id, start..end)
                .await
                .map_err(AppError::core)?;
            Ok(
                json!({"content_id":input.content_id,"start":start.to_string(),"end":end.to_string(),"data":views::bytes(&bytes),"next_start":(end<input.content_id.size()).then(||end.to_string())}),
            )
        }
        _ => Err(AppError::new("unknown_operation", name)),
    }
}

pub fn operations() -> Vec<Operation> {
    let id = content_id_schema();
    let text = json!({"type":"string"});
    vec![
        operation(
            "content.metadata",
            "Check exact local availability of a content identity.",
            json!({"content_id":id}),
            &["content_id"],
            false,
        ),
        operation(
            "content.read",
            "Read verified bytes, at most 64KiB. Byte offsets accept decimal strings.",
            json!({"content_id":id,"start":{"anyOf":[text,{"type":"integer","minimum":0}]},"length":{"type":"integer","minimum":0,"maximum":65536}}),
            &["content_id"],
            false,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::workflow::tests::test_run;

    #[test]
    fn large_content_sizes_round_trip_without_javascript_precision_loss() {
        let mut value = json!({"content_id":{"hash":"hash","format":"Raw","size":u64::MAX},"payload":"{\"size\":18446744073709551615}"});
        normalize_content_ids_output(&mut value);
        assert_eq!(value["content_id"]["size"], u64::MAX.to_string());
        normalize_content_ids_input(&mut value).unwrap();
        assert_eq!(value["content_id"]["size"].as_u64(), Some(u64::MAX));
        assert_eq!(value["payload"], "{\"size\":18446744073709551615}");
    }

    #[tokio::test]
    async fn metadata_and_bounded_reads_use_owned_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut run = test_run(dir.path());
        let run_id = run.manifest.run_id.clone();
        let store = run.live().unwrap().session.content_store().await.unwrap();
        let id = store.import_bytes(vec![7u8; 150_000]).await.unwrap();
        let metadata = dispatch(
            &mut run,
            "content.metadata",
            &json!({"run_id":run_id,"content_id":id}),
        )
        .await
        .unwrap();
        assert_eq!(metadata["complete"], true);
        let read = dispatch(
            &mut run,
            "content.read",
            &json!({"run_id":run_id,"content_id":id,"start":"149998","length":65536}),
        )
        .await
        .unwrap();
        assert_eq!(read["end"], "150000");
        assert_eq!(read["data"]["length"], 2);
        assert!(
            dispatch(
                &mut run,
                "content.read",
                &json!({"run_id":run_id,"content_id":id,"length":65537})
            )
            .await
            .is_err()
        );
        assert!(
            dispatch(
                &mut run,
                "content.read",
                &json!({"run_id":run_id,"content_id":id,"start":"150001"})
            )
            .await
            .is_err()
        );
    }
}
