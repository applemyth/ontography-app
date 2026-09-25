//! Content identities and composable packages remain core-owned values.

use super::workflow::{
    check_limit, content_id_schema, default_limit, operation, parse_args, payload_schema,
};
use crate::catalog::Operation;
use crate::state::ManagedRun;
use crate::{AppError, Result, views};
use ontography::{
    ContentId, ContentStore,
    package::{PackageDocument, PackageEnvelope, PackageStore, ResolvedPackage},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

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
struct BytesInput {
    payload: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathInput {
    path: PathBuf,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdInput {
    content_id: ContentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootInput {
    root: ContentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DigestInput {
    digest: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportInput {
    content_id: ContentId,
    path: PathBuf,
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
struct SequenceInput {
    children: Vec<ContentId>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CollectionEntry {
    name: String,
    content_id: ContentId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionInput {
    entries: Vec<CollectionEntry>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdPage {
    content_id: ContentId,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentInput {
    document: PackageDocument,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// All caller paths resolve against the run's project, never the daemon cwd.
pub(crate) fn local_path(run: &ManagedRun, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        run.manifest.project.join(path)
    }
}

/// Core artifact tags are shared/idempotent. Removing one must not break an
/// app-owned checkpoint or the immutable base needed to capture a live checkout.
pub(crate) async fn ensure_releasable(
    run: &ManagedRun,
    store: &ContentStore,
    id: ContentId,
) -> Result<()> {
    let same_tag = |other: ContentId| other.hash() == id.hash() && other.format() == id.format();
    if run.protected(id)
        || run.manifest.checkpoints.values().any(|checkpoint| {
            same_tag(checkpoint.base)
                || same_tag(checkpoint.root)
                || checkpoint.dependencies.iter().copied().any(same_tag)
        })
    {
        return Err(AppError::new(
            "resource_in_use",
            "content belongs to a saved workspace checkpoint",
        ));
    }
    let packages = PackageStore::new(store.clone());
    for handle in run.live()?.checkouts.values() {
        if packages
            .dependencies(handle.base)
            .await
            .map_err(AppError::core)?
            .into_iter()
            .any(same_tag)
        {
            return Err(AppError::new(
                "resource_in_use",
                "content belongs to a retained checkout base",
            ));
        }
    }
    Ok(())
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
    let mut result = page(package.entries(), offset, limit)?;
    result["root"] = json!(package.root());
    result["dependency_count"] = json!(package.dependencies().len());
    Ok(result)
}

pub async fn dispatch(run: &mut ManagedRun, name: &str, args: &Value) -> Result<Value> {
    let store = run
        .live()?
        .session
        .content_store()
        .await
        .map_err(AppError::core)?;
    match name {
        "content.import_bytes" => {
            let input: BytesInput = parse_args(run, args)?;
            let id = store
                .import_bytes(views::payload(&input.payload)?.to_vec())
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":id}))
        }
        "content.import_file" | "content.import_stream" => {
            let input: PathInput = parse_args(run, args)?;
            let path = local_path(run, &input.path);
            let id = if name == "content.import_file" {
                store.import_file(path).await
            } else {
                let file = tokio::fs::File::open(path).await?;
                let stream = futures_util::stream::try_unfold(file, |mut file| async move {
                    let mut bytes = vec![0u8; 65536];
                    let n = file.read(&mut bytes).await?;
                    if n == 0 {
                        Ok(None)
                    } else {
                        bytes.truncate(n);
                        Ok(Some((bytes.into(), file)))
                    }
                });
                store.import_stream(stream).await
            }
            .map_err(AppError::core)?;
            Ok(json!({"content_id":id}))
        }
        "content.metadata" => {
            let input: IdInput = parse_args(run, args)?;
            let metadata = store
                .metadata(input.content_id)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":metadata.id,"complete":metadata.complete}))
        }
        "content.resolve" => {
            let input: DigestInput = parse_args(run, args)?;
            let id = store
                .resolve(views::digest(&input.digest)?)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":id}))
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
        "content.export" => {
            let input: ExportInput = parse_args(run, args)?;
            let path = local_path(run, &input.path);
            store
                .export_file(input.content_id, &path)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"path":path,"content_id":input.content_id}))
        }
        "content.retain" | "content.release" => {
            let input: IdInput = parse_args(run, args)?;
            if name == "content.retain" {
                store
                    .retain(input.content_id)
                    .await
                    .map_err(AppError::core)?;
                Ok(json!({"retained":input.content_id}))
            } else {
                ensure_releasable(run, &store, input.content_id).await?;
                let released = store
                    .release(input.content_id)
                    .await
                    .map_err(AppError::core)?;
                Ok(json!({"content_id":input.content_id,"released":released}))
            }
        }
        "content.gc" => {
            let _: Empty = parse_args(run, args)?;
            store.collect_garbage().await.map_err(AppError::core)?;
            Ok(json!({"collected":true}))
        }
        "content.hash_sequence_put" => {
            let input: SequenceInput = parse_args(run, args)?;
            let id = store
                .import_hash_sequence(&input.children)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":id}))
        }
        "content.hash_sequence_read" => {
            let input: IdPage = parse_args(run, args)?;
            check_limit(input.limit)?;
            let hashes = store
                .read_hash_sequence(input.content_id)
                .await
                .map_err(AppError::core)?;
            page(
                &hashes.iter().map(ToString::to_string).collect::<Vec<_>>(),
                input.offset,
                input.limit,
            )
        }
        "content.collection_put" => {
            let input: CollectionInput = parse_args(run, args)?;
            let id = store
                .import_collection(input.entries.into_iter().map(|e| (e.name, e.content_id)))
                .await
                .map_err(AppError::core)?;
            Ok(json!({"content_id":id}))
        }
        "content.collection_read" => {
            let input: IdPage = parse_args(run, args)?;
            check_limit(input.limit)?;
            let entries = store
                .read_collection(input.content_id)
                .await
                .map_err(AppError::core)?
                .into_iter()
                .map(|(name, content_id)| CollectionEntry { name, content_id })
                .collect::<Vec<_>>();
            page(&entries, input.offset, input.limit)
        }
        "package.put" => {
            let input: DocumentInput = parse_args(run, args)?;
            let root = PackageStore::new(store)
                .put(&input.document)
                .await
                .map_err(AppError::core)?;
            Ok(json!({"root":root}))
        }
        "package.get" => {
            let input: RootInput = parse_args(run, args)?;
            let document = PackageStore::new(store)
                .get(input.root)
                .await
                .map_err(AppError::core)?;
            let value = json!({"root":input.root,"document":document});
            if serde_json::to_vec(&value)?.len() > 1024 * 1024 {
                return Err(AppError::new(
                    "result_too_large",
                    "package document exceeds preview budget; content.export or content.read can retrieve its exact bytes",
                ));
            }
            Ok(value)
        }
        "package.resolve" => {
            let input: RootPage = parse_args(run, args)?;
            check_limit(input.limit)?;
            let package = PackageStore::new(store)
                .resolve(input.root)
                .await
                .map_err(AppError::core)?;
            resolved_view(&package, input.offset, input.limit)
        }
        "package.dependencies" => {
            let input: RootPage = parse_args(run, args)?;
            check_limit(input.limit)?;
            let dependencies = PackageStore::new(store)
                .dependencies(input.root)
                .await
                .map_err(AppError::core)?;
            page(&dependencies, input.offset, input.limit)
        }
        "package.retain" => {
            let input: RootInput = parse_args(run, args)?;
            let dependencies = PackageStore::new(store.clone())
                .dependencies(input.root)
                .await
                .map_err(AppError::core)?;
            for &id in &dependencies {
                store.retain(id).await.map_err(AppError::core)?;
            }
            Ok(json!({"root":input.root,"retained_dependencies":dependencies.len()}))
        }
        "package.envelope" => {
            let input: RootInput = parse_args(run, args)?;
            // Resolve first: the convenience tool emits only a locally usable package.
            let dependencies = PackageStore::new(store)
                .dependencies(input.root)
                .await
                .map_err(AppError::core)?;
            let bytes = PackageEnvelope::new(input.root)
                .to_payload()
                .map_err(AppError::core)?;
            let payload = std::str::from_utf8(&bytes).map_err(AppError::core)?;
            Ok(json!({"root":input.root,"payload":payload,"dependency_count":dependencies.len()}))
        }
        "package.parse_envelope" => {
            let input: BytesInput = parse_args(run, args)?;
            let envelope = PackageEnvelope::from_payload(&views::payload(&input.payload)?)
                .map_err(AppError::core)?;
            Ok(json!({"root":envelope.map(|e|e.ontography_package)}))
        }
        _ => Err(AppError::new("unknown_operation", name)),
    }
}

pub fn operations() -> Vec<Operation> {
    let id = content_id_schema();
    let text = json!({"type":"string"});
    let page_fields = json!({"root":id,"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":1000}});
    let id_page = json!({"content_id":id,"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":1000}});
    vec![
        operation(
            "content.import_bytes",
            "Import and retain bounded bytes in the run's core store.",
            json!({"payload":payload_schema()}),
            &["payload"],
            true,
        ),
        operation(
            "content.import_file",
            "Import an owned copy of a local file; paths are absolute or relative to this run's project.",
            json!({"path":text}),
            &["path"],
            true,
        ),
        operation(
            "content.import_stream",
            "Import a local file using bounded 64KiB buffers into core's streaming import API.",
            json!({"path":text}),
            &["path"],
            true,
        ),
        operation(
            "content.metadata",
            "Check exact local availability of a content identity.",
            json!({"content_id":id}),
            &["content_id"],
            false,
        ),
        operation(
            "content.resolve",
            "Resolve a payload's domain-separated SHA-256 digest to a verified local content identity. This is not an iroh hash lookup.",
            json!({"digest":text}),
            &["digest"],
            false,
        ),
        operation(
            "content.read",
            "Read verified bytes, at most 64KiB. Byte offsets accept decimal strings.",
            json!({"content_id":id,"start":{"anyOf":[text,{"type":"integer","minimum":0}]},"length":{"type":"integer","minimum":0,"maximum":65536}}),
            &["content_id"],
            false,
        ),
        operation(
            "content.export",
            "Export verified bytes to a new file; an existing destination is rejected.",
            json!({"content_id":id,"path":text}),
            &["content_id", "path"],
            true,
        ),
        operation(
            "content.retain",
            "Retain the shared artifact tag. Core retention is idempotent, not reference counting.",
            json!({"content_id":id}),
            &["content_id"],
            true,
        ),
        operation(
            "content.release",
            "Release an incidental artifact tag. Checkpoints and live checkout bases are protected; accepted ledger dependencies remain core-retained.",
            json!({"content_id":id}),
            &["content_id"],
            true,
        ),
        operation(
            "content.gc",
            "Collect unretained content through core after all protected references remain pinned.",
            json!({}),
            &[],
            true,
        ),
        operation(
            "content.hash_sequence_put",
            "Store a native iroh hash sequence of raw child content.",
            json!({"children":{"type":"array","items":id}}),
            &["children"],
            true,
        ),
        operation(
            "content.hash_sequence_read",
            "Page the native hashes in a hash-sequence object.",
            id_page.clone(),
            &["content_id"],
            false,
        ),
        operation(
            "content.collection_put",
            "Store a native iroh named collection of raw blobs, separate from semantic package documents.",
            json!({"entries":{"type":"array","items":{"type":"object","additionalProperties":false,"properties":{"name":text,"content_id":id},"required":["name","content_id"]}}}),
            &["entries"],
            true,
        ),
        operation(
            "content.collection_read",
            "Page named native iroh collection members.",
            id_page,
            &["content_id"],
            false,
        ),
        operation(
            "package.put",
            "Store a core File, Collection, Changes, or Symlink document. Dependency availability is checked separately by package.resolve.",
            json!({"document":document_schema()}),
            &["document"],
            true,
        ),
        operation(
            "package.get",
            "Read a package document up to 1MiB; larger documents use content.export/read.",
            json!({"root":id}),
            &["root"],
            false,
        ),
        operation(
            "package.resolve",
            "Page a resolved semantic package tree. Hidden base dependencies are available through package.dependencies.",
            page_fields.clone(),
            &["root"],
            false,
        ),
        operation(
            "package.dependencies",
            "Page the complete representation closure, including hidden bases. Pass the entire closure in workflow/invocation submission contents.",
            page_fields,
            &["root"],
            false,
        ),
        operation(
            "package.retain",
            "Retain every dependency in a package's complete representation closure.",
            json!({"root":id}),
            &["root"],
            true,
        ),
        operation(
            "package.envelope",
            "Encode a locally available package as workflow payload text. Obtain the full contents list separately with package.dependencies before submission.",
            json!({"root":id}),
            &["root"],
            false,
        ),
        operation(
            "package.parse_envelope",
            "Recognize core's explicit package envelope; ordinary payloads return a null root.",
            json!({"payload":payload_schema()}),
            &["payload"],
            false,
        ),
    ]
}

fn document_schema() -> Value {
    let id = content_id_schema();
    json!({"oneOf":[
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"file"},"content":id,"executable":{"type":"boolean"}},"required":["kind","content","executable"]},
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"collection"},"entries":{"type":"object","additionalProperties":id}},"required":["kind","entries"]},
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"changes"},"base":id,"changes":{"type":"object","additionalProperties":{"anyOf":[id,{"type":"null"}]}}},"required":["kind","base","changes"]},
        {"type":"object","additionalProperties":false,"properties":{"kind":{"const":"symlink"},"target":{"type":"string"}},"required":["kind","target"]}
    ]})
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
    async fn native_collection_and_semantic_package_preserve_distinct_formats() {
        let dir = tempfile::tempdir().unwrap();
        let mut run = test_run(dir.path());
        let run_id = run.manifest.run_id.clone();
        let imported = dispatch(
            &mut run,
            "content.import_bytes",
            &json!({"run_id":run_id,"payload":"hello"}),
        )
        .await
        .unwrap()["content_id"]
            .clone();
        let collection = dispatch(
            &mut run,
            "content.collection_put",
            &json!({"run_id":run_id,"entries":[{"name":"hello","content_id":imported}]}),
        )
        .await
        .unwrap()["content_id"]
            .clone();
        assert_eq!(collection["format"], "HashSeq");
        let members = dispatch(
            &mut run,
            "content.collection_read",
            &json!({"run_id":run_id,"content_id":collection}),
        )
        .await
        .unwrap();
        assert_eq!(members["items"][0]["content_id"], imported);
        let file = dispatch(&mut run,"package.put",&json!({"run_id":run_id,"document":{"kind":"file","content":imported,"executable":false}})).await.unwrap()["root"].clone();
        assert_eq!(file["format"], "Raw");
        let envelope = dispatch(
            &mut run,
            "package.envelope",
            &json!({"run_id":run_id,"root":file}),
        )
        .await
        .unwrap();
        assert_eq!(
            dispatch(
                &mut run,
                "package.parse_envelope",
                &json!({"run_id":run_id,"payload":envelope["payload"]})
            )
            .await
            .unwrap()["root"],
            file
        );
        let dependencies = dispatch(
            &mut run,
            "package.dependencies",
            &json!({"run_id":run_id,"root":file}),
        )
        .await
        .unwrap();
        assert_eq!(dependencies["total"], 2);
        assert!(
            dependencies["items"]
                .as_array()
                .unwrap()
                .contains(&imported)
        );
    }

    #[tokio::test]
    async fn stream_import_export_and_bounded_reads_use_owned_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut run = test_run(dir.path());
        let run_id = run.manifest.run_id.clone();
        std::fs::write(dir.path().join("input"), vec![7u8; 150_000]).unwrap();
        let id = dispatch(
            &mut run,
            "content.import_stream",
            &json!({"run_id":run_id,"path":"input"}),
        )
        .await
        .unwrap()["content_id"]
            .clone();
        std::fs::remove_file(dir.path().join("input")).unwrap();
        let read = dispatch(
            &mut run,
            "content.read",
            &json!({"run_id":run_id,"content_id":id,"start":"149998","length":65536}),
        )
        .await
        .unwrap();
        assert_eq!(read["end"], "150000");
        assert_eq!(read["data"]["length"], 2);
        dispatch(
            &mut run,
            "content.export",
            &json!({"run_id":run_id,"content_id":id,"path":"output"}),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("output")).unwrap(),
            vec![7u8; 150_000]
        );
        assert!(
            dispatch(
                &mut run,
                "content.export",
                &json!({"run_id":run_id,"content_id":id,"path":"output"})
            )
            .await
            .is_err()
        );
        assert!(
            dispatch(
                &mut run,
                "content.read",
                &json!({"run_id":run_id,"content_id":id,"length":65537})
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn releasing_incidental_tags_preserves_committed_publication_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        let mut run = test_run(dir.path());
        let run_id = run.manifest.run_id.clone();
        let bytes = dispatch(
            &mut run,
            "content.import_bytes",
            &json!({"run_id":run_id,"payload":"retained by activation"}),
        )
        .await
        .unwrap()["content_id"]
            .clone();
        let root = dispatch(
            &mut run,
            "package.put",
            &json!({"run_id":run_id,"document":{"kind":"file","content":bytes,"executable":false}}),
        )
        .await
        .unwrap()["root"]
            .clone();
        let envelope = dispatch(
            &mut run,
            "package.envelope",
            &json!({"run_id":run_id,"root":root}),
        )
        .await
        .unwrap();
        crate::tools::workflow::dispatch(&mut run,"workflow.submit",&json!({"run_id":run_id,"trigger":{"kind":"root","node_id":"A","authority":[]},"result":envelope["payload"],"contents":[bytes,root]})).await.unwrap();
        for content_id in [&bytes, &root] {
            assert_eq!(
                dispatch(
                    &mut run,
                    "content.release",
                    &json!({"run_id":run_id,"content_id":content_id})
                )
                .await
                .unwrap()["released"],
                true
            );
        }
        dispatch(&mut run, "content.gc", &json!({"run_id":run_id}))
            .await
            .unwrap();
        assert_eq!(
            dispatch(
                &mut run,
                "package.dependencies",
                &json!({"run_id":run_id,"root":root})
            )
            .await
            .unwrap()["total"],
            2
        );
        assert_eq!(
            dispatch(
                &mut run,
                "content.read",
                &json!({"run_id":run_id,"content_id":bytes})
            )
            .await
            .unwrap()["data"]["text"],
            "retained by activation"
        );
    }
}
