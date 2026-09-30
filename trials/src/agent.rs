//! Agent mode: this binary as an agent node's program, placed by the
//! document's `argv`. Nothing delivers work to such a program; it pulls work
//! through the node tools, over `ontography node-mcp`, and records each
//! attempt in its node's witness file so the judge can match every
//! activation to what the program decided.

use crate::mcp::Mcp;
use crate::roles::{self, Style};
use anyhow::Result;
use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(long, value_enum)]
    style: Style,
    /// The node's name in the document.
    #[arg(long)]
    name: String,
    /// The directory of witness files, one per node.
    #[arg(long)]
    witness: PathBuf,
    /// The ontography binary whose `node-mcp` serves the node tools.
    #[arg(long)]
    ontography: PathBuf,
    /// The node's outgoing connections, for a routing agent.
    #[arg(long, value_delimiter = ',')]
    to: Vec<String>,
}

fn record(path: &Path, line: Value) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(format!("{line}\n").as_bytes())?;
    Ok(())
}

/// Bytes as a read reply carries them.
fn bytes(reply: &Value) -> Option<Vec<u8>> {
    for value in [reply, &reply["content"], &reply["data"]] {
        if let Some(text) = value["text"].as_str() {
            return Some(text.as_bytes().to_vec());
        }
        if let Some(items) = value["bytes"].as_array() {
            return items
                .iter()
                .map(|n| n.as_u64().and_then(|n| u8::try_from(n).ok()))
                .collect();
        }
    }
    None
}

/// The whole of one input, read in ranges.
async fn read(mcp: &mut Mcp, attempt: &str, handle: &Value) -> Result<Result<Vec<u8>, Value>> {
    let mut all = Vec::new();
    loop {
        let reply = mcp
            .call(
                "read_package",
                json!({"attempt_id": attempt, "handle": handle, "start": all.len()}),
            )
            .await?;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => return Ok(Err(error)),
        };
        let Some(chunk) = bytes(&reply) else {
            return Ok(Err(json!({"unreadable": reply})));
        };
        if chunk.is_empty() {
            return Ok(Ok(all));
        }
        all.extend(chunk);
    }
}

pub async fn run(args: Args) -> Result<()> {
    let path = args.witness.join(format!("{}.jsonl", args.name));
    let mut mcp = Mcp::start(&args.ontography).await?;
    let pid = std::process::id();
    let mut version: Option<Value> = None;
    loop {
        let next = match mcp.call("next_trigger", json!({})).await? {
            Ok(next) => next,
            Err(error) => {
                record(
                    &path,
                    json!({"pid": pid, "event": "next_trigger", "error": error}),
                )?;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        if !next["version"].is_null() {
            version = Some(next["version"].clone());
        }
        let Some(task) = next["task_id"].as_str().map(String::from) else {
            let mut wait = json!({"timeout_ms": 30000});
            if let Some(version) = &version {
                wait["after"] = version.clone();
            }
            let _ = mcp.call("wait_for_change", wait).await?;
            continue;
        };
        let begun = match mcp
            .call("begin_invocation", json!({"task_id": task}))
            .await?
        {
            Ok(begun) => begun,
            Err(error) => {
                record(
                    &path,
                    json!({"pid": pid, "event": "begin", "task": task, "error": error}),
                )?;
                continue;
            }
        };
        let attempt = begun["attempt_id"].as_str().unwrap_or_default().to_owned();
        let mut payloads = Vec::new();
        let mut unread = None;
        for input in begun["inputs"].as_array().into_iter().flatten() {
            match read(&mut mcp, &attempt, &input["handle"]).await? {
                Ok(payload) => payloads.push(payload),
                Err(error) => unread = Some(error),
            }
        }
        if let Some(error) = unread {
            record(
                &path,
                json!({"pid": pid, "event": "read", "attempt": attempt, "error": error, "inputs": begun["inputs"]}),
            )?;
            let _ = mcp
                .call(
                    "fail_invocation",
                    json!({"attempt_id": attempt, "reason": "unreadable input", "retryable": true}),
                )
                .await?;
            continue;
        }
        let input = roles::agent_input(payloads);
        let sha = roles::sha(&input);
        let seen = std::fs::read_to_string(&path)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains(&sha) && line.contains("\"begun\""))
            .count();
        record(
            &path,
            json!({"pid": pid, "event": "begun", "sha": sha, "attempt": attempt}),
        )?;
        if args.style == Style::Flaky && seen == 0 {
            let reply = mcp
                .call(
                    "fail_invocation",
                    json!({"attempt_id": attempt, "reason": "flaky", "retryable": true}),
                )
                .await?;
            record(
                &path,
                json!({"pid": pid, "event": "failed", "sha": sha, "reply": reply.is_ok()}),
            )?;
            continue;
        }
        let result = roles::agent_result(&args.name, &input);
        let mut submit = json!({"attempt_id": attempt, "result": {"message": result}});
        let routes = (args.style == Style::Route).then(|| roles::routes(&args.to, &input, &result));
        if let Some(routes) = &routes {
            submit["outputs"] = routes
                .iter()
                .map(|(edge, message)| json!({"message": message, "to": edge}))
                .collect();
        }
        let reply = mcp.call("submit_invocation", submit).await?;
        record(
            &path,
            json!({"pid": pid, "event": "submitted", "sha": sha, "result": result, "routes": routes,
                   "ok": reply.is_ok(), "reply": match &reply { Ok(v) | Err(v) => v }}),
        )?;
    }
}
