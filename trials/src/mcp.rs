//! A minimal MCP client for the node tools: it runs `ontography node-mcp`,
//! which reaches the node's tools through the connection variables the app
//! gives a node's program, and calls tools one at a time over its stdio.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Longest wait for one reply: `wait_for_change` waits up to 30 s itself.
const REPLY: Duration = Duration::from_secs(90);

pub struct Mcp {
    _child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    next: u64,
}

impl Mcp {
    pub async fn start(ontography: &Path) -> Result<Self> {
        let mut child = Command::new(ontography)
            .arg("node-mcp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start {} node-mcp", ontography.display()))?;
        let input = child.stdin.take().context("stdin")?;
        let output = BufReader::new(child.stdout.take().context("stdout")?).lines();
        let mut mcp = Self {
            _child: child,
            input,
            output,
            next: 0,
        };
        mcp.request(
            "initialize",
            json!({"protocolVersion": "2025-11-25", "capabilities": {},
                   "clientInfo": {"name": "ontography-trials", "version": env!("CARGO_PKG_VERSION")}}),
        )
        .await?;
        mcp.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await?;
        Ok(mcp)
    }

    async fn send(&mut self, message: Value) -> Result<()> {
        let mut line = serde_json::to_vec(&message)?;
        line.push(b'\n');
        self.input.write_all(&line).await?;
        self.input.flush().await?;
        Ok(())
    }

    /// Sends a request and returns its result, skipping notifications.
    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.next += 1;
        let id = self.next;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        loop {
            let line = tokio::time::timeout(REPLY, self.output.next_line())
                .await
                .context("node-mcp did not reply")??
                .context("node-mcp closed its output")?;
            let message: Value = serde_json::from_str(&line)?;
            if message["id"] != id {
                continue;
            }
            if let Some(error) = message.get("error") {
                bail!("{method}: {error}");
            }
            return Ok(message["result"].clone());
        }
    }

    /// Calls a node tool: `Ok` with its reply, or `Err` with the tool's error.
    pub async fn call(&mut self, tool: &str, arguments: Value) -> Result<Result<Value, Value>> {
        let result = self
            .request("tools/call", json!({"name": tool, "arguments": arguments}))
            .await?;
        let text = result["content"][0]["text"]
            .as_str()
            .context("tool result text")?;
        let reply: Value =
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.into()));
        Ok(if result["isError"] == true {
            Err(reply)
        } else {
            Ok(reply)
        })
    }
}
