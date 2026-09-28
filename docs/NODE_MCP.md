# Node MCP adapter

Managed Codex nodes receive an `ontography_node` MCP server automatically. The
adapter exposes the existing [shared node tools](NODE_TOOLS.md); their graph,
package, attempt, grant, and receipt behavior stays in `NodeToolContext`.

```text
Persistent Codex app-server (native TUI attached in the node PTY)
  → MCP stdio: ontography node-mcp
  → private execution socket
  → NodeToolContext → core
```

The MCP server command and connection environment are supplied through a
process-local Codex configuration override. The user's Codex configuration is
not rewritten. Custom `argv` commands inherit `ONTOGRAPHY_NODE_MCP_SOCKET` and
`ONTOGRAPHY_NODE_MCP_TOKEN`; their harness can run `ontography node-mcp` with
that environment. Custom commands do not receive Codex-specific arguments.

## Choosing tools

Set `tools` on the node definition, alongside `config` and `grants`:

```json
{
  "id": "observer",
  "kind": "agent",
  "config": {"prompt": "Inspect the graph and report what is waiting."},
  "tools": ["inspect_node", "inspect_graph", "list_inputs"]
}
```

- Omitted `tools` preserves the default catalog allowed by the node's grants.
- `"tools": []` exposes no shared node tools.
- An explicit list exposes only those names that the grants also permit.
- Unknown names are rejected when the workflow document is validated.

Selection is checked by both `catalog()` and `call()`, so calling a hidden tool
by name does not bypass the selection. It does not grant extra powers and does
not change Codex's built-in shell/file tools or other user-configured MCP servers. Tool selection belongs to the
node definition and applies independently of the transport. Selection and grant
edits update the live context without restarting the agent; connected MCP
clients receive `notifications/tools/list_changed`.

## Ownership and delivery

Each execution owns a fresh private Unix socket and connection token. The
adapter also checks the socket peer's user. Clients cannot choose a different
node by supplying a run or node ID. Stop and replacement close the endpoint;
old connection details cannot attach to a replacement execution. This is a
local user/process boundary, not an operating-system sandbox for an agent.

Tool results contain one MCP text item whose text is exactly `Reply::bytes()`.
The proxy acknowledges delivery after flushing that MCP response to stdout;
only then does the server call `Reply::sent()`. A socket write alone does not
mark a receipt sent. If acknowledgement is lost, the receipt remains prepared;
the transport does not infer whether Codex received it, and never replays a
call automatically. Tool execution failures use MCP `isError`; malformed or
unavailable calls use JSON-RPC errors.

Requests can run concurrently, so a waiting tool does not block ping or other
calls. Input/output frames, connections, outstanding calls, and delivery waits
are bounded. Partial frames survive concurrent replies and notifications.
Cancellation stops reads and waits. Accepted mutations settle under the node's
execution lifecycle, including after a client disconnect; cancellation is not
rollback. Node suspension still stops the execution and closes its attempts.

The adapter implements MCP stdio initialization, ping, tool listing/calls,
cancellation notifications, and catalog-change notifications. It negotiates
2025-11-25 and compatible earlier protocol versions. It advertises only tools;
there are no management, resources, or prompts endpoints. See the official
[stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#stdio)
and [tool protocol](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).

## Conversation delivery

The node runtime supplies initial and incoming work as conversation messages
through the persistent Codex app-server. The native CLI joins that same server
and thread. MCP supplies outgoing graph actions, package access, and completion
of the attempt named in each message. See [Workflow execution](WORKFLOWS.md).

Codex's own MCP approval policy remains in force. For example, an operator may
configure `mcp_servers.ontography_node.tools.submit_invocation.approval_mode`
in their Codex settings. Tool selection and graph grants remain separate checks.

The native verification uses a localhost Responses fixture to run real Codex
turns, queue a second message during an active turn, and publish both replies
through MCP. No external model is used. Node pane selection in the graph UI and
a full project acceptance run with live models remain separate work.
