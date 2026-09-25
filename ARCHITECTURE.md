# App implementation todo

The app builds on `../ontography-core`. These are the eight agreed pieces to implement or integrate. Their numbers identify the pieces; they do not prescribe implementation order.

- [ ] **1. Codex node definition**

   An Ontography node whose app implementation owns a tmux session containing a Codex session. Core supplies the node's identity and workflow rules. The node's identity persists across tmux or Codex restarts.

- [ ] **2. Workspace package**

   Uses core's existing package format: collections, files, symlinks, and changes against a base. Core resolves a complete directory view, prepares a private copy-on-write checkout, and captures edits as a changes package. The app defines the semantic layout of its contents.

- [ ] **3. Message package definition**

   Defines message content and its delivery behavior. Once Ontography accepts the delivery, the receiving harness inserts the text directly into Codex's conversation input, as though pasted to the agent. Receiving the message requires no separate package lookup. The contract validates the payload; the harness performs and records delivery.

- [ ] **4. Union package definition**

   An app-level package type permitting `Message | WorkspaceEnvelope`. It uses one core `Contract` with one associated object type; the validator accepts either payload form. Each delivery carries one package. A handoff containing both a message and a workspace can use one composed content package.

- [ ] **5. Edge definitions**

   Specify permitted endpoint types, authority requirements, and the accepted package contract. Concrete edges connect a source node to a target node. Connections are directed; replies require a corresponding reverse connection.

- [ ] **6. Node harness**

   Prepares context and workspaces, delivers messages, captures outputs, and submits operations through core. Agent requests are bound to the node and its active invocation. Core validates and commits workflow operations.

- [ ] **7. Node-scoped MCP interface**

   Implemented by the node's harness. Exposes the node's identity, outgoing connections, accepted package definitions, received inputs, and permitted context. Provides operations to read packages, construct messages or artifact packages, and request publication through an edge.

- [x] **8. Graph TUI and native management harness**

   This part consists of three pieces:

   | Piece | Responsibility |
   | --- | --- |
   | CLI + Pi harness | The `ontography` command connects to or starts the local server, then launches the management conversation with Ontography instructions and tools. |
   | Ontography tools | Expose core's supported graph and runtime capabilities. A detached Rust server retains core objects across client connections. The connection, handle ownership, tool arguments, results, and errors are implementation responsibilities within this integration. |
   | Graph TUI | Rust UI using Ratatui; visualize core state and issue actions through the existing server bindings. Pi remains the management harness. |

   Implemented scope: CLI/Pi integration, tools exposing the existing core, and Rust graph visualization. Our concrete Codex node, message/union package, and corresponding edge definitions are still future work in items 1–7. Their registration and management hooks follow those implementations.

   The graph UI is implemented in Rust. Default management uses Pi's existing interactive client. `ontography --ui` drives Pi through its RPC mode and renders the graph and conversation in Ratatui. The Pi extension supplies tool bindings; graph rendering belongs to the Rust client. A native Ratatui widget supports core cycles, self loops, and parallel edges; the [renderer decision](src/ui/README.md#renderer-decision) records the dependency evaluation.

   Core already establishes the graph/runtime API. The management tools bind that API to Pi, supplying serialization and retained-handle access. Graph authoring is one capability group within that interface. The core capability groups are:

   | Core capability | Operations to expose |
   | --- | --- |
   | Graph definition and admission | Build and validate nodes, edges, contracts, authority rules, and rewrite grammars. |
   | Run lifecycle | Create/open persistent sessions; start, suspend, resume, and terminally close runs; control available hosted executables. |
   | Graph rewriting | Prepare a rewrite, inspect its resulting graph and package retirements, and commit a current plan. |
   | Workflow operations | Submit activations and emissions; transfer outbound packages through permitted edges. |
   | Content, packages, and workspaces | Import/read/retain content, compose and resolve packages, check out and capture workspaces, diff and merge. |
   | Invocation and context | Issue scoped invocations, prepare granted context, and record exposure and delivery evidence. |
   | Inspection and observation | Inspect topology, pending work, package history, invocations, executions, and revision notifications. |

   Current integration work supplies tool schemas and descriptions, request/result conversion, and retention of core's live run/session objects. Core already supplies definition/configuration and registration primitives. Concrete app definitions are planned integration points; placeholder implementations are unnecessary. Once the node implementations exist, management tools can expose their tmux/Codex lifecycle, direct message delivery, workspace binding, and process reconciliation after rewrites. Management has graph/run scope; node MCP interfaces have node/invocation scope.

   Creating a graph produces a reusable definition. Starting it creates a run, which may be idle until work is submitted. A rewrite changes that run's current graph while preserving history, under its configured grammar. Schema, contracts, and rewrite grammar are fixed when the runtime is constructed. Core commits graph and live package state together; the app reconciles processes separately. A live rewrite does not automatically overwrite the saved definition.

Current design focus: the Pi management agent and its Ontography tools. Core already implements live-run ownership and lifecycle, through `RunningApplication` or its lower-level runtime, session, and execution objects. The detached Rust server keeps those objects accessible across tool calls and management clients. Core owns workflow/content persistence; Pi owns the management conversation.

Confirmed lifecycle: graph/node execution continues when Pi exits; a later `ontography` invocation reconnects. Pi's active management turn has client lifetime. Explicit run suspension preserves resumability; run closure is terminal. Explicit server stop gracefully suspends its resources and preserves durable runs. Server-crash recovery reconstructs committed state and requires explicit resumption initially.

The detailed sequence, API coverage, integration boundaries, and verification gates are in [PLAN.md](PLAN.md). Item 8 passed its implementation gates; [verification results](docs/VERIFICATION.md) record automated coverage, live Pi/terminal checks, and the limits of these results.
