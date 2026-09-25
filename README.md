# Ontography

A Rust management application for [`ontography-core`](../ontography-core), with Pi as its management agent and an optional Ratatui graph/conversation interface.

```sh
cargo build --locked
./target/debug/ontography              # Pi management conversation
./target/debug/ontography --ui         # Rust graph + Pi conversation
```

Keep the sibling core checkout available when building. Install Pi 0.85.1 separately; the binary embeds its Ontography extension. See [installation and usage](docs/INSTALL.md).

## Ownership

| Component | Responsibility |
|---|---|
| Rust CLI | Find or start the detached local server; launch the selected management client. |
| Pi | Conversation, model execution, and typed Ontography tools. |
| Rust server | Retain runs, accepted operations, execution hosts, invocations, workspaces, and transfers across client connections. |
| Core | Graph admission, authority and contracts, workflow commits, rewrites, content, packages, and durable history. |
| Rust TUI | Draw authoritative topology/frontier and conversation; issue actions through the same bindings. |

Closing either client leaves server-owned graph work running. Explicit suspension preserves resumability; closure terminally ends admission. Server restart requires explicit run resumption and expires transient handles. Saved definitions remain separate from the current graph of each run.

## Use without a model

```sh
./target/debug/ontography server start
./target/debug/ontography call system.hello --args '{}'
./target/debug/ontography call run.list --args '{}'
./target/debug/ontography server stop
```

The handshake describes the actual operations and argument schemas. [examples/flow.json](examples/flow.json) is a logical graph with a configured rewrite. Logical graphs can run idle; executable implementations must be registered by trusted Rust integration code. Native/provider applications can use the same persistent management lifecycle through `project.start`.

Tool families cover graph definitions, runs, execution, rewriting, workflow, content, packages, workspaces, iroh transfers, scoped invocations/context, inspection, and registered projects. [The binding inventory](docs/CORE_BINDINGS.md) records mappings and precise limitations.

## Development

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cd pi
npm ci --ignore-scripts
npm run check
npm test
```

Tests use temporary local sockets and iroh endpoints. Most integration tests use deterministic registered executables without a model.

The [implementation plan](PLAN.md) covers the management layer, item 8 of the [architecture](ARCHITECTURE.md). [Verification results](docs/VERIFICATION.md) record the completed checks. Concrete Codex/tmux nodes, semantic message delivery, and node-scoped MCP are subsequent architecture items. Core workspace/package functionality is available through this management layer now.
