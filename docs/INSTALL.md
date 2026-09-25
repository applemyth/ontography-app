# Install and run

This application targets macOS and Unix. Keep this checkout beside `../ontography-core`; the Rust dependency uses that path.

Required tools:

- Rust 1.91 or newer, Cargo, and the platform's native compiler tools.
- Node.js 22.19 or newer.
- Pi **0.85.1**, the version against which this extension is checked.

Check existing installations:

```sh
rustc --version
node --version
pi --version
```

If Pi is missing, install the supported version using your normal Node package setup:

```sh
npm install --global @earendil-works/pi-coding-agent@0.85.1
```

The application never installs or upgrades Pi during ordinary launch. An existing incompatible Pi installation produces a version error; select a compatible executable with `--pi /absolute/path/to/pi`.

Build or install the Rust command from this repository:

```sh
cargo build --locked
./target/debug/ontography --help

cargo install --path . --locked
```

The binary includes the Pi extension and management instructions. At launch it materializes versioned assets under the selected data directory; a deployed binary does not require this source checkout or an adjacent `node_modules` directory to load its extension. Building from this checkout still requires the sibling core source.

## Open the management interface

```sh
ontography
ontography --project /absolute/path/to/project
ontography --data-dir /absolute/path/to/ontography-data --project /absolute/path/to/project
```

`ontography` connects to the server for the selected data directory, starting it when needed, then launches native Pi. Pi retains its normal model, authentication, shell/file tools, and project settings. Use Pi's normal `/login` and model controls when configuring the management conversation.

The management instructions include the selected project directory. Ontography tools expose their actual argument schemas. Start with a request such as:

> Read `examples/flow.json`, validate its graph, and start a run in this project. Show its current graph and pending work.

`ontography_tools` lists available capability groups and activates their tools. A logical graph can be admitted and started before executable node implementations are available; inspect the returned execution status. The example uses UTF-8 payload contracts. It does not create Codex sessions or implement direct agent message delivery.

For the graph visualization interface:

```sh
ontography --ui
```

## Server lifetime

```sh
ontography server start
ontography server status
ontography server stop
```

Closing Pi or its terminal disconnects that client. The detached Rust server retains graph runs and accepted operations. Reopening `ontography` connects to those same live runs. Multiple clients using the same data directory share one server.

`server stop` performs an orderly suspension and preserves durable runs. Starting the server again lists those runs; resume the runs you want to use through the management tools. Explicit run closure is terminal for graph admission. Server restart also expires transient handles such as prepared rewrite plans and invocation capabilities; refresh state before using resources.

The management Pi process itself exits with its client. Use native Pi session controls to reopen prior conversation history when desired. Restoring a conversation reads current graph state and never replays previous mutations.

The data directory is selected by `--data-dir`, then `ONTOGRAPHY_DATA_DIR`, then `$XDG_DATA_HOME/ontography`, then `$HOME/.local/share/ontography`. Each directory has its own server and durable run stores. Server logs are under its `logs/` directory. A foreground server is available for development or an external supervisor:

```sh
ontography --data-dir /absolute/path/to/ontography-data server run
```

## Structured calls

Management operations are also available without a model:

```sh
ontography call system.hello --args '{}'
ontography call run.list --args '{}'
ontography call run.inspect --args '{"run_id":"RUN_UUID"}'
ontography call OPERATION --file /absolute/path/to/arguments.json
```

The arguments file contains the complete operation argument object. `system.hello` returns the operation catalog and schemas. Preserve string identifiers and decimal-string revisions exactly.

If a request loses its response, inspect the recorded outcome using the client/request IDs from its receipt. Do not automatically repeat the mutation: it may already have committed. Outcomes can be recovered while the original server instance retains the record. After restart, reconcile against current run state.

## Verify a development checkout

```sh
cargo test --locked
cd pi
npm ci --ignore-scripts
npm run check
npm test
```

Pi development dependencies are local and pinned in `pi/package-lock.json`. The extension uses Pi's own module resolver at runtime. Tests use local Unix sockets; environments that prohibit socket listeners must allow those tests to run outside that restriction. Most tests make no model calls.

Builds embed fingerprints of the app and core source. Clients require the server's exact app/core build identities, and persisted runs require their stored core build identity. Missing identity fields from an older development format are incompatible; this version provides no migration across core builds.

If startup fails, check the reported Pi version, project/data-directory paths, `ontography server status`, and the server log. The server preserves an incompatible existing owner's runs instead of replacing that process. Use the matching application build or explicitly stop the existing server before changing builds. Stopping an old server does not migrate its durable runs to another core build.
