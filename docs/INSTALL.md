# Install and run

This application targets macOS and Unix. Keep this checkout beside `../ontography-core`; the Rust dependency uses that path.

Required tools:

- Rust 1.91 or newer, Cargo, and the platform's native compiler tools.
- Node.js 22.19 or newer.
- Pi **0.85.1**, the version against which the extension is checked.

```sh
rustc --version
node --version
pi --version
```

If needed, install Pi using your normal Node package setup:

```sh
npm install --global @earendil-works/pi-coding-agent@0.85.1
```

Ontography does not install or upgrade Pi during launch. Select a compatible executable with `--pi /absolute/path/to/pi`; that choice is used when starting a manager process. An already running manager is reused.

```sh
cargo build --locked
./target/debug/ontography --help

cargo install --path . --locked
```

The binary embeds the Pi extension and management instructions, materializing versioned assets in the data directory. Running an installed binary does not require the source checkout or adjacent `node_modules`. Building still requires the sibling core source.

## Start a session

```sh
ontography
ontography --project /absolute/path/to/project new work
ontography ls
ontography attach work
ontography --data-dir /absolute/path/to/test-data new experiment
```

Bare `ontography` always creates a new session using the current directory, or `--project`. `new NAME` creates a named session; `attach NAME_OR_ID` explicitly returns to an existing one. **`--project` applies when creating a session; it does not change an existing session's project.** Pi `/quit` exits only Pi; `close NAME_OR_ID` closes the app session and graph.

Native Pi retains its authentication, model controls, shell/file tools, and global/project settings. Pi histories are stored within the owning Ontography session. Use Pi's normal `/login` and model controls as needed.

An initial management request can be:

> Read `examples/flow.json`, validate its graph, and start its run for this session. Show the graph and pending work.

`ontography_tools` enables capability groups. Session-bound tools resolve the graph automatically. The example uses UTF-8 payload contracts and logical nodes; it does not launch Codex workers.

## Graph display and detachment

- `/graph` inside Pi opens the session's graph; `q` or Escape returns to Pi.
- `ontography --ui` attaches to the same native manager and opens its graph first.
- **Ctrl-B, then D** detaches from the Pi terminal and leaves the manager and graph running.
- `ontography session detach SESSION_UUID` detaches the controlling client from another terminal, including while its graph view is open.

One controlling attachment is allowed per manager terminal. A second is rejected; detach the current controller first. Different sessions can have separate attached clients.

Pi `/new`, `/resume`, `/fork`, `/clone`, and `/tree` operate on conversations within the same app session and preserve its graph. Exiting Pi ends that manager process; later attachment resumes its active saved conversation while preserving the app session and graph.

Full command and lifecycle reference: [SESSIONS.md](SESSIONS.md).

## Storage and migration

Storage precedence is `--data-dir`, `ONTOGRAPHY_DATA_DIR`, `$XDG_DATA_HOME/ontography`, then `$HOME/.ontography`. Each canonical data directory has one server. Logs are under `logs/`; session records and Pi histories are under `sessions/`.

If the previous default `~/.local/share/ontography/` contains runs, default launch requires migration. Stop its server using the **matching old application build**, then run the new migration command:

```sh
/absolute/path/to/matching-old-ontography --data-dir "$HOME/.local/share/ontography" server stop
ontography migrate
```

The migration archives an existing legacy `~/.ontography/` as `~/.ontography.archive-UUID`, moves the stopped current store into `~/.ontography/`, and makes the old path a symlink to that same store. Locks reject active owners. A persisted journal allows the same command to finish an interrupted cutover. Existing app stores are not merged.

```sh
ontography migrate --from /absolute/path/to/old-store --to /absolute/path/to/new-store
```

`migrate` defaults are based on `HOME`; supply `--from`/`--to` for custom or XDG locations. Migrating storage preserves run IDs and core data. Existing runs have no inferred Pi owner: create an app session and adopt a selected run explicitly. Storage migration does not replace a run's core build or declaration.

**Local cutover completed on 2026-09-25.** The legacy home was archived, the current store moved, and all three previously active runs resumed with unchanged identities and graph state. See [VERIFICATION.md](VERIFICATION.md#completed-local-storage-migration) for the archive path and evidence. Other installations use the explicit migration procedure above.

## Server and structured calls

```sh
ontography server start
ontography server status
ontography server stop

ontography call system.hello --args '{}'
ontography call run.list --args '{}'
ontography --session SESSION_UUID call run.inspect --args '{}'
ontography call run.inspect --args '{"run_id":"RUN_UUID"}'
ontography call OPERATION --file /absolute/path/to/arguments.json
```

Server stop settles accepted operations, stops managers, and suspends core runs. Startup loads records without launching every saved manager. Attach to the session you want; `session resume SESSION_UUID` resumes its graph without opening a terminal. A foreground server can be used by an external supervisor:

```sh
ontography --data-dir /absolute/path/to/test-data server run
```

`system.hello` returns the operation catalog and schemas. Scope a call with `--session` to resolve and validate its graph target. Preserve string identifiers and decimal-string revisions exactly.

If a mutation loses its response, use its client/request IDs to inspect the outcome. Accepted work can have committed despite the missing response. Receipts remain within the original server instance; after restart, reconcile against current state before further work.

## Development verification and troubleshooting

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
cd pi
npm ci --ignore-scripts
npm run check
npm test
```

Pi development dependencies are local and pinned. Tests use temporary stores, Unix sockets, PTYs, and local iroh endpoints; environments that prohibit these resources need the relevant execution permission. Most tests make no model calls.

Builds embed app/core source fingerprints. Clients require the server's exact app/core builds; rebuilding the executable does not replace a running server. After a rebuild, use `ontography server stop` with the same data directory, then retry your command to start the current build. Explicit shutdown accepts a different app/core build when its wire protocol matches; ordinary commands still require an exact build match. Shutdown suspends graph resources and stops managers while preserving saved state. Persistent runs still require their stored core build and declaration identities; stopping the server does not migrate saved core data.

For startup failures, inspect the Pi version, project/data paths, `server status`, and the server log. Missing previously saved conversation history is reported explicitly. Empty initial conversations have reserved IDs and can be resumed before Pi writes their first history file.

Session recovery errors name the record that could not be loaded. A session
directory without `session.json` is not a recoverable session, even if runtime
cache files remain inside it. These directories are retained for inspection;
healthy sessions remain available.
