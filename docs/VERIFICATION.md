# Verification

## Typed workflow and session recovery fixes — 2026-09-30

Passed: **319 app Rust tests**, **33 Pi tests**, TypeScript checking,
all-target Clippy with warnings denied, formatting, and `git diff --check`.
Six opt-in native Rust tests remain ignored in the ordinary suite. Each test
added for a behavior fix fails on the tree before it. The sibling
`ontography-stress` quick preset passed **15 games, 0 with problems**: typed
games in tandem and concurrent modes and software games, on fixed seeds 1–5.
At a load average near 56, `concurrent_startups_detach_and_reconnect_to_one_live_server`
twice missed its 5-second endpoint-release deadline. Server shutdown and run
suspension are unchanged here, and it passed 10 of 10 runs alternating with
the tree before these fixes, which also passed 10 of 10.

- [Typed execution tests](../tests/typed_execution.rs) run the smelting document,
  script, and submit request extracted directly from `WORKFLOWS.md`. Command
  settings and human decisions request declared authority transitions; core
  rejects unauthorized changes and connections that refuse the output. A
  command authority no transition or connection allows is refused before any
  task runs, and a refused decision names its rule and leaves the task ready.
  Binary command input, output, and export preserve every byte. Stdout
  envelopes are refused, the malformed ones recorded by core, without consuming
  the input. Workspace captures still publish and export. Output refused for
  its contract parks after one attempt. Connection names that shadow nodes are
  refused only where a document adds them.
- [Workflow move tests](../tests/workflow_moves.rs) exercise submit, transfer,
  retire, and filtered inspections after nodes and connections are added or
  replaced. They cover pending ownership changes, missing bindings, and
  collisions between document names and live core identities. One move
  consumes a joined trigger observed by name after replacement. Collisions are
  refused before and after an edit applies, and inspections resolve exact
  identities without the workflow file.
- [Session recovery tests](../tests/session_recovery.rs) preserve legacy
  initialization records, conversation files, selection, and exclusive run
  claims. Sessions can activate their manager with an unavailable graph;
  recovery does not replay or replace incompatible old runs. Current-format
  validation and failed session-save behavior remain covered. A graph that
  cannot start or resume stays reported, in the session and by graph
  operations, until it works; a failed activation starts no work; closing
  completes around a run that cannot open. Legacy reservations hold against
  starts outside sessions but yield to a current session owning the run, and
  malformed saved starts report their own error. Pi shows the reason too.
- Startup binds the document once. Validators avoid copying payloads that
  cannot be envelopes. Obsolete workflow-state fallbacks and the plan `steps`
  alias are removed; the document `kind` alias remains usable.

These changes use existing core admission and identity rules. Empty authority
sets and atomic broadcast submissions retain their core behavior.

## One run type: typed documents and external nodes — 2026-09-29

Passed: **292 app Rust tests**, **32 Pi tests**, all-target Clippy with
warnings denied, formatting, and `git diff --check`. The sibling
`ontography-stress` crate's quick preset passed: **10 games, 0 with problems**,
over fixed seeds 1–5 in tandem and concurrent modes.

- Every run starts from a document. Documents declare contracts, result
  contracts, roots, transitions, and named parallel connections with their own
  contracts, authority, and match; unstated parts default to `payload` under
  `workflow`. Node-tool tests publish by connection name, apply a transition,
  set an outbound object type, and see core refuse a wrong contract and
  missing authority.
- A typed end-to-end flow runs a human entry with a two-tag root and a command
  whose result reaches an all-of connection and a `bytes` connection; core
  records the entry's whole ceiling on the delivered package.
- External nodes take core moves; moves for program or human nodes report
  `not_external`. An outside client imports a directory, sends it over a
  `workspace` connection to a command, and exports the command's edited
  workspace from an inbox.
- Edits keep a node or connection only while core declares it the same and
  refuse changed contracts or new tags. Saved previews survive a server kill
  and still commit.
- The stress games run their worlds as documents of external nodes, while
  core in tandem runs a declaration the game writes independently; server,
  core, and the rules decided every move alike, across joins, transitions,
  outbound packages, races, and crashes.

## Entering worker terminals from the graph — 2026-09-27

Passed: **244 app Rust tests**, including the real CLI/PTY checks in
[`node_panes.rs`](../tests/session_cli/node_panes.rs). All-target Clippy with
warnings denied, formatting, and `git diff --check` also pass.

- Enter in the graph resolves the selected agent in the owning session and
  attaches to its existing terminal. Typing and resize reach that worker.
  Ctrl-B D and Ctrl-B G return to the graph; repeated entry retains the same
  process and terminal identity.
- Detaching the parent session while inside a worker cancels the nested view
  and releases both controlling sockets. Manager and worker continue running;
  reattachment displays the same worker. Input bytes reach the worker exactly
  once and graph navigation is not forwarded to it.
- Non-agent and stopped nodes show an error without starting a process.
  Worker exit returns to the graph, and returning to Pi restores its input.
  Cross-session lookup and stale attachment identities are rejected.
- A migration-lock test failed in the first concurrent full run, then passed
  independently and in the full rerun. Pane tests use local shell fixtures;
  native Codex conversation/MCP checks are recorded separately below.

## Rebuild recovery and explicit server shutdown — 2026-09-27

Passed: **242 app Rust tests** (`cargo test --locked --quiet`), all-target
Clippy with warnings denied, formatting, and `git diff --check`.

- Ordinary connections still reject a different app/core build before sending
  operations. CLI tests cover `ls`, `server status`, and `server start`.
- Explicit `server stop` can shut down an older build with the same wire
  protocol. The request targets the server instance observed in its handshake;
  mismatched protocol versions and identities still fail. Automatic startup
  does not stop or replace an incompatible server.
- Missing session records name their exact path in recovery errors. Healthy
  sessions still load, and orphaned runtime files remain untouched.

## Persistent Codex app-server and incoming messages — 2026-09-27

Passed: **239 app Rust tests** and **4 opt-in native Codex tests**, run
separately against installed Codex **0.157.1**. The message fixture supplies
Responses events over localhost; no external model request is made.

```sh
cargo test --locked --quiet
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
git diff --check
cargo test --locked --lib -- --ignored --nocapture
```

- Each native node now owns a persistent app-server on a private Unix socket.
  Its native terminal joins the same live thread with `--remote`. Native checks
  verify the composer, that `/exit` leaves the thread loaded, and that a new
  server resumes the exact saved UUID. Mock checks reject missing/mismatched
  saved sessions and remove only obsolete graph entries from the input queue.
- Initial inputs and runnable incoming packages become recorded conversation
  messages with attempt/task IDs, sender names, text, and attachment handles.
  Existing joins, grants, task ownership, retry policy, and package receipts
  remain in the shared node-tool implementation. Input text is bounded; large
  inputs retain read handles. Open attempts bound queued delivery.
- The native message test holds the first model request open, delivers a second
  message, verifies it stays queued, then has real Codex execute two MCP
  `submit_invocation` calls against the test graph. Both replies reach the sink
  and both messages remain in the same conversation history.
- MCP transport overrides preserve user-configured tool approval policy. A
  separate native check starts turns through the controller, observes their
  MCP approval prompts in the terminal, submits approval through terminal input,
  and verifies both graph replies. Production does not automatically answer
  approval requests. The native MCP discovery/call check also passes.
- Lost queue acknowledgement leaves the exact message receipt prepared and
  terminates delivery without replay. RPC tests cover interleaved notifications,
  independent request/approval ID namespaces, concurrent calls, and disconnect.
  Failed turns use task retries; already committed work is not retried.
- Existing process supervision, forced cancellation, MCP revocation, command
  overrides, workspace retention, workflow reconciliation, and manager tests
  still pass. One manager-shell test timed out in the first full run, then
  passed independently and in the final full suite.

Codex app-server/remote transport is version-sensitive; these checks target the
installed version above. Graph UI node pane selection and a full project run
against live models remain outstanding. Ordinary assistant text does not publish
a graph result; the agent uses its selected graph tools. A successful turn may
leave an attempt open across conversation turns. See [WORKFLOWS.md](WORKFLOWS.md)
for queueing, approval, recovery, and delivery limits.

## Node MCP and tool selection — 2026-09-27

Passed: **238 app Rust tests**, all-target Clippy with warnings denied,
formatting, and `git diff --check`. Two native checks are opt-in and excluded
from that count; the Codex MCP interoperability check passed separately on
Codex **0.157.1**.

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
git diff --check
cargo test --locked --lib node_tool::tests::mcp::native_codex_discovers_and_calls_the_selected_node_tools -- --ignored --nocapture
```

- [MCP tests](../src/node_tool/tests/mcp.rs) launch the real stdio proxy and
  verify selected catalogs, grant enforcement, invalid calls, live list-change
  notifications, concurrent waits, cancellation, and partial-frame handling.
- A package is delivered, read, and published through MCP against a real core
  fixture. Receipt tests compare exact returned text with core's recorded
  digest and verify that sent marks wait for proxy delivery acknowledgement.
  Lost replies remain prepared; accepted mutations settle after disconnection.
- Connection tests reject another execution's token, expire stopped endpoints,
  preserve the node across client loss, and exit the proxy after server loss
  even with stdin still open. [Workflow tests](../tests/persistent_nodes.rs)
  verify automatic provisioning, selection changes without process restart,
  and revocation during suspension.
- The installed Codex MCP client discovers only the selected `inspect_node`
  tool and successfully calls it. This uses an ephemeral thread and fixture
  data, with no model turn or user-config changes.

The implementation and limits are described in [NODE_MCP.md](NODE_MCP.md).
Automatic incoming-work wakeups, worker panes, and a full interactive
multi-agent project acceptance run remain outstanding.

## Persistent Codex node foundation — 2026-09-27

Passed: **228 app Rust tests**, all-target Clippy with warnings denied,
formatting, and `git diff --check`. The native smoke is ignored by the standard
suite and passed separately.

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

The node runtime binds an agent execution to a managed PTY, persistent private
working directory, saved native conversation, and shared node-tool context.
The implementation uses the current sibling core checkout without modifying it.

- [Workflow lifecycle tests](../tests/persistent_nodes.rs) cover startup,
  suspension/resume, stable identity and working directory, graph/grant updates,
  config replacement, kind changes, removal, forced cancellation, and visible
  process failures. Initial input remains pending throughout these checks.
- [Launcher tests](../src/node_runtime/codex.rs) cover exact conversation
  resumption, missing or mismatched saved state, custom commands, and cancelled
  bootstrap cleanup.
- [Supervisor tests](../src/node_runtime/process.rs) exercise startup permission,
  lifetime-pipe loss, actual owner-process SIGKILL, terminal event-reader
  compatibility, and cleanup of jobs in other process groups in the owned
  terminal session. [Terminal tests](../src/terminal.rs) cover retained views,
  cancelled launch, natural exit, and explicit shutdown.

Native Codex **0.157.1** opened its resumed composer in the managed PTY and
resumed the same conversation UUID after shutdown. The ignored smoke test
creates an empty local conversation and makes no model turn. Its fixture uses
a process-local trust override for its empty working directory; production
retains normal native trust, onboarding, and approval behavior.

```sh
cargo test --locked --lib node_runtime::codex::tests::native_codex_idle_smoke -- --ignored --nocapture
```

This verifies the process/session foundation. Node MCP, incoming-work delivery,
package handoffs, and worker terminal panes remain unfinished; these checks do
not establish end-to-end communication between interactive agents.

## Persistent shell and session panel — 2026-09-26

Passed: **110 app Rust tests and 31 Pi tests**, TypeScript checking, all-target
Clippy with warnings denied, formatting, `git diff --check`, and the locked
offline build against committed core.

The sibling core working tree contains an unfinished API refactor that already
prevented the app from compiling before this change (56 baseline errors). This
session update leaves that work untouched. Verification uses an isolated copy of
the last committed core, `9336977`, with the corresponding app lockfile from
`e2d709a`. App source is shared with the real checkout; the verification workspace
is `/private/tmp/ontography-shell-r49ti58g/ontographyapp-v2`. The ordinary checkout
still needs adaptation to the new core APIs before it can build against that
uncommitted core. No dependency pin or core source change was introduced here.

- [Manager lifecycle tests](../tests/manager_lifecycle.rs) verify exclusive shell
  ownership, actual Pi process startup, Ctrl-C, abrupt launcher loss, Pi cleanup,
  terminal recovery, and reentry in the same shell.
- [CLI tests](../tests/session_cli.rs) verify `/quit`, saved/latest conversation
  selection, persistent shell variables across detach/attach, failed Pi launch
  recovery, resize, foreground interruption, detached shell exit, background-job
  cleanup, session isolation, server restart, and permanent closure.
- [Panel tests](../src/terminal_panel.rs) verify explicit process modes, launch
  errors, unavailable observations, bounded outstanding reads, exact package
  counts, responsive geometry, and clipped Unicode/style rendering.
- Review corrected failure isolation during exited-shell reconciliation and
  fenced replacement against an old shell's exit. A cursor-query race during
  split-view redraw was also removed.

Native **Pi 0.85.1** passed the complete flow in isolated storage: initial launch;
`/quit` to an empty-graph shell panel; graph status updates; wide/narrow layouts;
Ctrl-B G graph display; detach/attach with a retained shell variable; `pi` restoring
history written by Pi's native session writer; `/new` followed by `pi` selecting
the latest conversation; shell `exit` suspending the graph; and explicit attachment
resuming the same conversation and graph. Terminal attributes were restored on
detach and exit. No model call was needed. Evidence is retained temporarily at
`/private/tmp/ontography-cli-native-14_10lze/evidence.json`; its server was stopped.

The idle default server was replaced with this build at `~/.ontography`. Both
saved session records, their selections, and Pi references were preserved;
there are no active terminals or graph runs. Restart rediscovered the two legacy
directories `d5607719-44b0-42c9-a7df-7561b94bacdd` and
`f9957648-e9fc-4717-a4a8-c5ad44001bd2`, each containing only runtime artifacts and
no `session.json`. They remain untouched and appear as session recovery errors.
The initial activation check rejected those errors; subsequent inspection
confirmed the valid sessions were unchanged and the replacement server healthy.
Final evidence: `/private/tmp/ontography-shell-r49ti58g/activation-final.json`.
Application fingerprint:
`276b164a74256cc94fc2ca7c9a4f3b0a5404845e056bf30124b8f4731b1c867e`;
core fingerprint:
`0b5f4f316927115ac62d75b85bafa76438130d060a4a39c84db2e2307ba659e3`.

## Core extension and retirement integration — 2026-09-25

Passed: **104 app Rust tests, 51 core Rust tests, and 31 Pi tests**, TypeScript
checking, Clippy across all targets with warnings denied in both Rust crates,
formatting, and the locked offline build. Core retains one previously ignored
failed-SQLite-commit probe; it was not counted as passing.

- [Extension tests](../tests/extensions.rs) cover monotone admission, retained
  authority and topology, stale rewrite plans, persistent recovery, both sides
  of an interrupted journal commit, executable resume without replaying entry
  input, and rejection by fixed-graph fact APIs. An internal boundary test
  verifies both journal forms fit the metadata reader limit before core mutates.
- [Request tests](../tests/extension_scope.rs) exercise actual server dispatch,
  nested schema validation, session ownership, lifecycle gates, and receipt
  deduplication.
- [Retirement tests](../tests/retirement.rs) cover explicit retirement, all four
  reasons, local rewrite cleanup, `All` receiver route removal, invalid evidence,
  consumed/retired rejection, pagination, exports, reopening, and session scope.
- Pi tests verify capability discovery, generated schemas, default run-group
  availability, retirement group activation, scoped dispatch, and exact decimal
  revisions. Independent review also checked crash recovery and core application
  reconstruction.

A real compiled-CLI check used an isolated server: create a session/run, extend
all four vocabulary categories, submit and retire a package, inspect its evidence,
stop/restart the server, explicitly resume, and compare the recovered vocabulary,
unchanged graph, journal, package disposition, and retirement page. It passed;
the isolated server was stopped. Evidence is retained temporarily under
`/private/tmp/ontography-core-update-opz9g2tm/`.

Core's public API gained schema iterators and an application reconstruction
helper that retains executables and grammar. Its new integration test verifies
live extension followed by application resume without a second initial input.
The original core additions were committed separately before this integration.
See [CORE_UPDATE.md](CORE_UPDATE.md) for semantics, limits, and examples. Core
schema 9 requires fresh runs; this update implements no run migration.

The default server now runs the verified update at `~/.ontography`, with zero
runs, zero sessions, and no recovery errors. Application fingerprint:
`ff2cc187f5ac9b34697e2b024091d53ddb4ffea5a43d79b9aaaeb6ea58e60412`;
core fingerprint:
`0b5f4f316927115ac62d75b85bafa76438130d060a4a39c84db2e2307ba659e3`.
The previous manager was stopped and no executable worker bindings were active.
The old server was gracefully stopped, then its store was renamed intact to
`/Users/hershybar/.ontography.archive-core8-20260926T055318Z-e450eb46`.
The archive includes `previous-ontography`, the matching old binary. Directory
identity and hashes of all 12 retained JSON metadata files were checked after
rename. The earlier legacy archive was untouched. An initial replacement attempt
safely rolled back because the legacy alias required an initialized store; the
successful retry initialized the new store explicitly, then checked default-path
commands. `cutover.json` in the evidence directory records the result.

## Explicit attachment and new-session CLI — 2026-09-25

Bare `ontography` now always creates a session. Top-level `new`, `ls`, `attach`,
`close`, and the other session controls accept exact IDs or unique exact names.
`ls` separates session, terminal, and graph state; the existing nested command
forms and raw `session.list` API remain available.

Passed: **92 Rust tests**, Clippy across all targets with warnings denied,
formatting, `git diff --check`, and the locked build. The two new
[CLI integration tests](../tests/session_cli.rs) exercise real subprocesses,
server sockets, and PTYs: independent bare launches, explicit attachment,
manager reuse/replacement, graph survival after manager exit, scoped closure,
name ambiguity, ID precedence, listing, and noninteractive creation preflight.
The existing concurrent frontier test now accepts `run_suspended` when suspension
wins before observation begins; it still requires status/receipt replies before
the waiting request and prompt completion after suspension.

Native Pi **0.85.1** independently passed the following checks in temporary
storage, using the compiled application fingerprint
`f0aecd11551541253c899177c2a38cb30874948323ceee5475a45ba8c3170218`:

- Bare launch, detach, and another bare launch create two independent sessions
  and managers; the first graph remains active.
- Named `attach --ui` displays the original graph and reuses its Pi terminal.
- Native `/quit` returns to the shell, restores terminal attributes, and keeps
  the session, graph, and active conversation identity.
- Explicit attachment creates a replacement Pi process and renders saved
  history. The history was seeded with Pi's own session writer; no model call
  was needed. Another `/quit` and attachment retain that history.
- `close NAME` closes only that session and graph, leaving the other manager
  detached and running. `ls --json` reports both states correctly.

Evidence and terminal captures are retained temporarily under
`/private/tmp/ontography-cli-native-07n_yc29/`; its isolated server was stopped.
Pi extension code and sibling core were unchanged in this CLI update.

The idle default server was gracefully replaced with this build. All four run
summaries and the valid closed session record were preserved exactly: three
graphs remain suspended and one remains closed. Restart discovered two older
directories lacking `session.json` (`d5607719-44b0-42c9-a7df-7561b94bacdd` and
`f9957648-e9fc-4717-a4a8-c5ad44001bd2`). They contain legacy runtime artifacts,
were left untouched, and appear in session recovery errors. Upgrade evidence is
retained at `/private/tmp/ontography-cli-update-54lfbnk7/upgrade.json`.

## Persistent sessions — 2026-09-25

The persistent manager-session milestone is implemented and verified on macOS,
with native Pi 0.85.1 and the unchanged sibling core. The checked application
fingerprint is `64fdb11944f63fd5ec52acd36c89cefa983d33199189fae12172a71f301b6d34`;
the core fingerprint is
`819006a6fa15f139ccaee99ea2c17b7f1f51fc5f2270eafeeb95758c06904cda`.
All seven embedded Pi assets participate in the application fingerprint.

### Automated gates

Passed: **90 Rust tests, 30 Pi tests**, TypeScript checking, Clippy across all
targets with warnings denied, formatting, and `git diff --check`. The locked
offline build succeeds. Socket, PTY, and local iroh tests ran with permission to
create their required local resources.

| Requirement | Evidence |
| --- | --- |
| Durable session, optional graph, exclusive run ownership | Eight [session tests](../tests/session_ownership.rs): bind once, scoped dispatch, explicit adoption, saved conversations, close/selection recovery, and initialization retries before/after core creation. |
| Lifecycle controls remain usable | Session tests cover observation waits during suspension; [manager lifecycle](../tests/manager_lifecycle.rs) covers concurrent ensure, manager before graph initialization, suspension, explicit resume, and shutdown. |
| Detached native terminal | Fourteen tests across [terminal backend](../src/terminal.rs) and [client](../src/terminal_client.rs): detached draining, ordered input, exclusive controller, stale identities, terminal queries, process reaping, and history navigation isolated from live input/state. |
| Pi conversations retain graph identity | [Extension tests](../pi/test/session.test.ts) cover native lifecycle hooks, membership checks, preferences, failed registration, replacement, and saved-history materialization. Native checks below exercise the real harness. |
| Typed tools and connection recovery | [Bridge/tool tests](../pi/test/) verify session envelopes, reconnection, cancellation, no mutation replay, bounded output, schemas, and exact wide identifiers. |
| Fast slash commands | Installed-provider regressions reproduce stale-prefix and obsolete-candidate corruption; the public completion wrapper preserves current command text and arguments. Native checks exercise the correction. |
| Migration and recovered paths | Eight migration tests cover exclusive ownership, archival, interrupted rename/alias stages, ambiguous stores, and substituted directories. A session integration test relocates real native-history files and checks ownership through the old path alias. |

### Native acceptance

Two independent PTY harnesses exercised the same compiled application build.
Both isolated servers were explicitly stopped after verification.

- Native Pi made an actual `ontography_run_inspect({})` call using its bound run.
  Its first saved history became durably marked materialized. `/new`, `/resume`,
  `/clone`, and `/reload` preserved the app session and graph; `/graph` continued
  to work after those transitions. Burst slash input arrived and executed intact.
- `--ui` displayed the session's graph first, then returned to the same native Pi
  process. `/hotkeys` produced 93 retained history rows; Ctrl-B `[` and Page Up
  displayed older output, and `q`/Escape restored live input. Resizing from 80×24
  to 120×35 reached the server-owned terminal.
- Detach restored the original terminal attributes, alternate screen, and paste
  mode. Reattachment retained the same Pi PID and terminal identity. A second app
  session had a different manager and graph. Server restart retained the original
  session, conversation, and run IDs while creating a replacement terminal.

Native evidence was retained at
`/private/tmp/ontography-native-session-fncoalu3/` and
`/private/tmp/ontography-native-terminal-ublseek2/` during verification. Those
directories contain assertion records and terminal captures; they are temporary
verification artifacts. Native `/fork` was not separately exercised; its extension
hooks are covered by automated tests. Terminal presentation supports text and
color; inline images and hyperlinks are disabled.

### Completed local storage migration

The old server was gracefully stopped with its matching client. The verified new
binary then completed the authorized migration:

- Current store: `/Users/hershybar/.ontography`.
- Legacy archive:
  `/Users/hershybar/.ontography.archive-e0fa8fb7-5475-4a9e-bd4a-2c3a4777a00e`.
- `/Users/hershybar/.local/share/ontography` is an alias to the current store.
- Directory identities prove both trees were renamed intact. SHA-256 comparisons
  verified all 13 stopped run/definition files before and after relocation.
- All three previously active runs were resumed. Their run IDs, declarations,
  projects, checkpoints, admission, revisions, topology, and package frontiers
  matched pre-migration inspection. Both store paths connect to the same server;
  recovery errors are empty.

| Preserved run | Definition | Nodes / edges | Revision |
| --- | --- | --- | --- |
| `12dccebd-f344-4482-a0b8-4006f7e472ea` | fifteen-node-chain | 15 / 14 | 0 |
| `6c2d1977-4fd0-473a-b11d-448e92536b2a` | fifteen-node-chain-mixed | 15 / 21 | 0 |
| `a380573c-a8f0-4ff6-bdf0-8a1250ea99d7` | logical-flow | 2 / 1 | 0 |

Existing runs remain available for explicit session adoption; the migration does
not invent an earlier Pi association. Cutover evidence is retained under
`/private/tmp/ontography-cutover-6e7urwx1/`.

### Scope

This completes the manager-session foundation: native Pi ownership, multiple
conversations, session-bound graph tooling, persistent terminal attachment,
graph display, recovery, and storage migration. Concrete Codex worker nodes,
their message/workspace contracts, node MCP, default editing productions, and
worker process reconciliation remain the following implementation phase.

## Historical management release — 2026-09-24

Verified on macOS on 2026-09-24, using Rust 1.97.1, Node.js 22.22.3, Pi 0.85.1,
and the sibling `ontography-core` checkout. The application uses public core APIs;
this implementation made no changes to the sibling core.

### Automated checks

```sh
cargo test --locked --offline
cargo clippy --locked --offline --all-targets -- -D warnings
cargo fmt --check
cd pi
npm run check
npm test
```

The release checks passed 59 Rust tests and 14 Pi tests, TypeScript checking,
formatting, and Clippy with warnings denied. Local socket and iroh tests require
permission to create listeners. Tests register deterministic workers and providers;
the production registry does not ship those fixtures.

| Plan phase | Evidence |
| --- | --- |
| 1. Server and Pi | [Process tests](../tests/server_lifecycle.rs) cover concurrent startup, detached ownership, reconnect, build mismatch, response loss, and partial-frame multiplexing. Real native Pi loaded the embedded extension. |
| 2. Definitions and durable runs | [Management tests](../tests/management_flow.rs) cover admission, suspension, reopening, terminal closure, and persisted topology. [Recovery tests](../tests/state_recovery.rs) isolate damaged manifests from healthy runs. |
| 3. Governed work | Workflow tests exercise delivery, consumption, and contract rejection. [Management tests](../tests/management_flow.rs) reject schema-valid authority above the node's ceiling without changing revision or frontier; the same payload succeeds with permitted authority. [Execution tests](../src/tools/execution.rs) prove registered core work continues after clients disconnect and waits leave stop controls usable. |
| 4. Rewrites | Management tests check preparation, stale rejection, commit, and persistence. The live model flow independently confirms the committed topology through a second client. |
| 5. Packages and workspaces | [Content](../src/tools/content.rs) and [workspace](../src/tools/workspace.rs) tests cover package closure, edits/deletions/symlinks, unchanged references, checkpoints, GC, reopening, and merge conflicts. [Suspension tests](../src/state_tests.rs) preserve edits and recovery access after capture failure. |
| 6. Context, network, and applications | [Context tests](../src/tools/context.rs) cover grants, invocation/run scope, budgets, exact publication retry, and observed receipts. [Network tests](../tests/network_flow.rs) transfer a complete package closure between local endpoints and release resources for immediate resume. [Application lifecycle tests](../src/tools/project_tests.rs) verify native/provider start, suspension, and reconstruction in a fresh service. |
| 7. Management experience | [Pi tests](../pi/test/) cover schemas, wide identifiers, cancellation, reconnection, no mutation replay, build identity, and bounded previews. Installed Pi performed actual management tool calls; copied-binary launch verified embedded extension assets. |
| 8. Rust TUI | [UI tests](../src/ui/) cover graph identity, loops/parallel edges, resizing, selection, stale observations/previews, Pi streaming, and child cleanup. [Node-page tests](../src/ui/frontier.rs) use 400 real outbound packages to inspect a node absent from the global first 100, reach subsequent pages and history, and reject mixed revisions and obsolete responses. Actual terminal checks cover rendering, model tools, detach, session restoration, and terminal restoration. |

### Live management and terminal checks

Both native Pi and `ontography --ui` launched with the installed Pi configuration.
A live model read the example declaration, called graph validation and run start,
prepared and committed `remove_receiver`, and inspected the result. An independent
CLI inspection after UI detach confirmed node A, zero edges, revision 1, and open
admission. This checks the generated nested tool schemas against the real harness.

Client exit preserved the same server process and live run. Reattachment restored
the graph; `/session` restored the earlier Pi transcript. The Rust UI rendered at
80×24 and repainted at 120×35. Node selection updated the displayed details.
Ctrl-Q restored the original terminal attributes, cursor, alternate screen, and
bracketed-paste mode. Only explicit test cleanup stopped the isolated servers.
See [UI integration evidence](../src/ui/README.md).

Crash tests separately killed a server, reconstructed committed state, required
explicit resumption, and rejected expired transient handles. Failed workspace
capture retained recovery resources. Closing a suspended run did not relaunch its
declared executable.

### Scope of these results

- Architecture items 1–7 remain subsequent work: concrete Codex nodes,
  app-specific package contracts and edge definitions, receiving harnesses, and
  node-scoped MCP. The management layer exposes existing core package/workspace
  functionality and registered execution/application integrations now.
- Fact restore is checked transient inspection. Core has no public durable fact
  import or verified replay for rewrite/transfer histories. Ordinary reopening of
  those histories uses core's current-state loader.
- Workspace checks ran on the available macOS filesystem. Checkout requires
  supported copy-on-write behavior; these results do not establish support for
  other filesystems or execution confinement.
- The core's ignored failed-SQLite-commit orphan-retention probe remains an upstream
  limitation. App tests establish only the retention/GC paths they exercise.

The [binding inventory](CORE_BINDINGS.md) records precise operation coverage,
ownership, and additional public-core limitations.
