# Rust graph client

## Current session graph view

Native Pi runs in the session's server-owned shell PTY. Pi is full screen; after `/quit`, the shell shares the client with a session status panel. `/graph` in Pi or Ctrl-B G requests the owning session's graph. The attached Rust client opens `ui::run_graph_view` from `session_graph.rs`; `q` or Escape restores the same terminal. `ontography --ui` opens this graph view first through that same attachment. There is no independent run picker in this path.

The graph view reuses the Ratatui renderer described below. See [the session guide](../../docs/SESSIONS.md) for ownership, controls, and lifecycle, and [VERIFICATION.md](../../docs/VERIFICATION.md) for current acceptance evidence.

## Historical combined RPC interface

The following describes the initial implementation retained in source and tests. It is no longer the CLI's `--ui` path.

`ui::run(client, UiOptions { pi_command })` owns the terminal and launches the
prepared Pi command in RPC mode. The launcher supplies executable, project,
extension, instructions, and environment. Pi continues to own its conversation,
model, and tool execution. The Rust server owns graph runs after this client exits.

The UI refreshes core snapshots through `run.list` and `run.inspect`. Local node
selection and viewport positions are presentation state. Direct `:call` commands
use the same server operations as Pi's tools. There is no local graph editor or
second graph-state authority.

Controls: Tab selects a pane; Enter submits the prompt; Shift-Enter adds a newline.
In the graph pane, arrows select nodes/pan, Shift-Up/Down pan vertically, `[ ]`
select a run, Enter loads node-scoped received/outbound pages, and `o` cycles through
the loaded packages and opens their actual history (loading first pages if needed).
Each phase is bounded to 100 packages: `n` advances phases with continuation cursors;
`r` reloads from the beginning. Details show the page revision and continuation
cursors. Revision changes during pagination require `r`; mixed revisions are
rejected. Package pages and history responses follow the selected server/run/node,
and late responses from previous selections are discarded. `p` toggles a prepared rewrite
preview; preview state never replaces the live graph. In the conversation,
Up/Down or PageUp/PageDown scroll and End follows the latest response. Escape
cancels Pi's queued/current work. Ctrl-Q detaches without stopping the server.
`/new`, `/session PATH`, `/clone`, and `/fork ENTRY` control the Pi conversation.
`:package PACKAGE_ID` inspects package history in the selected run. `:call OP JSON`
invokes the same bindings used by the management agent.
The run picker is bounded to its first page; `:run RUN_ID` selects an exact run
beyond that page without loading an unbounded index.

## Renderer decision

Evaluated the public [tuiflow source](https://github.com/kraemahz/tuiflow) version
0.2.2. Its `GraphDocument::add_edge_with_data` in `src/document.rs` rejects self
loops and repeated identical port pairs. Parallel core edges would require
synthetic ports; a core self loop cannot use that insertion API. Its `NodeId` and
`EdgeId` are generated u32 values, requiring a separate stable-ID mapping. Its
manifest also uses Ratatui 0.29/Crossterm 0.28, whereas this application uses 0.30/0.29.
Layout takes explicit node positions and its existing render tests cover clipped
viewports. These are source findings, not a claim that the candidate fails all
cycles or cannot be adapted.

The application uses a small native Ratatui widget. Nodes retain their core string
IDs; all edges have independent lanes, including self loops and parallel edges.
The incident-edge list preserves full edge identities when routes overlap in a
small terminal. No DAG layout or port semantics are imposed on core. Tests cover
cycles/parallel edges/loops, selection under topology replacement, narrow resize,
pan clipping, and wide wire counters.

## Pi RPC contract

Checked the installed Pi 0.85.1 RPC documentation and
[upstream RPC reference](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md).
The client uses LF-delimited records, command IDs, text deltas, final-message
reconciliation, tool progress correlated by call ID, session restoration, and `agent_settled`.
`agent_end` alone does not mark Pi idle. Select/confirm/input/editor extension
dialogs are supported; notifications and editor prefill are displayed. Terminal
only custom extension components are unavailable in Pi RPC itself.

Pi stderr is drained separately. On client exit, stdin closes and Pi receives a
bounded opportunity to dispose its runtime before forced cleanup. Graph server
lifecycle operations are never part of client cleanup. A fake subprocess test
checks Unicode JSONL framing and orderly child cleanup without model calls.

## Integration verification

Verified with the installed Pi 0.85.1 in an actual PTY on 2026-09-24:

- Native Pi and the Rust graph client both launched the Ontography extension.
- A live model called `ontography_system_status`; a separate turn read the example
  declaration, validated and started it, prepared and committed `remove_receiver`,
  then inspected the resulting one-node graph. An independent CLI inspection
  confirmed revision 1, node A, no edges, and open admission after UI detach.
- Reattaching restored the graph; `/session` restored the earlier Pi transcript.
- The 80×24 screen showed revision and the selected node; resizing to 120×35
  repainted. Ctrl-Q restored the original terminal attributes, alternate screen,
  cursor, and bracketed-paste mode.
- Client exits left the same server process and graph run alive. Only explicit
  test cleanup stopped the server, preserving its runs.

These checks exercise logical graphs and the real management agent. Production
Codex worker definitions and their node terminals remain a separate implementation.
