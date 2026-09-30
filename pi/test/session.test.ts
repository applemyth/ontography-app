import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Check } from "typebox/value";
import { BridgeClient, BridgeError, type Arguments, type Hello } from "../client.ts";
import { registerSessionHooks, SessionBinding } from "../session.ts";
import { ManagementTools } from "../tools.ts";

type Hook = (event: Record<string, unknown>, ctx: ExtensionContext) => Promise<unknown>;
const sessionId = "app-session-1";
const directory = "/private/tmp/onto-history";

function fixture() {
  const requests: Array<{ operation: string; args: Arguments }> = [];
  const state = { active: "conversation-1", groups: ["content"], run: "run-1", revision: "1", failRegistration: false, failContext: false,
    historyExists: false, materialized: false, graph: undefined as unknown };
  const membership = new Map([["conversation-1", `${directory}/conversation-1.jsonl`], ["conversation-2", `${directory}/conversation-2.jsonl`]]);
  const hello: Hello = {
    protocol_version: 1, server_id: "server-1", app_version: "test", core_version: "test", app_build: "test", core_build: "test",
    operations: [
      { name: "flow.status", group: "flow", description: "Inspect", mutating: false, parameters: { type: "object", properties: { run_id: { type: "string" } }, required: ["run_id"], additionalProperties: false } },
      { name: "flow.suspend", group: "flow", description: "Suspend", mutating: true, parameters: { type: "object", properties: { run_id: { type: "string" } }, required: ["run_id"], additionalProperties: false } },
      { name: "content.read", group: "content", description: "Read content", mutating: false, parameters: { type: "object", properties: {} } },
      { name: "workspace.list", group: "workspace", description: "List workspaces", mutating: false, parameters: { type: "object", properties: {} } },
      ...["session.context", "session.conversation", "session.preferences", "session.create", "terminal.graph", "server.stop"].map((name) => ({
        name, group: name.split(".")[0]!, description: name, mutating: name !== "session.context", parameters: { type: "object", properties: { session_id: { type: "string" } } },
      })),
    ],
  };
  const load = () => {
    let activeTools = ["read", "bash"];
    const hooks = new Map<string, Hook>();
    const commands = new Map<string, { handler: (args: string, ctx: ExtensionContext) => Promise<void> }>();
    const definitions = new Map<string, ToolDefinition>();
    const notifications: string[] = [];
    const entries: Array<{ type: string; data: unknown }> = [];
    const pi = {
      on(name: string, handler: Hook) { hooks.set(name, handler); },
      registerCommand(name: string, command: { handler: (args: string, ctx: ExtensionContext) => Promise<void> }) { commands.set(name, command); },
      registerTool(tool: ToolDefinition) { definitions.set(tool.name, tool); },
      getActiveTools() { return activeTools; },
      setActiveTools(names: string[]) { activeTools = names; },
      appendEntry(type: string, data: unknown) { entries.push({ type, data }); },
    } as unknown as ExtensionAPI;
    const client = new BridgeClient({ socketPath: "/unused", appSessionId: sessionId });
    client.connect = async () => hello;
    let disconnects = 0;
    client.disconnect = () => { disconnects++; };
    client.call = async (operation, args) => {
      requests.push({ operation, args });
      let result: unknown = {};
      if (operation === "session.conversation") {
        assert.equal(args.session_id, sessionId);
        if (args.action === "activate") {
          if (state.failRegistration) throw new BridgeError("session_binding", "Registration rejected");
          assert.equal(typeof args.conversation_id, "string");
          assert.equal(typeof args.path, "string");
          membership.set(args.conversation_id as string, args.path as string);
          state.active = args.conversation_id as string;
          state.materialized = state.historyExists;
        } else if (args.action === "check") {
          if (![...membership.values()].includes(args.path as string)) throw new BridgeError("foreign_conversation", "Import explicitly");
          result = { allowed: true };
        }
      } else if (operation === "session.context") {
        if (state.failContext) throw new BridgeError("session_binding", "Context unavailable");
        result = { session: { session_id: sessionId, run_id: state.run, pi: { active_conversation_id: state.active, preferences: { tool_groups: [...state.groups] } } },
          conversations_dir: directory, graph: state.graph ?? { run_id: state.run, revision: state.revision } };
      } else if (operation === "session.preferences") {
        state.groups = [...(args.preferences as { tool_groups: string[] }).tool_groups];
        result = { preferences: { tool_groups: [...state.groups] } };
      } else if (operation === "flow.status" || operation === "flow.suspend") {
        if (args.run_id !== undefined && args.run_id !== state.run) throw new BridgeError("foreign_run", "This manager owns another run");
        result = { run_id: state.run };
      }
      return { result, receipt: { client_id: client.clientId, request_id: "request", operation, app_session_id: sessionId } };
    };
    const binding = new SessionBinding(client, sessionId);
    const tools = new ManagementTools(pi, client, binding);
    registerSessionHooks(pi, client, tools, binding);
    const context = (conversation = state.active) => ({
      hasUI: true,
      sessionManager: { getSessionId: () => conversation, getSessionFile: () => `${directory}/${conversation}.jsonl` },
      ui: { notify(message: string) { notifications.push(message); }, setStatus() {} },
    }) as unknown as ExtensionContext;
    return { client, binding, tools, definitions, commands, notifications, entries, activeTools: () => activeTools, disconnects: () => disconnects,
      fire: (name: string, event: Record<string, unknown> = {}, conversation?: string) => hooks.get(name)!(event, context(conversation)), context };
  };
  return { state, requests, membership, hello, load };
}

test("native new, resume, fork, and reload retain the graph and fixed workflow tool surface", async () => {
  const server = fixture();
  let runtime = server.load();
  await runtime.fire("session_start", { reason: "startup" });
  assert.ok(runtime.activeTools().includes("ontography_flow_status"));
  assert.equal(runtime.definitions.has("ontography_content_read"), false);
  assert.equal(runtime.entries.some((entry) => entry.type === "ontography_tool_groups"), false);
  for (const reason of ["new", "resume", "fork", "reload"]) {
    await runtime.fire("session_shutdown", { reason });
    assert.equal(runtime.disconnects(), 1);
    runtime = server.load();
    await runtime.fire("session_start", { reason }, reason === "reload" ? server.state.active : `conversation-${reason}`);
    assert.ok(runtime.activeTools().includes("ontography_flow_status"));
    assert.equal(runtime.definitions.has("ontography_workspace_list"), false);
    assert.equal(server.state.run, "run-1");
  }
  assert.equal(server.requests.some(({ operation }) => operation === "run.start" || operation === "run.close" || operation === "session.close"), false);
});

test("foreign resume is cancelled before conversation ownership changes", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  const before = server.state.active;
  assert.deepEqual(await runtime.fire("session_before_switch", { reason: "resume", targetSessionFile: "/another-app/foreign.jsonl" }), { cancel: true });
  assert.equal(server.state.active, before);
  assert.equal(await runtime.fire("session_before_switch", { reason: "resume", targetSessionFile: `${directory}/conversation-2.jsonl` }), undefined);
  assert.equal(server.state.active, before, "checking a resume target does not activate it before Pi succeeds");
});

test("failed registration and lost context keep graph tools closed until association is recovered", async () => {
  const server = fixture();
  server.state.failRegistration = true;
  const runtime = server.load();
  await runtime.fire("session_start");
  await runtime.tools.refresh();
  const suspend = runtime.definitions.get("ontography_flow_suspend")!;
  await assert.rejects(suspend.execute("suspend", {}, undefined, undefined, {} as never), /Registration rejected/);
  assert.equal(server.requests.some(({ operation }) => operation === "flow.suspend"), false);
  server.state.failRegistration = false;
  await runtime.fire("before_agent_start", { systemPrompt: "Base prompt" });
  assert.ok(runtime.activeTools().includes("ontography_flow_status"));
  server.state.failContext = true;
  await runtime.fire("before_agent_start", { systemPrompt: "Base prompt" });
  await assert.rejects(suspend.execute("suspend", {}, undefined, undefined, {} as never), /Context unavailable/);
  assert.equal(server.requests.some(({ operation }) => operation === "flow.suspend"), false);
});

test("bound tools default the current run but preserve explicit mismatches for rejection; internal operations are hidden", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  const inspect = runtime.definitions.get("ontography_flow_status")!;
  assert.equal(Check(inspect.parameters, {}), true);
  assert.equal(Check(inspect.parameters, { run_id: 12 }), false);
  assert.equal(Check(server.hello.operations[0]!.parameters as never, {}), false, "the published core schema remains unchanged");
  await inspect.execute("inspect", {}, undefined, undefined, {} as never);
  await assert.rejects(inspect.execute("foreign", { run_id: "other-run" }, undefined, undefined, {} as never), /foreign_run/);
  for (const name of ["ontography_session_create", "ontography_session_conversation", "ontography_session_preferences", "ontography_terminal_graph", "ontography_server_stop"]) {
    assert.equal(runtime.definitions.has(name), false);
  }
  assert.ok(runtime.definitions.has("ontography_session_context"));
});

test("fresh graph context is injected after tree changes and graph command targets the attached session", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  server.state.revision = "99";
  await runtime.fire("session_tree");
  const prompt = await runtime.fire("before_agent_start", { systemPrompt: "Original instructions" }) as { systemPrompt: string };
  assert.match(prompt.systemPrompt, /^Original instructions/);
  assert.match(prompt.systemPrompt, /"revision":"99"/);
  assert.match(prompt.systemPrompt, /do not create, copy, or rewind/i);
  await runtime.commands.get("graph")!.handler("", runtime.context());
  assert.deepEqual(server.requests.at(-1), { operation: "terminal.graph", args: { session_id: sessionId } });
});

test("an unavailable graph's reason reaches the agent", async () => {
  const server = fixture();
  server.state.graph = { run_id: "run-1", status: "unavailable", error: { code: "graph_unavailable", message: "this build cannot open the run" } };
  const runtime = server.load();
  await runtime.fire("session_start");
  const prompt = await runtime.fire("before_agent_start", { systemPrompt: "Base prompt" }) as { systemPrompt: string };
  assert.match(prompt.systemPrompt, /Bound graph run: run-1\. The graph is unavailable: this build cannot open the run\./);
});

test("server replacement re-registers the current conversation before continuing", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  const count = server.requests.length;
  server.hello.server_id = "server-2";
  await runtime.definitions.get("ontography_flow_status")!.execute("inspect", {}, undefined, undefined, {} as never);
  assert.deepEqual(server.requests.slice(count).map(({ operation }) => operation), ["session.conversation", "session.context", "flow.status"]);
});

test("completed agent turns report newly saved histories without resetting the graph or tool readiness", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  assert.equal(server.state.materialized, false);
  server.state.historyExists = true;
  await runtime.fire("agent_end");
  assert.equal(server.state.materialized, true);
  assert.equal(server.state.run, "run-1");
  assert.deepEqual(server.state.groups, ["content"]);
  const count = server.requests.length;
  await runtime.definitions.get("ontography_flow_status")!.execute("inspect", {}, undefined, undefined, {} as never);
  assert.deepEqual(server.requests.slice(count).map(({ operation }) => operation), ["flow.status"]);
  assert.equal(server.requests.some(({ operation }) => operation === "run.start" || operation === "run.close"), false);
});

test("retained tools and late events from a replaced runtime cannot reactivate its old conversation", async () => {
  const server = fixture();
  const old = server.load();
  await old.fire("session_start");
  const staleTool = old.definitions.get("ontography_flow_suspend")!;
  await old.fire("session_shutdown", { reason: "new" });
  const current = server.load();
  await current.fire("session_start", { reason: "new" }, "conversation-2");
  const count = server.requests.length;
  await assert.rejects(staleTool.execute("stale", {}, undefined, undefined, {} as never), /session_replaced/);
  await old.fire("agent_end", {}, "conversation-1");
  assert.equal(server.requests.length, count);
  assert.equal(server.state.active, "conversation-2");
});

test("failed reactivation revokes readiness of tools already registered in that runtime", async () => {
  const server = fixture();
  const runtime = server.load();
  await runtime.fire("session_start");
  const tool = runtime.definitions.get("ontography_flow_suspend")!;
  server.state.failRegistration = true;
  await runtime.fire("session_start", { reason: "reload" });
  await assert.rejects(tool.execute("suspend", {}, undefined, undefined, {} as never), /Registration rejected/);
  assert.equal(server.requests.some(({ operation }) => operation === "flow.suspend"), false);
});
