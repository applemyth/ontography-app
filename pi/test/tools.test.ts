import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Check } from "typebox/value";
import { BridgeClient, BridgeError, type Hello } from "../client.ts";
import { ManagementTools, modelResult, type ManagementScope } from "../tools.ts";

function setup(scope?: ManagementScope) {
  let active = ["read", "bash", "external_custom", "ontography_graph_save", "ontography_tools"];
  const definitions = new Map<string, ToolDefinition>();
  const entries: Array<{ type: string; data: unknown }> = [];
  const pi = {
    registerTool(tool: ToolDefinition) { definitions.set(tool.name, tool); },
    getActiveTools() { return active; },
    setActiveTools(names: string[]) { active = names; },
    appendEntry(type: string, data: unknown) { entries.push({ type, data }); },
  } as unknown as ExtensionAPI;
  const text = { type: "string" };
  const hello: Hello = {
    protocol_version: 1, server_id: "server", app_version: "test", core_version: "test", app_build: "app-build", core_build: "core-build",
    operations: [
      { name: "flow.edit", group: "flow", description: "Preview a workflow edit", mutating: true,
        parameters: { type: "object", properties: { run_id: text, document: { type: "object" } }, required: ["run_id", "document"], additionalProperties: false } },
      { name: "flow.status", group: "flow", description: "Read workflow status", mutating: false,
        parameters: { type: "object", properties: { run_id: text }, required: ["run_id"], additionalProperties: false } },
      ...["graph.save", "rewrite.commit", "run.extend", "content.read", "session.create", "terminal.graph", "catalog.list"].map((name) => ({
        name, group: name.split(".")[0]!, description: name, mutating: true,
        parameters: { type: "object", properties: {} },
      })),
    ],
  };
  const client = new BridgeClient({ socketPath: "/unused-in-tool-unit-test", ...(scope === undefined ? {} : { appSessionId: scope.sessionId }) });
  client.connect = async () => hello;
  const tools = new ManagementTools(pi, client, scope);
  return { client, tools, hello, definitions, entries, active: () => active };
}

test("only document tools are registered and all are active without group activation", async () => {
  const fixture = setup();
  const hello = await fixture.tools.refresh();
  assert.deepEqual(fixture.active(), ["read", "bash", "external_custom", "ontography_flow_edit", "ontography_flow_status"]);
  assert.deepEqual([...fixture.definitions.keys()], ["ontography_flow_edit", "ontography_flow_status"]);
  assert.deepEqual(hello.operations.map((operation) => operation.name), ["flow.edit", "flow.status"]);
  const edit = fixture.definitions.get("ontography_flow_edit")!;
  assert.equal(edit.executionMode, "sequential");
  assert.equal(Check(edit.parameters, { run_id: "run", document: {} }), true);
  assert.equal(Check(edit.parameters, { document: {} }), false);
  assert.equal(fixture.definitions.get("ontography_flow_status")!.executionMode, "parallel");
});

test("session tools default the owning run without mutating server schemas", async () => {
  let ready = 0;
  const scope: ManagementScope = { sessionId: "owning-session", async ensureReady() { ready++; } };
  const fixture = setup(scope);
  const schemas = structuredClone(fixture.hello.operations);
  await fixture.tools.refresh();
  const edit = fixture.definitions.get("ontography_flow_edit")!;
  assert.equal(Check(edit.parameters, { document: {} }), true);
  assert.equal(Check(edit.parameters, {}), false);
  assert.equal(Check(edit.parameters, { run_id: 42, document: {} }), false);
  assert.deepEqual(fixture.hello.operations, schemas);
  fixture.client.call = async (operation, args, options) => {
    assert.equal(operation, "flow.edit");
    assert.deepEqual(args, { document: { name: "workflow" } });
    assert.equal(fixture.entries[0]!.type, "ontography_request");
    assert.equal((fixture.entries[0]!.data as { app_session_id: string }).app_session_id, scope.sessionId);
    return { result: { revision: "18446744073709551615" }, receipt: { client_id: fixture.client.clientId, request_id: options!.requestId!, operation } };
  };
  const result = await edit.execute("edit", { document: { name: "workflow" } }, undefined, undefined, {} as never);
  assert.match((result.content[0] as { text: string }).text, /18446744073709551615/);
  assert.equal(ready, 1);
});

test("operation receipts are retained before dispatch and errors throw for Pi", async () => {
  const fixture = setup();
  await fixture.tools.refresh();
  fixture.client.call = async (operation, _args, options) => {
    assert.equal(fixture.entries.length, 1);
    assert.equal(fixture.entries[0]!.type, "ontography_request");
    throw new BridgeError("invalid_document", "Unknown step", { field: "edges" }, { client_id: fixture.client.clientId, request_id: options!.requestId!, operation });
  };
  await assert.rejects(fixture.definitions.get("ontography_flow_edit")!.execute("call", { run_id: "run", document: {} }, undefined, undefined, {} as never), /invalid_document: Unknown step.*Request:.*Details:/);
});

test("only session inspection and outcome recovery accompany the document tools", async () => {
  const fixture = setup();
  for (const name of ["session.context", "session.inspect", "operation.get", "session.preferences", "invocation.begin"]) {
    fixture.hello.operations.push({ name, group: name.split(".")[0]!, description: name, mutating: false, parameters: { type: "object", properties: {} } });
  }
  await fixture.tools.refresh();
  for (const name of ["ontography_session_context", "ontography_session_inspect", "ontography_operation_get"]) {
    assert.ok(fixture.active().includes(name));
  }
  assert.equal(fixture.definitions.has("ontography_session_preferences"), false);
  assert.equal(fixture.definitions.has("ontography_invocation_begin"), false);
  assert.equal(fixture.definitions.has("ontography_tools"), false);
});

test("ambiguous document operation names reject before any registration", async () => {
  const fixture = setup();
  fixture.hello.operations.push({ ...fixture.hello.operations[0]!, name: "flow.edit.preview" });
  fixture.hello.operations.push({ ...fixture.hello.operations[0]!, name: "flow.edit_preview" });
  await assert.rejects(fixture.tools.refresh(), /collides/);
  assert.equal(fixture.definitions.size, 0);
});

test("large results have a bounded model preview and complete structured details", () => {
  const result = { data: "x".repeat(50_000) };
  const rendered = modelResult(result);
  assert.ok(rendered.content[0]!.text.length < 21_000);
  assert.match(rendered.content[0]!.text, /Preview truncated/);
  assert.deepEqual(rendered.details.result, result);
});
