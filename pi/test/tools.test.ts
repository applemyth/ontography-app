import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Check } from "typebox/value";
import { BridgeClient, BridgeError, type Hello } from "../client.ts";
import { BOOTSTRAP_TOOL, ManagementTools, modelResult } from "../tools.ts";

function setup() {
  let active = ["read", "bash", "external_custom"];
  const definitions = new Map<string, ToolDefinition>();
  const entries: Array<{ type: string; data: unknown }> = [];
  const pi = {
    registerTool(tool: ToolDefinition) { definitions.set(tool.name, tool); },
    getActiveTools() { return active; },
    setActiveTools(names: string[]) { active = names; },
    appendEntry(type: string, data: unknown) { entries.push({ type, data }); },
  } as unknown as ExtensionAPI;
  const hello: Hello = {
    protocol_version: 1, server_id: "server", app_version: "test", core_version: "test", app_build: "app-build", core_build: "core-build",
    operations: [
      { name: "graph.create", group: "graph", description: "Create a graph", mutating: true,
        parameters: { type: "object", properties: { graph_id: { type: "string" } }, required: ["graph_id"], additionalProperties: false } },
      { name: "content.read", group: "content", description: "Read bounded bytes", mutating: false,
        parameters: { type: "object", properties: { content_id: { type: "string" } }, required: ["content_id"], additionalProperties: false } },
    ],
  };
  const client = new BridgeClient({ socketPath: "/unused-in-tool-unit-test" });
  client.connect = async () => hello;
  const tools = new ManagementTools(pi, client);
  tools.registerBootstrap();
  return { pi, client, tools, hello, definitions, entries, active: () => active };
}

test("server schemas become separate typed tools and native selections survive activation", async () => {
  const fixture = setup();
  await fixture.tools.refresh();
  assert.deepEqual(fixture.active(), ["read", "bash", "external_custom", BOOTSTRAP_TOOL, "ontography_graph_create"]);
  const graph = fixture.definitions.get("ontography_graph_create")!;
  assert.equal(graph.executionMode, "sequential");
  assert.equal(Check(graph.parameters, { graph_id: "g" }), true);
  assert.equal(Check(graph.parameters, { graph_id: 123 }), false);
  assert.equal(Check(graph.parameters, {}), false);
  const discovery = fixture.definitions.get(BOOTSTRAP_TOOL)!;
  await discovery.execute("test-call", { groups: ["content"] }, undefined, undefined, {} as never);
  assert.ok(fixture.active().includes("ontography_content_read"));
  assert.ok(fixture.active().includes("external_custom"));
  assert.equal(fixture.definitions.get("ontography_content_read")!.executionMode, "parallel");
});

test("operation requests are retained before dispatch and errors throw for Pi", async () => {
  const fixture = setup();
  await fixture.tools.refresh();
  fixture.client.call = async (operation, _args, options) => {
    assert.equal(fixture.entries.length, 1);
    assert.equal(fixture.entries[0]!.type, "ontography_request");
    assert.equal(operation, "graph.create");
    throw new BridgeError("core_rejection", "Invalid graph", { field: "edges" }, { client_id: fixture.client.clientId, request_id: options!.requestId!, operation });
  };
  await assert.rejects(fixture.definitions.get("ontography_graph_create")!.execute("call", { graph_id: "g" }, undefined, undefined, {} as never), /core_rejection: Invalid graph.*Request:.*Details:/);
});

test("ambiguous operation-to-tool names are rejected before registering operations", async () => {
  const fixture = setup();
  fixture.hello.operations.push({ ...fixture.hello.operations[0]!, name: "graph_create" });
  await assert.rejects(fixture.tools.refresh(), /collides/);
  assert.equal(fixture.definitions.size, 1);
});

test("large results have a bounded model preview and complete structured details", () => {
  const result = { data: "x".repeat(50_000) };
  const rendered = modelResult(result);
  assert.ok(rendered.content[0]!.text.length < 21_000);
  assert.match(rendered.content[0]!.text, /Preview truncated/);
  assert.deepEqual(rendered.details.result, result);
});
