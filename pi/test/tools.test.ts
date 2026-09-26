import assert from "node:assert/strict";
import { test } from "node:test";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Check } from "typebox/value";
import { BridgeClient, BridgeError, type Hello } from "../client.ts";
import { BOOTSTRAP_TOOL, ManagementTools, modelResult, type ManagementScope } from "../tools.ts";

function setup(scope?: ManagementScope) {
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
  const client = new BridgeClient({ socketPath: "/unused-in-tool-unit-test", ...(scope === undefined ? {} : { appSessionId: scope.sessionId }) });
  client.connect = async () => hello;
  const tools = new ManagementTools(pi, client, scope);
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

test("extension and retirement tools retain server schemas, scoped targets, activation groups, and exact records", async () => {
  let ready = 0;
  let savedGroups: string[] = [];
  const scope: ManagementScope = {
    sessionId: "owning-session",
    async ensureReady() { ready++; },
    async saveGroups(groups) { savedGroups = groups; },
  };
  const fixture = setup(scope);
  const text = { type: "string" };
  const extension = {
    type: "object", additionalProperties: false,
    properties: { node_types: { type: "array", items: text }, object_types: { type: "array", items: text }, authority_tags: { type: "array", items: text },
      contracts: { type: "array", items: {
        type: "object", additionalProperties: false,
        properties: { id: text, object_type: text, validator: { type: "string", enum: ["opaque_bytes", "utf8"] }, validator_version: { const: 1 } },
        required: ["id", "object_type", "validator", "validator_version"],
      } } },
  };
  fixture.hello.operations.push(
    { name: "run.extend", group: "run", description: "Extend vocabulary", mutating: true,
      parameters: { type: "object", properties: { run_id: text, extension }, required: ["run_id", "extension"], additionalProperties: false } },
    { name: "workflow.retire", group: "workflow", description: "Retire pending occurrence", mutating: true,
      parameters: { type: "object", properties: { run_id: text, package_id: text, evidence_activation_id: text }, required: ["run_id", "package_id"], additionalProperties: false } },
    { name: "inspect.retirements", group: "inspect", description: "Page retirement records", mutating: false,
      parameters: { type: "object", properties: { run_id: text, after: text, limit: { type: "integer", minimum: 1, maximum: 1000 } }, required: ["run_id"], additionalProperties: false } },
    { name: "inspect.package", group: "inspect", description: "Inspect package disposition", mutating: false,
      parameters: { type: "object", properties: { run_id: text, package_id: text }, required: ["run_id", "package_id"], additionalProperties: false } },
  );
  const originalSchemas = structuredClone(fixture.hello.operations);
  await fixture.tools.refresh();
  const extend = fixture.definitions.get("ontography_run_extend")!;
  const retire = fixture.definitions.get("ontography_workflow_retire")!;
  const history = fixture.definitions.get("ontography_inspect_retirements")!;
  assert.ok(fixture.active().includes(extend.name));
  assert.ok(!fixture.active().includes(retire.name));
  assert.ok(!fixture.active().includes(history.name));
  assert.equal(extend.executionMode, "sequential");
  assert.equal(retire.executionMode, "sequential");
  assert.equal(history.executionMode, "parallel");
  const additions = { object_types: ["Message"], contracts: [{ id: "message", object_type: "Message", validator: "utf8", validator_version: 1 }] };
  assert.equal(Check(extend.parameters, { extension: additions }), true);
  assert.equal(Check(extend.parameters, {}), false);
  assert.equal(Check(extend.parameters, { extension: { nodes: [] } }), false);
  assert.equal(Check(retire.parameters, { package_id: "ffffffff-ffff-ffff-ffff-ffffffffffff/00000000-0000-0000-0000-000000000001" }), true);
  assert.equal(Check(retire.parameters, {}), false);
  assert.equal(Check(history.parameters, { limit: 1001 }), false);
  assert.deepEqual(fixture.hello.operations, originalSchemas, "scoping must not mutate shared catalog schemas");

  await fixture.definitions.get(BOOTSTRAP_TOOL)!.execute("discover", { groups: ["workflow", "inspect"] }, undefined, undefined, {} as never);
  assert.ok(fixture.active().includes(retire.name) && fixture.active().includes(history.name));
  assert.ok(savedGroups.includes("workflow") && savedGroups.includes("inspect"));
  const evidence = "ffffffff-ffff-ffff-ffff-ffffffffffff";
  const packageId = `${evidence}/00000000-0000-0000-0000-000000000001`;
  const revision = "18446744073709551615";
  const retirement = { reason: "explicit", holder: "worker", phase: "outbound", revision, evidence_activation_id: evidence };
  const retirementRecord = { package_id: packageId, ...retirement };
  const requests: Array<{ operation: string; args: unknown }> = [];
  fixture.client.call = async (operation, args, options) => {
    requests.push({ operation, args });
    const receipt = { client_id: fixture.client.clientId, request_id: options!.requestId!, operation, app_session_id: scope.sessionId };
    const result = operation === "inspect.retirements" ? { revision, retirements: [retirementRecord], next_after: packageId }
      : operation === "inspect.package" ? { package: { package_id: packageId }, disposition: "retired", retirement }
      : operation === "run.extend" ? { revision } : { package_id: packageId, disposition: "retired", revision, retirement };
    return { result, receipt };
  };
  await extend.execute("extend", { extension: additions }, undefined, undefined, {} as never);
  await retire.execute("retire", { package_id: packageId, evidence_activation_id: evidence }, undefined, undefined, {} as never);
  const page = await history.execute("page", { after: packageId, limit: 1 }, undefined, undefined, {} as never);
  const detail = await fixture.definitions.get("ontography_inspect_package")!.execute("package", { package_id: packageId }, undefined, undefined, {} as never);
  assert.deepEqual(requests, [
    { operation: "run.extend", args: { extension: additions } },
    { operation: "workflow.retire", args: { package_id: packageId, evidence_activation_id: evidence } },
    { operation: "inspect.retirements", args: { after: packageId, limit: 1 } },
    { operation: "inspect.package", args: { package_id: packageId } },
  ], "server supplies the scoped run; Pi preserves exact identifiers and nested arguments");
  assert.equal(page.content[0]!.type, "text");
  assert.deepEqual(JSON.parse((page.content[0] as { text: string }).text).result, { revision, retirements: [retirementRecord], next_after: packageId });
  assert.deepEqual(JSON.parse((detail.content[0] as { text: string }).text).result, { package: { package_id: packageId }, disposition: "retired", retirement });
  assert.equal(ready, 5, "discovery and each mutation/inspection must check session readiness");
  assert.ok(fixture.entries.filter((entry) => entry.type === "ontography_request").every((entry) =>
    (entry.data as { app_session_id: string }).app_session_id === scope.sessionId));
});
