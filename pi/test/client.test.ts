import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { BridgeClient, BridgeError, parseHello, type Receipt } from "../client.ts";

interface Request { version: number; client_id: string; request_id: string; expected_server_id?: string; operation: string; args: Record<string, unknown> }
const operations = [
  { name: "inspect.echo", group: "inspect", description: "Return input", parameters: { type: "object" }, mutating: false },
  { name: "graph.create", group: "graph", description: "Create graph", parameters: { type: "object" }, mutating: true },
  { name: "operation.get", group: "operation", description: "Find original outcome", parameters: { type: "object" }, mutating: false },
];

async function fixture(t: TestContext, handle: (request: Request, socket: Socket, reply: (result: unknown) => void) => void,
  options: { maxFrameBytes?: number; timeoutMs?: number; expectedAppBuild?: string; expectedCoreBuild?: string } = {}) {
  const directory = await mkdtemp("/private/tmp/onto-pi-");
  const socketPath = join(directory, "s");
  const sockets = new Set<Socket>();
  const requests: Request[] = [];
  let connections = 0;
  const server = createServer((socket) => {
    connections++;
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
    socket.setEncoding("utf8");
    let input = "";
    socket.on("data", (chunk: string) => {
      input += chunk;
      let end: number;
      while ((end = input.indexOf("\n")) !== -1) {
        const request = JSON.parse(input.slice(0, end)) as Request;
        input = input.slice(end + 1);
        requests.push(request);
        const reply = (result: unknown) => socket.write(`${JSON.stringify({ version: 1, server_id: "server-1", request_id: request.request_id, status: "ok", result })}\n`);
        if (request.operation === "system.hello") reply({ protocol_version: 1, server_id: "server-1", app_version: "test", core_version: "test", app_build: "app-build", core_build: "core-build", operations });
        else {
          assert.equal(request.expected_server_id, "server-1", "operations bind the server identity before dispatch");
          handle(request, socket, reply);
        }
      }
    });
  });
  const client = new BridgeClient({ socketPath, ...options });
  t.after(async () => {
    client.disconnect();
    for (const socket of sockets) socket.destroy();
    if (server.listening) await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(socketPath, resolve); });
  return { client, requests, connections: () => connections };
}

test("concurrent calls share a handshake and preserve wide decimal IDs and Unicode", async (t) => {
  const setup = await fixture(t, (request, _socket, reply) => reply(request.args));
  const values = ["18446744073709551615", "9007199254740993", "λ → 🌳"];
  const responses = await Promise.all(values.map((value) => setup.client.call("inspect.echo", { value })));
  assert.deepEqual(responses.map((response) => response.result), values.map((value) => ({ value })));
  assert.equal(setup.connections(), 1);
  assert.equal(setup.requests.filter((request) => request.operation === "system.hello").length, 1);
  assert.equal(new Set(responses.map((response) => response.receipt.request_id)).size, 3);
});

test("lost mutation response reconnects to its retained outcome without replay", async (t) => {
  let accepted: Request | undefined;
  const setup = await fixture(t, (request, socket, reply) => {
    if (request.operation === "graph.create") { accepted = request; socket.destroy(); }
    else {
      assert.equal(request.args.request_id, accepted?.request_id);
      assert.equal(request.args.client_id, accepted?.client_id);
      reply({ state: "complete", result: { graph_id: "graph-1" } });
    }
  });
  let receipt: Receipt | undefined;
  await assert.rejects(setup.client.call("graph.create", {}), (error: unknown) => {
    assert.ok(error instanceof BridgeError);
    assert.equal(error.code, "unknown_outcome");
    receipt = error.receipt;
    return true;
  });
  assert.ok(receipt);
  assert.deepEqual((await setup.client.outcome(receipt)).result, { state: "complete", result: { graph_id: "graph-1" } });
  assert.equal(setup.requests.filter((request) => request.operation === "graph.create").length, 1);
  assert.equal(setup.connections(), 2);
});

test("cancellation stops the waiter while the accepted server operation continues", async (t) => {
  let complete: ((result: unknown) => void) | undefined;
  let started!: () => void;
  const received = new Promise<void>((resolve) => { started = resolve; });
  const setup = await fixture(t, (request, _socket, reply) => {
    if (request.operation === "graph.create") { complete = reply; started(); }
    else reply({ alive: true });
  });
  const controller = new AbortController();
  const result = setup.client.call("graph.create", {}, { signal: controller.signal });
  await received;
  controller.abort();
  await assert.rejects(result, (error: unknown) => error instanceof BridgeError && error.code === "waiting_cancelled" && error.receipt !== undefined);
  complete!({ graph_id: "completed-after-cancel" });
  assert.deepEqual((await setup.client.call("inspect.echo", {})).result, { alive: true });
  assert.deepEqual(setup.requests.map((request) => request.operation), ["system.hello", "graph.create", "inspect.echo"]);
});

test("pre-aborted work is never sent", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({}));
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(setup.client.call("graph.create", {}, { signal: controller.signal }), (error: unknown) => error instanceof BridgeError && error.code === "cancelled");
  assert.equal(setup.connections(), 0);
});

test("core rejections preserve their code, details, and request receipt", async (t) => {
  const setup = await fixture(t, (request, socket) => socket.write(`${JSON.stringify({ version: 1, server_id: "server-1", request_id: request.request_id, status: "error", error: { code: "stale_plan", message: "Plan revision is stale", details: { current_revision: "18446744073709551615" } } })}\n`));
  await assert.rejects(setup.client.call("graph.create", {}), (error: unknown) => {
    assert.ok(error instanceof BridgeError);
    assert.equal(error.code, "stale_plan");
    assert.deepEqual(error.details, { current_revision: "18446744073709551615" });
    assert.equal(error.receipt?.operation, "graph.create");
    return true;
  });
});

test("unsafe numeric identifiers are rejected on both sides of the protocol", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({ id: 2 ** 60 }));
  await assert.rejects(setup.client.call("graph.create", { id: 2 ** 60 }), (error: unknown) => error instanceof BridgeError && error.code === "protocol_error");
  assert.equal(setup.requests.filter((request) => request.operation === "graph.create").length, 0);
  await assert.rejects(setup.client.call("inspect.echo", {}), (error: unknown) => error instanceof BridgeError && error.code === "protocol_error");
});

test("request limits reject before sending and response limits close the connection", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({ data: "x".repeat(10_000) }), { maxFrameBytes: 2_000 });
  await assert.rejects(setup.client.call("graph.create", { data: "x".repeat(10_000) }), (error: unknown) => error instanceof BridgeError && error.code === "request_too_large");
  await assert.rejects(setup.client.call("inspect.echo", {}), (error: unknown) => error instanceof BridgeError && error.code === "protocol_error");
});

test("unknown operations and incompatible schemas are explicit", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({}));
  await assert.rejects(setup.client.call("codex.launch", {}), (error: unknown) => error instanceof BridgeError && error.code === "unavailable_capability");
  assert.throws(() => parseHello({ protocol_version: 2 }), /incompatible capability handshake/);
  assert.throws(() => parseHello({ protocol_version: 1, server_id: "s", app_version: "1", core_version: "1", app_build: "app-build", core_build: "core-build", operations: [operations[0], operations[0]] }), /duplicate operation schema/);
});

test("an incompatible source build is rejected before sending a mutation", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({}), { expectedAppBuild: "different-build", expectedCoreBuild: "core-build" });
  await assert.rejects(setup.client.call("graph.create", {}), (error: unknown) => error instanceof BridgeError && error.code === "incompatible_server");
  assert.deepEqual(setup.requests.map((request) => request.operation), ["system.hello"]);
});

test("original receipts cannot be resolved against a different server instance", async (t) => {
  const setup = await fixture(t, (_request, _socket, reply) => reply({}));
  await assert.rejects(setup.client.outcome({ client_id: "client", request_id: "r", server_id: "previous-server", operation: "graph.create" }),
    (error: unknown) => error instanceof BridgeError && error.code === "server_restarted");
  assert.equal(setup.requests.length, 1);
});
