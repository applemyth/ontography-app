import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type, type TSchema } from "typebox";
import { BridgeClient, BridgeError, object, type Arguments, type CallResult, type Hello, type Receipt } from "./client.ts";

export const DEFAULT_GROUPS = ["system", "catalog", "graph", "run", "rewrite", "operation"];
export const BOOTSTRAP_TOOL = "ontography_tools";
const MAX_MODEL_RESULT = 20_000;

export interface ManagementScope {
  sessionId: string;
  ensureReady(): Promise<void>;
  saveGroups(groups: string[]): Promise<void>;
  toolGroups?(): string[];
}

/** Native lifecycle/view operations are invoked by hooks, never by the model. */
function visibleInSession(operation: string): boolean {
  if (operation.startsWith("terminal.") || operation.startsWith("server.")) return false;
  if (operation.startsWith("session.")) return operation === "session.context" || operation === "session.inspect";
  return true;
}

function scopedParameters(parameters: Arguments): TSchema {
  const result = structuredClone(parameters);
  if (object(result.properties) && object(result.properties.run_id)) {
    result.properties.run_id.description = "Defaults to this Ontography session's graph run. An explicit run ID must match that run.";
    if (Array.isArray(result.required)) result.required = result.required.filter((key) => key !== "run_id");
  }
  return result as unknown as TSchema;
}

export function toolName(operation: string): string {
  return `ontography_${operation.replaceAll(".", "_")}`;
}

export function modelResult(result: unknown, receipt?: Receipt) {
  const text = JSON.stringify({ ...(receipt === undefined ? {} : { receipt }), result }, null, 2);
  return {
    content: [{ type: "text" as const, text: text.length <= MAX_MODEL_RESULT ? text :
      `${text.slice(0, MAX_MODEL_RESULT)}\n[Preview truncated. Use bounded inspect/read operations and explicit IDs to request the needed portion.]` }],
    details: { result, ...(receipt === undefined ? {} : { receipt }) },
  };
}

function toolError(error: unknown): Error {
  if (error instanceof BridgeError) {
    const details = error.details === undefined ? "" : ` Details: ${JSON.stringify(error.details).slice(0, 4_000)}`;
    return new Error(`${error.toString()}${details}`);
  }
  return error instanceof Error ? error : new Error(String(error));
}

/** Registers actual server schemas as individual native Pi tools. */
export class ManagementTools {
  private readonly pi: ExtensionAPI;
  private readonly client: BridgeClient;
  private readonly scope: ManagementScope | undefined;
  private hello: Hello | undefined;
  private groups = new Set(DEFAULT_GROUPS);
  private registered = new Set<string>();

  constructor(pi: ExtensionAPI, client: BridgeClient, scope?: ManagementScope) {
    this.pi = pi;
    this.client = client;
    this.scope = scope;
    if (scope !== undefined) this.groups.add("session");
  }

  registerBootstrap(): void {
    this.pi.registerTool({
      name: BOOTSTRAP_TOOL,
      label: "Ontography capabilities",
      description: "List Ontography server capabilities and activate their typed tool groups. Call without groups to inspect; supply groups to make their tools available. Does not execute graph operations.",
      promptSnippet: "Discover graph/run, vocabulary extension, workflow retirement, inspection, content, and context tools; activate their groups when needed.",
      parameters: Type.Object({ groups: Type.Optional(Type.Array(Type.String(), { description: "Capability groups to activate, added to the current selection." })) }, { additionalProperties: false }),
      executionMode: "sequential",
      execute: async (_callId, args) => {
        try {
          await this.scope?.ensureReady();
          await this.refresh(this.scope?.toolGroups?.());
          if (args.groups !== undefined) {
            const available = new Set(this.hello!.operations.map((operation) => operation.group));
            for (const group of args.groups) if (!available.has(group)) throw new Error(`Unknown capability group ${JSON.stringify(group)}. Available: ${[...available].sort().join(", ")}`);
            const groups = [...new Set([...this.groups, ...args.groups])];
            await this.scope?.saveGroups(groups);
            this.groups = new Set(groups);
            this.activate();
            // Legacy clients retain conversation-local preferences. Bound managers use
            // app-session preferences so /new does not reset their tool selection.
            if (this.scope === undefined) this.pi.appendEntry("ontography_tool_groups", { groups: [...this.groups] });
          }
          return modelResult({
            server_id: this.hello!.server_id,
            groups: [...new Set(this.hello!.operations.map((operation) => operation.group))].sort().map((group) => ({
              name: group,
              active: this.groups.has(group),
              tools: this.hello!.operations.filter((operation) => operation.group === group).map((operation) => ({ name: toolName(operation.name), operation: operation.name, description: operation.description })),
            })),
          });
        } catch (error) { throw toolError(error); }
      },
    });
  }

  async refresh(groups?: string[]): Promise<Hello> {
    const connected = await this.client.connect();
    const hello = this.scope === undefined ? connected : { ...connected, operations: connected.operations.filter((operation) => visibleInSession(operation.name)) };
    if (groups !== undefined) this.groups = new Set([...DEFAULT_GROUPS, ...(this.scope === undefined ? [] : ["session"]), ...groups]);
    const names = new Set<string>([BOOTSTRAP_TOOL]);
    for (const operation of hello.operations) {
      const name = toolName(operation.name);
      if (names.has(name)) throw new BridgeError("protocol_error", `Operation ${operation.name} collides with another Pi tool name.`);
      names.add(name);
    }
    const nativeSelection = this.pi.getActiveTools().filter((name) => !this.registered.has(name) && name !== BOOTSTRAP_TOOL);
    for (const operation of hello.operations) {
      const name = toolName(operation.name);
      this.pi.registerTool({
        name,
        label: `Ontography · ${operation.name}`,
        description: operation.description,
        promptSnippet: operation.description,
        parameters: this.scope === undefined ? operation.parameters as unknown as TSchema : scopedParameters(operation.parameters),
        executionMode: operation.mutating ? "sequential" : "parallel",
        execute: async (_callId, args: unknown, signal) => {
          if (!object(args)) throw new Error("Tool arguments must be a JSON object.");
          try {
            await this.scope?.ensureReady();
            const current = await this.client.connect();
            const requestId = randomUUID();
            const receipt: Receipt = { client_id: this.client.clientId, request_id: requestId, server_id: current.server_id, operation: operation.name,
              ...(this.scope === undefined ? {} : { app_session_id: this.scope.sessionId }) };
            // Retain the request identity before dispatch so a lost response can be inspected.
            this.pi.appendEntry("ontography_request", receipt);
            if (this.scope !== undefined && operation.name.startsWith("session.") && args.session_id !== undefined && args.session_id !== this.scope.sessionId) {
              throw new BridgeError("session_binding", "This manager cannot target another Ontography session.");
            }
            const arguments_ = this.scope !== undefined && operation.name.startsWith("session.") ? { ...args, session_id: this.scope.sessionId } : args;
            const response: CallResult = await this.client.call(operation.name, arguments_ as Arguments, {
              requestId, ...(signal === undefined ? {} : { signal }),
            });
            return modelResult(response.result, response.receipt);
          } catch (error) { throw toolError(error); }
        },
      });
    }
    this.hello = hello;
    this.registered = new Set([...this.registered, ...names]);
    this.activate(nativeSelection);
    return hello;
  }

  private activate(nativeSelection?: string[]): void {
    const native = nativeSelection ?? this.pi.getActiveTools().filter((name) => !this.registered.has(name));
    this.pi.setActiveTools([...new Set([
      ...native, BOOTSTRAP_TOOL,
      ...(this.hello?.operations.filter((operation) => this.groups.has(operation.group)).map((operation) => toolName(operation.name)) ?? []),
    ])]);
  }
}
