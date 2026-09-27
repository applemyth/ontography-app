import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { TSchema } from "typebox";
import { BridgeClient, BridgeError, object, type Arguments, type CallResult, type Hello, type Receipt } from "./client.ts";

const MAX_MODEL_RESULT = 20_000;

export interface ManagementScope {
  sessionId: string;
  ensureReady(): Promise<void>;
}

/** The manager has one workflow language; native hooks own lifecycle transport. */
function visibleToManager(operation: string): boolean {
  return operation.startsWith("flow.") || operation === "session.context" ||
    operation === "session.inspect" || operation === "operation.get";
}

function scopedParameters(parameters: Arguments): TSchema {
  const result = structuredClone(parameters);
  if (object(result.properties) && object(result.properties.run_id)) {
    result.properties.run_id.description = "Defaults to this Ontography session's graph run. An explicit run ID must match that run.";
  }
  if (Array.isArray(result.required)) result.required = result.required.filter((key) => !["run_id", "project", "session_id"].includes(String(key)));
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

  constructor(pi: ExtensionAPI, client: BridgeClient, scope?: ManagementScope) {
    this.pi = pi;
    this.client = client;
    this.scope = scope;
  }

  async refresh(): Promise<Hello> {
    const connected = await this.client.connect();
    const hello = { ...connected, operations: connected.operations.filter((operation) => visibleToManager(operation.name)) };
    const names = new Set<string>();
    for (const operation of hello.operations) {
      const name = toolName(operation.name);
      if (names.has(name)) throw new BridgeError("protocol_error", `Operation ${operation.name} collides with another Pi tool name.`);
      names.add(name);
    }
    const nativeSelection = this.pi.getActiveTools().filter((name) => !name.startsWith("ontography_"));
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
    this.activate(nativeSelection);
    return hello;
  }

  private activate(nativeSelection?: string[]): void {
    const native = nativeSelection ?? this.pi.getActiveTools().filter((name) => !name.startsWith("ontography_"));
    this.pi.setActiveTools([...new Set([
      ...native,
      ...(this.hello?.operations.map((operation) => toolName(operation.name)) ?? []),
    ])]);
  }
}
