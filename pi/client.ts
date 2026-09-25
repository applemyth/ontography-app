import { randomUUID } from "node:crypto";
import { createConnection, type Socket } from "node:net";

export const PROTOCOL_VERSION = 1;
export const SUPPORTED_PI_VERSION = "0.85.1";
export type Arguments = Record<string, unknown>;

export interface Operation {
  name: string;
  group: string;
  description: string;
  parameters: Arguments;
  mutating: boolean;
}

export interface Hello {
  protocol_version: number;
  server_id: string;
  app_version: string;
  core_version: string;
  app_build: string;
  core_build: string;
  operations: Operation[];
}

export interface Receipt {
  client_id: string;
  request_id: string;
  server_id?: string;
  operation: string;
}

export interface CallResult {
  result: unknown;
  receipt: Receipt;
}

export class BridgeError extends Error {
  readonly code: string;
  readonly details: unknown;
  readonly receipt: Receipt | undefined;

  constructor(code: string, message: string, details?: unknown, receipt?: Receipt) {
    super(message);
    this.name = "BridgeError";
    this.code = code;
    this.details = details;
    this.receipt = receipt;
  }

  override toString(): string {
    const reference = this.receipt === undefined ? "" : ` Request: ${JSON.stringify(this.receipt)}.`;
    return `${this.code}: ${this.message}${reference}`;
  }
}

interface Pending {
  receipt: Receipt;
  mutating: boolean;
  resolve: (value: CallResult) => void;
  reject: (error: BridgeError) => void;
  cleanup: () => void;
}

export interface ClientOptions {
  socketPath: string;
  clientId?: string;
  timeoutMs?: number;
  maxFrameBytes?: number;
  expectedAppBuild?: string;
  expectedCoreBuild?: string;
}

export function object(value: unknown): value is Arguments {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validateIntegers(value: unknown): void {
  if (typeof value === "number" && Number.isInteger(value) && !Number.isSafeInteger(value)) {
    throw new BridgeError("protocol_error", "An integer exceeds JavaScript's exact range; encode it as a decimal string.");
  }
  if (Array.isArray(value)) for (const item of value) validateIntegers(item);
  else if (object(value)) for (const item of Object.values(value)) validateIntegers(item);
}

export function parseHello(value: unknown): Hello {
  if (!object(value) || value.protocol_version !== PROTOCOL_VERSION ||
      typeof value.server_id !== "string" || typeof value.app_version !== "string" ||
      typeof value.core_version !== "string" || typeof value.app_build !== "string" ||
      typeof value.core_build !== "string" || !Array.isArray(value.operations)) {
    throw new BridgeError("protocol_error", "The server returned an incompatible capability handshake.");
  }
  const names = new Set<string>();
  const operations = value.operations.map((item: unknown): Operation => {
    if (!object(item) || typeof item.name !== "string" || !/^[a-z][a-z0-9_.]*$/.test(item.name) ||
        typeof item.group !== "string" || typeof item.description !== "string" ||
        !object(item.parameters) || item.parameters.type !== "object" || typeof item.mutating !== "boolean" ||
        names.has(item.name)) {
      throw new BridgeError("protocol_error", "The server returned an invalid or duplicate operation schema.");
    }
    names.add(item.name);
    return { name: item.name, group: item.group, description: item.description, parameters: item.parameters, mutating: item.mutating };
  });
  return { protocol_version: PROTOCOL_VERSION, server_id: value.server_id, app_version: value.app_version, core_version: value.core_version, app_build: value.app_build, core_build: value.core_build, operations };
}

/** Owns a connection, never the server or graph runs. No request is automatically replayed. */
export class BridgeClient {
  readonly clientId: string;
  private readonly options: ClientOptions & Required<Pick<ClientOptions, "clientId" | "timeoutMs" | "maxFrameBytes">>;
  private socket: Socket | undefined;
  private connecting: Promise<Hello> | undefined;
  private hello: Hello | undefined;
  private readonly pending = new Map<string, Pending>();

  constructor(options: ClientOptions) {
    this.clientId = options.clientId ?? randomUUID();
    this.options = { ...options, clientId: this.clientId, timeoutMs: options.timeoutMs ?? 30_000, maxFrameBytes: options.maxFrameBytes ?? 4_194_304 };
  }

  get serverId(): string | undefined { return this.hello?.server_id; }

  async connect(): Promise<Hello> {
    if (this.hello !== undefined && this.socket !== undefined && !this.socket.destroyed) return this.hello;
    if (this.connecting !== undefined) return this.connecting;
    this.connecting = this.open();
    try { return await this.connecting; }
    finally { this.connecting = undefined; }
  }

  private async open(): Promise<Hello> {
    const socket = createConnection(this.options.socketPath);
    this.socket = socket;
    this.hello = undefined;
    let buffer = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      try {
        buffer = Buffer.concat([buffer, chunk]);
        let end: number;
        while ((end = buffer.indexOf(10)) !== -1) {
          if (end + 1 > this.options.maxFrameBytes) throw new BridgeError("protocol_error", "Server response exceeds the frame limit.");
          const line = buffer.subarray(0, end).toString("utf8");
          buffer = buffer.subarray(end + 1);
          this.receive(JSON.parse(line));
        }
        if (buffer.length > this.options.maxFrameBytes) throw new BridgeError("protocol_error", "Server response exceeds the frame limit.");
      } catch (error) {
        this.fail(socket, error instanceof BridgeError ? error : new BridgeError("protocol_error", `Invalid server response: ${String(error)}`));
        socket.destroy();
      }
    });
    socket.on("error", (error) => this.fail(socket, new BridgeError("disconnected", error.message)));
    socket.on("close", () => this.fail(socket, new BridgeError("disconnected", "The server connection closed.")));
    try {
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => { socket.destroy(); reject(new BridgeError("connection_timeout", "Timed out connecting to the Ontography server.")); }, Math.min(this.options.timeoutMs, 5_000));
        const clear = () => clearTimeout(timeout);
        socket.once("connect", () => { clear(); resolve(); });
        socket.once("error", (error) => { clear(); reject(new BridgeError("disconnected", error.message)); });
        socket.once("close", () => { clear(); reject(new BridgeError("disconnected", "Connection closed before readiness.")); });
      });
      const response = await this.send("system.hello", {}, false);
      const hello = parseHello(response.result);
      if (hello.server_id !== response.receipt.server_id) throw new BridgeError("protocol_error", "Handshake server identity does not match its response.");
      if ((this.options.expectedAppBuild !== undefined && hello.app_build !== this.options.expectedAppBuild) ||
          (this.options.expectedCoreBuild !== undefined && hello.core_build !== this.options.expectedCoreBuild)) {
        throw new BridgeError("incompatible_server", "The server uses a different app/core build. Reconnect using its matching Ontography client.",
          { app_build: hello.app_build, core_build: hello.core_build, expected_app_build: this.options.expectedAppBuild, expected_core_build: this.options.expectedCoreBuild });
      }
      if (this.socket !== socket || socket.destroyed) throw new BridgeError("disconnected", "Connection closed during the handshake.");
      this.hello = hello;
      return hello;
    } catch (error) {
      this.fail(socket, error instanceof BridgeError ? error : new BridgeError("disconnected", String(error)));
      socket.destroy();
      throw error;
    }
  }

  async call(operation: string, args: Arguments, options: { signal?: AbortSignal; requestId?: string } = {}): Promise<CallResult> {
    if (options.signal?.aborted) throw new BridgeError("cancelled", "Cancelled before sending the request.");
    const hello = await this.connect();
    const descriptor = hello.operations.find((entry) => entry.name === operation);
    if (descriptor === undefined && operation !== "system.hello") throw new BridgeError("unavailable_capability", `The server does not expose ${operation}.`);
    if (options.signal?.aborted) throw new BridgeError("cancelled", "Cancelled before sending the request.");
    return this.send(operation, args, descriptor?.mutating ?? false, options);
  }

  /** Query the server's retained outcome; it never resends the original operation. */
  async outcome(receipt: Receipt): Promise<CallResult> {
    const hello = await this.connect();
    if (receipt.server_id !== undefined && receipt.server_id !== hello.server_id) {
      throw new BridgeError("server_restarted", "The original server instance ended. Reconcile against current run state; do not replay the mutation.", undefined, receipt);
    }
    return this.call("operation.get", { client_id: receipt.client_id, request_id: receipt.request_id });
  }

  private send(operation: string, args: Arguments, mutating: boolean, options: { signal?: AbortSignal; requestId?: string } = {}): Promise<CallResult> {
    const socket = this.socket;
    if (socket === undefined || socket.destroyed) return Promise.reject(new BridgeError("disconnected", "No live server connection."));
    const receipt: Receipt = { client_id: this.clientId, request_id: options.requestId ?? randomUUID(), operation, ...(this.serverId === undefined ? {} : { server_id: this.serverId }) };
    if (this.pending.has(receipt.request_id)) return Promise.reject(new BridgeError("duplicate_request", "A request with this ID is already pending.", undefined, receipt));
    try { validateIntegers(args); }
    catch (error) { return Promise.reject(error); }
    const frame = `${JSON.stringify({
      version: PROTOCOL_VERSION, client_id: this.clientId, request_id: receipt.request_id, operation, args,
      ...(operation === "system.hello" ? {} : { expected_server_id: this.serverId }),
    })}\n`;
    if (Buffer.byteLength(frame) > this.options.maxFrameBytes) return Promise.reject(new BridgeError("request_too_large", "Request exceeds the frame limit; use bounded content operations.", undefined, receipt));
    return new Promise<CallResult>((resolve, reject) => {
      const stopWaiting = (code: string, message: string) => {
        const pending = this.pending.get(receipt.request_id);
        if (pending === undefined) return;
        this.pending.delete(receipt.request_id);
        pending.cleanup();
        reject(new BridgeError(code, `${message} The server operation may continue; query operation.get with this receipt before retrying.`, undefined, receipt));
      };
      const abort = () => stopWaiting("waiting_cancelled", "Stopped waiting for the request.");
      const timeout = setTimeout(() => stopWaiting("unknown_outcome", "Timed out awaiting the operation result."), this.options.timeoutMs);
      const cleanup = () => { clearTimeout(timeout); options.signal?.removeEventListener("abort", abort); };
      this.pending.set(receipt.request_id, { receipt, mutating, resolve, reject, cleanup });
      options.signal?.addEventListener("abort", abort, { once: true });
      socket.write(frame, (error) => { if (error !== null && error !== undefined) this.fail(socket, new BridgeError("disconnected", error.message)); });
    });
  }

  private receive(value: unknown): void {
    validateIntegers(value);
    if (!object(value) || value.version !== PROTOCOL_VERSION || typeof value.server_id !== "string" || typeof value.request_id !== "string" ||
        (value.status !== "ok" && value.status !== "error")) throw new BridgeError("protocol_error", "Malformed server response envelope.");
    if (this.serverId !== undefined && this.serverId !== value.server_id) throw new BridgeError("protocol_error", "Server identity changed within a connection.");
    const pending = this.pending.get(value.request_id);
    // A cancelled waiter may still receive the result of its server-owned operation.
    if (pending === undefined) return;
    if (value.status === "error" && (!object(value.error) || typeof value.error.code !== "string" || typeof value.error.message !== "string")) {
      throw new BridgeError("protocol_error", "Malformed server error.");
    }
    this.pending.delete(value.request_id);
    pending.cleanup();
    const receipt = { ...pending.receipt, server_id: value.server_id };
    if (value.status === "ok") pending.resolve({ receipt, result: value.result });
    else {
      const error = value.error as { code: string; message: string; details?: unknown };
      pending.reject(new BridgeError(error.code, error.message, error.details, receipt));
    }
  }

  private fail(socket: Socket, error: BridgeError): void {
    if (this.socket !== socket) return;
    this.socket = undefined;
    this.hello = undefined;
    for (const pending of this.pending.values()) {
      pending.cleanup();
      pending.reject(new BridgeError(pending.mutating ? "unknown_outcome" : error.code,
        pending.mutating ? `${error.message} The mutation may have committed. Inspect operation.get before retrying.` : error.message,
        error.details, pending.receipt));
    }
    this.pending.clear();
  }

  disconnect(): void {
    const socket = this.socket;
    if (socket !== undefined) {
      this.fail(socket, new BridgeError("disconnected", "The management client disconnected."));
      socket.destroy();
    }
  }
}
