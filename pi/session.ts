import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { BridgeClient, BridgeError, object, type Arguments } from "./client.ts";
import { ManagementTools, type ManagementScope } from "./tools.ts";
import { applyTerminalCapabilities, registerTerminalPresentation } from "./terminal.ts";

interface ConversationReference {
  conversation_id: string;
  path: string;
}

export interface SessionContext {
  session: Arguments & {
    session_id: string;
    run_id: string | null;
    pi: Arguments & { active_conversation_id: string; preferences: Arguments };
  };
  conversations_dir: string;
  graph: unknown;
}

function parseContext(value: unknown, sessionId: string): SessionContext {
  if (!object(value) || !object(value.session) || value.session.session_id !== sessionId ||
      (value.session.run_id !== null && typeof value.session.run_id !== "string") ||
      !object(value.session.pi) || typeof value.session.pi.active_conversation_id !== "string" ||
      !object(value.session.pi.preferences) || typeof value.conversations_dir !== "string" ||
      (value.graph !== null && !object(value.graph))) {
    throw new BridgeError("session_binding", "The server returned an invalid Ontography session association.");
  }
  const groups = value.session.pi.preferences.tool_groups;
  if (groups !== undefined && (!Array.isArray(groups) || groups.some((group) => typeof group !== "string"))) {
    throw new BridgeError("session_binding", "The session's saved tool groups are invalid.");
  }
  return value as unknown as SessionContext;
}

/** Durable ownership lives in Rust; this object only proves the active Pi runtime has registered. */
export class SessionBinding implements ManagementScope {
  readonly sessionId: string;
  private readonly client: BridgeClient;
  private active: ConversationReference | undefined;
  private readyServer: string | undefined;
  private context: SessionContext | undefined;
  private registering: Promise<SessionContext> | undefined;
  private disposed = false;

  private assertLive(): void {
    if (this.disposed) throw new BridgeError("session_replaced", "This Pi conversation runtime has ended. Use the current manager's tools.");
  }

  constructor(client: BridgeClient, sessionId: string) {
    if (client.appSessionId !== sessionId) throw new Error("The bridge and Pi manager must use the same Ontography session identity.");
    this.client = client;
    this.sessionId = sessionId;
  }

  async activate(ctx: Pick<ExtensionContext, "sessionManager">): Promise<SessionContext> {
    this.assertLive();
    this.readyServer = undefined;
    this.context = undefined;
    this.active = undefined;
    const path = ctx.sessionManager.getSessionFile();
    if (!path) throw new BridgeError("session_binding", "Ontography requires a persistent Pi conversation.");
    this.active = { conversation_id: ctx.sessionManager.getSessionId(), path };
    return this.register();
  }

  private async register(): Promise<SessionContext> {
    this.assertLive();
    if (this.registering !== undefined) return this.registering;
    const active = this.active;
    if (active === undefined) throw new BridgeError("session_binding", "Pi has not registered its conversation with Ontography.");
    this.registering = (async () => {
      this.readyServer = undefined;
      const hello = await this.client.connect();
      this.assertLive();
      await this.client.call("session.conversation", { session_id: this.sessionId, action: "activate", ...active });
      this.assertLive();
      const context = await this.readContext();
      this.assertLive();
      if (context.session.pi.active_conversation_id !== active.conversation_id) {
        throw new BridgeError("session_binding", "The server did not retain this manager's active conversation.");
      }
      this.context = context;
      this.readyServer = hello.server_id;
      return context;
    })();
    try { return await this.registering; }
    finally { this.registering = undefined; }
  }

  async ensureReady(): Promise<void> {
    this.assertLive();
    const hello = await this.client.connect();
    this.assertLive();
    if (this.readyServer !== hello.server_id) await this.register();
  }

  private async readContext(): Promise<SessionContext> {
    return parseContext((await this.client.call("session.context", { session_id: this.sessionId })).result, this.sessionId);
  }

  async refreshContext(): Promise<SessionContext> {
    try {
      await this.ensureReady();
      const context = await this.readContext();
      this.assertLive();
      if (context.session.pi.active_conversation_id !== this.active?.conversation_id) {
        throw new BridgeError("session_binding", "The active management conversation changed. Reattach the owning manager before using graph tools.");
      }
      this.context = context;
      return context;
    } catch (error) {
      this.readyServer = undefined;
      throw error;
    }
  }

  async checkConversation(path: string): Promise<void> {
    await this.ensureReady();
    const result = (await this.client.call("session.conversation", { session_id: this.sessionId, action: "check", path })).result;
    if (!object(result) || result.allowed !== true) throw new BridgeError("session_binding", "This conversation does not belong to the current Ontography session. Import it explicitly before resuming.");
  }

  async reportSavedConversation(ctx: Pick<ExtensionContext, "sessionManager">): Promise<void> {
    this.assertLive();
    if (this.active?.conversation_id !== ctx.sessionManager.getSessionId() || this.active.path !== ctx.sessionManager.getSessionFile()) {
      this.readyServer = undefined;
      throw new BridgeError("session_binding", "The completed turn belongs to another conversation runtime.");
    }
    // Pi materializes a new history only after an assistant message is saved.
    // Registration lets Rust verify that file and durably remember its existence.
    await this.register();
  }

  disconnect(): void {
    this.disposed = true;
    this.readyServer = undefined;
    this.context = undefined;
    this.active = undefined;
    this.client.disconnect();
  }
}

function notify(ctx: Pick<ExtensionContext, "hasUI" | "ui">, error: unknown): void {
  if (ctx.hasUI) ctx.ui.notify(`Ontography session unavailable: ${String(error)}. Management tools remain disabled until reconnection succeeds.`, "error");
}

function graphContext(context: SessionContext): string {
  const graph = JSON.stringify(context.graph);
  const preview = graph.length <= 12_000 ? graph : `${graph.slice(0, 12_000)}\n[Graph preview truncated; use bounded inspection tools.]`;
  return `\n\nCurrent Ontography session: ${context.session.session_id}. ` +
    (context.session.run_id === null ? "Graph initialization is pending. Starting a run binds it to this session." : `Bound graph run: ${context.session.run_id}.`) +
    " Pi /new, /resume, /fork, /clone, and /tree change conversation context within this same app session. They do not create, copy, or rewind the graph. " +
    "Graph tools default to the bound run; explicit targets must match it. Current server state supersedes older conversation descriptions. " +
    `Do not replay past mutations when resuming.\nCurrent graph inspection: ${preview}`;
}

export function registerSessionHooks(pi: ExtensionAPI, client: BridgeClient, tools: ManagementTools, binding: SessionBinding): void {
  pi.on("session_start", async (_event, ctx) => {
    try {
      registerTerminalPresentation(ctx);
      await binding.activate(ctx);
      await tools.refresh();
      if (ctx.hasUI) ctx.ui.setStatus("ontography", `Ontography · ${binding.sessionId.slice(0, 8)}`);
    } catch (error) { notify(ctx, error); }
  });

  pi.on("session_before_switch", async (event, ctx) => {
    try {
      await binding.ensureReady();
      if (event.reason === "resume") {
        if (!event.targetSessionFile) throw new Error("Pi did not identify the destination conversation.");
        await binding.checkConversation(event.targetSessionFile);
      }
    } catch (error) { notify(ctx, error); return { cancel: true }; }
  });

  pi.on("session_before_fork", async (_event, ctx) => {
    try { await binding.ensureReady(); }
    catch (error) { notify(ctx, error); return { cancel: true }; }
  });

  pi.on("before_agent_start", async (event, ctx) => {
    applyTerminalCapabilities();
    try {
      const context = await binding.refreshContext();
      await tools.refresh();
      return { systemPrompt: event.systemPrompt + graphContext(context) };
    }
    catch (error) {
      notify(ctx, error);
      return { systemPrompt: `${event.systemPrompt}\n\nOntography session context is unavailable. Do not infer a current run from conversation history. Reattach the session if reconnection continues to fail.` };
    }
  });

  pi.on("session_tree", async (_event, ctx) => {
    try { await binding.refreshContext(); }
    catch (error) { notify(ctx, error); }
  });

  pi.on("agent_end", async (_event, ctx) => {
    // message_end extensions run before Pi writes the message. agent_end runs
    // after persistence, including the first assistant response's history file.
    try { await binding.reportSavedConversation(ctx); }
    catch (error) { notify(ctx, error); }
  });

  pi.registerCommand("graph", {
    description: "Show this Ontography session's graph; return to the same Pi terminal when closed.",
    handler: async (_args, ctx) => {
      try {
        await binding.ensureReady();
        await client.call("terminal.graph", { session_id: binding.sessionId });
      } catch (error) { notify(ctx, error); }
    },
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    // Pi also emits this for /new, /resume, /fork, and /reload. Rust owns app
    // lifecycle; this hook only releases the old extension's connection.
    binding.disconnect();
    if (ctx.hasUI) ctx.ui.setStatus("ontography", undefined);
  });
}
