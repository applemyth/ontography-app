import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { BridgeClient } from "./client.ts";
import { ManagementTools } from "./tools.ts";
import { registerSessionHooks, SessionBinding } from "./session.ts";
import { applyTerminalCapabilities } from "./terminal.ts";

export default function ontography(pi: ExtensionAPI): void {
  applyTerminalCapabilities();
  const socketPath = process.env.ONTOGRAPHY_SOCKET;
  if (socketPath === undefined || socketPath.length === 0) throw new Error("Launch this extension with ontography; ONTOGRAPHY_SOCKET must identify its server.");
  const expectedAppBuild = process.env.ONTOGRAPHY_APP_BUILD;
  const expectedCoreBuild = process.env.ONTOGRAPHY_CORE_BUILD;
  if (!expectedAppBuild || !expectedCoreBuild) throw new Error("Launch this extension with ontography; its app/core build identities are required.");
  // This nonsecret identifier remains stable when Pi reloads the extension.
  process.env.ONTOGRAPHY_CLIENT_ID ??= randomUUID();
  const sessionId = process.env.ONTOGRAPHY_SESSION_ID;
  const client = new BridgeClient({ socketPath, clientId: process.env.ONTOGRAPHY_CLIENT_ID, expectedAppBuild, expectedCoreBuild,
    ...(sessionId === undefined ? {} : { appSessionId: sessionId }) });
  const binding = sessionId === undefined ? undefined : new SessionBinding(client, sessionId);
  const tools = new ManagementTools(pi, client, binding);

  if (binding !== undefined) {
    registerSessionHooks(pi, client, tools, binding);
    return;
  }

  pi.on("session_start", async (_event, ctx) => {
    try {
      const hello = await tools.refresh();
      if (ctx.hasUI) ctx.ui.setStatus("ontography", `Ontography · ${hello.server_id.slice(0, 8)}`);
    } catch (error) {
      if (ctx.hasUI) ctx.ui.notify(`Ontography connection failed: ${String(error)}. Reattach the session to reconnect.`, "error");
    }
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    client.disconnect();
    if (ctx.hasUI) ctx.ui.setStatus("ontography", undefined);
  });
}
