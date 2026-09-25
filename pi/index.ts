import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { BridgeClient, object } from "./client.ts";
import { ManagementTools } from "./tools.ts";

export default function ontography(pi: ExtensionAPI): void {
  const socketPath = process.env.ONTOGRAPHY_SOCKET;
  if (socketPath === undefined || socketPath.length === 0) throw new Error("Launch this extension with ontography; ONTOGRAPHY_SOCKET must identify its server.");
  const expectedAppBuild = process.env.ONTOGRAPHY_APP_BUILD;
  const expectedCoreBuild = process.env.ONTOGRAPHY_CORE_BUILD;
  if (!expectedAppBuild || !expectedCoreBuild) throw new Error("Launch this extension with ontography; its app/core build identities are required.");
  // This nonsecret identifier remains stable when Pi reloads the extension.
  process.env.ONTOGRAPHY_CLIENT_ID ??= randomUUID();
  const client = new BridgeClient({ socketPath, clientId: process.env.ONTOGRAPHY_CLIENT_ID, expectedAppBuild, expectedCoreBuild });
  const tools = new ManagementTools(pi, client);
  tools.registerBootstrap();

  pi.on("session_start", async (_event, ctx) => {
    const entries = ctx.sessionManager.getBranch();
    let groups: string[] | undefined;
    for (const entry of entries) {
      if (entry.type === "custom" && entry.customType === "ontography_tool_groups" && object(entry.data) &&
          Array.isArray(entry.data.groups) && entry.data.groups.every((value: unknown) => typeof value === "string")) {
        groups = entry.data.groups as string[];
      }
    }
    try {
      const hello = await tools.refresh(groups);
      if (ctx.hasUI) ctx.ui.setStatus("ontography", `Ontography · ${hello.server_id.slice(0, 8)}`);
    } catch (error) {
      if (ctx.hasUI) ctx.ui.notify(`Ontography connection failed: ${String(error)}. Use ontography_tools to reconnect.`, "error");
      // Bootstrap remains available in interactive, RPC, JSON, and print modes.
    }
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    client.disconnect();
    if (ctx.hasUI) ctx.ui.setStatus("ontography", undefined);
  });
}
