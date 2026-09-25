import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { setCapabilityOverrides } from "@earendil-works/pi-tui";
import { guardSlashCompletion } from "./autocomplete.ts";

/** Match the Rust terminal's supported presentation without changing Pi settings. */
export function applyTerminalCapabilities(): boolean {
  if (process.env.ONTOGRAPHY_TERMINAL !== "1") return false;
  setCapabilityOverrides({ images: null, hyperlinks: false });
  return true;
}

export function registerTerminalPresentation(ctx: Pick<ExtensionContext, "mode" | "ui">): void {
  if (ctx.mode !== "tui" || !applyTerminalCapabilities()) return;
  ctx.ui.addAutocompleteProvider((current) => {
    // Pi reapplies user terminal settings after /reload's session_start hook,
    // then reconstructs completion providers. Reassert the hosted terminal's
    // capabilities here as well as during startup and before agent output.
    applyTerminalCapabilities();
    return guardSlashCompletion(current);
  });
}
