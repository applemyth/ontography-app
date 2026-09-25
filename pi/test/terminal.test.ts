import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import type { AutocompleteProviderFactory, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { getCapabilities, setCapabilityOverrides } from "@earendil-works/pi-tui";
import { applyTerminalCapabilities, registerTerminalPresentation } from "../terminal.ts";

function marker(t: TestContext, enabled: boolean): void {
  const previous = process.env.ONTOGRAPHY_TERMINAL;
  if (enabled) process.env.ONTOGRAPHY_TERMINAL = "1";
  else delete process.env.ONTOGRAPHY_TERMINAL;
  t.after(() => {
    if (previous === undefined) delete process.env.ONTOGRAPHY_TERMINAL;
    else process.env.ONTOGRAPHY_TERMINAL = previous;
    setCapabilityOverrides({});
  });
}

test("ordinary Pi clients retain their terminal capabilities", (t) => {
  marker(t, false);
  setCapabilityOverrides({ images: "kitty", hyperlinks: true });
  assert.equal(applyTerminalCapabilities(), false);
  assert.equal(getCapabilities().images, "kitty");
  assert.equal(getCapabilities().hyperlinks, true);
});

test("hosted native Pi capabilities are reasserted when reload rebuilds autocomplete", (t) => {
  marker(t, true);
  let factory: AutocompleteProviderFactory | undefined;
  const ctx = { mode: "tui", ui: { addAutocompleteProvider(value: AutocompleteProviderFactory) { factory = value; } } } as unknown as ExtensionContext;
  setCapabilityOverrides({ images: "kitty", hyperlinks: true });
  registerTerminalPresentation(ctx);
  assert.equal(getCapabilities().images, null);
  assert.equal(getCapabilities().hyperlinks, false);
  assert.ok(factory);
  // Native /reload reapplies settings after session_start, then reconstructs
  // the registered completion providers. Exercise that actual order.
  setCapabilityOverrides({ images: "iterm2", hyperlinks: true });
  factory({ getSuggestions: async () => null, applyCompletion: (lines, cursorLine, cursorCol) => ({ lines, cursorLine, cursorCol }) });
  assert.equal(getCapabilities().images, null);
  assert.equal(getCapabilities().hyperlinks, false);
});

test("headless Pi does not install terminal UI providers", (t) => {
  marker(t, true);
  let installs = 0;
  const ctx = { mode: "rpc", ui: { addAutocompleteProvider() { installs++; } } } as unknown as ExtensionContext;
  registerTerminalPresentation(ctx);
  assert.equal(installs, 0);
});
