import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import type { AutocompleteProviderFactory } from "@earendil-works/pi-coding-agent";
import { guardSlashCompletion } from "../autocomplete.ts";

type Provider = Parameters<AutocompleteProviderFactory>[0];
// Use the exact pi-tui shipped with our pinned Pi, including its shrinkwrapped
// dependency layout. This test reproduces the actual upstream completion bug.
const piRequire = createRequire(import.meta.resolve("@earendil-works/pi-coding-agent"));
const { CombinedAutocompleteProvider } = await import(pathToFileURL(piRequire.resolve("@earendil-works/pi-tui")).href) as {
  CombinedAutocompleteProvider: new (commands: Array<{ name: string }>, basePath: string) => Provider;
};

test("stale slash prefix confirms the current command without duplicating typed characters", () => {
  const native = new CombinedAutocompleteProvider([{ name: "graph" }], "/tmp");
  const item = { value: "graph", label: "/graph" };
  assert.equal(native.applyCompletion(["/graph"], 0, 6, item, "/g").lines[0], "/gragraph");
  const result = guardSlashCompletion(native).applyCompletion(["/graph"], 0, 6, item, "/g");
  assert.deepEqual(result, { lines: ["/graph "], cursorLine: 0, cursorCol: 7 });
  assert.equal(guardSlashCompletion(native).applyCompletion(["/gra"], 0, 4, item, "/g").lines[0], "/graph ");
});

test("obsolete slash candidates preserve a different command and completed arguments", () => {
  const native = new CombinedAutocompleteProvider([{ name: "name" }, { name: "settings" }], "/tmp");
  const wrapped = guardSlashCompletion(native);
  const name = { value: "name", label: "name" };
  assert.equal(native.applyCompletion(["/new"], 0, 4, name, "/n").lines[0], "/nname");
  assert.deepEqual(wrapped.applyCompletion(["/new"], 0, 4, name, "/n"), {
    lines: ["/new"], cursorLine: 0, cursorCol: 4,
  });
  for (const prefix of ["/", "/s", "/name"]) {
    const text = "/name smoke-original";
    assert.deepEqual(wrapped.applyCompletion([text], 0, text.length,
      { value: "settings", label: "settings" }, prefix), {
      lines: [text], cursorLine: 0, cursorCol: text.length,
    });
  }
  // A current fuzzy suggestion is intentional, even without a literal prefix match.
  assert.deepEqual(wrapped.applyCompletion(["/nm"], 0, 3, name, "/nm"),
    native.applyCompletion(["/nm"], 0, 3, name, "/nm"));
});

test("non-command completion, suggestions, and provider state remain delegated", async () => {
  let appliedPrefix: string | undefined;
  const provider: Provider & { marker: string } = {
    marker: "provider-this",
    triggerCharacters: ["#"],
    getSuggestions(_lines, _line, _column, _options) {
      assert.equal(this.marker, "provider-this");
      return Promise.resolve({ prefix: "@fi", items: [{ value: "file", label: "file" }] });
    },
    shouldTriggerFileCompletion() { assert.equal(this.marker, "provider-this"); return true; },
    applyCompletion(lines, cursorLine, cursorCol, _item, prefix) {
      assert.equal(this.marker, "provider-this");
      appliedPrefix = prefix;
      return { lines, cursorLine, cursorCol };
    },
  };
  const wrapped = guardSlashCompletion(provider);
  assert.deepEqual(wrapped.triggerCharacters, ["#"]);
  assert.equal(wrapped.shouldTriggerFileCompletion?.(["@fi"], 0, 3), true);
  assert.equal((await wrapped.getSuggestions(["@fi"], 0, 3, { signal: new AbortController().signal }))?.prefix, "@fi");
  for (const [text, prefix, value] of [
    ["@file", "@fi", "file"],
    ["/tmp/file", "/tmp/fi", "/tmp/file"],
    ["/model abc", "abc", "abcdef"],
    ["explain /graph", "/gr", "graph"],
  ]) {
    wrapped.applyCompletion([text!], 0, text!.length, { value: value!, label: value! }, prefix!);
    assert.equal(appliedPrefix, prefix);
  }
});
