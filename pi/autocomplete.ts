import type { AutocompleteProviderFactory } from "@earendil-works/pi-coding-agent";

/**
 * Pi 0.85.1 can confirm the previous slash suggestion while its asynchronous
 * suggestion refresh is pending. Editor.handleInput passes the old prefix to
 * CombinedAutocompleteProvider.applyCompletion, which subtracts prefix.length
 * from the current cursor. For current "/graph" and stale prefix "/g", that
 * produces "/gragraph"; an obsolete candidate can also replace "/new" with
 * "/nname" or overwrite command arguments. Verified in pi-tui editor/autocomplete
 * sources and a native Pi session; the Rust PTY regression proves bytes arrive
 * once and in order. Keep the correction at the public completion boundary.
 */
export const guardSlashCompletion: AutocompleteProviderFactory = (current) => ({
  ...(current.triggerCharacters === undefined ? {} : { triggerCharacters: current.triggerCharacters }),
  ...(current.shouldTriggerFileCompletion === undefined ? {} : {
    shouldTriggerFileCompletion: (lines, line, column) => current.shouldTriggerFileCompletion!(lines, line, column),
  }),
  getSuggestions: (lines, line, column, options) => current.getSuggestions(lines, line, column, options),
  applyCompletion(lines, line, column, item, prefix) {
    const text = (lines[line] ?? "").slice(0, column);
    const slashName = /^\/[^\s/]*$/;
    const staleCommand = prefix !== text && slashName.test(prefix) &&
      text.startsWith("/") && /^[^\s/]+$/.test(item.value);
    if (staleCommand) {
      const matchingCommand = slashName.test(text) &&
        item.value.toLocaleLowerCase().startsWith(text.slice(1).toLocaleLowerCase());
      if (matchingCommand) return current.applyCompletion(lines, line, column, item, text);
      // Enter still submits after confirming a slash suggestion. Keeping the
      // current input prevents an obsolete candidate from changing the command.
      return { lines, cursorLine: line, cursorCol: column };
    }
    return current.applyCompletion(lines, line, column, item, prefix);
  },
});
