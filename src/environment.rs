//! The environment the server gives the programs it starts.
//!
//! The background server outlives the terminal that started it, so it never
//! passes its own environment on. A session's programs, its Pi and its
//! agents, start with the environment of the client that activated the
//! session; work no session owns starts with the server's. Either way, one
//! rule drops what belongs to the terminal or agent session a command was
//! typed in, so no program inherits another's terminal, session, or
//! credentials.

use std::{collections::BTreeMap, sync::Arc};

/// Variables that describe the terminal a command was typed in. The server
/// gives each terminal it creates its own.
const TERMINAL: [&str; 10] = [
    "TERM",
    "COLORTERM",
    "TERMINFO",
    "TERMINFO_DIRS",
    "COLUMNS",
    "LINES",
    "STY",
    "WINDOW",
    "VTE_VERSION",
    "SSH_TTY",
];

/// Variable prefixes that terminal programs and emulators export.
const TERMINAL_PREFIXES: [&str; 10] = [
    "TERM_",
    "TMUX",
    "ITERM_",
    "KITTY_",
    "WEZTERM_",
    "ALACRITTY_",
    "GHOSTTY_",
    "KONSOLE_",
    "VSCODE_",
    "LC_TERMINAL",
];

/// One shell's own state, wrong for a program started elsewhere.
const SHELL: [&str; 4] = ["PWD", "OLDPWD", "SHLVL", "_"];

/// Variables an enclosing agent session exports for its own children.
/// Configuration, such as `CLAUDE_CONFIG_DIR` or `CODEX_HOME`, is kept.
const AGENT_SESSION: [&str; 12] = [
    "CLAUDECODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_CHILD_SESSION",
    "CLAUDE_CODE_SESSION_ATTENDED",
    "CLAUDE_CODE_MESSAGING_SOCKET",
    "CLAUDE_CODE_MESSAGING_TOKEN",
    "CLAUDE_CODE_EXECPATH",
    "CLAUDE_PID",
    "CLAUDE_EFFORT",
    "CODEX_SANDBOX",
    "CODEX_SANDBOX_NETWORK_DISABLED",
];

/// This app's per-program variables. The server sets them for each program
/// it starts, so inherited ones would name another session or node.
const APP_PREFIX: &str = "ONTOGRAPHY_";

/// What the background server process itself keeps. Nothing it needs
/// depends on the terminal that happened to start it.
const SERVER: [&str; 7] = ["HOME", "USER", "LOGNAME", "PATH", "SHELL", "TMPDIR", "LANG"];

/// A complete environment for the programs the server starts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Environment(Arc<BTreeMap<String, String>>);

impl Environment {
    /// The variables of `vars` that a program may inherit.
    pub fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Self {
        Self(Arc::new(
            vars.into_iter()
                .filter(|(name, value)| valid(name, value) && inheritable(name))
                .collect(),
        ))
    }

    /// This process's environment, as programs may inherit it.
    pub fn current() -> Self {
        Self::from_vars(std::env::vars_os().filter_map(|(name, value)| {
            Some((name.into_string().ok()?, value.into_string().ok()?))
        }))
    }

    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// The part of this environment the background server keeps for itself.
    pub fn for_server(&self) -> BTreeMap<String, String> {
        self.0
            .iter()
            .filter(|(name, _)| SERVER.contains(&name.as_str()) || name.starts_with("LC_"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

/// Whether a program can be given this variable at all.
fn valid(name: &str, value: &str) -> bool {
    !name.is_empty() && !name.contains(['=', '\0']) && !value.contains('\0')
}

fn inheritable(name: &str) -> bool {
    !(TERMINAL.contains(&name)
        || TERMINAL_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || SHELL.contains(&name)
        || AGENT_SESSION.contains(&name)
        || name.starts_with(APP_PREFIX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(names: &[&str]) -> Environment {
        Environment::from_vars(names.iter().map(|name| (name.to_string(), "x".to_owned())))
    }

    #[test]
    fn programs_inherit_the_user_but_not_their_terminal_or_agent_session() {
        let kept = environment(&[
            "HOME",
            "PATH",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "CLAUDE_CONFIG_DIR",
            "CODEX_HOME",
            "SSH_AUTH_SOCK",
            "LC_ALL",
            "TERMINAL_EDITOR",
            "CLAUDECODE",
            "CLAUDE_CODE_MESSAGING_TOKEN",
            "CLAUDE_CODE_SESSION_ID",
            "CODEX_SANDBOX",
            "TERM",
            "TERM_PROGRAM",
            "TMUX",
            "TMUX_PANE",
            "ITERM_SESSION_ID",
            "LC_TERMINAL",
            "VSCODE_IPC_HOOK_CLI",
            "PWD",
            "SHLVL",
            "ONTOGRAPHY_SESSION_ID",
            "ONTOGRAPHY_NODE_MCP_TOKEN",
        ]);
        assert_eq!(
            kept.vars().keys().collect::<Vec<_>>(),
            [
                "ANTHROPIC_API_KEY",
                "CLAUDE_CONFIG_DIR",
                "CODEX_HOME",
                "HOME",
                "LC_ALL",
                "OPENAI_API_KEY",
                "PATH",
                "SSH_AUTH_SOCK",
                "TERMINAL_EDITOR",
            ]
        );
    }

    #[test]
    fn malformed_variables_are_dropped() {
        let vars = [("A=B", "x"), ("", "x"), ("NUL", "a\0b"), ("OK", "yes")]
            .map(|(name, value)| (name.to_owned(), value.to_owned()));
        let environment = Environment::from_vars(vars);
        assert_eq!(environment.vars().keys().collect::<Vec<_>>(), ["OK"]);
    }

    #[test]
    fn the_server_keeps_only_what_it_needs_itself() {
        let client = environment(&[
            "HOME",
            "PATH",
            "LANG",
            "LC_CTYPE",
            "OPENAI_API_KEY",
            "EDITOR",
        ]);
        assert_eq!(
            client.for_server().keys().collect::<Vec<_>>(),
            ["HOME", "LANG", "LC_CTYPE", "PATH"]
        );
    }
}
