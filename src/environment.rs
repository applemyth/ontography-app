//! The environment the server gives the programs it starts.
//!
//! The background server outlives the terminal that started it, so it never
//! passes its own environment on. A session's programs, its Pi and its
//! agents, start with the environment of the command that activated the
//! session; work no session owns starts with the latest command's. Either
//! way, one rule drops what belongs to the terminal or agent session a
//! command was typed in, so no program inherits another's terminal, session,
//! or credentials.

use std::{collections::BTreeMap, sync::Arc};

/// Variables that describe the terminal a command was typed in. The server
/// gives each terminal it creates its own.
const TERMINAL: [&str; 9] = [
    "TERM",
    "COLORTERM",
    "TERMINFO",
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
const AGENT_SESSION: [&str; 13] = [
    "AI_AGENT",
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

/// Settings Claude Code gives the tools it runs, such as `GIT_EDITOR=true`.
/// People set some of them too, so they are dropped only from a command
/// typed inside Claude Code.
const CLAUDE_CODE_TOOLS: [&str; 3] = [
    "GIT_EDITOR",
    "COREPACK_ENABLE_AUTO_PIN",
    "NoDefaultCurrentDirectoryInExePath",
];

/// This app's per-program variables. The server sets them for each program
/// it starts, so inherited ones would name another session, node, or store.
const APP_PREFIX: &str = "ONTOGRAPHY_";

/// What the background server process itself keeps. Nothing it needs
/// depends on the terminal that happened to start it.
const SERVER: [&str; 9] = [
    "HOME",
    "USER",
    "LOGNAME",
    "PATH",
    "SHELL",
    "TMPDIR",
    "LANG",
    "RUST_BACKTRACE",
    "RUST_LOG",
];

/// A complete environment for the programs the server starts.
#[derive(Clone, Debug)]
pub struct Environment(Arc<BTreeMap<String, String>>);

impl Environment {
    /// The variables of `vars` that a program may inherit.
    pub fn from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Self {
        let vars: BTreeMap<_, _> = vars
            .into_iter()
            .filter(|(name, value)| valid(name, value))
            .collect();
        let inside_claude_code = vars.contains_key("CLAUDECODE");
        Self(Arc::new(
            vars.into_iter()
                .filter(|(name, _)| {
                    inheritable(name)
                        && !(inside_claude_code && CLAUDE_CODE_TOOLS.contains(&name.as_str()))
                })
                .collect(),
        ))
    }

    /// This process's environment, as programs may inherit it.
    pub fn current() -> Self {
        Self::from_vars(std::env::vars_os().filter_map(|(name, value)| {
            Some((name.into_string().ok()?, value.into_string().ok()?))
        }))
    }

    /// This environment with `name` set, such as a variable the server sets
    /// for every program it starts.
    pub fn with(&self, name: &str, value: impl Into<String>) -> Self {
        let mut vars = (*self.0).clone();
        vars.insert(name.to_owned(), value.into());
        Self(Arc::new(vars))
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

    fn names(environment: &Environment) -> Vec<&str> {
        environment.vars().keys().map(String::as_str).collect()
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
            "TERMINFO_DIRS",
            "GIT_EDITOR",
            "AI_AGENT",
            "CODEX_SANDBOX",
            "TERM",
            "TERM_PROGRAM",
            "TERMINFO",
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
            names(&kept),
            [
                "ANTHROPIC_API_KEY",
                "CLAUDE_CONFIG_DIR",
                "CODEX_HOME",
                "GIT_EDITOR",
                "HOME",
                "LC_ALL",
                "OPENAI_API_KEY",
                "PATH",
                "SSH_AUTH_SOCK",
                "TERMINAL_EDITOR",
                "TERMINFO_DIRS",
            ]
        );
    }

    #[test]
    fn a_command_typed_inside_claude_code_leaves_its_tool_settings_behind() {
        let inside = environment(&[
            "PATH",
            "CLAUDECODE",
            "CLAUDE_CODE_MESSAGING_TOKEN",
            "GIT_EDITOR",
            "COREPACK_ENABLE_AUTO_PIN",
            "NoDefaultCurrentDirectoryInExePath",
        ]);
        assert_eq!(names(&inside), ["PATH"]);
    }

    #[test]
    fn malformed_variables_are_dropped() {
        let vars = [("A=B", "x"), ("", "x"), ("NUL", "a\0b"), ("OK", "yes")]
            .map(|(name, value)| (name.to_owned(), value.to_owned()));
        assert_eq!(names(&Environment::from_vars(vars)), ["OK"]);
    }

    #[test]
    fn the_server_keeps_only_what_it_needs_itself() {
        let client = environment(&[
            "HOME",
            "PATH",
            "LANG",
            "LC_CTYPE",
            "RUST_LOG",
            "OPENAI_API_KEY",
            "EDITOR",
        ]);
        assert_eq!(
            client.for_server().keys().collect::<Vec<_>>(),
            ["HOME", "LANG", "LC_CTYPE", "PATH", "RUST_LOG"]
        );
    }

    #[test]
    fn the_server_adds_its_own_variables() {
        let environment = environment(&["PATH"]).with("ONTOGRAPHY_DATA_DIR", "/store");
        assert_eq!(environment.get("ONTOGRAPHY_DATA_DIR"), Some("/store"));
    }
}
