//! The crash-point library (`interpose/crashpoint.c`), from the harness's
//! side. It is plain C because the crate forbids unsafe Rust: the harness
//! compiles it with the system compiler when a crashpoints run begins and
//! loads it into servers only, through their environment. This module arms
//! it, marks the script's steps in its log, reads the log back, and names
//! each point so that the same point in different runs has the same name.

use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

const SOURCE: &str = include_str!("../interpose/crashpoint.c");

/// What the library does at its armed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum Action {
    /// Kill the server just before the call.
    Before,
    /// Make the call, then kill the server.
    After,
    /// Fail the call with EIO.
    Eio,
    /// Fail the call with ENOSPC.
    Enospc,
}

impl Action {
    pub const ALL: [Self; 4] = [Self::Before, Self::After, Self::Eio, Self::Enospc];

    pub fn name(self) -> &'static str {
        match self {
            Self::Before => "before",
            Self::After => "after",
            Self::Eio => "eio",
            Self::Enospc => "enospc",
        }
    }

    /// Whether the server dies at the point, rather than one call failing.
    pub fn kills(self) -> bool {
        matches!(self, Self::Before | Self::After)
    }
}

/// Compiles the library into `dir`, returning its path.
pub fn build(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let source = dir.join("crashpoint.c");
    std::fs::write(&source, SOURCE)?;
    let library = dir.join("crashpoint.dylib");
    let output = std::process::Command::new("cc")
        .args(["-dynamiclib", "-O2", "-Wall", "-Wextra", "-Werror", "-o"])
        .arg(&library)
        .arg(&source)
        .output()
        .context("run cc")?;
    if !output.status.success() {
        bail!(
            "compiling the crash-point library failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(library)
}

/// A server's environment for one run of a script: the library counts the
/// calls under `data` and records them to `log`; armed, it acts at the
/// `at`-th.
pub fn env(
    library: &Path,
    data: &Path,
    log: &Path,
    armed: Option<(usize, Action)>,
) -> Vec<(String, String)> {
    let text = |path: &Path| path.to_string_lossy().into_owned();
    let mut env = vec![
        ("DYLD_INSERT_LIBRARIES".into(), text(library)),
        ("TRIALS_CRASH_ROOT".into(), text(data)),
        ("TRIALS_CRASH_LOG".into(), text(log)),
        // The server's output and its diagnostics are not its state.
        (
            "TRIALS_CRASH_SKIP".into(),
            format!(
                "{}:{}",
                text(&data.join("server.log")),
                text(&data.join("logs"))
            ),
        ),
    ];
    if let Some((at, action)) = armed {
        env.push(("TRIALS_CRASH_AT".into(), at.to_string()));
        env.push(("TRIALS_CRASH_MODE".into(), action.name().into()));
    }
    env
}

/// Marks the start of a step in the log, so each point can be placed in the
/// step during which the server reached it.
pub fn mark(log: &Path, step: &str) {
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
    {
        let _ = file.write_all(format!("#\t{step}\n").as_bytes());
    }
}

/// One counted call, as the library recorded it.
#[derive(Clone, Debug)]
pub struct Call {
    pub index: usize,
    pub call: String,
    /// Its file, or a rename's two.
    pub paths: Vec<String>,
    /// The step under way when the server made it.
    pub step: String,
}

/// Every call in a log, in order.
pub fn read(log: &Path) -> Vec<Call> {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let mut step = String::from("server start");
    let mut calls = Vec::new();
    for line in text.lines() {
        let mut fields = line.split('\t');
        match fields.next() {
            Some("#") => step = fields.next().unwrap_or_default().to_owned(),
            Some(index) => {
                if let Ok(index) = index.parse() {
                    calls.push(Call {
                        index,
                        call: fields.next().unwrap_or_default().to_owned(),
                        paths: fields.map(String::from).collect(),
                        step: step.clone(),
                    });
                }
            }
            None => {}
        }
    }
    calls.sort_by_key(|call| call.index);
    calls
}

/// A point's name: the call and its paths within the data directory, with
/// the run's ID and other identities replaced.
pub fn label(call: &Call, data: &Path, run: &str) -> String {
    // The library sees paths as the file system spells them.
    let data = data.to_string_lossy().to_lowercase();
    let paths: Vec<String> = call
        .paths
        .iter()
        .map(|path| {
            let inside = path.to_lowercase().starts_with(&data);
            let relative = match path.get(data.len()..) {
                Some(rest) if inside => rest.trim_start_matches('/'),
                _ => path,
            };
            normalize(&relative.replace(run, "<run>"))
        })
        .collect();
    format!("{} {}", call.call, paths.join(" -> "))
}

/// Replaces UUIDs with `<id>` and long hexadecimal names with `<hash>`.
pub fn normalize(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let apart = |at: Option<&char>| at.is_none_or(|c| !c.is_ascii_alphanumeric());
    let uuid = |window: &[char]| {
        window.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == '-',
            _ => c.is_ascii_hexdigit(),
        })
    };
    let mut out = String::with_capacity(text.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        if word.len() >= 16 && word.chars().all(|c| c.is_ascii_hexdigit()) {
            out.push_str("<hash>");
        } else {
            out.push_str(word);
        }
        word.clear();
    };
    let mut i = 0;
    while i < chars.len() {
        let window = chars.get(i..i + 36);
        if word.is_empty()
            && window.is_some_and(uuid)
            && apart(i.checked_sub(1).and_then(|b| chars.get(b)))
            && apart(chars.get(i + 36))
        {
            out.push_str("<id>");
            i += 36;
            continue;
        }
        if chars[i].is_ascii_alphanumeric() {
            word.push(chars[i]);
        } else {
            flush(&mut word, &mut out);
            out.push(chars[i]);
        }
        i += 1;
    }
    flush(&mut word, &mut out);
    out
}
