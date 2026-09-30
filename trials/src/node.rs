//! Node mode: this binary as a command node's program, as a document's
//! `argv` places it. It reads the task's input on stdin, records the run in
//! its node's witness file, and behaves as its role says. The witness file is
//! the only outside record of how often the app ran a task.

use crate::roles::{self, Role};
use anyhow::Result;
use serde_json::json;
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(long, value_enum)]
    role: Role,
    /// The node's name in the document.
    #[arg(long)]
    name: String,
    /// The directory of witness files, one per node.
    #[arg(long)]
    witness: PathBuf,
}

pub fn run(args: Args) -> Result<()> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let sha = roles::sha(&input);
    let path = args.witness.join(format!("{}.jsonl", args.name));
    // A flaky program fails the first time it sees an input.
    let seen = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains(&sha))
        .count();
    let line = json!({"pid": std::process::id(), "sha": sha, "len": input.len(), "role": args.role.name()});
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    // One write, so concurrent runs never interleave within a line.
    file.write_all(format!("{line}\n").as_bytes())?;
    drop(file);
    let mut stdout = std::io::stdout().lock();
    match args.role {
        Role::Broken => std::process::exit(4),
        Role::Flaky if seen == 0 => std::process::exit(3),
        Role::Slow => {
            std::thread::sleep(std::time::Duration::from_secs(roles::SLOW_SECS));
            stdout.write_all(&roles::output(Role::Digest, &args.name, &input))?;
        }
        Role::Big => stdout.write_all(&vec![b'x'; roles::BIG_BYTES])?,
        role => stdout.write_all(&roles::output(role, &args.name, &input))?,
    }
    stdout.flush()?;
    Ok(())
}
