//! What a command node's program does with its input. The program (node mode)
//! and the judge share these recipes: the trial tests how the app runs,
//! retries and publishes programs, not the recipes themselves.

use sha2::{Digest, Sha256};

/// A command node's behaviour, chosen by the world.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Role {
    /// Prints a digest of its input.
    Digest,
    /// Fails the first time it sees an input, then behaves like `Digest`.
    Flaky,
    /// Always exits with an error.
    Broken,
    /// Prints bytes that are not UTF-8.
    Binary,
    /// Outlives its timeout.
    Slow,
    /// Prints more than the harness keeps.
    Big,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::Digest => "digest",
            Self::Flaky => "flaky",
            Self::Broken => "broken",
            Self::Binary => "binary",
            Self::Slow => "slow",
            Self::Big => "big",
        }
    }

    /// Whether a task at this node can ever succeed, given whether the node's
    /// result contract accepts any bytes.
    pub fn succeeds(self, bytes_result: bool) -> bool {
        match self {
            Self::Digest | Self::Flaky => true,
            Self::Binary => bytes_result,
            Self::Broken | Self::Slow | Self::Big => false,
        }
    }
}

/// Seconds a slow program sleeps; its node's timeout is shorter.
pub const SLOW_SECS: u64 = 3;
/// What a big program prints: one byte more than the harness keeps.
pub const BIG_BYTES: usize = 1024 * 1024 + 1;

pub fn sha(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// What a successful run of `role` at `node` prints for `input`.
pub fn output(role: Role, node: &str, input: &[u8]) -> Vec<u8> {
    let digest = sha(input);
    match role {
        Role::Binary => {
            let mut bytes = vec![0xff, 0xfe];
            bytes.extend_from_slice(node.as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(&digest.as_bytes()[..16]);
            bytes
        }
        _ => format!("{node} {} {}\n", &digest[..16], input.len()).into_bytes(),
    }
}

/// Core's commitment to payload bytes, as its documentation defines it.
pub fn content_digest(payload: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"ontography-payload/v1\0");
    hash.update(payload);
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
