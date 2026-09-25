use sha2::{Digest, Sha256};
use std::{fs, path::Path};

fn include(hash: &mut Sha256, base: &Path, path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
    if path.is_dir() {
        let mut entries = fs::read_dir(path)
            .expect("source directory")
            .map(|e| e.expect("source entry").path())
            .collect::<Vec<_>>();
        entries.sort();
        for entry in entries {
            include(hash, base, &entry);
        }
    } else {
        hash.update(
            path.strip_prefix(base)
                .unwrap_or(path)
                .as_os_str()
                .as_encoded_bytes(),
        );
        hash.update([0]);
        hash.update(fs::read(path).expect("source file"));
        hash.update([0]);
    }
}

fn identity(base: &Path, names: &[&str]) -> String {
    let mut hash = Sha256::new();
    for name in names {
        include(&mut hash, base, &base.join(name));
    }
    format!("{:x}", hash.finalize())
}

fn main() {
    let core = identity(
        Path::new("../ontography-core"),
        &["Cargo.toml", "src", "vendor"],
    );
    let app = identity(
        Path::new("."),
        &[
            "Cargo.toml",
            "Cargo.lock",
            "build.rs",
            "src",
            "pi/index.ts",
            "pi/client.ts",
            "pi/tools.ts",
            "pi/session.ts",
            "pi/autocomplete.ts",
            "pi/terminal.ts",
            "pi/instructions.md",
        ],
    );
    println!("cargo:rustc-env=ONTOGRAPHY_CORE_BUILD={core}");
    println!("cargo:rustc-env=ONTOGRAPHY_APP_BUILD={app}");
}
