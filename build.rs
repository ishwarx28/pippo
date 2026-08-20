// Builds application metadata and the colocated decision process.

use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=go");

    let suffix = if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        ".exe"
    } else {
        ""
    };
    let path = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set by Cargo"))
        .join(format!("pippod{suffix}"));
    let status = Command::new("go")
        .args(["build", "-trimpath", "-o"])
        .arg(&path)
        .arg(".")
        .current_dir("go")
        .status()
        .expect("Go is required to build pippod");
    assert!(status.success(), "failed to build pippod");
    println!("cargo:rustc-env=PIPPO_PIPPOD_PATH={}", path.display());

    tauri_build::build()
}
