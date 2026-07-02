use std::fs;
use std::process::Command;

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-ObjC");
    }

    emit_git_rerun_paths();
    let version = display_version();
    println!("cargo:rustc-env=CODEX_CLI_DISPLAY_VERSION={version}");
}

fn display_version() -> String {
    let package_version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".into());
    if package_version != "0.0.0" {
        return package_version;
    }

    git_describe_version().unwrap_or(package_version)
}

fn git_describe_version() -> Option<String> {
    let output = Command::new("git")
        .args([
            "describe", "--tags", "--match", "rust-v*", "--long", "--dirty", "--always",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let describe = String::from_utf8(output.stdout).ok()?;
    let describe = describe.trim();
    if describe.is_empty() {
        return None;
    }

    Some(format_git_describe_version(describe))
}

fn format_git_describe_version(describe: &str) -> String {
    let (describe, dirty_suffix) = describe
        .strip_suffix("-dirty")
        .map_or((describe, ""), |clean| (clean, ".dirty"));

    let Some((tag_and_count, git_sha)) = describe.rsplit_once('-') else {
        return format!("0.0.0+{describe}.custom{dirty_suffix}");
    };
    let Some((tag, count)) = tag_and_count.rsplit_once('-') else {
        return format!("0.0.0+{describe}.custom{dirty_suffix}");
    };
    let version = tag.strip_prefix("rust-v").unwrap_or(tag);
    format!("{version}+{count}.{git_sha}.custom{dirty_suffix}")
}

fn emit_git_rerun_paths() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/packed-refs");
    println!("cargo:rerun-if-changed=../../.git/refs/tags");

    let Ok(head) = fs::read_to_string("../../.git/HEAD") else {
        return;
    };
    let Some(ref_path) = head.strip_prefix("ref: ").map(str::trim) else {
        return;
    };
    println!("cargo:rerun-if-changed=../../.git/{ref_path}");
}
