// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{exit, Command};
fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);

    match task.as_deref() {
        Some("format") => format()?,
        Some("format-check") => format_check()?,
        Some("check-generated") => check_generated()?,
        Some("lint") => lint()?,
        Some("lint-fix") => lint_fix()?,
        Some("coverage") => coverage()?,
        Some("coverage-lcov") => coverage_lcov()?,
        Some("coverage-all") => coverage_all()?,
        Some("test-multilanguage") => test_multilanguage()?,
        _ => print_help(),
    }

    Ok(())
}

fn format() -> anyhow::Result<()> {
    println!("🎨 Formatting Rust code...");

    // Format main crate
    run_command("cargo", &["fmt"])?;

    // Format generator crate
    run_command("cargo", &["fmt", "--manifest-path", "generator/Cargo.toml"])?;

    println!("✅ Formatting complete!");
    Ok(())
}

fn format_check() -> anyhow::Result<()> {
    println!("🔍 Checking Rust code formatting...");

    // Check main crate
    let main_result = Command::new("cargo").args(["fmt", "--", "--check"]).status()?;

    // Check generator crate
    let gen_result = Command::new("cargo")
        .args(["fmt", "--manifest-path", "generator/Cargo.toml", "--", "--check"])
        .status()?;

    if !main_result.success() || !gen_result.success() {
        eprintln!("❌ Code is not formatted. Run: cargo xtask format");
        exit(1);
    }

    println!("✅ All code is properly formatted!");
    Ok(())
}

fn check_generated() -> anyhow::Result<()> {
    println!("🔍 Checking generated code formatting...");

    let generated_files = find_generated_files()?;

    if generated_files.is_empty() {
        println!("⚠️  No generated files found (build first with cargo build)");
        return Ok(());
    }

    println!("   Checking {} generated file(s)", generated_files.len());

    let status = Command::new("rustfmt").arg("--check").args(&generated_files).status()?;

    if !status.success() {
        eprintln!("\n❌ Generated code has formatting issues. Run: cargo build && cargo xtask format");
        exit(1);
    }

    println!("✅ All generated code is properly formatted!");
    Ok(())
}

fn find_generated_files() -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let target_dir = PathBuf::from("target/debug/build");

    if !target_dir.exists() {
        return Ok(files);
    }

    // Find the most recently modified generated directory to avoid
    // formatting stale build artifacts (there can be many).
    let mut newest_dir: Option<(PathBuf, std::time::SystemTime)> = None;

    for entry in fs::read_dir(&target_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() && path.file_name().unwrap().to_str().unwrap().starts_with("confluent-kafka-rust-") {
            let generated_dir = path.join("out/generated");
            if generated_dir.exists() {
                let modified = entry.metadata()?.modified()?;
                if newest_dir.as_ref().is_none_or(|(_, t)| modified > *t) {
                    newest_dir = Some((generated_dir, modified));
                }
            }
        }
    }

    if let Some((generated_dir, _)) = newest_dir {
        for file_entry in fs::read_dir(generated_dir)? {
            let file_entry = file_entry?;
            let file_path = file_entry.path();
            if file_path.extension().is_some_and(|ext| ext == "rs") {
                files.push(file_path);
            }
        }
    }

    Ok(files)
}

fn lint() -> anyhow::Result<()> {
    println!("🔍 Running clippy lints...");

    // Lint main crate
    run_command("cargo", &["clippy", "--all-targets", "--", "-D", "warnings"])?;

    // Lint generator crate
    run_command(
        "cargo",
        &[
            "clippy",
            "--manifest-path",
            "generator/Cargo.toml",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )?;

    println!("✅ No lint issues found!");
    Ok(())
}

fn lint_fix() -> anyhow::Result<()> {
    println!("🔧 Running clippy with automatic fixes...");

    // Fix main crate
    run_command(
        "cargo",
        &[
            "clippy",
            "--all-targets",
            "--fix",
            "--allow-dirty",
            "--allow-staged",
            "--",
            "-D",
            "warnings",
        ],
    )?;

    // Fix generator crate
    run_command(
        "cargo",
        &[
            "clippy",
            "--manifest-path",
            "generator/Cargo.toml",
            "--all-targets",
            "--fix",
            "--allow-dirty",
            "--allow-staged",
            "--",
            "-D",
            "warnings",
        ],
    )?;

    println!("✅ Lint fixes applied!");
    Ok(())
}

fn coverage() -> anyhow::Result<()> {
    println!("Running unit test coverage...");
    run_coverage_lcov(&[])?;
    run_grcov_html()?;
    println!("Coverage report: coverage/html/index.html");
    Ok(())
}

fn coverage_lcov() -> anyhow::Result<()> {
    println!("Running unit test coverage (lcov)...");
    run_coverage_lcov(&[])?;
    println!("lcov report: coverage/lcov.info");
    Ok(())
}

fn coverage_all() -> anyhow::Result<()> {
    println!("Running full test coverage (unit + integration, requires Docker)...");
    run_coverage_lcov(&["--features", "integration-tests"])?;
    run_grcov_html()?;
    println!("Coverage report: coverage/html/index.html");
    Ok(())
}

fn test_multilanguage() -> anyhow::Result<()> {
    println!("Running multilanguage integration tests (requires Docker)...");
    println!("This builds the python + c gRPC server images, then runs");
    println!("`cargo test --features integration-tests,multilanguage-tests`.");
    // Delegated to the Makefile target so the image-build steps stay in
    // one place; the Makefile shells out to bindings/{python,c}/Makefile.
    run_command("make", &["test-multilanguage"])
}

fn run_coverage_lcov(extra_args: &[&str]) -> anyhow::Result<()> {
    fs::create_dir_all("coverage")?;
    let mut args = vec![
        "llvm-cov",
        "--package",
        "confluent-kafka-rust",
        "--ignore-filename-regex",
        "(target/debug/build/.*/out/(test_)?generated/|src/bin/)",
        "--lcov",
        "--output-path",
        "coverage/lcov.info",
    ];
    args.extend_from_slice(extra_args);
    run_command("cargo", &args)
}

fn run_grcov_html() -> anyhow::Result<()> {
    run_command("grcov", &["coverage/lcov.info", "-s", ".", "-t", "html", "-o", "coverage/html"])
}

fn run_command(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(program).args(args).status()?;

    if !status.success() {
        anyhow::bail!("Command failed: {} {}", program, args.join(" "));
    }

    Ok(())
}

fn print_help() {
    eprintln!(
        "Tasks:
  format          Format all Rust code including generated files
  format-check    Check if code is formatted correctly
  check-generated Check generated code formatting only (no changes)
  lint            Run clippy lints (warnings are errors)
  lint-fix        Run clippy and automatically fix what it can
  coverage        Run unit test coverage (HTML report)
  coverage-lcov   Run unit test coverage (lcov for CI)
  coverage-all    Run all test coverage including integration (requires Docker)
  test-multilanguage  Run producer integration tests against rust/python/c backends (requires Docker)

Usage:
  cargo xtask format
  cargo xtask format-check
  cargo xtask check-generated
  cargo xtask lint
  cargo xtask lint-fix
  cargo xtask coverage
  cargo xtask coverage-lcov
  cargo xtask coverage-all
  cargo xtask test-multilanguage"
    );
}
