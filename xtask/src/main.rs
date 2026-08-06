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
        Some("doc-hygiene") => doc_hygiene()?,
        Some("lint-fix") => lint_fix()?,
        Some("coverage") => coverage()?,
        Some("coverage-lcov") => coverage_lcov()?,
        Some("coverage-all") => coverage_all()?,
        Some("test-multilanguage") => test_multilanguage()?,
        Some("producer-perf-test") => producer_perf_test()?,
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
    // Structural doc defects clippy cannot see: an item's attributes or doc
    // comment migrated onto a neighbour. Run first, because it is instant and its
    // failures are always real.
    doc_hygiene()?;

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

/// Run the producer performance test as an env-driven benchmark binary.
///
/// Runs the same `producer_perf_test` that ships in the integration suite, but in
/// release mode and driven entirely by environment variables
/// (`BOOTSTRAP_SERVERS`, `VALUE_SIZE`, `LIMIT_RPS`, `TEST_DURATION_SECONDS`,
/// `COMPRESSION_TYPE`, `P99_LIMIT_MS`, ... — see the doc comment at the top of
/// `tests/integration/producer_perf_test.rs`). It writes `metrics.jsonl` in the
/// schema `tools/performance_metrics_plot/plot_metrics.py` consumes. Keep this
/// in sync with the other producer performance tests in the project.
///
/// Extra arguments after `producer-perf-test` are forwarded to the test binary,
/// e.g. `cargo xtask producer-perf-test --test-threads=1`.
fn producer_perf_test() -> anyhow::Result<()> {
    println!("🚀 Running env-driven producer performance benchmark...");
    println!("   Configure via environment variables (BOOTSTRAP_SERVERS, VALUE_SIZE,");
    println!("   LIMIT_RPS, TEST_DURATION_SECONDS, COMPRESSION_TYPE, P99_LIMIT_MS, ...).");
    println!("   For a max-rate benchmark set LIMIT_RPS=0 and P99_LIMIT_MS=0.");
    println!("   See tests/integration/producer_perf_test.rs for the full list.");

    let mut args: Vec<String> = vec![
        "test".into(),
        "--release".into(),
        "--features".into(),
        "integration-tests".into(),
        "--test".into(),
        "integration".into(),
        "--".into(),
        "--exact".into(),
        "producer_perf_test::producer_perf_test".into(),
        "--nocapture".into(),
    ];
    // Forward any extra args (e.g. --test-threads=1) to the test binary.
    args.extend(env::args().skip(2));

    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run_command("cargo", &arg_refs)
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

/// Checks for two structural doc defects that neither `rustfmt` nor clippy reports,
/// both of which occurred in Milestone 11 Phase 8 — and the second of which occurred
/// *in the fix for* the first.
///
/// 1. **A migrated attribute or doc comment.** An item inserted into a preceding
///    item's attribute list silently steals the attributes and doc between them, so
///    the earlier item loses (for example) its `#[cfg(test)]` and ships in release
///    builds. Signature: a `///` line, then one or more `#[..]` lines, then another
///    `///` line. Rust accepts it, so only a shape check finds it.
///
/// 2. **A doc block stacked on a doc block.** An item's doc left in place while a
///    rewritten block was added below it, giving two summaries and two
///    `Translated from` lines that rustdoc runs into one paragraph. Signature: one
///    contiguous `///` run containing two or more `Translated from` openers.
///
/// Shape 1's check was shipped for Phase 8 as an ad-hoc script and could not see
/// shape 2 — an attribute-free stack. Both live here now, and in `lint`, because
/// CLAUDE.md §6 puts repeatable checks in xtask rather than in shell scripts, and
/// because a check nobody is obliged to run is a check that finds the next instance
/// one review round late.
///
/// # Two known edge cases, both zero-hit in this repo
///
/// Recorded so the next reader has the repro rather than re-deriving it (found by
/// Critic 48 pass 3, which tested this binary against a synthetic tree):
///
/// - **False positive on the conditional-doc idiom.** `/// doc` /
///   `#[cfg_attr(docsrs, doc = "..")]` / `/// doc` is legitimate and would be flagged
///   as shape 1, as would a bare `#[doc = ".."]` between doc lines. Exposure is nil and
///   checkable — `grep -rn 'cfg_attr' src/`, `grep -rn '#\[doc' src/` and `docsrs`
///   anywhere all return 0, and `cargo doc` is in neither CLAUDE.md's workflow list nor
///   `make verify`. If it ever fires, skip attributes whose content starts `doc` or
///   `cfg_attr(..., doc`.
/// - **False negative on multi-line attributes.** The skip loop consumes only lines that
///   *start* with `#[`, so `/// doc` / `#[cfg(all(` / `feature = "a",` / `))]` /
///   `/// doc` is not flagged. The two multi-line attribute sites in `src/`
///   (`consumer_group_metadata.rs:54` and `:67`, both `#[deprecated(`) were each read
///   and are ordinary doc → attribute → `pub fn`, so nothing is hiding behind the gap.
fn doc_hygiene() -> anyhow::Result<()> {
    println!("🔍 Checking doc-comment hygiene...");

    let mut findings: Vec<String> = Vec::new();
    for path in rust_sources("src")? {
        let text = fs::read_to_string(&path)?;
        let lines: Vec<&str> = text.lines().collect();

        for (index, line) in lines.iter().enumerate() {
            if !line.trim_start().starts_with("///") {
                continue;
            }
            // Shape 1: doc line -> attribute line(s) -> doc line.
            let mut cursor = index + 1;
            let mut saw_attribute = false;
            while cursor < lines.len() && lines[cursor].trim_start().starts_with("#[") {
                saw_attribute = true;
                cursor += 1;
            }
            if saw_attribute && cursor < lines.len() && lines[cursor].trim_start().starts_with("///") {
                findings.push(format!(
                    "{}:{} doc comment separated from its item by an attribute list — an item \
                     was probably inserted into the preceding item's attributes",
                    path.display(),
                    index + 1
                ));
            }
        }

        // Shape 2: one contiguous `///` run with two or more `Translated from` openers.
        let mut run_start: Option<usize> = None;
        let mut openers = 0;
        for (index, line) in lines.iter().enumerate().chain(std::iter::once((lines.len(), &""))) {
            if line.trim_start().starts_with("///") {
                if run_start.is_none() {
                    run_start = Some(index);
                    openers = 0;
                }
                if line.contains("Translated from") {
                    openers += 1;
                }
            } else {
                if let Some(start) = run_start.filter(|_| openers > 1) {
                    findings.push(format!(
                        "{}:{} one doc block carries {} `Translated from` openers — a doc block \
                         was probably stacked on another",
                        path.display(),
                        start + 1,
                        openers
                    ));
                }
                run_start = None;
            }
        }
    }

    if findings.is_empty() {
        println!("✅ Doc-comment hygiene clean!");
        return Ok(());
    }
    for finding in &findings {
        eprintln!("  {finding}");
    }
    Err(anyhow::anyhow!("{} doc-hygiene finding(s)", findings.len()))
}

/// Every `.rs` file under `root`, recursively, in a deterministic order.
fn rust_sources(root: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut stack = vec![PathBuf::from(root)];
    while let Some(dir) = stack.pop() {
        let mut entries: Vec<PathBuf> = fs::read_dir(&dir)?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<Result<_, _>>()?;
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

fn print_help() {
    eprintln!(
        "Tasks:
  format          Format all Rust code including generated files
  format-check    Check if code is formatted correctly
  check-generated Check generated code formatting only (no changes)
  lint            Run doc-hygiene plus clippy lints (warnings are errors)
  doc-hygiene     Check for migrated attributes and stacked doc blocks
  lint-fix        Run clippy and automatically fix what it can
  coverage        Run unit test coverage (HTML report)
  coverage-lcov   Run unit test coverage (lcov for CI)
  coverage-all    Run all test coverage including integration (requires Docker)
  test-multilanguage  Run producer integration tests against rust/python/c backends (requires Docker)
  producer-perf-test  Run the env-driven producer performance benchmark (requires Docker or BOOTSTRAP_SERVERS)

Usage:
  cargo xtask format
  cargo xtask format-check
  cargo xtask check-generated
  cargo xtask lint
  cargo xtask doc-hygiene
  cargo xtask lint-fix
  cargo xtask coverage
  cargo xtask coverage-lcov
  cargo xtask coverage-all
  cargo xtask test-multilanguage
  cargo xtask producer-perf-test"
    );
}
