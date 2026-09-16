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

mod check_bindings;

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
        Some("generate-error-codes") => generate_error_codes()?,
        Some("check-bindings") => check_bindings_task()?,
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
    check_error_codes_up_to_date()?;

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

/// Statically checks the hand-written CPython extension module for
/// `Py_BuildValue` / `PyArg_Parse*` format-arity mismatches.
///
/// These are variadic calls: a format string one unit short of its argument
/// list compiles without a warning and reads a garbage pointer at run time,
/// and for every admin RPC that Java's `MockAdminClient` leaves unsupported
/// the affected drain's success path is dead code in the test suite. So this
/// class of defect has to be caught statically or not at all.
///
/// An optional path argument overrides the default source file, which is what
/// lets the checker be pointed at an older revision of the file (extracted
/// with `git show`) to demonstrate that it detects a known-bad site.
fn check_bindings_task() -> anyhow::Result<()> {
    println!("🔍 Checking Python C-extension format-string arity...");

    let paths: Vec<PathBuf> = {
        let overrides: Vec<PathBuf> = env::args().skip(2).map(PathBuf::from).collect();
        if overrides.is_empty() {
            vec![PathBuf::from(check_bindings::DEFAULT_SOURCE)]
        } else {
            overrides
        }
    };

    for path in &paths {
        check_bindings::check_file(path)?;
    }

    println!("✅ No format-arity mismatches found!");
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

// ---------------------------------------------------------------------------
// Error-code constants generated from `kafka_common_ErrorCode_t`
// ---------------------------------------------------------------------------
//
// `src/ffi/common.rs`'s `kafka_common_ErrorCode_t` is the one place the error
// codes are declared. C sees them through the cbindgen-generated header, so it
// needs nothing here; the two consumers that cannot include that header need a
// copy of the values, and copies are what drift:
//
//   - `bindings/python/_error_code.py` -- the gRPC test servers put the real
//     code on their own synthetic errors ("unknown consumer_id"), and a unit
//     test asserts a code instead of matching message text. Private (leading
//     underscore): the Python public API is `code` / `message` /
//     `is_retriable` on `KafkaError` and nothing re-exports this module.
//   - `tests/common/error_code.rs` -- the multilanguage harness decodes a proto
//     `KafkaError` back into a Rust `Error` by its code. It cannot use the enum
//     itself: `src/ffi` is behind the `ffi` feature, which the multilanguage
//     test targets do not enable.
//
// `check-generated` re-runs the generation and fails on any difference, so a
// stale copy breaks the build rather than a test (CLAUDE.md #6: xtask programs
// rather than shell scripts).

const ERROR_CODE_SOURCE: &str = "src/ffi/common.rs";
const ERROR_CODE_PY: &str = "bindings/python/_error_code.py";
const ERROR_CODE_RS: &str = "tests/common/error_code.rs";

/// Extract `(name, value)` for every enumerator of `kafka_common_ErrorCode_t`.
fn parse_error_codes() -> anyhow::Result<Vec<(String, i32)>> {
    let source = fs::read_to_string(ERROR_CODE_SOURCE)?;
    let body = source
        .split_once("pub enum kafka_common_ErrorCode_t {")
        .map(|(_, rest)| rest)
        .ok_or_else(|| anyhow::anyhow!("{ERROR_CODE_SOURCE}: kafka_common_ErrorCode_t not found"))?;
    // The enum is the only item declared before the next top-level `}`.
    let body = body.split_once("\n}").map(|(body, _)| body).unwrap_or(body);

    let mut codes = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("kafka_common_ErrorCode_") else {
            continue;
        };
        let Some((name, value)) = rest.split_once(" = ") else {
            continue;
        };
        let value: i32 = value.trim_end_matches(',').parse()?;
        codes.push((name.to_string(), value));
    }
    if codes.is_empty() {
        anyhow::bail!("{ERROR_CODE_SOURCE}: kafka_common_ErrorCode_t has no enumerators");
    }
    Ok(codes)
}

fn error_codes_python(codes: &[(String, i32)]) -> String {
    let mut out = String::new();
    out.push_str(
        r#"# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Error-code constants -- GENERATED, DO NOT EDIT.

Generated from kafka_common_ErrorCode_t in src/ffi/common.rs by
`cargo xtask generate-error-codes`, and checked for staleness by
`cargo xtask check-generated`.

Private plumbing, not public API: KafkaError exposes `code`, `message` and
`is_retriable`, and neither producer.py nor consumer.py re-exports this module.
The users are the gRPC test servers, which stamp the real code on their own
synthetic errors, and the unit tests, which compare a code instead of matching
message text.

Values are the FFI error codes: Java's wire codes at Java's own values, plus
negatives for the classes only the client raises. They are injective over the
error classes, so the code alone identifies the class.
"""

"#,
    );
    for (name, value) in codes {
        out.push_str(&format!("{name} = {value}\n"));
    }
    out
}

fn error_codes_rust(codes: &[(String, i32)]) -> String {
    let mut out = String::new();
    out.push_str(
        r#"// Copyright 2025 Confluent Inc.
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

//! Error-code constants -- GENERATED, DO NOT EDIT.
//!
//! Generated from `kafka_common_ErrorCode_t` in `src/ffi/common.rs` by
//! `cargo xtask generate-error-codes`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! The multilanguage harness decodes a proto `KafkaError` back into an
//! [`Error`](confluent_kafka::common::Error) by its code, and cannot use the
//! enum itself: `src/ffi` is behind the `ffi` feature, which the multilanguage
//! test targets do not enable.
//!
//! Values are the FFI error codes: Java's wire codes at Java's own values, plus
//! negatives for the classes only the client raises. They are injective over
//! the error classes, so the code alone identifies the class.

"#,
    );
    for (name, value) in codes {
        out.push_str(&format!("pub const {name}: i32 = {value};\n"));
    }
    out
}

fn generate_error_codes() -> anyhow::Result<()> {
    println!("🔧 Generating error-code constants from {ERROR_CODE_SOURCE}...");

    let codes = parse_error_codes()?;
    fs::write(ERROR_CODE_PY, error_codes_python(&codes))?;
    fs::write(ERROR_CODE_RS, error_codes_rust(&codes))?;

    println!("✅ Wrote {} constants to {ERROR_CODE_PY} and {ERROR_CODE_RS}", codes.len());
    Ok(())
}

/// Fail when a generated error-code file no longer matches the Rust enum.
fn check_error_codes_up_to_date() -> anyhow::Result<()> {
    println!("🔍 Checking generated error-code constants...");

    let codes = parse_error_codes()?;
    let mut stale = Vec::new();
    for (path, expected) in [
        (ERROR_CODE_PY, error_codes_python(&codes)),
        (ERROR_CODE_RS, error_codes_rust(&codes)),
    ] {
        match fs::read_to_string(path) {
            Ok(actual) if actual == expected => {},
            _ => stale.push(path),
        }
    }

    if !stale.is_empty() {
        eprintln!("\n❌ Stale generated error-code constants: {}", stale.join(", "));
        eprintln!("   Run: cargo xtask generate-error-codes");
        exit(1);
    }

    println!("✅ Error-code constants are up to date ({} codes)", codes.len());
    Ok(())
}

fn lint() -> anyhow::Result<()> {
    // Structural doc defects clippy cannot see: an item's attributes or doc
    // comment migrated onto a neighbour. Run first, because it is instant and its
    // failures are always real.
    doc_hygiene()?;

    println!("🔍 Running clippy lints...");

    // Two passes over the whole workspace are needed to cover every module:
    //
    // - Default features: catches code behind `#[cfg(not(feature = ...))]`, and
    //   imports that are only unused when a feature is off.
    // - `--all-features`: `src/ffi/*` is gated behind `ffi`, and the integration
    //   tests behind `integration-tests` / `multilanguage-tests`. Without this
    //   pass those modules are compiled out and silently never linted.
    //
    // `--workspace` covers every member (including `generator`), so no
    // per-crate pass is needed.
    for pass in [
        &["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"][..],
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ][..],
    ] {
        run_command("cargo", pass)?;
    }

    // Lint the xtask crate itself. `cargo clippy` from the workspace root only
    // covers the root package, so without this the build tooling — including
    // the `check-bindings` scanner — would escape the lint gate entirely.
    run_command("cargo", &["clippy", "-p", "xtask", "--all-targets", "--", "-D", "warnings"])?;

    println!("✅ No lint issues found!");
    Ok(())
}

fn lint_fix() -> anyhow::Result<()> {
    println!("🔧 Running clippy with automatic fixes...");

    // Same two passes as `lint()` — see the comment there for why both are
    // needed. Fixing only the default-feature pass would leave `src/ffi/*` and
    // the integration tests untouched.
    for pass in [
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--fix",
            "--allow-dirty",
            "--allow-staged",
            "--",
            "-D",
            "warnings",
        ][..],
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--fix",
            "--allow-dirty",
            "--allow-staged",
            "--",
            "-D",
            "warnings",
        ][..],
    ] {
        run_command("cargo", pass)?;
    }

    // Fix the xtask crate itself (see the matching comment in `lint`).
    run_command(
        "cargo",
        &[
            "clippy",
            "-p",
            "xtask",
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
  check-generated Check generated code formatting and error-code staleness (no changes)
  generate-error-codes  Regenerate the error-code constants for Python and the test harness
  check-bindings  Check Py_BuildValue / PyArg_Parse* format arity in the Python C extension
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
  cargo xtask generate-error-codes
  cargo xtask check-bindings [path/to/file.c]
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
