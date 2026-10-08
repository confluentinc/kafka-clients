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

mod java;
mod lint_custom;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{exit, Command};
fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);

    match task.as_deref() {
        Some("format") => format()?,
        Some("format-check") => format_check()?,
        Some("check-generated") => check_generated()?,
        Some("generate-error-codes") => generate_error_codes()?,
        Some("java-deprecated") => java_deprecated()?,
        Some("fetch-java-refs") => fetch_java_refs()?,
        Some("lint-custom") => lint_custom::lint_custom()?,
        Some("lint") => lint()?,
        Some("doc-hygiene") => doc_hygiene()?,
        Some("lint-fix") => lint_fix()?,
        Some("coverage") => coverage()?,
        Some("coverage-lcov") => coverage_lcov()?,
        Some("coverage-all") => coverage_all()?,
        Some("test-multilanguage") => test_multilanguage()?,
        Some("producer-perf-test") => producer_perf_test()?,
        Some("package-check") => package_check()?,
        _ => print_help(),
    }

    Ok(())
}

fn format() -> anyhow::Result<()> {
    println!("🎨 Formatting Rust code...");

    // Format main crate
    run_command("cargo", &["fmt"])?;

    // Format generator crate
    run_command("cargo", &["fmt", "--manifest-path", "generator/crate/Cargo.toml"])?;

    println!("✅ Formatting complete!");
    Ok(())
}

fn format_check() -> anyhow::Result<()> {
    println!("🔍 Checking Rust code formatting...");

    // Check main crate
    let main_result = Command::new("cargo").args(["fmt", "--", "--check"]).status()?;

    // Check generator crate
    let gen_result = Command::new("cargo")
        .args(["fmt", "--manifest-path", "generator/crate/Cargo.toml", "--", "--check"])
        .status()?;

    if !main_result.success() || !gen_result.success() {
        eprintln!("❌ Code is not formatted. Run: cargo xtask format");
        exit(1);
    }

    println!("✅ All code is properly formatted!");
    Ok(())
}

fn check_generated() -> anyhow::Result<()> {
    check_message_specs_in_sync()?;
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

        if path.is_dir() && path.file_name().unwrap().to_str().unwrap().starts_with("confluent-kafka-") {
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
// Message specs copied from the `kafka` submodule
// ---------------------------------------------------------------------------
//
// `build.rs` generates the wire-protocol code from `generator/messages/*.json`
// at build time, so no generated message code is checked in. The specs are a
// copy of the `kafka` submodule's, kept in the crate because the crates.io
// package (`Cargo.toml` `include`) cannot reach the submodule. The copy is
// what can drift: a hand edit, or a submodule bump that does not re-copy the
// specs, would silently build wire formats other than the pinned Kafka
// release's.
//
// `check-generated` compares the two byte for byte and fails on any
// difference. Without the submodule it fails too, as `lint-custom` does: a
// skip that exits 0 would let a checkout without it pass unchecked.

const MESSAGE_SPECS: &str = "generator/messages";
const KAFKA_MESSAGE_SPECS: &str = "../kafka/clients/src/main/resources/common/message";

/// Fail when `generator/messages/` no longer matches the `kafka` submodule's specs.
fn check_message_specs_in_sync() -> anyhow::Result<()> {
    println!("🔍 Checking message specs against the kafka submodule...");

    // Hints run in `rust/`, xtask's working directory, so the submodule is `../kafka`.
    if !Path::new(KAFKA_MESSAGE_SPECS).is_dir() {
        eprintln!("\n❌ Cannot check message specs: no `{KAFKA_MESSAGE_SPECS}`");
        eprintln!("   Run in rust/: git submodule update --init ../kafka");
        exit(1);
    }

    let copy = read_message_specs(Path::new(MESSAGE_SPECS))?;
    let drift = message_spec_drift(&copy, &read_message_specs(Path::new(KAFKA_MESSAGE_SPECS))?);
    if !drift.is_empty() {
        eprintln!("\n❌ `{MESSAGE_SPECS}` does not match `{KAFKA_MESSAGE_SPECS}`:");
        for line in &drift {
            eprintln!("   {line}");
        }
        // A pull that moves the pinned commit leaves the submodule checkout
        // behind, and re-copying from it would revert the pulled specs.
        eprintln!("   Fix, in rust/:");
        eprintln!("     If you did not move the kafka submodule yourself, update it first (git pull does not):");
        eprintln!("       git submodule update ../kafka");
        eprintln!("     If they still differ, copy the submodule's specs:");
        eprintln!("       rm {MESSAGE_SPECS}/*.json && cp {KAFKA_MESSAGE_SPECS}/*.json {MESSAGE_SPECS}/");
        exit(1);
    }

    println!("✅ Message specs match the kafka submodule ({} specs)", copy.len());
    Ok(())
}

/// The specs in `dir` by file name: its `*.json` files, the ones the generator
/// reads (`generator/src/lib.rs`). Kafka's `README.md` beside them is not one.
fn read_message_specs(dir: &Path) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
    let mut specs = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "json") {
            specs.insert(entry.file_name().to_string_lossy().into_owned(), fs::read(&path)?);
        }
    }
    Ok(specs)
}

/// One line per spec that differs between the copy and the submodule's, in
/// file-name order; empty when they match.
fn message_spec_drift(copy: &BTreeMap<String, Vec<u8>>, kafka: &BTreeMap<String, Vec<u8>>) -> Vec<String> {
    let names: BTreeSet<&String> = copy.keys().chain(kafka.keys()).collect();
    names
        .into_iter()
        .filter_map(|name| match (copy.get(name), kafka.get(name)) {
            (Some(ours), Some(theirs)) if ours == theirs => None,
            (Some(_), Some(_)) => Some(format!("{name}: differs")),
            (Some(_), None) => Some(format!("{name}: only in {MESSAGE_SPECS}/")),
            (None, _) => Some(format!("{name}: only in the kafka submodule")),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Error-code constants generated from `kafka_common_ErrorCode_t`
// ---------------------------------------------------------------------------
//
// `src/ffi/common.rs`'s `kafka_common_ErrorCode_t` is the one place the error
// codes are declared. C sees them through the cbindgen-generated header, so it
// needs nothing here. `tests/common/error_code.rs` cannot include that header
// and needs a copy of the values, and copies are what drift: the multilanguage
// harness decodes a proto `KafkaError` back into a Rust `Error` by its code. It
// cannot use the enum itself: `src/ffi` is behind the `ffi` feature, which the
// multilanguage test targets do not enable.
//
// The Python binding's copy, `python/_error_code.py`, is generated and
// checked by the binding itself (`python/tools/generate_error_code.py`
// and `test/static/test_error_code_generated.py`).
//
// `check-generated` re-runs the generation and fails on any difference, so a
// stale copy breaks the build rather than a test (CLAUDE.md #8: xtask programs
// rather than shell scripts).

const ERROR_CODE_SOURCE: &str = "src/ffi/common.rs";
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

fn error_codes_rust(codes: &[(String, i32)], classes: &[(String, String)]) -> anyhow::Result<String> {
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
    out.push_str("use std::collections::HashSet;\n\n");
    for import in parse_error_class_imports()? {
        out.push_str(&format!("use {import};\n"));
    }
    out.push('\n');
    for (name, value) in codes {
        out.push_str(&format!("pub const {name}: i32 = {value};\n"));
    }
    out.push_str(
        r#"
/// Rebuild the error class that owns the protocol error `code`, carrying
/// `message` -- the table of `Errors::error_with_message` (Java's
/// `Errors.exception(String)`), spelled with the classes' public constructors
/// because `Errors` itself is not public API.
///
/// `None` for a code no broker-side class owns: `NONE`, and the negatives of
/// the classes only the client raises.
pub fn error_with_message(code: i32, message: String) -> Option<Error> {
    match code {
"#,
    );
    for (name, expr) in classes {
        out.push_str(&format!("        {name} => Some({expr}),\n"));
    }
    out.push_str("        _ => None,\n    }\n}\n");
    rustfmt(&out)
}

/// Format generated Rust source with the repository's `rustfmt.toml`, so the
/// generated file also passes `cargo xtask format-check`.
///
/// `cargo fmt` passes the crate's edition to rustfmt, overriding the
/// `edition = "2021"` of `rustfmt.toml`; so must we. The 2024 style edition
/// sorts imports differently (`Error` before `errors::*`), and without it the
/// output `format-check` accepts would never match the generator's.
fn rustfmt(source: &str) -> anyhow::Result<String> {
    use std::io::Write as _;
    use std::process::Stdio;

    let edition = crate_edition()?;
    let mut child = Command::new("rustfmt")
        .args([
            "--emit",
            "stdout",
            "--config-path",
            "rustfmt.toml",
            "--edition",
            &edition,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child.stdin.take().expect("piped stdin").write_all(source.as_bytes())?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        anyhow::bail!("rustfmt failed on the generated error-code table");
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// The `edition` of the root crate's `[package]`, as `cargo fmt` reads it.
fn crate_edition() -> anyhow::Result<String> {
    let manifest = fs::read_to_string("Cargo.toml")?;
    manifest
        .lines()
        .skip_while(|l| l.trim() != "[package]")
        .skip(1)
        .take_while(|l| !l.trim_start().starts_with('['))
        .find_map(|l| {
            let (key, value) = l.split_once('=')?;
            (key.trim() == "edition").then(|| value.trim().trim_matches('"').to_string())
        })
        .ok_or_else(|| anyhow::anyhow!("no `edition` in the `[package]` of Cargo.toml"))
}

const ERRORS_SOURCE: &str = "src/common/protocol/errors.rs";

/// Extract `(C enumerator name, constructor expression)` for every arm of
/// `Errors::error_with_message` that yields an error.
///
/// Each arm is `Self::<Variant> => Some(<expr>)`; the enumerator name is the
/// variant in SCREAMING_SNAKE_CASE, which is how `kafka_common_ErrorCode_t`
/// spells Java's `Errors` constants. An arm naming no enumerator is an error, so
/// the two tables cannot drift apart silently.
fn parse_error_classes(codes: &[(String, i32)]) -> anyhow::Result<Vec<(String, String)>> {
    use quote::ToTokens as _;

    let file = syn::parse_file(&fs::read_to_string(ERRORS_SOURCE)?)?;
    let body = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(item) => Some(item),
            _ => None,
        })
        .flat_map(|item| &item.items)
        .find_map(|item| match item {
            syn::ImplItem::Fn(f) if f.sig.ident == "error_with_message" => Some(&f.block),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("{ERRORS_SOURCE}: Errors::error_with_message not found"))?;
    let arms = body
        .stmts
        .iter()
        .find_map(|stmt| match stmt {
            syn::Stmt::Expr(syn::Expr::Match(m), _) => Some(&m.arms),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("{ERRORS_SOURCE}: error_with_message has no match"))?;

    let known: std::collections::HashSet<&str> = codes.iter().map(|(name, _)| name.as_str()).collect();
    let mut classes = Vec::new();
    for arm in arms {
        let syn::Pat::Path(path) = &arm.pat else {
            anyhow::bail!("{ERRORS_SOURCE}: unexpected error_with_message arm pattern");
        };
        let variant = path.path.segments.last().expect("non-empty path").ident.to_string();
        let mut expr = &*arm.body;
        if let syn::Expr::Block(block) = expr {
            match block.block.stmts.as_slice() {
                [syn::Stmt::Expr(inner, None)] => expr = inner,
                _ => anyhow::bail!("{ERRORS_SOURCE}: arm `{variant}` is not a single expression"),
            }
        }
        let inner = match expr {
            syn::Expr::Path(p) if p.path.is_ident("None") => continue,
            syn::Expr::Call(call) if matches!(&*call.func, syn::Expr::Path(p) if p.path.is_ident("Some")) => {
                &call.args[0]
            },
            _ => anyhow::bail!("{ERRORS_SOURCE}: arm `{variant}` is neither `Some(..)` nor `None`"),
        };
        let name = screaming_snake(&variant);
        if !known.contains(name.as_str()) {
            anyhow::bail!("{ERRORS_SOURCE}: `Errors::{variant}` has no `kafka_common_ErrorCode_{name}` enumerator");
        }
        classes.push((name, inner.to_token_stream().to_string()));
    }
    Ok(classes)
}

/// The `use crate::common::…` imports of [`ERRORS_SOURCE`], re-rooted at
/// `confluent_kafka`, so the constructor expressions copied out of
/// `error_with_message` resolve the same way in the generated file. Only the
/// `common` imports are copied: the arms name error classes, which live there.
fn parse_error_class_imports() -> anyhow::Result<Vec<String>> {
    use quote::ToTokens as _;

    let file = syn::parse_file(&fs::read_to_string(ERRORS_SOURCE)?)?;
    Ok(file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Use(item) => Some(item.tree.to_token_stream().to_string().replace(' ', "")),
            _ => None,
        })
        .filter_map(|tree| {
            tree.strip_prefix("crate::common::")
                .map(|rest| format!("confluent_kafka::common::{rest}"))
        })
        .collect())
}

/// `UnknownTopicOrPartition` -> `UNKNOWN_TOPIC_OR_PARTITION`.
fn screaming_snake(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

fn generate_error_codes() -> anyhow::Result<()> {
    println!("🔧 Generating error-code constants from {ERROR_CODE_SOURCE}...");

    let codes = parse_error_codes()?;
    let classes = parse_error_classes(&codes)?;
    fs::write(ERROR_CODE_RS, error_codes_rust(&codes, &classes)?)?;

    println!("✅ Wrote {} constants to {ERROR_CODE_RS}", codes.len());
    Ok(())
}

/// Fail when a generated error-code file no longer matches the Rust enum.
fn check_error_codes_up_to_date() -> anyhow::Result<()> {
    println!("🔍 Checking generated error-code constants...");

    let codes = parse_error_codes()?;
    let classes = parse_error_classes(&codes)?;
    let expected = error_codes_rust(&codes, &classes)?;
    if !fs::read_to_string(ERROR_CODE_RS).is_ok_and(|actual| actual == expected) {
        eprintln!("\n❌ Stale generated error-code constants: {ERROR_CODE_RS}");
        eprintln!("   Run: cargo xtask generate-error-codes");
        exit(1);
    }

    println!("✅ Error-code constants are up to date ({} codes)", codes.len());
    Ok(())
}

fn lint() -> anyhow::Result<()> {
    // Runs first: it is a fast source-only scan, so a violation is reported
    // before paying for three full clippy passes. It covers what clippy cannot:
    // e.g. clippy's `exhaustive_enums` covers the enum, `lint-custom` the shape
    // of its variants and the visibility of public structs' fields.
    lint_custom::lint_custom()?;

    // Structural doc defects clippy cannot see: an item's attributes or doc
    // comment migrated onto a neighbour. Run first, because it is instant and its
    // failures are always real.
    doc_hygiene()?;

    // CLAUDE.md §2's import rule, in the one place the compiler cannot enforce it:
    // a file module stays visible inside its own subtree even when declared `mod x;`.
    module_path_hygiene()?;

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
    // the `lint-custom` scanner — would escape the lint gate entirely.
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
    // Delegated to the repository root's Makefile so the image-build steps
    // stay in one place; it shells out to python/Makefile and c/Makefile.
    run_command("make", &["-C", "..", "test-integration-python", "test-integration-c"])
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
        "confluent-kafka",
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

// ---------------------------------------------------------------------------
// Published-package check
// ---------------------------------------------------------------------------
//
// `cargo package` only compiles the unpacked crate on its own. This check goes
// one step further and uses the `.crate` the way a user does: it depends on it
// from a fresh project outside the workspace, resolving the dependencies anew,
// so a file missing from the `include` list in `Cargo.toml`, or a dependency
// that only resolves through the workspace lock file, fails here and not after
// a release. It also bounds the compressed size, so the package cannot grow
// back unnoticed (tests, the C FFI, generated output, ...).

const PACKAGE_NAME: &str = "confluent-kafka";
/// The largest `.crate` accepted, compressed: 3 MiB (crates.io allows 10 MiB).
const MAX_PACKAGE_BYTES: u64 = 3 * 1024 * 1024;
const PACKAGE_CHECK_DIR: &str = "target/package-check";

fn package_check() -> anyhow::Result<()> {
    println!("📦 Packaging {PACKAGE_NAME}...");
    // `--no-verify`: the install below builds the packaged crate anyway.
    // `--allow-dirty`: check the working tree as it is, so the task also runs
    // before a commit; CI works on a clean checkout.
    run_command("cargo", &["package", "-p", PACKAGE_NAME, "--no-verify", "--allow-dirty"])?;

    let version = package_version()?;
    let crate_file = PathBuf::from(format!("target/package/{PACKAGE_NAME}-{version}.crate"));
    let size = fs::metadata(&crate_file)?.len();
    println!(
        "   {}: {:.2} MiB (limit {:.2} MiB)",
        crate_file.display(),
        size as f64 / (1024.0 * 1024.0),
        MAX_PACKAGE_BYTES as f64 / (1024.0 * 1024.0)
    );
    if size > MAX_PACKAGE_BYTES {
        eprintln!(
            "❌ {} is {size} bytes, above the {MAX_PACKAGE_BYTES}-byte limit. \
             Check the `include` list in Cargo.toml for files that should not ship.",
            crate_file.display()
        );
        exit(1);
    }

    println!("📥 Installing the package in a fresh project...");
    let check_dir = PathBuf::from(PACKAGE_CHECK_DIR);
    if check_dir.exists() {
        fs::remove_dir_all(&check_dir)?;
    }
    let unpacked = check_dir.join("unpacked");
    fs::create_dir_all(&unpacked)?;
    run_command(
        "tar",
        &[
            "-xzf",
            &crate_file.display().to_string(),
            "-C",
            &unpacked.display().to_string(),
        ],
    )?;
    let package_dir = fs::canonicalize(unpacked.join(format!("{PACKAGE_NAME}-{version}")))?;

    let consumer = check_dir.join("consumer");
    fs::create_dir_all(consumer.join("src"))?;
    // The empty `[workspace]` makes the project its own workspace root, so the
    // enclosing `rust/` workspace neither claims it nor lends it its lock file.
    fs::write(
        consumer.join("Cargo.toml"),
        format!(
            "[package]\nname = \"package-check\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n\
             [workspace]\n\n[dependencies]\n{PACKAGE_NAME} = {{ path = {:?} }}\n",
            package_dir.display().to_string()
        ),
    )?;
    fs::write(
        consumer.join("src/main.rs"),
        "fn main() {\n    let error: Option<confluent_kafka::common::Error> = None;\n    assert!(error.is_none());\n}\n",
    )?;
    let manifest = consumer.join("Cargo.toml").display().to_string();
    let status = Command::new("cargo")
        .args(["build", "--manifest-path", &manifest])
        .env("CARGO_TARGET_DIR", check_dir.join("target"))
        .status()?;
    if !status.success() {
        eprintln!("❌ A project depending on the packaged {PACKAGE_NAME} does not build");
        exit(1);
    }

    println!("✅ {PACKAGE_NAME} {version} packages within the size limit and builds as a dependency");
    Ok(())
}

/// The version of [`PACKAGE_NAME`], from `cargo pkgid`
/// (`path+file:///…/rust#confluent-kafka@0.1.0`, or `…#0.1.0` when the
/// directory is named after the package).
fn package_version() -> anyhow::Result<String> {
    let output = Command::new("cargo").args(["pkgid", "-p", PACKAGE_NAME]).output()?;
    if !output.status.success() {
        anyhow::bail!("cargo pkgid -p {PACKAGE_NAME} failed");
    }
    let pkgid = String::from_utf8(output.stdout)?;
    let fragment = pkgid.trim().rsplit('#').next().unwrap_or_default();
    Ok(fragment.rsplit('@').next().unwrap_or(fragment).to_string())
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
/// CLAUDE.md §8 puts repeatable checks in xtask rather than in shell scripts, and
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

/// CLAUDE.md §2: a translated Java class is imported through its parent module's
/// re-export (`crate::producer::ProducerRecord`), never through the file module that
/// holds it (the same type spelled with a `producer_record` segment in between). The
/// file module path is reserved for Java *nested* types, e.g. `ConfigSource` reached
/// through `config_entry`.
///
/// Most of that rule is enforced by the compiler, because the file modules are
/// declared `mod x;` — but a private module stays visible to the declaring module and
/// all of its descendants, so a sibling reaching through `super::<file module>::<Type>`
/// still compiles. This check covers exactly that blind spot.
///
/// It flags a `<file module>::<Name>` segment pair only when the file module's own
/// parent re-exports that exact `Name`, and only when the pair is reached through a
/// longer path (preceded by `::`). Both conditions matter: the first leaves Java nested
/// types alone (`common::network` re-exports `ChannelState` but not the nested `State`,
/// so only the former is a violation), and the second leaves the defining
/// `pub use` line in the parent's own `mod.rs` alone.
///
/// A file module whose name is also a directory module's name is skipped, because the
/// pair is keyed on the bare segment rather than the full path: the three
/// module-inception files (`metrics`, `utils`, `resource`) sit inside a directory of
/// the same name, so `common::metrics::Metrics` — the *correct* path — is
/// indistinguishable from a violation by segment name alone.
fn module_path_hygiene() -> anyhow::Result<()> {
    println!("🔍 Checking module-path hygiene...");

    // (file module segment, re-exported name) for every hand-written parent module.
    let mut reexports: Vec<(String, String)> = Vec::new();
    let mut directory_modules: Vec<String> = Vec::new();
    for path in rust_sources("src")? {
        if path.file_name().and_then(|n| n.to_str()) == Some("mod.rs") {
            if let Some(name) = path.parent().and_then(|d| d.file_name()).and_then(|n| n.to_str()) {
                directory_modules.push(name.to_string());
            }
        }
    }
    for path in rust_sources("src")? {
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if file_name != "mod.rs" && file_name != "lib.rs" {
            continue;
        }
        let dir = path.parent().unwrap_or(&path);
        let text = fs::read_to_string(&path)?;
        for body in use_statements(&text) {
            // Only a re-export of a *direct child file module* counts: `<seg>::<Name>`
            // or `self::<seg>::<Name>`, with `<seg>.rs` sitting next to this `mod.rs`.
            let body = body.trim_start_matches("self::");
            let Some((segment, tail)) = body.split_once("::") else {
                continue;
            };
            if segment.contains(' ') || !dir.join(format!("{segment}.rs")).is_file() {
                continue;
            }
            if directory_modules.iter().any(|name| name == segment) {
                continue;
            }
            for name in brace_list(tail) {
                // `pub use x::y as z;` re-exports under `z`; the path a caller must not
                // write is still `x::y`, so key on the original name.
                let original = name.split(" as ").next().unwrap_or(&name).trim().to_string();
                if !original.is_empty() && original != "*" && original != "self" {
                    reexports.push((segment.to_string(), original));
                }
            }
        }
    }
    reexports.sort();
    reexports.dedup();

    // A path is only ours to judge if its first segment names this crate. Without
    // that guard a file module sharing a name with a std one — `common/error.rs`
    // against `std::error` — matches every `std::error::Error` in the repo.
    let mut crate_roots: Vec<String> = ["crate", "self", "super", "confluent_kafka"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    crate_roots.extend(directory_modules.iter().cloned());
    crate_roots.sort();
    crate_roots.dedup();

    let mut findings: Vec<String> = Vec::new();
    for root in ["src", "tests", "examples", "consumer-perf/src"] {
        if !PathBuf::from(root).is_dir() {
            continue;
        }
        for path in rust_sources(root)? {
            let text = fs::read_to_string(&path)?;
            for (number, line) in text.lines().enumerate() {
                for (segment, name) in &reexports {
                    let needle = format!("::{segment}::{name}");
                    let mut from = 0;
                    while let Some(at) = line[from..].find(&needle) {
                        let start = from + at;
                        let end = start + needle.len();
                        from = start + 1;
                        // Reject a longer identifier on either side (`::topic::TopicX`).
                        if line[end..].starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                            continue;
                        }
                        let head: String = line[..start]
                            .chars()
                            .rev()
                            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect();
                        let root = head.trim_start_matches(':').split("::").next().unwrap_or_default();
                        if !crate_roots.iter().any(|allowed| allowed == root) {
                            continue;
                        }
                        findings.push(format!(
                            "{}:{} reaches `{name}` through the file module `{segment}` — \
                             import it from the parent re-export instead (CLAUDE.md §2)",
                            path.display(),
                            number + 1
                        ));
                    }
                }
            }
        }
    }

    if findings.is_empty() {
        println!("✅ Module-path hygiene clean!");
        return Ok(());
    }
    for finding in &findings {
        eprintln!("  {finding}");
    }
    Err(anyhow::anyhow!("{} module-path finding(s)", findings.len()))
}

/// The body of every `use` statement in `text`, whitespace-collapsed and with the
/// leading visibility/keyword and trailing `;` stripped. Multi-line statements are
/// joined, so a braced list arrives as one string.
fn use_statements(text: &str) -> Vec<String> {
    let mut bodies = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("use ") {
        let before_is_boundary = rest[..at].ends_with('\n')
            || rest[..at].trim_end().ends_with("pub")
            || rest[..at].trim_end().ends_with("(crate)")
            || rest[..at].chars().rev().take_while(|c| *c == ' ').count() == at;
        let after = &rest[at + 4..];
        let Some(end) = after.find(';') else { break };
        if before_is_boundary {
            bodies.push(after[..end].split_whitespace().collect::<Vec<_>>().join(" "));
        }
        rest = &after[end + 1..];
    }
    bodies
}

/// `Name` -> `["Name"]`; `{A, B as C}` -> `["A", "B as C"]`. Nested braces are skipped,
/// since this check only needs the simple shapes the repo's `mod.rs` files use.
fn brace_list(tail: &str) -> Vec<String> {
    let tail = tail.trim();
    let Some(inner) = tail.strip_prefix('{').and_then(|t| t.strip_suffix('}')) else {
        return vec![tail.to_string()];
    };
    if inner.contains('{') {
        return Vec::new();
    }
    inner
        .split(',')
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty())
        .collect()
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

/// Writes [`java::DEPRECATED_LIST`]: the union of the deprecated items of each
/// ref given (default [`java::DEPRECATION_REFS`]).
fn java_deprecated() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().skip(2).collect();
    let refs: Vec<&str> = if args.is_empty() {
        java::DEPRECATION_REFS.to_vec()
    } else {
        args.iter().map(String::as_str).collect()
    };
    let mut items = std::collections::BTreeSet::new();
    for reference in &refs {
        let classes = java::load_ref(reference).ok_or_else(|| {
            anyhow::anyhow!("ref `{reference}` is not in the `kafka` submodule (run `git -C ../kafka fetch --tags`)")
        })?;
        items.extend(java::deprecated_items(&classes));
    }
    let mut out = format!(
        "# Java client API marked @Deprecated in Apache Kafka {}.\n\
         # Generated by `cargo xtask java-deprecated`; read by `cargo xtask lint-custom`.\n\
         # CLAUDE.md §3: deprecated API MUST NOT be translated.\n\
         # A class covers its members and nested classes; `#name(T1,T2)` is one overload, `#NAME` a field.\n",
        refs.join(" + ")
    );
    for item in &items {
        out.push_str(item);
        out.push('\n');
    }
    fs::write(java::DEPRECATED_LIST, out)?;
    println!("✅ Wrote {} deprecated items to {}", items.len(), java::DEPRECATED_LIST);
    Ok(())
}

/// Fetches the tags of [`java::lint_refs`] into the `kafka` submodule, one
/// commit deep each, so a shallow clone (CI's) can run `lint-custom`, which
/// reads those refs and fails without them.
fn fetch_java_refs() -> anyhow::Result<()> {
    let refs = java::lint_refs();
    let mut args = vec!["-C", java::KAFKA_SUBMODULE, "fetch", "--depth=1", "--no-tags", "origin"];
    let specs: Vec<String> = refs.iter().map(|r| format!("+refs/tags/{r}:refs/tags/{r}")).collect();
    args.extend(specs.iter().map(String::as_str));
    let status = Command::new("git").args(&args).status()?;
    if !status.success() {
        anyhow::bail!("fetching {} into `{}` failed", refs.join(", "), java::KAFKA_SUBMODULE);
    }
    println!("✅ Fetched {} into `{}`", refs.join(", "), java::KAFKA_SUBMODULE);
    Ok(())
}

fn print_help() {
    eprintln!(
        "Tasks:
  format          Format all Rust code including generated files
  format-check    Check if code is formatted correctly
  check-generated Check message-spec drift, generated code formatting and error-code staleness (no changes)
  generate-error-codes  Regenerate the error-code constants for the multilanguage test harness
  java-deprecated List the Java client's @Deprecated API in ../design/current/java-deprecated.txt
  fetch-java-refs Fetch the Kafka tags lint-custom reads into the kafka submodule
  lint-custom     Run the source-level rules clippy cannot express
                  (also runs as the first step of `lint`):
                    check-no-data-carrying-enum-variants  public enum variants hold no data inline
                    check-no-public-field                 public structs have no `pub` field
  lint            Run doc-hygiene plus clippy lints (warnings are errors)
  doc-hygiene     Check for migrated attributes and stacked doc blocks
  lint-fix        Run clippy and automatically fix what it can
  coverage        Run unit test coverage (HTML report)
  coverage-lcov   Run unit test coverage (lcov for CI)
  coverage-all    Run all test coverage including integration (requires Docker)
  test-multilanguage  Run producer integration tests against rust/python/c backends (requires Docker)
  producer-perf-test  Run the env-driven producer performance benchmark (requires Docker or BOOTSTRAP_SERVERS)
  package-check   Package the crate, fail above 3 MiB, and build a fresh project depending on it

Usage:
  cargo xtask format
  cargo xtask format-check
  cargo xtask check-generated
  cargo xtask generate-error-codes
  cargo xtask java-deprecated [kafka-ref ...]
  cargo xtask lint-custom
  cargo xtask lint
  cargo xtask doc-hygiene
  cargo xtask lint-fix
  cargo xtask coverage
  cargo xtask coverage-lcov
  cargo xtask coverage-all
  cargo xtask test-multilanguage
  cargo xtask producer-perf-test
  cargo xtask package-check"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs(files: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
        files
            .iter()
            .map(|&(name, body)| (name.to_string(), body.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn matching_specs_have_no_drift() {
        let both = specs(&[("A.json", "{}"), ("B.json", "{ \"apiKey\": 1 }")]);
        assert!(message_spec_drift(&both, &both).is_empty());
    }

    #[test]
    fn reports_every_kind_of_drift_in_name_order() {
        let copy = specs(&[("A.json", "{}"), ("B.json", "{ \"edited\": true }"), ("D.json", "{}")]);
        let kafka = specs(&[("A.json", "{}"), ("B.json", "{}"), ("C.json", "{}")]);
        assert_eq!(
            message_spec_drift(&copy, &kafka),
            [
                "B.json: differs",
                "C.json: only in the kafka submodule",
                "D.json: only in generator/messages/"
            ]
        );
    }

    #[test]
    fn reads_only_json_files() {
        let dir = std::env::temp_dir().join(format!("xtask-message-specs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("nested.json")).unwrap();
        fs::write(dir.join("A.json"), "{}").unwrap();
        fs::write(dir.join("README.md"), "# Messages").unwrap();
        let read = read_message_specs(&dir);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(read.unwrap(), specs(&[("A.json", "{}")]));
    }
}
