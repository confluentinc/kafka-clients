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
        Some("chaos") => chaos()?,
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

/// Runs the flag-driven chaos / fault-injection runner (producer + KIP-848
/// consumer), the single-command entry point at parity with librdkafka's
/// `chaos.py`.
///
/// It stands up a dedicated Docker cluster and stops/kills/restarts its
/// brokers, so it is slow, destructive, and excluded from the default sweep
/// (`test = false` + `#[ignore]`). CLI flags are translated to `CHAOS_*`
/// environment variables and forwarded to the `chaos_run` test (the same
/// env-driven pattern the producer perf test uses).
///
/// Flags (defaults mirror librdkafka's chaos.py where they overlap):
///   --brokers N            broker count (3)
///   --num-topics N         number of test topics (1). With >1, --topic is the
///                          prefix and topics are named <topic>_0.._N-1; one
///                          producer runs per topic (each at --rps, aggregate =
///                          N*rps) and consumers subscribe to all topics
///   --partitions N         partitions per topic (6)
///   --replication-factor N replication factor per topic (default min(brokers,3);
///                          FATAL if > brokers)
///   --msg-size N           producer value payload size in bytes (100)
///   --cycles N             chaos cycles (3)
///   (broker rolling is the default fault; layer more faults on with the flags
///    below, each taking an OPTIONAL cadence N = every N cycles, else every cycle)
///   --no-broker-roll       disable the implicit broker roll (e.g. pure migration)
///   --topic-recreate [N]      also delete/recreate the topic
///   --reassign-partitions [N] also reassign partitions (data moves)
///   --change-leader [N]       also do a preferred-leader change (no data move)
///   --unclean              SIGKILL instead of SIGTERM for broker roll
///   --workload role:backend  repeatable; role=producer|consumer,
///                            backend=rust|python|python-async|c
///                            (default: producer:rust,consumer:rust)
///   --consumers N          shorthand for 1 rust producer + N rust consumers
///                          (librdkafka's --consumers; not usable with --workload)
///   --consumer-churn-min M      consumer churn: keep the live consumer count in
///   --consumer-churn-max X      [M, X]; each cycle stop a random batch then start
///                               a random batch (librdkafka chaos churn). Both
///                               required together; owns the consumer set (not
///                               usable with --consumers/--workload)
///   --rps N                producer target records/sec, 0 = max (200)
///   --stop-s N             seconds a broker stays down per roll (5)
///   --drain-s N            drain window at the end (15)
///   --dwell-s N            delete->recreate dwell for topic-recreate (0)
///   --leave-broker-down N  keep broker N down for the whole run
///   --random               chaos-monkey mode: each cycle the seeded RNG picks
///                          whether/which fault fires (broker-roll, recreate,
///                          reassign, change-leader — all candidates) AND its
///                          parameters (broker, clean/unclean, down, dwell) and
///                          the timing. Ignores the fixed cadences.
///   --action-prob P        --random: per-cycle probability a fault fires (0.7)
///   --seed N               reproducibility seed (0 = auto-pick & print). Drives
///                          the broker-roll order and, with --random, the ENTIRE
///                          run; rerun with the printed seed to reproduce it
///   --commit sync|async    consumer commit mode (sync)
///   --topic NAME           topic name (chaos-run)
///   --rebalance-add-cycle N     add a consumer at cycle N (rebalance chaos)
///   --rebalance-remove-cycle N  remove that consumer at cycle N
///   --rebalance-mid-roll        fire the add/remove INSIDE the broker-roll
///                               down-window (rebalance overlaps leader
///                               migration in time); needs a roll that cycle
///   --reports              write target/chaos-runs/<id>/ (verdict, leader
///                          changes, per-workload client logs, summary)
///   --log-budget-mb N      per-workload client-log rotation budget (64)
///   --repeat N             run up to N times, stop on first failure, append
///                          target/chaos-runs/run-history.tsv (until-fail loop)
///
/// A bare `--scenario NAME` instead runs the named `#[ignore]` smoke test
/// (e.g. `--scenario simple_flow_clean_broker_roll`).
///
/// See design/current/chaos-fault-injection-harness.md and chaos-parity-gap.md.
fn chaos() -> anyhow::Result<()> {
    println!("🔥 Running chaos / fault-injection runner (Docker required)...");
    println!("   Slow and destructive to its own cluster; not part of `cargo test`.");

    let raw: Vec<String> = env::args().skip(2).collect();

    // `--scenario NAME`: run that named smoke test instead of the generic
    // runner; forward remaining args straight through to libtest.
    if let Some(pos) = raw.iter().position(|a| a == "--scenario") {
        let name = raw.get(pos + 1).cloned().unwrap_or_default();
        if name.is_empty() {
            anyhow::bail!("--scenario requires a test name");
        }
        let mut args: Vec<String> = vec![
            "test".into(),
            "--features".into(),
            "integration-tests".into(),
            "--test".into(),
            "chaos".into(),
        ];
        // any flags before/after --scenario NAME (besides the pair) pass through
        for (i, a) in raw.iter().enumerate() {
            if i == pos || i == pos + 1 {
                continue;
            }
            args.push(a.clone());
        }
        args.push("--".into());
        args.push("--ignored".into());
        args.push("--nocapture".into());
        args.push("--exact".into());
        args.push(name);
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        return run_command("cargo", &arg_refs);
    }

    // `--repeat N` is an xtask-level loop control (the chaos_until_fail.sh
    // analog), not a CHAOS_* flag — pull it out before parsing the rest.
    let mut raw = raw;
    let repeat = take_repeat(&mut raw)?;

    // Otherwise parse the chaos flags into CHAOS_* env vars for `chaos_run`.
    let env_vars = parse_chaos_flags(&raw)?;
    for (k, v) in &env_vars {
        // SAFETY: single-threaded xtask main before any threads are spawned.
        unsafe { env::set_var(k, v) };
    }

    // A python/c workload backend needs the gRPC bridge, which only compiles
    // under `multilanguage-tests`. Auto-select the wider feature when such a
    // workload is requested so the user does not have to.
    let needs_grpc = env_vars
        .iter()
        .find(|(k, _)| k == "CHAOS_WORKLOADS")
        .map(|(_, v)| v.contains(":python") || v.contains(":c"))
        .unwrap_or(false);
    let feature = if needs_grpc {
        "multilanguage-tests"
    } else {
        "integration-tests"
    };
    if needs_grpc {
        println!("   python/c workload requested → building with `--features multilanguage-tests`");
    }

    let args: Vec<&str> = vec![
        "test",
        "--features",
        feature,
        "--test",
        "chaos",
        "--",
        "--ignored",
        "--nocapture",
        "--exact",
        "run_test::chaos_run",
    ];

    // Run once, or loop until failure / `repeat` iterations (the
    // chaos_until_fail.sh analog), appending a TSV history line per iteration.
    let history = "target/chaos-runs/run-history.tsv";
    for iter in 1..=repeat {
        if repeat > 1 {
            println!("🔁 chaos iteration {iter}/{repeat}");
        }
        let start = std::time::Instant::now();
        let result = run_command("cargo", &args);
        let secs = start.elapsed().as_secs();
        let verdict = if result.is_ok() { "PASS" } else { "FAIL" };
        append_history(history, iter, verdict, secs);
        if let Err(e) = result {
            eprintln!("❌ chaos iteration {iter} FAILED after {secs}s — stopping loop (history: {history})");
            return Err(e);
        }
    }
    if repeat > 1 {
        println!("✅ all {repeat} chaos iterations passed (history: {history})");
    }
    Ok(())
}

/// Extract `--repeat N` from the raw args, returning N (default 1). The flag
/// and its value are removed so the remaining args parse as chaos flags.
fn take_repeat(raw: &mut Vec<String>) -> anyhow::Result<u32> {
    if let Some(pos) = raw.iter().position(|a| a == "--repeat") {
        let val = raw.get(pos + 1).ok_or_else(|| anyhow::anyhow!("--repeat requires a count"))?;
        let n: u32 = val
            .parse()
            .map_err(|_| anyhow::anyhow!("--repeat must be a positive integer"))?;
        anyhow::ensure!(n >= 1, "--repeat must be >= 1");
        raw.drain(pos..=pos + 1);
        Ok(n)
    } else {
        Ok(1)
    }
}

/// Append one tab-separated line to the chaos run history (iso-time, iteration,
/// verdict, seconds) — the run-history.tsv analog. Best-effort.
fn append_history(path: &str, iter: u32, verdict: &str, secs: u64) {
    use std::io::Write as _;
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = writeln!(f, "{ts}\t{iter}\t{verdict}\t{secs}");
    }
}

/// Translate `--flag value` / `--bool-flag` chaos flags into the `CHAOS_*`
/// environment variables `ChaosConfig::from_env` reads. `--workload` is
/// repeatable and accumulates into a comma-separated `CHAOS_WORKLOADS`.
fn parse_chaos_flags(raw: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut workloads: Vec<String> = Vec::new();
    let mut i = 0;

    // Fault flags with an OPTIONAL cadence argument (present = fire every cycle;
    // `N` = every N cycles). Broker rolling is implicit/default-on and toggled
    // off with `--no-broker-roll`, so it is not in this table.
    let fault: &[(&str, &str)] = &[
        ("--topic-recreate", "CHAOS_TOPIC_RECREATE"),
        ("--reassign-partitions", "CHAOS_REASSIGN_PARTITIONS"),
        ("--change-leader", "CHAOS_CHANGE_LEADER"),
    ];

    // (flag, env-var) pairs that take a value.
    let valued: &[(&str, &str)] = &[
        ("--brokers", "CHAOS_BROKERS"),
        ("--num-topics", "CHAOS_NUM_TOPICS"),
        ("--partitions", "CHAOS_PARTITIONS"),
        ("--replication-factor", "CHAOS_REPLICATION_FACTOR"),
        ("--msg-size", "CHAOS_MSG_SIZE"),
        ("--cycles", "CHAOS_CYCLES"),
        ("--rps", "CHAOS_RPS"),
        ("--stop-s", "CHAOS_STOP_S"),
        ("--up-wait-s", "CHAOS_UP_WAIT_S"),
        ("--warmup-s", "CHAOS_WARMUP_S"),
        ("--between-s", "CHAOS_BETWEEN_S"),
        ("--drain-s", "CHAOS_DRAIN_S"),
        ("--idle-threshold-s", "CHAOS_IDLE_THRESHOLD_S"),
        ("--leave-broker-down", "CHAOS_LEAVE_BROKER_DOWN"),
        ("--seed", "CHAOS_SEED"),
        ("--action-prob", "CHAOS_ACTION_PROB"),
        ("--dwell-s", "CHAOS_DWELL_S"),
        ("--rebalance-add-cycle", "CHAOS_REBALANCE_ADD_CYCLE"),
        ("--rebalance-remove-cycle", "CHAOS_REBALANCE_REMOVE_CYCLE"),
        ("--log-budget-mb", "CHAOS_LOG_BUDGET_MB"),
        ("--commit", "CHAOS_COMMIT"),
        ("--topic", "CHAOS_TOPIC"),
        ("--consumers", "CHAOS_CONSUMERS"),
        ("--consumer-churn-min", "CHAOS_CONSUMER_CHURN_MIN"),
        ("--consumer-churn-max", "CHAOS_CONSUMER_CHURN_MAX"),
    ];

    while i < raw.len() {
        let arg = raw[i].as_str();
        if arg == "--unclean" {
            out.push(("CHAOS_UNCLEAN".to_string(), "1".to_string()));
            i += 1;
        } else if arg == "--reports" {
            out.push(("CHAOS_REPORTS".to_string(), "1".to_string()));
            i += 1;
        } else if arg == "--workload" {
            let v = raw
                .get(i + 1)
                .ok_or_else(|| anyhow::anyhow!("--workload requires role:backend"))?;
            workloads.push(v.clone());
            i += 2;
        } else if arg == "--no-broker-roll" {
            out.push(("CHAOS_NO_BROKER_ROLL".to_string(), "1".to_string()));
            i += 1;
        } else if arg == "--rebalance-mid-roll" {
            out.push(("CHAOS_REBALANCE_MID_ROLL".to_string(), "1".to_string()));
            i += 1;
        } else if arg == "--random" {
            out.push(("CHAOS_RANDOM".to_string(), "1".to_string()));
            i += 1;
        } else if let Some((_, envk)) = fault.iter().find(|(f, _)| *f == arg) {
            // Fault flag with an OPTIONAL cadence: `--topic-recreate` (every
            // cycle) or `--topic-recreate 2` (every 2 cycles). The next token is
            // the cadence only if it is a bare number; otherwise the flag is
            // bare and the token belongs to the next flag.
            let cadence = match raw.get(i + 1) {
                Some(v) if v.parse::<u32>().is_ok() => {
                    i += 2;
                    v.clone()
                },
                _ => {
                    i += 1;
                    "1".to_string()
                },
            };
            out.push((envk.to_string(), cadence));
        } else if let Some((_, envk)) = valued.iter().find(|(f, _)| *f == arg) {
            let v = raw.get(i + 1).ok_or_else(|| anyhow::anyhow!("{arg} requires a value"))?;
            out.push((envk.to_string(), v.clone()));
            i += 2;
        } else {
            anyhow::bail!("unknown chaos flag: {arg} (see `cargo xtask` help)");
        }
    }

    if !workloads.is_empty() {
        out.push(("CHAOS_WORKLOADS".to_string(), workloads.join(",")));
    }
    Ok(out)
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
  chaos           Flag-driven chaos / fault-injection runner for producer + consumer (requires Docker)
                    e.g. cargo xtask chaos --brokers 3 --cycles 3 --unclean \
                             --workload producer:rust --workload consumer:rust
                    --scenario NAME runs a named #[ignore] smoke test instead

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
  cargo xtask producer-perf-test
  cargo xtask chaos"
    );
}
