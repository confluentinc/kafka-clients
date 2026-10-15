// Copyright 2026 Confluent Inc.
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

//! `cargo xtask perf-cloud`: runs the producer performance harnesses against
//! a remote SASL_SSL (PLAIN) cluster, such as a Confluent Cloud cluster, and
//! compares the clients. Whatever provisions the cluster (a CI pipeline, an
//! operator) calls it with the cluster's bootstrap and credentials.
//!
//! A run compares one **client group** across a list of tests from
//! [`TESTS_FILE`]. Each test runs every arm of the group, one after another,
//! against the same cluster:
//!
//! - `rust`: Rust (native), librdkafka (C) and Java. Rust is compared with
//!   both of the others.
//! - `python`: the Rust-backed and the librdkafka-backed Python clients, sync
//!   and async. Each Rust-backed client is compared with its librdkafka
//!   equivalent (sync with sync, async with async).
//!
//! Subcommands:
//!
//! - `list` prints the tests file.
//! - `check` validates a run (group, test list, durations) and prints its
//!   time estimate, failing above `--budget-min`. It needs no cluster, so a
//!   caller can run it before provisioning one: a typo or an over-long list
//!   then costs seconds, not a provisioning cycle.
//! - `build` builds the group's harnesses once.
//! - `run` creates each arm's topic, runs the arm, collects its
//!   `results.json` and deletes the topic, then writes `report.md` (a table
//!   per test with the changes against each baseline) and `junit.xml`.
//!
//! The arms are the existing producer harnesses, driven by the same
//! environment variables `tools/deploy_and_run_perf/deploy_and_run_perf.py`
//! sets on EC2. Topics are created here, never by a harness
//! (`CREATE_TOPIC=False`): the Rust harness cannot create topics, and one
//! creation path gives every arm the same topic.
//!
//! The SASL credentials come from `PERF_API_KEY` / `PERF_API_SECRET`, never
//! from the command line, and the secret is masked in everything this driver
//! prints or writes (harness output included), because the results directory
//! is meant to be published (e.g. as CI artifacts).
//!
//! Optional run metadata, recorded in the report when set: `AGENT_REGION`,
//! `AGENT_AZ_ID`, `PERF_CLUSTER_ID`, `PERF_CLUSTER_SIZE` (free text, e.g.
//! `10 CKU`), `PERF_CLUSTER_INGRESS_MB_S` (also enables the "near the
//! ingress limit" notes), and on Semaphore `SEMAPHORE_AGENT_MACHINE_TYPE` and
//! `SEMAPHORE_WORKFLOW_ID`. The commit is this repository's `HEAD`.

mod json;
mod report;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead as _, BufReader, Read, Write as _};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};

use crate::chaos_matrix::{group_running, now_utc, package_root, signal_group, stop_group};

/// The tests that can be named in a run, relative to the repository root.
const TESTS_FILE: &str = "tools/perf-cloud/tests.txt";

/// Setup time per arm on top of warmup and measurement: topic creation and
/// deletion, client start-up, the harnesses' 10 s cooldown sampling.
const ARM_SETUP_MINUTES: u64 = 2;

/// The longest measured window and warmup a run may ask for.
const MAX_DURATION_MINUTES: u64 = 60;
const MAX_WARMUP_MINUTES: u64 = 30;

/// Above this share of the cluster's ingress a fixed-rate test risks being
/// throttled.
const INGRESS_WARN_SHARE: f64 = 0.8;

/// How long an arm may run past its warmup + measurement before it is
/// stopped as hung.
const ARM_TIMEOUT_SLACK: Duration = Duration::from_secs(20 * 60);

/// Grace between SIGINT and SIGKILL when an arm is stopped (short in tests,
/// whose `sh` background jobs ignore SIGINT and always need the SIGKILL).
const STOP_GRACE: Duration = Duration::from_secs(if cfg!(test) { 2 } else { 30 });

/// Each `kafka-topics.sh` call.
const TOPIC_COMMAND_TIMEOUT: Duration = Duration::from_secs(180);

/// Topic creation retries: a new API key can take a few minutes before the
/// cluster accepts it.
const TOPIC_CREATE_ATTEMPTS: u32 = 8;
const TOPIC_CREATE_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Every environment variable a producer harness reads, its A/B debugging
/// knobs included. Each arm starts from the job's environment with all of
/// them removed, so a stray value in the job cannot change one arm and not
/// the others.
const HARNESS_VARS: &[&str] = &[
    "ASYNC",
    "BATCH_SIZE",
    "BOOTSTRAP_SERVERS",
    "BUFFER_MEMORY",
    "CLIENT_VERSION",
    "COMPRESSION_TYPE",
    "CREATE_TOPIC",
    "DO_VERIFY",
    "ENABLE_IDEMPOTENCE",
    "KEY_SIZE",
    "LIBRDKAFKA_PYTHON",
    "LIMIT_RPS",
    "LINGER_MS",
    "MAX_IN_FLIGHT",
    "MAX_REQUEST_SIZE",
    "METRICS_FILE",
    "NO_MSG_F_BLOCK",
    "NUM_MESSAGES",
    "P99_LIMIT_MS",
    "PARTITIONER",
    "PARTITIONS",
    "PERF_DEBUG_TIMING",
    "PERF_LRK_STATS",
    "RESULTS_FILE",
    "SASL_MECHANISM",
    "SASL_PASSWORD",
    "SASL_USERNAME",
    "SECURITY_PROTOCOL",
    "SKIP_PARTITIONER_OVERRIDE",
    "SSL_CA_LOCATION",
    "STICKY_LINGER_MS",
    "TEST_DURATION_SECONDS",
    "TOPIC_NAME",
    "USE_DEFAULTS",
    "VALUE_SIZE",
    "VERIFY_CONSUMED",
    "WARMUP_SECONDS",
];

pub fn run(raw: &[String]) -> anyhow::Result<()> {
    let Some((sub, rest)) = raw.split_first() else {
        println!("{HELP}");
        return Ok(());
    };
    match sub.as_str() {
        "list" => list(),
        "check" => {
            let options = RunOptions::parse(rest, false)?;
            check(&options)
        },
        "build" => {
            let flags = parse_flags(rest, &["--group"], &[])?;
            build(Group::parse(required(&flags, "--group")?)?)
        },
        "run" => {
            let options = RunOptions::parse(rest, true)?;
            run_tests(&options)
        },
        "help" | "--help" | "-h" => {
            println!("{HELP}");
            Ok(())
        },
        other => bail!("unknown perf-cloud subcommand `{other}`\n\n{HELP}"),
    }
}

const HELP: &str = "cargo xtask perf-cloud <subcommand>

  list                     Print the tests that can be run (tools/perf-cloud/tests.txt)
  check  --group G --tests ID,ID [--duration-min N] [--warmup-min N]
         [--budget-min N] [--ingress-mb-s N]
                           Validate a run and print its time estimate; fail when
                           it exceeds --budget-min, warn about fixed-rate tests
                           near the cluster's --ingress-mb-s
  build  --group G         Build the group's harnesses
  run    --group G --tests ID,ID [--duration-min N] [--warmup-min N]
         --bootstrap HOST:PORT --kafka-bin DIR --results-dir DIR
         [--topic-prefix P] [--push-artifacts]
                           (--push-artifacts: push each finished test's results
                           with Semaphore's `artifact` CLI)
                           Run the tests (credentials from PERF_API_KEY / PERF_API_SECRET)

  G is `rust` (Rust, librdkafka, Java) or `python` (Rust- and librdkafka-backed
  Python clients, sync and async). Durations are minutes: --duration-min
  defaults to 30 (at most 60), --warmup-min to 5 (at most 30).";

// ---------------------------------------------------------------------------
// Client groups and arms
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Group {
    Rust,
    Python,
}

impl Group {
    fn parse(value: &str) -> anyhow::Result<Group> {
        match value {
            "rust" => Ok(Group::Rust),
            "python" => Ok(Group::Python),
            other => bail!("unknown client group `{other}` (expected `rust` or `python`)"),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Group::Rust => "rust",
            Group::Python => "python",
        }
    }

    /// The arms, in the order they run within a test.
    pub(crate) fn arms(self) -> &'static [Arm] {
        match self {
            Group::Rust => &[Arm::Rust, Arm::Librdkafka, Arm::Java],
            Group::Python => &[
                Arm::PythonSyncRust,
                Arm::PythonSyncLibrdkafka,
                Arm::PythonAsyncRust,
                Arm::PythonAsyncLibrdkafka,
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arm {
    Rust,
    Librdkafka,
    Java,
    PythonSyncRust,
    PythonSyncLibrdkafka,
    PythonAsyncRust,
    PythonAsyncLibrdkafka,
}

impl Arm {
    /// Directory and topic name component.
    pub(crate) fn id(self) -> &'static str {
        match self {
            Arm::Rust => "rust",
            Arm::Librdkafka => "librdkafka",
            Arm::Java => "java",
            Arm::PythonSyncRust => "python-sync-rust",
            Arm::PythonSyncLibrdkafka => "python-sync-librdkafka",
            Arm::PythonAsyncRust => "python-async-rust",
            Arm::PythonAsyncLibrdkafka => "python-async-librdkafka",
        }
    }

    /// The row name in the report.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Arm::Rust => "Rust (native)",
            Arm::Librdkafka => "librdkafka (C)",
            Arm::Java => "Java",
            Arm::PythonSyncRust => "python-sync (Rust)",
            Arm::PythonSyncLibrdkafka => "python-sync (librdkafka)",
            Arm::PythonAsyncRust => "python-async (Rust)",
            Arm::PythonAsyncLibrdkafka => "python-async (librdkafka)",
        }
    }

    /// The `client` its harness writes into `results.json`, checked so a
    /// wrong `CLIENT_VERSION` / `ASYNC` cannot pass as the arm.
    fn expected_client(self) -> &'static str {
        match self {
            Arm::Rust => "rust",
            Arm::Librdkafka => "librdkafka",
            Arm::Java => "java",
            Arm::PythonSyncRust => "python-rust",
            Arm::PythonSyncLibrdkafka => "python-librdkafka",
            Arm::PythonAsyncRust => "python-rust-async",
            Arm::PythonAsyncLibrdkafka => "python-librdkafka-async",
        }
    }

    /// The command that runs this arm's harness (the `RUN_PERF_SH` recipes
    /// of `deploy_and_run_perf.py`), to be started in the arm's directory.
    fn command(self, repo: &Path) -> Command {
        match self {
            Arm::Rust => {
                let mut c = Command::new(cargo());
                c.args(["xtask", "producer-perf-test", "--test-threads=1"])
                    .current_dir(repo.join("rust"));
                c
            },
            Arm::Librdkafka => Command::new(c_harness(repo)),
            Arm::Java => {
                let mut c = Command::new("java");
                c.arg("-jar").arg(java_jar(repo));
                c
            },
            Arm::PythonSyncRust | Arm::PythonAsyncRust => {
                let mut c = Command::new(venv_python(repo, "venv"));
                c.arg(python_harness(repo));
                c
            },
            Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka => {
                let mut c = Command::new(venv_python(repo, "venv-librdkafka"));
                c.arg(python_harness(repo));
                c
            },
        }
    }

    /// The settings this arm's harness needs on top of the shared ones,
    /// spelled the way that harness parses them.
    fn own_env(self, repo: &Path) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = Vec::new();
        let mut set = |k: &str, v: &str| env.push((k.to_string(), v.to_string()));
        match self {
            Arm::Rust | Arm::Java => {},
            // CLIENT_VERSION=2 selects librdkafka (3 is the Rust client via
            // the C FFI).
            Arm::Librdkafka => set("CLIENT_VERSION", "2"),
            Arm::PythonSyncRust | Arm::PythonAsyncRust | Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka => {
                let librdkafka = matches!(self, Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka);
                let is_async = matches!(self, Arm::PythonAsyncRust | Arm::PythonAsyncLibrdkafka);
                set("CLIENT_VERSION", if librdkafka { "2" } else { "3" });
                set("ASYNC", if is_async { "True" } else { "False" });
                // The Rust-backed run reaches the librdkafka venv through this
                // for its helpers (librdkafka_helpers.py).
                set("LIBRDKAFKA_PYTHON", &venv_python(repo, "venv-librdkafka").display().to_string());
            },
        }
        env
    }

    /// `ENABLE_IDEMPOTENCE` as this harness reads it: the C harness turns it
    /// on only for exactly `True`; the others pass the value to the client
    /// configuration, which takes `true` / `false`.
    fn idempotence_value(self, enabled: bool) -> &'static str {
        match (self, enabled) {
            (Arm::Librdkafka, true) => "True",
            (Arm::Librdkafka, false) => "False",
            (_, true) => "true",
            (_, false) => "false",
        }
    }

    /// What must exist before the arm can run, from `build`.
    fn prerequisites(self, repo: &Path) -> Vec<PathBuf> {
        match self {
            // Built by `cargo` on first use; `build` compiles it ahead.
            Arm::Rust => Vec::new(),
            Arm::Librdkafka => vec![c_harness(repo)],
            Arm::Java => vec![java_jar(repo)],
            Arm::PythonSyncRust | Arm::PythonAsyncRust => vec![venv_python(repo, "venv")],
            Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka => vec![venv_python(repo, "venv-librdkafka")],
        }
    }
}

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

fn c_harness(repo: &Path) -> PathBuf {
    repo.join("c/build/producer_perf_test")
}

fn java_jar(repo: &Path) -> PathBuf {
    repo.join("tools/java-perf-test/build/libs/java-perf-test-all.jar")
}

fn python_harness(repo: &Path) -> PathBuf {
    repo.join("python/test/performance/producer_performance_test.py")
}

fn venv_python(repo: &Path, venv: &str) -> PathBuf {
    repo.join(venv).join("bin/python")
}

fn repo_root() -> &'static Path {
    package_root().parent().expect("the Rust workspace lives inside the repository")
}

// ---------------------------------------------------------------------------
// The tests file
// ---------------------------------------------------------------------------

/// One line of [`TESTS_FILE`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PerfTest {
    pub(crate) id: String,
    groups: Vec<Group>,
    /// Harness settings in file order, values already validated.
    pub(crate) settings: Vec<(String, String)>,
}

/// What a setting's value may be.
#[derive(Clone, Copy)]
enum Kind {
    /// A whole number, 0 allowed.
    Count,
    /// A whole number above 0.
    Positive,
    Compression,
    Bool,
}

/// The harness settings a test may set; every producer harness reads all of
/// them, in the same units, except that the C harness raises a `BATCH_SIZE`
/// below 1024 KiB to 1 MiB for librdkafka (see [`MIN_RUST_GROUP_BATCH_KIB`]).
/// A setting a test leaves out takes each harness's own default, and those
/// differ (`MAX_IN_FLIGHT`: 1000 in the C harness, the client default of 5
/// in Rust and Java), as in the EC2 runs; set it in the test to pin it.
/// Units: `BATCH_SIZE` / `MAX_REQUEST_SIZE` in KiB, `BUFFER_MEMORY` in MiB.
const SETTINGS: &[(&str, Kind)] = &[
    ("PARTITIONS", Kind::Positive),
    ("VALUE_SIZE", Kind::Positive),
    ("KEY_SIZE", Kind::Count),
    ("LIMIT_RPS", Kind::Count),
    ("BATCH_SIZE", Kind::Positive),
    ("LINGER_MS", Kind::Count),
    ("MAX_IN_FLIGHT", Kind::Positive),
    ("BUFFER_MEMORY", Kind::Positive),
    ("MAX_REQUEST_SIZE", Kind::Positive),
    ("COMPRESSION_TYPE", Kind::Compression),
    ("ENABLE_IDEMPOTENCE", Kind::Bool),
];

/// Set by every test, so no arm falls back to a harness default the others
/// might not share. `LIMIT_RPS=0` is an unbounded test.
const REQUIRED_SETTINGS: &[&str] = &["PARTITIONS", "VALUE_SIZE", "LIMIT_RPS"];

const MAX_TEST_ID_LEN: usize = 60;

/// The smallest `BATCH_SIZE` (KiB) a `rust`-group test may set: below it the
/// C harness's librdkafka arm runs with 1 MiB batches
/// (`c/tests/producer_perf_test.c`) while Rust and Java use the value, so
/// the arms would not be comparable.
const MIN_RUST_GROUP_BATCH_KIB: u64 = 1024;

impl PerfTest {
    pub(crate) fn setting(&self, key: &str) -> Option<&str> {
        self.settings.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    fn number(&self, key: &str) -> u64 {
        // Validated when the file was parsed.
        self.setting(key).and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    fn partitions(&self) -> u64 {
        self.number("PARTITIONS")
    }

    /// The target ingress in MB/s (payload only), `None` when unbounded.
    fn rate_mb_s(&self) -> Option<f64> {
        let rps = self.number("LIMIT_RPS");
        if rps == 0 {
            return None;
        }
        let size = self.number("KEY_SIZE") + self.number("VALUE_SIZE");
        Some((rps * size) as f64 / 1e6)
    }

    fn applies_to(&self, group: Group) -> bool {
        self.groups.contains(&group)
    }

    /// The test's settings as `arm` reads them (see [`Arm::idempotence_value`]).
    fn env_for(&self, arm: Arm) -> Vec<(String, String)> {
        self.settings
            .iter()
            .map(|(k, v)| {
                let v = if k == "ENABLE_IDEMPOTENCE" {
                    arm.idempotence_value(v == "true").to_string()
                } else {
                    v.clone()
                };
                (k.clone(), v)
            })
            .collect()
    }
}

/// Parse the tests file: `<id> <group,group> KEY=VALUE ...` per line, `#`
/// starts a comment.
fn parse_tests(text: &str) -> anyhow::Result<Vec<PerfTest>> {
    let mut tests: Vec<PerfTest> = Vec::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let test = parse_test_line(line).with_context(|| format!("{TESTS_FILE}:{line_no}"))?;
        if tests.iter().any(|t| t.id == test.id) {
            bail!("{TESTS_FILE}:{line_no}: test `{}` is defined twice", test.id);
        }
        tests.push(test);
    }
    if tests.is_empty() {
        bail!("{TESTS_FILE} defines no tests");
    }
    Ok(tests)
}

fn parse_test_line(line: &str) -> anyhow::Result<PerfTest> {
    let mut words = line.split_whitespace();
    let id = words.next().unwrap_or_default().to_string();
    validate_test_id(&id)?;
    let groups_word = words
        .next()
        .ok_or_else(|| anyhow::anyhow!("test `{id}` names no client group"))?;
    let mut groups = Vec::new();
    for g in groups_word.split(',') {
        let group = Group::parse(g)?;
        if groups.contains(&group) {
            bail!("test `{id}` names group `{g}` twice");
        }
        groups.push(group);
    }
    let mut settings: Vec<(String, String)> = Vec::new();
    for word in words {
        let (key, value) = word
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("test `{id}`: `{word}` is not KEY=VALUE"))?;
        let Some(&(_, kind)) = SETTINGS.iter().find(|(k, _)| *k == key) else {
            let known: Vec<&str> = SETTINGS.iter().map(|(k, _)| *k).collect();
            bail!("test `{id}`: unknown setting `{key}` (known: {})", known.join(", "));
        };
        if settings.iter().any(|(k, _)| k == key) {
            bail!("test `{id}`: `{key}` is set twice");
        }
        validate_setting(key, value, kind).with_context(|| format!("test `{id}`"))?;
        settings.push((key.to_string(), value.to_string()));
    }
    for key in REQUIRED_SETTINGS {
        if !settings.iter().any(|(k, _)| k == key) {
            bail!("test `{id}` must set {key}");
        }
    }
    let batch_kib = settings
        .iter()
        .find(|(k, _)| k == "BATCH_SIZE")
        .and_then(|(_, v)| v.parse::<u64>().ok());
    if groups.contains(&Group::Rust) && batch_kib.is_some_and(|kib| kib < MIN_RUST_GROUP_BATCH_KIB) {
        bail!(
            "test `{id}`: BATCH_SIZE below {MIN_RUST_GROUP_BATCH_KIB} KiB is not comparable in the rust group (the C \
             harness runs librdkafka with 1 MiB batches below it); limit the test to `python`"
        );
    }
    Ok(PerfTest { id, groups, settings })
}

fn validate_test_id(id: &str) -> anyhow::Result<()> {
    let valid = !id.is_empty()
        && id.len() <= MAX_TEST_ID_LEN
        && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !id.starts_with('-');
    if !valid {
        bail!(
            "invalid test id `{id}`: use lowercase letters, digits and `-`, at most {MAX_TEST_ID_LEN} characters, \
             not starting with `-`"
        );
    }
    Ok(())
}

fn validate_setting(key: &str, value: &str, kind: Kind) -> anyhow::Result<()> {
    let ok = match kind {
        // Digits only: `u64::from_str` would also take a leading `+`, which
        // the C harness's `atoi` reads differently from the others.
        Kind::Count => !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) && value.parse::<u64>().is_ok(),
        Kind::Positive => {
            !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) && value.parse::<u64>().is_ok_and(|n| n > 0)
        },
        Kind::Compression => matches!(value, "none" | "gzip" | "snappy" | "lz4" | "zstd"),
        Kind::Bool => matches!(value, "true" | "false"),
    };
    if !ok {
        let expected = match kind {
            Kind::Count => "a whole number",
            Kind::Positive => "a whole number above 0",
            Kind::Compression => "one of none, gzip, snappy, lz4, zstd",
            Kind::Bool => "`true` or `false`",
        };
        bail!("{key}={value}: expected {expected}");
    }
    Ok(())
}

fn load_tests() -> anyhow::Result<Vec<PerfTest>> {
    let path = repo_root().join(TESTS_FILE);
    let text = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    parse_tests(&text)
}

/// The tests named in `list` (comma-separated), in that order.
fn select_tests(all: &[PerfTest], list: &str, group: Group) -> anyhow::Result<Vec<PerfTest>> {
    let mut selected: Vec<PerfTest> = Vec::new();
    for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some(test) = all.iter().find(|t| t.id == name) else {
            let ids: Vec<&str> = all.iter().map(|t| t.id.as_str()).collect();
            bail!("unknown test `{name}` (known: {})", ids.join(", "));
        };
        if !test.applies_to(group) {
            let groups: Vec<&str> = test.groups.iter().map(|g| g.name()).collect();
            bail!(
                "test `{name}` does not apply to client group `{}` (only {})",
                group.name(),
                groups.join(", ")
            );
        }
        if selected.iter().any(|t| t.id == name) {
            bail!("test `{name}` is listed twice");
        }
        selected.push(test.clone());
    }
    if selected.is_empty() {
        bail!("no tests given (--tests ID,ID)");
    }
    Ok(selected)
}

fn list() -> anyhow::Result<()> {
    let tests = load_tests()?;
    println!("Tests in {TESTS_FILE}:");
    for t in &tests {
        let groups: Vec<&str> = t.groups.iter().map(|g| g.name()).collect();
        println!("  {:<40} {:<12} {}", t.id, groups.join(","), report::scenario(t));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

struct RunOptions {
    group: Group,
    tests: Vec<PerfTest>,
    duration_min: u64,
    warmup_min: u64,
    /// `check` only: the most minutes the tests may take, and the cluster's
    /// ingress limit.
    budget_min: Option<u64>,
    ingress_mb_s: Option<f64>,
    /// `run` only.
    bootstrap: String,
    kafka_bin: PathBuf,
    results_dir: PathBuf,
    topic_prefix: String,
    push_artifacts: bool,
}

const SHARED_FLAGS: &[&str] = &["--group", "--tests", "--duration-min", "--warmup-min"];
const CHECK_FLAGS: &[&str] = &["--budget-min", "--ingress-mb-s"];
const RUN_FLAGS: &[&str] = &["--bootstrap", "--kafka-bin", "--results-dir", "--topic-prefix"];

impl RunOptions {
    fn parse(raw: &[String], for_run: bool) -> anyhow::Result<RunOptions> {
        let mut allowed: Vec<&str> = SHARED_FLAGS.to_vec();
        let mut switches: Vec<&str> = Vec::new();
        if for_run {
            allowed.extend_from_slice(RUN_FLAGS);
            switches.push("--push-artifacts");
        } else {
            allowed.extend_from_slice(CHECK_FLAGS);
        }
        let flags = parse_flags(raw, &allowed, &switches)?;
        let group = Group::parse(required(&flags, "--group")?)?;
        let duration_min = minutes(&flags, "--duration-min", 30, 1, MAX_DURATION_MINUTES)?;
        let warmup_min = minutes(&flags, "--warmup-min", 5, 0, MAX_WARMUP_MINUTES)?;
        let tests = select_tests(&load_tests()?, required(&flags, "--tests")?, group)?;
        let budget_min = match flags.get("--budget-min") {
            Some(_) => Some(minutes(&flags, "--budget-min", 0, 1, u64::from(u32::MAX))?),
            None => None,
        };
        let ingress_mb_s = match flags.get("--ingress-mb-s") {
            Some(raw) => match raw.trim().parse::<f64>() {
                Ok(v) if v.is_finite() && v > 0.0 => Some(v),
                _ => bail!("--ingress-mb-s: `{raw}` is not a positive number"),
            },
            None => None,
        };
        let (bootstrap, kafka_bin, results_dir, topic_prefix) = if for_run {
            let topic_prefix = flags.get("--topic-prefix").cloned().unwrap_or_else(|| "perf-local".to_string());
            validate_topic_prefix(&topic_prefix)?;
            (
                required(&flags, "--bootstrap")?.to_string(),
                PathBuf::from(required(&flags, "--kafka-bin")?),
                PathBuf::from(required(&flags, "--results-dir")?),
                topic_prefix,
            )
        } else {
            (String::new(), PathBuf::new(), PathBuf::new(), String::new())
        };
        Ok(RunOptions {
            group,
            tests,
            duration_min,
            warmup_min,
            budget_min,
            ingress_mb_s,
            bootstrap,
            kafka_bin,
            results_dir,
            topic_prefix,
            push_artifacts: flags.contains_key("--push-artifacts"),
        })
    }

    fn arm_minutes(&self) -> u64 {
        self.warmup_min + self.duration_min + ARM_SETUP_MINUTES
    }

    fn estimate_minutes(&self) -> u64 {
        self.tests.len() as u64 * self.group.arms().len() as u64 * self.arm_minutes()
    }
}

/// `--flag value` / `--flag=value` options and value-less switches. A switch
/// maps to an empty string.
fn parse_flags(raw: &[String], allowed: &[&str], switches: &[&str]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut args = raw.iter();
    while let Some(arg) = args.next() {
        if !arg.starts_with("--") {
            bail!("unexpected argument `{arg}`");
        }
        let (flag, value) = if switches.contains(&arg.as_str()) {
            (arg.clone(), String::new())
        } else if let Some((flag, value)) = arg.split_once('=') {
            (flag.to_string(), value.to_string())
        } else {
            let value = args.next().ok_or_else(|| anyhow::anyhow!("{arg} needs a value"))?;
            (arg.clone(), value.clone())
        };
        if !allowed.contains(&flag.as_str()) && !switches.contains(&flag.as_str()) {
            bail!("unknown option `{flag}`\n\n{HELP}");
        }
        if out.insert(flag.clone(), value).is_some() {
            bail!("{flag} is given twice");
        }
    }
    Ok(out)
}

fn required<'a>(flags: &'a BTreeMap<String, String>, flag: &str) -> anyhow::Result<&'a str> {
    match flags.get(flag) {
        Some(v) if !v.trim().is_empty() => Ok(v.trim()),
        _ => bail!("{flag} is required"),
    }
}

fn minutes(flags: &BTreeMap<String, String>, flag: &str, default: u64, min: u64, max: u64) -> anyhow::Result<u64> {
    let Some(raw) = flags.get(flag) else {
        return Ok(default);
    };
    let value: u64 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{flag}: `{raw}` is not a whole number of minutes"))?;
    if !(min..=max).contains(&value) {
        bail!("{flag} must be between {min} and {max} minutes, got {value}");
    }
    Ok(value)
}

/// Topic names are `<prefix>-<test>-<arm>`; Kafka allows `[A-Za-z0-9._-]`,
/// at most 249 characters, and the test id and arm add at most ~85.
fn validate_topic_prefix(prefix: &str) -> anyhow::Result<()> {
    let valid = !prefix.is_empty()
        && prefix.len() <= 100
        && prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if !valid {
        bail!("invalid --topic-prefix `{prefix}`: use letters, digits, `.`, `_` and `-`, at most 100 characters");
    }
    Ok(())
}

fn topic_name(prefix: &str, test: &PerfTest, arm: Arm) -> String {
    format!("{prefix}-{}-{}", test.id, arm.id())
}

// ---------------------------------------------------------------------------
// check
// ---------------------------------------------------------------------------

fn check(options: &RunOptions) -> anyhow::Result<()> {
    let group = options.group;
    let arms: Vec<&str> = group.arms().iter().map(|a| a.label()).collect();
    println!("Client group: {}; arms in order: {}", group.name(), arms.join(", "));
    if let Some(ingress) = options.ingress_mb_s {
        println!("Cluster ingress limit: ~{ingress:.0} MB/s");
    }
    println!("Tests ({}), in order:", options.tests.len());
    let mut warnings = Vec::new();
    for (i, t) in options.tests.iter().enumerate() {
        let load = match t.rate_mb_s() {
            Some(mb_s) => {
                if let Some(ingress) = options.ingress_mb_s.filter(|i| mb_s > INGRESS_WARN_SHARE * i) {
                    warnings.push(format!(
                        "{}: {mb_s:.0} MB/s is above {:.0}% of the cluster's ~{ingress:.0} MB/s ingress; the cluster \
                         may throttle it",
                        t.id,
                        INGRESS_WARN_SHARE * 100.0
                    ));
                }
                format!("~{mb_s:.1} MB/s")
            },
            None => "unbounded (may reach the cluster's ingress limit)".to_string(),
        };
        println!("  {}. {:<40} {}  [{load}]", i + 1, t.id, report::scenario(t));
    }
    let estimate = options.estimate_minutes();
    let per_test = options.arm_minutes() * group.arms().len() as u64;
    println!(
        "Per arm: {} min warmup + {} min measured + ~{ARM_SETUP_MINUTES} min setup; per test ~{}",
        options.warmup_min,
        options.duration_min,
        report::hours_minutes(per_test)
    );
    match options.budget_min {
        Some(budget) => println!(
            "Estimated test time: {} (budget {})",
            report::hours_minutes(estimate),
            report::hours_minutes(budget)
        ),
        None => println!("Estimated test time: {}", report::hours_minutes(estimate)),
    }
    for w in &warnings {
        println!("WARNING: {w}");
    }
    if let Some(budget) = options.budget_min.filter(|b| estimate > *b) {
        bail!(
            "{} tests take ~{}, more than the budget of {}; at {} min per arm at most {} test(s) fit",
            options.tests.len(),
            report::hours_minutes(estimate),
            report::hours_minutes(budget),
            options.duration_min,
            budget / per_test
        );
    }
    println!("OK");
    Ok(())
}

// ---------------------------------------------------------------------------
// build
// ---------------------------------------------------------------------------

/// Build the group's harnesses, as `deploy_and_run_perf.py`'s bootstrap
/// does. The system packages must already be installed: for `rust`
/// librdkafka-dev (pkg-config `rdkafka`), cmake, a JDK 17+ and Gradle (`$GRADLE`
/// or `gradle`); for `python` python3-venv and python3-dev (`$PERF_PYTHON` or
/// `python3`).
fn build(group: Group) -> anyhow::Result<()> {
    let repo = repo_root();
    let rust = repo.join("rust");
    match group {
        Group::Rust => {
            // The C harness links the Rust client's C library statically and
            // needs its header, so the FFI build comes first. Only its
            // librdkafka backend (CLIENT_VERSION=2) runs here, so the Rust code
            // in it is unused and built without native CPU flags.
            step(
                "Rust client C library (FFI)",
                Command::new(cargo())
                    .args(["build", "--features", "ffi", "--release"])
                    .current_dir(&rust),
            )?;
            let rdkafka = Command::new("pkg-config").args(["--exists", "rdkafka"]).status();
            if !rdkafka.is_ok_and(|s| s.success()) {
                bail!("librdkafka-dev is not installed (pkg-config cannot find rdkafka)");
            }
            step(
                "C producer harness: configure",
                Command::new("cmake")
                    .arg("-S")
                    .arg(repo.join("c"))
                    .arg("-B")
                    .arg(repo.join("c/build"))
                    .arg(format!("-DRUST_PROJECT_ROOT={}", rust.display()))
                    .arg("-DCMAKE_C_FLAGS=-O2 -march=native"),
            )?;
            step(
                "C producer harness: build",
                Command::new("cmake")
                    .arg("--build")
                    .arg(repo.join("c/build"))
                    .args(["--target", "producer_perf_test"]),
            )?;
            // The Rust arm runs `cargo xtask producer-perf-test`; compiling its
            // test binary now keeps the compile out of the first arm.
            step(
                "Rust producer harness",
                Command::new(cargo())
                    .args([
                        "test",
                        "--release",
                        "--features",
                        "integration-tests",
                        "--test",
                        "performance",
                        "--no-run",
                    ])
                    .current_dir(&rust),
            )?;
            let gradle = std::env::var("GRADLE").unwrap_or_else(|_| "gradle".to_string());
            step(
                "Java producer harness",
                Command::new(gradle)
                    .args(["shadowJar", "--console=plain"])
                    .current_dir(repo.join("tools/java-perf-test")),
            )?;
        },
        Group::Python => {
            // As the EC2 bootstrap: the Rust library and the extension are
            // built for the machine's CPU.
            step(
                "Rust client C library (FFI)",
                Command::new(cargo())
                    .args(["build", "--features", "ffi", "--release"])
                    .env("RUSTFLAGS", "-C target-cpu=native")
                    .current_dir(&rust),
            )?;
            let python = std::env::var("PERF_PYTHON").unwrap_or_else(|_| "python3".to_string());
            let venv = venv_python(repo, "venv");
            step(
                "Python venv (Rust-backed client)",
                Command::new(&python).arg("-m").arg("venv").arg(repo.join("venv")),
            )?;
            step(
                "pip upgrade",
                Command::new(&venv).args(["-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"]),
            )?;
            step(
                "Rust-backed Python client",
                Command::new(&venv)
                    .args(["-m", "pip", "install", "-e", "."])
                    .env("CONFLUENT_KAFKA_LIB_DIR", rust.join("target/release"))
                    .env("CFLAGS", "-O2 -march=native")
                    .current_dir(repo.join("python")),
            )?;
            // pytest: the performance package's conftest imports it.
            step(
                "harness dependencies",
                Command::new(&venv).args(["-m", "pip", "install", "psutil", "pytest"]),
            )?;
            let librdkafka = venv_python(repo, "venv-librdkafka");
            step(
                "Python venv (librdkafka-backed client)",
                Command::new(&python).arg("-m").arg("venv").arg(repo.join("venv-librdkafka")),
            )?;
            step(
                "pip upgrade",
                Command::new(&librdkafka).args(["-m", "pip", "install", "--upgrade", "pip"]),
            )?;
            step(
                "librdkafka-backed Python client",
                Command::new(&librdkafka)
                    .args(["-m", "pip", "install", "-r"])
                    .arg(repo.join("python/test/performance/requirements-librdkafka.txt")),
            )?;
        },
    }
    for arm in group.arms() {
        for path in arm.prerequisites(repo) {
            if !path.exists() {
                bail!("the build did not produce {} ({})", path.display(), arm.label());
            }
        }
    }
    println!("✅ Built the {} harnesses", group.name());
    Ok(())
}

fn step(what: &str, command: &mut Command) -> anyhow::Result<()> {
    println!("==> {what}: {}", describe(command));
    let status = command.status().with_context(|| format!("starting {what}"))?;
    if !status.success() {
        bail!("{what} failed ({status})");
    }
    Ok(())
}

fn describe(command: &Command) -> String {
    let mut out = command.get_program().to_string_lossy().into_owned();
    for arg in command.get_args() {
        out.push(' ');
        out.push_str(&arg.to_string_lossy());
    }
    out
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// The SASL credentials of the run's API key.
struct Credentials {
    key: String,
    secret: String,
}

impl Credentials {
    fn from_env() -> anyhow::Result<Credentials> {
        let get = |name: &str| -> anyhow::Result<String> {
            match std::env::var(name) {
                Ok(v) if !v.is_empty() && v != "null" => Ok(v),
                _ => bail!("{name} is not set (the API key and secret come from PERF_API_KEY / PERF_API_SECRET)"),
            }
        };
        Ok(Credentials { key: get("PERF_API_KEY")?, secret: get("PERF_API_SECRET")? })
    }
}

/// How an arm ended.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Outcome {
    Passed,
    Failed(String),
}

/// The measured numbers of one arm, from its `results.json`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Metrics {
    /// The harness's own name for the client.
    pub(crate) client: String,
    pub(crate) msg_s: f64,
    pub(crate) mib_s: f64,
    pub(crate) p50: f64,
    pub(crate) p90: f64,
    pub(crate) p95: f64,
    pub(crate) p99: f64,
    pub(crate) p999: f64,
    pub(crate) max: f64,
    pub(crate) avg: f64,
    pub(crate) cpu_pct: f64,
    pub(crate) rss_mib: f64,
    pub(crate) messages: f64,
}

impl Metrics {
    fn from_results(text: &str) -> anyhow::Result<Metrics> {
        let v = json::parse(text)?;
        let top = |key: &str| {
            v.get(key)
                .and_then(json::Value::as_f64)
                .ok_or_else(|| anyhow::anyhow!("no `{key}`"))
        };
        let latency = v.get("latency_ms").ok_or_else(|| anyhow::anyhow!("no `latency_ms`"))?;
        let lat = |key: &str| {
            latency
                .get(key)
                .and_then(json::Value::as_f64)
                .ok_or_else(|| anyhow::anyhow!("no `latency_ms.{key}`"))
        };
        let client = v
            .get("client")
            .and_then(json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("no `client`"))?;
        Ok(Metrics {
            client: client.to_string(),
            msg_s: top("throughput_msg_s")?,
            mib_s: top("throughput_mib_s")?,
            p50: lat("p50")?,
            p90: lat("p90")?,
            p95: lat("p95")?,
            p99: lat("p99")?,
            p999: lat("p999")?,
            max: lat("max")?,
            avg: lat("avg")?,
            cpu_pct: top("cpu_avg_pct")?,
            rss_mib: top("rss_avg_kib")? / 1024.0,
            messages: top("messages_measured")?,
        })
    }
}

pub(crate) struct ArmResult {
    pub(crate) arm: Arm,
    pub(crate) outcome: Outcome,
    pub(crate) metrics: Option<Metrics>,
    pub(crate) elapsed: Duration,
}

pub(crate) struct TestResult {
    pub(crate) test: PerfTest,
    pub(crate) arms: Vec<ArmResult>,
}

/// Run-wide facts shown at the top of the report.
pub(crate) struct RunInfo {
    pub(crate) group: Group,
    pub(crate) duration_min: u64,
    pub(crate) warmup_min: u64,
    pub(crate) started: String,
    /// `(label, value)` pairs, only the ones known.
    pub(crate) facts: Vec<(String, String)>,
    pub(crate) ingress_mb_s: Option<f64>,
}

fn run_tests(options: &RunOptions) -> anyhow::Result<()> {
    crate::signals::install();
    let repo = repo_root();
    let credentials = Credentials::from_env()?;
    let topics_cli = options.kafka_bin.join("kafka-topics.sh");
    if !topics_cli.is_file() {
        bail!("{} not found (--kafka-bin)", topics_cli.display());
    }
    for arm in options.group.arms() {
        for path in arm.prerequisites(repo) {
            if !path.exists() {
                bail!(
                    "{} is missing ({}); run `cargo xtask perf-cloud build --group {}` first",
                    path.display(),
                    arm.label(),
                    options.group.name()
                );
            }
        }
    }
    for test in &options.tests {
        let dir = options.results_dir.join(&test.id);
        if dir.exists() {
            bail!("{} already exists; use a new --results-dir", dir.display());
        }
    }
    fs::create_dir_all(&options.results_dir).with_context(|| format!("creating {}", options.results_dir.display()))?;
    // Absolute, because each arm runs in its own directory.
    let results_dir = fs::canonicalize(&options.results_dir)?;

    // The admin client's properties hold the secret: they go to a private
    // directory outside the results, which are published.
    let work = PrivateDir::create()?;
    let client_properties = work.path().join("client.properties");
    write_private(&client_properties, &client_properties_text(&options.bootstrap, &credentials))?;
    let redactions = Arc::new(vec![credentials.secret.clone()]);

    let info = run_info(options);
    // One baseline for the whole run: a signal at any point (during a
    // command, between arms, during an artifact push) stops the run.
    let signals_baseline = crate::signals::count();
    let mut results: Vec<TestResult> = Vec::new();
    let mut interrupted = false;
    'tests: for (i, test) in options.tests.iter().enumerate() {
        println!(
            "\n######## Test {}/{}: {} — {} ########",
            i + 1,
            options.tests.len(),
            test.id,
            report::scenario(test)
        );
        let mut arms = Vec::new();
        for &arm in options.group.arms() {
            if crate::signals::count() > signals_baseline {
                interrupted = true;
                results.push(TestResult { test: test.clone(), arms });
                break 'tests;
            }
            let ctx = ArmContext {
                repo,
                options,
                test,
                arm,
                dir: results_dir.join(&test.id).join(arm.id()),
                topic: topic_name(&options.topic_prefix, test, arm),
                topics_cli: &topics_cli,
                client_properties: &client_properties,
                credentials: &credentials,
                redactions: Arc::clone(&redactions),
                signals_baseline,
            };
            let result = run_arm(&ctx)?;
            let stop = matches!(&result.outcome, Outcome::Failed(r) if r == INTERRUPTED);
            arms.push(result);
            if stop {
                interrupted = true;
                results.push(TestResult { test: test.clone(), arms });
                break 'tests;
            }
        }
        results.push(TestResult { test: test.clone(), arms });
        write_reports(&results_dir, &info, &results)?;
        if options.push_artifacts {
            push_artifact(&results_dir.join(&test.id), &format!("perf-results/{}", test.id));
            push_reports(&results_dir);
        }
    }
    write_reports(&results_dir, &info, &results)?;
    if options.push_artifacts {
        push_reports(&results_dir);
    }
    println!("\nReport: {}", results_dir.join("report.md").display());

    if interrupted {
        bail!("interrupted by {}", crate::signals::last_name());
    }
    let failed: Vec<String> = results
        .iter()
        .flat_map(|t| {
            t.arms.iter().filter_map(move |a| match &a.outcome {
                Outcome::Failed(reason) => Some(format!("{} / {}: {reason}", t.test.id, a.arm.label())),
                Outcome::Passed => None,
            })
        })
        .collect();
    if !failed.is_empty() {
        bail!("{} arm(s) failed:\n  {}", failed.len(), failed.join("\n  "));
    }
    println!("✅ All {} test(s) passed", results.len());
    Ok(())
}

fn run_info(options: &RunOptions) -> RunInfo {
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let mut facts = Vec::new();
    // This repository's commit, which is the one built: a CI job may have
    // checked it out from another repository's pipeline.
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_root())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    let kernel = Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty());
    // A limit is positive; anything else (e.g. a `0` meaning "not reported")
    // would flag every result as near it.
    let ingress = env("PERF_CLUSTER_INGRESS_MB_S")
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0);
    for (label, value) in [
        ("Commit", commit),
        ("Workflow", env("SEMAPHORE_WORKFLOW_ID")),
        ("Agent", env("SEMAPHORE_AGENT_MACHINE_TYPE")),
        ("Agent region", env("AGENT_REGION")),
        ("Agent AZ ID", env("AGENT_AZ_ID")),
        ("Kernel", kernel),
        ("Cluster", env("PERF_CLUSTER_ID")),
        ("Cluster size", env("PERF_CLUSTER_SIZE")),
        ("Cluster ingress limit", ingress.map(|mb| format!("{mb:.0} MB/s"))),
    ] {
        if let Some(value) = value {
            facts.push((label.to_string(), value));
        }
    }
    RunInfo {
        group: options.group,
        duration_min: options.duration_min,
        warmup_min: options.warmup_min,
        started: now_utc(),
        facts,
        ingress_mb_s: ingress,
    }
}

fn write_reports(results_dir: &Path, info: &RunInfo, results: &[TestResult]) -> anyhow::Result<()> {
    fs::write(results_dir.join("report.md"), report::markdown(info, results))?;
    fs::write(results_dir.join("junit.xml"), report::junit(results))?;
    Ok(())
}

/// Push to the Semaphore job's artifacts (`--push-artifacts`) right away, so a
/// job that later hits its time limit keeps the tests that finished. A failed push is only
/// logged: the epilogue pushes everything again.
fn push_artifact(path: &Path, destination: &str) {
    let status = Command::new("artifact")
        .args(["push", "job"])
        .arg(path)
        .args(["--destination", destination, "--force"])
        .status();
    if !status.is_ok_and(|s| s.success()) {
        println!("WARNING: could not push {} as a job artifact", path.display());
    }
}

fn push_reports(results_dir: &Path) {
    push_artifact(&results_dir.join("report.md"), "perf-results/report.md");
    push_artifact(&results_dir.join("junit.xml"), "perf-results/junit.xml");
}

/// The Java-form properties `kafka-topics.sh` reads (`--command-config`).
fn client_properties_text(bootstrap: &str, credentials: &Credentials) -> String {
    format!(
        "bootstrap.servers={bootstrap}\nsecurity.protocol=SASL_SSL\nsasl.mechanism=PLAIN\n\
         sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{}\" password=\"{}\";\n",
        jaas_escape(&credentials.key),
        jaas_escape(&credentials.secret)
    )
}

/// A JAAS option value is a double-quoted string; `\` and `"` are escaped.
fn jaas_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Write `contents` to a new file only the current user can read.
fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

/// A temporary directory only the current user can enter, removed on drop.
struct PrivateDir(PathBuf);

impl PrivateDir {
    fn create() -> anyhow::Result<PrivateDir> {
        let path = std::env::temp_dir().join(format!("perf-cloud-{}", std::process::id()));
        fs::create_dir(&path).with_context(|| format!("creating {}", path.display()))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        Ok(PrivateDir(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for PrivateDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The reason recorded for an arm stopped by a signal to the driver.
const INTERRUPTED: &str = "interrupted";

struct ArmContext<'a> {
    repo: &'a Path,
    options: &'a RunOptions,
    test: &'a PerfTest,
    arm: Arm,
    dir: PathBuf,
    topic: String,
    topics_cli: &'a Path,
    client_properties: &'a Path,
    credentials: &'a Credentials,
    redactions: Arc<Vec<String>>,
    /// [`crate::signals::count`] when the run started.
    signals_baseline: u32,
}

/// Create the arm's topic, run the arm, read its results, delete the topic.
/// An error is returned only for local I/O failures of the results
/// directory; everything about the arm itself, a harness that cannot be
/// started included, ends up in its [`Outcome`], so the run goes on.
fn run_arm(ctx: &ArmContext<'_>) -> anyhow::Result<ArmResult> {
    let started = Instant::now();
    fs::create_dir_all(&ctx.dir).with_context(|| format!("creating {}", ctx.dir.display()))?;
    let log = ArmLog::create(&ctx.dir.join("run.log"), Arc::clone(&ctx.redactions), ctx.signals_baseline)?;
    log.line(&format!(
        "==> {} / {} on topic {} ({})",
        ctx.test.id,
        ctx.arm.label(),
        ctx.topic,
        now_utc()
    ));

    let created = create_topic(ctx, &log).unwrap_or_else(|e| Err(format!("{e:#}")));
    let outcome = match created {
        Err(reason) => Outcome::Failed(reason),
        Ok(()) => {
            let outcome = run_harness(ctx, &log).unwrap_or_else(|e| Outcome::Failed(format!("{e:#}")));
            if log.interrupted() {
                // Stopping promptly matters more; the cluster deletion
                // removes the topic.
                log.line(&format!("==> interrupted: leaving topic {} to the cluster deletion", ctx.topic));
            } else if let Err(e) = delete_topic(ctx, &log) {
                log.line(&format!("WARNING: could not delete topic {}: {e:#}", ctx.topic));
            }
            outcome
        },
    };
    let (outcome, metrics) = match outcome {
        Outcome::Passed => match fs::read_to_string(ctx.dir.join("results.json")) {
            Ok(text) => match Metrics::from_results(&text) {
                Ok(m) if m.client != ctx.arm.expected_client() => (
                    Outcome::Failed(format!(
                        "results.json is from client `{}`, expected `{}`",
                        m.client,
                        ctx.arm.expected_client()
                    )),
                    None,
                ),
                Ok(m) => (Outcome::Passed, Some(m)),
                Err(e) => (Outcome::Failed(format!("unreadable results.json: {e}")), None),
            },
            Err(e) => (Outcome::Failed(format!("no results.json: {e}")), None),
        },
        failed => {
            // A harness that failed after writing its summary (e.g. a
            // verification mismatch) still has numbers worth showing.
            let metrics = fs::read_to_string(ctx.dir.join("results.json"))
                .ok()
                .and_then(|text| Metrics::from_results(&text).ok());
            (failed, metrics)
        },
    };
    match &outcome {
        Outcome::Passed => log.line(&format!("==> {} / {}: passed", ctx.test.id, ctx.arm.label())),
        Outcome::Failed(reason) => log.line(&format!("==> {} / {}: FAILED: {reason}", ctx.test.id, ctx.arm.label())),
    }
    Ok(ArmResult { arm: ctx.arm, outcome, metrics, elapsed: started.elapsed() })
}

fn run_harness(ctx: &ArmContext<'_>, log: &ArmLog) -> anyhow::Result<Outcome> {
    let options = ctx.options;
    let mut command = ctx.arm.command(ctx.repo);
    // The Rust arm runs in the workspace (cargo needs it); every other arm
    // runs in its own directory, since several harnesses write
    // metrics.jsonl to the current directory.
    if ctx.arm != Arm::Rust {
        command.current_dir(&ctx.dir);
    }
    for var in HARNESS_VARS {
        command.env_remove(var);
    }
    let mut env: Vec<(String, String)> = vec![
        ("BOOTSTRAP_SERVERS".into(), options.bootstrap.clone()),
        ("TOPIC_NAME".into(), ctx.topic.clone()),
        ("SECURITY_PROTOCOL".into(), "SASL_SSL".into()),
        ("SASL_MECHANISM".into(), "PLAIN".into()),
        ("SASL_USERNAME".into(), ctx.credentials.key.clone()),
        ("SASL_PASSWORD".into(), ctx.credentials.secret.clone()),
        ("WARMUP_SECONDS".into(), (options.warmup_min * 60).to_string()),
        ("TEST_DURATION_SECONDS".into(), (options.duration_min * 60).to_string()),
        // Report only: no harness-side latency budget.
        ("P99_LIMIT_MS".into(), "0".into()),
        // The driver created the topic (C and Java read `False`; Python only
        // creates on `True`; the Rust harness never creates).
        ("CREATE_TOPIC".into(), "False".into()),
        ("RESULTS_FILE".into(), ctx.dir.join("results.json").display().to_string()),
        ("METRICS_FILE".into(), ctx.dir.join("metrics.jsonl").display().to_string()),
    ];
    env.extend(ctx.test.env_for(ctx.arm));
    env.extend(ctx.arm.own_env(ctx.repo));
    for (k, v) in &env {
        command.env(k, v);
    }
    log.line(&format!("==> {}", describe(&command)));
    let timeout = Duration::from_secs((options.warmup_min + options.duration_min) * 60) + ARM_TIMEOUT_SLACK;
    Ok(match run_logged(&mut command, log, timeout)? {
        Exit::Status(status) if status.success() => Outcome::Passed,
        Exit::Status(status) => Outcome::Failed(format!("harness exited with {status}")),
        Exit::TimedOut => {
            Outcome::Failed(format!("harness still running after {} min; stopped", timeout.as_secs() / 60))
        },
        Exit::Interrupted => Outcome::Failed(INTERRUPTED.to_string()),
    })
}

fn topics_command(ctx: &ArmContext<'_>) -> Command {
    let mut command = Command::new(ctx.topics_cli);
    command
        .args(["--bootstrap-server", &ctx.options.bootstrap, "--command-config"])
        .arg(ctx.client_properties);
    command
}

/// `Ok(Err(reason))` when the topic could not be created.
fn create_topic(ctx: &ArmContext<'_>, log: &ArmLog) -> anyhow::Result<Result<(), String>> {
    let partitions = ctx.test.partitions().to_string();
    for attempt in 1..=TOPIC_CREATE_ATTEMPTS {
        log.line(&format!(
            "==> creating topic {} with {partitions} partitions (attempt {attempt}/{TOPIC_CREATE_ATTEMPTS})",
            ctx.topic
        ));
        // No --replication-factor: the cluster's default (Confluent Cloud
        // rejects anything else).
        let mut create = topics_command(ctx);
        create.args([
            "--create",
            "--if-not-exists",
            "--topic",
            &ctx.topic,
            "--partitions",
            &partitions,
        ]);
        match run_logged(&mut create, log, TOPIC_COMMAND_TIMEOUT)? {
            Exit::Interrupted => return Ok(Err(INTERRUPTED.to_string())),
            Exit::Status(s) if s.success() => {
                if topic_described(ctx, log)? {
                    return Ok(Ok(()));
                }
            },
            Exit::Status(_) | Exit::TimedOut => {},
        }
        if attempt < TOPIC_CREATE_ATTEMPTS && sleep_unless_signalled(TOPIC_CREATE_RETRY_DELAY, log) {
            return Ok(Err(INTERRUPTED.to_string()));
        }
    }
    Ok(Err(format!(
        "could not create topic {} after {TOPIC_CREATE_ATTEMPTS} attempts",
        ctx.topic
    )))
}

/// Whether `--describe` sees the topic with all its partitions.
fn topic_described(ctx: &ArmContext<'_>, log: &ArmLog) -> anyhow::Result<bool> {
    let mut describe = topics_command(ctx);
    describe.args(["--describe", "--topic", &ctx.topic]);
    let partitions = ctx.test.partitions();
    Ok(match output_logged(&mut describe, log, TOPIC_COMMAND_TIMEOUT)? {
        Some(text) => {
            let seen = text
                .lines()
                .filter(|l| l.contains("Partition:") && l.contains(&ctx.topic))
                .count() as u64;
            if seen < partitions {
                log.line(&format!("==> topic {} shows {seen} of {partitions} partitions", ctx.topic));
            }
            seen >= partitions
        },
        None => false,
    })
}

/// Best effort: the cluster is deleted at the end of the run anyway.
fn delete_topic(ctx: &ArmContext<'_>, log: &ArmLog) -> anyhow::Result<()> {
    log.line(&format!("==> deleting topic {}", ctx.topic));
    let mut delete = topics_command(ctx);
    delete.args(["--delete", "--if-exists", "--topic", &ctx.topic]);
    match run_logged(&mut delete, log, TOPIC_COMMAND_TIMEOUT)? {
        Exit::Status(s) if s.success() => {},
        _ => log.line(&format!(
            "WARNING: could not delete topic {}; the cluster deletion removes it",
            ctx.topic
        )),
    }
    Ok(())
}

/// Sleep for `duration`; `true` if the run is (or gets) interrupted.
fn sleep_unless_signalled(duration: Duration, log: &ArmLog) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if log.interrupted() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

/// An arm's log: every line goes to the job output and to `run.log`, with
/// the secret masked.
struct ArmLog {
    file: Arc<Mutex<File>>,
    redactions: Arc<Vec<String>>,
    /// The run's [`crate::signals::count`] baseline: every command run
    /// through this log stops once a signal has arrived since then.
    signals_baseline: u32,
}

impl ArmLog {
    fn create(path: &Path, redactions: Arc<Vec<String>>, signals_baseline: u32) -> anyhow::Result<ArmLog> {
        let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
        Ok(ArmLog { file: Arc::new(Mutex::new(file)), redactions, signals_baseline })
    }

    fn interrupted(&self) -> bool {
        crate::signals::count() > self.signals_baseline
    }

    fn line(&self, text: &str) {
        write_line(&self.file, &self.redactions, text);
    }
}

fn write_line(file: &Mutex<File>, redactions: &[String], text: &str) {
    let text = redact(text, redactions);
    println!("{text}");
    if let Ok(mut f) = file.lock() {
        let _ = writeln!(f, "{text}");
    }
}

fn redact(text: &str, redactions: &[String]) -> String {
    let mut out = text.to_string();
    for secret in redactions.iter().filter(|s| !s.is_empty()) {
        out = out.replace(secret.as_str(), "<redacted>");
    }
    out
}

enum Exit {
    Status(ExitStatus),
    TimedOut,
    Interrupted,
}

/// Run `command` in its own process group, streaming its output into `log`,
/// stopping the whole group on timeout or on a signal to the driver; not
/// started at all once the run is interrupted.
fn run_logged(command: &mut Command, log: &ArmLog, timeout: Duration) -> anyhow::Result<Exit> {
    let (exit, _) = run_capturing(command, log, timeout, false)?;
    Ok(exit)
}

/// Like [`run_logged`], returning the (redacted) output when the command
/// succeeded.
fn output_logged(command: &mut Command, log: &ArmLog, timeout: Duration) -> anyhow::Result<Option<String>> {
    Ok(match run_capturing(command, log, timeout, true)? {
        (Exit::Status(s), captured) if s.success() => Some(captured),
        _ => None,
    })
}

fn run_capturing(
    command: &mut Command,
    log: &ArmLog,
    timeout: Duration,
    capture: bool,
) -> anyhow::Result<(Exit, String)> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if log.interrupted() {
        return Ok((Exit::Interrupted, String::new()));
    }
    let mut child = command.spawn().with_context(|| format!("starting {}", describe(command)))?;
    let pgid = child.id();
    let captured = Arc::new(Mutex::new(String::new()));
    let pumps = [
        pump(child.stdout.take(), log, capture.then(|| Arc::clone(&captured))),
        pump(child.stderr.take(), log, None),
    ];
    let deadline = Instant::now() + timeout;
    let exit = loop {
        if let Some(status) = child.try_wait()? {
            break Exit::Status(status);
        }
        if log.interrupted() {
            stop_group(&mut child, pgid, STOP_GRACE)?;
            break Exit::Interrupted;
        }
        if Instant::now() >= deadline {
            stop_group(&mut child, pgid, STOP_GRACE)?;
            break Exit::TimedOut;
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    // A process the command started can outlive it and keep the pipes open;
    // give the group a moment, then stop it so the pumps reach end of file.
    reap_group(&mut child, pgid)?;
    for p in pumps.into_iter().flatten() {
        let _ = p.join();
    }
    let text = captured.lock().map(|s| s.clone()).unwrap_or_default();
    Ok((exit, text))
}

fn reap_group(child: &mut Child, pgid: u32) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && group_running(child, pgid)? {
        std::thread::sleep(Duration::from_millis(200));
    }
    if group_running(child, pgid)? {
        signal_group(pgid, "KILL");
        let _ = child.wait();
    }
    Ok(())
}

/// Copy `source` line by line into the log (redacted), optionally keeping a
/// copy of the text.
fn pump(
    source: Option<impl Read + Send + 'static>,
    log: &ArmLog,
    keep: Option<Arc<Mutex<String>>>,
) -> Option<JoinHandle<()>> {
    let source = source?;
    let file = Arc::clone(&log.file);
    let redactions = Arc::clone(&log.redactions);
    Some(std::thread::spawn(move || {
        let mut reader = BufReader::new(source);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = String::from_utf8_lossy(&buf);
                    let line = line.trim_end_matches(['\n', '\r']);
                    write_line(&file, &redactions, line);
                    if let Some(keep) = &keep {
                        if let Ok(mut k) = keep.lock() {
                            let _ = writeln!(k, "{}", redact(line, &redactions));
                        }
                    }
                },
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
# comment line
producer-fixed  rust,python  PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 BATCH_SIZE=1024  # trailing
producer-max    rust         PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=0 ENABLE_IDEMPOTENCE=true COMPRESSION_TYPE=lz4
";

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_the_tests_file() {
        let tests = parse_tests(SAMPLE).unwrap();
        assert_eq!(tests.len(), 2);
        assert_eq!(tests[0].id, "producer-fixed");
        assert_eq!(tests[0].groups, vec![Group::Rust, Group::Python]);
        assert_eq!(tests[0].setting("BATCH_SIZE"), Some("1024"));
        assert_eq!(tests[0].partitions(), 200);
        assert_eq!(tests[0].rate_mb_s(), Some(10.24));
        assert_eq!(tests[1].groups, vec![Group::Rust]);
        assert_eq!(tests[1].rate_mb_s(), None);
    }

    #[test]
    fn the_checked_in_tests_file_is_valid() {
        let tests = load_tests().unwrap();
        for group in [Group::Rust, Group::Python] {
            assert!(tests.iter().any(|t| t.applies_to(group)), "no test for {}", group.name());
        }
    }

    #[test]
    fn rejects_bad_test_lines() {
        for (line, message) in [
            ("Bad-Id rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", "invalid test id"),
            ("t", "names no client group"),
            ("t java PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", "unknown client group `java`"),
            ("t rust,rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", "names group `rust` twice"),
            ("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ACKS=1", "unknown setting `ACKS`"),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 LIMIT_RPS=1",
                "`LIMIT_RPS` is set twice",
            ),
            (
                "t rust PARTITIONS=0 VALUE_SIZE=1 LIMIT_RPS=0",
                "PARTITIONS=0: expected a whole number above 0",
            ),
            ("t rust PARTITIONS=+5 VALUE_SIZE=1 LIMIT_RPS=0", "PARTITIONS=+5"),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=-1",
                "LIMIT_RPS=-1: expected a whole number",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 COMPRESSION_TYPE=brotli",
                "COMPRESSION_TYPE=brotli",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ENABLE_IDEMPOTENCE=True",
                "ENABLE_IDEMPOTENCE=True",
            ),
            ("t rust PARTITIONS=1 VALUE_SIZE=1", "must set LIMIT_RPS"),
            (
                "t rust,python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 BATCH_SIZE=16",
                "BATCH_SIZE below 1024 KiB",
            ),
            ("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS", "`LIMIT_RPS` is not KEY=VALUE"),
        ] {
            let err = format!("{:#}", parse_tests(line).unwrap_err());
            assert!(err.contains(message), "{line:?}: {err}");
        }
        let err =
            parse_tests("a rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0\na python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0")
                .unwrap_err()
                .to_string();
        assert!(err.contains(":2: test `a` is defined twice"), "{err}");
        // Small batches are fine where no librdkafka C arm runs.
        parse_tests("t python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 BATCH_SIZE=16").unwrap();
        assert!(parse_tests("# only a comment\n")
            .unwrap_err()
            .to_string()
            .contains("defines no tests"));
    }

    #[test]
    fn selects_tests_in_the_given_order() {
        let all = parse_tests(SAMPLE).unwrap();
        let picked = select_tests(&all, " producer-max , producer-fixed,", Group::Rust).unwrap();
        let ids: Vec<&str> = picked.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["producer-max", "producer-fixed"]);

        let err = select_tests(&all, "producer-nope", Group::Rust).unwrap_err().to_string();
        assert_eq!(err, "unknown test `producer-nope` (known: producer-fixed, producer-max)");
        let err = select_tests(&all, "producer-max", Group::Python).unwrap_err().to_string();
        assert_eq!(err, "test `producer-max` does not apply to client group `python` (only rust)");
        let err = select_tests(&all, "producer-fixed,producer-fixed", Group::Rust)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "test `producer-fixed` is listed twice");
        let err = select_tests(&all, " , ", Group::Rust).unwrap_err().to_string();
        assert_eq!(err, "no tests given (--tests ID,ID)");
    }

    #[test]
    fn parses_flags_and_switches() {
        let flags = parse_flags(
            &strings(&["--group", "rust", "--tests=a,b", "--push-artifacts"]),
            &["--group", "--tests"],
            &["--push-artifacts"],
        )
        .unwrap();
        assert_eq!(flags.get("--group").map(String::as_str), Some("rust"));
        assert_eq!(flags.get("--tests").map(String::as_str), Some("a,b"));
        assert!(flags.contains_key("--push-artifacts"));

        let err = parse_flags(&strings(&["--nope", "x"]), &["--group"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("unknown option `--nope`"), "{err}");
        let err = parse_flags(&strings(&["--group"]), &["--group"], &[]).unwrap_err().to_string();
        assert_eq!(err, "--group needs a value");
        let err = parse_flags(&strings(&["--group", "a", "--group", "b"]), &["--group"], &[])
            .unwrap_err()
            .to_string();
        assert_eq!(err, "--group is given twice");
        let err = parse_flags(&strings(&["rust"]), &["--group"], &[]).unwrap_err().to_string();
        assert_eq!(err, "unexpected argument `rust`");
    }

    #[test]
    fn validates_minutes() {
        let flags = parse_flags(&strings(&["--duration-min", "60", "--warmup-min", "0"]), SHARED_FLAGS, &[]).unwrap();
        assert_eq!(minutes(&flags, "--duration-min", 30, 1, 60).unwrap(), 60);
        assert_eq!(minutes(&flags, "--warmup-min", 5, 0, 30).unwrap(), 0);
        assert_eq!(minutes(&BTreeMap::new(), "--duration-min", 30, 1, 60).unwrap(), 30);
        let flags = parse_flags(&strings(&["--duration-min", "61"]), SHARED_FLAGS, &[]).unwrap();
        assert_eq!(
            minutes(&flags, "--duration-min", 30, 1, 60).unwrap_err().to_string(),
            "--duration-min must be between 1 and 60 minutes, got 61"
        );
        let flags = parse_flags(&strings(&["--duration-min", "half"]), SHARED_FLAGS, &[]).unwrap();
        assert_eq!(
            minutes(&flags, "--duration-min", 30, 1, 60).unwrap_err().to_string(),
            "--duration-min: `half` is not a whole number of minutes"
        );
    }

    fn options(group: Group, tests: usize, duration_min: u64) -> RunOptions {
        let test = parse_tests("t rust,python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        RunOptions {
            group,
            tests: vec![test; tests],
            duration_min,
            warmup_min: 5,
            budget_min: None,
            ingress_mb_s: None,
            bootstrap: String::new(),
            kafka_bin: PathBuf::new(),
            results_dir: PathBuf::new(),
            topic_prefix: String::new(),
            push_artifacts: false,
        }
    }

    #[test]
    fn estimates() {
        // ~1 h 50 min per rust test at 30 min, ~4 h 30 min per python test at
        // 60 min.
        assert_eq!(options(Group::Rust, 1, 30).estimate_minutes(), 111);
        assert_eq!(options(Group::Python, 1, 60).estimate_minutes(), 268);
    }

    #[test]
    fn check_rejects_a_list_over_the_budget() {
        let with_budget = |tests: usize, budget: u64| RunOptions {
            budget_min: Some(budget),
            ingress_mb_s: Some(240.0),
            ..options(Group::Python, tests, 60)
        };
        let err = check(&with_budget(6, 1340)).unwrap_err().to_string();
        assert_eq!(
            err,
            "6 tests take ~26 h 48 min, more than the budget of 22 h 20 min; at 60 min per arm at most 5 test(s) fit"
        );
        check(&with_budget(5, 1340)).unwrap();
        // Without a budget any length passes.
        check(&options(Group::Python, 6, 60)).unwrap();
    }

    #[test]
    fn check_flags_are_parsed() {
        let args = |extra: &[&str]| -> Vec<String> {
            ["--group", "rust", "--tests", "producer-fixed-10k-1kb-200p"]
                .iter()
                .chain(extra)
                .map(|s| s.to_string())
                .collect()
        };
        let options = RunOptions::parse(&args(&["--budget-min", "90", "--ingress-mb-s", "600"]), false).unwrap();
        assert_eq!(options.budget_min, Some(90));
        assert_eq!(options.ingress_mb_s, Some(600.0));
        let options = RunOptions::parse(&args(&[]), false).unwrap();
        assert_eq!((options.budget_min, options.ingress_mb_s), (None, None));
        assert_eq!(
            RunOptions::parse(&args(&["--ingress-mb-s", "-1"]), false)
                .err()
                .unwrap()
                .to_string(),
            "--ingress-mb-s: `-1` is not a positive number"
        );
        // `run` does not take the `check` flags.
        assert!(RunOptions::parse(&args(&["--budget-min", "90"]), true).is_err());
    }

    #[test]
    fn each_harness_gets_its_own_boolean_spelling() {
        let test = parse_tests("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ENABLE_IDEMPOTENCE=true")
            .unwrap()
            .remove(0);
        let idempotence = |arm: Arm| {
            test.env_for(arm)
                .into_iter()
                .find(|(k, _)| k == "ENABLE_IDEMPOTENCE")
                .map(|(_, v)| v)
        };
        assert_eq!(idempotence(Arm::Librdkafka).as_deref(), Some("True"));
        assert_eq!(idempotence(Arm::Rust).as_deref(), Some("true"));
        assert_eq!(idempotence(Arm::Java).as_deref(), Some("true"));
        assert_eq!(idempotence(Arm::PythonSyncLibrdkafka).as_deref(), Some("true"));
        assert_eq!(Arm::Librdkafka.idempotence_value(false), "False");
    }

    #[test]
    fn python_arms_select_backend_and_mode() {
        let repo = Path::new("/repo");
        let env = |arm: Arm| -> BTreeMap<String, String> { arm.own_env(repo).into_iter().collect() };
        let sync_rust = env(Arm::PythonSyncRust);
        assert_eq!(sync_rust["CLIENT_VERSION"], "3");
        assert_eq!(sync_rust["ASYNC"], "False");
        assert_eq!(sync_rust["LIBRDKAFKA_PYTHON"], "/repo/venv-librdkafka/bin/python");
        let async_librdkafka = env(Arm::PythonAsyncLibrdkafka);
        assert_eq!(async_librdkafka["CLIENT_VERSION"], "2");
        assert_eq!(async_librdkafka["ASYNC"], "True");
        assert_eq!(env(Arm::Librdkafka)["CLIENT_VERSION"], "2");
        assert!(env(Arm::Rust).is_empty());
        assert_eq!(
            Arm::PythonAsyncLibrdkafka.command(repo).get_program(),
            "/repo/venv-librdkafka/bin/python"
        );
    }

    #[test]
    fn every_setting_is_cleared_before_an_arm_starts() {
        for (key, _) in SETTINGS {
            assert!(HARNESS_VARS.contains(key), "{key} is not in HARNESS_VARS");
        }
    }

    #[test]
    fn group_arms() {
        assert_eq!(Group::Rust.arms(), &[Arm::Rust, Arm::Librdkafka, Arm::Java]);
        assert_eq!(Group::Python.arms().len(), 4);
        assert_eq!(
            Group::parse("java").unwrap_err().to_string(),
            "unknown client group `java` (expected `rust` or `python`)"
        );
    }

    #[test]
    fn topic_names_and_prefixes() {
        let test = parse_tests("producer-fixed rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        assert_eq!(
            topic_name("perf-abc", &test, Arm::PythonAsyncLibrdkafka),
            "perf-abc-producer-fixed-python-async-librdkafka"
        );
        validate_topic_prefix("perf-0d0c9b9e-1f7a-4d0b-9c39-ff4b6a0f1d2e").unwrap();
        assert!(validate_topic_prefix("perf/x").is_err());
        assert!(validate_topic_prefix("").is_err());
        // The longest name stays well inside Kafka's 249 characters.
        let longest = format!(
            "{}-{}-{}",
            "p".repeat(100),
            "t".repeat(MAX_TEST_ID_LEN),
            Arm::PythonAsyncLibrdkafka.id()
        );
        assert!(longest.len() <= 249);
    }

    #[test]
    fn reads_a_results_file() {
        let m = Metrics::from_results(
            r#"{"test": "producer", "client": "rust", "messages_measured": 600000, "duration_s": 60.00,
                "throughput_msg_s": 10000.00, "throughput_mib_s": 9.77,
                "latency_ms": {"min": 1, "avg": 3.25, "p50": 3, "p90": 4, "p95": 5, "p99": 8, "p999": 12, "max": 40},
                "cpu_avg_pct": 21.50, "rss_avg_kib": 51200.00}"#,
        )
        .unwrap();
        assert_eq!(m.client, "rust");
        assert_eq!(m.msg_s, 10000.0);
        assert_eq!(m.p99, 8.0);
        assert_eq!(m.rss_mib, 50.0);
        let err = Metrics::from_results(r#"{"client": "rust", "throughput_msg_s": 1}"#)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "no `latency_ms`");
    }

    #[test]
    fn masks_the_secret_and_escapes_jaas_values() {
        let redactions = vec!["s3cr3t".to_string()];
        assert_eq!(
            redact("password=\"s3cr3t\" again s3cr3t", &redactions),
            "password=\"<redacted>\" again <redacted>"
        );
        assert_eq!(redact("nothing here", &[String::new()]), "nothing here");
        let text = client_properties_text("b:9092", &Credentials { key: "K".into(), secret: "a\"b\\c".into() });
        assert!(text.contains(r#"username="K" password="a\"b\\c";"#), "{text}");
        assert!(
            text.starts_with("bootstrap.servers=b:9092\nsecurity.protocol=SASL_SSL\n"),
            "{text}"
        );
    }

    #[test]
    fn runs_a_command_with_redacted_output_and_a_timeout() {
        // A signal another test raises would read as an interrupt here.
        let _signals = crate::signals::test_lock();
        let dir = std::env::temp_dir().join(format!("perf-cloud-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let log = ArmLog::create(
            &dir.join("run.log"),
            Arc::new(vec!["hunter2".to_string()]),
            crate::signals::count(),
        )
        .unwrap();

        let mut ok = Command::new("sh");
        ok.args(["-c", "echo out hunter2; echo err >&2"]);
        let text = output_logged(&mut ok, &log, Duration::from_secs(30)).unwrap();
        assert_eq!(text.as_deref(), Some("out <redacted>\n"));

        let mut failing = Command::new("sh");
        failing.args(["-c", "exit 3"]);
        assert!(output_logged(&mut failing, &log, Duration::from_secs(30)).unwrap().is_none());

        // A child that outlives the timeout is stopped, and a background
        // process it left holding the pipes does not hang the driver.
        let mut slow = Command::new("sh");
        slow.args(["-c", "sleep 60 & sleep 60"]);
        let started = Instant::now();
        assert!(matches!(
            run_logged(&mut slow, &log, Duration::from_secs(1)).unwrap(),
            Exit::TimedOut
        ));
        assert!(started.elapsed() < Duration::from_secs(20), "took {:?}", started.elapsed());

        let logged = fs::read_to_string(dir.join("run.log")).unwrap();
        assert!(logged.contains("out <redacted>") && logged.contains("err"), "{logged}");
        assert!(!logged.contains("hunter2"), "{logged}");
        fs::remove_dir_all(&dir).unwrap();
    }
}
