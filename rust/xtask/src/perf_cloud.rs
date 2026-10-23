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

//! `cargo xtask perf-cloud`: runs the producer, transactional producer and
//! consumer performance harnesses against a remote SASL_SSL (PLAIN) cluster, such as a
//! Confluent Cloud cluster, and compares the clients. Whatever provisions the
//! cluster (a CI pipeline, an operator) calls it with the cluster's bootstrap
//! and credentials.
//!
//! A run compares the clients of one **client group** (all of them, or the
//! ones `--clients` selects) on a list of tests: presets from [`TESTS_FILE`]
//! (`--tests`), a named list of them from [`BATCHES_FILE`] (`--batch`), or
//! one test given by its settings (`--scenario`) and named after them. The
//! tests run one after another, and each runs every selected client (an
//! **arm**), one after another, against the same cluster:
//!
//! - `rust`: Rust (native), librdkafka (C) and Java. Rust and Java are each
//!   compared with librdkafka, the baseline; Rust is never compared with
//!   Java.
//! - `python`: the Rust-backed and the librdkafka-backed Python clients, sync
//!   and async. Each Rust-backed client is compared with its librdkafka
//!   equivalent (sync with sync, async with async).
//!
//! A test's `WORKLOAD` is `producer` (the producer harnesses, the default),
//! `transactional` (the transactional producer harnesses, in `TXN_MODE`
//! `produce` or `eos`) or `consumer` (the consumer harnesses, reading a load
//! this driver produces at the test's rate with Kafka's
//! `kafka-producer-perf-test.sh` from `--kafka-bin`).
//!
//! Subcommands:
//!
//! - `list` prints the tests and the batches.
//! - `check` validates a run (group, clients, tests, durations) and prints
//!   its time estimate, failing above `--budget-min`. It needs no cluster, so
//!   a caller can run it before provisioning one: a typo or an over-long list
//!   then costs seconds, not a provisioning cycle.
//! - `build` builds the group's harnesses once.
//! - `run` creates each arm's topics, runs the arm, collects its
//!   `results.json` and deletes the topics, then writes `report.md` (per
//!   test, a column per client and a row per metric, with each client's
//!   change against its librdkafka baseline) and `junit.xml`. With
//!   `--push-artifacts` they also go to Semaphore: the job's artifacts, and
//!   `report.md` as the workflow's report (its Report tab). While a test
//!   runs, `report.md` is the report so far, written every few minutes with
//!   the running client's numbers so far (see [`live`]).
//!
//! The arms are the existing (transactional) producer and consumer
//! harnesses, driven by the same environment variables (or, for the Rust, C
//! and Java consumers, options) `tools/deploy_and_run_perf/deploy_and_run_perf.py`
//! sets on EC2. Topics are created here, never by a harness
//! (`CREATE_TOPIC=False`): the Rust harnesses cannot create topics, and one
//! creation path gives every arm the same topic. An `eos` test also gets a
//! source topic, which its harness fills with Kafka's
//! `kafka-producer-perf-test.sh` from `--kafka-bin` before measuring.
//!
//! The SASL credentials come from `PERF_API_KEY` / `PERF_API_SECRET`, never
//! from the command line, and both are masked in everything this driver
//! prints or writes (harness output included), because the results directory
//! is meant to be published (e.g. as CI artifacts).
//!
//! Optional run metadata, recorded in the report when set: `AGENT_AZ_ID`,
//! `PERF_CLUSTER_SIZE` (free text, e.g. `10 CKU`) and
//! `PERF_CLUSTER_INGRESS_MB_S` (with it, a throughput Δ whose clients came
//! near that limit reads `cluster-bound`). `AGENT_REGION`, `PERF_CLUSTER_ID`, and on Semaphore
//! `SEMAPHORE_AGENT_MACHINE_TYPE` and `SEMAPHORE_WORKFLOW_ID`, go to the log
//! only, with this repository's `HEAD` commit and the kernel. The report also
//! gives the cluster's AZ IDs, which the driver reads from the cluster itself
//! (see [`cluster_zones`]).

mod live;
mod report;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead as _, BufReader, Read, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};

use self::live::{LiveReport, RunningArm};
use crate::chaos_matrix::{group_running, now_utc, package_root, signal_group, stop_group};

/// The tests that can be named in a run, relative to the repository root.
const TESTS_FILE: &str = "tools/perf-cloud/tests.txt";

/// Named lists of those tests (`--batch`), relative to the repository root.
const BATCHES_FILE: &str = "tools/perf-cloud/batches.txt";

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

/// How long an `artifact push` may take.
const PUSH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Each `kafka-topics.sh` call.
const TOPIC_COMMAND_TIMEOUT: Duration = Duration::from_secs(180);

/// The `kafka-broker-api-versions.sh` call that reads the brokers' racks.
const BROKERS_COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// Topic creation retries: a new API key can take a few minutes before the
/// cluster accepts it.
const TOPIC_CREATE_ATTEMPTS: u32 = 8;
const TOPIC_CREATE_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The rate at which an `eos` test's harness is assumed to fill its source
/// topic (Kafka's `kafka-producer-perf-test.sh`, one Java producer), in MB/s:
/// the time estimate and the arm's timeout allow for it.
const SEED_MB_S: f64 = 50.0;

/// The most requests in flight an idempotent (or transactional) producer
/// allows.
const IDEMPOTENT_MAX_IN_FLIGHT: u64 = 5;

/// Every environment variable a harness reads, its A/B debugging knobs
/// included. Each arm starts from the job's environment with all of them
/// removed, so a stray value in the job cannot change one arm and not the
/// others.
const HARNESS_VARS: &[&str] = &[
    "ABORT_RATE",
    "ACKS",
    "ASYNC",
    "BATCH_SIZE",
    "BOOTSTRAP_SERVERS",
    "BUFFER_MEMORY",
    "CLIENT_VERSION",
    "COMPRESSION_TYPE",
    "CONSUMER_BATCH_SIZE",
    "CREATE_TOPIC",
    "DO_VERIFY",
    "ENABLE_IDEMPOTENCE",
    "FETCH_MAX_WAIT_MS",
    "FETCH_MIN_BYTES",
    "GROUP_ID",
    "INTERVAL_SECONDS",
    "JOIN_TIMEOUT_SECONDS",
    "KAFKA_BIN",
    "KEY_SIZE",
    "LIBRDKAFKA_BATCH",
    "LIBRDKAFKA_PYTHON",
    "LIMIT_RPS",
    "LINGER_MS",
    "MAX_IN_FLIGHT",
    "MAX_PARTITION_FETCH_BYTES",
    "MAX_POLL_RECORDS",
    "MAX_REQUEST_SIZE",
    "METRICS_FILE",
    "NO_MSG_F_BLOCK",
    "NUM_MESSAGES",
    "NUM_TRANSACTIONAL_PRODUCERS",
    "P99_LIMIT_MS",
    "PARTITIONER",
    "PARTITIONS",
    "PERF_DEBUG_TIMING",
    "PERF_LRK_STATS",
    "PERF_SETUP_ATTEMPTS",
    "POLL_SINGLE",
    "POLL_TIMEOUT_MS",
    "READINESS_MIN_RECORDS",
    "READINESS_TIMEOUT_SECONDS",
    "RECORDS_PER_TRANSACTION",
    "RESULTS_FILE",
    "SASL_MECHANISM",
    "SASL_PASSWORD",
    "SASL_USERNAME",
    "SECURITY_PROTOCOL",
    "SEED_COUNT",
    "SEED_THROUGHPUT",
    "SETTLE_TIMEOUT_SECONDS",
    "SKIP_PARTITIONER_OVERRIDE",
    "SOURCE_TOPIC",
    "SSL_CA_LOCATION",
    "STICKY_LINGER_MS",
    "TEST_DURATION_SECONDS",
    "THROUGHPUT",
    "TOPIC_NAME",
    "TRANSACTIONAL_ID",
    "TXN_MODE",
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
                           and the batches of them (tools/perf-cloud/batches.txt)
  check  --group G [--clients C,C] (--tests ID,ID | --batch B | --scenario S)
         [--duration-min N] [--warmup-min N] [--budget-min N] [--ingress-mb-s N]
                           Validate a run and print its time estimate; fail when
                           it exceeds --budget-min, warn about fixed-rate tests
                           near the cluster's --ingress-mb-s
  build  --group G         Build the group's harnesses
  run    --group G [--clients C,C] (--tests ID,ID | --batch B | --scenario S)
         [--duration-min N] [--warmup-min N]
         --bootstrap HOST:PORT --kafka-bin DIR --results-dir DIR
         [--topic-prefix P] [--push-artifacts]
                           Run the tests (credentials from PERF_API_KEY / PERF_API_SECRET);
                           --topic-prefix defaults to perf-local-<Unix time>, new for each
                           run, and its topics must not exist yet;
                           --push-artifacts pushes each finished test's results with
                           Semaphore's `artifact` CLI, report.md also as the workflow report,
                           which also gets the report so far every 5 min while a test runs

  G is `rust` (Rust, librdkafka, Java) or `python` (Rust- and librdkafka-backed
  Python clients, sync and async). C is `all` (the default) or the group's
  clients to run: rust, librdkafka, java / python-sync-rust,
  python-sync-librdkafka, python-async-rust, python-async-librdkafka.
  B is a batch of the group's tests, run one after another on the same cluster,
  e.g. producer-basic-perf-python.
  S is one test given by its settings, as in the tests file, e.g.
  \"WORKLOAD=producer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 ACKS=all\"
  or \"WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000\"; it is
  named after them (producer-fixed-10k-1kb-200p, consumer-fixed-10k-1kb-200p). Durations are minutes:
  --duration-min defaults to 30 (at most 60), --warmup-min to 5 (at most 30).";

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

    /// The arms `--clients` names (`all`, or arm ids separated by commas), in
    /// the group's order.
    fn select_arms(self, list: &str) -> anyhow::Result<Vec<Arm>> {
        let ids: Vec<&str> = self.arms().iter().map(|a| a.id()).collect();
        let list = list.trim();
        if list == "all" {
            return Ok(self.arms().to_vec());
        }
        let mut named: Vec<Arm> = Vec::new();
        for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let Some(arm) = self.arms().iter().find(|a| a.id() == name) else {
                bail!(
                    "unknown client `{name}` in group `{}` (expected `all` or some of: {})",
                    self.name(),
                    ids.join(", ")
                );
            };
            if named.contains(arm) {
                bail!("client `{name}` is listed twice");
            }
            named.push(*arm);
        }
        if named.is_empty() {
            bail!("no clients given (--clients all, or some of: {})", ids.join(", "));
        }
        Ok(self.arms().iter().copied().filter(|a| named.contains(a)).collect())
    }
}

/// What a test measures. It picks the harnesses each arm runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Workload {
    /// The producer harnesses.
    Producer,
    /// The transactional producer harnesses (`TXN_MODE` `produce` or `eos`).
    Transactional,
    /// The consumer harnesses, reading a load this driver produces at the
    /// test's rate with Kafka's `kafka-producer-perf-test.sh` ([`LoadGenerator`]).
    Consumer,
}

impl Workload {
    fn parse(value: &str) -> anyhow::Result<Workload> {
        match value {
            "producer" => Ok(Workload::Producer),
            "transactional" => Ok(Workload::Transactional),
            "consumer" => Ok(Workload::Consumer),
            other => bail!("WORKLOAD={other}: expected `producer`, `transactional` or `consumer`"),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Workload::Producer => "producer",
            Workload::Transactional => "transactional",
            Workload::Consumer => "consumer",
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

    /// Its name in the report and the logs. Never `(C)` or `(R)`, which
    /// Confluence and Semaphore render as © and ®.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Arm::Rust => "Rust",
            Arm::Librdkafka => "librdkafka",
            Arm::Java => "Java",
            Arm::PythonSyncRust => "Python sync (Rust)",
            Arm::PythonSyncLibrdkafka => "Python sync (librdkafka)",
            Arm::PythonAsyncRust => "Python async (Rust)",
            Arm::PythonAsyncLibrdkafka => "Python async (librdkafka)",
        }
    }

    /// The `client` its harness writes into `results.json`, checked so a
    /// wrong `CLIENT_VERSION` / `ASYNC` cannot pass as the arm. The Python
    /// transactional harness writes the same name for sync and async.
    fn expected_client(self, workload: Workload) -> &'static str {
        match (workload, self) {
            (Workload::Producer, Arm::Rust) => "rust",
            (Workload::Producer, Arm::Librdkafka) => "librdkafka",
            (Workload::Producer, Arm::Java) => "java",
            (Workload::Producer, Arm::PythonSyncRust) => "python-rust",
            (Workload::Producer, Arm::PythonSyncLibrdkafka) => "python-librdkafka",
            (Workload::Producer, Arm::PythonAsyncRust) => "python-rust-async",
            (Workload::Producer, Arm::PythonAsyncLibrdkafka) => "python-librdkafka-async",
            (Workload::Transactional, Arm::Rust) => "rust-txn",
            (Workload::Transactional, Arm::Librdkafka) => "librdkafka-txn",
            (Workload::Transactional, Arm::Java) => "java-txn",
            (Workload::Transactional, Arm::PythonSyncRust | Arm::PythonAsyncRust) => "python-rust-txn",
            (Workload::Transactional, Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka) => {
                "python-librdkafka-txn"
            },
            (Workload::Consumer, Arm::Rust) => "rust-consumer",
            (Workload::Consumer, Arm::Librdkafka) => "librdkafka-consumer",
            (Workload::Consumer, Arm::Java) => "java-consumer",
            (Workload::Consumer, Arm::PythonSyncRust) => "python-rust-consumer",
            (Workload::Consumer, Arm::PythonSyncLibrdkafka) => "python-librdkafka-consumer",
            (Workload::Consumer, Arm::PythonAsyncRust) => "python-rust-async-consumer",
            (Workload::Consumer, Arm::PythonAsyncLibrdkafka) => "python-librdkafka-async-consumer",
        }
    }

    /// The command that runs this arm's harness for `workload` (the
    /// `RUN_PERF_SH` recipes of `deploy_and_run_perf.py`), to be started in
    /// the arm's directory unless it sets its own. A consumer harness other
    /// than Python's also takes [`consumer_args`].
    fn command(self, repo: &Path, workload: Workload) -> Command {
        let transactional = workload == Workload::Transactional;
        match self {
            Arm::Rust if workload == Workload::Consumer => Command::new(rust_consumer_harness(repo)),
            Arm::Java if workload == Workload::Consumer => {
                let mut c = Command::new("java");
                let classpath = format!("{}:{}", java_jar(repo).display(), java_consumer_classes(repo).display());
                c.arg("-cp").arg(classpath).arg("JavaE2E");
                c
            },
            Arm::Rust => {
                let task = if transactional {
                    "transactional-producer-perf-test"
                } else {
                    "producer-perf-test"
                };
                let mut c = Command::new(cargo());
                c.args(["xtask", task, "--test-threads=1"]).current_dir(repo.join("rust"));
                c
            },
            Arm::Librdkafka => Command::new(c_harness(repo, workload)),
            Arm::Java => {
                let mut c = Command::new("java");
                if transactional {
                    c.arg("-cp")
                        .arg(java_jar(repo))
                        .arg("io.confluent.kafka.perftest.TransactionalProducerPerformanceTest");
                } else {
                    c.arg("-jar").arg(java_jar(repo));
                }
                c
            },
            Arm::PythonSyncRust | Arm::PythonAsyncRust => {
                let mut c = Command::new(venv_python(repo, "venv"));
                c.arg(python_harness(repo, workload));
                c
            },
            Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka => {
                let mut c = Command::new(venv_python(repo, "venv-librdkafka"));
                c.arg(python_harness(repo, workload));
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

    /// What must exist before the arm can run `workload`, from `build`.
    fn prerequisites(self, repo: &Path, workload: Workload) -> Vec<PathBuf> {
        match self {
            Arm::Rust if workload == Workload::Consumer => vec![rust_consumer_harness(repo)],
            // Built by `cargo` on first use; `build` compiles it ahead.
            Arm::Rust => Vec::new(),
            Arm::Librdkafka => vec![c_harness(repo, workload)],
            Arm::Java if workload == Workload::Consumer => {
                vec![java_jar(repo), java_consumer_classes(repo).join("JavaE2E.class")]
            },
            Arm::Java => vec![java_jar(repo)],
            Arm::PythonSyncRust | Arm::PythonAsyncRust => vec![venv_python(repo, "venv")],
            Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka => vec![venv_python(repo, "venv-librdkafka")],
        }
    }
}

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

fn c_harness(repo: &Path, workload: Workload) -> PathBuf {
    match workload {
        Workload::Producer => repo.join("c/build/producer_perf_test"),
        Workload::Transactional => repo.join("c/build/transactional_producer_perf_test"),
        // Where `deploy_and_run_perf.py` builds it too.
        Workload::Consumer => consumer_compare_dir(repo).join("build/librdkafka_e2e"),
    }
}

/// The consumer harnesses of librdkafka (C) and Java.
fn consumer_compare_dir(repo: &Path) -> PathBuf {
    repo.join("rust/consumer-perf/compare")
}

fn rust_consumer_harness(repo: &Path) -> PathBuf {
    repo.join("rust/target/release/consumer-perf")
}

fn java_jar(repo: &Path) -> PathBuf {
    repo.join("tools/java-perf-test/build/libs/java-perf-test-all.jar")
}

/// Where `JavaE2E.class` is compiled to, against [`java_jar`]'s classes.
fn java_consumer_classes(repo: &Path) -> PathBuf {
    consumer_compare_dir(repo).join("build")
}

fn python_harness(repo: &Path, workload: Workload) -> PathBuf {
    match workload {
        Workload::Producer => repo.join("python/test/performance/producer_performance_test.py"),
        Workload::Transactional => repo.join("python/test/performance/transactional_producer_performance_test.py"),
        Workload::Consumer => repo.join("python/test/performance/consumer_performance_test.py"),
    }
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

/// A test: a line of [`TESTS_FILE`], or a `--scenario`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PerfTest {
    pub(crate) id: String,
    groups: Vec<Group>,
    pub(crate) workload: Workload,
    /// Harness settings in the given order, values already validated.
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
    Acks,
    /// A decimal number from 0 to 1.
    Fraction,
    TxnMode,
}

/// A harness setting a test may set.
struct Setting {
    key: &'static str,
    kind: Kind,
    /// The largest value of a [`Kind::Count`] or [`Kind::Positive`] setting.
    max: u64,
    /// The workloads whose harnesses read it; empty for all of them.
    only: &'static [Workload],
}

const fn setting(key: &'static str, kind: Kind, only: &'static [Workload]) -> Setting {
    Setting { key, kind, max: 0, only }
}

const fn number_setting(key: &'static str, kind: Kind, max: u64, only: &'static [Workload]) -> Setting {
    Setting { key, kind, max, only }
}

/// The settings of every workload.
const ALL: &[Workload] = &[];
/// The producers' settings. A consumer test's load comes from Kafka's
/// `kafka-producer-perf-test.sh` with its own defaults, which only
/// `COMPRESSION_TYPE` changes.
const PRODUCING: &[Workload] = &[Workload::Producer, Workload::Transactional];

/// The harness settings a test may set. Every harness of a workload reads
/// all of that workload's settings, in the same units, except that the C
/// harnesses raise a `BATCH_SIZE` below 1024 KiB to 1 MiB for librdkafka (see
/// [`MIN_LIBRDKAFKA_BATCH_KIB`]). A setting a test leaves out takes each
/// harness's own default, and those differ (`MAX_IN_FLIGHT`: 1000 in the C
/// producer harness, the client default of 5 in Rust and Java), as in the EC2
/// runs; set it in the test to pin it. Units: `BATCH_SIZE` /
/// `MAX_REQUEST_SIZE` in KiB, `BUFFER_MEMORY` in MiB. The transactional
/// harnesses always use idempotence and `acks=all`.
///
/// Each number has a maximum: the harnesses read most of them as 32-bit
/// integers (the C harnesses with `atoi`, Java with `Integer.parseInt`, which
/// falls back to its default on a value it cannot read), and a client's own
/// limit is lower for some (librdkafka's `linger.ms`, `fetch.min.bytes`,
/// `fetch.wait.max.ms`). A larger value would fail, or run differently in
/// each harness, only after the cluster is paid for.
///
/// In a consumer test `PARTITIONS`, `VALUE_SIZE`, `LIMIT_RPS` and
/// `COMPRESSION_TYPE` shape the load, and the consumer settings left out keep
/// each client's default (the harnesses are told to: `USE_DEFAULTS=True`).
/// Those defaults agree: a 1-byte `fetch.min.bytes`, a 500 ms wait, 1 MiB per
/// partition and 500 records per poll (librdkafka has no `max.poll.records`;
/// its harnesses consume batches of `MAX_POLL_RECORDS`, 500 by default).
/// `LIBRDKAFKA_BATCH` changes how the librdkafka clients alone read (see
/// [`LibrdkafkaRead`]): the others always poll batches of up to
/// `max.poll.records`.
const SETTINGS: &[Setting] = &[
    number_setting("PARTITIONS", Kind::Positive, 10_000, ALL),
    // Bytes; see also MAX_RECORD_BYTES.
    number_setting("VALUE_SIZE", Kind::Positive, MAX_RECORD_BYTES, ALL),
    number_setting("KEY_SIZE", Kind::Count, MAX_RECORD_BYTES, PRODUCING),
    // Records per second: with the longest warmup and measurement, the record
    // counts derived from it stay 32-bit.
    number_setting("LIMIT_RPS", Kind::Count, 1_000_000, ALL),
    // KiB: 64 MiB.
    number_setting("BATCH_SIZE", Kind::Positive, 65_536, PRODUCING),
    // librdkafka's maximum.
    number_setting("LINGER_MS", Kind::Count, 900_000, PRODUCING),
    number_setting("MAX_IN_FLIGHT", Kind::Positive, 1_000_000, PRODUCING),
    // MiB: 16 GiB.
    number_setting("BUFFER_MEMORY", Kind::Positive, 16_384, PRODUCING),
    // KiB: 100 MiB.
    number_setting("MAX_REQUEST_SIZE", Kind::Positive, 102_400, PRODUCING),
    setting("COMPRESSION_TYPE", Kind::Compression, ALL),
    setting("ACKS", Kind::Acks, &[Workload::Producer]),
    setting("ENABLE_IDEMPOTENCE", Kind::Bool, &[Workload::Producer]),
    number_setting("RECORDS_PER_TRANSACTION", Kind::Positive, 1_000_000, &[Workload::Transactional]),
    setting("ABORT_RATE", Kind::Fraction, &[Workload::Transactional]),
    number_setting("NUM_TRANSACTIONAL_PRODUCERS", Kind::Positive, 64, &[Workload::Transactional]),
    setting("TXN_MODE", Kind::TxnMode, &[Workload::Transactional]),
    // Bytes; librdkafka takes 1 to 100,000,000.
    number_setting("FETCH_MIN_BYTES", Kind::Positive, 100_000_000, &[Workload::Consumer]),
    // Milliseconds (librdkafka's `fetch.wait.max.ms`, at most 300,000).
    number_setting("FETCH_MAX_WAIT_MS", Kind::Count, 300_000, &[Workload::Consumer]),
    // Bytes (librdkafka's `fetch.message.max.bytes`, at most 1,000,000,000).
    number_setting(
        "MAX_PARTITION_FETCH_BYTES",
        Kind::Positive,
        1_000_000_000,
        &[Workload::Consumer],
    ),
    // The C consumer harness allocates an array of this many entries.
    number_setting("MAX_POLL_RECORDS", Kind::Positive, 1_000_000, &[Workload::Consumer]),
    // Records; 0 polls one at a time. The same array as MAX_POLL_RECORDS.
    number_setting("LIBRDKAFKA_BATCH", Kind::Count, 1_000_000, &[Workload::Consumer]),
];

/// How the librdkafka clients (C, and Python on librdkafka) read in a
/// consumer test: `LIBRDKAFKA_BATCH=0` polls one record at a time
/// (`rd_kafka_consumer_poll`, Python's `Consumer.poll()`), `LIBRDKAFKA_BATCH=N`
/// consumes batches of N (`rd_kafka_consume_batch_queue`, Python's
/// `Consumer.consume(num_messages=N)`). Unset, they consume batches of
/// `MAX_POLL_RECORDS` (500 by default), the most records a Rust or Java poll
/// returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LibrdkafkaRead {
    Poll,
    Batch(u64),
}

impl LibrdkafkaRead {
    fn of(test: &PerfTest) -> Option<LibrdkafkaRead> {
        match lookup_number(&test.settings, "LIBRDKAFKA_BATCH")? {
            0 => Some(LibrdkafkaRead::Poll),
            n => Some(LibrdkafkaRead::Batch(n)),
        }
    }
}

/// The largest record (key plus value, in bytes) a test may produce: under
/// the clients' default 1 MiB request size (Java's and the Rust client's
/// `max.request.size`, which `kafka-producer-perf-test.sh` keeps too) with
/// room for the batch and record headers, and under Confluent Cloud's default
/// 2 MiB `max.message.bytes`. A larger record is rejected by some clients
/// only, or by the broker, after the cluster is paid for.
const MAX_RECORD_BYTES: u64 = 1_000_000;

/// The bytes a request needs on top of its one record (batch and record
/// headers), when `MAX_REQUEST_SIZE` bounds the record.
const REQUEST_OVERHEAD_BYTES: u64 = 512;

/// Set by every test, so no arm falls back to a harness default the others
/// might not share. `LIMIT_RPS=0` is an unbounded test.
const REQUIRED_SETTINGS: &[&str] = &["PARTITIONS", "VALUE_SIZE", "LIMIT_RPS"];

const MAX_TEST_ID_LEN: usize = 60;

/// The smallest `BATCH_SIZE` (KiB) a test may set when the librdkafka (C)
/// client runs: below it the C harnesses run librdkafka with 1 MiB batches
/// (`c/tests/producer_perf_test.c`, `c/tests/transactional_producer_perf_test.c`)
/// while the other clients use the value, so they would not be comparable.
const MIN_LIBRDKAFKA_BATCH_KIB: u64 = 1024;

/// The value `settings` give `key`.
fn lookup<'a>(settings: &'a [(String, String)], key: &str) -> Option<&'a str> {
    settings.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// The whole number `settings` give `key`.
fn lookup_number(settings: &[(String, String)], key: &str) -> Option<u64> {
    lookup(settings, key).and_then(|v| v.parse().ok())
}

/// The bytes of one record (key plus value).
fn record_bytes(settings: &[(String, String)]) -> u64 {
    lookup_number(settings, "KEY_SIZE").unwrap_or(0) + lookup_number(settings, "VALUE_SIZE").unwrap_or(0)
}

impl PerfTest {
    pub(crate) fn setting(&self, key: &str) -> Option<&str> {
        lookup(&self.settings, key)
    }

    fn number(&self, key: &str) -> u64 {
        // Validated when the test was parsed.
        lookup_number(&self.settings, key).unwrap_or(0)
    }

    fn partitions(&self) -> u64 {
        self.number("PARTITIONS")
    }

    /// A transactional test in `TXN_MODE=eos`, which consumes from a source
    /// topic.
    pub(crate) fn is_eos(&self) -> bool {
        self.workload == Workload::Transactional && self.setting("TXN_MODE") == Some("eos")
    }

    /// The target ingress in MB/s (payload only), `None` when unbounded.
    fn rate_mb_s(&self) -> Option<f64> {
        let rps = self.number("LIMIT_RPS");
        if rps == 0 {
            return None;
        }
        Some(rps as f64 * record_bytes(&self.settings) as f64 / 1e6)
    }

    /// The minutes an `eos` arm's harness takes to fill its source topic with
    /// the records it then consumes (`LIMIT_RPS` × the measured seconds), at
    /// [`SEED_MB_S`]; 0 for other tests.
    fn seed_minutes(&self, duration_min: u64) -> u64 {
        if !self.is_eos() {
            return 0;
        }
        let size = record_bytes(&self.settings);
        let bytes = self.number("LIMIT_RPS") as f64 * (duration_min * 60) as f64 * size as f64;
        (bytes / (SEED_MB_S * 1e6) / 60.0).ceil() as u64
    }

    fn applies_to(&self, group: Group) -> bool {
        self.groups.contains(&group)
    }

    /// Whether the test can compare `arms`.
    fn check_arms(&self, arms: &[Arm]) -> anyhow::Result<()> {
        let batch_kib = self.setting("BATCH_SIZE").and_then(|v| v.parse::<u64>().ok());
        if arms.contains(&Arm::Librdkafka) && batch_kib.is_some_and(|kib| kib < MIN_LIBRDKAFKA_BATCH_KIB) {
            bail!(
                "test `{}`: BATCH_SIZE below {MIN_LIBRDKAFKA_BATCH_KIB} KiB is not comparable with the librdkafka (C) \
                 client (its harness runs librdkafka with 1 MiB batches below it); leave that client out",
                self.id
            );
        }
        // Their harnesses count the warmup in records, LIMIT_RPS × the warmup
        // seconds (`consumer_args`), which an unbounded load does not give.
        if self.workload == Workload::Consumer && self.number("LIMIT_RPS") == 0 {
            if let Some(arm) = arms.iter().find(|a| matches!(a, Arm::Rust | Arm::Java | Arm::Librdkafka)) {
                bail!(
                    "test `{}`: a consumer test with an unbounded load (LIMIT_RPS=0) runs only the Python clients, \
                     whose warmup is timed; the {} client counts its warmup in records at LIMIT_RPS",
                    self.id,
                    arm.id()
                );
            }
        }
        Ok(())
    }

    /// The test's settings as `arm` reads them (see [`Arm::idempotence_value`]),
    /// and what they imply for it.
    fn env_for(&self, arm: Arm) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = self
            .settings
            .iter()
            // Read by the driver, below and in `consumer_args`.
            .filter(|(k, _)| k != "LIBRDKAFKA_BATCH")
            .map(|(k, v)| {
                let v = if k == "ENABLE_IDEMPOTENCE" {
                    arm.idempotence_value(v == "true").to_string()
                } else {
                    v.clone()
                };
                (k.clone(), v)
            })
            .collect();
        // With acks=0 the clients report no offsets, which the harnesses'
        // delivery checks require.
        if self.setting("ACKS") == Some("0") {
            env.push(("DO_VERIFY".to_string(), "False".to_string()));
        }
        // The C producer harness sets MAX_IN_FLIGHT to 1000 unless told
        // otherwise, which librdkafka refuses with idempotence; left unset,
        // librdkafka would use 5 itself.
        if arm == Arm::Librdkafka
            && self.workload == Workload::Producer
            && self.setting("ENABLE_IDEMPOTENCE") == Some("true")
            && self.setting("MAX_IN_FLIGHT").is_none()
        {
            env.push(("MAX_IN_FLIGHT".to_string(), IDEMPOTENT_MAX_IN_FLIGHT.to_string()));
        }
        // The Python harness's librdkafka backend reads MAX_POLL_RECORDS as
        // its consume() batch only (librdkafka has no max.poll.records), and
        // POLL_SINGLE as one poll() per record.
        if matches!(arm, Arm::PythonSyncLibrdkafka | Arm::PythonAsyncLibrdkafka) {
            match LibrdkafkaRead::of(self) {
                Some(LibrdkafkaRead::Poll) => env.push(("POLL_SINGLE".to_string(), "True".to_string())),
                Some(LibrdkafkaRead::Batch(n)) => {
                    env.retain(|(k, _)| k != "MAX_POLL_RECORDS");
                    env.push(("MAX_POLL_RECORDS".to_string(), n.to_string()));
                },
                None => {},
            }
        }
        env
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
    let (workload, settings) = parse_settings(&format!("test `{id}`"), words)?;
    Ok(PerfTest { id, groups, workload, settings })
}

/// A `--scenario`: settings as in the tests file, for `group`, named after
/// them ([`scenario_id`]).
fn parse_scenario(text: &str, group: Group) -> anyhow::Result<PerfTest> {
    let (workload, settings) = parse_settings("--scenario", text.split_whitespace())?;
    let id = scenario_id(workload, &settings);
    Ok(PerfTest { id, groups: vec![group], workload, settings })
}

/// Parse `KEY=VALUE` words into a workload and harness settings. `WORKLOAD`
/// (`producer`, the default, `transactional` or `consumer`) picks the
/// harnesses and is not passed to them. `what` names the test in errors.
fn parse_settings<'a>(
    what: &str,
    words: impl Iterator<Item = &'a str>,
) -> anyhow::Result<(Workload, Vec<(String, String)>)> {
    let mut workload: Option<Workload> = None;
    let mut settings: Vec<(String, String)> = Vec::new();
    for word in words {
        let (key, value) = word
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("{what}: `{word}` is not KEY=VALUE"))?;
        if key == "WORKLOAD" {
            if workload.is_some() {
                bail!("{what}: `WORKLOAD` is set twice");
            }
            workload = Some(Workload::parse(value).with_context(|| what.to_string())?);
            continue;
        }
        let Some(def) = SETTINGS.iter().find(|s| s.key == key) else {
            let known: Vec<&str> = SETTINGS.iter().map(|s| s.key).collect();
            bail!("{what}: unknown setting `{key}` (known: WORKLOAD, {})", known.join(", "));
        };
        if settings.iter().any(|(k, _)| k == key) {
            bail!("{what}: `{key}` is set twice");
        }
        validate_setting(key, value, def.kind, def.max).with_context(|| what.to_string())?;
        settings.push((key.to_string(), value.to_string()));
    }
    let workload = workload.unwrap_or(Workload::Producer);
    for (key, _) in &settings {
        let only = SETTINGS.iter().find(|s| s.key == key).map_or(ALL, |s| s.only);
        if only.is_empty() || only.contains(&workload) {
            continue;
        }
        match (workload, only) {
            (Workload::Transactional, [Workload::Producer]) => {
                bail!("{what}: {key} does not apply to transactional tests (they always use idempotence and acks=all)")
            },
            (_, [Workload::Transactional]) => {
                bail!("{what}: {key} applies to transactional tests only (WORKLOAD=transactional)")
            },
            (Workload::Consumer, _) => bail!(
                "{what}: {key} does not apply to consumer tests (their load comes from Kafka's \
                 kafka-producer-perf-test.sh)"
            ),
            (_, [Workload::Consumer]) => bail!("{what}: {key} applies to consumer tests only (WORKLOAD=consumer)"),
            _ => bail!("{what}: {key} does not apply to {} tests", workload.name()),
        }
    }
    for key in REQUIRED_SETTINGS {
        if !settings.iter().any(|(k, _)| k == key) {
            bail!("{what} must set {key}");
        }
    }
    let get = |key: &str| lookup(&settings, key);
    let number = |key: &str| lookup_number(&settings, key);
    let record = record_bytes(&settings);
    if record > MAX_RECORD_BYTES {
        bail!(
            "{what}: KEY_SIZE + VALUE_SIZE is {record} bytes, above {MAX_RECORD_BYTES}: larger records exceed the \
             clients' default 1 MiB request size"
        );
    }
    if let Some(kib) = number("MAX_REQUEST_SIZE") {
        if record + REQUEST_OVERHEAD_BYTES > kib * 1024 {
            bail!(
                "{what}: KEY_SIZE + VALUE_SIZE is {record} bytes, which with {REQUEST_OVERHEAD_BYTES} bytes of headers \
                 does not fit MAX_REQUEST_SIZE={kib} KiB"
            );
        }
    }
    if get("ENABLE_IDEMPOTENCE") == Some("true") {
        if let Some(acks) = get("ACKS").filter(|a| !matches!(*a, "all" | "-1")) {
            bail!("{what}: ACKS={acks} with ENABLE_IDEMPOTENCE=true: an idempotent producer needs ACKS=all");
        }
    }
    let idempotent = workload == Workload::Transactional || get("ENABLE_IDEMPOTENCE") == Some("true");
    if idempotent && number("MAX_IN_FLIGHT").is_some_and(|n| n > IDEMPOTENT_MAX_IN_FLIGHT) {
        bail!(
            "{what}: MAX_IN_FLIGHT above {IDEMPOTENT_MAX_IN_FLIGHT}: an idempotent or transactional producer allows \
             at most {IDEMPOTENT_MAX_IN_FLIGHT}"
        );
    }
    if get("TXN_MODE") == Some("eos") && number("LIMIT_RPS") == Some(0) {
        bail!(
            "{what}: TXN_MODE=eos needs LIMIT_RPS above 0: each client's source topic is first filled with LIMIT_RPS \
             × the measured seconds records"
        );
    }
    Ok((workload, settings))
}

/// The name of a `--scenario`: its workload, rate, value size and partitions,
/// then each other setting that differs from the harnesses' default, in a
/// fixed order (`producer-fixed-10k-1kb-200p`,
/// `txn-max-1kb-36p-1000-per-txn-abort50-16-producers`,
/// `consumer-fixed-10k-1kb-200p-fetchmin4mb-maxpoll4000`). A name over
/// [`MAX_TEST_ID_LEN`] is cut short and ends with a hash of the whole name.
fn scenario_id(workload: Workload, settings: &[(String, String)]) -> String {
    let get = |key: &str| lookup(settings, key);
    let number = |key: &str| lookup_number(settings, key);
    let mut parts: Vec<String> = Vec::new();
    parts.push(
        match (workload, get("TXN_MODE")) {
            (Workload::Producer, _) => "producer",
            (Workload::Transactional, Some("eos")) => "eos",
            (Workload::Transactional, _) => "txn",
            (Workload::Consumer, _) => "consumer",
        }
        .to_string(),
    );
    parts.push(match number("LIMIT_RPS").unwrap_or(0) {
        0 => "max".to_string(),
        rps if rps % 1000 == 0 => format!("fixed-{}k", rps / 1000),
        rps => format!("fixed-{rps}"),
    });
    parts.push(size_label(number("VALUE_SIZE").unwrap_or(0)));
    parts.push(format!("{}p", number("PARTITIONS").unwrap_or(0)));
    if let Some(n) = number("KEY_SIZE").filter(|n| *n > 0) {
        parts.push(format!("key{}", size_label(n)));
    }
    if let Some(acks) = get("ACKS").filter(|a| !matches!(*a, "all" | "-1")) {
        parts.push(format!("acks{acks}"));
    }
    if let Some(codec) = get("COMPRESSION_TYPE").filter(|c| *c != "none") {
        parts.push(codec.to_string());
    }
    if let Some(kib) = number("BATCH_SIZE").filter(|n| *n != 1024) {
        parts.push(format!("batch{}", size_label(kib * 1024)));
    }
    if let Some(ms) = number("LINGER_MS").filter(|n| *n != 5) {
        parts.push(format!("linger{ms}ms"));
    }
    if get("ENABLE_IDEMPOTENCE") == Some("true") {
        parts.push("idempotent".to_string());
    }
    if let Some(n) = number("MAX_IN_FLIGHT") {
        parts.push(format!("inflight{n}"));
    }
    if let Some(mib) = number("BUFFER_MEMORY") {
        parts.push(format!("buffer{}", size_label(mib * 1024 * 1024)));
    }
    if let Some(kib) = number("MAX_REQUEST_SIZE") {
        parts.push(format!("maxrequest{}", size_label(kib * 1024)));
    }
    // The consumer settings: any one set differs from a client default.
    if let Some(bytes) = number("FETCH_MIN_BYTES") {
        parts.push(format!("fetchmin{}", size_label(bytes)));
    }
    if let Some(ms) = number("FETCH_MAX_WAIT_MS") {
        parts.push(format!("fetchwait{ms}ms"));
    }
    if let Some(bytes) = number("MAX_PARTITION_FETCH_BYTES") {
        parts.push(format!("partfetch{}", size_label(bytes)));
    }
    if let Some(n) = number("MAX_POLL_RECORDS") {
        parts.push(format!("maxpoll{n}"));
    }
    match number("LIBRDKAFKA_BATCH") {
        Some(0) => parts.push("librdkafka-poll".to_string()),
        Some(n) => parts.push(format!("librdkafka-batch{n}")),
        None => {},
    }
    if workload == Workload::Transactional {
        parts.push(format!("{}-per-txn", number("RECORDS_PER_TRANSACTION").unwrap_or(100)));
        if let Some(rate) = get("ABORT_RATE").and_then(|v| v.parse::<f64>().ok()).filter(|r| *r > 0.0) {
            // `0.5` -> `50`, `0.125` -> `12p5`: a test id has no `.`.
            let percent = format!("{:.1}", rate * 100.0);
            parts.push(format!("abort{}", percent.trim_end_matches(".0").replace('.', "p")));
        }
        if let Some(n) = number("NUM_TRANSACTIONAL_PRODUCERS").filter(|n| *n > 1) {
            parts.push(format!("{n}-producers"));
        }
    }
    let id = parts.join("-");
    if id.len() <= MAX_TEST_ID_LEN {
        return id;
    }
    // FNV-1a: stable across builds, unlike the standard library's hasher.
    let hash = id.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    let keep = MAX_TEST_ID_LEN - 9;
    format!("{}-{:08x}", id[..keep].trim_end_matches('-'), hash as u32)
}

/// `1048576` -> `1mb`, `1024` -> `1kb`, `100` -> `100b`.
fn size_label(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    match bytes {
        b if b >= MIB && b % MIB == 0 => format!("{}mb", b / MIB),
        b if b >= KIB && b % KIB == 0 => format!("{}kb", b / KIB),
        b => format!("{b}b"),
    }
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

/// Check `value` against `kind`; `max` bounds a [`Kind::Count`] or
/// [`Kind::Positive`] value.
fn validate_setting(key: &str, value: &str, kind: Kind, max: u64) -> anyhow::Result<()> {
    // Digits only: `u64::from_str` would also take a leading `+`, which the
    // C harness's `atoi` reads differently from the others.
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let ok = match kind {
        Kind::Count => digits(value) && value.parse::<u64>().is_ok_and(|n| n <= max),
        Kind::Positive => digits(value) && value.parse::<u64>().is_ok_and(|n| n > 0 && n <= max),
        Kind::Compression => matches!(value, "none" | "gzip" | "snappy" | "lz4" | "zstd"),
        Kind::Bool => matches!(value, "true" | "false"),
        Kind::Acks => matches!(value, "all" | "-1" | "1" | "0"),
        Kind::Fraction => {
            let well_formed = match value.split_once('.') {
                Some((whole, fraction)) => digits(whole) && digits(fraction),
                None => digits(value),
            };
            well_formed && value.parse::<f64>().is_ok_and(|v| (0.0..=1.0).contains(&v))
        },
        Kind::TxnMode => matches!(value, "produce" | "eos"),
    };
    if !ok {
        let expected = match kind {
            Kind::Count => format!("a whole number up to {max}"),
            Kind::Positive => format!("a whole number from 1 to {max}"),
            Kind::Compression => "one of none, gzip, snappy, lz4, zstd".to_string(),
            Kind::Bool => "`true` or `false`".to_string(),
            Kind::Acks => "one of all, -1, 1, 0".to_string(),
            Kind::Fraction => "a decimal number from 0 to 1, such as 0.5".to_string(),
            Kind::TxnMode => "`produce` or `eos`".to_string(),
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

/// A line of [`BATCHES_FILE`]: tests of [`TESTS_FILE`] that run one after
/// another, on one cluster, under one name.
#[derive(Debug, Clone, PartialEq)]
struct Batch {
    name: String,
    group: Group,
    /// Ids of tests that apply to `group`, in the order they run.
    tests: Vec<String>,
}

/// Parse the batches file against the `tests` it names: `<name> <group>
/// <test-id> ...` per line, `#` starts a comment.
fn parse_batches(text: &str, tests: &[PerfTest]) -> anyhow::Result<Vec<Batch>> {
    let mut batches: Vec<Batch> = Vec::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let batch = parse_batch_line(line, tests).with_context(|| format!("{BATCHES_FILE}:{line_no}"))?;
        if batches.iter().any(|b| b.name == batch.name) {
            bail!("{BATCHES_FILE}:{line_no}: batch `{}` is defined twice", batch.name);
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn parse_batch_line(line: &str, tests: &[PerfTest]) -> anyhow::Result<Batch> {
    let mut words = line.split_whitespace();
    let name = words.next().unwrap_or_default().to_string();
    // The same rules as a test id: it names the run in CI.
    validate_test_id(&name).map_err(|_| {
        anyhow::anyhow!(
            "invalid batch name `{name}`: use lowercase letters, digits and `-`, at most {MAX_TEST_ID_LEN} \
             characters, not starting with `-`"
        )
    })?;
    let group_word = words
        .next()
        .ok_or_else(|| anyhow::anyhow!("batch `{name}` names no client group"))?;
    let group = Group::parse(group_word).with_context(|| format!("batch `{name}`"))?;
    let ids: Vec<&str> = words.collect();
    if ids.is_empty() {
        bail!("batch `{name}` names no tests");
    }
    let selected = select_tests(tests, &ids.join(","), group).with_context(|| format!("batch `{name}`"))?;
    Ok(Batch { name, group, tests: selected.into_iter().map(|t| t.id).collect() })
}

fn load_batches(tests: &[PerfTest]) -> anyhow::Result<Vec<Batch>> {
    let path = repo_root().join(BATCHES_FILE);
    let text = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    parse_batches(&text, tests)
}

/// The tests of the batch `name`, which must be for `group`.
fn select_batch(all: &[PerfTest], batches: &[Batch], name: &str, group: Group) -> anyhow::Result<Vec<PerfTest>> {
    let Some(batch) = batches.iter().find(|b| b.name == name) else {
        let names: Vec<&str> = batches.iter().map(|b| b.name.as_str()).collect();
        bail!("unknown batch `{name}` (known: {})", names.join(", "));
    };
    if batch.group != group {
        bail!(
            "batch `{name}` runs the {} group's clients, not the {} group's (--group)",
            batch.group.name(),
            group.name()
        );
    }
    select_tests(all, &batch.tests.join(","), group)
}

fn list() -> anyhow::Result<()> {
    let tests = load_tests()?;
    println!("Tests in {TESTS_FILE}:");
    for t in &tests {
        let groups: Vec<&str> = t.groups.iter().map(|g| g.name()).collect();
        println!("  {:<46} {:<12} {}", t.id, groups.join(","), report::scenario(t));
    }
    println!("\nBatches in {BATCHES_FILE} (--batch), each test in turn on one cluster:");
    for b in &load_batches(&tests)? {
        println!("  {:<46} {:<12} {}", b.name, b.group.name(), b.tests.join(" "));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

struct RunOptions {
    group: Group,
    /// The group's arms to run (`--clients`), in the group's order.
    arms: Vec<Arm>,
    tests: Vec<PerfTest>,
    /// The batch (`--batch`) the tests are, if any.
    batch: Option<String>,
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

const SHARED_FLAGS: &[&str] = &[
    "--group",
    "--clients",
    "--tests",
    "--batch",
    "--scenario",
    "--duration-min",
    "--warmup-min",
];
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
        let arms = group.select_arms(flags.get("--clients").map_or("all", String::as_str))?;
        let duration_min = minutes(&flags, "--duration-min", 1, MAX_DURATION_MINUTES)?.unwrap_or(30);
        let warmup_min = minutes(&flags, "--warmup-min", 0, MAX_WARMUP_MINUTES)?.unwrap_or(5);
        let batch = flags.get("--batch").cloned();
        let tests = match (flags.get("--tests"), batch.as_deref(), flags.get("--scenario")) {
            (Some(list), None, None) => select_tests(&load_tests()?, list, group)?,
            (None, Some(name), None) => {
                let all = load_tests()?;
                select_batch(&all, &load_batches(&all)?, name, group)?
            },
            (None, None, Some(scenario)) => vec![parse_scenario(scenario, group)?],
            (None, None, None) => bail!("--tests, --batch or --scenario is required"),
            _ => bail!("give one of --tests, --batch and --scenario"),
        };
        for test in &tests {
            test.check_arms(&arms)?;
        }
        let budget_min = minutes(&flags, "--budget-min", 1, u64::from(u32::MAX))?;
        let ingress_mb_s = match flags.get("--ingress-mb-s") {
            Some(raw) => match raw.trim().parse::<f64>() {
                Ok(v) if v.is_finite() && v > 0.0 => Some(v),
                _ => bail!("--ingress-mb-s: `{raw}` is not a positive number"),
            },
            None => None,
        };
        let (bootstrap, kafka_bin, results_dir, topic_prefix) = if for_run {
            // New for each run, so a run never finds the topics, and the
            // consumer groups named after them, of one before it.
            let topic_prefix = flags.get("--topic-prefix").cloned().unwrap_or_else(|| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                format!("perf-local-{}", now.as_secs())
            });
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
            arms,
            tests,
            batch,
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

    /// The minutes one arm of `test` takes, its source topic's filling
    /// included.
    fn arm_minutes(&self, test: &PerfTest) -> u64 {
        self.warmup_min + self.duration_min + ARM_SETUP_MINUTES + test.seed_minutes(self.duration_min)
    }

    fn test_minutes(&self, test: &PerfTest) -> u64 {
        self.arms.len() as u64 * self.arm_minutes(test)
    }

    fn estimate_minutes(&self) -> u64 {
        self.tests.iter().map(|t| self.test_minutes(t)).sum()
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

/// A `flag` of whole minutes from `min` to `max`, `None` when not given.
fn minutes(flags: &BTreeMap<String, String>, flag: &str, min: u64, max: u64) -> anyhow::Result<Option<u64>> {
    let Some(raw) = flags.get(flag) else {
        return Ok(None);
    };
    let value: u64 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{flag}: `{raw}` is not a whole number of minutes"))?;
    if !(min..=max).contains(&value) {
        bail!("{flag} must be between {min} and {max} minutes, got {value}");
    }
    Ok(Some(value))
}

/// Topic names are `<prefix>-<test>-<arm>`, and `<that>-source` for an
/// `eos` test's source topic; Kafka allows `[A-Za-z0-9._-]`, at most 249
/// characters, and all but the prefix add at most ~92.
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

/// The topic an `eos` arm consumes from (see [`topic_name`]).
fn source_topic_name(topic: &str) -> String {
    format!("{topic}-source")
}

// ---------------------------------------------------------------------------
// check
// ---------------------------------------------------------------------------

fn check(options: &RunOptions) -> anyhow::Result<()> {
    let group = options.group;
    let arms: Vec<&str> = options.arms.iter().map(|a| a.label()).collect();
    println!("Client group: {}; clients in order: {}", group.name(), arms.join(", "));
    if let Some(ingress) = options.ingress_mb_s {
        println!("Cluster ingress limit: ~{ingress:.0} MB/s");
    }
    match &options.batch {
        Some(batch) => println!("Batch {batch}: {} tests, in order:", options.tests.len()),
        None => println!("Tests ({}), in order:", options.tests.len()),
    }
    let mut warnings = Vec::new();
    for t in &options.tests {
        if let (Some(mb_s), Some(ingress)) = (t.rate_mb_s(), options.ingress_mb_s) {
            if mb_s > ingress {
                bail!(
                    "{}: its fixed rate, {mb_s:.0} MB/s, is above the cluster's ~{ingress:.0} MB/s ingress, so no \
                     client could hold it; use a larger cluster or lower LIMIT_RPS (LIMIT_RPS=0 for an unbounded producer \
                     test)",
                    t.id
                );
            }
        }
    }
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
        println!("  {}. {}", i + 1, t.id);
        println!("     {}  [{load}]", report::scenario(t));
        let seed = t.seed_minutes(options.duration_min);
        let seeding = if seed > 0 {
            format!(" + ~{seed} min filling the source topic")
        } else {
            String::new()
        };
        println!(
            "     per client: {} min warmup + {} min measured + ~{ARM_SETUP_MINUTES} min setup{seeding}; \
             test ~{}",
            options.warmup_min,
            options.duration_min,
            report::hours_minutes(options.test_minutes(t))
        );
    }
    let estimate = options.estimate_minutes();
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
        // How many of the tests, in order, fit.
        let mut total = 0;
        let fit = options
            .tests
            .iter()
            .take_while(|t| {
                total += options.test_minutes(t);
                total <= budget
            })
            .count();
        let n = options.tests.len();
        let advice = if fit == 0 {
            "shorten the measurement or run fewer clients".to_string()
        } else {
            format!("only the first {fit} fit")
        };
        bail!(
            "{n} {} ~{}, more than the budget of {}; {advice}",
            if n == 1 { "test takes" } else { "tests take" },
            report::hours_minutes(estimate),
            report::hours_minutes(budget)
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
/// librdkafka-dev (pkg-config `rdkafka`), cmake, a C compiler (`cc`), a JDK
/// 17+ and Gradle (`$GRADLE` or `gradle`); for `python` python3-venv and
/// python3-dev (`$PERF_PYTHON` or `python3`).
fn build(group: Group) -> anyhow::Result<()> {
    let repo = repo_root();
    let rust = repo.join("rust");
    match group {
        Group::Rust => {
            // The C harness links the Rust client's C library statically and
            // needs its header, so the FFI build comes first. Only its
            // librdkafka backend (CLIENT_VERSION=2) runs here, so the library
            // is built without native CPU flags. It is not wholly unused:
            // the library bundles its own zstd, whose global `ZSTD_*` symbols
            // the executable exports, so librdkafka's zstd calls can bind to
            // that copy instead of the system libzstd.
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
                "C (transactional) producer harnesses: configure",
                Command::new("cmake")
                    .arg("-S")
                    .arg(repo.join("c"))
                    .arg("-B")
                    .arg(repo.join("c/build"))
                    .arg(format!("-DRUST_PROJECT_ROOT={}", rust.display()))
                    .arg("-DCMAKE_C_FLAGS=-O2 -march=native"),
            )?;
            step(
                "C (transactional) producer harnesses: build",
                Command::new("cmake").arg("--build").arg(repo.join("c/build")).args([
                    "--target",
                    "producer_perf_test",
                    "transactional_producer_perf_test",
                ]),
            )?;
            // As `deploy_and_run_perf.py` builds it: plain librdkafka, no Rust.
            let rdkafka_flags = Command::new("pkg-config")
                .args(["--cflags", "--libs", "rdkafka"])
                .output()
                .context("running pkg-config")?;
            if !rdkafka_flags.status.success() {
                bail!("pkg-config --cflags --libs rdkafka failed");
            }
            let consumer_c = c_harness(repo, Workload::Consumer);
            if let Some(dir) = consumer_c.parent() {
                fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            }
            step(
                "librdkafka (C) consumer harness",
                Command::new("cc")
                    .args(["-O2", "-march=native", "-o"])
                    .arg(&consumer_c)
                    .arg(consumer_compare_dir(repo).join("librdkafka_e2e.c"))
                    .args(String::from_utf8_lossy(&rdkafka_flags.stdout).split_whitespace())
                    .args(["-lm", "-lpthread"]),
            )?;
            // The Rust arm runs `cargo xtask producer-perf-test` or
            // `transactional-producer-perf-test`, both in this test binary;
            // compiling it now keeps the compile out of the first arm.
            step(
                "Rust (transactional) producer harnesses",
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
            // Built like the producer harnesses above, without native CPU
            // flags (a different RUSTFLAGS would rebuild the whole workspace).
            step(
                "Rust consumer harness",
                Command::new(cargo())
                    .args(["build", "--release", "-p", "consumer-perf"])
                    .current_dir(&rust),
            )?;
            let gradle = std::env::var("GRADLE").unwrap_or_else(|_| "gradle".to_string());
            step(
                "Java (transactional) producer harnesses",
                Command::new(gradle)
                    .args(["shadowJar", "--console=plain"])
                    .current_dir(repo.join("tools/java-perf-test")),
            )?;
            // The kafka-clients it runs against come with the producer jar.
            step(
                "Java consumer harness",
                Command::new("javac")
                    .arg("-cp")
                    .arg(java_jar(repo))
                    .arg("-d")
                    .arg(java_consumer_classes(repo))
                    .arg(consumer_compare_dir(repo).join("JavaE2E.java")),
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
        for workload in [Workload::Producer, Workload::Transactional, Workload::Consumer] {
            for path in arm.prerequisites(repo, workload) {
                if !path.exists() {
                    bail!("the build did not produce {} ({})", path.display(), arm.label());
                }
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
            let value = match std::env::var(name) {
                Ok(v) if !v.is_empty() && v != "null" => v,
                _ => bail!("{name} is not set (the API key and secret come from PERF_API_KEY / PERF_API_SECRET)"),
            };
            check_credential(name, &value)?;
            Ok(value)
        };
        Ok(Credentials { key: get("PERF_API_KEY")?, secret: get("PERF_API_SECRET")? })
    }
}

/// The key and secret go into `client.properties` inside a double-quoted JAAS
/// value, which its readers parse differently: the Java tools through
/// `Properties.load`, which removes one level of `\` escapes, the Rust
/// consumer harness as written. No escaping suits both, so a `"`, a `\` or a
/// control character is refused (Confluent Cloud's never contain one). The
/// error does not show the value.
fn check_credential(name: &str, value: &str) -> anyhow::Result<()> {
    if value.chars().any(|c| c == '"' || c == '\\' || c.is_control()) {
        bail!("{name} contains a `\"`, a `\\` or a control character, which the harnesses cannot read back");
    }
    Ok(())
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
    /// The transactional harnesses' numbers, `None` for the others.
    pub(crate) txn: Option<TxnMetrics>,
}

/// The transaction numbers of a transactional harness's `results.json`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TxnMetrics {
    pub(crate) per_s: f64,
    pub(crate) committed: f64,
    pub(crate) aborted: f64,
    /// `begin_transaction` to `commit_transaction` completing.
    pub(crate) commit_p50: f64,
    pub(crate) commit_p99: f64,
}

impl Metrics {
    fn from_results(text: &str) -> anyhow::Result<Metrics> {
        let v: serde_json::Value = serde_json::from_str(text)?;
        let top = |key: &str| {
            v.get(key)
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| anyhow::anyhow!("no `{key}`"))
        };
        let latency = v.get("latency_ms").ok_or_else(|| anyhow::anyhow!("no `latency_ms`"))?;
        let lat = |key: &str| {
            latency
                .get(key)
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| anyhow::anyhow!("no `latency_ms.{key}`"))
        };
        let client = v
            .get("client")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("no `client`"))?;
        let txn = match v.get("transactions_per_s") {
            None => None,
            Some(per_s) => {
                let commit = v
                    .get("commit_latency_ms")
                    .ok_or_else(|| anyhow::anyhow!("no `commit_latency_ms`"))?;
                let commit_lat = |key: &str| {
                    commit
                        .get(key)
                        .and_then(serde_json::Value::as_f64)
                        .ok_or_else(|| anyhow::anyhow!("no `commit_latency_ms.{key}`"))
                };
                Some(TxnMetrics {
                    per_s: per_s
                        .as_f64()
                        .ok_or_else(|| anyhow::anyhow!("`transactions_per_s` is not a number"))?,
                    committed: top("committed_transactions")?,
                    aborted: top("aborted_transactions")?,
                    commit_p50: commit_lat("p50")?,
                    commit_p99: commit_lat("p99")?,
                })
            },
        };
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
            txn,
        })
    }
}

pub(crate) struct ArmResult {
    pub(crate) arm: Arm,
    pub(crate) outcome: Outcome,
    pub(crate) metrics: Option<Metrics>,
    /// A consumer test's load: the rate `kafka-producer-perf-test.sh`
    /// reached (msg/s), when its output gave one.
    pub(crate) load_msg_s: Option<f64>,
    pub(crate) elapsed: Duration,
}

pub(crate) struct TestResult {
    pub(crate) test: PerfTest,
    pub(crate) arms: Vec<ArmResult>,
}

/// Run-wide facts shown at the top of the report.
pub(crate) struct RunInfo {
    pub(crate) group: Group,
    /// The arms that ran, in order.
    pub(crate) arms: Vec<Arm>,
    /// The tests the run runs, in order.
    pub(crate) tests: Vec<PerfTest>,
    pub(crate) duration_min: u64,
    pub(crate) warmup_min: u64,
    pub(crate) started: String,
    /// `(label, value)` pairs, only the ones known.
    pub(crate) facts: Vec<(String, String)>,
    pub(crate) ingress_mb_s: Option<f64>,
}

fn run_tests(options: &RunOptions) -> anyhow::Result<()> {
    crate::signals::install();
    // One baseline for the whole run, taken as soon as the handler is in
    // place: a signal at any later point (during the setup below, a command,
    // between arms, an artifact push) stops the run.
    let signals_baseline = crate::signals::count();
    let repo = repo_root();
    let credentials = Credentials::from_env()?;
    // The harnesses get them as SASL_USERNAME / SASL_PASSWORD; no other child
    // process needs them. Nothing else runs yet, so the environment can change.
    std::env::remove_var("PERF_API_KEY");
    std::env::remove_var("PERF_API_SECRET");
    let topics_cli = options.kafka_bin.join("kafka-topics.sh");
    if !topics_cli.is_file() {
        bail!("{} not found (--kafka-bin)", topics_cli.display());
    }
    // The eos harnesses fill their source topic with it; the consumer tests'
    // load comes from it.
    let seeder = options.kafka_bin.join("kafka-producer-perf-test.sh");
    let needs_seeder = |t: &PerfTest| t.is_eos() || t.workload == Workload::Consumer;
    if options.tests.iter().any(needs_seeder) && !seeder.is_file() {
        bail!(
            "{} not found (--kafka-bin; TXN_MODE=eos and consumer tests need it)",
            seeder.display()
        );
    }
    let kafka_bin = fs::canonicalize(&options.kafka_bin)?;
    for &arm in &options.arms {
        for test in &options.tests {
            for path in arm.prerequisites(repo, test.workload) {
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
    // The key too: not a secret on its own, but the published logs need it no
    // more than the secret.
    let redactions = Arc::new(vec![credentials.secret.clone(), credentials.key.clone()]);

    let live = LiveReport::start(run_info(options), results_dir.clone(), options.push_artifacts, signals_baseline);
    // Read once a test has run: a new API key can take minutes before the
    // cluster accepts it.
    let mut zones_read = false;
    let mut interrupted = false;
    for (i, test) in options.tests.iter().enumerate() {
        println!(
            "\n######## Test {}/{}: {} — {} ########",
            i + 1,
            options.tests.len(),
            test.id,
            report::scenario(test)
        );
        live.update(|p| p.begin_test(test.clone()));
        for &arm in &options.arms {
            if crate::signals::count() > signals_baseline {
                interrupted = true;
                break;
            }
            let topic = topic_name(&options.topic_prefix, test, arm);
            let ctx = ArmContext {
                repo,
                options,
                test,
                arm,
                dir: results_dir.join(&test.id).join(arm.id()),
                source_topic: test.is_eos().then(|| source_topic_name(&topic)),
                topic,
                topics_cli: &topics_cli,
                kafka_bin: &kafka_bin,
                client_properties: &client_properties,
                credentials: &credentials,
                redactions: Arc::clone(&redactions),
                signals_baseline,
            };
            live.update(|p| p.begin_arm(RunningArm::new(test, arm, &ctx.dir)));
            live.publish_now();
            let result = match run_arm(&ctx) {
                Ok(result) => result,
                Err(e) => {
                    // Not to leave the report so far waiting for a test that
                    // will not finish.
                    if let Err(report) = end_test(&live, &results_dir, options, test, false) {
                        println!("WARNING: could not write the report: {report:#}");
                    }
                    return Err(e);
                },
            };
            interrupted = matches!(&result.outcome, Outcome::Failed(r) if r == INTERRUPTED);
            live.update(|p| p.end_arm(result));
            if interrupted {
                break;
            }
        }
        if !interrupted && !zones_read {
            let zones = cluster_zones(
                &kafka_bin,
                &options.bootstrap,
                &client_properties,
                work.path(),
                signals_baseline,
            );
            if let Some(zones) = zones {
                zones_read = true;
                live.update(|p| add_cluster_zones(&mut p.info.facts, &zones));
            }
        }
        // An interrupted test's directory goes with the epilogue's push.
        end_test(&live, &results_dir, options, test, !interrupted)?;
        if interrupted {
            break;
        }
    }
    let results = live.finish();
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
    let commit = stdout_of(Command::new("git").args(["rev-parse", "HEAD"]).current_dir(repo_root()));
    let kernel = stdout_of(Command::new("uname").arg("-r"));
    // The baseline clients come from package repositories at their latest
    // version, so a report records which one it measured.
    let uses = |arms: &[Arm]| options.arms.iter().any(|a| arms.contains(a));
    let librdkafka = if uses(&[Arm::Librdkafka]) {
        stdout_of(Command::new("pkg-config").args(["--modversion", "rdkafka"]))
    } else {
        None
    };
    // `confluent_kafka.version()` is a `(str, int)` tuple in older releases
    // and a plain `str` in current ones, where `[0]` would keep only its first
    // character; `libversion()` is still a tuple.
    let python_librdkafka = if uses(&[Arm::PythonSyncLibrdkafka, Arm::PythonAsyncLibrdkafka]) {
        stdout_of(Command::new(venv_python(repo_root(), "venv-librdkafka")).args([
            "-I",
            "-c",
            "import confluent_kafka as c; v = c.version(); \
             print(f'{v if isinstance(v, str) else v[0]} (librdkafka {c.libversion()[0]})')",
        ]))
    } else {
        None
    };
    // A limit is positive; anything else (e.g. a `0` meaning "not reported")
    // would flag every result as near it.
    let ingress = env("PERF_CLUSTER_INGRESS_MB_S")
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0);
    // The report keeps to what the results depend on; the job's log keeps
    // where they came from.
    for (label, value) in [
        ("Commit", commit),
        ("Workflow", env("SEMAPHORE_WORKFLOW_ID")),
        ("Agent", env("SEMAPHORE_AGENT_MACHINE_TYPE")),
        ("Agent region", env("AGENT_REGION")),
        ("Kernel", kernel),
        ("Cluster", env("PERF_CLUSTER_ID")),
    ] {
        if let Some(value) = value {
            println!("{label}: {value}");
        }
    }
    for (label, value) in [
        // The machine first: report::MACHINE_FACTS lists these two.
        ("Instance", env("SEMAPHORE_AGENT_MACHINE_TYPE")),
        ("CPU", cpu()),
        ("Batch", options.batch.clone()),
        ("Agent AZ ID", env("AGENT_AZ_ID")),
        ("Cluster size", env("PERF_CLUSTER_SIZE")),
        ("Cluster ingress limit", ingress.map(|mb| format!("{mb:.0} MB/s"))),
        ("librdkafka version", librdkafka),
        ("confluent-kafka version (PyPI)", python_librdkafka),
    ] {
        if let Some(value) = value {
            facts.push((label.to_string(), value));
        }
    }
    RunInfo {
        group: options.group,
        arms: options.arms.clone(),
        tests: options.tests.clone(),
        duration_min: options.duration_min,
        warmup_min: options.warmup_min,
        started: now_utc(),
        facts,
        ingress_mb_s: ingress,
    }
}

/// This machine's CPU model and logical CPU count (`AMD EPYC 9R45, 16
/// vCPUs`), from Linux's `/proc/cpuinfo`; `None` elsewhere.
fn cpu() -> Option<String> {
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    let model = info
        .lines()
        .find_map(|l| l.strip_prefix("model name")?.split_once(':').map(|(_, m)| m.trim().to_string()))?;
    let count = info.lines().filter(|l| l.starts_with("processor")).count();
    Some(format!("{model}, {count} vCPUs"))
}

/// A command's standard output, trimmed, when it ran, succeeded and printed
/// something.
fn stdout_of(command: &mut Command) -> Option<String> {
    command
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The cluster's AZ IDs: its brokers' racks, which Confluent Cloud sets to
/// the AZ ID each broker runs in, as `kafka-broker-api-versions.sh` lists
/// them. Its output (every broker's API versions) goes to a file in `work`,
/// not the job log. Empty when no broker has a rack; `None` when the brokers
/// could not be read, so a later call can try again.
fn cluster_zones(
    kafka_bin: &Path,
    bootstrap: &str,
    client_properties: &Path,
    work: &Path,
    signals_baseline: u32,
) -> Option<Vec<String>> {
    let tool = kafka_bin.join("kafka-broker-api-versions.sh");
    let output = work.join("brokers.txt");
    let status = (|| -> anyhow::Result<Option<ExitStatus>> {
        let mut command = Command::new(&tool);
        command
            .args(["--bootstrap-server", bootstrap, "--command-config"])
            .arg(client_properties)
            .stdin(Stdio::null())
            .stdout(File::create(&output)?)
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().with_context(|| format!("starting {}", tool.display()))?;
        let pgid = child.id();
        let exit = wait_group(&mut child, pgid, BROKERS_COMMAND_TIMEOUT, signals_baseline, &mut || Ok(false))?;
        reap_group(&mut child, pgid)?;
        Ok(match exit {
            Exit::Status(status) => Some(status),
            _ => None,
        })
    })();
    let failure = match status {
        Ok(Some(status)) if status.success() => None,
        Ok(Some(status)) => Some(format!("it exited with {status}")),
        Ok(None) => Some("it was stopped".to_string()),
        Err(e) => Some(format!("{e:#}")),
    };
    if let Some(failure) = failure {
        println!(
            "Could not read the cluster's AZ IDs with {} ({failure}); trying again after the next test",
            tool.display()
        );
        return None;
    }
    let zones = broker_racks(&fs::read_to_string(&output).unwrap_or_default());
    if zones.is_empty() {
        println!("The cluster's brokers have no rack, so its AZ IDs are unknown");
    } else {
        println!("Cluster AZ IDs: {}", zones.join(", "));
    }
    Some(zones)
}

/// The distinct broker racks in `kafka-broker-api-versions.sh` output, whose
/// broker lines read `host:port (id: 0 rack: usw2-az1 isFenced: false) -> (`
/// (`... rack: usw2-az1) -> (` before Kafka 4.0, `rack: null` without one).
fn broker_racks(output: &str) -> Vec<String> {
    let mut racks: Vec<String> = output
        .lines()
        .filter(|line| line.contains(" (id: "))
        .filter_map(|line| line.split_once(" rack: ")?.1.split([' ', ')']).next())
        .filter(|rack| !rack.is_empty() && *rack != "null")
        .map(str::to_string)
        .collect();
    racks.sort();
    racks.dedup();
    racks
}

/// Record the cluster's AZ IDs right after the agent's, so the two read
/// together. Nothing when they are unknown.
fn add_cluster_zones(facts: &mut Vec<(String, String)>, zones: &[String]) {
    if zones.is_empty() {
        return;
    }
    let at = facts.iter().position(|(label, _)| label == "Agent AZ ID").map_or(0, |i| i + 1);
    facts.insert(at, ("Cluster AZ IDs".to_string(), zones.join(", ")));
}

/// End the running test: its full report replaces the report so far, and
/// with `--push-artifacts` is pushed, with the test's directory when
/// `push_dir`.
fn end_test(
    live: &LiveReport,
    results_dir: &Path,
    options: &RunOptions,
    test: &PerfTest,
    push_dir: bool,
) -> anyhow::Result<()> {
    live.publish_full(|p| {
        p.end_test();
        write_reports(results_dir, &p.info, &p.results)?;
        if options.push_artifacts {
            if push_dir {
                push_artifact("job", &results_dir.join(&test.id), &format!("perf-results/{}", test.id));
            }
            push_reports(results_dir);
        }
        Ok(())
    })
}

fn write_reports(results_dir: &Path, info: &RunInfo, results: &[TestResult]) -> anyhow::Result<()> {
    fs::write(results_dir.join("report.md"), report::markdown(info, results, None))?;
    fs::write(results_dir.join("junit.xml"), report::junit(results))?;
    Ok(())
}

/// Push to Semaphore's artifacts (`--push-artifacts`) right away, so a job
/// that later hits its time limit keeps the tests that finished. `level` is
/// `job` or `workflow`. A push still running after [`PUSH_TIMEOUT`], or when
/// a signal arrives, is stopped. A failed push is only logged: the epilogue
/// pushes everything again.
fn push_artifact(level: &str, path: &Path, destination: &str) {
    let pushed = (|| -> anyhow::Result<bool> {
        let mut child = Command::new("artifact")
            .args(["push", level])
            .arg(path)
            .args(["--destination", destination, "--force"])
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()?;
        let pgid = child.id();
        // An interrupted run still pushes its report: only a signal during
        // the push stops it.
        let exit = wait_group(&mut child, pgid, PUSH_TIMEOUT, crate::signals::count(), &mut || Ok(false))?;
        reap_group(&mut child, pgid)?;
        Ok(matches!(exit, Exit::Status(status) if status.success()))
    })();
    if !pushed.unwrap_or(false) {
        println!("WARNING: could not push {} as a {level} artifact", path.display());
    }
}

/// The report and the JUnit file as job artifacts, and the report as the
/// workflow's report too (Semaphore shows `.semaphore/REPORT.md` of the
/// workflow artifacts in its Report tab).
fn push_reports(results_dir: &Path) {
    let report = results_dir.join("report.md");
    push_artifact("job", &report, "perf-results/report.md");
    push_artifact("job", &results_dir.join("junit.xml"), "perf-results/junit.xml");
    push_artifact("workflow", &report, ".semaphore/REPORT.md");
}

/// The Java-form properties `kafka-topics.sh` reads (`--command-config`). The
/// credentials need no escaping (see [`check_credential`]).
fn client_properties_text(bootstrap: &str, credentials: &Credentials) -> String {
    format!(
        "bootstrap.servers={bootstrap}\nsecurity.protocol=SASL_SSL\nsasl.mechanism=PLAIN\n\
         sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{}\" password=\"{}\";\n",
        credentials.key, credentials.secret
    )
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
        // The time too: a killed run leaves its directory behind, and a later
        // run can get the same process id.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("perf-cloud-{}-{}", std::process::id(), now.as_nanos()));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .with_context(|| format!("creating {}", path.display()))?;
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
    /// The topic an `eos` arm consumes from, which it fills first.
    source_topic: Option<String>,
    topics_cli: &'a Path,
    /// `--kafka-bin`, absolute (the harnesses run in other directories).
    kafka_bin: &'a Path,
    client_properties: &'a Path,
    credentials: &'a Credentials,
    redactions: Arc<Vec<String>>,
    /// [`crate::signals::count`] when the run started.
    signals_baseline: u32,
}

/// Create the arm's topics, run the arm, read its results, delete the
/// topics. An error is returned only for local I/O failures of the results
/// directory; everything about the arm itself, a harness that cannot be
/// started included, ends up in its [`Outcome`], so the run goes on.
fn run_arm(ctx: &ArmContext<'_>) -> anyhow::Result<ArmResult> {
    let started = Instant::now();
    fs::create_dir_all(&ctx.dir).with_context(|| format!("creating {}", ctx.dir.display()))?;
    let log = ArmLog::create(&ctx.dir.join("run.log"), Arc::clone(&ctx.redactions), ctx.signals_baseline)?;
    let source = match &ctx.source_topic {
        Some(source) => format!(", consuming from {source}"),
        None => String::new(),
    };
    log.line(&format!(
        "==> {} / {} on topic {}{source} ({})",
        ctx.test.id,
        ctx.arm.label(),
        ctx.topic,
        now_utc()
    ));

    // The source topic first: an eos harness fills it, then consumes it into
    // the topic.
    let topics: Vec<&str> = ctx
        .source_topic
        .iter()
        .map(String::as_str)
        .chain([ctx.topic.as_str()])
        .collect();
    let mut created: Vec<&str> = Vec::new();
    let mut failure: Option<String> = None;
    for &topic in &topics {
        match create_topic(ctx, topic, &log).unwrap_or_else(|e| Err(format!("{e:#}"))) {
            Ok(()) => created.push(topic),
            Err(reason) => {
                failure = Some(reason);
                break;
            },
        }
    }
    let (outcome, load_msg_s) = match failure {
        Some(reason) => (Outcome::Failed(reason), None),
        None => run_harness(ctx, &log).unwrap_or_else(|e| (Outcome::Failed(format!("{e:#}")), None)),
    };
    if log.interrupted() {
        // Stopping promptly matters more; the cluster deletion removes the
        // topics.
        if !created.is_empty() {
            log.line(&format!(
                "==> interrupted: leaving {} to the cluster deletion",
                created.join(", ")
            ));
        }
    } else {
        for topic in &created {
            if let Err(e) = delete_topic(ctx, topic, &log) {
                log.line(&format!("WARNING: could not delete topic {topic}: {e:#}"));
            }
        }
    }
    let expected = ctx.arm.expected_client(ctx.test.workload);
    let (outcome, metrics) = match outcome {
        Outcome::Passed => match fs::read_to_string(ctx.dir.join("results.json")) {
            Ok(text) => match Metrics::from_results(&text) {
                Ok(m) if m.client != expected => (
                    Outcome::Failed(format!("results.json is from client `{}`, expected `{expected}`", m.client)),
                    None,
                ),
                Ok(m) => (Outcome::Passed, Some(m)),
                Err(e) => (Outcome::Failed(format!("unreadable results.json: {e}")), None),
            },
            Err(e) => (Outcome::Failed(format!("no results.json: {e}")), None),
        },
        failed => {
            // A harness that failed after writing its summary (e.g. a
            // verification mismatch) still has numbers worth showing, when
            // they are the arm's own client's; the report never compares them.
            let metrics = fs::read_to_string(ctx.dir.join("results.json"))
                .ok()
                .and_then(|text| Metrics::from_results(&text).ok())
                .filter(|m| m.client == expected);
            (failed, metrics)
        },
    };
    match &outcome {
        Outcome::Passed => log.line(&format!("==> {} / {}: passed", ctx.test.id, ctx.arm.label())),
        Outcome::Failed(reason) => log.line(&format!("==> {} / {}: FAILED: {reason}", ctx.test.id, ctx.arm.label())),
    }
    Ok(ArmResult { arm: ctx.arm, outcome, metrics, load_msg_s, elapsed: started.elapsed() })
}

/// Run the arm's harness: its outcome, and for a consumer test the rate its
/// load reached.
fn run_harness(ctx: &ArmContext<'_>, log: &ArmLog) -> anyhow::Result<(Outcome, Option<f64>)> {
    let options = ctx.options;
    let consumer = ctx.test.workload == Workload::Consumer;
    let mut command = ctx.arm.command(ctx.repo, ctx.test.workload);
    // The Rust producer arms run in the workspace (cargo needs it); every
    // other arm runs in its own directory, since several harnesses write
    // metrics.jsonl to the current directory.
    if command.get_current_dir().is_none() {
        command.current_dir(&ctx.dir);
    }
    if consumer {
        command.args(consumer_args(ctx));
    }
    for var in HARNESS_VARS {
        command.env_remove(var);
    }
    without_cargo_library_paths(&mut command, ctx.repo);
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
    if ctx.test.workload == Workload::Transactional {
        // The arm's own transactional ids (`<id>-<k>` per producer), so no arm
        // fences or inherits the transactions of another.
        env.push(("TRANSACTIONAL_ID".into(), ctx.topic.clone()));
    }
    if let Some(source) = &ctx.source_topic {
        env.push(("SOURCE_TOPIC".into(), source.clone()));
        // Its own consumer group, so it starts from the source's beginning.
        env.push(("GROUP_ID".into(), format!("{}-eos", ctx.topic)));
        // The harness fills the source topic with Kafka's
        // kafka-producer-perf-test.sh from here before it measures.
        env.push(("KAFKA_BIN".into(), ctx.kafka_bin.display().to_string()));
    }
    if consumer {
        // The Python harness's group (the others take it in
        // `consumer_args`), and the client defaults for every consumer
        // setting the test leaves out.
        env.push(("GROUP_ID".into(), ctx.topic.clone()));
        env.push(("USE_DEFAULTS".into(), "True".into()));
    }
    env.extend(ctx.arm.own_env(ctx.repo));
    for (k, v) in &env {
        command.env(k, v);
    }
    let minutes = options.warmup_min + options.duration_min + ctx.test.seed_minutes(options.duration_min);
    let timeout = Duration::from_secs(minutes * 60) + ARM_TIMEOUT_SLACK;
    let mut load = if consumer {
        if log.interrupted() {
            return Ok((Outcome::Failed(INTERRUPTED.to_string()), None));
        }
        let load_path = ctx.dir.join("load.log");
        let load_log = ArmLog::create(&load_path, Arc::clone(&ctx.redactions), ctx.signals_baseline)?;
        log.line("==> load generator started, output in load.log");
        Some(LoadGenerator::start(ctx, timeout.as_secs(), &load_log, load_path)?)
    } else {
        None
    };
    log.line(&format!("==> {}", describe(&command)));
    // A harness whose load has stopped is stopped too: with no records its
    // count-based warmup would never end, and it would wait out its timeout.
    let exit = match &mut load {
        Some(load) => run_watched(&mut command, log, timeout, &mut || load.has_stopped()),
        None => run_logged(&mut command, log, timeout),
    };
    // The load is stopped whatever became of the harness.
    let (load_stopped, load_msg_s) = match load {
        Some(load) => load.stop()?,
        None => (None, None),
    };
    let outcome = match exit? {
        Exit::Status(status) if status.success() => Outcome::Passed,
        Exit::Status(status) => Outcome::Failed(format!("harness exited with {status}")),
        Exit::TimedOut => {
            Outcome::Failed(format!("harness still running after {} min; stopped", timeout.as_secs() / 60))
        },
        Exit::Interrupted => Outcome::Failed(INTERRUPTED.to_string()),
        Exit::Stopped => Outcome::Failed("stopped because its load stopped".to_string()),
    };
    let outcome = match (outcome, load_stopped) {
        // Part of the run had no load: whatever the harness measured, it is
        // not the test's.
        (_, Some(reason)) if !log.interrupted() => Outcome::Failed(reason),
        (outcome, _) => outcome,
    };
    Ok((outcome, load_msg_s))
}

/// The dynamic-library search paths `cargo run` (and so `cargo xtask`)
/// prefixes with its target directories and the toolchain's libraries.
const LIBRARY_PATH_VARS: &[&str] = &["LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"];

/// Give `command` the library search paths the driver was started with,
/// without the entries cargo added. Inherited, they come first and would
/// load a debug build of the Rust client's library left in
/// `rust/target/debug` (by any earlier debug build or test of the workspace)
/// ahead of the release build a harness links: `LD_LIBRARY_PATH` outranks the
/// RUNPATH the Python extension finds it through.
fn without_cargo_library_paths(command: &mut Command, repo: &Path) {
    let rust = repo.join("rust");
    let mut cargo_dirs = vec![rust.join("target")];
    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        cargo_dirs.push(rust.join(dir));
    }
    if let Some(home) = std::env::var_os("RUSTUP_HOME") {
        cargo_dirs.push(PathBuf::from(home).join("toolchains"));
    }
    for var in LIBRARY_PATH_VARS {
        if let Some(value) = std::env::var_os(var) {
            match strip_paths(&value, &cargo_dirs) {
                Some(kept) => command.env(var, kept),
                None => command.env_remove(var),
            };
        }
    }
}

/// `value`, a `:`-separated path list, without empty entries, entries under
/// one of `dirs` and the toolchain's `lib/rustlib` directories; `None` when
/// nothing is left.
fn strip_paths(value: &OsStr, dirs: &[PathBuf]) -> Option<OsString> {
    let kept: Vec<PathBuf> = std::env::split_paths(value)
        .filter(|p| {
            !p.as_os_str().is_empty()
                && !dirs.iter().any(|d| p.starts_with(d))
                && !p.to_string_lossy().contains("/lib/rustlib/")
        })
        .collect();
    if kept.is_empty() {
        return None;
    }
    std::env::join_paths(kept).ok()
}

/// The rate (records/s) in `kafka-producer-perf-test.sh`'s output: its final
/// summary line when it has one, else the windows it reports every few
/// seconds (`<n> records sent, <rate> records/sec ...`), weighted by their
/// records. `None` without either.
fn load_rate(output: &str) -> Option<f64> {
    let mut records = 0.0;
    let mut seconds = 0.0;
    for line in output.lines() {
        let Some((count, rest)) = line.trim().split_once(" records sent, ") else {
            continue;
        };
        let (Ok(count), Some(Ok(rate))) = (
            count.parse::<f64>(),
            rest.split_once(" records/sec").map(|(r, _)| r.parse::<f64>()),
        ) else {
            continue;
        };
        if !(count.is_finite() && rate.is_finite() && rate > 0.0) {
            continue;
        }
        // The summary (it also has the percentiles) covers the whole run.
        if line.contains("50th") {
            return Some(rate);
        }
        records += count;
        seconds += count / rate;
    }
    (seconds > 0.0).then(|| records / seconds)
}

/// The command-line options of a consumer harness other than Python's (which
/// reads the environment): the arm's topic, its group (named after the
/// topic), the measured seconds, the warmup, the consumer settings the test
/// sets, where the harness writes `results.json`, and that the driver
/// produces the load (`--no-produce`). These harnesses count their warmup in
/// records: at the test's fixed rate, `LIMIT_RPS` × the warmup seconds stand
/// for the warmup minutes.
fn consumer_args(ctx: &ArmContext<'_>) -> Vec<String> {
    let options = ctx.options;
    let test = ctx.test;
    let (warmup_flag, settings) = match ctx.arm {
        Arm::Rust => ("--warmup-messages", CONSUMER_FLAGS),
        Arm::Java => ("--warmup", CONSUMER_FLAGS),
        Arm::Librdkafka => ("--warmup", LIBRDKAFKA_CONSUMER_FLAGS),
        Arm::PythonSyncRust | Arm::PythonSyncLibrdkafka | Arm::PythonAsyncRust | Arm::PythonAsyncLibrdkafka => {
            return Vec::new();
        },
    };
    let rate = test.number("LIMIT_RPS");
    // The settings' maximums keep it far from overflowing.
    let warmup_records = rate.saturating_mul(options.warmup_min * 60);
    let dir = ctx.dir.display().to_string();
    let mut args: Vec<String> = Vec::new();
    let mut pairs: Vec<(&str, String)> = vec![
        ("--bootstrap", options.bootstrap.clone()),
        ("--topic", ctx.topic.clone()),
        ("--group-id", ctx.topic.clone()),
        ("--duration", (options.duration_min * 60).to_string()),
        (warmup_flag, warmup_records.to_string()),
        ("--results-file", ctx.dir.join("results.json").display().to_string()),
    ];
    let librdkafka_read = if ctx.arm == Arm::Librdkafka {
        LibrdkafkaRead::of(test)
    } else {
        None
    };
    for (key, flag) in settings {
        // Its batch is LIBRDKAFKA_BATCH when the test sets that.
        if *key == "MAX_POLL_RECORDS" && librdkafka_read.is_some() {
            continue;
        }
        if let Some(value) = test.setting(key) {
            pairs.push((flag, value.to_string()));
        }
    }
    if let Some(LibrdkafkaRead::Batch(n)) = librdkafka_read {
        pairs.push(("--max-poll-records", n.to_string()));
    }
    match ctx.arm {
        Arm::Rust | Arm::Librdkafka => {
            // Its MiB/s are records × this size; the rate is for its banner.
            pairs.push(("--message-size", test.number("VALUE_SIZE").to_string()));
            pairs.push(("--throughput", rate.to_string()));
            pairs.push(("--results-dir", dir));
        },
        _ => {},
    }
    // The C harness reads SASL from the environment instead.
    if matches!(ctx.arm, Arm::Rust | Arm::Java) {
        pairs.push(("--client-config", ctx.client_properties.display().to_string()));
    }
    for (flag, value) in pairs {
        args.push(flag.to_string());
        args.push(value);
    }
    match ctx.arm {
        Arm::Rust => args.extend(["--no-produce".to_string(), "--no-create-topic".to_string()]),
        Arm::Librdkafka => args.push("--no-produce".to_string()),
        _ => {},
    }
    if librdkafka_read == Some(LibrdkafkaRead::Poll) {
        args.push("--single-poll".to_string());
    }
    args
}

/// The Rust and Java consumer harnesses' option for each consumer setting.
const CONSUMER_FLAGS: &[(&str, &str)] = &[
    ("FETCH_MIN_BYTES", "--fetch-min-bytes"),
    ("FETCH_MAX_WAIT_MS", "--fetch-max-wait-ms"),
    ("MAX_PARTITION_FETCH_BYTES", "--max-partition-fetch-bytes"),
    ("MAX_POLL_RECORDS", "--max-poll-records"),
];

/// The librdkafka (C) consumer harness's, after librdkafka's names.
const LIBRDKAFKA_CONSUMER_FLAGS: &[(&str, &str)] = &[
    ("FETCH_MIN_BYTES", "--fetch-min-bytes"),
    ("FETCH_MAX_WAIT_MS", "--fetch-wait-max-ms"),
    ("MAX_PARTITION_FETCH_BYTES", "--fetch-message-max-bytes"),
    ("MAX_POLL_RECORDS", "--max-poll-records"),
];

/// The records an unbounded load may produce per second: more than one
/// `kafka-producer-perf-test.sh` reaches, so the harness, stopped first, ends
/// the run. Kept small enough for the script, which samples latencies every
/// `--num-records` / 500,000 records in an `int`.
const UNBOUNDED_LOAD_MAX_RPS: u64 = 10_000_000;

/// The load generator's `--num-records` and `--throughput` for `seconds` of
/// records at `rate` msg/s; `rate` 0 is unbounded (`--throughput -1`).
fn load_records_and_throughput(rate: u64, seconds: u64) -> (String, String) {
    match rate {
        0 => (UNBOUNDED_LOAD_MAX_RPS.saturating_mul(seconds).to_string(), "-1".to_string()),
        rate => (rate.saturating_mul(seconds).to_string(), rate.to_string()),
    }
}

/// Kafka's `kafka-producer-perf-test.sh` producing a consumer test's load
/// into the arm's topic at `LIMIT_RPS`, or as fast as it can for
/// `LIMIT_RPS=0`, with the test's `VALUE_SIZE` and `COMPRESSION_TYPE`. It starts before the harness and stops after it, so
/// every client reads the same load from the same producer. It runs on this
/// machine, so the records' CreateTime and the consumer's clock agree: a
/// consumer's latency is end to end. Its output goes to the arm's `load.log`.
struct LoadGenerator {
    child: Child,
    pgid: u32,
    pumps: Vec<JoinHandle<()>>,
    /// Its `load.log`, read for the rate it reached.
    log_path: PathBuf,
}

impl LoadGenerator {
    /// Start it with `seconds` of records: the harness is stopped before it
    /// could read them all, so a generator that ends first has failed.
    fn start(ctx: &ArmContext<'_>, seconds: u64, log: &ArmLog, log_path: PathBuf) -> anyhow::Result<LoadGenerator> {
        let test = ctx.test;
        let (records, throughput) = load_records_and_throughput(test.number("LIMIT_RPS"), seconds);
        let mut command = Command::new(ctx.kafka_bin.join("kafka-producer-perf-test.sh"));
        command
            .arg("--topic")
            .arg(&ctx.topic)
            .arg("--num-records")
            .arg(records)
            .arg("--record-size")
            .arg(test.number("VALUE_SIZE").to_string())
            .arg("--throughput")
            .arg(throughput)
            .arg("--producer.config")
            .arg(ctx.client_properties)
            // As the EC2 runs. A consumer sees a record once it is
            // replicated, whatever the producer's acks.
            .args(["--producer-props", "acks=1"]);
        if let Some(codec) = test.setting("COMPRESSION_TYPE") {
            command.arg(format!("compression.type={codec}"));
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        log.line(&format!("==> {}", describe(&command)));
        let mut child = command.spawn().with_context(|| format!("starting {}", describe(&command)))?;
        let pgid = child.id();
        let pumps = [
            pump(child.stdout.take(), log, None),
            pump(child.stderr.take(), log, None),
        ]
        .into_iter()
        .flatten()
        .collect();
        Ok(LoadGenerator { child, pgid, pumps, log_path })
    }

    /// Whether it has stopped by itself.
    fn has_stopped(&mut self) -> anyhow::Result<bool> {
        Ok(self.child.try_wait()?.is_some())
    }

    /// Stop it. The reason when it had already stopped by itself, so part
    /// of the run had no load, and the rate it reached ([`load_rate`]).
    fn stop(mut self) -> anyhow::Result<(Option<String>, Option<f64>)> {
        let stopped = self.child.try_wait()?;
        if stopped.is_none() {
            stop_group(&mut self.child, self.pgid, STOP_GRACE)?;
        }
        reap_group(&mut self.child, self.pgid)?;
        for p in self.pumps {
            let _ = p.join();
        }
        let rate = fs::read_to_string(&self.log_path).ok().as_deref().and_then(load_rate);
        let reason = stopped.map(|status| {
            format!("the load generator (kafka-producer-perf-test.sh) stopped with {status} before the harness; see load.log")
        });
        Ok((reason, rate))
    }
}

fn topics_command(ctx: &ArmContext<'_>) -> Command {
    let mut command = Command::new(ctx.topics_cli);
    command
        .args(["--bootstrap-server", &ctx.options.bootstrap, "--command-config"])
        .arg(ctx.client_properties);
    command
}

/// `Ok(Err(reason))` when the topic could not be created.
fn create_topic(ctx: &ArmContext<'_>, topic: &str, log: &ArmLog) -> anyhow::Result<Result<(), String>> {
    let partitions = ctx.test.partitions().to_string();
    for attempt in 1..=TOPIC_CREATE_ATTEMPTS {
        log.line(&format!(
            "==> creating topic {topic} with {partitions} partitions (attempt {attempt}/{TOPIC_CREATE_ATTEMPTS})"
        ));
        // No --replication-factor: the cluster's default (Confluent Cloud
        // rejects anything else).
        let mut create = topics_command(ctx);
        create.args([
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            &partitions,
        ]);
        match run_logged(&mut create, log, TOPIC_COMMAND_TIMEOUT)? {
            Exit::Interrupted => return Ok(Err(INTERRUPTED.to_string())),
            Exit::Status(s) if s.success() => match topic_described(ctx, topic, log)? {
                Ok(true) => return Ok(Ok(())),
                Ok(false) => {},
                Err(reason) => return Ok(Err(reason)),
            },
            Exit::Status(_) | Exit::TimedOut | Exit::Stopped => {},
        }
        if attempt < TOPIC_CREATE_ATTEMPTS && sleep_unless_signalled(TOPIC_CREATE_RETRY_DELAY, log) {
            return Ok(Err(INTERRUPTED.to_string()));
        }
    }
    Ok(Err(format!(
        "could not create topic {topic} after {TOPIC_CREATE_ATTEMPTS} attempts"
    )))
}

/// Whether `--describe` sees the topic with all its partitions; an error
/// (`Err` inside) when it has more than the test's, so it existed before the
/// run.
fn topic_described(ctx: &ArmContext<'_>, topic: &str, log: &ArmLog) -> anyhow::Result<Result<bool, String>> {
    let mut describe = topics_command(ctx);
    describe.args(["--describe", "--topic", topic]);
    let partitions = ctx.test.partitions();
    Ok(match output_logged(&mut describe, log, TOPIC_COMMAND_TIMEOUT)? {
        Some(text) => {
            // `Topic: <name>\tPartition: ...`: the name followed by a tab, so
            // a topic does not count the partitions of `<name>-source`.
            let seen = text
                .lines()
                .filter(|l| l.contains("Partition:") && l.contains(&format!("Topic: {topic}\t")))
                .count() as u64;
            if seen > partitions {
                return Ok(Err(format!(
                    "topic {topic} already exists, with {seen} partitions instead of {partitions}; delete it or use \
                     another --topic-prefix"
                )));
            }
            if seen < partitions {
                log.line(&format!("==> topic {topic} shows {seen} of {partitions} partitions"));
            }
            Ok(seen == partitions)
        },
        None => Ok(false),
    })
}

/// Best effort: the cluster is deleted at the end of the run anyway.
fn delete_topic(ctx: &ArmContext<'_>, topic: &str, log: &ArmLog) -> anyhow::Result<()> {
    log.line(&format!("==> deleting topic {topic}"));
    let mut delete = topics_command(ctx);
    delete.args(["--delete", "--if-exists", "--topic", topic]);
    match run_logged(&mut delete, log, TOPIC_COMMAND_TIMEOUT)? {
        Exit::Status(s) if s.success() => {},
        _ => log.line(&format!(
            "WARNING: could not delete topic {topic}; the cluster deletion removes it"
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
/// the credentials masked.
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
    write_redacted(file, &redact(text, redactions));
}

/// [`write_line`] for text already redacted.
fn write_redacted(file: &Mutex<File>, text: &str) {
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
    /// Stopped because the caller's `stop_if` said so.
    Stopped,
}

/// Run `command` in its own process group, streaming its output into `log`,
/// stopping the whole group on timeout or on a signal to the driver; not
/// started at all once the run is interrupted.
fn run_logged(command: &mut Command, log: &ArmLog, timeout: Duration) -> anyhow::Result<Exit> {
    run_watched(command, log, timeout, &mut || Ok(false))
}

/// Like [`run_logged`], also stopping the group, with [`Exit::Stopped`], as
/// soon as `stop_if` returns `true` (it is asked every half second).
fn run_watched(
    command: &mut Command,
    log: &ArmLog,
    timeout: Duration,
    stop_if: &mut dyn FnMut() -> anyhow::Result<bool>,
) -> anyhow::Result<Exit> {
    let (exit, _) = run_capturing(command, log, timeout, false, stop_if)?;
    Ok(exit)
}

/// Like [`run_logged`], returning the (redacted) output when the command
/// succeeded.
fn output_logged(command: &mut Command, log: &ArmLog, timeout: Duration) -> anyhow::Result<Option<String>> {
    Ok(match run_capturing(command, log, timeout, true, &mut || Ok(false))? {
        (Exit::Status(s), captured) if s.success() => Some(captured),
        _ => None,
    })
}

fn run_capturing(
    command: &mut Command,
    log: &ArmLog,
    timeout: Duration,
    capture: bool,
    stop_if: &mut dyn FnMut() -> anyhow::Result<bool>,
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
    let exit = wait_group(&mut child, pgid, timeout, log.signals_baseline, stop_if)?;
    // A process the command started can outlive it and keep the pipes open;
    // give the group a moment, then stop it so the pumps reach end of file.
    reap_group(&mut child, pgid)?;
    for p in pumps.into_iter().flatten() {
        let _ = p.join();
    }
    let text = captured.lock().map(|s| s.clone()).unwrap_or_default();
    Ok((exit, text))
}

/// Wait for `child` to exit, stopping its process group on a signal to the
/// driver (since `signals_baseline`), after `timeout`, or as soon as
/// `stop_if` returns `true` (it is asked every half second).
fn wait_group(
    child: &mut Child,
    pgid: u32,
    timeout: Duration,
    signals_baseline: u32,
    stop_if: &mut dyn FnMut() -> anyhow::Result<bool>,
) -> anyhow::Result<Exit> {
    let deadline = Instant::now() + timeout;
    let exit = loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Exit::Status(status));
        }
        if crate::signals::count() > signals_baseline {
            break Exit::Interrupted;
        }
        if Instant::now() >= deadline {
            break Exit::TimedOut;
        }
        if stop_if()? {
            break Exit::Stopped;
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    stop_group(child, pgid, STOP_GRACE)?;
    Ok(exit)
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
                    let line = redact(line.trim_end_matches(['\n', '\r']), &redactions);
                    write_redacted(&file, &line);
                    if let Some(keep) = &keep {
                        if let Ok(mut k) = keep.lock() {
                            let _ = writeln!(k, "{line}");
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
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 LINGER=5",
                "unknown setting `LINGER`",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 LIMIT_RPS=1",
                "`LIMIT_RPS` is set twice",
            ),
            (
                "t rust PARTITIONS=0 VALUE_SIZE=1 LIMIT_RPS=0",
                "PARTITIONS=0: expected a whole number from 1 to 10000",
            ),
            (
                "t rust PARTITIONS=10001 VALUE_SIZE=1 LIMIT_RPS=0",
                "PARTITIONS=10001: expected a whole number from 1 to 10000",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=10000000000000000",
                "LIMIT_RPS=10000000000000000: expected a whole number up to 1000000",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=4294968320 LIMIT_RPS=0",
                "VALUE_SIZE=4294968320: expected a whole number from 1 to 1000000",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=999000 KEY_SIZE=1001 LIMIT_RPS=0",
                "KEY_SIZE + VALUE_SIZE is 1000001 bytes, above 1000000",
            ),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1024 KEY_SIZE=0 LIMIT_RPS=0 MAX_REQUEST_SIZE=1",
                "KEY_SIZE + VALUE_SIZE is 1024 bytes, which with 512 bytes of headers does not fit MAX_REQUEST_SIZE=1 KiB",
            ),
            (
                "t rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=1 FETCH_MIN_BYTES=100000001",
                "FETCH_MIN_BYTES=100000001: expected a whole number from 1 to 100000000",
            ),
            (
                "t rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 NUM_TRANSACTIONAL_PRODUCERS=65",
                "NUM_TRANSACTIONAL_PRODUCERS=65: expected a whole number from 1 to 64",
            ),
            ("t rust PARTITIONS=+5 VALUE_SIZE=1 LIMIT_RPS=0", "PARTITIONS=+5"),
            (
                "t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=-1",
                "LIMIT_RPS=-1: expected a whole number up to 1000000",
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
    fn parses_the_batches_file() {
        let tests = parse_tests(SAMPLE).unwrap();
        let batches = parse_batches(
            "# comment\nboth  rust  producer-max producer-fixed  # trailing\n\npython-only python producer-fixed\n",
            &tests,
        )
        .unwrap();
        assert_eq!(
            batches,
            [
                Batch {
                    name: "both".to_string(),
                    group: Group::Rust,
                    tests: strings(&["producer-max", "producer-fixed"]),
                },
                Batch {
                    name: "python-only".to_string(),
                    group: Group::Python,
                    tests: strings(&["producer-fixed"]),
                },
            ]
        );
        assert!(parse_batches("# none\n", &tests).unwrap().is_empty());
        for (text, message) in [
            ("Bad rust producer-max", "invalid batch name `Bad`"),
            ("b", "batch `b` names no client group"),
            ("b java producer-max", "unknown client group `java`"),
            ("b rust", "batch `b` names no tests"),
            ("b rust producer-nope", "unknown test `producer-nope`"),
            (
                "b python producer-max",
                "test `producer-max` does not apply to client group `python` (only rust)",
            ),
            ("b rust producer-max producer-max", "test `producer-max` is listed twice"),
            ("b rust producer-max\nb rust producer-fixed", "batch `b` is defined twice"),
        ] {
            let e = format!("{:#}", parse_batches(text, &tests).unwrap_err());
            assert!(e.starts_with(BATCHES_FILE), "{text}: {e}");
            assert!(e.contains(message), "{text}: {e}");
        }
    }

    #[test]
    fn the_checked_in_batches_file_is_valid() {
        let tests = load_tests().unwrap();
        let batches = load_batches(&tests).unwrap();
        for name in ["producer-basic-perf-python", "consumer-basic-perf-python"] {
            let batch = batches.iter().find(|b| b.name == name).unwrap();
            assert_eq!(batch.group, Group::Python, "{name}");
            for id in &batch.tests {
                let test = tests.iter().find(|t| &t.id == id).unwrap();
                test.check_arms(Group::Python.arms()).unwrap();
            }
        }
    }

    #[test]
    fn selects_a_batch() {
        let all = parse_tests(SAMPLE).unwrap();
        let batches = parse_batches("b rust producer-max producer-fixed\n", &all).unwrap();
        let picked = select_batch(&all, &batches, "b", Group::Rust).unwrap();
        let ids: Vec<&str> = picked.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["producer-max", "producer-fixed"]);
        let err = select_batch(&all, &batches, "c", Group::Rust).unwrap_err().to_string();
        assert_eq!(err, "unknown batch `c` (known: b)");
        let err = select_batch(&all, &batches, "b", Group::Python).unwrap_err().to_string();
        assert_eq!(err, "batch `b` runs the rust group's clients, not the python group's (--group)");
    }

    #[test]
    fn runs_a_batch_by_name() {
        let parse = |args: &[&str]| RunOptions::parse(&strings(args), false);
        let options = parse(&["--group", "python", "--batch", "producer-basic-perf-python"]).unwrap();
        assert_eq!(options.batch.as_deref(), Some("producer-basic-perf-python"));
        let ids: Vec<&str> = options.tests.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "producer-max-1kb-200p",
                "producer-max-1kb-36p-lz4",
                "producer-fixed-100k-1kb-200p",
                "producer-max-1kb-200p-key100b",
            ]
        );
        check(&options).unwrap();
        let options = parse(&["--group", "python", "--tests", "producer-max-1kb-200p"]).unwrap();
        assert_eq!(options.batch, None);
        let err = |args: &[&str]| parse(args).err().unwrap().to_string();
        assert_eq!(
            err(&["--group", "rust", "--batch", "producer-basic-perf-python"]),
            "batch `producer-basic-perf-python` runs the python group's clients, not the rust group's (--group)"
        );
        assert_eq!(
            err(&[
                "--group",
                "python",
                "--batch",
                "producer-basic-perf-python",
                "--tests",
                "producer-max-1kb-200p"
            ]),
            "give one of --tests, --batch and --scenario"
        );
        assert_eq!(
            err(&["--group", "python", "--batch", "b", "--scenario", "PARTITIONS=1"]),
            "give one of --tests, --batch and --scenario"
        );
        assert_eq!(err(&["--group", "python"]), "--tests, --batch or --scenario is required");
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
        assert_eq!(minutes(&flags, "--duration-min", 1, 60).unwrap(), Some(60));
        assert_eq!(minutes(&flags, "--warmup-min", 0, 30).unwrap(), Some(0));
        assert_eq!(minutes(&BTreeMap::new(), "--duration-min", 1, 60).unwrap(), None);
        let flags = parse_flags(&strings(&["--duration-min", "61"]), SHARED_FLAGS, &[]).unwrap();
        assert_eq!(
            minutes(&flags, "--duration-min", 1, 60).unwrap_err().to_string(),
            "--duration-min must be between 1 and 60 minutes, got 61"
        );
        let flags = parse_flags(&strings(&["--duration-min", "half"]), SHARED_FLAGS, &[]).unwrap();
        assert_eq!(
            minutes(&flags, "--duration-min", 1, 60).unwrap_err().to_string(),
            "--duration-min: `half` is not a whole number of minutes"
        );
    }

    fn options(group: Group, tests: usize, duration_min: u64) -> RunOptions {
        let test = parse_tests("t rust,python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        RunOptions {
            group,
            arms: group.arms().to_vec(),
            tests: vec![test; tests],
            batch: None,
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
            "6 tests take ~26 h 48 min, more than the budget of 22 h 20 min; only the first 5 fit"
        );
        check(&with_budget(5, 1340)).unwrap();
        let err = check(&with_budget(1, 100)).unwrap_err().to_string();
        assert_eq!(
            err,
            "1 test takes ~4 h 28 min, more than the budget of 1 h 40 min; shorten the measurement or run fewer clients"
        );
        // Fewer clients, less time.
        let two = RunOptions {
            arms: vec![Arm::PythonSyncRust, Arm::PythonSyncLibrdkafka],
            ..with_budget(1, 140)
        };
        assert_eq!(two.estimate_minutes(), 134);
        check(&two).unwrap();
        // Without a budget any length passes.
        check(&options(Group::Python, 6, 60)).unwrap();
    }

    #[test]
    fn check_rejects_a_fixed_rate_above_ingress() {
        let with_test = |line: &str, ingress: Option<f64>| RunOptions {
            tests: vec![parse_tests(line).unwrap().remove(0)],
            ingress_mb_s: ingress,
            ..options(Group::Rust, 1, 5)
        };
        // 300,000 × 1 KiB ≈ 307 MB/s.
        let fast = "t rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=300000";
        let err = check(&with_test(fast, Some(240.0))).unwrap_err().to_string();
        assert_eq!(
            err,
            "t: its fixed rate, 307 MB/s, is above the cluster's ~240 MB/s ingress, so no client could hold it; use a \
             larger cluster or lower LIMIT_RPS (LIMIT_RPS=0 for an unbounded producer test)"
        );
        check(&with_test(fast, Some(600.0))).unwrap();
        // Unknown ingress, or an unbounded test: nothing to compare.
        check(&with_test(fast, None)).unwrap();
        check(&with_test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0", Some(1.0))).unwrap();
    }

    #[test]
    fn selects_clients() {
        let ids = |group: Group, list: &str| -> anyhow::Result<Vec<&'static str>> {
            Ok(group.select_arms(list)?.into_iter().map(Arm::id).collect())
        };
        assert_eq!(ids(Group::Rust, "all").unwrap(), ["rust", "librdkafka", "java"]);
        // The group's order, whatever the list's.
        assert_eq!(ids(Group::Rust, " java, rust ").unwrap(), ["rust", "java"]);
        assert_eq!(
            ids(Group::Python, "python-async-librdkafka,python-sync-rust").unwrap(),
            ["python-sync-rust", "python-async-librdkafka"]
        );
        assert_eq!(
            ids(Group::Rust, "rust,python-sync-rust").unwrap_err().to_string(),
            "unknown client `python-sync-rust` in group `rust` (expected `all` or some of: rust, librdkafka, java)"
        );
        assert_eq!(
            ids(Group::Rust, "rust,rust").unwrap_err().to_string(),
            "client `rust` is listed twice"
        );
        assert_eq!(
            ids(Group::Rust, " , ").unwrap_err().to_string(),
            "no clients given (--clients all, or some of: rust, librdkafka, java)"
        );
    }

    #[test]
    fn names_a_scenario_after_its_settings() {
        let id = |text: &str| parse_scenario(text, Group::Rust).unwrap().id;
        assert_eq!(
            id("PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            "producer-fixed-10k-1kb-200p"
        );
        // Settings at the harnesses' defaults add nothing, others are named.
        assert_eq!(
            id(
                "WORKLOAD=producer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=0 ACKS=all COMPRESSION_TYPE=none \
                BATCH_SIZE=1024 LINGER_MS=5 ENABLE_IDEMPOTENCE=false"
            ),
            "producer-max-1kb-200p"
        );
        assert_eq!(
            id("PARTITIONS=36 VALUE_SIZE=100 LIMIT_RPS=1500 ACKS=1 COMPRESSION_TYPE=lz4 BATCH_SIZE=64 LINGER_MS=0"),
            "producer-fixed-1500-100b-36p-acks1-lz4-batch64kb-linger0ms"
        );
        assert_eq!(
            id("WORKLOAD=transactional PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            "txn-fixed-10k-1kb-200p-100-per-txn"
        );
        assert_eq!(
            id(
                "WORKLOAD=transactional TXN_MODE=eos PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=1000 \
                RECORDS_PER_TRANSACTION=1000 ABORT_RATE=0.125 NUM_TRANSACTIONAL_PRODUCERS=16"
            ),
            "eos-fixed-1k-1kb-36p-1000-per-txn-abort12p5-16-producers"
        );
        assert_eq!(
            id("WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            "consumer-fixed-10k-1kb-200p"
        );
        // Any consumer setting a test sets differs from a client default.
        assert_eq!(
            id("WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 FETCH_MIN_BYTES=4194304 MAX_POLL_RECORDS=4000"),
            "consumer-fixed-10k-1kb-200p-fetchmin4mb-maxpoll4000"
        );
        assert_eq!(
            id(
                "WORKLOAD=consumer PARTITIONS=36 VALUE_SIZE=100 LIMIT_RPS=1500 COMPRESSION_TYPE=lz4 \
                FETCH_MAX_WAIT_MS=100 MAX_PARTITION_FETCH_BYTES=4194304"
            ),
            "consumer-fixed-1500-100b-36p-lz4-fetchwait100ms-partfetch4mb"
        );
        // How the librdkafka clients read.
        assert_eq!(
            id("WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=300000 LIBRDKAFKA_BATCH=0"),
            "consumer-fixed-300k-1kb-200p-librdkafka-poll"
        );
        assert_eq!(
            id("WORKLOAD=consumer PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=1000 MAX_POLL_RECORDS=4500 LIBRDKAFKA_BATCH=4000"),
            "consumer-fixed-1k-1kb-36p-maxpoll4500-librdkafka-batch4000"
        );
        // Too long: cut, with a hash of the whole name.
        let long = id(
            "PARTITIONS=200 VALUE_SIZE=524288 LIMIT_RPS=0 KEY_SIZE=1024 ACKS=1 COMPRESSION_TYPE=zstd \
                       BATCH_SIZE=2048 LINGER_MS=100 MAX_IN_FLIGHT=5 BUFFER_MEMORY=64 MAX_REQUEST_SIZE=2048",
        );
        // At most the limit: the cut name loses a trailing `-`.
        assert!(long.len() <= MAX_TEST_ID_LEN && long.len() >= MAX_TEST_ID_LEN - 1, "{long}");
        assert!(
            long.starts_with("producer-max-512kb-200p-key1kb-acks1-zstd-batch2mb-"),
            "{long}"
        );
        validate_test_id(&long).unwrap();
        // Two scenarios with the same start differ by their hash.
        let other = id(
            "PARTITIONS=200 VALUE_SIZE=524288 LIMIT_RPS=0 KEY_SIZE=1024 ACKS=1 COMPRESSION_TYPE=zstd \
                        BATCH_SIZE=2048 LINGER_MS=100 MAX_IN_FLIGHT=5 BUFFER_MEMORY=64 MAX_REQUEST_SIZE=4096",
        );
        assert_ne!(long, other);

        let test = parse_scenario("PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", Group::Python).unwrap();
        assert!(test.applies_to(Group::Python) && !test.applies_to(Group::Rust));
        assert_eq!(test.workload, Workload::Producer);
    }

    #[test]
    fn checks_settings_against_the_workload() {
        let err = |text: &str| format!("{:#}", parse_scenario(text, Group::Rust).unwrap_err());
        let base = "PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=100";
        for (extra, message) in [
            (
                "WORKLOAD=consume",
                "--scenario: WORKLOAD=consume: expected `producer`, `transactional` or `consumer`",
            ),
            ("WORKLOAD=producer WORKLOAD=producer", "--scenario: `WORKLOAD` is set twice"),
            (
                "WORKLOAD=transactional ACKS=1",
                "--scenario: ACKS does not apply to transactional tests (they always use idempotence and acks=all)",
            ),
            (
                "WORKLOAD=transactional ENABLE_IDEMPOTENCE=false",
                "--scenario: ENABLE_IDEMPOTENCE does not apply to transactional tests",
            ),
            (
                "ABORT_RATE=0.5",
                "--scenario: ABORT_RATE applies to transactional tests only (WORKLOAD=transactional)",
            ),
            (
                "ENABLE_IDEMPOTENCE=true ACKS=1",
                "--scenario: ACKS=1 with ENABLE_IDEMPOTENCE=true: an idempotent producer needs ACKS=all",
            ),
            (
                "ENABLE_IDEMPOTENCE=true MAX_IN_FLIGHT=6",
                "--scenario: MAX_IN_FLIGHT above 5: an idempotent or transactional producer allows at most 5",
            ),
            ("WORKLOAD=transactional MAX_IN_FLIGHT=10", "--scenario: MAX_IN_FLIGHT above 5"),
            ("ACKS=2", "ACKS=2: expected one of all, -1, 1, 0"),
            (
                "WORKLOAD=transactional ABORT_RATE=1.5",
                "ABORT_RATE=1.5: expected a decimal number from 0 to 1, such as 0.5",
            ),
            ("WORKLOAD=transactional ABORT_RATE=.5", "ABORT_RATE=.5"),
            (
                "WORKLOAD=transactional TXN_MODE=consume",
                "TXN_MODE=consume: expected `produce` or `eos`",
            ),
            ("WORKLOAD=transactional RECORDS_PER_TRANSACTION=0", "RECORDS_PER_TRANSACTION=0"),
            (
                "WORKLOAD=consumer BATCH_SIZE=1024",
                "--scenario: BATCH_SIZE does not apply to consumer tests (their load comes from Kafka's \
                 kafka-producer-perf-test.sh)",
            ),
            ("WORKLOAD=consumer ACKS=1", "--scenario: ACKS does not apply to consumer tests"),
            (
                "WORKLOAD=consumer KEY_SIZE=16",
                "--scenario: KEY_SIZE does not apply to consumer tests",
            ),
            (
                "WORKLOAD=consumer ABORT_RATE=0.5",
                "--scenario: ABORT_RATE applies to transactional tests only",
            ),
            (
                "FETCH_MIN_BYTES=1",
                "--scenario: FETCH_MIN_BYTES applies to consumer tests only (WORKLOAD=consumer)",
            ),
            (
                "WORKLOAD=transactional MAX_POLL_RECORDS=500",
                "--scenario: MAX_POLL_RECORDS applies to consumer tests only",
            ),
            ("WORKLOAD=consumer FETCH_MIN_BYTES=0", "FETCH_MIN_BYTES=0"),
            ("WORKLOAD=consumer MAX_POLL_RECORDS=0", "MAX_POLL_RECORDS=0"),
            (
                "LIBRDKAFKA_BATCH=0",
                "--scenario: LIBRDKAFKA_BATCH applies to consumer tests only (WORKLOAD=consumer)",
            ),
            (
                "WORKLOAD=consumer LIBRDKAFKA_BATCH=1000001",
                "LIBRDKAFKA_BATCH=1000001: expected a whole number up to 1000000",
            ),
        ] {
            let e = err(&format!("{base} {extra}"));
            assert!(e.contains(message), "{extra}: {e}");
        }
        let e = err("WORKLOAD=transactional TXN_MODE=eos PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0");
        assert!(e.contains("TXN_MODE=eos needs LIMIT_RPS above 0"), "{e}");
        assert_eq!(err("PARTITIONS=1 VALUE_SIZE=1"), "--scenario must set LIMIT_RPS");
        // Accepted: idempotence with acks=all / -1 and at most 5 in flight,
        // and the transactional settings.
        for ok in [
            "ENABLE_IDEMPOTENCE=true ACKS=-1 MAX_IN_FLIGHT=5",
            "ENABLE_IDEMPOTENCE=false ACKS=0 MAX_IN_FLIGHT=1000",
            "WORKLOAD=transactional TXN_MODE=eos ABORT_RATE=0 RECORDS_PER_TRANSACTION=10 NUM_TRANSACTIONAL_PRODUCERS=16",
            "WORKLOAD=transactional ABORT_RATE=1.0",
            "WORKLOAD=consumer COMPRESSION_TYPE=lz4 FETCH_MIN_BYTES=1 FETCH_MAX_WAIT_MS=0 \
             MAX_PARTITION_FETCH_BYTES=1048576 MAX_POLL_RECORDS=500",
            "WORKLOAD=consumer LIBRDKAFKA_BATCH=0",
            "WORKLOAD=consumer MAX_POLL_RECORDS=4500 LIBRDKAFKA_BATCH=4000",
        ] {
            parse_scenario(&format!("{base} {ok}"), Group::Rust).unwrap();
        }
    }

    #[test]
    fn small_batches_leave_out_the_librdkafka_client() {
        let test = parse_scenario("PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 BATCH_SIZE=16", Group::Rust).unwrap();
        let err = test.check_arms(Group::Rust.arms()).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "test `{}`: BATCH_SIZE below 1024 KiB is not comparable with the librdkafka (C) client (its harness \
                 runs librdkafka with 1 MiB batches below it); leave that client out",
                test.id
            )
        );
        test.check_arms(&[Arm::Rust, Arm::Java]).unwrap();
        test.check_arms(Group::Python.arms()).unwrap();
    }

    #[test]
    fn an_unbounded_consumer_load_runs_only_the_python_clients() {
        let test = parse_scenario("WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", Group::Python).unwrap();
        assert_eq!(test.id, "consumer-max-1b-1p");
        test.check_arms(Group::Python.arms()).unwrap();
        for arm in [Arm::Rust, Arm::Java, Arm::Librdkafka] {
            let err = test.check_arms(&[arm]).unwrap_err().to_string();
            assert_eq!(
                err,
                format!(
                    "test `consumer-max-1b-1p`: a consumer test with an unbounded load (LIMIT_RPS=0) runs only the \
                     Python clients, whose warmup is timed; the {} client counts its warmup in records at LIMIT_RPS",
                    arm.id()
                )
            );
        }
        // A fixed load is for every client.
        let fixed = parse_scenario("WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=1", Group::Rust).unwrap();
        fixed.check_arms(Group::Rust.arms()).unwrap();
    }

    #[test]
    fn sizes_the_load_generator() {
        assert_eq!(load_records_and_throughput(300, 60), ("18000".to_string(), "300".to_string()));
        assert_eq!(load_records_and_throughput(0, 60), ("600000000".to_string(), "-1".to_string()));
        // The script's latency sampling interval, records / 500,000, fits its
        // int for any run the budget allows (a day).
        let (records, _) = load_records_and_throughput(0, 24 * 3600);
        assert!(records.parse::<u64>().unwrap() / 500_000 <= i32::MAX as u64);
    }

    #[test]
    fn derives_settings_for_each_client() {
        let env = |text: &str, arm: Arm| -> BTreeMap<String, String> {
            parse_scenario(text, Group::Rust).unwrap().env_for(arm).into_iter().collect()
        };
        // acks=0: no offsets to verify.
        let acks0 = env("PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ACKS=0", Arm::Java);
        assert_eq!(acks0.get("DO_VERIFY").map(String::as_str), Some("False"));
        assert_eq!(acks0.get("ACKS").map(String::as_str), Some("0"));
        assert!(!env("PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ACKS=1", Arm::Java).contains_key("DO_VERIFY"));
        // An idempotent librdkafka producer gets at most 5 in flight unless
        // the test sets it.
        let idempotent = "PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 ENABLE_IDEMPOTENCE=true";
        assert_eq!(
            env(idempotent, Arm::Librdkafka).get("MAX_IN_FLIGHT").map(String::as_str),
            Some("5")
        );
        assert!(!env(idempotent, Arm::Rust).contains_key("MAX_IN_FLIGHT"));
        assert_eq!(
            env(&format!("{idempotent} MAX_IN_FLIGHT=2"), Arm::Librdkafka)
                .get("MAX_IN_FLIGHT")
                .map(String::as_str),
            Some("2")
        );
        assert!(!env("PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", Arm::Librdkafka).contains_key("MAX_IN_FLIGHT"));
        // `WORKLOAD` picks the harness; it is not one of their settings.
        let txn = env("WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0", Arm::Librdkafka);
        assert!(!txn.contains_key("WORKLOAD") && !txn.contains_key("MAX_IN_FLIGHT"), "{txn:?}");
    }

    #[test]
    fn times_the_eos_source_filling() {
        let test = |text: &str| parse_scenario(text, Group::Rust).unwrap();
        // 10,000 msg/s × 30 min × 1,024 B is 18.4 GB, ~6.1 min at 50 MB/s.
        let eos = test("WORKLOAD=transactional TXN_MODE=eos PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000");
        assert!(eos.is_eos());
        assert_eq!(eos.seed_minutes(30), 7);
        assert_eq!(
            test("WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000").seed_minutes(30),
            0
        );
        assert_eq!(test("PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000").seed_minutes(30), 0);
        // Each arm fills its own source topic.
        let mut options = options(Group::Rust, 0, 30);
        options.tests = vec![eos];
        assert_eq!(options.estimate_minutes(), 3 * (5 + 30 + 2 + 7));
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
            Arm::PythonAsyncLibrdkafka.command(repo, Workload::Producer).get_program(),
            "/repo/venv-librdkafka/bin/python"
        );
    }

    #[test]
    fn each_workload_runs_its_own_harnesses() {
        let repo = Path::new("/repo");
        let args = |arm: Arm, workload: Workload| -> Vec<String> {
            let command = arm.command(repo, workload);
            std::iter::once(command.get_program())
                .chain(command.get_args())
                .map(|a| a.to_string_lossy().into_owned())
                .collect()
        };
        assert!(args(Arm::Rust, Workload::Producer).contains(&"producer-perf-test".to_string()));
        assert!(args(Arm::Rust, Workload::Transactional).contains(&"transactional-producer-perf-test".to_string()));
        assert_eq!(args(Arm::Librdkafka, Workload::Producer), ["/repo/c/build/producer_perf_test"]);
        assert_eq!(
            args(Arm::Librdkafka, Workload::Transactional),
            ["/repo/c/build/transactional_producer_perf_test"]
        );
        let jar = "/repo/tools/java-perf-test/build/libs/java-perf-test-all.jar";
        assert_eq!(args(Arm::Java, Workload::Producer), ["java", "-jar", jar]);
        assert_eq!(
            args(Arm::Java, Workload::Transactional),
            [
                "java",
                "-cp",
                jar,
                "io.confluent.kafka.perftest.TransactionalProducerPerformanceTest"
            ]
        );
        assert_eq!(
            args(Arm::PythonSyncRust, Workload::Transactional),
            [
                "/repo/venv/bin/python",
                "/repo/python/test/performance/transactional_producer_performance_test.py"
            ]
        );
        assert_eq!(
            Arm::Librdkafka.prerequisites(repo, Workload::Transactional),
            [PathBuf::from("/repo/c/build/transactional_producer_perf_test")]
        );
        assert_eq!(Arm::Java.expected_client(Workload::Producer), "java");
        assert_eq!(Arm::Java.expected_client(Workload::Transactional), "java-txn");
        assert_eq!(Arm::PythonAsyncRust.expected_client(Workload::Producer), "python-rust-async");
        assert_eq!(Arm::PythonAsyncRust.expected_client(Workload::Transactional), "python-rust-txn");

        assert_eq!(args(Arm::Rust, Workload::Consumer), ["/repo/rust/target/release/consumer-perf"]);
        assert!(Arm::Rust.command(repo, Workload::Consumer).get_current_dir().is_none());
        assert_eq!(
            args(Arm::Librdkafka, Workload::Consumer),
            ["/repo/rust/consumer-perf/compare/build/librdkafka_e2e"]
        );
        assert_eq!(
            args(Arm::Java, Workload::Consumer),
            [
                "java",
                "-cp",
                &format!("{jar}:/repo/rust/consumer-perf/compare/build"),
                "JavaE2E"
            ]
        );
        assert_eq!(
            args(Arm::PythonAsyncLibrdkafka, Workload::Consumer),
            [
                "/repo/venv-librdkafka/bin/python",
                "/repo/python/test/performance/consumer_performance_test.py"
            ]
        );
        assert_eq!(
            Arm::Rust.prerequisites(repo, Workload::Consumer),
            [PathBuf::from("/repo/rust/target/release/consumer-perf")]
        );
        assert_eq!(
            Arm::Java.prerequisites(repo, Workload::Consumer),
            [
                PathBuf::from(jar),
                PathBuf::from("/repo/rust/consumer-perf/compare/build/JavaE2E.class")
            ]
        );
        assert_eq!(Arm::Librdkafka.expected_client(Workload::Consumer), "librdkafka-consumer");
        assert_eq!(
            Arm::PythonAsyncLibrdkafka.expected_client(Workload::Consumer),
            "python-librdkafka-async-consumer"
        );
    }

    /// The options `consumer_args` gives `arm` for a consumer `scenario`.
    fn consumer_args_for(scenario: &str, arm: Arm) -> Vec<String> {
        let mut options = options(Group::Rust, 0, 30);
        options.bootstrap = "broker:9092".to_string();
        let test = parse_scenario(scenario, Group::Rust).unwrap();
        let credentials = Credentials { key: "KEY".to_string(), secret: "hunter2".to_string() };
        let ctx = ArmContext {
            repo: Path::new("/repo"),
            options: &options,
            test: &test,
            arm,
            dir: PathBuf::from("/results/t/rust"),
            topic: "perf-t".to_string(),
            source_topic: None,
            topics_cli: Path::new("/kafka/bin/kafka-topics.sh"),
            kafka_bin: Path::new("/kafka/bin"),
            client_properties: Path::new("/private/client.properties"),
            credentials: &credentials,
            redactions: Arc::new(Vec::new()),
            signals_baseline: 0,
        };
        consumer_args(&ctx)
    }

    #[test]
    fn passes_a_consumer_test_to_each_harness() {
        let scenario = "WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 FETCH_MAX_WAIT_MS=100 \
                        MAX_POLL_RECORDS=4000";
        let args = |arm: Arm| consumer_args_for(scenario, arm).join(" ");
        // 30 measured minutes, and 5 warmup minutes at 10,000 msg/s.
        let common = "--bootstrap broker:9092 --topic perf-t --group-id perf-t --duration 1800";
        let results = "--results-file /results/t/rust/results.json";
        assert_eq!(
            args(Arm::Rust),
            format!(
                "{common} --warmup-messages 3000000 {results} --fetch-max-wait-ms 100 --max-poll-records 4000 \
                 --message-size 1024 --throughput 10000 --results-dir /results/t/rust \
                 --client-config /private/client.properties --no-produce --no-create-topic"
            )
        );
        assert_eq!(
            args(Arm::Librdkafka),
            format!(
                "{common} --warmup 3000000 {results} --fetch-wait-max-ms 100 --max-poll-records 4000 \
                 --message-size 1024 --throughput 10000 --results-dir /results/t/rust --no-produce"
            )
        );
        assert_eq!(
            args(Arm::Java),
            format!(
                "{common} --warmup 3000000 {results} --fetch-max-wait-ms 100 --max-poll-records 4000 \
                 --client-config /private/client.properties"
            )
        );
        // The Python harness reads the environment.
        for arm in Group::Python.arms() {
            assert_eq!(args(*arm), "", "{arm:?}");
        }
        // The credentials never reach a command line.
        for arm in Group::Rust.arms() {
            assert!(!args(*arm).contains("hunter2"), "{arm:?}");
        }
    }

    #[test]
    fn tells_the_librdkafka_clients_how_to_read() {
        let base = "WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 MAX_POLL_RECORDS=4500";
        let poll = format!("{base} LIBRDKAFKA_BATCH=0");
        let batch = format!("{base} LIBRDKAFKA_BATCH=4000");
        let args = |scenario: &str, arm: Arm| consumer_args_for(scenario, arm).join(" ");
        // The C client: one record per poll, or its own batch in place of
        // MAX_POLL_RECORDS.
        let c_poll = args(&poll, Arm::Librdkafka);
        assert!(c_poll.ends_with("--no-produce --single-poll"), "{c_poll}");
        assert!(!c_poll.contains("--max-poll-records"), "{c_poll}");
        let c_batch = args(&batch, Arm::Librdkafka);
        assert!(c_batch.contains("--max-poll-records 4000 "), "{c_batch}");
        assert!(!c_batch.contains("4500") && !c_batch.contains("--single-poll"), "{c_batch}");
        assert!(args(base, Arm::Librdkafka).contains("--max-poll-records 4500 "));
        // Rust and Java keep MAX_POLL_RECORDS.
        for arm in [Arm::Rust, Arm::Java] {
            for scenario in [&poll, &batch] {
                let a = args(scenario, arm);
                assert!(
                    a.contains("--max-poll-records 4500 ") && !a.contains("--single-poll"),
                    "{arm:?}: {a}"
                );
            }
        }
        // Python: the librdkafka backend through POLL_SINGLE or its batch,
        // the Rust backend unchanged; no harness reads LIBRDKAFKA_BATCH.
        let env = |scenario: &str, arm: Arm| -> Vec<(String, String)> {
            parse_scenario(scenario, Group::Python).unwrap().env_for(arm)
        };
        let get = |env: &[(String, String)], key: &str| -> Vec<String> {
            env.iter().filter(|(k, _)| k == key).map(|(_, v)| v.clone()).collect()
        };
        for arm in [Arm::PythonSyncLibrdkafka, Arm::PythonAsyncLibrdkafka] {
            let polled = env(&poll, arm);
            assert_eq!(get(&polled, "POLL_SINGLE"), ["True"], "{arm:?}");
            let batched = env(&batch, arm);
            assert_eq!(get(&batched, "MAX_POLL_RECORDS"), ["4000"], "{arm:?}");
            assert!(get(&batched, "POLL_SINGLE").is_empty(), "{arm:?}");
            assert_eq!(get(&env(base, arm), "MAX_POLL_RECORDS"), ["4500"], "{arm:?}");
        }
        for arm in [Arm::PythonSyncRust, Arm::PythonAsyncRust] {
            for scenario in [&poll, &batch] {
                let e = env(scenario, arm);
                assert_eq!(get(&e, "MAX_POLL_RECORDS"), ["4500"], "{arm:?}");
                assert!(get(&e, "POLL_SINGLE").is_empty(), "{arm:?}");
            }
        }
        for arm in Group::Python.arms().iter().chain(Group::Rust.arms()) {
            for scenario in [&poll, &batch] {
                assert!(get(&env(scenario, *arm), "LIBRDKAFKA_BATCH").is_empty(), "{arm:?}");
            }
        }
        // The C harness parses the switch.
        let c = fs::read_to_string(repo_root().join("rust/consumer-perf/compare/librdkafka_e2e.c")).unwrap();
        assert!(c.contains("\"--single-poll\""));
    }

    /// Each option the driver gives a consumer harness is one the harness
    /// parses.
    #[test]
    fn consumer_harnesses_parse_what_they_are_given() {
        let scenario = "WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=1 FETCH_MIN_BYTES=1 FETCH_MAX_WAIT_MS=1 \
                        MAX_PARTITION_FETCH_BYTES=1 MAX_POLL_RECORDS=1";
        let repo = repo_root();
        for (arm, source) in [
            (Arm::Rust, "rust/consumer-perf/src/main.rs"),
            (Arm::Librdkafka, "rust/consumer-perf/compare/librdkafka_e2e.c"),
            (Arm::Java, "rust/consumer-perf/compare/JavaE2E.java"),
        ] {
            let text = fs::read_to_string(repo.join(source)).unwrap();
            let args = consumer_args_for(scenario, arm);
            let flags: Vec<&String> = args.iter().filter(|a| a.starts_with("--")).collect();
            // Every consumer setting is passed.
            assert_eq!(
                flags.iter().filter(|f| f.contains("fetch") || f.contains("poll")).count(),
                4,
                "{args:?}"
            );
            for flag in flags {
                assert!(text.contains(&format!("\"{flag}\"")), "{source} does not parse {flag}");
            }
        }
    }

    #[test]
    fn runs_the_load_of_a_consumer_test() {
        // A signal another test raises would read as an interrupt here.
        let _signals = crate::signals::test_lock();
        let dir = std::env::temp_dir().join(format!("perf-cloud-load-{}", std::process::id()));
        let kafka_bin = dir.join("bin");
        fs::create_dir_all(&kafka_bin).unwrap();
        let script = kafka_bin.join("kafka-producer-perf-test.sh");
        let mut options = options(Group::Rust, 0, 30);
        options.bootstrap = "broker:9092".to_string();
        let credentials = Credentials { key: "KEY".to_string(), secret: "hunter2".to_string() };
        // Run the script as the generator until `ready`, then stop it.
        let run = |scenario: &str, body: &str, ready: &dyn Fn(&mut LoadGenerator, &str) -> bool| {
            fs::write(&script, format!("#!/bin/sh\n{body}\n")).unwrap();
            fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
            let test = parse_scenario(scenario, Group::Rust).unwrap();
            let ctx = ArmContext {
                repo: Path::new("/repo"),
                options: &options,
                test: &test,
                arm: Arm::Rust,
                dir: dir.clone(),
                topic: "perf-t".to_string(),
                source_topic: None,
                topics_cli: Path::new("/kafka/bin/kafka-topics.sh"),
                kafka_bin: &kafka_bin,
                client_properties: Path::new("/private/client.properties"),
                credentials: &credentials,
                redactions: Arc::new(vec!["hunter2".to_string()]),
                signals_baseline: crate::signals::count(),
            };
            let log_path = dir.join("load.log");
            let _ = fs::remove_file(&log_path);
            let log = ArmLog::create(&log_path, Arc::clone(&ctx.redactions), ctx.signals_baseline).unwrap();
            let mut load = LoadGenerator::start(&ctx, 90, &log, log_path.clone()).unwrap();
            // A new script can take a while to start (macOS checks it first).
            let deadline = Instant::now() + Duration::from_secs(20);
            while !ready(&mut load, &fs::read_to_string(&log_path).unwrap()) {
                assert!(Instant::now() < deadline, "the script did not get ready");
                std::thread::sleep(Duration::from_millis(20));
            }
            let stopped = load.stop().unwrap();
            drop(log);
            (stopped, fs::read_to_string(&log_path).unwrap())
        };

        // Still running at the end: stopped, and not a failure; its rate is
        // read from the windows it printed.
        let ((stopped, rate), logged) = run(
            "WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000 COMPRESSION_TYPE=lz4",
            "echo \"args: $*\"; echo hunter2; \
             echo '50000 records sent, 10000.0 records/sec (9.77 MB/sec), 3.2 ms avg latency, 192.0 ms max latency.'; \
             echo '40000 records sent, 8000.0 records/sec (7.81 MB/sec), 3.0 ms avg latency, 90.0 ms max latency.'; \
             echo started; exec sleep 60",
            &|_, logged| logged.contains("started"),
        );
        assert_eq!(stopped, None);
        assert_eq!(rate, Some(9000.0));
        assert!(
            logged.contains(
                "args: --topic perf-t --num-records 900000 --record-size 1024 --throughput 10000 \
                 --producer.config /private/client.properties --producer-props acks=1 compression.type=lz4"
            ),
            "{logged}"
        );
        assert!(!logged.contains("hunter2"), "{logged}");

        // Ended before the harness: part of the run had no load.
        let ((stopped, rate), _) = run(
            "WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=1",
            "exit 3",
            &|load, _| load.has_stopped().unwrap(),
        );
        assert_eq!(rate, None);
        let reason = stopped.unwrap();
        assert!(
            reason.starts_with("the load generator (kafka-producer-perf-test.sh) stopped with ")
                && reason.contains('3')
                && reason.ends_with(" before the harness; see load.log"),
            "{reason}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reads_the_rate_of_the_load() {
        let window = |n: u64, rate: f64| {
            format!(
                "{n} records sent, {rate:.1} records/sec (9.77 MB/sec), 3.2 ms avg latency, 192.0 ms max latency.\n"
            )
        };
        // Windows, weighted by their records: 150,000 records in 25 s.
        let text = format!(
            "==> started\n{}WARN something\n{}",
            window(50_000, 10_000.0),
            window(100_000, 5_000.0)
        );
        assert_eq!(load_rate(&text), Some(6_000.0));
        // The final summary, when there is one, covers the whole run.
        let summary = "300000 records sent, 9876.5 records/sec (9.65 MB/sec), 3.10 ms avg latency, 200.00 ms max \
                       latency, 3 ms 50th, 5 ms 95th, 9 ms 99th, 40 ms 99.9th.";
        assert_eq!(load_rate(&format!("{}{summary}\n", window(50_000, 10_000.0))), Some(9876.5));
        // Nothing to read.
        assert_eq!(load_rate("==> started\nError: no such topic\n"), None);
        assert_eq!(load_rate("0 records sent, 0.0 records/sec (0.00 MB/sec)\n"), None);
    }

    #[test]
    fn a_watched_command_stops_when_asked() {
        let _signals = crate::signals::test_lock();
        let dir = std::env::temp_dir().join(format!("perf-cloud-watch-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let log = ArmLog::create(&dir.join("run.log"), Arc::new(Vec::new()), crate::signals::count()).unwrap();
        let started = Instant::now();
        let mut asked = 0;
        let exit = run_watched(Command::new("sleep").arg("60"), &log, Duration::from_secs(120), &mut || {
            asked += 1;
            Ok(asked >= 3)
        })
        .unwrap();
        assert!(matches!(exit, Exit::Stopped));
        assert!(started.elapsed() < Duration::from_secs(30), "{:?}", started.elapsed());
        // Never asked to stop: it runs to its end.
        let exit = run_watched(&mut Command::new("true"), &log, Duration::from_secs(60), &mut || Ok(false)).unwrap();
        assert!(matches!(exit, Exit::Status(s) if s.success()));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn strips_the_library_paths_cargo_adds() {
        let dirs = [
            PathBuf::from("/w/rust/target"),
            PathBuf::from("/home/u/.rustup/toolchains"),
        ];
        let stripped = |value: &str| strip_paths(OsStr::new(value), &dirs).map(|v| v.to_string_lossy().into_owned());
        assert_eq!(
            stripped(
                "/w/rust/target/debug/deps:/w/rust/target/debug:/home/u/.rustup/toolchains/1.95-x86_64/lib:\
                 /usr/lib/rustlib/x86_64-unknown-linux-gnu/lib/rustlib/x86_64/lib:/opt/lib::/usr/local/lib"
            ),
            Some("/opt/lib:/usr/local/lib".to_string())
        );
        // A sibling directory with the same prefix is kept.
        assert_eq!(stripped("/w/rust/target2/lib"), Some("/w/rust/target2/lib".to_string()));
        assert_eq!(stripped("/w/rust/target/release"), None);
    }

    #[test]
    fn every_setting_is_cleared_before_an_arm_starts() {
        for setting in SETTINGS {
            assert!(HARNESS_VARS.contains(&setting.key), "{} is not in HARNESS_VARS", setting.key);
        }
    }

    /// The names `source` reads from the environment: the string literal
    /// right after a read (`getenv("X")`, `env_or("X", ..)`, `envInt("X", ..)`,
    /// `os.environ.get("X")`, `os.environ["X"]`, ...) or right before
    /// `in os.environ`.
    fn env_reads(source: &str) -> Vec<String> {
        const READS: &[&str] = &[
            "getenv(",
            "env_or(",
            "env_opt(",
            "env_parse(",
            "env::var(",
            "envOr(",
            "envInt(",
            "envLong(",
            "envDouble(",
            "envBool(",
            "_env_int(",
            "os.environ.get(",
            "os.environ[",
        ];
        let name = |quoted: &str| -> Option<String> {
            let quote = quoted.chars().next().filter(|c| matches!(c, '"' | '\''))?;
            let rest = &quoted[1..];
            let name = &rest[..rest.find(quote)?];
            let valid =
                name.len() > 1 && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
            valid.then(|| name.to_string())
        };
        let mut names = Vec::new();
        for read in READS {
            for (i, _) in source.match_indices(read) {
                names.extend(name(source[i + read.len()..].trim_start()));
            }
        }
        for (i, _) in source.match_indices(" in os.environ") {
            let before = source[..i].trim_end();
            if let Some(quote) = before.chars().last().filter(|c| matches!(c, '"' | '\'')) {
                if let Some(start) = before[..before.len() - 1].rfind(quote) {
                    names.extend(name(&before[start..]));
                }
            }
        }
        names
    }

    /// No variable in the job's environment reaches one arm and not the
    /// others: whatever a harness reads, the driver clears.
    #[test]
    fn harness_vars_cover_what_the_harnesses_read() {
        assert_eq!(
            env_reads(
                "getenv(\"A_B\") env_or(\"SASL_MECHANISM\", \"PLAIN\") os.environ.get(\n \"CC\")\n'DD' in os.environ \
                 getenv(name) x"
            ),
            ["A_B", "SASL_MECHANISM", "CC", "DD"]
        );
        let repo = repo_root();
        let mut sources = vec![
            "rust/tests/performance/producer_perf_test.rs",
            "rust/tests/performance/transactional_producer_perf_test.rs",
            "c/tests/producer_perf_test.c",
            "c/tests/transactional_producer_perf_test.c",
            "python/test/performance/producer_performance_test.py",
            "python/test/performance/transactional_producer_performance_test.py",
            // The other consumer harnesses take options, not variables (the C
            // one reads only the SASL settings and HOME).
            "python/test/performance/consumer_performance_test.py",
            "python/test/performance/performance_common.py",
            "python/test/performance/librdkafka_helpers.py",
        ]
        .into_iter()
        .map(|p| repo.join(p))
        .collect::<Vec<_>>();
        let java = repo.join("tools/java-perf-test/src/main/java/io/confluent/kafka/perftest");
        for entry in fs::read_dir(&java).unwrap() {
            sources.push(entry.unwrap().path());
        }
        let mut read = 0;
        for path in &sources {
            let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            for name in env_reads(&text) {
                read += 1;
                assert!(
                    HARNESS_VARS.contains(&name.as_str()),
                    "{} reads {name}, which is not in HARNESS_VARS",
                    path.display()
                );
            }
        }
        // The scan found the reads at all.
        assert!(read > 100, "only {read} reads found");
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
        assert_eq!(source_topic_name("perf-abc-eos-x-rust"), "perf-abc-eos-x-rust-source");
        validate_topic_prefix("perf-0d0c9b9e-1f7a-4d0b-9c39-ff4b6a0f1d2e").unwrap();
        assert!(validate_topic_prefix("perf/x").is_err());
        assert!(validate_topic_prefix("").is_err());
        // The longest name stays well inside Kafka's 249 characters.
        let longest = source_topic_name(&format!(
            "{}-{}-{}",
            "p".repeat(100),
            "t".repeat(MAX_TEST_ID_LEN),
            Arm::PythonAsyncLibrdkafka.id()
        ));
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
        assert_eq!(m.txn, None);
        let err = Metrics::from_results(r#"{"client": "rust", "throughput_msg_s": 1}"#)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "no `latency_ms`");

        let txn = r#"{"test": "producer", "client": "rust-txn", "messages_measured": 600000,
            "throughput_msg_s": 10000.00, "throughput_mib_s": 9.77,
            "committed_transactions": 6000, "aborted_transactions": 0, "aborted_records": 0,
            "transactions_per_s": 100.00,
            "latency_ms": {"min": 1, "avg": 3.25, "p50": 3, "p90": 4, "p95": 5, "p99": 8, "p999": 12, "max": 40},
            "commit_latency_ms": {"min": 1, "avg": 12.5, "p50": 11, "p90": 14, "p95": 15, "p99": 21, "p999": 30,
                                  "max": 40},
            "cpu_avg_pct": 21.50, "rss_avg_kib": 51200.00}"#;
        let m = Metrics::from_results(txn).unwrap();
        assert_eq!(
            m.txn,
            Some(TxnMetrics {
                per_s: 100.0,
                committed: 6000.0,
                aborted: 0.0,
                commit_p50: 11.0,
                commit_p99: 21.0
            })
        );
        let err = Metrics::from_results(&txn.replace("\"commit_latency_ms\"", "\"x\""))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "no `commit_latency_ms`");

        // As the C consumer harness writes it.
        let consumer = r#"{
  "test": "consumer",
  "client": "librdkafka-consumer",
  "topic": "perf-e2e-c-68117",
  "messages_measured": 40006,
  "duration_s": 20.00,
  "throughput_msg_s": 2000.14,
  "throughput_mib_s": 1.95,
  "latency_ms": {"min": 0, "avg": 3.95, "p50": 4, "p90": 7, "p95": 7, "p99": 8, "p999": 18, "max": 23},
  "cpu_avg_pct": 1.45,
  "rss_avg_kib": 16442.67
}"#;
        let m = Metrics::from_results(consumer).unwrap();
        assert_eq!(m.client, "librdkafka-consumer");
        assert_eq!(m.msg_s, 2000.14);
        assert_eq!(m.p99, 8.0);
        assert_eq!(m.txn, None);
    }

    #[test]
    fn masks_the_secret_and_writes_the_client_properties() {
        let redactions = vec!["s3cr3t".to_string()];
        assert_eq!(
            redact("password=\"s3cr3t\" again s3cr3t", &redactions),
            "password=\"<redacted>\" again <redacted>"
        );
        assert_eq!(redact("nothing here", &[String::new()]), "nothing here");
        let text = client_properties_text("b:9092", &Credentials { key: "K".into(), secret: "a+b/c=".into() });
        assert!(text.contains(r#"username="K" password="a+b/c=";"#), "{text}");
        assert!(
            text.starts_with("bootstrap.servers=b:9092\nsecurity.protocol=SASL_SSL\n"),
            "{text}"
        );
    }

    #[test]
    fn refuses_credentials_the_harnesses_cannot_read_back() {
        check_credential("PERF_API_SECRET", "Ab+/9=").unwrap();
        for bad in ["a\"b", "a\\b", "a\nb"] {
            let e = check_credential("PERF_API_SECRET", bad).unwrap_err().to_string();
            assert_eq!(
                e,
                "PERF_API_SECRET contains a `\"`, a `\\` or a control character, which the harnesses cannot read back"
            );
            assert!(!e.contains(bad));
        }
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

    #[test]
    fn reads_the_broker_racks() {
        let output = "\
b0-pkc.example:9092 (id: 0 rack: usw2-az4 isFenced: false) -> (
\tProduce(0): 3 to 12 [usable: 12],
)
b1-pkc.example:9092 (id: 1 rack: usw2-az1 isFenced: false) -> (
)
b2-pkc.example:9092 (id: 2 rack: usw2-az4 isFenced: false) -> (
)
b3-pkc.example:9092 (id: 3 rack: usw2-az2) -> (
)
";
        assert_eq!(broker_racks(output), strings(&["usw2-az1", "usw2-az2", "usw2-az4"]));
        assert!(broker_racks("localhost:9092 (id: 1 rack: null isFenced: false) -> (\n)\n").is_empty());
        assert!(broker_racks("").is_empty());

        let mut facts = vec![
            ("Agent AZ ID".to_string(), "usw2-az2".to_string()),
            ("Cluster size".to_string(), "10 CKU".to_string()),
        ];
        add_cluster_zones(&mut facts, &[]);
        assert_eq!(facts.len(), 2);
        add_cluster_zones(&mut facts, &strings(&["usw2-az1", "usw2-az2"]));
        assert_eq!(facts[1], ("Cluster AZ IDs".to_string(), "usw2-az1, usw2-az2".to_string()));
        let mut facts = vec![("Cluster size".to_string(), "10 CKU".to_string())];
        add_cluster_zones(&mut facts, &strings(&["usw2-az1"]));
        assert_eq!(facts[0].0, "Cluster AZ IDs");
    }

    #[test]
    fn reads_the_cluster_zones_with_the_kafka_tool() {
        // A signal another test raises would stop the tool here.
        let _signals = crate::signals::test_lock();
        let dir = std::env::temp_dir().join(format!("perf-cloud-zones-{}", std::process::id()));
        let kafka_bin = dir.join("bin");
        fs::create_dir_all(&kafka_bin).unwrap();
        let script = kafka_bin.join("kafka-broker-api-versions.sh");
        let zones = |body: &str| {
            fs::write(&script, format!("#!/bin/sh\n{body}\n")).unwrap();
            fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
            cluster_zones(
                &kafka_bin,
                "broker:9092",
                Path::new("/private/client.properties"),
                &dir,
                crate::signals::count(),
            )
        };
        // The tool gets the bootstrap and the client properties.
        let read = zones(
            r#"[ "$*" = "--bootstrap-server broker:9092 --command-config /private/client.properties" ] || exit 9
echo 'b0:9092 (id: 0 rack: usw2-az2 isFenced: false) -> ('
echo 'b1:9092 (id: 1 rack: usw2-az1 isFenced: false) -> ('"#,
        );
        assert_eq!(read, Some(strings(&["usw2-az1", "usw2-az2"])));
        // Read, without racks: nothing to try again.
        assert_eq!(zones("echo 'b0:9092 (id: 0 rack: null) -> ('"), Some(vec![]));
        // Not read (e.g. the API key is not accepted yet): try again later.
        assert_eq!(zones("echo 'b0:9092 (id: 0 rack: usw2-az2) -> ('; exit 1"), None);
        // No tool: not read either, and not an error that stops the run.
        fs::remove_file(&script).unwrap();
        let missing = cluster_zones(&kafka_bin, "b:9092", Path::new("/p"), &dir, crate::signals::count());
        assert_eq!(missing, None);
        let _ = fs::remove_dir_all(&dir);
    }
}
