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

//! `cargo xtask chaos-matrix`: run every scenario of a matrix file across a
//! set of security protocols and message sizes, one run at a time.
//!
//! Each scenario line of the matrix carries the chaos flags that define it
//! (`cargo xtask chaos` flags); the runner appends the per-run dimensions
//! (`--security-protocol`, `--msg-size`, `--rps`, `--reports`) and runs the
//! `chaos_run` test with the resulting `CHAOS_*` environment, exactly as
//! `cargo xtask chaos` does. The test binaries are built once, up front, and
//! executed directly, so a source edit made while the matrix runs does not
//! reach its later runs. Runs are strictly sequential: every run owns a
//! dedicated Docker cluster, and two at once would compete for the host; for
//! the same reason one output directory takes one runner at a time
//! (`runner.lock`).
//!
//! Everything lands under the output directory (default
//! `target/chaos-matrix/<matrix name>/`):
//!
//! - `runs/<run id>/run.log` — the run's complete stdout + stderr.
//! - `runs/<run id>/command.txt` — the equivalent `cargo xtask chaos` command.
//! - `runs/<run id>/reports/` — the harness's report directory (verdict,
//!   leader changes, per-client logs, signature summary), moved here.
//! - `results.tsv` — one line per finished run (machine-readable), with the
//!   chaos flags it ran with.
//! - `summary.md` — the human-readable summary, rewritten after every run.
//! - `plan.txt` — every run of the matrix, in order, with its chaos flags.
//! - `matrix.log` — timestamped progress.
//!
//! A re-invocation with the same output directory resumes: runs already in
//! `results.tsv` with the same chaos flags are skipped (`--rerun-failed`
//! re-runs the ones that did not pass, keeping the previous attempt's
//! directory); a run recorded with other flags — its scenario line or `--rps`
//! changed — runs again. A narrowed invocation (`--only`, fewer `--protocols`
//! or `--msg-sizes`) only selects what to run: the summary, `plan.txt` and the
//! exit status cover the whole matrix the directory holds.
//!
//! Ctrl-C, SIGTERM or SIGHUP stops the current run the way a timeout does
//! (SIGINT to it, so it tears its cluster down; SIGKILL after a grace period
//! or on a second signal), leaves it unrecorded and ends the matrix.
//!
//! Matrix file format: one scenario per line, `ID | description | flags`;
//! blank lines and lines starting with `#` are ignored.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read as _, Write as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context as _};

pub mod status;

/// libtest arguments selecting the flag-driven runner. The test binary is
/// executed directly (see [`build_test_binaries`]).
const RUNNER_ARGS: &[&str] = &["--ignored", "--nocapture", "--exact", "run_test::chaos_run"];

/// Flags the runner sets for every run; a scenario line must not set them.
const DIMENSION_FLAGS: &[&str] = &["--security-protocol", "--msg-size", "--rps", "--reports"];

/// Security protocols the harness accepts for `--security-protocol`.
const PROTOCOLS: &[&str] = &["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"];

/// The file, in the output directory, that only one runner holds at a time.
const LOCK_FILE: &str = "runner.lock";
/// The file, in the output directory, listing every run of the matrix.
const PLAN_FILE: &str = "plan.txt";

/// How long a timed-out run gets to tear its cluster down after SIGINT
/// before its process group is killed.
const INTERRUPT_GRACE: Duration = Duration::from_secs(180);
/// Grace after SIGINT for a run that stopped making progress. A wedged test
/// process usually no longer polls its Ctrl-C handler either, so waiting the
/// full interrupt grace would only delay the next run.
const STALL_GRACE: Duration = Duration::from_secs(60);
/// How much longer than its longest silent phase a run may go without output
/// before it counts as stalled (when that exceeds `--stall-min`).
const STALL_MARGIN: Duration = Duration::from_secs(5 * 60);
/// The harness phases that print nothing for as long as their flag says.
/// A flag not given leaves its phase at the harness default, seconds to a
/// minute, which `--stall-min` always covers.
const SILENT_PHASES: &[&str] = &[
    "CHAOS_WARMUP_S",
    "CHAOS_DRAIN_S",
    "CHAOS_BETWEEN_S",
    "CHAOS_UP_WAIT_S",
    "CHAOS_OUTAGE_S",
    "CHAOS_DWELL_S",
    "CHAOS_STOP_S",
];
/// How long to sample a stalled test process's stacks before it is signalled,
/// so a wedge leaves evidence of where it was stuck.
const STALL_SAMPLE_SECS: &str = "5";
/// The longest a `docker` command may take; Docker Desktop can hang.
const DOCKER_TIMEOUT: Duration = Duration::from_secs(60);
/// The harness's notice when it had to force-exit a process that passed but
/// did not terminate after teardown; libtest's summary never prints then.
pub(crate) const FORCED_EXIT_AFTER_PASS: &str = "chaos: FORCED EXIT after a PASS verdict";
/// The harness's line naming its cluster's Docker network,
/// `kafka-net-<suffix>`; the cluster's brokers are `kafka-<n>-<suffix>`.
const CLUSTER_NETWORK_LINE: &str = "chaos: cluster network ";

/// The outcome of a run that failed with only the known recreate defect on
/// every attempt.
const KNOWN_DEFECT: &str = "KNOWN-DEFECT";

/// Verdict lines copied into the results, as (column, verdict line prefix).
const METRICS: &[(&str, &str)] = &[
    ("delivered", "delivered (acked) records"),
    ("lost", "lost (delivered, unseen)"),
    ("failed_sends", "failed sends"),
    ("dup_by_index", "duplicates (by index)"),
    ("double_writes", "duplicates (double write)"),
    ("in_flight_peak", "in-flight peak (producer)"),
    ("rebalance", "rebalance callbacks"),
    ("commit_checks", "committed offsets checked"),
    ("ordering", "ordering violations"),
    ("commit_errors", "consumer commit errors"),
];

/// Result columns, in `results.tsv` order. Columns were only ever appended,
/// so a row written before the trailing ones existed still reads.
const COLUMNS: &[&str] = &[
    "run_id",
    "scenario",
    "protocol",
    "msg_size",
    "outcome",
    "started",
    "duration_s",
    "produce_rate",
    "produce_mib_s",
    "delivered",
    "lost",
    "failed_sends",
    "dup_by_index",
    "double_writes",
    "in_flight_peak",
    "rebalance",
    "commit_checks",
    "ordering",
    "commit_errors",
    "reasons",
    "dir",
    "attempts",
    "chaos_args",
];

/// How many trailing columns a row may lack: `attempts` and `chaos_args`.
const OPTIONAL_TRAILING_COLUMNS: usize = 2;

#[derive(Debug, Clone)]
struct Options {
    matrix: PathBuf,
    out: PathBuf,
    protocols: Vec<String>,
    msg_sizes: Vec<usize>,
    rps: u32,
    only: Option<BTreeSet<String>>,
    run_timeout: Duration,
    rerun_failed: bool,
    /// Run ids to run again whatever their recorded outcome (`--rerun`).
    rerun: BTreeSet<String>,
    check_only: bool,
    /// A run whose log has not grown for this long is stopped as STALLED
    /// (or longer, for a run whose silent phases are longer).
    stall_timeout: Duration,
    /// Attempts at a run that keeps failing with the known recreate defect.
    known_defect_attempts: u32,
}

/// One scenario line of the matrix file.
#[derive(Debug, Clone, PartialEq)]
struct Scenario {
    id: String,
    description: String,
    args: Vec<String>,
    /// The optional fourth column: the only protocols this scenario runs
    /// with. `None` runs it with every `--protocols` value.
    protocols: Option<Vec<String>>,
}

impl Scenario {
    fn runs_with(&self, protocol: &str) -> bool {
        self.protocols.as_ref().is_none_or(|only| only.iter().any(|p| p == protocol))
    }
}

/// One planned run: a scenario at one protocol and message size.
#[derive(Debug, Clone)]
struct Planned {
    scenario: Scenario,
    protocol: String,
    msg_size: usize,
    rps: u32,
}

impl Planned {
    fn run_id(&self) -> String {
        format!("{}-{}-{}", self.scenario.id, self.protocol, size_label(self.msg_size))
    }

    /// The scenario's flags plus the per-run dimensions.
    fn chaos_args(&self) -> Vec<String> {
        let mut args = self.scenario.args.clone();
        args.extend([
            "--security-protocol".to_string(),
            self.protocol.clone(),
            "--msg-size".to_string(),
            self.msg_size.to_string(),
            "--rps".to_string(),
            self.rps.to_string(),
            "--reports".to_string(),
        ]);
        args
    }
}

/// `100` -> `100B`, `1048576` -> `1MiB`.
fn size_label(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{}MiB", bytes / MIB)
    } else {
        format!("{bytes}B")
    }
}

/// Inverse of `size_label`: `100B` -> 100, `1MiB` -> 1048576.
fn parse_size_label(label: &str) -> Option<usize> {
    if let Some(mib) = label.strip_suffix("MiB") {
        return mib.parse::<usize>().ok()?.checked_mul(1024 * 1024);
    }
    label.strip_suffix('B')?.parse().ok()
}

/// The scenario id of a run id `<scenario>-<protocol>-<size>`. Scenario ids
/// may contain dashes; protocols and size labels never do.
fn scenario_of(run_id: &str) -> &str {
    run_id.rsplitn(3, '-').nth(2).unwrap_or("")
}

/// The protocol and message size of a run id `<scenario>-<protocol>-<size>`.
fn run_dimensions(run_id: &str) -> Option<(&str, usize)> {
    let mut parts = run_id.rsplitn(3, '-');
    let size = parse_size_label(parts.next()?)?;
    let protocol = parts.next()?;
    parts.next()?;
    Some((protocol, size))
}

/// Entry point for `cargo xtask chaos-matrix`.
pub fn run(raw: &[String]) -> anyhow::Result<()> {
    let options = parse_options(raw)?;
    let text = fs::read_to_string(&options.matrix)
        .with_context(|| format!("reading matrix file {}", options.matrix.display()))?;
    let scenarios = parse_matrix(&text)?;
    // What this invocation runs; the summary covers the whole matrix.
    let plan = plan_runs(&scenarios, &options)?;
    for warning in runless_scenarios(&scenarios, &options) {
        println!("chaos-matrix: WARNING: {warning}");
    }
    if plan.is_empty() {
        bail!(
            "nothing to run: no scenario{} runs with --protocols {}",
            if options.only.is_some() { " named by --only" } else { "" },
            options.protocols.join(",")
        );
    }
    let unknown: Vec<&String> = options
        .rerun
        .iter()
        .filter(|id| !plan.iter().any(|p| &p.run_id() == *id))
        .collect();
    if !unknown.is_empty() {
        bail!("--rerun names run id(s) that are not in this matrix's plan: {unknown:?}");
    }
    // Every run's flags must parse before anything starts, so a typo in line
    // 20 does not surface hours into the matrix.
    for planned in &plan {
        crate::parse_chaos_flags(&planned.chaos_args())
            .with_context(|| format!("scenario {} has invalid flags", planned.scenario.id))?;
    }

    // Build once up front: a compile error should stop the matrix, not be
    // recorded as a failure of every run, and build time should not count
    // towards the first run's duration.
    let binaries = build_test_binaries(&plan)?;
    println!("chaos-matrix: checking the configuration of all {} run(s)", plan.len());
    check_configurations(&plan, &binaries)?;
    if options.check_only {
        println!("chaos-matrix: all {} run configuration(s) accepted", plan.len());
        return Ok(());
    }

    fs::create_dir_all(options.out.join("runs"))?;
    let _lock = RunnerLock::acquire(&options.out)?;
    refuse_if_previous_run_alive(&options.out)?;
    let results_path = options.out.join("results.tsv");
    let mut results = load_results(&results_path)?;
    let previous_plan: Vec<String> = read_plan(&options.out).into_iter().map(|(id, _)| id).collect();
    let (all_protocols, all_sizes, full) = full_plan(&scenarios, &options, &previous_plan, &results)?;

    fs::copy(&options.matrix, options.out.join("matrix.txt"))?;
    // Lets `chaos-matrix-status` tell whether a runner is working on this
    // output directory, whatever order its flags were given in.
    write_pid_file(&options.out, None)?;
    // The whole matrix in run order, for `chaos-matrix-status`, which can
    // neither rebuild it nor tell a run recorded with other flags.
    let plan_lines: String = full
        .iter()
        .map(|p| format!("{}\t{}\n", p.run_id(), p.chaos_args().join(" ")))
        .collect();
    fs::write(options.out.join(PLAN_FILE), plan_lines)?;
    let mut log = MatrixLog::open(&options.out.join("matrix.log"))?;
    // `touch <out>/STOP` ends the matrix cleanly between runs. One still here
    // was meant for a session that has ended; honouring it would end this one
    // before it starts.
    let stop_file = options.out.join("STOP");
    if stop_file.exists() {
        let _ = fs::remove_file(&stop_file);
        log.line("removed a STOP file left over from an earlier session");
    }
    let environment = describe_environment(&options);
    // Appended, not overwritten: a resumed matrix records each session's
    // commit and host state, since runs may span both.
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(options.out.join("environment.txt"))?
        .write_all(format!("{environment}---\n").as_bytes())?;
    log.line(&format!(
        "matrix {} — {} scenario(s), up to {} protocol(s), {} size(s): {} run(s) of {} in the matrix; {} already recorded",
        options.matrix.display(),
        scenarios.len(),
        options.protocols.len(),
        options.msg_sizes.len(),
        plan.len(),
        full.len(),
        plan.iter().filter(|p| recorded(&results, p).is_some()).count()
    ));
    let changed: Vec<String> = plan
        .iter()
        .filter(|p| results.contains_key(&p.run_id()) && recorded(&results, p).is_none())
        .map(Planned::run_id)
        .collect();
    if !changed.is_empty() {
        log.line(&format!(
            "{} recorded run(s) ran with other chaos flags (their scenario line or --rps changed) and run again: {}",
            changed.len(),
            changed.join(", ")
        ));
    }
    let summary = |results: &BTreeMap<String, RunResult>| {
        render_summary(&options, &environment, &scenarios, &all_protocols, &all_sizes, &full, results)
    };

    crate::signals::install();
    let total = plan.len();
    let mut stopped = false;
    for (n, planned) in plan.iter().enumerate() {
        let run_id = planned.run_id();
        if let Some(previous) = recorded(&results, planned) {
            let rerun_requested = options.rerun.contains(&run_id);
            if !rerun_requested && (previous.outcome == "PASS" || !options.rerun_failed) {
                continue;
            }
        }
        if crate::signals::count() > 0 {
            fs::write(options.out.join("summary.md"), summary(&results))?;
            log.line(&format!(
                "interrupted by {} before {run_id} started — re-run the same command to resume",
                crate::signals::last_name()
            ));
            bail!("chaos matrix interrupted by {}", crate::signals::last_name());
        }
        // Re-running the same command resumes from here.
        if stop_file.exists() {
            let _ = fs::remove_file(&stop_file);
            log.line(&format!(
                "stop requested; {run_id} and later runs not started — re-run to resume"
            ));
            stopped = true;
            break;
        }
        match harness_broker_containers() {
            Some(leftovers) if !leftovers.is_empty() => log.line(&format!(
                "WARNING: broker containers from an earlier cluster are still present ({}); they compete \
                 with this run for the host",
                leftovers.join(", ")
            )),
            Some(_) => {},
            None => log.line("WARNING: could not list Docker containers (`docker ps` failed or timed out)"),
        }
        log.line(&format!(
            "[{}/{total}] {run_id} — {} — starting",
            n + 1,
            planned.scenario.description
        ));
        // A failure that is exactly the known multi-topic recreate defect is
        // retried, keeping every attempt's directory: a later clean attempt
        // is a genuine pass, and a scenario that hits it on every attempt is
        // recorded as KNOWN-DEFECT rather than FAIL. Any other outcome is
        // final on the first attempt.
        let mut attempt = 1;
        let mut duration_s = 0;
        let finished = loop {
            let Some(mut result) = execute(planned, &options, &binaries, &mut log)? else {
                break None;
            };
            duration_s += result.duration_s;
            let known = result.columns.get("known_defect").cloned();
            match known {
                Some(defect) if result.outcome == "FAIL" => {
                    if attempt < options.known_defect_attempts {
                        let kept = unused_path(&options.out.join("runs").join(format!("{run_id}.attempt-{attempt}")));
                        fs::rename(options.out.join("runs").join(&run_id), &kept)?;
                        log.line(&format!(
                            "[{}/{total}] {run_id} — attempt {attempt}/{} hit the known recreate defect; retrying \
                             (logs kept in {})",
                            n + 1,
                            options.known_defect_attempts,
                            kept.display()
                        ));
                        attempt += 1;
                        continue;
                    }
                    result.outcome = KNOWN_DEFECT.to_string();
                    result.reasons = format!("known defect on all {attempt} attempt(s): {defect}");
                    break Some(result);
                },
                _ => break Some(result),
            }
        };
        let Some(mut result) = finished else {
            fs::write(options.out.join("summary.md"), summary(&results))?;
            log.line(&format!(
                "interrupted by {}: {run_id} was stopped and is not recorded — re-run the same command to resume",
                crate::signals::last_name()
            ));
            bail!("chaos matrix interrupted by {}", crate::signals::last_name());
        };
        // Every attempt's time: a retried run kept the host that long.
        result.duration_s = duration_s;
        result.columns.insert("attempts".to_string(), attempt.to_string());
        result.columns.insert("chaos_args".to_string(), planned.chaos_args().join(" "));
        log.line(&format!(
            "[{}/{total}] {run_id} — {} in {}s (delivered {}, lost {}, {} records/s){}",
            n + 1,
            result.outcome,
            result.duration_s,
            result.get("delivered"),
            result.get("lost"),
            result.get("produce_rate"),
            if result.reasons.is_empty() {
                String::new()
            } else {
                format!(" — {}", result.reasons)
            }
        ));
        results.insert(run_id, result);
        write_results(&results_path, &results)?;
        fs::write(options.out.join("summary.md"), summary(&results))?;
    }
    fs::write(options.out.join("summary.md"), summary(&results))?;
    let recorded_runs: Vec<(String, &RunResult)> = full
        .iter()
        .filter_map(|p| recorded(&results, p).map(|r| (p.run_id(), r)))
        .collect();
    if !stopped {
        // A STOP made during the last run had nothing left to stop; it must
        // not stop the next session instead.
        if stop_file.exists() {
            let _ = fs::remove_file(&stop_file);
            log.line("stop requested during the last run; nothing was left to stop");
        }
        let passed = recorded_runs.iter().filter(|(_, r)| r.outcome == "PASS").count();
        let known = recorded_runs.iter().filter(|(_, r)| r.outcome == KNOWN_DEFECT).count();
        log.line(&format!(
            "matrix finished: {passed}/{} passed, {known} known-defect; summary in {}",
            full.len(),
            options.out.join("summary.md").display()
        ));
    }
    let failed = failed_runs(&recorded_runs);
    if !failed.is_empty() {
        bail!(
            "chaos matrix: {} of {} run(s) did not pass: {}",
            failed.len(),
            full.len(),
            failed.join(", ")
        );
    }
    Ok(())
}

/// The recorded runs that fail the matrix: every outcome but PASS and
/// KNOWN-DEFECT. A KNOWN-DEFECT run failed on every attempt with exactly the
/// signature of the known, documented multi-topic recreate defect and nothing
/// else; failing the matrix for it would keep the matrix red until that defect
/// is fixed and hide any new failure behind it. It stays visible in the summary,
/// the log and `results.tsv`, never counted as a pass. Runs a STOP left unrun
/// are not failures either.
fn failed_runs(recorded: &[(String, &RunResult)]) -> Vec<String> {
    recorded
        .iter()
        .filter(|(_, r)| r.outcome != "PASS" && r.outcome != KNOWN_DEFECT)
        .map(|(id, _)| id.clone())
        .collect()
}

/// The recorded result of `planned`, unless it was recorded with other chaos
/// flags (its scenario line or `--rps` changed since): that result describes a
/// different run. A row written before `results.tsv` recorded the flags
/// cannot tell, and counts as this run's: re-running every earlier result
/// would cost hours for a change that most likely never happened.
fn recorded<'a>(results: &'a BTreeMap<String, RunResult>, planned: &Planned) -> Option<&'a RunResult> {
    results
        .get(&planned.run_id())
        .filter(|r| r.ran_with(&planned.chaos_args().join(" ")))
}

fn parse_options(raw: &[String]) -> anyhow::Result<Options> {
    let mut matrix = None;
    let mut out = None;
    let mut protocols = vec!["plaintext".to_string(), "ssl".to_string(), "sasl_ssl".to_string()];
    let mut msg_sizes = vec![100, 1024 * 1024];
    let mut rps = 1000;
    let mut only = None;
    let mut run_timeout = Duration::from_secs(240 * 60);
    let mut rerun_failed = false;
    let mut rerun = BTreeSet::new();
    let mut check_only = false;
    // Covers the harness's default silent phases with a wide margin; a run
    // whose flags make a silent phase longer gets more (`stall_threshold`).
    let mut stall_timeout = Duration::from_secs(20 * 60);
    let mut known_defect_attempts = 3;
    let mut i = 0;
    while i < raw.len() {
        let flag = raw[i].as_str();
        if flag == "--rerun-failed" || flag == "--check-only" {
            if flag == "--rerun-failed" {
                rerun_failed = true;
            } else {
                check_only = true;
            }
            i += 1;
            continue;
        }
        let value = raw.get(i + 1).with_context(|| format!("{flag} requires a value"))?.as_str();
        if value.starts_with("--") {
            bail!("{flag} requires a value, but got the flag {value}");
        }
        match flag {
            "--matrix" => matrix = Some(PathBuf::from(value)),
            "--out" => out = Some(PathBuf::from(value)),
            "--protocols" => protocols = dedup(split_list(value)),
            "--msg-sizes" => {
                msg_sizes = dedup(
                    split_list(value)
                        .iter()
                        .map(|s| s.parse().with_context(|| format!("--msg-sizes: '{s}' is not a byte count")))
                        .collect::<anyhow::Result<_>>()?,
                );
            },
            "--rps" => rps = value.parse().context("--rps must be a number")?,
            "--only" => only = Some(split_list(value).into_iter().collect()),
            "--rerun" => rerun = split_list(value).into_iter().collect(),
            "--run-timeout-min" => run_timeout = minutes(flag, value)?,
            "--stall-min" => stall_timeout = minutes(flag, value)?,
            "--known-defect-attempts" => {
                known_defect_attempts = value.parse().context("--known-defect-attempts must be a number")?;
                if known_defect_attempts == 0 {
                    bail!("--known-defect-attempts must be >= 1");
                }
            },
            other => bail!("unknown chaos-matrix flag: {other}"),
        }
        i += 2;
    }
    let matrix = matrix.context("--matrix FILE is required")?;
    for protocol in &protocols {
        if !PROTOCOLS.contains(&protocol.as_str()) {
            bail!("--protocols: '{protocol}' is not one of {}", PROTOCOLS.join(", "));
        }
    }
    if protocols.is_empty() || msg_sizes.is_empty() {
        bail!("--protocols and --msg-sizes must each name at least one value");
    }
    let out = out.unwrap_or_else(|| {
        let stem = matrix.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        PathBuf::from("target/chaos-matrix").join(stem)
    });
    Ok(Options {
        matrix,
        out,
        protocols,
        msg_sizes,
        rps,
        only,
        run_timeout,
        rerun_failed,
        rerun,
        check_only,
        stall_timeout,
        known_defect_attempts,
    })
}

/// A whole number of minutes, at least one, as a duration.
fn minutes(flag: &str, value: &str) -> anyhow::Result<Duration> {
    let minutes: u64 = value.parse().with_context(|| format!("{flag} must be a number"))?;
    if minutes == 0 {
        bail!("{flag} must be >= 1");
    }
    minutes
        .checked_mul(60)
        .map(Duration::from_secs)
        .with_context(|| format!("{flag} is too large"))
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// `values` without repeats, in first-seen order: `--protocols ssl,ssl`
/// means one ssl column, not every ssl run twice.
fn dedup<T: PartialEq>(values: Vec<T>) -> Vec<T> {
    let mut unique = Vec::with_capacity(values.len());
    for value in values {
        if !unique.contains(&value) {
            unique.push(value);
        }
    }
    unique
}

/// Parse the matrix file into its scenarios, rejecting duplicate ids and
/// scenario lines that set a per-run dimension themselves.
fn parse_matrix(text: &str) -> anyhow::Result<Vec<Scenario>> {
    let mut scenarios: Vec<Scenario> = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('|').map(str::trim).collect();
        let (id, description, flags, protocols) = match parts[..] {
            [id, description, flags] => (id, description, flags, None),
            [id, description, flags, protocols] => (id, description, flags, Some(protocols)),
            _ => bail!("matrix line {}: expected `ID | description | flags [| protocols]`", number + 1),
        };
        let protocols = match protocols {
            None => None,
            Some(list) => {
                let list = dedup(split_list(list));
                if list.is_empty() {
                    bail!("matrix line {}: the protocols column is empty", number + 1);
                }
                if let Some(bad) = list.iter().find(|p| !PROTOCOLS.contains(&p.as_str())) {
                    bail!(
                        "matrix line {}: protocol '{bad}' is not one of {}",
                        number + 1,
                        PROTOCOLS.join(", ")
                    );
                }
                Some(list)
            },
        };
        if id.is_empty() || id.contains(char::is_whitespace) {
            bail!("matrix line {}: scenario id '{id}' must be one non-empty word", number + 1);
        }
        if scenarios.iter().any(|s| s.id == id) {
            bail!("matrix line {}: duplicate scenario id '{id}'", number + 1);
        }
        let args: Vec<String> = flags.split_whitespace().map(str::to_string).collect();
        // `--msg-size=5` sets the dimension just as `--msg-size 5` does.
        if let Some(flag) = args
            .iter()
            .map(|a| a.split_once('=').map_or(a.as_str(), |(flag, _)| flag))
            .find(|flag| DIMENSION_FLAGS.contains(flag))
        {
            bail!(
                "matrix line {}: scenario '{id}' sets {flag}, which the matrix runner sets for every run",
                number + 1
            );
        }
        scenarios.push(Scenario { id: id.to_string(), description: description.to_string(), args, protocols });
    }
    if scenarios.is_empty() {
        bail!("the matrix file has no scenarios");
    }
    Ok(scenarios)
}

/// Every run, in execution order: message size, then protocol, then scenario.
/// So the whole default-size matrix finishes before the large-record one, and
/// each protocol's column completes as a block.
fn plan_runs(scenarios: &[Scenario], options: &Options) -> anyhow::Result<Vec<Planned>> {
    if let Some(only) = &options.only {
        let unknown: Vec<&String> = only.iter().filter(|id| !scenarios.iter().any(|s| &s.id == *id)).collect();
        if !unknown.is_empty() {
            bail!("--only names unknown scenario id(s): {unknown:?}");
        }
    }
    let mut plan = Vec::new();
    for &msg_size in &options.msg_sizes {
        for protocol in &options.protocols {
            for scenario in scenarios {
                if options.only.as_ref().is_some_and(|only| !only.contains(&scenario.id))
                    || !scenario.runs_with(protocol)
                {
                    continue;
                }
                plan.push(Planned {
                    scenario: scenario.clone(),
                    protocol: protocol.clone(),
                    msg_size,
                    rps: options.rps,
                });
            }
        }
    }
    Ok(plan)
}

/// A warning for each selected scenario that has no run, because its
/// protocols column names none of `--protocols`.
fn runless_scenarios(scenarios: &[Scenario], options: &Options) -> Vec<String> {
    scenarios
        .iter()
        .filter(|s| options.only.as_ref().is_none_or(|only| only.contains(&s.id)))
        .filter(|s| !options.protocols.iter().any(|p| s.runs_with(p)))
        .map(|s| {
            format!(
                "scenario {} has no runs: its protocols column ({}) names none of --protocols {}",
                s.id,
                s.protocols.as_deref().unwrap_or_default().join(","),
                options.protocols.join(",")
            )
        })
        .collect()
}

/// The whole matrix this output directory holds: every scenario of the
/// matrix file at every protocol and message size planned or recorded here
/// before, plus this invocation's, as (protocols, message sizes, runs). A
/// narrowed invocation (`--only`, fewer `--protocols` / `--msg-sizes`) only
/// selects what to run; the summary, `plan.txt` and the exit status still
/// cover all of it. Dimensions come in first-seen order, the earlier plan's
/// first; a scenario no longer in the matrix file contributes none.
fn full_plan(
    scenarios: &[Scenario],
    options: &Options,
    previous_plan: &[String],
    results: &BTreeMap<String, RunResult>,
) -> anyhow::Result<(Vec<String>, Vec<usize>, Vec<Planned>)> {
    let in_matrix = |id: &str| scenarios.iter().any(|s| s.id == scenario_of(id));
    let mut protocols: Vec<String> = Vec::new();
    let mut msg_sizes: Vec<usize> = Vec::new();
    let earlier = previous_plan
        .iter()
        .map(String::as_str)
        .chain(results.keys().map(String::as_str))
        .filter(|id| in_matrix(id))
        .filter_map(run_dimensions)
        .map(|(protocol, size)| (protocol.to_string(), size));
    let current = options
        .msg_sizes
        .iter()
        .flat_map(|&size| options.protocols.iter().map(move |p| (p.clone(), size)));
    for (protocol, size) in earlier.chain(current) {
        if PROTOCOLS.contains(&protocol.as_str()) && !protocols.contains(&protocol) {
            protocols.push(protocol);
        }
        if !msg_sizes.contains(&size) {
            msg_sizes.push(size);
        }
    }
    let mut whole = options.clone();
    whole.protocols = protocols;
    whole.msg_sizes = msg_sizes;
    whole.only = None;
    let plan = plan_runs(scenarios, &whole)?;
    Ok((whole.protocols, whole.msg_sizes, plan))
}

/// The runs `plan.txt` lists, as (run id, chaos flags). A `plan.txt` from
/// before it listed the flags has the ids only.
fn read_plan(out: &Path) -> Vec<(String, Option<String>)> {
    fs::read_to_string(out.join(PLAN_FILE))
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| match line.split_once('\t') {
            Some((id, args)) => (id.trim().to_string(), Some(args.to_string())),
            None => (line.trim().to_string(), None),
        })
        .collect()
}

/// One finished run, as recorded in `results.tsv`.
#[derive(Debug, Clone, Default)]
struct RunResult {
    outcome: String,
    duration_s: u64,
    reasons: String,
    columns: BTreeMap<String, String>,
}

impl RunResult {
    fn get(&self, column: &str) -> &str {
        match column {
            "outcome" => &self.outcome,
            "reasons" => &self.reasons,
            _ => self.columns.get(column).map(String::as_str).unwrap_or(""),
        }
    }

    /// Whether this result is of a run with these chaos flags; a row that
    /// did not record its flags cannot tell, and counts as such.
    fn ran_with(&self, chaos_args: &str) -> bool {
        let recorded = self.get("chaos_args");
        recorded.is_empty() || recorded == chaos_args
    }

    /// Whether the run was cut short, so its metrics are partial.
    fn interrupted(&self) -> bool {
        self.outcome == "TIMEOUT" || self.outcome == "STALLED"
    }

    fn to_tsv(&self) -> String {
        COLUMNS
            .iter()
            .map(|c| match *c {
                "duration_s" => self.duration_s.to_string(),
                other => tsv_cell(self.get(other)),
            })
            .collect::<Vec<_>>()
            .join("\t")
    }

    fn from_tsv(line: &str) -> Option<(String, Self)> {
        let cells: Vec<&str> = line.split('\t').collect();
        // Rows written before the trailing `attempts` / `chaos_args` columns
        // existed have fewer cells; they read as a single attempt with
        // unknown flags.
        if cells.len() > COLUMNS.len() || cells.len() + OPTIONAL_TRAILING_COLUMNS < COLUMNS.len() {
            return None;
        }
        let mut result = RunResult::default();
        for (column, cell) in COLUMNS.iter().zip(&cells) {
            match *column {
                "outcome" => result.outcome = cell.to_string(),
                "reasons" => result.reasons = cell.to_string(),
                "duration_s" => result.duration_s = cell.parse().unwrap_or(0),
                other => {
                    result.columns.insert(other.to_string(), cell.to_string());
                },
            }
        }
        Some((cells[0].to_string(), result))
    }
}

fn tsv_cell(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}

/// The recorded runs. A missing file is an empty record; a file that cannot
/// be read is an error, since the next write would replace it.
fn load_results(path: &Path) -> anyhow::Result<BTreeMap<String, RunResult>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    Ok(text.lines().skip(1).filter_map(RunResult::from_tsv).collect())
}

fn write_results(path: &Path, results: &BTreeMap<String, RunResult>) -> anyhow::Result<()> {
    let mut text = COLUMNS.join("\t");
    text.push('\n');
    for result in results.values() {
        text.push_str(&result.to_tsv());
        text.push('\n');
    }
    // Write-then-rename so an interrupted write never truncates the record
    // of every earlier run.
    let tmp = path.with_extension("tsv.tmp");
    fs::write(&tmp, text)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// The chaos test executable for each cargo feature, keyed by feature.
type TestBinaries = BTreeMap<&'static str, PathBuf>;

/// Build the chaos test once per feature the plan's runs need (as
/// `cargo xtask chaos` would pick it for each) and return the executables.
/// Runs execute these directly rather than through `cargo test`, which would
/// rebuild a source tree edited while the matrix runs into its later runs.
fn build_test_binaries(plan: &[Planned]) -> anyhow::Result<TestBinaries> {
    let mut features = BTreeSet::new();
    for planned in plan {
        features.insert(crate::chaos_test_feature(&crate::parse_chaos_flags(&planned.chaos_args())?));
    }
    let mut binaries = TestBinaries::new();
    for feature in features {
        println!("chaos-matrix: building the chaos test binary (--features {feature})");
        let output = Command::new("cargo")
            .args(["test", "--features", feature, "--test", "chaos", "--no-run"])
            // JSON artifacts on stdout; diagnostics still rendered on stderr.
            .arg("--message-format=json-render-diagnostics")
            .current_dir(package_root())
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .output()
            .context("running cargo")?;
        if !output.status.success() {
            bail!("building the chaos test binary (--features {feature}) failed");
        }
        let executable = test_executable(&String::from_utf8_lossy(&output.stdout))
            .with_context(|| format!("cargo reported no chaos test executable (--features {feature})"))?;
        binaries.insert(feature, executable);
    }
    Ok(binaries)
}

/// The executable of the `chaos` test target, from `cargo --message-format
/// json` output (one JSON object per line).
fn test_executable(cargo_json: &str) -> Option<PathBuf> {
    cargo_json
        .lines()
        .filter(|l| {
            l.contains(r#""reason":"compiler-artifact""#)
                && l.contains(r#""kind":["test"]"#)
                && l.contains(r#""name":"chaos""#)
        })
        .find_map(|l| json_string_field(l, "executable"))
        .map(PathBuf::from)
}

/// The string value of `"key":"…"` in one line of JSON, unescaped. Enough for
/// cargo's messages; `null` (no executable) reads as `None`.
fn json_string_field(line: &str, key: &str) -> Option<String> {
    let start = line.find(&format!("\"{key}\":\""))? + key.len() + 4;
    let mut value = String::new();
    let mut chars = line[start..].chars();
    loop {
        match chars.next()? {
            '"' => return Some(value),
            '\\' => match chars.next()? {
                'n' => value.push('\n'),
                't' => value.push('\t'),
                'u' => {
                    let code: String = chars.by_ref().take(4).collect();
                    value.push(char::from_u32(u32::from_str_radix(&code, 16).ok()?)?);
                },
                other => value.push(other),
            },
            c => value.push(c),
        }
    }
}

/// The command for one run: the chaos test binary its workloads need, with
/// exactly that run's `CHAOS_*` environment, in the package root (where
/// `cargo test` would run it). Shared by the configuration check and the real
/// run, so the check validates precisely what will run.
fn runner_command(planned: &Planned, binaries: &TestBinaries) -> anyhow::Result<Command> {
    let env = crate::parse_chaos_flags(&planned.chaos_args())?;
    let feature = crate::chaos_test_feature(&env);
    let executable = binaries
        .get(feature)
        .with_context(|| format!("no chaos test binary was built with --features {feature}"))?;
    let mut command = Command::new(executable);
    command.args(RUNNER_ARGS).current_dir(package_root());
    crate::set_chaos_env(&mut command, &env);
    Ok(command)
}

/// Have the harness validate every planned run's configuration (it returns
/// right after parsing it), so a line the harness rejects stops the matrix
/// before the first cluster starts.
fn check_configurations(plan: &[Planned], binaries: &TestBinaries) -> anyhow::Result<()> {
    let mut rejected = Vec::new();
    for planned in plan {
        let output = runner_command(planned, binaries)?
            .env("CHAOS_CHECK_CONFIG", "1")
            .stdin(Stdio::null())
            .output()
            .context("running the configuration check")?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !text.contains("chaos: configuration OK") {
            let reason = text
                .lines()
                .find_map(|l| l.trim().strip_prefix("invalid chaos configuration: "))
                .unwrap_or("the check did not report OK; run it by hand for details");
            rejected.push(format!("{}: {reason}", planned.run_id()));
        }
    }
    if !rejected.is_empty() {
        bail!("the harness rejects {} run(s):\n  {}", rejected.len(), rejected.join("\n  "));
    }
    Ok(())
}

/// How long `planned`'s log may stay silent before the run counts as
/// stalled: `--stall-min`, or longer when one of the run's silent phases is
/// configured longer than that allows for.
fn stall_threshold(minimum: Duration, env: &[(String, String)]) -> Duration {
    let longest = env
        .iter()
        .filter(|(key, _)| SILENT_PHASES.contains(&key.as_str()))
        .filter_map(|(_, value)| value.parse::<u64>().ok())
        .max()
        .unwrap_or(0);
    minimum.max(Duration::from_secs(longest).saturating_add(STALL_MARGIN))
}

/// Write the runner's pid file: the runner's pid, and the process group of
/// the run it is executing, if any. A later session refuses to start while
/// that group lives (`refuse_if_previous_run_alive`).
fn write_pid_file(out: &Path, run_group: Option<u32>) -> anyhow::Result<()> {
    let mut text = format!("{}\n", std::process::id());
    if let Some(pgid) = run_group {
        text.push_str(&format!("child {pgid}\n"));
    }
    fs::write(out.join(status::PID_FILE), text)?;
    Ok(())
}

/// Run one planned run to completion and collect its result; `None` when a
/// signal to the runner stopped it, so it is not recorded.
fn execute(
    planned: &Planned,
    options: &Options,
    binaries: &TestBinaries,
    log: &mut MatrixLog,
) -> anyhow::Result<Option<RunResult>> {
    let run_id = planned.run_id();
    let dir = options.out.join("runs").join(&run_id);
    if dir.exists() {
        // A re-run (or a run the runner was stopped in the middle of): keep
        // the earlier attempt rather than mixing two runs' logs.
        let kept = unused_path(&options.out.join("runs").join(format!("{run_id}.prev-{}", unix_secs())));
        fs::rename(&dir, &kept)?;
    }
    fs::create_dir_all(&dir)?;
    let args = planned.chaos_args();
    fs::write(dir.join("command.txt"), format!("cargo xtask chaos {}\n", args.join(" ")))?;
    let stall_timeout = stall_threshold(options.stall_timeout, &crate::parse_chaos_flags(&args)?);

    let log_path = dir.join("run.log");
    let log_file = File::create(&log_path)?;
    let mut command = runner_command(planned, binaries)?;
    command
        .stdin(Stdio::null())
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        // Its own process group, so a timeout can signal the test binary and
        // everything it started (native gRPC backends) together.
        .process_group(0);

    // Harness clusters already present before this run, to tell what the run
    // left behind when it does not name its cluster. `None`: Docker could
    // not say.
    let containers_before: Option<BTreeSet<String>> = harness_broker_containers().map(|c| c.into_iter().collect());

    let started = now_utc();
    let clock = Instant::now();
    let mut child = command.spawn().context("spawning the chaos test")?;
    let pid = child.id();
    write_pid_file(&options.out, Some(pid))?;
    let mut ended = Ended::Exited;
    let mut last_len = 0;
    let mut last_progress = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        let len = fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
        if len != last_len {
            last_len = len;
            last_progress = Instant::now();
        }
        let cause = if crate::signals::count() > 0 {
            Some(Ended::Signalled)
        } else if clock.elapsed() >= options.run_timeout {
            Some(Ended::TimedOut)
        } else if last_progress.elapsed() >= stall_timeout {
            Some(Ended::Stalled)
        } else {
            None
        };
        if let Some(cause) = cause {
            ended = cause;
            if cause == Ended::Stalled {
                sample_stacks(pid, &dir.join("stack-sample.txt"), log, &run_id);
            }
            // SIGINT first: the harness treats it like Ctrl-C, writes its
            // reports and tears the cluster down. A stalled run has usually
            // stopped polling its signal handler too, so it gets less grace.
            let grace = if cause == Ended::Stalled {
                STALL_GRACE
            } else {
                INTERRUPT_GRACE
            };
            stop_group(&mut child, pid, grace)?;
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    let duration_s = clock.elapsed().as_secs();
    // The run's group is gone; a later session need not wait for it.
    write_pid_file(&options.out, None)?;

    // Lossy: a client log line cut mid-character must not cost the result.
    let text = String::from_utf8_lossy(&fs::read(&log_path).unwrap_or_default()).into_owned();
    let mut parsed = parse_run_log(&text, ended);
    clean_up_cluster(parsed.cluster_network.as_deref(), containers_before.as_ref(), log, &run_id);
    match ended {
        Ended::TimedOut => parsed.reasons.insert(
            0,
            format!(
                "exceeded the run timeout of {} min and was interrupted; its metrics and any later reason describe \
                 the interrupted run (partial)",
                options.run_timeout.as_secs() / 60
            ),
        ),
        Ended::Stalled => parsed.reasons.insert(
            0,
            format!(
                "no output for {} min: the test process stopped making progress and was killed; its metrics are \
                 partial",
                stall_timeout.as_secs() / 60
            ),
        ),
        Ended::Exited | Ended::Signalled => {},
    }
    if let Some(reports) = &parsed.reports_dir {
        // The harness prints the path relative to its own working directory,
        // the package root, which need not be this process's.
        let source = package_root().join(reports);
        if source.is_dir() {
            // A failed move must not abort the matrix before the result is
            // recorded; the reports just stay where the harness wrote them.
            if let Err(e) = move_dir(&source, &dir.join("reports")) {
                log.line(&format!(
                    "{run_id}: WARNING: could not move {} into {}: {e}; the reports stay there",
                    source.display(),
                    dir.display()
                ));
            }
        }
    }
    if ended == Ended::Signalled {
        return Ok(None);
    }

    let mut columns = parsed.metrics;
    columns.insert("run_id".to_string(), run_id);
    columns.insert("scenario".to_string(), planned.scenario.id.clone());
    columns.insert("protocol".to_string(), planned.protocol.clone());
    columns.insert("msg_size".to_string(), planned.msg_size.to_string());
    columns.insert("started".to_string(), started);
    columns.insert("dir".to_string(), dir.display().to_string());
    if let Some((rate, mib)) = parsed.produce_rate {
        columns.insert("produce_rate".to_string(), format!("{rate:.0}"));
        columns.insert("produce_mib_s".to_string(), format!("{mib:.1}"));
    }
    if let Some(defect) = parsed.known_defect {
        columns.insert("known_defect".to_string(), defect);
    }
    Ok(Some(RunResult {
        outcome: parsed.outcome,
        duration_s,
        reasons: parsed.reasons.join(" / "),
        columns,
    }))
}

/// How a run's process came to an end.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Ended {
    /// It exited on its own.
    Exited,
    /// It ran past `--run-timeout-min` and was stopped.
    TimedOut,
    /// Its log did not grow for `--stall-min` and it was stopped.
    Stalled,
    /// The runner was signalled (Ctrl-C, SIGTERM, SIGHUP) and stopped it.
    Signalled,
}

/// Stop process group `pgid`, led by `child`: SIGINT, then SIGKILL once
/// `grace` has passed — or at once when another signal reaches the runner
/// meanwhile (a second Ctrl-C). Returns when the group is gone.
fn stop_group(child: &mut Child, pgid: u32, grace: Duration) -> anyhow::Result<()> {
    let signals_before = crate::signals::count();
    signal_group(pgid, "INT");
    // Wait for the whole group, not just `child`: a process the test started
    // can outlive it, and must still be killed if it hangs.
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline && group_running(child, pgid)? {
        if crate::signals::count() > signals_before {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    if group_running(child, pgid)? {
        signal_group(pgid, "KILL");
        let _ = child.wait();
        // Orphaned group members are reaped by init, not by us.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && group_alive(pgid) {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    Ok(())
}

pub(crate) fn signal_group(pid: u32, signal: &str) {
    let _ = Command::new("kill").args(["-s", signal, "--", &format!("-{pid}")]).status();
}

/// Whether `child` (the leader of process group `pgid`) or any other member
/// of its group is still running. Reaps `child` once it has exited, since its
/// zombie would otherwise keep the group looking alive.
pub(crate) fn group_running(child: &mut Child, pgid: u32) -> std::io::Result<bool> {
    Ok(child.try_wait()?.is_none() || group_alive(pgid))
}

/// Whether any process of group `pgid` exists (`kill -0` to the group).
fn group_alive(pgid: u32) -> bool {
    Command::new("kill")
        .args(["-0", "--", &format!("-{pgid}")])
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Whether a process of group `pgid` is a chaos test (or the `cargo test`
/// running one), not an unrelated group that reused the id.
fn group_runs_chaos(pgid: u32) -> bool {
    Command::new("pgrep")
        .args(["-g", &pgid.to_string(), "-f", "chaos"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Refuse to start while the run an earlier session started is still alive
/// (its runner was killed outright, so it never stopped the run): a new run
/// would compete with it for the host and move its directory aside.
fn refuse_if_previous_run_alive(out: &Path) -> anyhow::Result<()> {
    let group = fs::read_to_string(out.join(status::PID_FILE))
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix("child ")?.trim().parse::<u32>().ok());
    if let Some(pgid) = group {
        if group_alive(pgid) && group_runs_chaos(pgid) {
            bail!(
                "a run started by an earlier chaos-matrix session in {} is still running (process group {pgid}); \
                 stop it with `kill -INT -- -{pgid}` or wait for it to end, then re-run",
                out.display()
            );
        }
    }
    Ok(())
}

/// The output directory's lock: one runner per directory, or two would run
/// clusters side by side and overwrite each other's results. Released (the
/// file removed) on drop; a lock whose runner is gone is taken over.
struct RunnerLock {
    path: PathBuf,
}

impl RunnerLock {
    fn acquire(out: &Path) -> anyhow::Result<Self> {
        let path = out.join(LOCK_FILE);
        let pid = std::process::id();
        // Written aside, then hard-linked into place: the link either fails
        // (a lock exists) or publishes a complete pid, never an empty file a
        // racing runner could mistake for a stale lock.
        let staged = out.join(format!("{LOCK_FILE}.{pid}"));
        fs::write(&staged, format!("{pid}\n"))?;
        let taken = Self::link(&staged, &path, out);
        let _ = fs::remove_file(&staged);
        taken.map(|()| Self { path })
    }

    fn link(staged: &Path, path: &Path, out: &Path) -> anyhow::Result<()> {
        for _ in 0..2 {
            match fs::hard_link(staged, path) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    let holder = fs::read_to_string(path).unwrap_or_default().trim().to_string();
                    if status::is_live_runner(&holder) {
                        bail!(
                            "another chaos-matrix runner (pid {holder}) is working on {}; wait for it, or stop it \
                             (Ctrl-C, or `touch {}/STOP` between runs), before starting another",
                            out.display(),
                            out.display()
                        );
                    }
                    // Its runner is gone without releasing it.
                    let _ = fs::remove_file(path);
                },
                Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
            }
        }
        bail!("could not take {}: it keeps reappearing", path.display())
    }
}

impl Drop for RunnerLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Write a stack sample of the chaos test binary in process group `pgid` to
/// `out`, before a stalled run is signalled. Best effort: macOS `sample` is
/// used, and a host without it (or a binary that already exited) just leaves
/// no sample.
fn sample_stacks(pgid: u32, out: &Path, log: &mut MatrixLog, run_id: &str) {
    let Ok(found) = Command::new("pgrep")
        .args(["-g", &pgid.to_string(), "-f", "deps/chaos-"])
        .output()
    else {
        return;
    };
    let Some(pid) = String::from_utf8_lossy(&found.stdout)
        .lines()
        .next()
        .map(str::trim)
        .map(str::to_string)
    else {
        return;
    };
    let sampled = Command::new("/usr/bin/sample")
        .args([pid.as_str(), STALL_SAMPLE_SECS, "-file"])
        .arg(out)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if matches!(sampled, Ok(status) if status.success()) {
        log.line(&format!(
            "{run_id}: stalled — stack sample of pid {pid} written to {}",
            out.display()
        ));
    }
}

/// Run `command` with its stdout captured, killing it after `timeout`.
/// `None` if it could not start or timed out.
fn output_with_timeout(command: &mut Command, timeout: Duration) -> Option<(ExitStatus, Vec<u8>)> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    // Drained on the side, so a large output cannot fill the pipe and block
    // the command while it is being waited for.
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = stdout.read_to_end(&mut bytes);
        bytes
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            },
        }
    };
    let bytes = reader.join().ok()?;
    Some((status?, bytes))
}

/// The stdout of a successful `docker` command, bounded by [`DOCKER_TIMEOUT`].
fn docker_output(args: &[&str]) -> Option<String> {
    let (status, stdout) = output_with_timeout(Command::new("docker").args(args), DOCKER_TIMEOUT)?;
    status.success().then(|| String::from_utf8_lossy(&stdout).into_owned())
}

/// Every harness broker container (`kafka-<node>-<suffix>`), running or not.
/// `None` when Docker could not list them: that is not "there are none".
fn harness_broker_containers() -> Option<Vec<String>> {
    let names = docker_output(&["ps", "-a", "--format", "{{.Names}}"])?;
    Some(
        names
            .lines()
            .filter(|name| is_harness_broker_name(name))
            .map(str::to_string)
            .collect(),
    )
}

/// Remove what is left of the run's cluster. A killed run never reaches the
/// harness's teardown, and a later run must not share the host with its
/// brokers; but only this run's cluster may go — another harness or
/// integration-test cluster may be running beside the matrix. The run's own
/// cluster is the one on the network its log names; a log that names none
/// (the harness died before its cluster was up, or predates that line) only
/// gets a warning about what appeared during the run.
fn clean_up_cluster(network: Option<&str>, before: Option<&BTreeSet<String>>, log: &mut MatrixLog, run_id: &str) {
    if let Some(network) = network {
        let removed = remove_cluster(network);
        if !removed.is_empty() {
            log.line(&format!(
                "{run_id}: removed the cluster it left behind ({}, network {network})",
                removed.join(", ")
            ));
        }
        return;
    }
    let (Some(before), Some(after)) = (before, harness_broker_containers()) else {
        return;
    };
    let appeared: Vec<String> = after.into_iter().filter(|name| !before.contains(name)).collect();
    if !appeared.is_empty() {
        log.line(&format!(
            "{run_id}: WARNING: broker containers appeared during the run ({}), but its log names no cluster \
             network, so they are left alone; remove them by hand if they are this run's",
            appeared.join(", ")
        ));
    }
}

/// Remove every container on cluster network `network` (`kafka-net-<suffix>`)
/// — its brokers, and any gRPC backend container attached to it — and every
/// `kafka-<n>-<suffix>` broker even if it is no longer attached (a stopped
/// one), with their volumes; then the network, which Docker refuses to remove
/// while a container is still on it. Returns the containers removed.
fn remove_cluster(network: &str) -> Vec<String> {
    let Some(suffix) = network.strip_prefix("kafka-net-") else {
        return Vec::new();
    };
    let mut members: BTreeSet<String> = docker_output(&[
        "ps",
        "-a",
        "--filter",
        &format!("network={network}"),
        "--format",
        "{{.Names}}",
    ])
    .unwrap_or_default()
    .lines()
    .map(str::trim)
    .filter(|name| !name.is_empty())
    .map(str::to_string)
    .collect();
    members.extend(
        harness_broker_containers()
            .unwrap_or_default()
            .into_iter()
            .filter(|name| name.splitn(3, '-').nth(2) == Some(suffix)),
    );
    // Never anything of the ducktape environment, whatever it is attached to.
    members.retain(|name| !name.starts_with("ducker"));
    let members: Vec<String> = members.into_iter().collect();
    if !members.is_empty() {
        let mut args = vec!["rm", "-f", "-v"];
        args.extend(members.iter().map(String::as_str));
        let _ = docker_output(&args);
    }
    let _ = docker_output(&["network", "rm", network]);
    members
}

/// What a run's log says about it.
#[derive(Debug, Default, PartialEq)]
struct ParsedLog {
    outcome: String,
    metrics: BTreeMap<String, String>,
    reasons: Vec<String>,
    /// Summed over the run's producers: (records/s, MiB/s).
    produce_rate: Option<(f64, f64)>,
    reports_dir: Option<String>,
    /// The verifier's `KNOWN DEFECT:` label, when the failure matched the
    /// known multi-topic recreate defect's signature.
    known_defect: Option<String>,
    /// The Docker network of the run's cluster, `kafka-net-<suffix>`.
    cluster_network: Option<String>,
}

fn parse_run_log(log: &str, ended: Ended) -> ParsedLog {
    // The runner prints the abnormal-end notices to stderr (the matching
    // `FAIL (watchdog)` / `FAIL (panic)` headers go only to verdict.txt), so
    // classify on those notices, and on them before the verdict: a run that
    // panicked can still print a clean-looking partial verdict.
    let outcome = if ended == Ended::TimedOut {
        "TIMEOUT"
    } else if ended == Ended::Stalled {
        "STALLED"
    } else if log.contains("invalid chaos configuration") {
        "CONFIG-ERROR"
    } else if log.contains("chaos: WATCHDOG") {
        "FAIL (watchdog)"
    } else if log.contains("chaos: ABORTED by a panic") {
        "FAIL (panic)"
    } else if log.contains("chaos: INTERRUPTED") {
        "FAIL (interrupted)"
    } else if log.contains("test result: ok. 1 passed") || log.contains(FORCED_EXIT_AFTER_PASS) {
        "PASS"
    } else if log.contains("=== Chaos verdict: FAIL") {
        "FAIL"
    } else {
        // Never reached a verdict: the cluster did not start, or the process
        // died before the scenario began.
        "ERROR"
    };

    let mut metrics = BTreeMap::new();
    let mut reasons: Vec<String> = Vec::new();
    let mut rates: Vec<(f64, f64)> = Vec::new();
    let mut reports_dir = None;
    let mut panic_message = None;
    let mut aborted_by: Option<String> = None;
    let mut known_defect = None;
    let mut cluster_network = None;
    let lines: Vec<&str> = log.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if let Some(defect) = trimmed.strip_prefix("KNOWN DEFECT: ") {
            known_defect = Some(defect.chars().take(300).collect::<String>());
        }
        for (column, prefix) in METRICS {
            if let Some(rest) = trimmed.strip_prefix(prefix) {
                if let Some(value) = rest.trim_start().strip_prefix(':') {
                    metrics.insert(column.to_string(), value.trim().to_string());
                }
            }
        }
        if let Some(reason) = trimmed.strip_prefix("FAIL: ") {
            let reason: String = reason.chars().take(300).collect();
            if !reasons.contains(&reason) {
                reasons.push(reason);
            }
        }
        if let Some(rate) = parse_producer_rate(trimmed) {
            rates.push(rate);
        }
        if let Some(dir) = trimmed
            .strip_prefix("chaos: reports written to ")
            .or_else(|| trimmed.strip_prefix("chaos: reports -> "))
        {
            reports_dir = Some(dir.trim().to_string());
        }
        if let Some(network) = trimmed.strip_prefix(CLUSTER_NETWORK_LINE).map(str::trim) {
            if is_cluster_network_name(network) {
                cluster_network = Some(network.to_string());
            }
        }
        if let Some(message) = trimmed.strip_prefix("chaos: ABORTED by a panic in the run: ") {
            aborted_by = Some(message.chars().take(300).collect());
        } else if panic_message.is_none() && trimmed.starts_with("thread '") && trimmed.contains("panicked at") {
            panic_message = lines.get(i + 1).map(|next| next.trim().chars().take(300).collect::<String>());
        }
    }
    // The scenario's own panic is the reason it stopped: it leads.
    if let Some(message) = aborted_by {
        reasons.retain(|r| r != &message);
        reasons.insert(0, message);
    }
    // The test's final panic only restates a failure (the verdict assertion,
    // the watchdog/interrupt notice) — except when nothing else explains the
    // run, e.g. the cluster never started.
    if reasons.is_empty() && outcome != "PASS" {
        reasons.extend(panic_message);
    }
    // Each producer prints its throughput line exactly once, so the sum is
    // the run's aggregate produce rate.
    let produce_rate = (!rates.is_empty()).then(|| rates.iter().fold((0.0, 0.0), |a, r| (a.0 + r.0, a.1 + r.1)));
    // The label only means something on an ordinary verdict failure; a run
    // that also panicked or wedged failed for more than the known defect.
    let known_defect = known_defect.filter(|_| outcome == "FAIL");
    ParsedLog {
        outcome: outcome.to_string(),
        metrics,
        reasons,
        produce_rate,
        reports_dir,
        known_defect,
        cluster_network,
    }
}

/// Parse `chaos: producer-rust-1 sent N records in Ts (R records/s, M MiB/s,
/// target X records/s)` into (R, M).
fn parse_producer_rate(line: &str) -> Option<(f64, f64)> {
    if !line.starts_with("chaos: producer-") || !line.contains(" sent ") {
        return None;
    }
    let inside = line.split_once('(')?.1;
    let (rate, rest) = inside.split_once(" records/s, ")?;
    let mib = rest.split_once(" MiB/s")?.0;
    Some((rate.trim().parse().ok()?, mib.trim().parse().ok()?))
}

fn is_harness_broker_name(name: &str) -> bool {
    let mut parts = name.splitn(3, '-');
    parts.next() == Some("kafka")
        && parts
            .next()
            .is_some_and(|node| !node.is_empty() && node.chars().all(|c| c.is_ascii_digit()))
        && parts.next().is_some_and(|suffix| !suffix.is_empty())
}

/// `kafka-net-<suffix>`, the suffix plain alphanumerics: a harness cluster
/// network, safe to act on.
fn is_cluster_network_name(name: &str) -> bool {
    name.strip_prefix("kafka-net-")
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// Host, Docker and source-tree facts recorded with the results.
fn describe_environment(options: &Options) -> String {
    let capture = |program: &str, args: &[&str]| {
        Command::new(program)
            .args(args)
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    let commit = capture("git", &["rev-parse", "--short", "HEAD"]);
    let branch = capture("git", &["rev-parse", "--abbrev-ref", "HEAD"]);
    let dirty = !capture("git", &["status", "--porcelain", "--", "src", "tests", "xtask"]).is_empty();
    let docker = {
        let raw = capture("docker", &["info", "--format", "{{.NCPU}} {{.MemTotal}}"]);
        match raw.split_once(' ') {
            Some((cpus, bytes)) => match bytes.parse::<u64>() {
                Ok(bytes) => format!("{cpus} CPUs, {:.1} GiB memory", bytes as f64 / (1u64 << 30) as f64),
                Err(_) => raw.clone(),
            },
            None => raw.clone(),
        }
    };
    // The broker tag lives in the shared test cluster code; read it rather
    // than restate it here, so the record cannot drift from what ran.
    let kafka_tag = fs::read_to_string(package_root().join("tests/common/kafka_cluster.rs"))
        .ok()
        .and_then(|src| {
            src.lines().find_map(|l| {
                l.trim()
                    .strip_prefix("const KAFKA_TAG: &str = \"")
                    .map(|r| r.trim_end_matches("\";").to_string())
            })
        })
        .unwrap_or_else(|| "unknown".to_string());
    let kafka_image = format!("apache/kafka:{kafka_tag}");
    format!(
        "started: {}\nbranch: {branch}\ncommit: {commit}{}\nmatrix: {}\nprotocols: {}\nmessage sizes: {}\nproducer rate: {} records/s per topic\nrun timeout: {} min\ndocker: {docker}\nbroker image: {kafka_image}\nhost: {}\n",
        now_utc(),
        if dirty { " (uncommitted changes in src/tests/xtask)" } else { "" },
        options.matrix.display(),
        options.protocols.join(", "),
        options.msg_sizes.iter().map(|s| size_label(*s)).collect::<Vec<_>>().join(", "),
        options.rps,
        options.run_timeout.as_secs() / 60,
        capture("uname", &["-srm"]),
    )
}

/// The summary of the whole matrix: `protocols` × `msg_sizes` × every
/// scenario (`plan`), whatever this invocation selected to run.
fn render_summary(
    options: &Options,
    environment: &str,
    scenarios: &[Scenario],
    protocols: &[String],
    msg_sizes: &[usize],
    plan: &[Planned],
    results: &BTreeMap<String, RunResult>,
) -> String {
    let mut out = String::new();
    let name = options
        .matrix
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    out.push_str(&format!("# Chaos matrix: {name}\n\nLast updated {}.\n\n", now_utc()));
    out.push_str("```\n");
    out.push_str(environment);
    out.push_str("```\n\n");

    let finished: Vec<(String, &RunResult)> = plan
        .iter()
        .filter_map(|p| recorded(results, p).map(|r| (p.run_id(), r)))
        .collect();
    let passed = finished.iter().filter(|(_, r)| r.outcome == "PASS").count();
    out.push_str(&format!(
        "**{} of {} runs finished: {passed} passed, {} did not pass, {} not run yet.**\n\n",
        finished.len(),
        plan.len(),
        finished.len() - passed,
        plan.len() - finished.len()
    ));

    if scenarios.iter().any(|s| s.protocols.is_some()) {
        out.push_str(
            "A dash (—) marks a protocol the matrix file does not run that scenario with (its protocols \
             column).\n\n",
        );
    }
    let changed = plan
        .iter()
        .any(|p| results.contains_key(&p.run_id()) && recorded(results, p).is_none());
    if changed {
        out.push_str(
            "\"not run (flags changed)\" marks a run recorded with other chaos flags (its scenario line or \
             `--rps` changed since); that result no longer counts.\n\n",
        );
    }
    // One grid per message size: scenario rows, protocol columns.
    let in_plan: Vec<&Scenario> = scenarios
        .iter()
        .filter(|s| plan.iter().any(|p| p.scenario.id == s.id))
        .collect();
    for &msg_size in msg_sizes {
        out.push_str(&format!(
            "## Message size {} ({} bytes)\n\n| # | Scenario |",
            size_label(msg_size),
            msg_size
        ));
        for protocol in protocols {
            out.push_str(&format!(" {protocol} |"));
        }
        out.push_str("\n|---|---|");
        for _ in protocols {
            out.push_str("---|");
        }
        out.push('\n');
        for scenario in &in_plan {
            out.push_str(&format!("| {} | {} |", scenario.id, scenario.description));
            for protocol in protocols {
                let planned = Planned {
                    scenario: (*scenario).clone(),
                    protocol: protocol.clone(),
                    msg_size,
                    rps: options.rps,
                };
                let cell = match recorded(results, &planned) {
                    _ if !scenario.runs_with(protocol) => "—".to_string(),
                    Some(r) => r.outcome.clone(),
                    None if results.contains_key(&planned.run_id()) => "not run (flags changed)".to_string(),
                    None => "not run".to_string(),
                };
                out.push_str(&format!(" {cell} |"));
            }
            out.push('\n');
        }
        out.push('\n');
    }

    out.push_str("## Scenario commands\n\n");
    out.push_str("Each run appends `--security-protocol P --msg-size S --rps R --reports` to the flags below.\n\n");
    out.push_str("| # | Scenario | Flags |\n|---|---|---|\n");
    for scenario in &in_plan {
        out.push_str(&format!(
            "| {} | {} | `{}` |\n",
            scenario.id,
            scenario.description,
            scenario.args.join(" ")
        ));
    }
    out.push('\n');

    out.push_str("## Run details\n\n");
    out.push_str(
        "Attempts above 1 are retries after the known multi-topic recreate defect; the duration covers every \
         attempt, the metrics are the last attempt's, and earlier attempts' logs are kept in \
         `runs/<run>.attempt-<n>/`. KNOWN-DEFECT means every attempt hit that defect's exact signature and \
         nothing else failed. The metrics of a TIMEOUT or STALLED run are marked partial: it was interrupted, \
         and they describe it up to then.\n\n",
    );
    out.push_str(
        "| Run | Outcome | Attempts | Duration | Produce rate (rec/s) | MiB/s | Delivered | Lost | Failed sends | \
         Committed checks | Ordering | Rebalance callbacks |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for (run_id, r) in &finished {
        let attempts = match r.get("attempts") {
            "" => "1",
            n => n,
        };
        let metric = |column: &str| match r.get(column) {
            value if r.interrupted() && !value.is_empty() => format!("{value} (partial)"),
            value => value.to_string(),
        };
        out.push_str(&format!(
            "| {run_id} | {} | {attempts} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            r.outcome,
            format_duration(r.duration_s),
            metric("produce_rate"),
            metric("produce_mib_s"),
            metric("delivered"),
            metric("lost"),
            metric("failed_sends"),
            metric("commit_checks"),
            metric("ordering"),
            metric("rebalance"),
        ));
    }
    out.push('\n');

    let failures: Vec<&(String, &RunResult)> = finished.iter().filter(|(_, r)| r.outcome != "PASS").collect();
    out.push_str("## Runs that did not pass\n\n");
    if failures.is_empty() {
        out.push_str("None.\n");
    }
    for (run_id, r) in failures {
        out.push_str(&format!(
            "- **{run_id}** — {}: {}. Logs: `{}`\n",
            r.outcome,
            if r.reasons.is_empty() {
                "no verdict reason recorded; see run.log"
            } else {
                &r.reasons
            },
            r.get("dir")
        ));
    }
    out
}

fn format_duration(secs: u64) -> String {
    format!("{}m{:02}s", secs / 60, secs % 60)
}

/// Timestamped progress, to stdout and `matrix.log`.
struct MatrixLog {
    file: File,
}

impl MatrixLog {
    fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(Self { file: OpenOptions::new().create(true).append(true).open(path)? })
    }

    fn line(&mut self, message: &str) {
        let line = format!("{} {message}", now_utc());
        // Not `println!`, which panics once the terminal is gone (after a
        // SIGHUP), while the runner still has a run to stop and record.
        let _ = writeln!(std::io::stdout(), "{line}");
        let _ = writeln!(self.file, "{line}");
    }
}

/// The root of the package the chaos test belongs to, which `cargo test` makes
/// the test binary's working directory: the workspace root, xtask's parent.
pub(crate) fn package_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives inside the workspace")
}

/// Move directory `from` to `to`, copying and then removing it when a rename
/// is not possible (e.g. `to` is on another filesystem).
fn move_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    if fs::rename(from, to).is_ok() {
        return Ok(());
    }
    copy_dir(from, to)?;
    fs::remove_dir_all(from)
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// `path` if nothing is there yet, else `path` with the first free `-<n>`
/// suffix. Kept attempt directories are never overwritten: a re-run of a run
/// that was already retried finds its earlier `.attempt-1` in place.
fn unused_path(path: &Path) -> PathBuf {
    if !path.exists() {
        return path.to_path_buf();
    }
    (2..)
        .map(|n| PathBuf::from(format!("{}-{n}", path.display())))
        .find(|candidate| !candidate.exists())
        .expect("an unbounded suffix range always has a free name")
}

/// The current UTC time as `YYYY-MM-DDTHH:MM:SSZ`.
fn now_utc() -> String {
    Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| unix_secs().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    fn options(protocols: &[&str], sizes: &[usize]) -> Options {
        Options {
            matrix: PathBuf::from("m.txt"),
            out: PathBuf::from("out"),
            protocols: protocols.iter().map(|s| s.to_string()).collect(),
            msg_sizes: sizes.to_vec(),
            rps: 1000,
            only: None,
            run_timeout: Duration::from_secs(60),
            rerun_failed: false,
            rerun: BTreeSet::new(),
            check_only: false,
            stall_timeout: Duration::from_secs(60),
            known_defect_attempts: 3,
        }
    }

    #[test]
    fn matrix_lines_parse_and_comments_are_skipped() {
        let text = "# header\n\n1 | Rolling restart | --brokers 5 --unclean\nP1 | Fan-out |  --partitions 48 \n";
        let scenarios = parse_matrix(text).unwrap();
        assert_eq!(
            scenarios,
            vec![
                Scenario {
                    id: "1".into(),
                    description: "Rolling restart".into(),
                    args: vec!["--brokers".into(), "5".into(), "--unclean".into()],
                    protocols: None,
                },
                Scenario {
                    id: "P1".into(),
                    description: "Fan-out".into(),
                    args: vec!["--partitions".into(), "48".into()],
                    protocols: None,
                },
            ]
        );
    }

    #[test]
    fn the_protocols_column_restricts_a_scenario() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n2 | b | --cycles 2 | plaintext\n").unwrap();
        assert_eq!(scenarios[1].protocols, Some(vec!["plaintext".to_string()]));
        let plan = plan_runs(&scenarios, &options(&["plaintext", "ssl", "sasl_ssl"], &[100])).unwrap();
        assert_eq!(
            plan.iter().map(Planned::run_id).collect::<Vec<_>>(),
            ["1-plaintext-100B", "2-plaintext-100B", "1-ssl-100B", "1-sasl_ssl-100B"]
        );
        let bad = parse_matrix("1 | a | --cycles 1 | tls\n").unwrap_err();
        assert_eq!(
            bad.to_string(),
            "matrix line 1: protocol 'tls' is not one of plaintext, ssl, sasl_plaintext, sasl_ssl"
        );
        let empty = parse_matrix("1 | a | --cycles 1 | \n").unwrap_err();
        assert_eq!(empty.to_string(), "matrix line 1: the protocols column is empty");
    }

    #[test]
    fn matrix_rejects_duplicates_dimension_flags_and_malformed_lines() {
        let dup = parse_matrix("1 | a | --cycles 1\n1 | b | --cycles 2\n").unwrap_err();
        assert_eq!(dup.to_string(), "matrix line 2: duplicate scenario id '1'");
        let dim = parse_matrix("1 | a | --cycles 1 --msg-size 5\n").unwrap_err();
        assert_eq!(
            dim.to_string(),
            "matrix line 1: scenario '1' sets --msg-size, which the matrix runner sets for every run"
        );
        let inline = parse_matrix("1 | a | --cycles 1 --rps=5\n").unwrap_err();
        assert_eq!(
            inline.to_string(),
            "matrix line 1: scenario '1' sets --rps, which the matrix runner sets for every run"
        );
        let bad = parse_matrix("1 | only two\n").unwrap_err();
        assert_eq!(
            bad.to_string(),
            "matrix line 1: expected `ID | description | flags [| protocols]`"
        );
    }

    #[test]
    fn plan_orders_size_then_protocol_then_scenario_and_appends_dimensions() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n2 | b | --cycles 2\n").unwrap();
        let plan = plan_runs(&scenarios, &options(&["plaintext", "ssl"], &[100, 1024 * 1024])).unwrap();
        let ids: Vec<String> = plan.iter().map(Planned::run_id).collect();
        assert_eq!(
            ids,
            [
                "1-plaintext-100B",
                "2-plaintext-100B",
                "1-ssl-100B",
                "2-ssl-100B",
                "1-plaintext-1MiB",
                "2-plaintext-1MiB",
                "1-ssl-1MiB",
                "2-ssl-1MiB",
            ]
        );
        assert_eq!(
            plan[4].chaos_args().join(" "),
            "--cycles 1 --security-protocol plaintext --msg-size 1048576 --rps 1000 --reports"
        );
    }

    #[test]
    fn only_filters_scenarios_and_rejects_unknown_ids() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n2 | b | --cycles 2\n").unwrap();
        let mut opts = options(&["plaintext"], &[100]);
        opts.only = Some(["2".to_string()].into_iter().collect());
        let plan = plan_runs(&scenarios, &opts).unwrap();
        assert_eq!(plan.iter().map(Planned::run_id).collect::<Vec<_>>(), ["2-plaintext-100B"]);
        opts.only = Some(["9".to_string()].into_iter().collect());
        assert!(plan_runs(&scenarios, &opts).is_err());
    }

    #[test]
    fn a_passing_log_yields_its_metrics_rate_and_reports_dir() {
        let log = "\
chaos: reports -> target/chaos-runs/1-2
chaos: producer-rust-1 sent 6000 records in 6.0s (1000 records/s, 0.1 MiB/s, target 1000 records/s)
=== Chaos verdict: PASS ===
  delivered (acked) records : 6000
  failed sends              : 0
  lost (delivered, unseen)  : 0
  rebalance callbacks       : revoked=1 assigned=2 lost=0 (1 consumer(s) with listener)
  committed offsets checked : 12 (0 violation(s))
chaos: reports written to target/chaos-runs/1-2
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 70 filtered out
";
        let parsed = parse_run_log(log, Ended::Exited);
        assert_eq!(parsed.outcome, "PASS");
        assert_eq!(parsed.metrics["delivered"], "6000");
        assert_eq!(parsed.metrics["lost"], "0");
        assert_eq!(parsed.metrics["commit_checks"], "12 (0 violation(s))");
        assert_eq!(parsed.produce_rate, Some((1000.0, 0.1)));
        assert_eq!(parsed.reports_dir.as_deref(), Some("target/chaos-runs/1-2"));
        assert!(parsed.reasons.is_empty());
    }

    #[test]
    fn failing_logs_are_classified_with_their_reasons() {
        let verdict = "=== Chaos verdict: FAIL ===\n  lost (delivered, unseen)  : 3\n  FAIL: data loss: 3 acknowledged record(s) never consumed\n  FAIL: data loss: 3 acknowledged record(s) never consumed\n";
        let parsed = parse_run_log(verdict, Ended::Exited);
        assert_eq!(parsed.outcome, "FAIL");
        assert_eq!(parsed.reasons, ["data loss: 3 acknowledged record(s) never consumed"]);

        // Died before the scenario: no verdict, the reason is the panic text.
        let panicked =
            "thread 'run_test::chaos_run' (1) panicked at tests/chaos/harness.rs:130:17:\ncluster did not start\n";
        let parsed = parse_run_log(panicked, Ended::Exited);
        assert_eq!(parsed.outcome, "ERROR");
        assert_eq!(parsed.reasons, ["cluster did not start"]);

        // Panicked mid-scenario: classified as such even though the partial
        // verdict printed after it says PASS.
        let aborted = "chaos: ABORTED by a panic in the run: broker 2 did not become operational within 120s\n\
                       Partial verdict below.\n=== Chaos verdict: PASS ===\n";
        let parsed = parse_run_log(aborted, Ended::Exited);
        assert_eq!(parsed.outcome, "FAIL (panic)");
        assert_eq!(parsed.reasons, ["broker 2 did not become operational within 120s"]);

        let wedged = "chaos: WATCHDOG — run exceeded 900s without finishing\n=== Chaos verdict: PASS ===\n";
        assert_eq!(parse_run_log(wedged, Ended::Exited).outcome, "FAIL (watchdog)");
        assert_eq!(
            parse_run_log("chaos: INTERRUPTED (Ctrl-C)", Ended::Exited).outcome,
            "FAIL (interrupted)"
        );
        assert_eq!(
            parse_run_log("invalid chaos configuration: x", Ended::Exited).outcome,
            "CONFIG-ERROR"
        );
        assert_eq!(parse_run_log("test result: ok. 1 passed;", Ended::TimedOut).outcome, "TIMEOUT");
        // A filter that matched no test is not a pass.
        assert_eq!(parse_run_log("test result: ok. 0 passed;", Ended::Exited).outcome, "ERROR");
        assert_eq!(parse_run_log("chaos: cycle 7/12", Ended::Stalled).outcome, "STALLED");
        // A passing run the harness had to force-exit never prints libtest's
        // summary; a failing one it force-exited is still a failure.
        let forced_pass = "=== Chaos verdict: PASS ===\nchaos: FORCED EXIT after a PASS verdict — the process did not terminate after teardown\n";
        assert_eq!(parse_run_log(forced_pass, Ended::Exited).outcome, "PASS");
        let forced_fail = "=== Chaos verdict: FAIL ===\nchaos: FORCED EXIT — the process did not terminate after teardown (the run had already failed)\n";
        assert_eq!(parse_run_log(forced_fail, Ended::Exited).outcome, "FAIL");
    }

    #[test]
    fn the_known_defect_label_is_read_only_from_an_ordinary_verdict_failure() {
        let failed = "=== Chaos verdict: FAIL ===\n  FAIL: data loss: 3 acknowledged record(s) never consumed\n  \
                      KNOWN DEFECT: multi-topic recreate stale positions — 3 record(s) lost\n";
        let parsed = parse_run_log(failed, Ended::Exited);
        assert_eq!(parsed.outcome, "FAIL");
        assert_eq!(
            parsed.known_defect.as_deref(),
            Some("multi-topic recreate stale positions — 3 record(s) lost")
        );
        // Also panicked: more than the known defect went wrong.
        let panicked = format!("chaos: ABORTED by a panic in the run: boom\n{failed}");
        assert_eq!(parse_run_log(&panicked, Ended::Exited).known_defect, None);
    }

    #[test]
    fn results_written_before_the_trailing_columns_still_load() {
        let current = RunResult { outcome: "PASS".into(), ..RunResult::default() }.to_tsv();
        let without_args = current.rsplit_once('\t').unwrap().0;
        let (_, back) = RunResult::from_tsv(without_args).expect("a row without the chaos_args cell loads");
        assert_eq!(back.outcome, "PASS");
        assert_eq!(back.get("chaos_args"), "");
        // Unknown flags count as this run's: no hours of re-runs for a
        // change that most likely never happened.
        assert!(back.ran_with("--cycles 1"));
        let without_attempts = without_args.rsplit_once('\t').unwrap().0;
        let (_, back) = RunResult::from_tsv(without_attempts).expect("a row without the attempts cell loads");
        assert_eq!(back.get("attempts"), "");
        let too_short = without_attempts.rsplit_once('\t').unwrap().0;
        assert!(RunResult::from_tsv(too_short).is_none());
    }

    #[test]
    fn a_run_recorded_with_other_flags_is_not_recorded() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n").unwrap();
        let plan = plan_runs(&scenarios, &options(&["plaintext"], &[100])).unwrap();
        let mut result = RunResult { outcome: "PASS".into(), ..RunResult::default() };
        result.columns.insert(
            "chaos_args".into(),
            "--cycles 1 --security-protocol plaintext --msg-size 100 --rps 1000 --reports".into(),
        );
        let mut results = BTreeMap::new();
        results.insert("1-plaintext-100B".to_string(), result.clone());
        assert!(recorded(&results, &plan[0]).is_some());
        // The same run id at another --rps, or after the scenario line changed.
        let mut faster = options(&["plaintext"], &[100]);
        faster.rps = 5000;
        assert!(recorded(&results, &plan_runs(&scenarios, &faster).unwrap()[0]).is_none());
        let edited = parse_matrix("1 | a | --cycles 2\n").unwrap();
        assert!(recorded(&results, &plan_runs(&edited, &options(&["plaintext"], &[100])).unwrap()[0]).is_none());
        // Round-trips through results.tsv.
        let (_, back) = RunResult::from_tsv(&result.to_tsv()).unwrap();
        assert_eq!(back.get("chaos_args"), result.get("chaos_args"));
    }

    #[test]
    fn a_narrowed_invocation_still_summarises_the_whole_matrix() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n2 | b | --cycles 2\n").unwrap();
        let previous: Vec<String> = plan_runs(&scenarios, &options(&["plaintext", "ssl"], &[100, 1024 * 1024]))
            .unwrap()
            .iter()
            .map(Planned::run_id)
            .collect();
        // Session 2 runs only scenario 2 at ssl / 100B.
        let mut narrowed = options(&["ssl"], &[100]);
        narrowed.only = Some(["2".to_string()].into_iter().collect());
        assert_eq!(
            plan_runs(&scenarios, &narrowed)
                .unwrap()
                .iter()
                .map(Planned::run_id)
                .collect::<Vec<_>>(),
            ["2-ssl-100B"]
        );
        let (protocols, sizes, full) = full_plan(&scenarios, &narrowed, &previous, &BTreeMap::new()).unwrap();
        assert_eq!(protocols, ["plaintext", "ssl"]);
        assert_eq!(sizes, [100, 1024 * 1024]);
        assert_eq!(full.iter().map(Planned::run_id).collect::<Vec<_>>(), previous);
        // A first session has no earlier plan: its own dimensions.
        let (protocols, _, full) = full_plan(&scenarios, &narrowed, &[], &BTreeMap::new()).unwrap();
        assert_eq!(protocols, ["ssl"]);
        assert_eq!(full.len(), 2, "--only narrows what runs, not the matrix");
        // A recorded result adds its dimensions; one of a scenario no longer
        // in the matrix file adds none.
        let mut results = BTreeMap::new();
        results.insert("1-sasl_ssl-1MiB".to_string(), RunResult::default());
        results.insert("9-sasl_plaintext-100B".to_string(), RunResult::default());
        let (protocols, sizes, _) = full_plan(&scenarios, &narrowed, &[], &results).unwrap();
        assert_eq!(protocols, ["sasl_ssl", "ssl"]);
        assert_eq!(sizes, [1024 * 1024, 100]);

        let summary = render_summary(
            &narrowed,
            "env\n",
            &scenarios,
            &["plaintext".to_string(), "ssl".to_string()],
            &[100],
            &plan_runs(&scenarios, &options(&["plaintext", "ssl"], &[100])).unwrap(),
            &BTreeMap::new(),
        );
        assert!(summary.contains("**0 of 4 runs finished: 0 passed, 0 did not pass, 4 not run yet.**"));
        assert!(summary.contains("| 1 | a | not run | not run |"));
    }

    #[test]
    fn duplicate_dimensions_are_one_dimension() {
        let parsed = parse_options(&strings(&[
            "--matrix",
            "m.txt",
            "--protocols",
            "ssl,plaintext,ssl",
            "--msg-sizes",
            "100,100,1048576",
        ]))
        .unwrap();
        assert_eq!(parsed.protocols, ["ssl", "plaintext"]);
        assert_eq!(parsed.msg_sizes, [100, 1024 * 1024]);
        let scenarios = parse_matrix("1 | a | --cycles 1 | ssl, ssl\n").unwrap();
        assert_eq!(scenarios[0].protocols, Some(vec!["ssl".to_string()]));
    }

    #[test]
    fn timeouts_must_be_whole_minutes_that_fit() {
        let err = |flag: &str, value: &str| {
            parse_options(&strings(&["--matrix", "m.txt", flag, value]))
                .unwrap_err()
                .to_string()
        };
        assert_eq!(err("--run-timeout-min", "0"), "--run-timeout-min must be >= 1");
        assert_eq!(err("--stall-min", "0"), "--stall-min must be >= 1");
        assert_eq!(
            err("--run-timeout-min", &u64::MAX.to_string()),
            "--run-timeout-min is too large"
        );
        assert_eq!(err("--run-timeout-min", "soon"), "--run-timeout-min must be a number");
        assert_eq!(
            err("--out", "--check-only"),
            "--out requires a value, but got the flag --check-only"
        );
        let ok = parse_options(&strings(&["--matrix", "m.txt", "--run-timeout-min", "2"])).unwrap();
        assert_eq!(ok.run_timeout, Duration::from_secs(120));
    }

    #[test]
    fn a_scenario_without_runs_is_reported() {
        let scenarios = parse_matrix("1 | a | --cycles 1\n2 | b | --cycles 2 | sasl_ssl\n").unwrap();
        assert_eq!(
            runless_scenarios(&scenarios, &options(&["plaintext"], &[100])),
            ["scenario 2 has no runs: its protocols column (sasl_ssl) names none of --protocols plaintext"]
        );
        let mut only_two = options(&["plaintext"], &[100]);
        only_two.only = Some(["2".to_string()].into_iter().collect());
        assert!(plan_runs(&scenarios, &only_two).unwrap().is_empty());
        let mut only_one = options(&["plaintext"], &[100]);
        only_one.only = Some(["1".to_string()].into_iter().collect());
        assert!(runless_scenarios(&scenarios, &only_one).is_empty());
    }

    #[test]
    fn the_stall_threshold_covers_the_longest_silent_phase() {
        let minimum = Duration::from_secs(20 * 60);
        let env = |flags: &[&str]| crate::parse_chaos_flags(&strings(flags)).unwrap();
        assert_eq!(stall_threshold(minimum, &env(&["--cycles", "3"])), minimum);
        assert_eq!(stall_threshold(minimum, &env(&["--drain-s", "300"])), minimum);
        // A 30 min drain prints nothing for 30 min: 30 + the 5 min margin.
        assert_eq!(
            stall_threshold(minimum, &env(&["--drain-s", "1800", "--warmup-s", "600"])),
            Duration::from_secs(35 * 60)
        );
        assert_eq!(
            stall_threshold(minimum, &env(&["--warmup-s=2400"])),
            Duration::from_secs(45 * 60)
        );
    }

    #[test]
    fn the_test_executable_is_read_from_cargo_json() {
        let json = concat!(
            r#"{"reason":"compiler-artifact","package_id":"path+file:///w/rust#confluent-kafka@0.1.0","#,
            r#""target":{"kind":["lib"],"crate_types":["lib"],"name":"confluent_kafka"},"executable":null}"#,
            "\n",
            r#"{"reason":"compiler-artifact","target":{"kind":["test"],"crate_types":["bin"],"name":"chaos","#,
            r#""src_path":"/w/rust/tests/chaos/main.rs"},"profile":{"test":true},"#,
            r#""executable":"/w/rust/target/debug/deps/chaos-0123abcd","fresh":true}"#,
            "\n",
            r#"{"reason":"build-finished","success":true}"#,
            "\n"
        );
        assert_eq!(
            test_executable(json),
            Some(PathBuf::from("/w/rust/target/debug/deps/chaos-0123abcd"))
        );
        assert_eq!(test_executable(r#"{"reason":"build-finished","success":true}"#), None);
        assert_eq!(
            json_string_field(r#"{"executable":"C:\\t\\chaos \"x\" \u00e9"}"#, "executable").as_deref(),
            Some("C:\\t\\chaos \"x\" é")
        );
    }

    #[test]
    fn the_cluster_network_is_read_from_the_run_log() {
        let log = "chaos: cluster network kafka-net-1a2b3c4d\n=== Chaos verdict: PASS ===\n";
        assert_eq!(
            parse_run_log(log, Ended::Exited).cluster_network.as_deref(),
            Some("kafka-net-1a2b3c4d")
        );
        // Anything else on that line is not acted on.
        let odd = "chaos: cluster network kafka-net-x; docker rm everything\n";
        assert_eq!(parse_run_log(odd, Ended::Exited).cluster_network, None);
        assert!(is_cluster_network_name("kafka-net-1a2b3c4d"));
        assert!(!is_cluster_network_name("kafka-net-"));
        assert!(!is_cluster_network_name("ducknet"));
        assert!(!is_cluster_network_name("kafka-net-a b"));
    }

    #[test]
    fn interrupted_runs_say_so_first_and_their_metrics_are_partial() {
        let mut result = RunResult { outcome: "TIMEOUT".into(), ..RunResult::default() };
        result.columns.insert("lost".into(), "12".into());
        result.columns.insert("chaos_args".into(), String::new());
        assert!(result.interrupted());
        let scenarios = parse_matrix("1 | a | --cycles 1\n").unwrap();
        let opts = options(&["plaintext"], &[100]);
        let plan = plan_runs(&scenarios, &opts).unwrap();
        let results: BTreeMap<String, RunResult> = [("1-plaintext-100B".to_string(), result)].into_iter().collect();
        let summary = render_summary(&opts, "", &scenarios, &opts.protocols, &opts.msg_sizes, &plan, &results);
        assert!(summary.contains("| 12 (partial) |"), "{summary}");
        assert!(!RunResult { outcome: "FAIL".into(), ..RunResult::default() }.interrupted());
    }

    #[test]
    fn an_unreadable_results_file_is_an_error_not_an_empty_record() {
        let root = std::env::temp_dir().join(format!("chaos-matrix-results-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        assert!(load_results(&root.join("results.tsv")).unwrap().is_empty());
        // A directory where the file should be: reading fails, and it must
        // not read as "nothing recorded" (the next write would replace it).
        fs::create_dir(root.join("results.tsv")).unwrap();
        let err = load_results(&root.join("results.tsv")).unwrap_err();
        assert_eq!(err.to_string(), format!("reading {}", root.join("results.tsv").display()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn one_runner_per_output_directory() {
        let out = std::env::temp_dir().join(format!("chaos-matrix-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        fs::create_dir_all(&out).unwrap();
        // A stand-in runner: its command line carries the `chaos-matrix` token.
        let mut runner = Command::new("sh")
            .args(["-c", "sleep 30; :", "chaos-matrix"])
            .process_group(0)
            .spawn()
            .unwrap();
        fs::write(out.join(LOCK_FILE), format!("{}\n", runner.id())).unwrap();
        let err = RunnerLock::acquire(&out).err().expect("a live runner holds the lock");
        assert_eq!(
            err.to_string(),
            format!(
                "another chaos-matrix runner (pid {}) is working on {}; wait for it, or stop it (Ctrl-C, or \
                 `touch {}/STOP` between runs), before starting another",
                runner.id(),
                out.display(),
                out.display()
            )
        );
        signal_group(runner.id(), "KILL");
        runner.wait().unwrap();
        // Its runner is gone: the lock is stale and taken over, then released.
        let lock = RunnerLock::acquire(&out).unwrap();
        assert_eq!(
            fs::read_to_string(out.join(LOCK_FILE)).unwrap(),
            format!("{}\n", std::process::id())
        );
        drop(lock);
        assert!(!out.join(LOCK_FILE).exists());
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0, "no staged lock file is left behind");
        fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn a_run_left_alive_by_a_killed_runner_blocks_a_new_session() {
        let out = std::env::temp_dir().join(format!("chaos-matrix-orphan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        fs::create_dir_all(&out).unwrap();
        // The orphaned run: its own process group, a chaos-looking command.
        let mut run = Command::new("sh")
            .args(["-c", "sleep 30; :", "deps/chaos-0123"])
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = run.id();
        fs::write(out.join(status::PID_FILE), format!("1\nchild {pgid}\n")).unwrap();
        let err = refuse_if_previous_run_alive(&out).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "a run started by an earlier chaos-matrix session in {} is still running (process group {pgid}); \
                 stop it with `kill -INT -- -{pgid}` or wait for it to end, then re-run",
                out.display()
            )
        );
        signal_group(pgid, "KILL");
        run.wait().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while group_alive(pgid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        refuse_if_previous_run_alive(&out).unwrap();
        // A pid file without a run (between runs) blocks nothing either.
        write_pid_file(&out, None).unwrap();
        refuse_if_previous_run_alive(&out).unwrap();
        fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn stopping_a_run_sends_sigint_then_kills_after_the_grace() {
        // Honours SIGINT, as the harness does: gone well before the grace.
        let mut polite = Command::new("sh").args(["-c", "sleep 30; :"]).process_group(0).spawn().unwrap();
        let pgid = polite.id();
        let started = Instant::now();
        stop_group(&mut polite, pgid, Duration::from_secs(20)).unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(!group_alive(pgid));
        // Ignores it, like a wedged test: killed once the grace is over.
        let mut wedged = Command::new("sh")
            .args(["-c", "trap '' INT; sleep 30; :"])
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = wedged.id();
        // Let the shell install its trap before it is signalled.
        std::thread::sleep(Duration::from_millis(300));
        let started = Instant::now();
        stop_group(&mut wedged, pgid, Duration::from_secs(1)).unwrap();
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert!(!group_alive(pgid));
    }

    #[test]
    fn a_hung_command_is_cut_off() {
        let started = Instant::now();
        assert!(output_with_timeout(Command::new("sleep").arg("30"), Duration::from_millis(300)).is_none());
        assert!(started.elapsed() < Duration::from_secs(10));
        let (status, out) = output_with_timeout(Command::new("echo").arg("hi"), Duration::from_secs(10)).unwrap();
        assert!(status.success());
        assert_eq!(out, b"hi\n");
    }

    #[test]
    fn run_ids_split_back_into_their_dimensions() {
        assert_eq!(run_dimensions("broker-roll-sasl_ssl-1MiB"), Some(("sasl_ssl", 1024 * 1024)));
        assert_eq!(run_dimensions("1-plaintext-100B"), Some(("plaintext", 100)));
        assert_eq!(run_dimensions("junk"), None);
        assert_eq!(parse_size_label(&format!("{}MiB", usize::MAX)), None);
    }

    #[test]
    fn producer_rates_are_summed_across_producers() {
        let log = "chaos: producer-rust-100 sent 10 records in 1.0s (400 records/s, 400.0 MiB/s, target 1000 records/s)\n\
                   chaos: producer-rust-101 sent 10 records in 1.0s (350 records/s, 350.5 MiB/s, target 1000 records/s)\n";
        assert_eq!(parse_run_log(log, Ended::Exited).produce_rate, Some((750.0, 750.5)));
    }

    #[test]
    fn results_round_trip_through_tsv() {
        let mut result = RunResult {
            outcome: "FAIL".into(),
            duration_s: 61,
            reasons: "a\tb".into(),
            columns: BTreeMap::new(),
        };
        result.columns.insert("run_id".into(), "1-ssl-100B".into());
        result.columns.insert("delivered".into(), "5".into());
        let (id, back) = RunResult::from_tsv(&result.to_tsv()).unwrap();
        assert_eq!(id, "1-ssl-100B");
        assert_eq!(back.outcome, "FAIL");
        assert_eq!(back.duration_s, 61);
        assert_eq!(back.reasons, "a b", "tabs in a reason must not split the row");
        assert_eq!(back.get("delivered"), "5");
    }

    #[test]
    fn only_harness_broker_containers_count_as_leftovers() {
        assert!(is_harness_broker_name("kafka-1-a1b2c3"));
        assert!(is_harness_broker_name("kafka-12-x"));
        assert!(!is_harness_broker_name("ducker01"));
        assert!(!is_harness_broker_name("kafka-net-abc"));
        assert!(!is_harness_broker_name("kafka-grpc-server"));
        assert!(!is_harness_broker_name("kafka-1-"));
    }

    #[test]
    fn sizes_are_labelled_in_bytes_or_mebibytes() {
        assert_eq!(size_label(100), "100B");
        assert_eq!(size_label(1024 * 1024), "1MiB");
        assert_eq!(size_label(1_000_000), "1000000B");
    }

    #[test]
    fn only_outcomes_other_than_pass_and_known_defect_fail_the_matrix() {
        let result = |outcome: &str| RunResult { outcome: outcome.into(), ..RunResult::default() };
        let (pass, known, fail, stalled) = (result("PASS"), result(KNOWN_DEFECT), result("FAIL"), result("STALLED"));
        let recorded = [
            ("1-plaintext-100B".to_string(), &pass),
            ("12-plaintext-100B".to_string(), &known),
            ("13-plaintext-100B".to_string(), &fail),
            ("15-plaintext-100B".to_string(), &stalled),
        ];
        assert_eq!(failed_runs(&recorded), ["13-plaintext-100B", "15-plaintext-100B"]);
        assert!(failed_runs(&recorded[..2]).is_empty());
    }

    #[test]
    fn each_run_is_built_with_the_feature_its_workloads_need() {
        let scenarios =
            parse_matrix("1 | rust | --cycles 1\n2 | python | --workload producer:rust --workload consumer:python\n")
                .unwrap();
        let plan = plan_runs(&scenarios, &options(&["plaintext"], &[100])).unwrap();
        let binaries: TestBinaries = [
            ("integration-tests", PathBuf::from("/bin/chaos-integration")),
            ("multilanguage-tests", PathBuf::from("/bin/chaos-multilanguage")),
        ]
        .into_iter()
        .collect();
        let programs: Vec<String> = plan
            .iter()
            .map(|p| {
                runner_command(p, &binaries)
                    .unwrap()
                    .get_program()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(programs, ["/bin/chaos-integration", "/bin/chaos-multilanguage"]);
        let command = runner_command(&plan[0], &binaries).unwrap();
        let args: Vec<String> = command.get_args().map(|a| a.to_string_lossy().into_owned()).collect();
        assert_eq!(args, RUNNER_ARGS);
        assert_eq!(command.get_current_dir(), Some(package_root()));
        // A binary that was not built is an error, not a fallback.
        let partial: TestBinaries = [("integration-tests", PathBuf::from("/bin/x"))].into_iter().collect();
        assert_eq!(
            runner_command(&plan[1], &partial).unwrap_err().to_string(),
            "no chaos test binary was built with --features multilanguage-tests"
        );
    }

    #[test]
    fn the_grace_wait_covers_the_whole_process_group() {
        // The shell exits at once, like `cargo` on SIGINT, while its
        // background child — the test binary's stand-in, which also ignores
        // SIGINT — lives on in the same process group.
        let mut child = Command::new("sh")
            .args(["-c", "sleep 30 &"])
            .stdout(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let pgid = child.id();
        child.wait().unwrap();
        assert!(group_running(&mut child, pgid).unwrap(), "the group outlives its leader");
        signal_group(pgid, "KILL");
        let deadline = Instant::now() + Duration::from_secs(10);
        while group_alive(pgid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!group_running(&mut child, pgid).unwrap());
    }

    #[test]
    fn reports_resolve_against_the_package_root_and_move_by_copy_too() {
        assert!(package_root().join("tests/chaos").is_dir());
        let root = std::env::temp_dir().join(format!("chaos-matrix-move-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let source = root.join("reports");
        fs::create_dir_all(source.join("clients")).unwrap();
        fs::write(source.join("verdict.txt"), "PASS").unwrap();
        fs::write(source.join("clients/consumer.log"), "log").unwrap();
        // The fallback path taken when a rename crosses filesystems.
        copy_dir(&source, &root.join("copied")).unwrap();
        assert_eq!(fs::read_to_string(root.join("copied/clients/consumer.log")).unwrap(), "log");
        move_dir(&source, &root.join("moved")).unwrap();
        assert!(!source.exists());
        assert_eq!(fs::read_to_string(root.join("moved/verdict.txt")).unwrap(), "PASS");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn kept_attempt_directories_are_never_reused() {
        let root = std::env::temp_dir().join(format!("chaos-matrix-unused-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let wanted = root.join("15-plaintext-1MiB.attempt-1");
        assert_eq!(unused_path(&wanted), wanted);
        fs::create_dir(&wanted).unwrap();
        assert_eq!(unused_path(&wanted), root.join("15-plaintext-1MiB.attempt-1-2"));
        fs::create_dir(root.join("15-plaintext-1MiB.attempt-1-2")).unwrap();
        assert_eq!(unused_path(&wanted), root.join("15-plaintext-1MiB.attempt-1-3"));
        fs::remove_dir_all(&root).unwrap();
    }
}
