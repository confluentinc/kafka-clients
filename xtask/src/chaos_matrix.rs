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
//! `cargo xtask chaos` does. Runs are strictly sequential: every run owns a
//! dedicated Docker cluster, and two at once would compete for the host.
//!
//! Everything lands under the output directory (default
//! `target/chaos-matrix/<matrix name>/`):
//!
//! - `runs/<run id>/run.log` — the run's complete stdout + stderr.
//! - `runs/<run id>/command.txt` — the equivalent `cargo xtask chaos` command.
//! - `runs/<run id>/reports/` — the harness's report directory (verdict,
//!   leader changes, per-client logs, signature summary), moved here.
//! - `results.tsv` — one line per finished run (machine-readable).
//! - `summary.md` — the human-readable summary, rewritten after every run.
//! - `matrix.log` — timestamped progress.
//!
//! A re-invocation with the same output directory resumes: runs already in
//! `results.tsv` are skipped (`--rerun-failed` re-runs the ones that did not
//! pass, keeping the previous attempt's directory).
//!
//! Matrix file format: one scenario per line, `ID | description | flags`;
//! blank lines and lines starting with `#` are ignored.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context as _};

pub mod status;

/// `cargo` arguments that build and select the chaos runner test, built with
/// `feature` (see `crate::chaos_test_feature`).
fn test_args(feature: &str) -> [&str; 5] {
    ["test", "--features", feature, "--test", "chaos"]
}
/// libtest arguments selecting the flag-driven runner.
const RUNNER_ARGS: &[&str] = &["--", "--ignored", "--nocapture", "--exact", "run_test::chaos_run"];

/// Flags the runner sets for every run; a scenario line must not set them.
const DIMENSION_FLAGS: &[&str] = &["--security-protocol", "--msg-size", "--rps", "--reports"];

/// Security protocols the harness accepts for `--security-protocol`.
const PROTOCOLS: &[&str] = &["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"];

/// How long a timed-out run gets to tear its cluster down after SIGINT
/// before its process group is killed.
const INTERRUPT_GRACE: Duration = Duration::from_secs(180);
/// Grace after SIGINT for a run that stopped making progress. A wedged test
/// process usually no longer polls its Ctrl-C handler either, so waiting the
/// full interrupt grace would only delay the next run.
const STALL_GRACE: Duration = Duration::from_secs(60);
/// How long to sample a stalled test process's stacks before it is signalled,
/// so a wedge leaves evidence of where it was stuck.
const STALL_SAMPLE_SECS: &str = "5";
/// The harness's notice when it had to force-exit a process that passed but
/// did not terminate after teardown; libtest's summary never prints then.
const FORCED_EXIT_AFTER_PASS: &str = "chaos: FORCED EXIT after a PASS verdict";

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

/// Result columns, in `results.tsv` order.
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
];

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
    /// A run whose log has not grown for this long is stopped as STALLED.
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
}

impl Planned {
    fn run_id(&self) -> String {
        format!("{}-{}-{}", self.scenario.id, self.protocol, size_label(self.msg_size))
    }

    /// The scenario's flags plus the per-run dimensions.
    fn chaos_args(&self, rps: u32) -> Vec<String> {
        let mut args = self.scenario.args.clone();
        args.extend([
            "--security-protocol".to_string(),
            self.protocol.clone(),
            "--msg-size".to_string(),
            self.msg_size.to_string(),
            "--rps".to_string(),
            rps.to_string(),
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

/// Entry point for `cargo xtask chaos-matrix`.
pub fn run(raw: &[String]) -> anyhow::Result<()> {
    let options = parse_options(raw)?;
    let text = fs::read_to_string(&options.matrix)
        .with_context(|| format!("reading matrix file {}", options.matrix.display()))?;
    let scenarios = parse_matrix(&text)?;
    let plan = plan_runs(&scenarios, &options)?;
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
        crate::parse_chaos_flags(&planned.chaos_args(options.rps))
            .with_context(|| format!("scenario {} has invalid flags", planned.scenario.id))?;
    }

    // Build once up front: a compile error should stop the matrix, not be
    // recorded as a failure of every run, and build time should not count
    // towards the first run's duration. Once per feature the plan's runs
    // need, as `cargo xtask chaos` would pick it for each.
    let mut features = BTreeSet::new();
    for planned in &plan {
        features.insert(crate::chaos_test_feature(&crate::parse_chaos_flags(
            &planned.chaos_args(options.rps),
        )?));
    }
    for feature in features {
        println!("chaos-matrix: building the chaos test binary (--features {feature})");
        let status = Command::new("cargo")
            .args(test_args(feature))
            .arg("--no-run")
            .status()
            .context("running cargo")?;
        if !status.success() {
            bail!("building the chaos test binary (--features {feature}) failed");
        }
    }
    println!("chaos-matrix: checking the configuration of all {} run(s)", plan.len());
    check_configurations(&plan, options.rps)?;
    if options.check_only {
        println!("chaos-matrix: all {} run configuration(s) accepted", plan.len());
        return Ok(());
    }

    fs::create_dir_all(options.out.join("runs"))?;
    fs::copy(&options.matrix, options.out.join("matrix.txt"))?;
    // Lets `chaos-matrix-status` tell whether a runner is working on this
    // output directory, whatever order its flags were given in.
    fs::write(options.out.join(status::PID_FILE), format!("{}\n", std::process::id()))?;
    // The run order, for `chaos-matrix-status` (it cannot rebuild `--only`).
    let plan_ids: Vec<String> = plan.iter().map(Planned::run_id).collect();
    fs::write(options.out.join("plan.txt"), plan_ids.join("\n") + "\n")?;
    let results_path = options.out.join("results.tsv");
    let mut results = load_results(&results_path)?;
    let mut log = MatrixLog::open(&options.out.join("matrix.log"))?;
    let environment = describe_environment(&options);
    // Appended, not overwritten: a resumed matrix records each session's
    // commit and host state, since runs may span both.
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(options.out.join("environment.txt"))?
        .write_all(format!("{environment}---\n").as_bytes())?;
    log.line(&format!(
        "matrix {} — {} scenario(s), up to {} protocol(s), {} size(s): {} run(s); {} already recorded",
        options.matrix.display(),
        scenarios.len(),
        options.protocols.len(),
        options.msg_sizes.len(),
        plan.len(),
        plan.iter().filter(|p| results.contains_key(&p.run_id())).count()
    ));

    let total = plan.len();
    for (n, planned) in plan.iter().enumerate() {
        let run_id = planned.run_id();
        if let Some(previous) = results.get(&run_id) {
            let rerun_requested = options.rerun.contains(&run_id);
            if !rerun_requested && (previous.outcome == "PASS" || !options.rerun_failed) {
                continue;
            }
        }
        // `touch <out>/STOP` ends the matrix cleanly between runs; re-running
        // the same command resumes from here.
        let stop_file = options.out.join("STOP");
        if stop_file.exists() {
            let _ = fs::remove_file(&stop_file);
            log.line(&format!(
                "stop requested; {run_id} and later runs not started — re-run to resume"
            ));
            break;
        }
        let leftovers = harness_broker_containers();
        if !leftovers.is_empty() {
            log.line(&format!(
                "WARNING: broker containers from an earlier cluster are still present ({}); they compete \
                 with this run for the host",
                leftovers.join(", ")
            ));
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
        let mut result = loop {
            let mut result = execute(planned, &options, &mut log)?;
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
                    break result;
                },
                _ => break result,
            }
        };
        result.columns.insert("attempts".to_string(), attempt.to_string());
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
        fs::write(
            options.out.join("summary.md"),
            render_summary(&options, &environment, &scenarios, &plan, &results),
        )?;
    }
    fs::write(
        options.out.join("summary.md"),
        render_summary(&options, &environment, &scenarios, &plan, &results),
    )?;
    let recorded: Vec<(String, &RunResult)> = plan
        .iter()
        .filter_map(|p| results.get(&p.run_id()).map(|r| (p.run_id(), r)))
        .collect();
    let passed = recorded.iter().filter(|(_, r)| r.outcome == "PASS").count();
    let known = recorded.iter().filter(|(_, r)| r.outcome == KNOWN_DEFECT).count();
    log.line(&format!(
        "matrix finished: {passed}/{total} passed, {known} known-defect; summary in {}",
        options.out.join("summary.md").display()
    ));
    let failed = failed_runs(&recorded);
    if !failed.is_empty() {
        bail!(
            "chaos matrix: {} of {total} run(s) did not pass: {}",
            failed.len(),
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
    // The harness's longest silent phases (a 300 s drain, a 120 s broker
    // wait, a 120 s gap between cycles) stay far below this.
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
        match flag {
            "--matrix" => matrix = Some(PathBuf::from(value)),
            "--out" => out = Some(PathBuf::from(value)),
            "--protocols" => protocols = split_list(value),
            "--msg-sizes" => {
                msg_sizes = split_list(value)
                    .iter()
                    .map(|s| s.parse().with_context(|| format!("--msg-sizes: '{s}' is not a byte count")))
                    .collect::<anyhow::Result<_>>()?;
            },
            "--rps" => rps = value.parse().context("--rps must be a number")?,
            "--only" => only = Some(split_list(value).into_iter().collect()),
            "--rerun" => rerun = split_list(value).into_iter().collect(),
            "--run-timeout-min" => {
                let minutes: u64 = value.parse().context("--run-timeout-min must be a number")?;
                run_timeout = Duration::from_secs(minutes * 60);
            },
            "--stall-min" => {
                let minutes: u64 = value.parse().context("--stall-min must be a number")?;
                if minutes == 0 {
                    bail!("--stall-min must be >= 1");
                }
                stall_timeout = Duration::from_secs(minutes * 60);
            },
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

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
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
                let list = split_list(list);
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
        if let Some(flag) = args.iter().find(|a| DIMENSION_FLAGS.contains(&a.as_str())) {
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
                plan.push(Planned { scenario: scenario.clone(), protocol: protocol.clone(), msg_size });
            }
        }
    }
    Ok(plan)
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
        // Rows written before the trailing `attempts` column existed have one
        // cell fewer; they read as a single attempt.
        if cells.len() != COLUMNS.len() && cells.len() + 1 != COLUMNS.len() {
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

fn load_results(path: &Path) -> anyhow::Result<BTreeMap<String, RunResult>> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(BTreeMap::new());
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

/// The `cargo test` invocation for one run, with exactly that run's `CHAOS_*`
/// environment. Shared by the configuration check and the real run, so the
/// check validates precisely what will run.
fn runner_command(planned: &Planned, rps: u32) -> anyhow::Result<Command> {
    let env = crate::parse_chaos_flags(&planned.chaos_args(rps))?;
    let mut command = Command::new("cargo");
    command.args(test_args(crate::chaos_test_feature(&env))).args(RUNNER_ARGS);
    // Only this run's settings: a `CHAOS_*` left in the invoking shell would
    // otherwise leak into every run.
    for (key, _) in std::env::vars().filter(|(k, _)| k.starts_with("CHAOS_")) {
        command.env_remove(key);
    }
    command.envs(env);
    Ok(command)
}

/// Have the harness validate every planned run's configuration (it returns
/// right after parsing it), so a line the harness rejects stops the matrix
/// before the first cluster starts.
fn check_configurations(plan: &[Planned], rps: u32) -> anyhow::Result<()> {
    let mut rejected = Vec::new();
    for planned in plan {
        let output = runner_command(planned, rps)?
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

/// Run one planned run to completion and collect its result.
fn execute(planned: &Planned, options: &Options, log: &mut MatrixLog) -> anyhow::Result<RunResult> {
    let run_id = planned.run_id();
    let dir = options.out.join("runs").join(&run_id);
    if dir.exists() {
        // A re-run (or a run the runner was stopped in the middle of): keep
        // the earlier attempt rather than mixing two runs' logs.
        let kept = unused_path(&options.out.join("runs").join(format!("{run_id}.prev-{}", unix_secs())));
        fs::rename(&dir, &kept)?;
    }
    fs::create_dir_all(&dir)?;
    let args = planned.chaos_args(options.rps);
    fs::write(dir.join("command.txt"), format!("cargo xtask chaos {}\n", args.join(" ")))?;

    let log_path = dir.join("run.log");
    let log_file = File::create(&log_path)?;
    let mut command = runner_command(planned, options.rps)?;
    command
        .stdin(Stdio::null())
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        // Its own process group, so a timeout can signal the test binary and
        // not just the `cargo` wrapper in front of it.
        .process_group(0);

    // Harness clusters already present before this run; anything new after it
    // ends belongs to this run.
    let containers_before: BTreeSet<String> = harness_broker_containers().into_iter().collect();

    let started = now_utc();
    let clock = Instant::now();
    let mut child = command.spawn().context("spawning cargo test")?;
    let pid = child.id();
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
        let cause = if clock.elapsed() >= options.run_timeout {
            Some(Ended::TimedOut)
        } else if last_progress.elapsed() >= options.stall_timeout {
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
            signal_group(pid, "INT");
            let grace = if cause == Ended::Stalled {
                STALL_GRACE
            } else {
                INTERRUPT_GRACE
            };
            // Wait for the whole group, not just `child`: `cargo` has no
            // SIGINT handler and dies at once, while the test binary it ran
            // keeps tearing down (and must still be killed if it hangs).
            let deadline = Instant::now() + grace;
            while Instant::now() < deadline && group_running(&mut child, pid)? {
                std::thread::sleep(Duration::from_secs(1));
            }
            if group_running(&mut child, pid)? {
                signal_group(pid, "KILL");
                let _ = child.wait();
                // The orphaned test binary is reaped by init, not by us.
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline && group_alive(pid) {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    let duration_s = clock.elapsed().as_secs();

    // A killed run never reaches the harness's teardown, and a later run must
    // not share the host with its brokers: remove exactly the containers (and
    // their network) this run created, never anything that predates it.
    let leaked: Vec<String> = harness_broker_containers()
        .into_iter()
        .filter(|name| !containers_before.contains(name))
        .collect();
    if !leaked.is_empty() {
        log.line(&format!(
            "{run_id}: removing the cluster it left behind ({})",
            leaked.join(", ")
        ));
        remove_cluster(&leaked);
    }

    let text = fs::read_to_string(&log_path).unwrap_or_default();
    let mut parsed = parse_run_log(&text, ended);
    if ended == Ended::Stalled {
        parsed.reasons.insert(
            0,
            format!(
                "no output for {} min: the test process stopped making progress and was killed",
                options.stall_timeout.as_secs() / 60
            ),
        );
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
    Ok(RunResult {
        outcome: parsed.outcome,
        duration_s,
        reasons: parsed.reasons.join(" / "),
        columns,
    })
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
}

fn signal_group(pid: u32, signal: &str) {
    let _ = Command::new("kill").args(["-s", signal, "--", &format!("-{pid}")]).status();
}

/// Whether `child` (the leader of process group `pgid`) or any other member
/// of its group is still running. Reaps `child` once it has exited, since its
/// zombie would otherwise keep the group looking alive.
fn group_running(child: &mut Child, pgid: u32) -> std::io::Result<bool> {
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

/// Every harness broker container (`kafka-<node>-<suffix>`), running or not.
fn harness_broker_containers() -> Vec<String> {
    let Ok(output) = Command::new("docker").args(["ps", "-a", "--format", "{{.Names}}"]).output() else {
        return Vec::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|name| is_harness_broker_name(name))
        .map(str::to_string)
        .collect()
}

/// Remove the given harness broker containers with their volumes, and the
/// `kafka-net-<suffix>` network of each cluster they belong to.
fn remove_cluster(containers: &[String]) {
    let _ = Command::new("docker").args(["rm", "-f", "-v"]).args(containers).output();
    let networks: BTreeSet<String> = containers
        .iter()
        .filter_map(|name| name.splitn(3, '-').nth(2))
        .map(|suffix| format!("kafka-net-{suffix}"))
        .collect();
    for network in networks {
        let _ = Command::new("docker").args(["network", "rm", &network]).output();
    }
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
    let kafka_tag = fs::read_to_string("tests/common/kafka_cluster.rs")
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

fn render_summary(
    options: &Options,
    environment: &str,
    scenarios: &[Scenario],
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

    let recorded: Vec<&RunResult> = plan.iter().filter_map(|p| results.get(&p.run_id())).collect();
    let passed = recorded.iter().filter(|r| r.outcome == "PASS").count();
    out.push_str(&format!(
        "**{} of {} runs finished: {passed} passed, {} did not pass, {} not run yet.**\n\n",
        recorded.len(),
        plan.len(),
        recorded.len() - passed,
        plan.len() - recorded.len()
    ));

    if scenarios.iter().any(|s| s.protocols.is_some()) {
        out.push_str(
            "A dash (—) marks a protocol the matrix file does not run that scenario with (its protocols \
             column).\n\n",
        );
    }
    // One grid per message size: scenario rows, protocol columns.
    let in_plan: Vec<&Scenario> = scenarios
        .iter()
        .filter(|s| plan.iter().any(|p| p.scenario.id == s.id))
        .collect();
    for &msg_size in &options.msg_sizes {
        out.push_str(&format!(
            "## Message size {} ({} bytes)\n\n| # | Scenario |",
            size_label(msg_size),
            msg_size
        ));
        for protocol in &options.protocols {
            out.push_str(&format!(" {protocol} |"));
        }
        out.push_str("\n|---|---|");
        for _ in &options.protocols {
            out.push_str("---|");
        }
        out.push('\n');
        for scenario in &in_plan {
            out.push_str(&format!("| {} | {} |", scenario.id, scenario.description));
            for protocol in &options.protocols {
                let planned = Planned { scenario: (*scenario).clone(), protocol: protocol.clone(), msg_size };
                let cell = match results.get(&planned.run_id()) {
                    _ if !scenario.runs_with(protocol) => "—".to_string(),
                    Some(r) => r.outcome.clone(),
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
        "Attempts above 1 are retries after the known multi-topic recreate defect; the metrics are the last \
         attempt's, and earlier attempts' logs are kept in `runs/<run>.attempt-<n>/`. KNOWN-DEFECT means \
         every attempt hit that defect's exact signature and nothing else failed.\n\n",
    );
    out.push_str(
        "| Run | Outcome | Attempts | Duration | Produce rate (rec/s) | MiB/s | Delivered | Lost | Failed sends | \
         Committed checks | Ordering | Rebalance callbacks |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for planned in plan {
        let run_id = planned.run_id();
        if let Some(r) = results.get(&run_id) {
            let attempts = match r.get("attempts") {
                "" => "1",
                n => n,
            };
            out.push_str(&format!(
                "| {run_id} | {} | {attempts} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                r.outcome,
                format_duration(r.duration_s),
                r.get("produce_rate"),
                r.get("produce_mib_s"),
                r.get("delivered"),
                r.get("lost"),
                r.get("failed_sends"),
                r.get("commit_checks"),
                r.get("ordering"),
                r.get("rebalance"),
            ));
        }
    }
    out.push('\n');

    let failures: Vec<(String, &RunResult)> = plan
        .iter()
        .filter_map(|p| results.get(&p.run_id()).map(|r| (p.run_id(), r)))
        .filter(|(_, r)| r.outcome != "PASS")
        .collect();
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
        println!("{line}");
        let _ = writeln!(self.file, "{line}");
    }
}

/// The root of the package the chaos test belongs to, which `cargo test` makes
/// the test binary's working directory: the workspace root, xtask's parent.
fn package_root() -> &'static Path {
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
            plan[4].chaos_args(1000).join(" "),
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
    fn results_written_before_the_attempts_column_still_load() {
        let current = RunResult { outcome: "PASS".into(), ..RunResult::default() }.to_tsv();
        let legacy = current.rsplit_once('\t').unwrap().0;
        let (_, back) = RunResult::from_tsv(legacy).expect("a row without the attempts cell loads");
        assert_eq!(back.outcome, "PASS");
        assert_eq!(back.get("attempts"), "");
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
    fn the_rust_client_matrix_ci_runs_is_well_formed() {
        let text = fs::read_to_string(package_root().join("tests/chaos/matrix/rust-client.txt")).unwrap();
        let scenarios = parse_matrix(&text).unwrap();
        let plan = plan_runs(&scenarios, &options(&["plaintext", "ssl", "sasl_ssl"], &[100])).unwrap();
        for planned in &plan {
            crate::parse_chaos_flags(&planned.chaos_args(1000)).unwrap_or_else(|e| panic!("{}: {e}", planned.run_id()));
        }
        // Topic recreation with more than one topic needs the explicit opt-in,
        // or the harness rejects the run's configuration.
        for scenario in &scenarios {
            let has = |flag: &str| scenario.args.iter().any(|a| a == flag);
            let topics = scenario
                .args
                .windows(2)
                .find(|w| w[0] == "--num-topics")
                .map_or(1, |w| w[1].parse::<u32>().unwrap());
            if has("--topic-recreate") && topics > 1 {
                assert!(has("--allow-multi-topic-recreate"), "scenario {}", scenario.id);
            }
        }
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
        let features: Vec<String> = plan
            .iter()
            .map(|p| {
                let command = runner_command(p, 1000).unwrap();
                let args: Vec<String> = command.get_args().map(|a| a.to_string_lossy().into_owned()).collect();
                args[args.iter().position(|a| a == "--features").unwrap() + 1].clone()
            })
            .collect();
        assert_eq!(features, ["integration-tests", "multilanguage-tests"]);
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
