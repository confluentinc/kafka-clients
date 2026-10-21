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

//! `report.md` and `junit.xml` of a `perf-cloud run`.
//!
//! The markdown opens with the results and ends with the run's parameters, as
//! a list. A run of one test (as CI runs) lists the test's settings there
//! too; a run of several lists each test's settings under its results. Each
//! test has a table with a row per metric, a column per client that ran and,
//! after the clients, a Δ column per comparison giving a client's change
//! against its librdkafka baseline: in the `rust` group Rust and Java each against librdkafka, in
//! the `python` group each Rust-based client against the librdkafka-based one
//! of its mode. Never Rust against Java. Every change is banded, with no
//! legend:
//!
//! - 🟢 better, or at most 10 % worse;
//! - 🔴 more than 10 % worse.
//!
//! "Worse" follows the metric: lower throughput or transactions/s, higher
//! latency, CPU or RSS. Counts (messages, transactions) get no Δ. A Δ is
//! `n/a` unless both clients passed: a failed arm's numbers (from a partial
//! run) are shown in its column, with a note, but never compared. A
//! throughput Δ is `cluster-bound` when either client's ingress came near the
//! cluster's limit, which then capped it rather than the client. The notes
//! under a table name the clients that failed or did not run, and those that
//! missed a fixed rate.
//!
//! While a test runs, the report so far ([`Live`], written by
//! [`super::live`]) opens with when it was written. Its running client shows
//! the numbers it has measured so far that need no percentiles (throughput,
//! average and maximum latency, CPU, RSS, records), its other rows `–`; the
//! clients yet to start are noted, and a Δ either of whose clients has not
//! finished is `pending` (`n/a` at once when the other failed).

use std::fmt::Write as _;

use super::live::{SoFar, INTERVAL};
use super::{Arm, ArmResult, Group, Metrics, Outcome, PerfTest, RunInfo, TestResult, Workload};

/// A report written while the last of its tests runs.
pub(crate) struct Live {
    /// When, as [`crate::chaos_matrix::now_utc`] gives it.
    pub(crate) updated: String,
    /// The test's client running now, if any.
    pub(crate) running: Option<Running>,
}

/// A client running now, with what it measured so far (`None` before its
/// measurement started).
pub(crate) struct Running {
    pub(crate) arm: Arm,
    pub(crate) so_far: Option<SoFar>,
}

/// Above this share of the cluster's ingress limit a result may be capped by
/// the cluster rather than by the client.
const NEAR_INGRESS_SHARE: f64 = 0.85;

/// Below this share of a fixed-rate test's target a client did not hold the
/// rate, so its latency is not comparable with one that did.
const HELD_RATE_SHARE: f64 = 0.95;

const MIB: f64 = 1024.0 * 1024.0;

/// A Δ column: `candidate`'s change against `baseline`, under `header`.
pub(crate) struct Comparison {
    pub(crate) candidate: Arm,
    pub(crate) baseline: Arm,
    header: &'static str,
}

/// The comparisons of a group, in column order. The baseline is always a
/// librdkafka-based client.
pub(crate) fn comparisons(group: Group) -> &'static [Comparison] {
    const fn c(candidate: Arm, baseline: Arm, header: &'static str) -> Comparison {
        Comparison { candidate, baseline, header }
    }
    const RUST: &[Comparison] = &[
        c(Arm::Rust, Arm::Librdkafka, "Δ Rust vs librdkafka"),
        c(Arm::Java, Arm::Librdkafka, "Δ Java vs librdkafka"),
    ];
    const PYTHON: &[Comparison] = &[
        c(Arm::PythonSyncRust, Arm::PythonSyncLibrdkafka, "Δ sync"),
        c(Arm::PythonAsyncRust, Arm::PythonAsyncLibrdkafka, "Δ async"),
    ];
    match group {
        Group::Rust => RUST,
        Group::Python => PYTHON,
    }
}

/// A group's report title.
fn title(group: Group) -> &'static str {
    match group {
        Group::Rust => "Rust client performance results",
        Group::Python => "Python client performance results",
    }
}

/// Which way a metric improves.
#[derive(Clone, Copy)]
enum Better {
    Higher,
    Lower,
}

/// A report row: its label, how to read it from [`Metrics`] (`None` when
/// the client did not report it), its decimals, which way is better (`None`
/// for counts, which get no Δ), and whether only transactional tests have it.
struct Row {
    label: &'static str,
    value: fn(&Metrics) -> Option<f64>,
    decimals: usize,
    better: Option<Better>,
    transactional: bool,
    /// A rate the cluster's ingress limit can cap.
    throughput: bool,
    /// How to read it from a running client's numbers so far; `None` for the
    /// rows only a finished client has.
    so_far: Option<fn(&SoFar) -> Option<f64>>,
}

impl Row {
    /// The row, shown for a running client too.
    const fn so_far(self, value: fn(&SoFar) -> Option<f64>) -> Row {
        Row { so_far: Some(value), ..self }
    }
}

const fn row(
    label: &'static str,
    value: fn(&Metrics) -> Option<f64>,
    decimals: usize,
    better: Option<Better>,
    transactional: bool,
) -> Row {
    Row { label, value, decimals, better, transactional, throughput: false, so_far: None }
}

const fn rate_row(
    label: &'static str,
    value: fn(&Metrics) -> Option<f64>,
    decimals: usize,
    transactional: bool,
) -> Row {
    Row {
        label,
        value,
        decimals,
        better: Some(Better::Higher),
        transactional,
        throughput: true,
        so_far: None,
    }
}

const ROWS: &[Row] = &[
    rate_row("Throughput (msg/s)", |m| Some(m.msg_s), 0, false).so_far(SoFar::msg_s),
    rate_row("Throughput (MiB/s)", |m| Some(m.mib_s), 2, false).so_far(SoFar::mib_s),
    rate_row("Transactions/s", |m| m.txn.as_ref().map(|t| t.per_s), 2, true),
    row("Latency avg (ms)", |m| Some(m.avg), 2, Some(Better::Lower), false).so_far(|s| s.latency_avg),
    row("Latency p50 (ms)", |m| Some(m.p50), 2, Some(Better::Lower), false),
    row("Latency p90 (ms)", |m| Some(m.p90), 2, Some(Better::Lower), false),
    row("Latency p95 (ms)", |m| Some(m.p95), 2, Some(Better::Lower), false),
    row("Latency p99 (ms)", |m| Some(m.p99), 2, Some(Better::Lower), false),
    row("Latency p99.9 (ms)", |m| Some(m.p999), 2, Some(Better::Lower), false),
    row("Latency max (ms)", |m| Some(m.max), 2, Some(Better::Lower), false).so_far(|s| s.latency_max),
    row(
        "Commit latency p50 (ms)",
        |m| m.txn.as_ref().map(|t| t.commit_p50),
        2,
        Some(Better::Lower),
        true,
    ),
    row(
        "Commit latency p99 (ms)",
        |m| m.txn.as_ref().map(|t| t.commit_p99),
        2,
        Some(Better::Lower),
        true,
    ),
    row("CPU (%)", |m| Some(m.cpu_pct), 1, Some(Better::Lower), false).so_far(|s| s.cpu_pct),
    row("RSS (MiB)", |m| Some(m.rss_mib), 1, Some(Better::Lower), false).so_far(|s| s.rss_mib),
    row("Messages measured", |m| Some(m.messages), 0, None, false).so_far(|s| Some(s.messages)),
    row("Committed transactions", |m| m.txn.as_ref().map(|t| t.committed), 0, None, true),
    row("Aborted transactions", |m| m.txn.as_ref().map(|t| t.aborted), 0, None, true),
];

/// A column of a test's table after the metric names.
enum Column {
    Client(Arm),
    Delta(&'static Comparison),
}

/// The run's clients in order, then a Δ column per comparison whose clients
/// both ran.
fn columns(info: &RunInfo) -> Vec<Column> {
    let mut columns: Vec<Column> = info.arms.iter().map(|&arm| Column::Client(arm)).collect();
    for comparison in comparisons(info.group) {
        if info.arms.contains(&comparison.candidate) && info.arms.contains(&comparison.baseline) {
            columns.push(Column::Delta(comparison));
        }
    }
    columns
}

/// The optional settings a test may set, after its workload, partitions,
/// value size and rate: `(key, name in a scenario, name in a parameter
/// table, unit)`.
const SETTING_NAMES: &[(&str, &str, &str, &str)] = &[
    ("KEY_SIZE", "key", "Key size", " B"),
    ("ACKS", "acks", "acks", ""),
    ("COMPRESSION_TYPE", "compression", "Compression", ""),
    ("BATCH_SIZE", "batch", "Batch size", " KiB"),
    ("LINGER_MS", "linger", "Linger", " ms"),
    ("ENABLE_IDEMPOTENCE", "idempotence", "Idempotence", ""),
    ("MAX_IN_FLIGHT", "in-flight", "Max in-flight requests", ""),
    ("BUFFER_MEMORY", "buffer", "Buffer memory", " MiB"),
    ("MAX_REQUEST_SIZE", "max request", "Max request size", " KiB"),
    (
        "RECORDS_PER_TRANSACTION",
        "records per transaction",
        "Records per transaction",
        "",
    ),
    ("ABORT_RATE", "abort rate", "Abort rate", ""),
    (
        "NUM_TRANSACTIONAL_PRODUCERS",
        "transactional producers",
        "Transactional producers",
        "",
    ),
    ("FETCH_MIN_BYTES", "fetch.min.bytes", "fetch.min.bytes", " B"),
    ("FETCH_MAX_WAIT_MS", "fetch.max.wait.ms", "fetch.max.wait.ms", " ms"),
    (
        "MAX_PARTITION_FETCH_BYTES",
        "max.partition.fetch.bytes",
        "max.partition.fetch.bytes",
        " B",
    ),
    ("MAX_POLL_RECORDS", "max.poll.records", "max.poll.records", ""),
];

/// A one-line description of a test's settings.
pub(crate) fn scenario(test: &PerfTest) -> String {
    let workload = match test.workload {
        Workload::Producer => "producer".to_string(),
        Workload::Transactional => format!("transactional ({})", test.setting("TXN_MODE").unwrap_or("produce")),
        Workload::Consumer => "consumer".to_string(),
    };
    let mut parts = vec![
        workload,
        format!("{} partitions", test.setting("PARTITIONS").unwrap_or("?")),
        format!("{} B values", test.setting("VALUE_SIZE").unwrap_or("?")),
    ];
    match test.setting("LIMIT_RPS") {
        Some("0") | None => parts.push("unbounded rate".to_string()),
        Some(rps) => parts.push(format!("{rps} msg/s")),
    }
    for &(key, label, _, unit) in SETTING_NAMES {
        if let Some(v) = test.setting(key) {
            parts.push(format!("{label} {v}{unit}"));
        }
    }
    parts.join(", ")
}

/// A test's settings as parameter-table rows.
fn parameters(test: &PerfTest) -> Vec<(String, String)> {
    let workload = match test.workload {
        Workload::Producer => "Producer".to_string(),
        Workload::Transactional => match test.setting("TXN_MODE").unwrap_or("produce") {
            "eos" => "Transactional (EOS)".to_string(),
            mode => format!("Transactional ({mode})"),
        },
        Workload::Consumer => "Consumer".to_string(),
    };
    let mut rows = vec![("Workload".to_string(), workload)];
    let mut add = |name: &str, value: Option<String>| {
        if let Some(value) = value {
            rows.push((name.to_string(), value));
        }
    };
    add("Partitions", test.setting("PARTITIONS").map(grouped));
    add("Value size", test.setting("VALUE_SIZE").map(|v| format!("{} B", grouped(v))));
    add(
        "Rate",
        Some(match test.setting("LIMIT_RPS") {
            Some("0") | None => "Unbounded".to_string(),
            Some(rps) => format!("{} msg/s", grouped(rps)),
        }),
    );
    for &(key, _, name, unit) in SETTING_NAMES {
        add(name, test.setting(key).map(|v| format!("{}{unit}", grouped(v))));
    }
    rows
}

/// A whole number with a `,` between thousands; any other value unchanged.
fn grouped(value: &str) -> String {
    match value.parse::<u64>() {
        Ok(n) if value.bytes().all(|b| b.is_ascii_digit()) => number(n as f64, 0),
        _ => value.to_string(),
    }
}

/// `2026-10-15T15:57:31Z` → `2026-10-15 15:57:31 UTC`; anything else
/// unchanged.
fn start_time(started: &str) -> String {
    match started.strip_suffix('Z') {
        Some(time) => format!("{} UTC", time.replacen('T', " ", 1)),
        None => started.to_string(),
    }
}

/// `90` → `1 h 30 min`, `45` → `45 min`.
pub(crate) fn hours_minutes(minutes: u64) -> String {
    match (minutes / 60, minutes % 60) {
        (0, m) => format!("{m} min"),
        (h, 0) => format!("{h} h"),
        (h, m) => format!("{h} h {m} min"),
    }
}

/// `value` with `decimals` decimals and a `,` between thousands
/// (`1234567.891` → `1,234,567.89`).
fn number(value: f64, decimals: usize) -> String {
    let text = format!("{value:.decimals$}");
    if !value.is_finite() {
        return text;
    }
    let (sign, unsigned) = match text.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", text.as_str()),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    let mut out = String::from(sign);
    for (i, digit) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    if let Some(fraction) = fraction {
        out.push('.');
        out.push_str(fraction);
    }
    out
}

/// How much worse than its baseline a client can be and still be green.
const WORSE_TOLERANCE: f64 = 0.10;

/// The relative change of `candidate` against `baseline` and its band.
/// `None` when the baseline is zero (no meaningful ratio).
fn delta(candidate: f64, baseline: f64, better: Better) -> Option<(f64, &'static str)> {
    if baseline == 0.0 || !baseline.is_finite() || !candidate.is_finite() {
        return None;
    }
    let change = (candidate - baseline) / baseline;
    let worse = match better {
        Better::Higher => -change,
        Better::Lower => change,
    };
    let band = if worse <= WORSE_TOLERANCE { "🟢" } else { "🔴" };
    Some((change * 100.0, band))
}

/// Text safe inside a markdown table cell.
fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace(['\n', '\r'], " ")
}

fn find(arms: &[ArmResult], arm: Arm) -> Option<&ArmResult> {
    arms.iter().find(|a| a.arm == arm)
}

fn metrics_of(arms: &[ArmResult], arm: Arm) -> Option<&Metrics> {
    find(arms, arm).and_then(|a| a.metrics.as_ref())
}

/// The metrics of `arm` when it passed: the only ones a Δ compares.
fn passed_metrics_of(arms: &[ArmResult], arm: Arm) -> Option<&Metrics> {
    find(arms, arm)
        .filter(|a| a.outcome == Outcome::Passed)
        .and_then(|a| a.metrics.as_ref())
}

/// The MB/s a client sent to (or, in a consumer test, read from) the
/// cluster. A transactional test's throughput counts committed records only,
/// while its aborted records crossed the ingress too.
fn ingress_mb_s(m: &Metrics, abort_rate: f64) -> f64 {
    let committed = m.mib_s * MIB / 1e6;
    if abort_rate > 0.0 && abort_rate < 1.0 {
        committed / (1.0 - abort_rate)
    } else {
        committed
    }
}

/// The report of `results`; with `live`, the report so far, whose last test
/// is still running.
pub(crate) fn markdown(info: &RunInfo, results: &[TestResult], live: Option<&Live>) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# {}", title(info.group));
    if let Some(live) = live {
        let _ = writeln!(
            out,
            "\n> In progress, updated {} and every {} while a test runs. A running client shows its throughput, \
             average and maximum latency, CPU, RSS and messages so far; the full report replaces this one when the \
             test finishes.",
            start_time(&live.updated),
            hours_minutes(INTERVAL.as_secs() / 60)
        );
    }
    // A run of one test (as CI runs) lists the test's settings with the
    // run's; a run of several lists each test's under its results.
    let single = match info.tests.as_slice() {
        [test] => Some(test),
        _ => None,
    };
    let columns = columns(info);
    if results.is_empty() {
        let _ = writeln!(out, "\nNo test has finished yet.");
    }
    for (i, result) in results.iter().enumerate() {
        let live = live.filter(|_| i + 1 == results.len());
        match single {
            Some(_) => {
                let _ = writeln!(out, "\n## Results\n");
                results_table(&mut out, info, &columns, result, live);
            },
            None => {
                let _ = writeln!(out, "\n## {}\n", inline(&result.test.id));
                results_table(&mut out, info, &columns, result, live);
                let _ = writeln!(out, "\n### Parameters\n");
                parameter_list(&mut out, &parameters(&result.test));
            },
        }
    }

    let clients: Vec<&str> = info.arms.iter().map(|a| a.label()).collect();
    let mut rows = vec![
        ("Start time".to_string(), start_time(&info.started)),
        ("Clients".to_string(), clients.join(", ")),
    ];
    if let Some(test) = single {
        rows.extend(parameters(test));
    }
    rows.push(("Warmup".to_string(), format!("{} min per client", info.warmup_min)));
    rows.push(("Measurement".to_string(), format!("{} min per client", info.duration_min)));
    rows.extend(info.facts.iter().cloned());
    let _ = writeln!(out, "\n## Parameters\n");
    parameter_list(&mut out, &rows);
    out
}

/// `(parameter, value)` rows as a list.
fn parameter_list(out: &mut String, rows: &[(String, String)]) {
    for (name, value) in rows {
        let _ = writeln!(out, "- {}: {}", inline(name), inline(value));
    }
}

/// Text that stays on its line.
fn inline(text: &str) -> String {
    text.replace(['\n', '\r'], " ")
}

/// A test's results table and the notes on it; with `live`, the test is
/// still running.
fn results_table(out: &mut String, info: &RunInfo, columns: &[Column], result: &TestResult, live: Option<&Live>) {
    let running = live.and_then(|l| l.running.as_ref());
    let test = &result.test;
    let headers: Vec<&str> = columns
        .iter()
        .map(|c| match c {
            Column::Client(arm) => arm.label(),
            Column::Delta(comparison) => comparison.header,
        })
        .collect();
    let _ = writeln!(out, "| Metric | {} |", headers.join(" | "));
    let _ = writeln!(out, "|---|{}", "---:|".repeat(columns.len()));

    let transactional = test.workload == Workload::Transactional;
    let abort_rate: f64 = test.setting("ABORT_RATE").and_then(|v| v.parse().ok()).unwrap_or(0.0);
    // Whether `arm`'s ingress came near the cluster's limit.
    let near_ingress = |arm: Arm| match (metrics_of(&result.arms, arm), info.ingress_mb_s) {
        (Some(m), Some(limit)) => ingress_mb_s(m, abort_rate) >= NEAR_INGRESS_SHARE * limit,
        _ => false,
    };
    for row in ROWS.iter().filter(|r| transactional || !r.transactional) {
        let cells: Vec<String> = columns
            .iter()
            .map(|c| match c {
                Column::Client(arm) => {
                    let value = match running.filter(|r| r.arm == *arm) {
                        Some(r) => row.so_far.zip(r.so_far.as_ref()).and_then(|(value, s)| value(s)),
                        None => metrics_of(&result.arms, *arm).and_then(row.value),
                    };
                    match value {
                        Some(v) => number(v, row.decimals),
                        None => "–".to_string(),
                    }
                },
                Column::Delta(comparison) => {
                    let Some(better) = row.better else {
                        return String::new();
                    };
                    let (candidate, baseline) = (comparison.candidate, comparison.baseline);
                    // Until both clients finish, unless one that did cannot be
                    // compared.
                    let unfinished = |arm: Arm| find(&result.arms, arm).is_none();
                    let comparable = |arm: Arm| {
                        find(&result.arms, arm).is_none_or(|a| a.outcome == Outcome::Passed && a.metrics.is_some())
                    };
                    if live.is_some()
                        && (unfinished(candidate) || unfinished(baseline))
                        && comparable(candidate)
                        && comparable(baseline)
                    {
                        return "pending".to_string();
                    }
                    let pair = (
                        passed_metrics_of(&result.arms, candidate).and_then(row.value),
                        passed_metrics_of(&result.arms, baseline).and_then(row.value),
                    );
                    match pair {
                        (Some(_), Some(_)) if row.throughput && (near_ingress(candidate) || near_ingress(baseline)) => {
                            "cluster-bound".to_string()
                        },
                        (Some(c), Some(b)) => match delta(c, b, better) {
                            Some((pct, band)) => format!("{band} {pct:+.1}%"),
                            None => "n/a".to_string(),
                        },
                        _ => "n/a".to_string(),
                    }
                },
            })
            .collect();
        let _ = writeln!(out, "| {} | {} |", row.label, cells.join(" | "));
    }

    let mut notes = Vec::new();
    let consumer = test.workload == Workload::Consumer;
    // The committed rate a fixed-rate test aims at: its aborted transactions'
    // records are sent but not counted.
    let rate: f64 = test.setting("LIMIT_RPS").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let target = rate * (1.0 - abort_rate);
    for &arm in &info.arms {
        let label = arm.label();
        let Some(a) = find(&result.arms, arm) else {
            notes.push(match running.filter(|r| r.arm == arm) {
                Some(Running { so_far: Some(s), .. }) => {
                    let measured = match (s.seconds / 60.0) as u64 {
                        0 => "under 1 min".to_string(),
                        minutes => hours_minutes(minutes),
                    };
                    format!(
                        "{label} is running: {measured} of its {} measurement so far. Its other numbers come when \
                         it finishes.",
                        hours_minutes(info.duration_min)
                    )
                },
                Some(Running { so_far: None, .. }) => format!("{label} is running and has not started measuring yet."),
                None if live.is_some() => format!("{label} has not started yet."),
                None => format!("{label} did not run."),
            });
            continue;
        };
        if let Outcome::Failed(reason) = &a.outcome {
            let reason = cell(reason.trim_end_matches('.'));
            notes.push(match a.metrics {
                Some(_) => {
                    format!("{label} failed: {reason}. Its results are from a partial run and are not compared.")
                },
                None => format!("{label} failed: {reason}."),
            });
            continue;
        }
        let Some(m) = &a.metrics else {
            continue;
        };
        // A consumer reads what its load produced: when the load fell short,
        // so did the consumer, through no fault of its own.
        let short_load = a
            .load_msg_s
            .filter(|r| consumer && target > 0.0 && *r < HELD_RATE_SHARE * target);
        if let Some(load) = short_load {
            notes.push(format!(
                "The load producer for {label} reached {} msg/s, below the {} msg/s target. Its throughput and \
                 latency reflect the load, not the client.",
                number(load, 0),
                number(target, 0)
            ));
        } else if target > 0.0 && m.msg_s < HELD_RATE_SHARE * target {
            notes.push(format!(
                "{label} reached {} msg/s, below the {} msg/s target. Its latency is not comparable with that of \
                 clients that held the rate.",
                number(m.msg_s, 0),
                number(target, 0)
            ));
        }
    }
    if !notes.is_empty() {
        let _ = writeln!(out, "\n**Notes**\n");
        for note in notes {
            let _ = writeln!(out, "- {note}");
        }
    }
}

/// XML 1.0 text: escaped, without the control characters it cannot carry.
fn xml(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\t' | '\n' | '\r' => out.push(c),
            c if (c as u32) < 0x20 => {},
            c => out.push(c),
        }
    }
    out
}

/// One `<testsuite>` per test and one `<testcase>` per client, for
/// Semaphore's test-results view.
pub(crate) fn junit(results: &[TestResult]) -> String {
    let count = |pred: fn(&ArmResult) -> bool| results.iter().flat_map(|t| &t.arms).filter(|a| pred(a)).count();
    let total = count(|_| true);
    let failures = count(|a| matches!(a.outcome, Outcome::Failed(_)));
    // `fold` from 0.0: an empty `f64` sum is -0.0, which would print as `-0.0`.
    let seconds = results
        .iter()
        .flat_map(|t| &t.arms)
        .fold(0.0, |s, a| s + a.elapsed.as_secs_f64());
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<testsuites name=\"perf-cloud\" tests=\"{total}\" failures=\"{failures}\" errors=\"0\" time=\"{seconds:.1}\">"
    );
    for t in results {
        let failed = t.arms.iter().filter(|a| matches!(a.outcome, Outcome::Failed(_))).count();
        let time = t.arms.iter().fold(0.0, |s, a| s + a.elapsed.as_secs_f64());
        let _ = writeln!(
            out,
            "  <testsuite name=\"{}\" tests=\"{}\" failures=\"{failed}\" errors=\"0\" time=\"{time:.1}\">",
            xml(&t.test.id),
            t.arms.len()
        );
        for a in &t.arms {
            let _ = write!(
                out,
                "    <testcase classname=\"{}\" name=\"{}\" time=\"{:.1}\"",
                xml(&t.test.id),
                xml(a.arm.label()),
                a.elapsed.as_secs_f64()
            );
            let summary = a.metrics.as_ref().map(|m| {
                let mut s = format!(
                    "client={} messages={:.0} msg/s={:.0} MiB/s={:.2} p50={:.2}ms p99={:.2}ms p99.9={:.2}ms \
                     max={:.2}ms cpu={:.1}% rss={:.1}MiB",
                    m.client, m.messages, m.msg_s, m.mib_s, m.p50, m.p99, m.p999, m.max, m.cpu_pct, m.rss_mib
                );
                if let Some(t) = &m.txn {
                    let _ = write!(
                        s,
                        " txn/s={:.2} committed={:.0} aborted={:.0} commit_p50={:.2}ms commit_p99={:.2}ms",
                        t.per_s, t.committed, t.aborted, t.commit_p50, t.commit_p99
                    );
                }
                s
            });
            match (&a.outcome, summary) {
                (Outcome::Passed, None) => out.push_str("/>\n"),
                (outcome, summary) => {
                    out.push_str(">\n");
                    if let Outcome::Failed(reason) = outcome {
                        let _ = writeln!(out, "      <failure message=\"{0}\">{0}</failure>", xml(reason));
                    }
                    if let Some(s) = summary {
                        let _ = writeln!(out, "      <system-out>{}</system-out>", xml(&s));
                    }
                    out.push_str("    </testcase>\n");
                },
            }
        }
        out.push_str("  </testsuite>\n");
    }
    out.push_str("</testsuites>\n");
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::{parse_tests, TxnMetrics, SETTINGS};
    use super::*;

    #[test]
    fn names_every_optional_setting() {
        // Shown apart, in the scenario's and the parameters' first lines.
        const SHOWN_APART: &[&str] = &["PARTITIONS", "VALUE_SIZE", "LIMIT_RPS", "TXN_MODE"];
        let named: Vec<&str> = SETTING_NAMES.iter().map(|(key, ..)| *key).collect();
        for setting in SETTINGS {
            assert_eq!(
                named.contains(&setting.key),
                !SHOWN_APART.contains(&setting.key),
                "{}",
                setting.key
            );
        }
        for key in named {
            assert!(SETTINGS.iter().any(|s| s.key == key), "{key} is not a setting");
        }
    }

    fn metrics(client: &str, msg_s: f64, p99: f64) -> Metrics {
        Metrics {
            client: client.to_string(),
            msg_s,
            mib_s: msg_s * 1024.0 / MIB,
            p50: 2.0,
            p90: 3.0,
            p95: 4.0,
            p99,
            p999: 10.0,
            max: 20.0,
            avg: 2.5,
            cpu_pct: 50.0,
            rss_mib: 100.0,
            messages: msg_s * 60.0,
            txn: None,
        }
    }

    fn txn_metrics(client: &str, msg_s: f64, commit_p99: f64) -> Metrics {
        Metrics {
            txn: Some(TxnMetrics {
                per_s: msg_s / 100.0,
                committed: msg_s * 60.0 / 100.0,
                aborted: msg_s * 60.0 / 100.0,
                commit_p50: 20.0,
                commit_p99,
            }),
            ..metrics(client, msg_s, 30.0)
        }
    }

    fn arm(arm: Arm, outcome: Outcome, metrics: Option<Metrics>) -> ArmResult {
        ArmResult { arm, outcome, metrics, load_msg_s: None, elapsed: Duration::from_secs(90) }
    }

    /// The report of a run of `results`' tests.
    fn render(group: Group, arms: &[Arm], ingress: Option<f64>, results: &[TestResult]) -> String {
        render_live(group, arms, ingress, results, None)
    }

    /// The report of a run of `results`' tests; with `live`, the report so far.
    fn render_live(
        group: Group,
        arms: &[Arm],
        ingress: Option<f64>,
        results: &[TestResult],
        live: Option<&Live>,
    ) -> String {
        let info = RunInfo {
            group,
            arms: arms.to_vec(),
            tests: results.iter().map(|r| r.test.clone()).collect(),
            duration_min: 30,
            warmup_min: 5,
            started: "2026-10-14T10:00:00Z".to_string(),
            facts: vec![("Cluster size".to_string(), "2 CKU".to_string())],
            ingress_mb_s: ingress,
        };
        markdown(&info, results, live)
    }

    fn test(line: &str) -> PerfTest {
        parse_tests(line).unwrap().remove(0)
    }

    /// Every table row of `md` has as many cells as its header.
    fn assert_rectangular(md: &str) {
        let mut width = None;
        for line in md.lines() {
            if !line.starts_with('|') {
                width = None;
                continue;
            }
            // Escaped pipes inside a cell are not separators.
            let cells = line.replace("\\|", "").matches('|').count();
            match width {
                None => width = Some(cells),
                Some(w) => assert_eq!(w, cells, "{line}\n{md}"),
            }
        }
    }

    fn line_starting<'a>(md: &'a str, prefix: &str) -> &'a str {
        md.lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("no line starting with `{prefix}`:\n{md}"))
    }

    #[test]
    fn bands_follow_the_metric_direction() {
        // Throughput: higher is better.
        let (pct, band) = delta(110.0, 100.0, Better::Higher).unwrap();
        assert!((pct - 10.0).abs() < 1e-9 && band == "🟢", "{pct} {band}");
        assert_eq!(delta(90.0, 100.0, Better::Higher).map(|d| d.1), Some("🟢"));
        assert_eq!(delta(89.0, 100.0, Better::Higher).map(|d| d.1), Some("🔴"));
        assert_eq!(delta(79.0, 100.0, Better::Higher).map(|d| d.1), Some("🔴"));
        // Latency: lower is better.
        assert_eq!(delta(5.0, 10.0, Better::Lower).map(|d| d.1), Some("🟢"));
        assert_eq!(delta(11.0, 10.0, Better::Lower).map(|d| d.1), Some("🟢"));
        assert_eq!(delta(11.5, 10.0, Better::Lower).map(|d| d.1), Some("🔴"));
        assert_eq!(delta(1.0, 0.0, Better::Lower), None);
    }

    #[test]
    fn formats_numbers() {
        assert_eq!(number(1_234_567.891, 2), "1,234,567.89");
        assert_eq!(number(999.0, 0), "999");
        assert_eq!(number(1000.0, 0), "1,000");
        assert_eq!(number(100_000.4, 0), "100,000");
        assert_eq!(number(-1234.5, 1), "-1,234.5");
        assert_eq!(number(0.0, 2), "0.00");
    }

    #[test]
    fn formats_durations_and_scenarios() {
        assert_eq!(hours_minutes(45), "45 min");
        assert_eq!(hours_minutes(120), "2 h");
        assert_eq!(hours_minutes(1350), "22 h 30 min");
        let t =
            test("t rust PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 BATCH_SIZE=1024 COMPRESSION_TYPE=lz4 ACKS=1");
        assert_eq!(
            scenario(&t),
            "producer, 200 partitions, 1024 B values, 10000 msg/s, acks 1, compression lz4, batch 1024 KiB"
        );
        let t = test("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0");
        assert_eq!(scenario(&t), "producer, 1 partitions, 1 B values, unbounded rate");
        let t = test(
            "t rust WORKLOAD=transactional TXN_MODE=eos PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=1000 \
             RECORDS_PER_TRANSACTION=1000 ABORT_RATE=0.5 NUM_TRANSACTIONAL_PRODUCERS=16",
        );
        assert_eq!(
            scenario(&t),
            "transactional (eos), 36 partitions, 1024 B values, 1000 msg/s, records per transaction 1000, abort rate \
             0.5, transactional producers 16"
        );
        let t = test("t rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0");
        assert_eq!(
            scenario(&t),
            "transactional (produce), 1 partitions, 1 B values, unbounded rate"
        );
    }

    #[test]
    fn formats_parameters() {
        assert_eq!(grouped("1024"), "1,024");
        assert_eq!(grouped("0"), "0");
        assert_eq!(grouped("0.125"), "0.125");
        assert_eq!(grouped("+5"), "+5");
        assert_eq!(grouped("none"), "none");
        assert_eq!(start_time("2026-10-15T15:57:31Z"), "2026-10-15 15:57:31 UTC");
        assert_eq!(start_time("1760543851"), "1760543851");
    }

    #[test]
    fn markdown_has_columns_deltas_and_failures() {
        let results = vec![TestResult {
            test: test("producer-max rust PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 500_000.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 400_000.0, 10.0))),
                arm(Arm::Java, Outcome::Failed("harness exited | with 1".to_string()), None),
            ],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let md = render(Group::Rust, &all, Some(600.0), &results);
        // The results first, the parameters after them, as a list.
        assert!(
            md.starts_with("# Rust client performance results\n\n## Results\n\n| Metric |"),
            "{md}"
        );
        assert!(!md.contains("| Parameter |"), "{md}");
        assert!(md.contains("\n- Clients: Rust, librdkafka, Java\n"), "{md}");
        assert!(md.contains("\n- Cluster size: 2 CKU\n"), "{md}");
        // No legend: the colors read on their own.
        assert!(!md.contains("Δ columns") && !md.contains("Worse means"), "{md}");
        // The clients come first, then the comparisons.
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | Rust | librdkafka | Java | Δ Rust vs librdkafka | Δ Java vs librdkafka |"
        );
        assert_eq!(line_starting(&md, "|---|---:"), "|---|---:|---:|---:|---:|---:|");
        // A failure is a note, not a row.
        assert!(!md.contains("| Result |"), "{md}");
        // Rust came near the cluster's ingress limit: its throughput may be
        // the cluster's, so it is not compared.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 500,000 | 400,000 | – | cluster-bound | n/a |"
        );
        assert_eq!(
            line_starting(&md, "| Latency p99 (ms) |"),
            "| Latency p99 (ms) | 9.00 | 10.00 | – | 🟢 -10.0% | n/a |"
        );
        assert_eq!(
            line_starting(&md, "| Messages measured |"),
            "| Messages measured | 30,000,000 | 24,000,000 | – |  |  |"
        );
        // No transaction rows for a producer test.
        assert!(!md.contains("Transactions/s"), "{md}");
        // 500k × 1 KiB/s is 512 MB/s, 85 % of 600.
        // The cluster-bound Δ needs no note.
        assert!(
            md.contains("\n**Notes**\n\n- Java failed: harness exited \\| with 1.\n\n## Parameters\n"),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_lists_a_single_test_with_the_run_parameters() {
        let results = vec![TestResult {
            test: test("c rust WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 COMPRESSION_TYPE=none"),
            arms: vec![arm(
                Arm::Rust,
                Outcome::Passed,
                Some(metrics("rust-consumer", 10_000.0, 12.0)),
            )],
        }];
        let md = render(Group::Rust, &[Arm::Rust], Some(600.0), &results);
        assert!(
            md.starts_with("# Rust client performance results\n\n## Results\n\n| Metric | Rust |\n"),
            "{md}"
        );
        assert!(
            md.ends_with(
                "\n## Parameters\n\n\
                 - Start time: 2026-10-14 10:00:00 UTC\n\
                 - Clients: Rust\n\
                 - Workload: Consumer\n\
                 - Partitions: 200\n\
                 - Value size: 1,024 B\n\
                 - Rate: 10,000 msg/s\n\
                 - Compression: none\n\
                 - Warmup: 5 min per client\n\
                 - Measurement: 30 min per client\n\
                 - Cluster size: 2 CKU\n"
            ),
            "{md}"
        );
        // Nothing to note: no methodology, no warning.
        assert!(!md.contains("Notes"), "{md}");
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_lists_each_tests_parameters_under_its_heading() {
        let results: Vec<TestResult> = [
            "a rust PARTITIONS=1 VALUE_SIZE=100 LIMIT_RPS=0",
            "b rust WORKLOAD=transactional TXN_MODE=eos PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=1000 \
             RECORDS_PER_TRANSACTION=1000 ABORT_RATE=0.5 NUM_TRANSACTIONAL_PRODUCERS=16",
        ]
        .iter()
        .map(|line| TestResult { test: test(line), arms: vec![] })
        .collect();
        let md = render(Group::Rust, &[Arm::Rust], None, &results);
        assert!(
            md.starts_with("# Rust client performance results\n\n## a\n\n| Metric | Rust |\n"),
            "{md}"
        );
        assert!(
            md.contains(
                "- Rust did not run.\n\
                 \n### Parameters\n\n\
                 - Workload: Producer\n\
                 - Partitions: 1\n\
                 - Value size: 100 B\n\
                 - Rate: Unbounded\n\
                 \n## b\n\n| Metric | Rust |\n"
            ),
            "{md}"
        );
        assert!(
            md.contains(
                "- Rust did not run.\n\
                 \n### Parameters\n\n\
                 - Workload: Transactional (EOS)\n\
                 - Partitions: 36\n\
                 - Value size: 1,024 B\n\
                 - Rate: 1,000 msg/s\n\
                 - Records per transaction: 1,000\n\
                 - Abort rate: 0.5\n\
                 - Transactional producers: 16\n\
                 \n## Parameters\n\n- Start time:"
            ),
            "{md}"
        );
        // The run's parameters leave the tests' to their sections.
        let (_, run) = md.rsplit_once("\n## Parameters\n").unwrap();
        assert!(!run.contains("Workload"), "{md}");
        assert_eq!(md.matches("- Rust did not run.\n").count(), 2, "{md}");
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_shows_only_the_clients_that_ran() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1000.0, 9.0))),
                arm(Arm::Java, Outcome::Passed, Some(metrics("java", 1500.0, 9.0))),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Java], None, &results);
        assert!(md.contains("\n- Clients: Rust, Java\n"), "{md}");
        // librdkafka is the only baseline: Rust and Java are never compared.
        assert_eq!(line_starting(&md, "| Metric |"), "| Metric | Rust | Java |");
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 1,500 |"
        );
        assert!(!md.contains("librdkafka"), "{md}");
        assert!(!md.contains("Δ"), "{md}");
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_bands_javas_delta_too() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 600.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 1000.0, 10.0))),
                arm(Arm::Java, Outcome::Passed, Some(metrics("java", 600.0, 9.0))),
            ],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let md = render(Group::Rust, &all, None, &results);
        // The same change, banded alike for Rust and Java.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 600 | 1,000 | 600 | 🔴 -40.0% | 🔴 -40.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Latency p99 (ms) |"),
            "| Latency p99 (ms) | 9.00 | 10.00 | 9.00 | 🟢 -10.0% | 🟢 -10.0% |"
        );
        assert_rectangular(&md);

        // Java's Δ alone.
        let md = render(Group::Rust, &[Arm::Librdkafka, Arm::Java], None, &results);
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | librdkafka | Java | Δ Java vs librdkafka |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 600 | 🔴 -40.0% |"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_marks_arms_not_run() {
        let results = vec![TestResult {
            test: test("t python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0"),
            arms: vec![arm(
                Arm::PythonSyncRust,
                Outcome::Failed("interrupted".to_string()),
                None,
            )],
        }];
        let python = Group::Python.arms();
        let md = render(Group::Python, python, None, &results);
        assert!(md.starts_with("# Python client performance results\n"), "{md}");
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | Python sync (Rust) | Python sync (librdkafka) | Python async (Rust) | Python async \
             (librdkafka) | Δ sync | Δ async |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | – | – | – | – | n/a | n/a |"
        );
        assert!(
            md.contains(
                "\n**Notes**\n\n\
                 - Python sync (Rust) failed: interrupted.\n\
                 - Python sync (librdkafka) did not run.\n\
                 - Python async (Rust) did not run.\n\
                 - Python async (librdkafka) did not run.\n\
                 \n## Parameters\n"
            ),
            "{md}"
        );
        assert_rectangular(&md);
        assert!(render(Group::Python, python, None, &[]).contains("No test has finished yet."));
    }

    #[test]
    fn markdown_flags_clients_below_a_fixed_rate() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 9_990.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 9_000.0, 10.0))),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results);
        assert!(
            md.contains(
                "\n- librdkafka reached 9,000 msg/s, below the 10,000 msg/s target. Its latency is not comparable \
                 with that of clients that held the rate.\n"
            ),
            "{md}"
        );
        assert!(!md.contains("- Rust reached"), "{md}");
    }

    #[test]
    fn markdown_has_transaction_rows() {
        let results = vec![TestResult {
            test: test("txn rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000 ABORT_RATE=0.5"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(txn_metrics("rust-txn", 5_000.0, 40.0))),
                arm(
                    Arm::Librdkafka,
                    Outcome::Passed,
                    Some(txn_metrics("librdkafka-txn", 4_000.0, 50.0)),
                ),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results);
        assert!(md.contains("\n- Workload: Transactional (produce)\n"), "{md}");
        assert_eq!(
            line_starting(&md, "| Transactions/s |"),
            "| Transactions/s | 50.00 | 40.00 | 🟢 +25.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Commit latency p99 (ms) |"),
            "| Commit latency p99 (ms) | 40.00 | 50.00 | 🟢 -20.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Committed transactions |"),
            "| Committed transactions | 3,000 | 2,400 |  |"
        );
        // Half the transactions abort, so 5,000 committed msg/s holds the
        // 10,000 msg/s rate.
        assert!(!md.contains("- Rust reached"), "{md}");
        assert!(
            md.contains("\n- librdkafka reached 4,000 msg/s, below the 5,000 msg/s target."),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_compares_only_clients_that_passed() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1_000.0, 9.0))),
                arm(
                    Arm::Librdkafka,
                    Outcome::Failed("harness exited with 1.".to_string()),
                    Some(metrics("librdkafka", 2_000.0, 5.0)),
                ),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results);
        // The failed client's numbers are shown, never compared.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 2,000 | n/a |"
        );
        assert_eq!(
            line_starting(&md, "| Latency p99 (ms) |"),
            "| Latency p99 (ms) | 9.00 | 5.00 | n/a |"
        );
        assert!(
            md.contains(
                "\n- librdkafka failed: harness exited with 1. Its results are from a partial run and are not \
                 compared.\n"
            ),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_counts_aborted_records_against_the_ingress() {
        let results = vec![TestResult {
            test: test("txn rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0 ABORT_RATE=0.5"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(txn_metrics("rust-txn", 5_000.0, 40.0))),
                arm(
                    Arm::Librdkafka,
                    Outcome::Passed,
                    Some(txn_metrics("librdkafka-txn", 2_000.0, 50.0)),
                ),
            ],
        }];
        // 5,000 committed records/s of 1 KiB are 5.1 MB/s; with as many
        // aborted, 10.2 MB/s crossed the 12 MB/s ingress.
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], Some(12.0), &results);
        assert!(!md.contains("Notes"), "{md}");
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 5,000 | 2,000 | cluster-bound |"
        );
        assert_eq!(
            line_starting(&md, "| Transactions/s |"),
            "| Transactions/s | 50.00 | 20.00 | cluster-bound |"
        );
        // Latency is still compared.
        assert_eq!(
            line_starting(&md, "| Commit latency p99 (ms) |"),
            "| Commit latency p99 (ms) | 40.00 | 50.00 | 🟢 -20.0% |"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_blames_a_short_load_not_the_consumer() {
        let consumer = |client: &str, msg_s: f64, load: f64, arm_id: Arm| ArmResult {
            load_msg_s: Some(load),
            ..arm(arm_id, Outcome::Passed, Some(metrics(client, msg_s, 9.0)))
        };
        let results = vec![TestResult {
            test: test("c rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            arms: vec![
                consumer("rust-consumer", 9_000.0, 9_000.0, Arm::Rust),
                consumer("librdkafka-consumer", 9_000.0, 9_990.0, Arm::Librdkafka),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results);
        // Rust's load fell short: the note names the load.
        assert!(
            md.contains(
                "\n- The load producer for Rust reached 9,000 msg/s, below the 10,000 msg/s target. Its throughput \
                 and latency reflect the load, not the client.\n"
            ),
            "{md}"
        );
        assert!(!md.contains("- Rust reached"), "{md}");
        // librdkafka's load held: the shortfall is the client's.
        assert!(
            md.contains("\n- librdkafka reached 9,000 msg/s, below the 10,000 msg/s target."),
            "{md}"
        );
        assert!(!md.contains("The load producer for librdkafka"), "{md}");
        // How a consumer test measures is the documentation's, not the report's.
        assert!(!md.contains("kafka-producer-perf-test.sh"), "{md}");
    }

    fn so_far(msg_s: f64, minutes: f64) -> SoFar {
        let seconds = minutes * 60.0;
        SoFar {
            seconds,
            messages: msg_s * seconds,
            bytes: msg_s * seconds * 1024.0,
            latency_avg: Some(3.25),
            latency_max: Some(40.0),
            cpu_pct: Some(55.5),
            rss_mib: Some(120.3),
        }
    }

    #[test]
    fn live_markdown_shows_the_running_client_so_far() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1_000.0, 9.0)))],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let live = Live {
            updated: "2026-10-21T10:05:00Z".to_string(),
            running: Some(Running { arm: Arm::Librdkafka, so_far: Some(so_far(2_000.0, 12.5)) }),
        };
        let md = render_live(Group::Rust, &all, None, &results, Some(&live));
        assert!(
            md.starts_with(
                "# Rust client performance results\n\n> In progress, updated 2026-10-21 10:05:00 UTC and every 5 \
                 min while a test runs. A running client shows its throughput, average and maximum latency, CPU, \
                 RSS and messages so far; the full report replaces this one when the test finishes.\n\n## \
                 Results\n"
            ),
            "{md}"
        );
        // The finished client's numbers, the running one's so far, and no Δ
        // until both of its clients finish.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 2,000 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (MiB/s) |"),
            "| Throughput (MiB/s) | 0.98 | 1.95 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Latency avg (ms) |"),
            "| Latency avg (ms) | 2.50 | 3.25 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Latency p99 (ms) |"),
            "| Latency p99 (ms) | 9.00 | – | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Latency max (ms) |"),
            "| Latency max (ms) | 20.00 | 40.00 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| CPU (%) |"),
            "| CPU (%) | 50.0 | 55.5 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| RSS (MiB) |"),
            "| RSS (MiB) | 100.0 | 120.3 | – | pending | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Messages measured |"),
            "| Messages measured | 60,000 | 1,500,000 | – |  |  |"
        );
        assert!(
            md.contains(
                "\n**Notes**\n\n\
                 - librdkafka is running: 12 min of its 30 min measurement so far. Its other numbers come when it \
                 finishes.\n\
                 - Java has not started yet.\n\
                 \n## Parameters\n"
            ),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn live_markdown_compares_the_clients_that_finished() {
        let results = vec![TestResult {
            test: test("txn rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(txn_metrics("rust-txn", 1_000.0, 40.0))),
                arm(
                    Arm::Librdkafka,
                    Outcome::Passed,
                    Some(txn_metrics("librdkafka-txn", 1_000.0, 50.0)),
                ),
            ],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let live = Live {
            updated: "2026-10-21T10:05:00Z".to_string(),
            running: Some(Running { arm: Arm::Java, so_far: None }),
        };
        let md = render_live(Group::Rust, &all, None, &results, Some(&live));
        assert_eq!(
            line_starting(&md, "| Commit latency p99 (ms) |"),
            "| Commit latency p99 (ms) | 40.00 | 50.00 | – | 🟢 -20.0% | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 1,000 | – | 🟢 +0.0% | pending |"
        );
        assert!(
            md.contains("\n**Notes**\n\n- Java is running and has not started measuring yet.\n\n## Parameters\n"),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn live_markdown_runs_only_its_last_test() {
        let results = vec![
            TestResult {
                test: test("a rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
                arms: vec![arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1_000.0, 9.0)))],
            },
            TestResult { test: test("b rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"), arms: vec![] },
        ];
        let live = Live { updated: "2026-10-21T10:05:00Z".to_string(), running: None };
        let md = render_live(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results, Some(&live));
        let (a, b) = md.split_once("\n## b\n").unwrap();
        // The finished test's missing client did not run; the running test's
        // have not started.
        assert!(a.contains("- librdkafka did not run.\n") && !a.contains("pending"), "{md}");
        assert_eq!(
            line_starting(b, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | – | – | pending |"
        );
        assert!(
            b.contains("- Rust has not started yet.\n- librdkafka has not started yet.\n"),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn live_markdown_notes_a_failure_beside_the_running_client() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            arms: vec![arm(
                Arm::Rust,
                Outcome::Failed("harness exited with 1".to_string()),
                Some(metrics("rust", 1_000.0, 9.0)),
            )],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let live = Live {
            updated: "2026-10-21T10:05:00Z".to_string(),
            running: Some(Running { arm: Arm::Librdkafka, so_far: Some(so_far(2_000.0, 0.5)) }),
        };
        let md = render_live(Group::Rust, &all, None, &results, Some(&live));
        // A failed client is never compared, so its Δ is not pending.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 2,000 | – | n/a | pending |"
        );
        // A running client below the rate gets no note until it finishes.
        assert!(
            md.contains(
                "\n**Notes**\n\n\
                 - Rust failed: harness exited with 1. Its results are from a partial run and are not compared.\n\
                 - librdkafka is running: under 1 min of its 30 min measurement so far. Its other numbers come when \
                 it finishes.\n\
                 - Java has not started yet.\n\
                 \n## Parameters\n"
            ),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn live_markdown_compares_a_python_pair_that_finished() {
        let results = vec![TestResult {
            test: test("t python PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::PythonSyncRust, Outcome::Passed, Some(metrics("a", 1_000.0, 9.0))),
                arm(Arm::PythonSyncLibrdkafka, Outcome::Passed, Some(metrics("b", 1_000.0, 10.0))),
            ],
        }];
        // No CPU or RSS sample yet.
        let so_far = SoFar { cpu_pct: None, rss_mib: None, ..so_far(500.0, 2.0) };
        let live = Live {
            updated: "2026-10-21T10:05:00Z".to_string(),
            running: Some(Running { arm: Arm::PythonAsyncRust, so_far: Some(so_far) }),
        };
        let md = render_live(Group::Python, Group::Python.arms(), None, &results, Some(&live));
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 1,000 | 500 | – | 🟢 +0.0% | pending |"
        );
        assert_eq!(
            line_starting(&md, "| CPU (%) |"),
            "| CPU (%) | 50.0 | 50.0 | – | – | 🟢 +0.0% | pending |"
        );
        assert!(
            md.contains(
                "- Python async (Rust) is running: 2 min of its 30 min measurement so far. Its other numbers come \
                 when it finishes.\n- Python async (librdkafka) has not started yet.\n"
            ),
            "{md}"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn junit_escapes_and_counts() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1000.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Failed("bad <\"&'> \u{1}end".to_string()), None),
                arm(Arm::Java, Outcome::Passed, Some(txn_metrics("java-txn", 1000.0, 9.0))),
            ],
        }];
        let xml = junit(&results);
        assert!(
            xml.contains("<testsuites name=\"perf-cloud\" tests=\"3\" failures=\"1\" errors=\"0\" time=\"270.0\">"),
            "{xml}"
        );
        assert!(xml.contains("<testsuite name=\"t\" tests=\"3\" failures=\"1\""), "{xml}");
        assert!(
            xml.contains("<testcase classname=\"t\" name=\"librdkafka\" time=\"90.0\">"),
            "{xml}"
        );
        assert!(xml.contains("<failure message=\"bad &lt;&quot;&amp;&apos;&gt; end\">"), "{xml}");
        assert!(xml.contains("<system-out>client=rust messages=60000 msg/s=1000"), "{xml}");
        assert!(
            xml.contains("rss=100.0MiB txn/s=10.00 committed=600 aborted=600 commit_p50=20.00ms commit_p99=9.00ms"),
            "{xml}"
        );
        assert_eq!(
            junit(&[]).lines().nth(1),
            Some("<testsuites name=\"perf-cloud\" tests=\"0\" failures=\"0\" errors=\"0\" time=\"0.0\">")
        );
    }
}
