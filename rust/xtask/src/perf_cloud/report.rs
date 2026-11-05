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
//! a list, the machine the clients ran on first. A run of one test (as CI
//! runs) lists the test's settings there too; a run of several lists each
//! test's settings under its results. Below the `#` title, headings stay at
//! `###` and below, which renderers draw without a rule under them. Each test
//! has a table with a row per metric, a column per client that ran and, after
//! the clients, a Δ
//! column per comparison giving a client's change against its librdkafka
//! baseline: in the `rust` group Rust and Java each against librdkafka, in
//! the `python` group each Rust-based client against the librdkafka-based one
//! of its mode. Never Rust against Java. Every change is colored by the
//! metric's direction, with no legend and no tolerance:
//!
//! - 🟢 better: higher throughput or transactions/s, lower latency, CPU or
//!   RSS;
//! - 🔴 worse;
//! - no color when it rounds to no change.
//!
//! Counts (messages, transactions) get no Δ, and neither does a CPU or RSS
//! of 0, which a harness writes when it took no sample: it shows as missing.
//! A Δ is `n/a` unless both clients passed: a failed arm's numbers (from a
//! partial run) are shown in its column but never compared. A throughput Δ is
//! `cluster-bound` when either client's ingress came near the cluster's
//! limit, which then capped it rather than the client; never in a compressed
//! test, whose ingress is unknown (the harnesses report uncompressed bytes,
//! the cluster meters compressed ones). The table's first row, Status, says
//! how each client ended: passed, failed, or not run, and whether it missed a
//! fixed rate (`below rate`), or missed it after its consumer load did
//! (`load short`). A client below its rate still passed: its Δ are colored
//! like any other, and its Status tells the reader it missed its rate. A
//! latency percentile in the harnesses' top histogram bucket, which holds
//! every longer latency too, is shown as at least that bucket (`≥ 10,001.00`).
//!
//! The report has one note, under its title, when the librdkafka client ran
//! a producer or transactional (`produce`) test: its harness produces without
//! `RD_KAFKA_MSG_F_COPY`, so its CPU and RSS leave out the payload copy every
//! other client makes.
//!
//! While a test runs, the report so far ([`Live`], written by
//! [`super::live`]) opens with when it was written. Its running client shows
//! the numbers it has measured so far that need no percentiles (throughput,
//! average and maximum latency, CPU, RSS, records), its other rows `–`; its
//! Status is `running`, that of the clients yet to start `waiting`, and a Δ
//! either of whose clients has not finished is `pending` (`n/a` at once when
//! the other failed).

use std::fmt::Write as _;

use super::live::{SoFar, INTERVAL};
use super::{
    Arm, ArmResult, Group, Metrics, Outcome, PerfTest, RunInfo, TestResult, Workload, PLAN_BUFFER_MEMORY_MIB, PRODUCING,
};

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
/// rate. Its Δ are still colored like any other's: its Status (`below rate`,
/// `load short`) tells the reader it missed the rate.
const HELD_RATE_SHARE: f64 = 0.95;

const MIB: f64 = 1024.0 * 1024.0;

/// The run facts ([`RunInfo::facts`]) naming the machine the clients ran on,
/// listed first among the parameters.
pub(crate) const MACHINE_FACTS: &[&str] = &["Instance", "CPU"];

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
        c(Arm::PythonSyncRust, Arm::PythonSyncLibrdkafka, "Δ sync (Rust vs librdkafka)"),
        c(Arm::PythonAsyncRust, Arm::PythonAsyncLibrdkafka, "Δ async (Rust vs librdkafka)"),
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
    /// A latency percentile, read from the harnesses' histograms.
    percentile: bool,
    /// How to read it from a running client's numbers so far; `None` for the
    /// rows only a finished client has.
    so_far: Option<fn(&SoFar) -> Option<f64>>,
}

impl Row {
    /// The row, shown for a running client too.
    const fn so_far(self, value: fn(&SoFar) -> Option<f64>) -> Row {
        Row { so_far: Some(value), ..self }
    }

    /// The row, a latency percentile.
    const fn percentile(self) -> Row {
        Row { percentile: true, ..self }
    }
}

const fn row(
    label: &'static str,
    value: fn(&Metrics) -> Option<f64>,
    decimals: usize,
    better: Option<Better>,
    transactional: bool,
) -> Row {
    Row {
        label,
        value,
        decimals,
        better,
        transactional,
        throughput: false,
        percentile: false,
        so_far: None,
    }
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
        percentile: false,
        so_far: None,
    }
}

/// A CPU or RSS average, `None` when 0: what a harness writes when it took
/// no sample.
fn sampled(value: f64) -> Option<f64> {
    (value > 0.0).then_some(value)
}

/// The latency, in ms, of the top bucket of the histograms the harnesses of
/// `workload` read their percentiles from: that bucket holds every longer
/// latency too, so a percentile there is that or more.
fn histogram_top_ms(workload: Workload) -> f64 {
    match workload {
        // 1 ms buckets to 10,000 ms, then one for anything longer.
        Workload::Producer | Workload::Transactional => 10_001.0,
        // 1 ms buckets to 600,000 ms, the last holding anything longer.
        Workload::Consumer => 600_000.0,
    }
}

/// `mark` before a latency percentile of `workload` in its histogram's top
/// bucket (see [`histogram_top_ms`]), which is at least that; `""` before any
/// other.
fn at_least(value: f64, workload: Workload, mark: &'static str) -> &'static str {
    if value >= histogram_top_ms(workload) {
        mark
    } else {
        ""
    }
}

const ROWS: &[Row] = &[
    rate_row("Throughput (msg/s)", |m| Some(m.msg_s), 0, false).so_far(SoFar::msg_s),
    rate_row("Throughput (MiB/s)", |m| Some(m.mib_s), 2, false).so_far(SoFar::mib_s),
    rate_row("Transactions/s", |m| m.txn.as_ref().map(|t| t.per_s), 2, true),
    row("Latency avg (ms)", |m| Some(m.avg), 2, Some(Better::Lower), false).so_far(|s| s.latency_avg),
    row("Latency p50 (ms)", |m| Some(m.p50), 2, Some(Better::Lower), false).percentile(),
    row("Latency p90 (ms)", |m| Some(m.p90), 2, Some(Better::Lower), false).percentile(),
    row("Latency p95 (ms)", |m| Some(m.p95), 2, Some(Better::Lower), false).percentile(),
    row("Latency p99 (ms)", |m| Some(m.p99), 2, Some(Better::Lower), false).percentile(),
    row("Latency p99.9 (ms)", |m| Some(m.p999), 2, Some(Better::Lower), false).percentile(),
    row("Latency max (ms)", |m| Some(m.max), 2, Some(Better::Lower), false).so_far(|s| s.latency_max),
    row(
        "Commit latency p50 (ms)",
        |m| m.txn.as_ref().map(|t| t.commit_p50),
        2,
        Some(Better::Lower),
        true,
    )
    .percentile(),
    row(
        "Commit latency p99 (ms)",
        |m| m.txn.as_ref().map(|t| t.commit_p99),
        2,
        Some(Better::Lower),
        true,
    )
    .percentile(),
    row("CPU (%)", |m| sampled(m.cpu_pct), 1, Some(Better::Lower), false).so_far(|s| s.cpu_pct.and_then(sampled)),
    row("RSS (MiB)", |m| sampled(m.rss_mib), 1, Some(Better::Lower), false).so_far(|s| s.rss_mib.and_then(sampled)),
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
    (
        "BUFFER_MEMORY_ALLOCATION_STRATEGY",
        "buffer allocation",
        "Buffer allocation strategy",
        "",
    ),
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
    ("LIBRDKAFKA_BATCH", "librdkafka", "librdkafka reads", ""),
];

/// A setting's value as a scenario or a parameter table shows it, `number`
/// formatting a whole number.
fn setting_value(key: &str, value: &str, unit: &str, number: fn(&str) -> String) -> String {
    match (key, value) {
        ("LIBRDKAFKA_BATCH", "0") => "poll(), one record at a time".to_string(),
        ("LIBRDKAFKA_BATCH", n) => format!("consume_batch, {} records", number(n)),
        _ => format!("{}{unit}", number(value)),
    }
}

/// `test`'s `key` as a scenario or a parameter table shows it: the value the
/// test sets or, for `BUFFER_MEMORY` in a producing test, the driver's
/// default, which every producing client gets when the test sets none.
fn shown_setting(test: &PerfTest, key: &str, unit: &str, number: fn(&str) -> String) -> Option<String> {
    match test.setting(key) {
        Some(value) => Some(setting_value(key, value, unit, number)),
        None if key == "BUFFER_MEMORY" && PRODUCING.contains(&test.workload) => Some(format!(
            "{} (default)",
            setting_value(key, &PLAN_BUFFER_MEMORY_MIB.to_string(), unit, number)
        )),
        None => None,
    }
}

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
        if let Some(value) = shown_setting(test, key, unit, str::to_string) {
            parts.push(format!("{label} {value}"));
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
        add(name, shown_setting(test, key, unit, grouped));
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

/// The relative change of `candidate` against `baseline` as a Δ cell: 🟢 when
/// it is better, 🔴 when worse, uncolored when it rounds to no change. `None`
/// when the baseline is zero (no meaningful ratio).
fn delta(candidate: f64, baseline: f64, better: Better) -> Option<String> {
    if baseline == 0.0 || !baseline.is_finite() || !candidate.is_finite() {
        return None;
    }
    // Rounded as shown, so the color never disagrees with the number.
    let pct = ((candidate - baseline) / baseline * 1000.0).round() / 10.0;
    if pct == 0.0 {
        return Some("0.0%".to_string());
    }
    let improved = match better {
        Better::Higher => pct > 0.0,
        Better::Lower => pct < 0.0,
    };
    Some(format!("{} {pct:+.1}%", if improved { "🟢" } else { "🔴" }))
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
/// cluster, uncompressed: only an uncompressed test's is what the cluster
/// meters. A transactional test's throughput counts committed records only,
/// while its aborted records crossed the ingress too.
fn ingress_mb_s(m: &Metrics, abort_rate: f64) -> f64 {
    let committed = m.mib_s * MIB / 1e6;
    if abort_rate > 0.0 && abort_rate < 1.0 {
        committed / (1.0 - abort_rate)
    } else {
        committed
    }
}

/// Whether the run has the librdkafka client produce from its harness's
/// payloads without copying them: the C producer harness and the
/// transactional one in `produce` mode pass no `RD_KAFKA_MSG_F_COPY` (`eos`
/// copies each consumed record).
fn librdkafka_skips_copy(info: &RunInfo) -> bool {
    info.arms.contains(&Arm::Librdkafka)
        && info
            .tests
            .iter()
            .any(|t| t.workload == Workload::Producer || (t.workload == Workload::Transactional && !t.is_eos()))
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
    if librdkafka_skips_copy(info) {
        let _ = writeln!(
            out,
            "\n> In producer and transactional (produce) tests the librdkafka harness produces without \
             `RD_KAFKA_MSG_F_COPY`: librdkafka sends the harness's pre-generated payloads in place, so its CPU and \
             RSS leave out the payload copy every other client makes."
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
        // `###` and below, under the `#` title: renderers draw a rule under
        // `#` and `##`, which would sit between a heading and its table.
        match single {
            Some(_) => {
                let _ = writeln!(out, "\n### Results\n");
                results_table(&mut out, info, &columns, result, live);
            },
            None => {
                let _ = writeln!(out, "\n### {}\n", inline(&result.test.id));
                results_table(&mut out, info, &columns, result, live);
                let _ = writeln!(out, "\n#### Parameters\n");
                parameter_list(&mut out, &parameters(&result.test));
            },
        }
    }

    let clients: Vec<&str> = info.arms.iter().map(|a| a.label()).collect();
    // The machine the clients ran on leads, as it bounds every result.
    let (machine, facts): (Vec<_>, Vec<_>) = info
        .facts
        .iter()
        .cloned()
        .partition(|(label, _)| MACHINE_FACTS.contains(&label.as_str()));
    let mut rows = vec![("Start time".to_string(), start_time(&info.started))];
    rows.extend(machine);
    rows.push(("Clients".to_string(), clients.join(", ")));
    if let Some(test) = single {
        rows.extend(parameters(test));
    }
    rows.push(("Warmup".to_string(), format!("{} min per client", info.warmup_min)));
    rows.push(("Measurement".to_string(), format!("{} min per client", info.duration_min)));
    rows.extend(facts);
    let _ = writeln!(out, "\n### Parameters\n");
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

/// How `arm` ended in `result`'s test, as its Status cell: `passed`, `failed`
/// or `not run`; with `live`, `running` or `waiting` until it finishes. A
/// client that passed but missed a fixed rate is `below rate`, or `load
/// short` when its consumer load fell short too.
fn status(arm: Arm, result: &TestResult, live: Option<&Live>) -> &'static str {
    let Some(a) = find(&result.arms, arm) else {
        return match live {
            Some(l) if l.running.as_ref().is_some_and(|r| r.arm == arm) => "running",
            Some(_) => "waiting",
            None => "not run",
        };
    };
    let Some(m) = a.metrics.as_ref().filter(|_| a.outcome == Outcome::Passed) else {
        return match a.outcome {
            Outcome::Passed => "passed",
            Outcome::Failed(_) => "failed",
        };
    };
    // The committed rate a fixed-rate test aims at: its aborted transactions'
    // records are sent but not counted.
    let test = &result.test;
    let abort_rate: f64 = test.setting("ABORT_RATE").and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let rate: f64 = test.setting("LIMIT_RPS").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    let target = rate * (1.0 - abort_rate);
    let below_rate = target > 0.0 && m.msg_s < HELD_RATE_SHARE * target;
    if !below_rate {
        return "passed";
    }
    // A consumer reads what its load produced: when the load fell short too,
    // so did the consumer, through no fault of its own.
    let short_load = a
        .load_msg_s
        .is_some_and(|r| test.workload == Workload::Consumer && r < HELD_RATE_SHARE * target);
    if short_load {
        "load short"
    } else {
        "below rate"
    }
}

/// A test's results table; with `live`, the test is still running.
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
    let statuses: Vec<&str> = columns
        .iter()
        .map(|c| match c {
            Column::Client(arm) => status(*arm, result, live),
            Column::Delta(_) => "",
        })
        .collect();
    let _ = writeln!(out, "| Status | {} |", statuses.join(" | "));

    let transactional = test.workload == Workload::Transactional;
    let abort_rate: f64 = test.setting("ABORT_RATE").and_then(|v| v.parse().ok()).unwrap_or(0.0);
    // The cluster meters compressed bytes, which no harness reports.
    let compressed = test.setting("COMPRESSION_TYPE").is_some_and(|c| c != "none");
    // Whether `arm`'s ingress came near the cluster's limit.
    let near_ingress = |arm: Arm| match (metrics_of(&result.arms, arm), info.ingress_mb_s) {
        (Some(m), Some(limit)) if !compressed => ingress_mb_s(m, abort_rate) >= NEAR_INGRESS_SHARE * limit,
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
                        Some(v) if row.percentile => {
                            format!("{}{}", at_least(v, test.workload, "≥ "), number(v, row.decimals))
                        },
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
                        (Some(c), Some(b)) => delta(c, b, better).unwrap_or_else(|| "n/a".to_string()),
                        _ => "n/a".to_string(),
                    }
                },
            })
            .collect();
        let _ = writeln!(out, "| {} | {} |", row.label, cells.join(" | "));
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
            let percentile = |v: f64| format!("{}{v:.2}", at_least(v, t.test.workload, "≥"));
            let summary = a.metrics.as_ref().map(|m| {
                let mut s = format!(
                    "client={} messages={:.0} msg/s={:.0} MiB/s={:.2} p50={}ms p99={}ms p99.9={}ms max={:.2}ms \
                     cpu={:.1}% rss={:.1}MiB",
                    m.client,
                    m.messages,
                    m.msg_s,
                    m.mib_s,
                    percentile(m.p50),
                    percentile(m.p99),
                    percentile(m.p999),
                    m.max,
                    m.cpu_pct,
                    m.rss_mib
                );
                if let Some(txn) = &m.txn {
                    let _ = write!(
                        s,
                        " txn/s={:.2} committed={:.0} aborted={:.0} commit_p50={}ms commit_p99={}ms",
                        txn.per_s,
                        txn.committed,
                        txn.aborted,
                        percentile(txn.commit_p50),
                        percentile(txn.commit_p99)
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

    #[test]
    fn says_how_the_librdkafka_clients_read() {
        let tests = parse_tests(
            "poll python WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=300000 LIBRDKAFKA_BATCH=0\n\
             batch python WORKLOAD=consumer PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=300000 MAX_POLL_RECORDS=4500 \
             LIBRDKAFKA_BATCH=4000\n",
        )
        .unwrap();
        assert_eq!(
            scenario(&tests[0]),
            "consumer, 200 partitions, 1024 B values, 300000 msg/s, librdkafka poll(), one record at a time"
        );
        assert!(scenario(&tests[1]).ends_with("max.poll.records 4500, librdkafka consume_batch, 4000 records"));
        let rows = parameters(&tests[1]);
        assert!(
            rows.contains(&("max.poll.records".to_string(), "4,500".to_string())),
            "{rows:?}"
        );
        assert!(
            rows.contains(&("librdkafka reads".to_string(), "consume_batch, 4,000 records".to_string())),
            "{rows:?}"
        );
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
    fn colors_follow_the_metric_direction_with_no_tolerance() {
        let d = |c: f64, b: f64, better: Better| delta(c, b, better).unwrap();
        // Throughput: higher is better, and any drop is worse.
        assert_eq!(d(110.0, 100.0, Better::Higher), "🟢 +10.0%");
        assert_eq!(d(99.0, 100.0, Better::Higher), "🔴 -1.0%");
        assert_eq!(d(79.0, 100.0, Better::Higher), "🔴 -21.0%");
        // Latency, CPU, RSS: lower is better, and any rise is worse.
        assert_eq!(d(5.0, 10.0, Better::Lower), "🟢 -50.0%");
        assert_eq!(d(10.1, 10.0, Better::Lower), "🔴 +1.0%");
        // No change, as shown, gets no color.
        assert_eq!(d(100.0, 100.0, Better::Higher), "0.0%");
        assert_eq!(d(100.04, 100.0, Better::Lower), "0.0%");
        assert_eq!(d(99.96, 100.0, Better::Higher), "0.0%");
        assert_eq!(d(100.06, 100.0, Better::Lower), "🔴 +0.1%");
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
            "producer, 200 partitions, 1024 B values, 10000 msg/s, acks 1, compression lz4, batch 1024 KiB, buffer 32 \
             MiB (default)"
        );
        let t = test("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0 BUFFER_MEMORY=64");
        assert_eq!(
            scenario(&t),
            "producer, 1 partitions, 1 B values, unbounded rate, buffer 64 MiB"
        );
        let t = test(
            "t rust WORKLOAD=transactional TXN_MODE=eos PARTITIONS=36 VALUE_SIZE=1024 LIMIT_RPS=1000 \
             RECORDS_PER_TRANSACTION=1000 ABORT_RATE=0.5 NUM_TRANSACTIONAL_PRODUCERS=16",
        );
        assert_eq!(
            scenario(&t),
            "transactional (eos), 36 partitions, 1024 B values, 1000 msg/s, buffer 32 MiB (default), records per \
             transaction 1000, abort rate 0.5, transactional producers 16"
        );
        let t = test("t rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0");
        assert_eq!(
            scenario(&t),
            "transactional (produce), 1 partitions, 1 B values, unbounded rate, buffer 32 MiB (default)"
        );
        // A consumer test's load keeps its own buffer.
        let t = test("t rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=10");
        assert_eq!(scenario(&t), "consumer, 1 partitions, 1 B values, 10 msg/s");
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
        // The results first, the parameters after them, as a list; under the
        // title no heading above `###`, so none is drawn with a rule under it.
        // The librdkafka producer's note, under the title, is the only one.
        assert!(
            md.starts_with(
                "# Rust client performance results\n\n> In producer and transactional (produce) tests the librdkafka \
                 harness produces without `RD_KAFKA_MSG_F_COPY`: librdkafka sends the harness's pre-generated \
                 payloads in place, so its CPU and RSS leave out the payload copy every other client makes.\n\n### \
                 Results\n\n| Metric |"
            ),
            "{md}"
        );
        assert!(!md.contains("\n## "), "{md}");
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
        // How each client ended is the table's first row.
        assert_eq!(
            md.lines().skip_while(|l| !l.starts_with("|---|")).nth(1),
            Some("| Status | passed | passed | failed |  |  |")
        );
        // Rust came near the cluster's ingress limit (500k × 1 KiB/s is
        // 512 MB/s, 85 % of 600): its throughput may be the cluster's, so it
        // is not compared.
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
        // No notes: the table goes straight on to the parameters.
        assert!(!md.contains("Notes"), "{md}");
        assert!(md.contains("| – |  |  |\n\n### Parameters\n"), "{md}");
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
            md.starts_with(
                "# Rust client performance results\n\n### Results\n\n| Metric | Rust |\n|---|---:|\n| Status | passed |\n"
            ),
            "{md}"
        );
        assert!(
            md.ends_with(
                "\n### Parameters\n\n\
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
    fn markdown_names_the_machine_first() {
        let results = vec![TestResult { test: test("t python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0"), arms: vec![] }];
        let info = RunInfo {
            group: Group::Python,
            arms: vec![Arm::PythonSyncRust],
            tests: vec![results[0].test.clone()],
            duration_min: 5,
            warmup_min: 1,
            started: "2026-10-14T10:00:00Z".to_string(),
            facts: vec![
                ("Cluster size".to_string(), "11 CKU".to_string()),
                ("Instance".to_string(), "m8i.4xlarge (EC2 i-0123)".to_string()),
                ("CPU".to_string(), "Intel(R) Xeon(R) 6975P-C, 16 vCPUs".to_string()),
            ],
            ingress_mb_s: None,
        };
        let md = markdown(&info, &results, None);
        assert!(
            md.contains(
                "\n### Parameters\n\n\
                 - Start time: 2026-10-14 10:00:00 UTC\n\
                 - Instance: m8i.4xlarge (EC2 i-0123)\n\
                 - CPU: Intel(R) Xeon(R) 6975P-C, 16 vCPUs\n\
                 - Clients: Python sync (Rust)\n"
            ),
            "{md}"
        );
        assert!(
            md.ends_with("- Measurement: 5 min per client\n- Cluster size: 11 CKU\n"),
            "{md}"
        );
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
            md.starts_with(
                "# Rust client performance results\n\n### a\n\n| Metric | Rust |\n|---|---:|\n| Status | not run |\n"
            ),
            "{md}"
        );
        assert!(
            md.contains(
                "| Messages measured | – |\n\
                 \n#### Parameters\n\n\
                 - Workload: Producer\n\
                 - Partitions: 1\n\
                 - Value size: 100 B\n\
                 - Rate: Unbounded\n\
                 - Buffer memory: 32 MiB (default)\n\
                 \n### b\n\n| Metric | Rust |\n|---|---:|\n| Status | not run |\n"
            ),
            "{md}"
        );
        assert!(
            md.contains(
                "| Aborted transactions | – |\n\
                 \n#### Parameters\n\n\
                 - Workload: Transactional (EOS)\n\
                 - Partitions: 36\n\
                 - Value size: 1,024 B\n\
                 - Rate: 1,000 msg/s\n\
                 - Buffer memory: 32 MiB (default)\n\
                 - Records per transaction: 1,000\n\
                 - Abort rate: 0.5\n\
                 - Transactional producers: 16\n\
                 \n### Parameters\n\n- Start time:"
            ),
            "{md}"
        );
        // The run's parameters leave the tests' to their sections.
        let (_, run) = md.rsplit_once("\n### Parameters\n").unwrap();
        assert!(!run.contains("Workload"), "{md}");
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
             (librdkafka) | Δ sync (Rust vs librdkafka) | Δ async (Rust vs librdkafka) |"
        );
        assert_eq!(
            line_starting(&md, "| Status |"),
            "| Status | failed | not run | not run | not run |  |  |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | – | – | – | – | n/a | n/a |"
        );
        assert!(!md.contains("Notes"), "{md}");
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
        // Rust held 99.9 % of the rate; librdkafka 90 %.
        assert_eq!(line_starting(&md, "| Status |"), "| Status | passed | below rate |  |");
        // A client below the rate is still compared: its Status says it
        // missed the rate.
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 9,990 | 9,000 | 🟢 +11.0% |"
        );
    }

    #[test]
    fn markdown_never_calls_a_compressed_test_cluster_bound() {
        let result = |compression: &str| TestResult {
            test: test(&format!(
                "t rust PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=0 COMPRESSION_TYPE={compression}"
            )),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 500_000.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 400_000.0, 10.0))),
            ],
        };
        let arms = [Arm::Rust, Arm::Librdkafka];
        // 512 MB/s of uncompressed records, 85 % of 600: the cluster may have
        // capped them.
        let md = render(Group::Rust, &arms, Some(600.0), &[result("none")]);
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 500,000 | 400,000 | cluster-bound |"
        );
        // Compressed, far fewer bytes may have crossed the ingress, so the
        // clients are compared.
        let md = render(Group::Rust, &arms, Some(600.0), &[result("lz4")]);
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 500,000 | 400,000 | 🟢 +25.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (MiB/s) |"),
            "| Throughput (MiB/s) | 488.28 | 390.62 | 🟢 +25.0% |"
        );
    }

    #[test]
    fn markdown_shows_no_cpu_or_rss_sample_as_missing() {
        let unsampled = Metrics { cpu_pct: 0.0, rss_mib: 0.0, ..metrics("rust", 1_000.0, 9.0) };
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(unsampled)),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 1_000.0, 9.0))),
            ],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let live = Live {
            updated: "2026-10-21T10:05:00Z".to_string(),
            running: Some(Running {
                arm: Arm::Java,
                so_far: Some(SoFar { cpu_pct: Some(0.0), rss_mib: Some(0.0), ..so_far(1_000.0, 1.0) }),
            }),
        };
        let md = render_live(Group::Rust, &all, None, &results, Some(&live));
        assert_eq!(line_starting(&md, "| CPU (%) |"), "| CPU (%) | – | 50.0 | – | n/a | pending |");
        assert_eq!(
            line_starting(&md, "| RSS (MiB) |"),
            "| RSS (MiB) | – | 100.0 | – | n/a | pending |"
        );
        assert_rectangular(&md);
    }

    #[test]
    fn markdown_notes_a_librdkafka_producer_that_does_not_copy() {
        const NOTE: &str = "the librdkafka harness produces without `RD_KAFKA_MSG_F_COPY`";
        let noted = |group: Group, arms: &[Arm], lines: &[&str]| {
            let results: Vec<TestResult> =
                lines.iter().map(|line| TestResult { test: test(line), arms: vec![] }).collect();
            render(group, arms, None, &results).contains(NOTE)
        };
        let producer = "p rust,python PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0";
        let produce = "t rust,python WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0";
        let eos = "e rust WORKLOAD=transactional TXN_MODE=eos PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=10";
        let consumer = "c rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=10";
        let rust = [Arm::Rust, Arm::Librdkafka];
        assert!(noted(Group::Rust, &rust, &[producer]));
        assert!(noted(Group::Rust, &rust, &[produce]));
        assert!(noted(Group::Rust, &rust, &[consumer, producer]));
        // The eos harness copies each record it consumed; a consumer
        // produces nothing.
        assert!(!noted(Group::Rust, &rust, &[eos]));
        assert!(!noted(Group::Rust, &rust, &[consumer]));
        // Without the librdkafka (C) client, or in the Python group, whose
        // librdkafka clients copy.
        assert!(!noted(Group::Rust, &[Arm::Rust, Arm::Java], &[producer]));
        assert!(!noted(Group::Python, Group::Python.arms(), &[producer, produce]));
    }

    #[test]
    fn markdown_marks_a_percentile_past_the_histogram() {
        let results = vec![TestResult {
            test: test("txn rust WORKLOAD=transactional PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(
                    Arm::Rust,
                    Outcome::Passed,
                    Some(Metrics { p999: 10_001.0, max: 25_000.0, ..txn_metrics("rust-txn", 1_000.0, 10_001.0) }),
                ),
                arm(
                    Arm::Librdkafka,
                    Outcome::Passed,
                    Some(Metrics { p999: 10_000.0, ..txn_metrics("librdkafka-txn", 1_000.0, 50.0) }),
                ),
            ],
        }];
        let md = render(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None, &results);
        // The top bucket holds every latency from 10,001 ms; the one below it
        // and the exact maximum are what they say.
        assert_eq!(
            line_starting(&md, "| Latency p99.9 (ms) |"),
            "| Latency p99.9 (ms) | ≥ 10,001.00 | 10,000.00 | 0.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Commit latency p99 (ms) |"),
            "| Commit latency p99 (ms) | ≥ 10,001.00 | 50.00 | 🔴 +19902.0% |"
        );
        assert_eq!(
            line_starting(&md, "| Latency max (ms) |"),
            "| Latency max (ms) | 25,000.00 | 20.00 | 🔴 +124900.0% |"
        );
        let xml = junit(&results);
        assert!(xml.contains("p99.9=≥10001.00ms max=25000.00ms"), "{xml}");
        assert!(xml.contains("commit_p99=≥10001.00ms"), "{xml}");
        assert!(xml.contains("p99.9=10000.00ms max=20.00ms"), "{xml}");

        // A consumer's histogram goes on to 600,000 ms.
        let consumer = |p99: f64| TestResult {
            test: test("c rust WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=10000"),
            arms: vec![arm(
                Arm::Rust,
                Outcome::Passed,
                Some(metrics("rust-consumer", 10_000.0, p99)),
            )],
        };
        let p99 = |p99: f64| {
            let md = render(Group::Rust, &[Arm::Rust], None, &[consumer(p99)]);
            line_starting(&md, "| Latency p99 (ms) |").to_string()
        };
        assert_eq!(p99(10_001.0), "| Latency p99 (ms) | 10,001.00 |");
        assert_eq!(p99(600_000.0), "| Latency p99 (ms) | ≥ 600,000.00 |");
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
        // 10,000 msg/s rate, and 4,000 does not.
        assert_eq!(line_starting(&md, "| Status |"), "| Status | passed | below rate |  |");
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
        assert_eq!(line_starting(&md, "| Status |"), "| Status | passed | failed |  |");
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
                consumer("java-consumer", 10_000.0, 9_400.0, Arm::Java),
            ],
        }];
        let all = [Arm::Rust, Arm::Librdkafka, Arm::Java];
        let md = render(Group::Rust, &all, None, &results);
        // Rust's load fell short, so its shortfall is the load's; librdkafka's
        // load held, so its shortfall is the client's. Java held the rate,
        // whatever its load averaged.
        assert_eq!(
            line_starting(&md, "| Status |"),
            "| Status | load short | below rate | passed |  |  |"
        );
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
                 RSS and messages so far; the full report replaces this one when the test finishes.\n\n> In \
                 producer and transactional (produce) tests"
            ),
            "{md}"
        );
        assert!(md.contains("every other client makes.\n\n### Results\n"), "{md}");
        assert_eq!(
            line_starting(&md, "| Status |"),
            "| Status | passed | running | waiting |  |  |"
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
        assert!(!md.contains("Notes"), "{md}");
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
            "| Throughput (msg/s) | 1,000 | 1,000 | – | 0.0% | pending |"
        );
        assert_eq!(line_starting(&md, "| Status |"), "| Status | passed | passed | running |  |  |");
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
        let (a, b) = md.split_once("\n### b\n").unwrap();
        // The finished test's missing client did not run; the running test's
        // have not started.
        assert_eq!(line_starting(a, "| Status |"), "| Status | passed | not run |  |");
        assert!(!a.contains("pending"), "{md}");
        assert_eq!(
            line_starting(b, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | – | – | pending |"
        );
        assert_eq!(line_starting(b, "| Status |"), "| Status | waiting | waiting |  |");
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
        // A running client is not judged against the rate until it finishes.
        assert_eq!(
            line_starting(&md, "| Status |"),
            "| Status | failed | running | waiting |  |  |"
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
            "| Throughput (msg/s) | 1,000 | 1,000 | 500 | – | 0.0% | pending |"
        );
        assert_eq!(
            line_starting(&md, "| CPU (%) |"),
            "| CPU (%) | 50.0 | 50.0 | – | – | 0.0% | pending |"
        );
        assert_eq!(
            line_starting(&md, "| Status |"),
            "| Status | passed | passed | running | waiting |  |  |"
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
