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
//! The markdown has one table per test: a row per metric, a column per client
//! that ran, and after each baseline a Δ column giving the change of the
//! Rust-based client against it, banded:
//!
//! - 🟢 better, or at most 10 % worse;
//! - 🟡 10–20 % worse;
//! - 🔴 more than 20 % worse.
//!
//! "Worse" follows the metric: lower throughput or transactions/s, higher
//! latency, CPU or RSS. Counts (messages, transactions) get no Δ.

use std::fmt::Write as _;

use super::{Arm, ArmResult, Group, Metrics, Outcome, PerfTest, RunInfo, TestResult, Workload};

/// Above this share of the cluster's ingress limit a result may be capped by
/// the cluster rather than by the client.
const NEAR_INGRESS_SHARE: f64 = 0.85;

/// Below this share of a fixed-rate test's target a client did not hold the
/// rate, so its latency is not comparable with one that did.
const HELD_RATE_SHARE: f64 = 0.95;

const MIB: f64 = 1024.0 * 1024.0;

/// The comparisons of a group: `(candidate, baseline, Δ column header)`. The
/// candidate runs before its baseline.
pub(crate) fn comparisons(group: Group) -> &'static [(Arm, Arm, &'static str)] {
    match group {
        Group::Rust => &[
            (Arm::Rust, Arm::Librdkafka, "Δ Rust vs librdkafka"),
            (Arm::Rust, Arm::Java, "Δ Rust vs Java"),
        ],
        Group::Python => &[
            (Arm::PythonSyncRust, Arm::PythonSyncLibrdkafka, "Δ sync: Rust vs librdkafka"),
            (Arm::PythonAsyncRust, Arm::PythonAsyncLibrdkafka, "Δ async: Rust vs librdkafka"),
        ],
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
}

const fn row(
    label: &'static str,
    value: fn(&Metrics) -> Option<f64>,
    decimals: usize,
    better: Option<Better>,
    transactional: bool,
) -> Row {
    Row { label, value, decimals, better, transactional }
}

const ROWS: &[Row] = &[
    row("Throughput (msg/s)", |m| Some(m.msg_s), 0, Some(Better::Higher), false),
    row("Throughput (MiB/s)", |m| Some(m.mib_s), 2, Some(Better::Higher), false),
    row(
        "Transactions/s",
        |m| m.txn.as_ref().map(|t| t.per_s),
        2,
        Some(Better::Higher),
        true,
    ),
    row("Latency avg (ms)", |m| Some(m.avg), 2, Some(Better::Lower), false),
    row("Latency p50 (ms)", |m| Some(m.p50), 2, Some(Better::Lower), false),
    row("Latency p90 (ms)", |m| Some(m.p90), 2, Some(Better::Lower), false),
    row("Latency p95 (ms)", |m| Some(m.p95), 2, Some(Better::Lower), false),
    row("Latency p99 (ms)", |m| Some(m.p99), 2, Some(Better::Lower), false),
    row("Latency p99.9 (ms)", |m| Some(m.p999), 2, Some(Better::Lower), false),
    row("Latency max (ms)", |m| Some(m.max), 2, Some(Better::Lower), false),
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
    row("CPU (%)", |m| Some(m.cpu_pct), 1, Some(Better::Lower), false),
    row("RSS (MiB)", |m| Some(m.rss_mib), 1, Some(Better::Lower), false),
    row("Messages measured", |m| Some(m.messages), 0, None, false),
    row("Committed transactions", |m| m.txn.as_ref().map(|t| t.committed), 0, None, true),
    row("Aborted transactions", |m| m.txn.as_ref().map(|t| t.aborted), 0, None, true),
];

/// A column of a test's table after the metric names.
enum Column {
    Client(Arm),
    Delta {
        candidate: Arm,
        baseline: Arm,
        header: &'static str,
    },
}

/// The run's clients in order, each baseline followed by its Δ column when
/// its candidate ran too.
fn columns(info: &RunInfo) -> Vec<Column> {
    let mut columns = Vec::new();
    for &arm in &info.arms {
        columns.push(Column::Client(arm));
        for &(candidate, baseline, header) in comparisons(info.group) {
            if baseline == arm && info.arms.contains(&candidate) {
                columns.push(Column::Delta { candidate, baseline, header });
            }
        }
    }
    columns
}

/// A one-line description of a test's settings.
pub(crate) fn scenario(test: &PerfTest) -> String {
    let workload = match test.workload {
        Workload::Producer => "producer".to_string(),
        Workload::Transactional => format!("transactional ({})", test.setting("TXN_MODE").unwrap_or("produce")),
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
    let units = [
        ("KEY_SIZE", "key", " B"),
        ("ACKS", "acks", ""),
        ("COMPRESSION_TYPE", "compression", ""),
        ("BATCH_SIZE", "batch", " KiB"),
        ("LINGER_MS", "linger", " ms"),
        ("ENABLE_IDEMPOTENCE", "idempotence", ""),
        ("MAX_IN_FLIGHT", "in-flight", ""),
        ("BUFFER_MEMORY", "buffer", " MiB"),
        ("MAX_REQUEST_SIZE", "max request", " KiB"),
        ("RECORDS_PER_TRANSACTION", "records per transaction", ""),
        ("ABORT_RATE", "abort rate", ""),
        ("NUM_TRANSACTIONAL_PRODUCERS", "transactional producers", ""),
    ];
    for (key, label, unit) in units {
        if let Some(v) = test.setting(key) {
            parts.push(format!("{label} {v}{unit}"));
        }
    }
    parts.join(", ")
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
    let band = if worse <= 0.10 {
        "🟢"
    } else if worse <= 0.20 {
        "🟡"
    } else {
        "🔴"
    };
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

fn mb_s(m: &Metrics) -> f64 {
    m.mib_s * MIB / 1e6
}

pub(crate) fn markdown(info: &RunInfo, results: &[TestResult]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Performance test results: {} client group\n", info.group.name());
    let _ = writeln!(out, "- Started: {}", info.started);
    let labels: Vec<&str> = info.arms.iter().map(|a| a.label()).collect();
    let _ = writeln!(out, "- Clients: {}", labels.join(", "));
    let _ = writeln!(
        out,
        "- Each client: {} min warmup, then {} min measured",
        info.warmup_min, info.duration_min
    );
    for (label, value) in &info.facts {
        let _ = writeln!(out, "- {label}: {}", cell(value));
    }
    let columns = columns(info);
    if columns.iter().any(|c| matches!(c, Column::Delta { .. })) {
        let _ = writeln!(
            out,
            "\nA Δ column gives the Rust-based client's change against the baseline it names: 🟢 better or at most \
             10% worse, 🟡 10–20% worse, 🔴 more than 20% worse (lower throughput or transactions/s; higher latency, \
             CPU or RSS)."
        );
    }
    if results.is_empty() {
        let _ = writeln!(out, "\nNo test has finished yet.");
    }

    for result in results {
        test_table(&mut out, info, &columns, result);
    }
    out
}

/// A test's section: its id, settings, table and notes.
fn test_table(out: &mut String, info: &RunInfo, columns: &[Column], result: &TestResult) {
    let test = &result.test;
    let _ = writeln!(out, "\n## {}\n\n{}\n", test.id, cell(&scenario(test)));
    let headers: Vec<&str> = columns
        .iter()
        .map(|c| match c {
            Column::Client(arm) => arm.label(),
            Column::Delta { header, .. } => header,
        })
        .collect();
    let _ = writeln!(out, "| Metric | {} |", headers.join(" | "));
    let _ = writeln!(out, "|---|{}", "---:|".repeat(columns.len()));

    let results: Vec<String> = columns
        .iter()
        .map(|c| match c {
            Column::Client(arm) => match find(&result.arms, *arm) {
                None => "not run".to_string(),
                Some(a) => match &a.outcome {
                    Outcome::Passed => "✅".to_string(),
                    Outcome::Failed(reason) => format!("❌ {}", cell(reason)),
                },
            },
            Column::Delta { .. } => String::new(),
        })
        .collect();
    let _ = writeln!(out, "| Result | {} |", results.join(" | "));

    let transactional = test.workload == Workload::Transactional;
    for row in ROWS.iter().filter(|r| transactional || !r.transactional) {
        let cells: Vec<String> = columns
            .iter()
            .map(|c| match c {
                Column::Client(arm) => match metrics_of(&result.arms, *arm).and_then(row.value) {
                    Some(v) => number(v, row.decimals),
                    None => "–".to_string(),
                },
                Column::Delta { candidate, baseline, .. } => {
                    let Some(better) = row.better else {
                        return String::new();
                    };
                    let pair = (
                        metrics_of(&result.arms, *candidate).and_then(row.value),
                        metrics_of(&result.arms, *baseline).and_then(row.value),
                    );
                    match pair {
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
    if transactional {
        notes.push(
            "Throughput counts committed records; a record's latency runs from its send to its transaction's commit."
                .to_string(),
        );
    }
    // The committed rate a fixed-rate test aims at: its aborted transactions'
    // records are sent but not counted.
    let abort_rate: f64 = test.setting("ABORT_RATE").and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let target = test.setting("LIMIT_RPS").and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) * (1.0 - abort_rate);
    for &arm in &info.arms {
        let Some(m) = metrics_of(&result.arms, arm) else {
            continue;
        };
        if let Some(limit) = info.ingress_mb_s {
            if mb_s(m) >= NEAR_INGRESS_SHARE * limit {
                notes.push(format!(
                    "⚠️ {} produced {:.0} MB/s, {:.0}% of the cluster's {limit:.0} MB/s ingress limit: the cluster may \
                     have capped it.",
                    arm.label(),
                    mb_s(m),
                    100.0 * mb_s(m) / limit
                ));
            }
        }
        if target > 0.0 && m.msg_s < HELD_RATE_SHARE * target {
            notes.push(format!(
                "⚠️ {} reached {} msg/s, below the {} msg/s target (capped): its latency is not comparable with a \
                 client that held the rate.",
                arm.label(),
                number(m.msg_s, 0),
                number(target, 0)
            ));
        }
    }
    if !notes.is_empty() {
        out.push('\n');
        for note in notes {
            let _ = writeln!(out, "{note}\n");
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

    use super::super::{parse_tests, TxnMetrics};
    use super::*;

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
        ArmResult { arm, outcome, metrics, elapsed: Duration::from_secs(90) }
    }

    fn info(group: Group, arms: &[Arm], ingress: Option<f64>) -> RunInfo {
        RunInfo {
            group,
            arms: arms.to_vec(),
            duration_min: 30,
            warmup_min: 5,
            started: "2026-10-14T10:00:00Z".to_string(),
            facts: vec![("Cluster".to_string(), "lkc-abc".to_string())],
            ingress_mb_s: ingress,
        }
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
        assert_eq!(delta(85.0, 100.0, Better::Higher).map(|d| d.1), Some("🟡"));
        assert_eq!(delta(79.0, 100.0, Better::Higher).map(|d| d.1), Some("🔴"));
        // Latency: lower is better.
        assert_eq!(delta(5.0, 10.0, Better::Lower).map(|d| d.1), Some("🟢"));
        assert_eq!(delta(11.5, 10.0, Better::Lower).map(|d| d.1), Some("🟡"));
        assert_eq!(delta(12.5, 10.0, Better::Lower).map(|d| d.1), Some("🔴"));
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
        let md = markdown(&info(Group::Rust, &all, Some(600.0)), &results);
        assert!(md.starts_with("# Performance test results: rust client group\n"), "{md}");
        assert!(md.contains("- Clients: Rust (native), librdkafka (C), Java\n"), "{md}");
        assert!(md.contains("- Cluster: lkc-abc"), "{md}");
        assert!(md.contains("A Δ column gives the Rust-based client's change"), "{md}");
        assert!(
            md.contains("## producer-max\n\nproducer, 200 partitions, 1024 B values, unbounded rate\n"),
            "{md}"
        );
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | Rust (native) | librdkafka (C) | Δ Rust vs librdkafka | Java | Δ Rust vs Java |"
        );
        assert_eq!(line_starting(&md, "|---|"), "|---|---:|---:|---:|---:|---:|");
        assert_eq!(
            line_starting(&md, "| Result |"),
            "| Result | ✅ | ✅ |  | ❌ harness exited \\| with 1 |  |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 500,000 | 400,000 | 🟢 +25.0% | – | n/a |"
        );
        assert_eq!(
            line_starting(&md, "| Latency p99 (ms) |"),
            "| Latency p99 (ms) | 9.00 | 10.00 | 🟢 -10.0% | – | n/a |"
        );
        assert_eq!(
            line_starting(&md, "| Messages measured |"),
            "| Messages measured | 30,000,000 | 24,000,000 |  | – |  |"
        );
        // No transaction rows for a producer test.
        assert!(!md.contains("Transactions/s"), "{md}");
        // 500k × 1 KiB/s is 512 MB/s, 85 % of 600.
        assert!(
            md.contains("⚠️ Rust (native) produced 512 MB/s, 85% of the cluster's 600 MB/s ingress limit"),
            "{md}"
        );
        assert!(!md.contains("⚠️ librdkafka"), "{md}");
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
        let md = markdown(&info(Group::Rust, &[Arm::Rust, Arm::Java], None), &results);
        assert!(md.contains("- Clients: Rust (native), Java\n"), "{md}");
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | Rust (native) | Java | Δ Rust vs Java |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | 1,000 | 1,500 | 🔴 -33.3% |"
        );
        assert!(!md.contains("librdkafka"), "{md}");
        assert_rectangular(&md);

        // Without a candidate and its baseline there is no Δ, nor its legend.
        let md = markdown(&info(Group::Rust, &[Arm::Librdkafka, Arm::Java], None), &[]);
        assert!(!md.contains("Δ"), "{md}");
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
        let md = markdown(&info(Group::Python, python, None), &results);
        assert_eq!(
            line_starting(&md, "| Metric |"),
            "| Metric | python-sync (Rust) | python-sync (librdkafka) | Δ sync: Rust vs librdkafka | python-async (Rust) \
             | python-async (librdkafka) | Δ async: Rust vs librdkafka |"
        );
        assert_eq!(
            line_starting(&md, "| Result |"),
            "| Result | ❌ interrupted | not run |  | not run | not run |  |"
        );
        assert_eq!(
            line_starting(&md, "| Throughput (msg/s) |"),
            "| Throughput (msg/s) | – | – | n/a | – | – | n/a |"
        );
        assert_rectangular(&md);
        assert!(markdown(&info(Group::Python, python, None), &[]).contains("No test has finished yet."));
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
        let md = markdown(&info(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None), &results);
        assert!(
            md.contains("⚠️ librdkafka (C) reached 9,000 msg/s, below the 10,000 msg/s target (capped)"),
            "{md}"
        );
        assert!(!md.contains("⚠️ Rust (native)"), "{md}");
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
        let md = markdown(&info(Group::Rust, &[Arm::Rust, Arm::Librdkafka], None), &results);
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
        assert!(md.contains("Throughput counts committed records"), "{md}");
        // Half the transactions abort, so 5,000 committed msg/s holds the
        // 10,000 msg/s rate.
        assert!(!md.contains("⚠️ Rust (native)"), "{md}");
        assert!(
            md.contains("⚠️ librdkafka (C) reached 4,000 msg/s, below the 5,000 msg/s target (capped)"),
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
            xml.contains("<testcase classname=\"t\" name=\"librdkafka (C)\" time=\"90.0\">"),
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
