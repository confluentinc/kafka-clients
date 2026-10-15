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
//! The markdown has one table per test with a row per client, then a row per comparison giving the change of
//! the Rust-based client against its baseline, banded:
//!
//! - 🟢 better, or at most 10 % worse;
//! - 🟡 10–20 % worse;
//! - 🔴 more than 20 % worse.
//!
//! "Worse" follows the metric: lower throughput, higher latency, CPU or RSS.

use std::fmt::Write as _;

use super::{Arm, ArmResult, Group, Metrics, Outcome, PerfTest, RunInfo, TestResult};

/// Above this share of the cluster's ingress limit a result may be capped by
/// the cluster rather than by the client.
const NEAR_INGRESS_SHARE: f64 = 0.85;

/// Below this share of a fixed-rate test's target a client did not hold the
/// rate, so its latency is not comparable with one that did.
const HELD_RATE_SHARE: f64 = 0.95;

const MIB: f64 = 1024.0 * 1024.0;

/// The comparisons of a group: `(candidate, baseline)`.
pub(crate) fn comparisons(group: Group) -> &'static [(Arm, Arm)] {
    match group {
        Group::Rust => &[(Arm::Rust, Arm::Librdkafka), (Arm::Rust, Arm::Java)],
        Group::Python => &[
            (Arm::PythonSyncRust, Arm::PythonSyncLibrdkafka),
            (Arm::PythonAsyncRust, Arm::PythonAsyncLibrdkafka),
        ],
    }
}

/// Which way a metric improves.
#[derive(Clone, Copy)]
enum Better {
    Higher,
    Lower,
}

/// A report column: header, how to read it from [`Metrics`], its decimals,
/// and which way is better.
struct Column {
    header: &'static str,
    value: fn(&Metrics) -> f64,
    decimals: usize,
    better: Better,
}

const COLUMNS: &[Column] = &[
    Column { header: "msg/s", value: |m| m.msg_s, decimals: 0, better: Better::Higher },
    Column { header: "MiB/s", value: |m| m.mib_s, decimals: 2, better: Better::Higher },
    Column { header: "avg ms", value: |m| m.avg, decimals: 2, better: Better::Lower },
    Column { header: "p50 ms", value: |m| m.p50, decimals: 2, better: Better::Lower },
    Column { header: "p90 ms", value: |m| m.p90, decimals: 2, better: Better::Lower },
    Column { header: "p95 ms", value: |m| m.p95, decimals: 2, better: Better::Lower },
    Column { header: "p99 ms", value: |m| m.p99, decimals: 2, better: Better::Lower },
    Column { header: "p99.9 ms", value: |m| m.p999, decimals: 2, better: Better::Lower },
    Column { header: "max ms", value: |m| m.max, decimals: 2, better: Better::Lower },
    Column { header: "CPU %", value: |m| m.cpu_pct, decimals: 1, better: Better::Lower },
    Column { header: "RSS MiB", value: |m| m.rss_mib, decimals: 1, better: Better::Lower },
];

/// A one-line description of a test's settings.
pub(crate) fn scenario(test: &PerfTest) -> String {
    let mut parts = vec![
        format!("{} partitions", test.setting("PARTITIONS").unwrap_or("?")),
        format!("{} B values", test.setting("VALUE_SIZE").unwrap_or("?")),
    ];
    match test.setting("LIMIT_RPS") {
        Some("0") | None => parts.push("unbounded rate".to_string()),
        Some(rps) => parts.push(format!("{rps} msg/s")),
    }
    let units = [
        ("KEY_SIZE", "key", " B"),
        ("BATCH_SIZE", "batch", " KiB"),
        ("LINGER_MS", "linger", " ms"),
        ("MAX_IN_FLIGHT", "in-flight", ""),
        ("BUFFER_MEMORY", "buffer", " MiB"),
        ("MAX_REQUEST_SIZE", "max request", " KiB"),
        ("COMPRESSION_TYPE", "compression", ""),
        ("ENABLE_IDEMPOTENCE", "idempotence", ""),
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

fn mb_s(m: &Metrics) -> f64 {
    m.mib_s * MIB / 1e6
}

pub(crate) fn markdown(info: &RunInfo, results: &[TestResult]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Performance test results: {} client group\n", info.group.name());
    let _ = writeln!(out, "- Started: {}", info.started);
    let _ = writeln!(
        out,
        "- Each client: {} min warmup, then {} min measured",
        info.warmup_min, info.duration_min
    );
    for (label, value) in &info.facts {
        let _ = writeln!(out, "- {label}: {}", cell(value));
    }
    let _ = writeln!(
        out,
        "\nComparison rows give the change against the baseline: 🟢 better or at most 10% worse, 🟡 10–20% worse, \
         🔴 more than 20% worse (lower throughput; higher latency, CPU or RSS)."
    );
    if results.is_empty() {
        let _ = writeln!(out, "\nNo test has finished yet.");
    }

    let header: Vec<&str> = COLUMNS.iter().map(|c| c.header).collect();
    for result in results {
        let _ = writeln!(out, "\n## {}\n\n{}\n", result.test.id, scenario(&result.test));
        let _ = writeln!(out, "| Client | {} | Result |", header.join(" | "));
        let _ = writeln!(out, "|---|{}---|", "---:|".repeat(COLUMNS.len()));
        let mut notes = Vec::new();
        for arm in info.group.arms() {
            let Some(a) = find(&result.arms, *arm) else {
                let _ = writeln!(out, "| {} |{} not run |", arm.label(), " – |".repeat(COLUMNS.len()));
                continue;
            };
            let values: Vec<String> = match &a.metrics {
                Some(m) => COLUMNS.iter().map(|c| format!("{:.*}", c.decimals, (c.value)(m))).collect(),
                None => vec!["–".to_string(); COLUMNS.len()],
            };
            let status = match &a.outcome {
                Outcome::Passed => "✅".to_string(),
                Outcome::Failed(reason) => format!("❌ {}", cell(reason)),
            };
            let _ = writeln!(out, "| {} | {} | {status} |", arm.label(), values.join(" | "));
            if let (Some(m), Some(limit)) = (&a.metrics, info.ingress_mb_s) {
                if mb_s(m) >= NEAR_INGRESS_SHARE * limit {
                    notes.push(format!(
                        "⚠️ {} produced {:.0} MB/s, {:.0}% of the cluster's {limit:.0} MB/s ingress limit: the cluster \
                         may have capped it.",
                        arm.label(),
                        mb_s(m),
                        100.0 * mb_s(m) / limit
                    ));
                }
            }
            let target: f64 = result.test.setting("LIMIT_RPS").and_then(|v| v.parse().ok()).unwrap_or(0.0);
            if let Some(m) = &a.metrics {
                if target > 0.0 && m.msg_s < HELD_RATE_SHARE * target {
                    notes.push(format!(
                        "⚠️ {} reached {:.0} msg/s, below the {target:.0} msg/s target (capped): its latency is not \
                         comparable with a client that held the rate.",
                        arm.label(),
                        m.msg_s
                    ));
                }
            }
        }
        for (candidate, baseline) in comparisons(info.group) {
            let pair = (
                find(&result.arms, *candidate).and_then(|a| a.metrics.as_ref()),
                find(&result.arms, *baseline).and_then(|a| a.metrics.as_ref()),
            );
            let cells: Vec<String> = match pair {
                (Some(c), Some(b)) => COLUMNS
                    .iter()
                    .map(|col| match delta((col.value)(c), (col.value)(b), col.better) {
                        Some((pct, band)) => format!("{band} {pct:+.1}%"),
                        None => "n/a".to_string(),
                    })
                    .collect(),
                _ => vec!["n/a".to_string(); COLUMNS.len()],
            };
            let _ = writeln!(
                out,
                "| **{} vs {}** | {} | |",
                candidate.label(),
                baseline.label(),
                cells.join(" | ")
            );
        }
        if !notes.is_empty() {
            out.push('\n');
            for note in notes {
                let _ = writeln!(out, "{note}");
            }
        }
    }
    out
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
                format!(
                    "client={} messages={:.0} msg/s={:.0} MiB/s={:.2} p50={:.2}ms p99={:.2}ms p99.9={:.2}ms \
                     max={:.2}ms cpu={:.1}% rss={:.1}MiB",
                    m.client, m.messages, m.msg_s, m.mib_s, m.p50, m.p99, m.p999, m.max, m.cpu_pct, m.rss_mib
                )
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

    use super::super::parse_tests;
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
        }
    }

    fn arm(arm: Arm, outcome: Outcome, metrics: Option<Metrics>) -> ArmResult {
        ArmResult { arm, outcome, metrics, elapsed: Duration::from_secs(90) }
    }

    fn info(group: Group, ingress: Option<f64>) -> RunInfo {
        RunInfo {
            group,
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
    fn formats_durations_and_scenarios() {
        assert_eq!(hours_minutes(45), "45 min");
        assert_eq!(hours_minutes(120), "2 h");
        assert_eq!(hours_minutes(1350), "22 h 30 min");
        let t = test("t rust PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=10000 BATCH_SIZE=1024 COMPRESSION_TYPE=lz4");
        assert_eq!(
            scenario(&t),
            "200 partitions, 1024 B values, 10000 msg/s, batch 1024 KiB, compression lz4"
        );
        let t = test("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0");
        assert_eq!(scenario(&t), "1 partitions, 1 B values, unbounded rate");
    }

    #[test]
    fn markdown_has_rows_comparisons_and_failures() {
        let results = vec![TestResult {
            test: test("producer-max rust PARTITIONS=200 VALUE_SIZE=1024 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 500_000.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Passed, Some(metrics("librdkafka", 400_000.0, 10.0))),
                arm(Arm::Java, Outcome::Failed("harness exited | with 1".to_string()), None),
            ],
        }];
        let md = markdown(&info(Group::Rust, Some(600.0)), &results);
        assert!(md.starts_with("# Performance test results: rust client group\n"), "{md}");
        assert!(md.contains("- Cluster: lkc-abc"), "{md}");
        assert!(
            md.contains("## producer-max\n\n200 partitions, 1024 B values, unbounded rate\n"),
            "{md}"
        );
        assert!(md.contains("| Rust (native) | 500000 | 488.28 | 2.50 |"), "{md}");
        assert!(md.contains("| Java | – |"), "{md}");
        assert!(md.contains("❌ harness exited \\| with 1 |"), "{md}");
        assert!(
            md.contains("| **Rust (native) vs librdkafka (C)** | 🟢 +25.0% | 🟢 +25.0% | 🟢 +0.0% |"),
            "{md}"
        );
        assert!(md.contains("| **Rust (native) vs Java** | n/a |"), "{md}");
        // 500k × 1 KiB/s is 512 MB/s, 85 % of 600.
        assert!(
            md.contains("⚠️ Rust (native) produced 512 MB/s, 85% of the cluster's 600 MB/s ingress limit"),
            "{md}"
        );
        assert!(!md.contains("⚠️ librdkafka"), "{md}");
        // Every table row has the same number of cells as the header.
        let widths: Vec<usize> = md
            .lines()
            .filter(|l| l.starts_with('|') && !l.starts_with("|---"))
            .map(|l| l.matches(" |").count())
            .collect();
        assert!(widths.iter().all(|w| *w == widths[0]), "{widths:?}\n{md}");
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
        let md = markdown(&info(Group::Python, None), &results);
        assert!(md.contains("| python-sync (librdkafka) | – |"), "{md}");
        assert!(md.contains("not run |"), "{md}");
        assert!(
            md.contains("| **python-async (Rust) vs python-async (librdkafka)** | n/a |"),
            "{md}"
        );
        assert!(markdown(&info(Group::Python, None), &[]).contains("No test has finished yet."));
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
        let md = markdown(&info(Group::Rust, None), &results);
        assert!(
            md.contains("⚠️ librdkafka (C) reached 9000 msg/s, below the 10000 msg/s target (capped)"),
            "{md}"
        );
        assert!(!md.contains("⚠️ Rust (native)"), "{md}");
    }

    #[test]
    fn junit_escapes_and_counts() {
        let results = vec![TestResult {
            test: test("t rust PARTITIONS=1 VALUE_SIZE=1 LIMIT_RPS=0"),
            arms: vec![
                arm(Arm::Rust, Outcome::Passed, Some(metrics("rust", 1000.0, 9.0))),
                arm(Arm::Librdkafka, Outcome::Failed("bad <\"&'> \u{1}end".to_string()), None),
            ],
        }];
        let xml = junit(&results);
        assert!(
            xml.contains("<testsuites name=\"perf-cloud\" tests=\"2\" failures=\"1\" errors=\"0\" time=\"180.0\">"),
            "{xml}"
        );
        assert!(xml.contains("<testsuite name=\"t\" tests=\"2\" failures=\"1\""), "{xml}");
        assert!(
            xml.contains("<testcase classname=\"t\" name=\"librdkafka (C)\" time=\"90.0\">"),
            "{xml}"
        );
        assert!(xml.contains("<failure message=\"bad &lt;&quot;&amp;&apos;&gt; end\">"), "{xml}");
        assert!(xml.contains("<system-out>client=rust messages=60000 msg/s=1000"), "{xml}");
        assert_eq!(
            junit(&[]).lines().nth(1),
            Some("<testsuites name=\"perf-cloud\" tests=\"0\" failures=\"0\" errors=\"0\" time=\"0.0\">")
        );
    }
}
