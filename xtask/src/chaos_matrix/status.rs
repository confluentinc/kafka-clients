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

//! `cargo xtask chaos-matrix-status`: a read-only progress view of a matrix
//! run, from the files the runner writes (`results.tsv`, `matrix.log`, the
//! current run's `run.log`). It never touches the runner, its runs, or Docker
//! state; stopping it (Ctrl-C) has no effect on the matrix.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context as _};

use super::{
    format_duration, is_harness_broker_name, load_results, now_utc, parse_matrix, plan_runs, split_list, Options,
    RunResult,
};

const MATRIX_ROOT: &str = "target/chaos-matrix";

/// Entry point for `cargo xtask chaos-matrix-status`.
pub fn run(raw: &[String]) -> anyhow::Result<()> {
    let mut out: Option<PathBuf> = None;
    let mut watch: Option<u64> = None;
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--out" => {
                out = Some(PathBuf::from(raw.get(i + 1).context("--out requires a directory")?));
                i += 2;
            },
            "--watch" => {
                // Optional seconds value.
                match raw.get(i + 1).and_then(|v| v.parse::<u64>().ok()) {
                    Some(secs) => {
                        watch = Some(secs.max(2));
                        i += 2;
                    },
                    None => {
                        watch = Some(30);
                        i += 1;
                    },
                }
            },
            other => bail!("unknown chaos-matrix-status flag: {other} (flags: --out DIR, --watch [SECS])"),
        }
    }
    let out = match out {
        Some(dir) => dir,
        None => latest_matrix_dir()?,
    };
    match watch {
        None => {
            print!("{}", render(&out)?);
        },
        Some(secs) => loop {
            let body = render(&out).unwrap_or_else(|e| format!("error: {e:#}\n"));
            // Clear the screen and home the cursor, then redraw.
            print!("\x1b[2J\x1b[H{body}\n(refreshing every {secs}s — Ctrl-C to quit; the matrix keeps running)\n");
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            std::thread::sleep(Duration::from_secs(secs));
        },
    }
    Ok(())
}

/// The most recently active matrix output directory under `target/chaos-matrix`.
fn latest_matrix_dir() -> anyhow::Result<PathBuf> {
    let entries = fs::read_dir(MATRIX_ROOT).with_context(|| format!("no {MATRIX_ROOT} directory; pass --out DIR"))?;
    entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.join("matrix.log").is_file())
        .max_by_key(|p| modified(&p.join("matrix.log")))
        .with_context(|| format!("no matrix output under {MATRIX_ROOT}; pass --out DIR"))
}

fn modified(path: &Path) -> SystemTime {
    fs::metadata(path).and_then(|m| m.modified()).unwrap_or(SystemTime::UNIX_EPOCH)
}

/// The planned run ids, in execution order: `plan.txt` when the runner wrote
/// one, else rebuilt from the copied matrix and the recorded dimensions.
fn planned_run_ids(out: &Path) -> anyhow::Result<(Vec<String>, BTreeMap<String, String>)> {
    let text = fs::read_to_string(out.join("matrix.txt")).context("reading matrix.txt")?;
    let scenarios = parse_matrix(&text)?;
    let descriptions: BTreeMap<String, String> =
        scenarios.iter().map(|s| (s.id.clone(), s.description.clone())).collect();
    if let Ok(plan) = fs::read_to_string(out.join("plan.txt")) {
        return Ok((plan.lines().map(str::to_string).collect(), descriptions));
    }
    let environment = fs::read_to_string(out.join("environment.txt")).unwrap_or_default();
    let latest = environment.split("---\n").filter(|b| !b.trim().is_empty()).last().unwrap_or("");
    let field = |name: &str| {
        latest
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name}: ")))
            .map(str::to_string)
            .unwrap_or_default()
    };
    let protocols = split_list(&field("protocols"));
    let msg_sizes = split_list(&field("message sizes"))
        .iter()
        .filter_map(|label| parse_size_label(label))
        .collect::<Vec<_>>();
    if protocols.is_empty() || msg_sizes.is_empty() {
        bail!("cannot tell the matrix's protocols and sizes: no plan.txt and no readable environment.txt");
    }
    let options = Options {
        matrix: out.join("matrix.txt"),
        out: out.to_path_buf(),
        protocols,
        msg_sizes,
        rps: 0,
        only: None,
        run_timeout: Duration::ZERO,
        rerun_failed: false,
        rerun: Default::default(),
        check_only: false,
        stall_timeout: Duration::ZERO,
        known_defect_attempts: 1,
    };
    let ids = plan_runs(&scenarios, &options)?.iter().map(|p| p.run_id()).collect();
    Ok((ids, descriptions))
}

/// Inverse of `size_label`: `100B` -> 100, `1MiB` -> 1048576.
fn parse_size_label(label: &str) -> Option<usize> {
    if let Some(mib) = label.strip_suffix("MiB") {
        return mib.parse::<usize>().ok().map(|n| n * 1024 * 1024);
    }
    label.strip_suffix('B')?.parse().ok()
}

/// The run the runner last started and has not recorded, from `matrix.log`.
fn current_run(log: &str, results: &BTreeMap<String, RunResult>) -> Option<String> {
    let line = log.lines().rev().find(|l| l.ends_with("— starting"))?;
    let id = line.split_once("] ")?.1.split(" — ").next()?.to_string();
    (!results.contains_key(&id)).then_some(id)
}

fn runner_pids() -> Vec<String> {
    Command::new("pgrep")
        .args(["-f", "xtask chaos-matrix --matrix"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().map(str::to_string).collect())
        .unwrap_or_default()
}

fn running_brokers() -> Vec<String> {
    Command::new("docker")
        .args(["ps", "--format", "{{.Names}}"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|n| is_harness_broker_name(n))
                .map(|n| n.split('-').take(2).collect::<Vec<_>>().join("-"))
                .collect()
        })
        .unwrap_or_default()
}

fn format_long(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format_duration(secs)
    }
}

fn render(out: &Path) -> anyhow::Result<String> {
    let results = load_results(&out.join("results.tsv"))?;
    let (plan, descriptions) = planned_run_ids(out)?;
    let log = fs::read_to_string(out.join("matrix.log")).unwrap_or_default();
    let finished: Vec<(&String, &RunResult)> = plan.iter().filter_map(|id| results.get(id).map(|r| (id, r))).collect();
    let passed = finished.iter().filter(|(_, r)| r.outcome == "PASS").count();
    let remaining = plan.len() - finished.len();
    let pids = runner_pids();
    let finished_marker = log
        .lines()
        .rev()
        .find(|l| l.contains("matrix finished") || l.contains("stop requested"));

    let mut s = String::new();
    s.push_str(&format!("Chaos matrix  {}\n", out.display()));
    s.push_str(&format!("Now           {}\n", now_utc()));
    let runner = if !pids.is_empty() {
        format!("running (pid {})", pids.join(", "))
    } else if let Some(line) = finished_marker {
        format!("not running — {}", line.split_once(' ').map(|x| x.1).unwrap_or(line))
    } else {
        "NOT RUNNING (stopped without finishing; re-run the same chaos-matrix command to resume)".to_string()
    };
    s.push_str(&format!("Runner        {runner}\n"));
    s.push_str(&format!(
        "Progress      {}/{} finished: {passed} passed, {} not passed, {remaining} to go\n",
        finished.len(),
        plan.len(),
        finished.len() - passed
    ));

    let current = if pids.is_empty() {
        None
    } else {
        current_run(&log, &results)
    };
    let mut current_elapsed = 0;
    if let Some(id) = &current {
        let position = plan.iter().position(|p| p == id).map(|n| n + 1).unwrap_or(0);
        let scenario = id.split('-').next().unwrap_or("");
        s.push_str(&format!(
            "Current       [{position}/{}] {id} — {}\n",
            plan.len(),
            descriptions.get(scenario).map(String::as_str).unwrap_or("")
        ));
        let run_log = out.join("runs").join(id).join("run.log");
        if let Ok(meta) = fs::metadata(&run_log) {
            let started = meta.created().or_else(|_| meta.modified()).unwrap_or(SystemTime::now());
            current_elapsed = SystemTime::now().duration_since(started).map(|d| d.as_secs()).unwrap_or(0);
        }
        let text = fs::read_to_string(&run_log).unwrap_or_default();
        let cycle = text
            .lines()
            .rev()
            .find_map(|l| l.trim().strip_prefix("chaos: cycle "))
            .unwrap_or("-");
        s.push_str(&format!(
            "              running {}, cycle {cycle}\n",
            format_long(current_elapsed)
        ));
        let last = text
            .lines()
            .rev()
            .map(str::trim)
            .find(|l| l.starts_with("chaos:"))
            .unwrap_or("(no harness output yet)");
        let last: String = last.chars().take(110).collect();
        s.push_str(&format!("              last: {last}\n"));
    }

    if remaining > 0 && !finished.is_empty() {
        let average = finished.iter().map(|(_, r)| r.duration_s).sum::<u64>() / finished.len() as u64;
        let left = (average * remaining as u64).saturating_sub(current_elapsed.min(average));
        s.push_str(&format!(
            "Estimate      ~{} left (average {} over {} finished run(s); runs vary a lot by scenario)\n",
            format_long(left),
            format_long(average),
            finished.len()
        ));
    }
    let brokers = running_brokers();
    s.push_str(&format!(
        "Brokers up    {}\n",
        if brokers.is_empty() {
            "none".to_string()
        } else {
            brokers.join(" ")
        }
    ));

    s.push_str("\nRecent runs\n");
    let mut recent: Vec<&(&String, &RunResult)> = finished.iter().collect();
    recent.sort_by_key(|(_, r)| r.get("started").to_string());
    if recent.is_empty() {
        s.push_str("  (none finished yet)\n");
    }
    for (id, r) in recent.iter().rev().take(8) {
        s.push_str(&format!(
            "  {:<18} {:<22} {:>7}  delivered {:>8}  lost {:>4}  {:>5} rec/s\n",
            r.outcome,
            id,
            format_long(r.duration_s),
            r.get("delivered"),
            r.get("lost"),
            r.get("produce_rate")
        ));
    }

    let failures: Vec<_> = finished.iter().filter(|(_, r)| r.outcome != "PASS").collect();
    s.push_str("\nNot passed\n");
    if failures.is_empty() {
        s.push_str("  (none)\n");
    }
    for (id, r) in failures {
        let reason: String = r.reasons.chars().take(140).collect();
        s.push_str(&format!("  {id}  {}  {reason}\n", r.outcome));
        s.push_str(&format!("    logs: {}\n", out.join("runs").join(id).display()));
    }
    s.push_str(&format!("\nFull summary: {}\n", out.join("summary.md").display()));
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_labels_parse_back() {
        assert_eq!(parse_size_label("100B"), Some(100));
        assert_eq!(parse_size_label("1MiB"), Some(1024 * 1024));
        assert_eq!(parse_size_label("junk"), None);
    }

    #[test]
    fn the_current_run_is_the_last_started_and_unrecorded() {
        let log = "t [1/3] 1-plaintext-100B — a — starting\nt [1/3] 1-plaintext-100B — PASS in 5s\n\
                   t [2/3] 2-plaintext-100B — b — starting\n";
        let mut results = BTreeMap::new();
        assert_eq!(current_run(log, &results).as_deref(), Some("2-plaintext-100B"));
        results.insert("2-plaintext-100B".to_string(), RunResult::default());
        assert_eq!(current_run(log, &results), None);
    }
}
