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
    docker_output, format_duration, is_harness_broker_name, load_results, now_utc, parse_matrix, parse_size_label,
    plan_runs, read_plan, scenario_of, split_list, Options, RunResult, PLAN_FILE,
};

const MATRIX_ROOT: &str = "target/chaos-matrix";
/// The file, in the output directory, holding the pid of the runner working
/// on it (first line) and, while a run is executing, `child <pgid>`: the
/// process group of that run.
pub const PID_FILE: &str = "runner.pid";

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

/// The planned runs in execution order, each with the chaos flags it runs
/// with when the runner recorded them: `plan.txt` when the runner wrote one,
/// else rebuilt from the copied matrix and the recorded dimensions.
type Plan = Vec<(String, Option<String>)>;

fn planned_runs(out: &Path) -> anyhow::Result<(Plan, BTreeMap<String, String>)> {
    let text = fs::read_to_string(out.join("matrix.txt")).context("reading matrix.txt")?;
    let scenarios = parse_matrix(&text)?;
    let descriptions: BTreeMap<String, String> =
        scenarios.iter().map(|s| (s.id.clone(), s.description.clone())).collect();
    if out.join(PLAN_FILE).is_file() {
        return Ok((read_plan(out), descriptions));
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
    let runs = plan_runs(&scenarios, &options)?.iter().map(|p| (p.run_id(), None)).collect();
    Ok((runs, descriptions))
}

/// The run the runner last started and has not finished, from `matrix.log`.
/// Decided from the log alone: during a rerun, `results.tsv` still holds the
/// previous attempt's row until the new one finishes.
fn current_run(log: &str) -> Option<String> {
    let mut lines = log.lines().rev();
    let line = lines.by_ref().find(|l| l.ends_with("— starting"))?;
    let id = line.split_once("] ")?.1.split(" — ").next()?.to_string();
    // Every later line is newer; the run's `<outcome> in <N>s (delivered …)`
    // line among them means it finished.
    let prefix = format!("] {id} — ");
    let finished = log
        .lines()
        .rev()
        .take_while(|l| *l != line)
        .any(|l| l.split_once(&prefix).is_some_and(|(_, rest)| rest.contains("s (delivered ")));
    (!finished).then_some(id)
}

/// The line that ended the latest runner session (`matrix finished`, `stop
/// requested`, or `interrupted by <signal>`), if that session ended; a marker
/// from an earlier session of a resumed matrix does not count.
fn session_end_marker(log: &str) -> Option<&str> {
    for line in log.lines().rev() {
        let message = line.split_once(' ').map_or(line, |x| x.1);
        // Every session starts with `matrix <file> — N scenario(s), …`.
        if message.starts_with("matrix ") && message.contains(" scenario(s), ") {
            return None;
        }
        if message.starts_with("matrix finished")
            || message.starts_with("stop requested;")
            || message.starts_with("interrupted by ")
        {
            return Some(line);
        }
    }
    None
}

/// The pid of the runner working on `out`, from the pid file it writes (its
/// first line), if that process is still a chaos-matrix runner.
fn runner_pid(out: &Path) -> Option<String> {
    let text = fs::read_to_string(out.join(PID_FILE)).ok()?;
    let pid = text.lines().next()?.trim().to_string();
    is_live_runner(&pid).then_some(pid)
}

/// Whether `pid` is a live chaos-matrix runner other than this process.
pub(super) fn is_live_runner(pid: &str) -> bool {
    if pid.parse::<u32>().map_or(true, |pid| pid == std::process::id()) {
        return false;
    }
    Command::new("ps")
        .args(["-o", "args=", "-p", pid])
        .output()
        .is_ok_and(|output| is_runner_command(&String::from_utf8_lossy(&output.stdout)))
}

/// Whether a process command line is a `chaos-matrix` runner (and not, say,
/// an unrelated process that later reused a stale pid).
fn is_runner_command(args: &str) -> bool {
    args.split_whitespace().any(|a| a == "chaos-matrix")
}

fn running_brokers() -> Vec<String> {
    docker_output(&["ps", "--format", "{{.Names}}"])
        .unwrap_or_default()
        .lines()
        .filter(|n| is_harness_broker_name(n))
        .map(|n| n.split('-').take(2).collect::<Vec<_>>().join("-"))
        .collect()
}

fn format_long(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format_duration(secs)
    }
}

/// The planned runs that have a result, with it. A result recorded with other
/// chaos flags than the plan lists is of a run that no longer counts (the
/// runner runs it again); when either side does not say, it counts.
fn finished_runs<'a>(plan: &'a Plan, results: &'a BTreeMap<String, RunResult>) -> Vec<(&'a String, &'a RunResult)> {
    plan.iter()
        .filter_map(|(id, args)| results.get(id).map(|r| (id, args, r)))
        .filter(|(_, args, r)| args.as_deref().is_none_or(|args| r.ran_with(args)))
        .map(|(id, _, r)| (id, r))
        .collect()
}

fn render(out: &Path) -> anyhow::Result<String> {
    let results = load_results(&out.join("results.tsv"))?;
    let (plan, descriptions) = planned_runs(out)?;
    let log = fs::read_to_string(out.join("matrix.log")).unwrap_or_default();
    let finished = finished_runs(&plan, &results);
    let passed = finished.iter().filter(|(_, r)| r.outcome == "PASS").count();
    let remaining = plan.len() - finished.len();
    let pid = runner_pid(out);
    let finished_marker = session_end_marker(&log);

    let mut s = String::new();
    s.push_str(&format!("Chaos matrix  {}\n", out.display()));
    s.push_str(&format!("Now           {}\n", now_utc()));
    let runner = if let Some(pid) = &pid {
        format!("running (pid {pid})")
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

    let current = if pid.is_none() { None } else { current_run(&log) };
    let mut current_elapsed = 0;
    if let Some(id) = &current {
        let position = plan.iter().position(|(p, _)| p == id).map(|n| n + 1).unwrap_or(0);
        let scenario = scenario_of(id);
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
    fn the_current_run_is_the_last_started_and_unfinished() {
        let log = "t [1/3] 1-plaintext-100B — a — starting\n\
                   t [1/3] 1-plaintext-100B — PASS in 5s (delivered 10, lost 0, 2 records/s)\n\
                   t [2/3] 2-plaintext-100B — b — starting\n";
        assert_eq!(current_run(log).as_deref(), Some("2-plaintext-100B"));
        let finished = format!("{log}t [2/3] 2-plaintext-100B — FAIL in 9s (delivered 5, lost 1, 1 records/s)\n");
        assert_eq!(current_run(&finished), None);
        // A retry after the known defect is not the end of the run.
        let retrying = format!(
            "{log}t [2/3] 2-plaintext-100B — attempt 1/3 hit the known recreate defect; retrying (logs kept in x)\n"
        );
        assert_eq!(current_run(&retrying).as_deref(), Some("2-plaintext-100B"));
    }

    #[test]
    fn a_rerun_is_current_although_its_previous_attempt_is_recorded() {
        // Session 1 recorded a FAIL; session 2 (`--rerun-failed`) started it
        // again. Only the log tells the rerun is in progress.
        let log = "t1 matrix m.txt — 2 scenario(s), up to 1 protocol(s), 1 size(s): 2 run(s) of 2 in the matrix; 0 already recorded\n\
                   t1 [1/2] 1-plaintext-100B — a — starting\n\
                   t1 [1/2] 1-plaintext-100B — FAIL in 5s (delivered 10, lost 1, 2 records/s)\n\
                   t1 [2/2] 2-plaintext-100B — b — starting\n\
                   t1 [2/2] 2-plaintext-100B — PASS in 5s (delivered 10, lost 0, 2 records/s)\n\
                   t1 matrix finished: 1/2 passed, 0 known-defect; summary in s\n\
                   t2 matrix m.txt — 2 scenario(s), up to 1 protocol(s), 1 size(s): 2 run(s) of 2 in the matrix; 2 already recorded\n\
                   t2 [1/2] 1-plaintext-100B — a — starting\n";
        assert_eq!(current_run(log).as_deref(), Some("1-plaintext-100B"));
        // And session 1's end marker does not describe session 2.
        assert_eq!(session_end_marker(log), None);
        // STOP is honoured between runs only: the rerun finishes, then the
        // next run (here a `--rerun` of the passed one) is not started.
        let stopped = format!(
            "{log}t2 [1/2] 1-plaintext-100B — PASS in 6s (delivered 10, lost 0, 2 records/s)\n\
             t2 stop requested; 2-plaintext-100B and later runs not started — re-run to resume\n"
        );
        assert_eq!(current_run(&stopped), None);
        assert!(session_end_marker(&stopped).is_some_and(|l| l.starts_with("t2 stop requested;")));
        // A STOP that came during the last run is consumed and reported, but
        // the session still finished.
        let finished = format!(
            "{log}t2 [1/2] 1-plaintext-100B — PASS in 6s (delivered 10, lost 0, 2 records/s)\n\
             t2 stop requested during the last run; nothing was left to stop\n\
             t2 matrix finished: 2/2 passed, 0 known-defect; summary in s\n"
        );
        assert!(session_end_marker(&finished).is_some_and(|l| l.starts_with("t2 matrix finished")));
        let interrupted = format!(
            "{log}t2 interrupted by SIGINT: 1-plaintext-100B was stopped and is not recorded — re-run the same \
             command to resume\n"
        );
        assert!(session_end_marker(&interrupted).is_some_and(|l| l.starts_with("t2 interrupted by SIGINT")));
    }

    #[test]
    fn a_blank_plan_line_is_no_run_and_other_flags_are_no_result() {
        let out = std::env::temp_dir().join(format!("chaos-matrix-status-plan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        fs::create_dir_all(&out).unwrap();
        fs::write(out.join("matrix.txt"), "1 | a | --cycles 1\n2 | b | --cycles 2\n").unwrap();
        // An empty plan once wrote "\n", which read back as one phantom run.
        fs::write(out.join(PLAN_FILE), "\n").unwrap();
        assert!(planned_runs(&out).unwrap().0.is_empty());
        fs::write(
            out.join(PLAN_FILE),
            "1-plaintext-100B\t--cycles 1 --rps 1000\n\n2-plaintext-100B\t--cycles 2 --rps 1000\n",
        )
        .unwrap();
        let (plan, _) = planned_runs(&out).unwrap();
        assert_eq!(plan.len(), 2);
        let row = |args: &str| {
            let mut result = RunResult { outcome: "PASS".into(), ..RunResult::default() };
            result.columns.insert("chaos_args".into(), args.into());
            result
        };
        let results: BTreeMap<String, RunResult> = [
            ("1-plaintext-100B".to_string(), row("--cycles 1 --rps 1000")),
            // Recorded at another --rps: the runner runs it again.
            ("2-plaintext-100B".to_string(), row("--cycles 2 --rps 50")),
        ]
        .into_iter()
        .collect();
        let finished: Vec<&String> = finished_runs(&plan, &results).into_iter().map(|(id, _)| id).collect();
        assert_eq!(finished, ["1-plaintext-100B"]);
        fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn the_runner_pid_is_the_first_line_of_the_pid_file() {
        let out = std::env::temp_dir().join(format!("chaos-matrix-status-pid-{}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        fs::create_dir_all(&out).unwrap();
        // This test process is no chaos-matrix runner, so it does not count,
        // whatever the second line says.
        fs::write(out.join(PID_FILE), format!("{}\nchild 1\n", std::process::id())).unwrap();
        assert_eq!(runner_pid(&out), None);
        assert!(!is_live_runner("not-a-pid"));
        fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn only_a_chaos_matrix_process_counts_as_the_runner() {
        assert!(is_runner_command(
            "target/debug/xtask chaos-matrix --out o --matrix my-matrix.txt\n"
        ));
        assert!(!is_runner_command("target/debug/xtask chaos-matrix-status --watch 30"));
        assert!(!is_runner_command("/usr/bin/vim notes.txt"));
        assert!(!is_runner_command(""));
    }

    #[test]
    fn scenario_ids_may_contain_dashes() {
        assert_eq!(scenario_of("1-plaintext-100B"), "1");
        assert_eq!(scenario_of("broker-roll-sasl_ssl-1MiB"), "broker-roll");
        assert_eq!(scenario_of("junk"), "");
    }
}
