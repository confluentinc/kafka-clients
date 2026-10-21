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

//! The report while a test runs. A thread writes `report.md` every
//! [`INTERVAL`], and as each client starts, with the clients that finished
//! and the numbers so far of the one running (with `--push-artifacts`, it
//! pushes it as the workflow's report too, for Semaphore's Report tab). The
//! test's full report replaces it when the test finishes.
//!
//! A running client's numbers come from what its harness writes as it goes:
//!
//! - the producer and transactional harnesses and the Python consumer write
//!   one line per window (about a second) to `metrics.jsonl` in the arm's
//!   directory: JSON whose leaves are numbers as strings, with the window's
//!   messages, bytes, latency total, count and max, one CPU and one RSS
//!   sample, and the measurement's start and end (`-inf` until set);
//! - the Rust, C and Java consumers print an `[interval N]` line every few
//!   seconds once their warmup is over, which the driver copies into the
//!   arm's `run.log`: the seconds since the measurement started, the
//!   interval's records, latency average and max, CPU and RSS (MiB).
//!
//! Neither has percentiles that add up across windows, so a running client
//! shows throughput, average and maximum latency, CPU, RSS and its records,
//! all over its measurement so far.
//!
//! The cost lands on the agent while a client measures: every [`INTERVAL`],
//! a reread of those files (about 1.5 KiB per second measured) and, with
//! `--push-artifacts`, one upload of the report. Once the run is signalled to
//! stop, the thread writes nothing more.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use super::{push_artifact, report, Arm, ArmResult, PerfTest, RunInfo, TestResult, Workload};
use crate::chaos_matrix::now_utc;

/// How often the report is written while a test runs.
pub(crate) const INTERVAL: Duration = Duration::from_secs(5 * 60);

const MIB: f64 = 1024.0 * 1024.0;

/// What a running client measured so far.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SoFar {
    /// The seconds measured.
    pub(crate) seconds: f64,
    pub(crate) messages: f64,
    pub(crate) bytes: f64,
    pub(crate) latency_avg: Option<f64>,
    pub(crate) latency_max: Option<f64>,
    /// The average of the CPU samples, % of one core.
    pub(crate) cpu_pct: Option<f64>,
    /// The average of the RSS samples.
    pub(crate) rss_mib: Option<f64>,
}

impl SoFar {
    pub(crate) fn msg_s(&self) -> Option<f64> {
        (self.seconds > 0.0).then(|| self.messages / self.seconds)
    }

    pub(crate) fn mib_s(&self) -> Option<f64> {
        (self.seconds > 0.0).then(|| self.bytes / MIB / self.seconds)
    }
}

/// Sums over windows or intervals.
#[derive(Default)]
struct Sums {
    messages: f64,
    bytes: f64,
    latency_sum: f64,
    latency_count: f64,
    latency_max: Option<f64>,
    cpu_sum: f64,
    cpu_count: f64,
    rss_mib_sum: f64,
    rss_count: f64,
}

impl Sums {
    fn latency(&mut self, sum: f64, count: f64, max: Option<f64>) {
        if count > 0.0 && sum.is_finite() {
            self.latency_sum += sum;
            self.latency_count += count;
        }
        if let Some(max) = max.filter(|m| m.is_finite()) {
            self.latency_max = Some(self.latency_max.map_or(max, |m| m.max(max)));
        }
    }

    fn cpu(&mut self, pct: Option<f64>) {
        if let Some(pct) = pct.filter(|p| p.is_finite()) {
            self.cpu_sum += pct;
            self.cpu_count += 1.0;
        }
    }

    fn rss(&mut self, mib: Option<f64>) {
        if let Some(mib) = mib.filter(|m| m.is_finite()) {
            self.rss_mib_sum += mib;
            self.rss_count += 1.0;
        }
    }

    fn so_far(self, seconds: f64) -> SoFar {
        let average = |sum: f64, count: f64| (count > 0.0).then(|| sum / count);
        SoFar {
            seconds: seconds.max(0.0),
            messages: self.messages,
            bytes: self.bytes,
            latency_avg: average(self.latency_sum, self.latency_count),
            latency_max: self.latency_max,
            cpu_pct: average(self.cpu_sum, self.cpu_count),
            rss_mib: average(self.rss_mib_sum, self.rss_count),
        }
    }
}

/// The lines of `text` that are whole: a harness may be writing the last one.
fn complete_lines(text: &str) -> std::str::Lines<'_> {
    text.rfind('\n').map_or("", |end| &text[..end]).lines()
}

/// The number at `path` in `v`, whether written as a number or as a string
/// (`"12.5"`, `"1.2E7"`, `"-inf"`).
fn number_at(v: &serde_json::Value, path: &[&str]) -> Option<f64> {
    let v = path.iter().try_fold(v, |v, key| v.get(key))?;
    v.as_f64().or_else(|| v.as_str()?.trim().parse().ok())
}

/// What the windows of a `metrics.jsonl` hold so far; `None` before the
/// measurement started. Messages and latency count from every window of the
/// measurement (a producer's last acks can land in the window that ends it),
/// CPU and RSS only from the windows before its end, as the harnesses'
/// averages do.
fn from_windows(text: &str) -> Option<SoFar> {
    let mut start = None;
    let mut end = None;
    let mut last_window_end = None;
    let mut sums = Sums::default();
    for line in complete_lines(text) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(started) = number_at(&v, &["measurement_start_ms"]).filter(|t| t.is_finite()) else {
            continue;
        };
        start = Some(started);
        let ended = number_at(&v, &["measurement_end_ms"]).filter(|t| t.is_finite());
        end = ended.or(end);
        last_window_end = number_at(&v, &["window_end_ms"]).or(last_window_end);
        sums.messages += number_at(&v, &["messages", "total"]).unwrap_or(0.0);
        sums.bytes += number_at(&v, &["bytes", "total"]).unwrap_or(0.0);
        let count = number_at(&v, &["latency", "count"]).unwrap_or(0.0);
        if count > 0.0 {
            sums.latency(
                number_at(&v, &["latency", "total"]).unwrap_or(f64::NAN),
                count,
                number_at(&v, &["latency", "max"]),
            );
        }
        if ended.is_none() {
            sums.cpu(number_at(&v, &["cpu", "average"]));
            sums.rss(number_at(&v, &["rss", "average"]).map(|bytes| bytes / MIB));
        }
    }
    let start = start?;
    let until = end.or(last_window_end)?;
    Some(sums.so_far((until - start) / 1000.0))
}

/// The number after `key` in `line`, past any padding (`t=  10.0s` → 10.0).
fn field(line: &str, key: &str) -> Option<f64> {
    let rest = line[line.find(key)? + key.len()..].trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || matches!(c, '.' | '-' | '+')))
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// What the `[interval N] t=… msgs=… thr=… msg/s avg=… p50=… p99=… p999=…
/// max=… ms cpu=…% rss=…MB` lines in `log` hold so far, at `record_bytes`
/// per record; `None` before the first. Each line is one interval: its
/// average latency is weighted by its records.
fn from_interval_lines(log: &str, record_bytes: f64) -> Option<SoFar> {
    let mut seconds = None;
    let mut sums = Sums::default();
    for line in complete_lines(log) {
        let Some(line) = line.trim_start().strip_prefix("[interval ") else {
            continue;
        };
        let (Some(t), Some(msgs)) = (field(line, " t="), field(line, " msgs=")) else {
            continue;
        };
        seconds = Some(t);
        sums.messages += msgs;
        sums.bytes += msgs * record_bytes;
        if msgs > 0.0 {
            let avg = field(line, " avg=").unwrap_or(f64::NAN);
            sums.latency(avg * msgs, msgs, field(line, " max="));
        }
        sums.cpu(field(line, " cpu="));
        sums.rss(field(line, " rss="));
    }
    Some(sums.so_far(seconds?))
}

/// Where a running client's numbers so far are.
enum Source {
    /// A `metrics.jsonl` of windows.
    Windows(PathBuf),
    /// The `[interval N]` lines of a `run.log`, at `record_bytes` per record.
    IntervalLines { log: PathBuf, record_bytes: f64 },
}

/// The client running now.
pub(super) struct RunningArm {
    arm: Arm,
    source: Source,
}

impl RunningArm {
    /// `arm` running `test` in `dir`, its directory.
    pub(super) fn new(test: &PerfTest, arm: Arm, dir: &Path) -> RunningArm {
        let source = match (test.workload, arm) {
            // Their load's records have no key.
            (Workload::Consumer, Arm::Rust | Arm::Librdkafka | Arm::Java) => {
                Source::IntervalLines { log: dir.join("run.log"), record_bytes: test.number("VALUE_SIZE") as f64 }
            },
            _ => Source::Windows(dir.join("metrics.jsonl")),
        };
        RunningArm { arm, source }
    }

    fn so_far(&self) -> Option<SoFar> {
        let read = |path: &Path| fs::read(path).ok().map(|b| String::from_utf8_lossy(&b).into_owned());
        match &self.source {
            Source::Windows(path) => from_windows(&read(path)?),
            Source::IntervalLines { log, record_bytes } => from_interval_lines(&read(log)?, *record_bytes),
        }
    }
}

/// The run's results as they come.
pub(super) struct Progress {
    pub(super) info: RunInfo,
    pub(super) results: Vec<TestResult>,
    /// Whether the last of `results` is still running.
    in_progress: bool,
    running: Option<RunningArm>,
}

impl Progress {
    pub(super) fn begin_test(&mut self, test: PerfTest) {
        self.results.push(TestResult { test, arms: Vec::new() });
        self.in_progress = true;
    }

    pub(super) fn begin_arm(&mut self, running: RunningArm) {
        self.running = Some(running);
    }

    pub(super) fn end_arm(&mut self, result: ArmResult) {
        self.running = None;
        if let Some(test) = self.results.last_mut() {
            test.arms.push(result);
        }
    }

    pub(super) fn end_test(&mut self) {
        self.in_progress = false;
        self.running = None;
    }

    /// The report so far; `None` when no test is running.
    fn live_markdown(&self) -> Option<String> {
        if !self.in_progress {
            return None;
        }
        let running = self
            .running
            .as_ref()
            .map(|r| report::Running { arm: r.arm, so_far: r.so_far() });
        let live = report::Live { updated: now_utc(), running };
        Some(report::markdown(&self.info, &self.results, Some(&live)))
    }
}

/// Where a report so far goes.
type Sink = Box<dyn FnMut(&str) + Send>;

/// `results_dir/report.md`, renamed into place so it is never half-written,
/// and with `push` the workflow's report.
fn report_sink(results_dir: PathBuf, push: bool) -> Sink {
    Box::new(move |markdown| {
        let path = results_dir.join("report.md");
        let partial = results_dir.join(".report.md.partial");
        if let Err(e) = fs::write(&partial, markdown).and_then(|()| fs::rename(&partial, &path)) {
            println!("WARNING: could not write {}: {e}", path.display());
            return;
        }
        if push {
            push_artifact("workflow", &path, ".semaphore/REPORT.md");
        }
    })
}

/// The run's [`Progress`], and the thread that writes the report from it
/// while a test runs.
///
/// The thread renders a report under the progress lock and takes the
/// publishing lock before it lets the progress go, then writes and pushes.
/// The run changes the progress through [`LiveReport::update`], which never
/// waits for a push, and writes each test's full report through
/// [`LiveReport::publish_full`], which takes both locks in the same order: a
/// report so far rendered before the full one lands before it, and none is
/// rendered after it.
pub(super) struct LiveReport {
    progress: Arc<Mutex<Progress>>,
    publishing: Arc<Mutex<()>>,
    /// Dropped to stop the thread.
    wake: Option<Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl LiveReport {
    /// Writes `results_dir/report.md`, and with `push` pushes it as the
    /// workflow's report, until a signal arrives after `signals_baseline`.
    pub(super) fn start(info: RunInfo, results_dir: PathBuf, push: bool, signals_baseline: u32) -> LiveReport {
        LiveReport::start_with(info, report_sink(results_dir, push), signals_baseline)
    }

    fn start_with(info: RunInfo, sink: Sink, signals_baseline: u32) -> LiveReport {
        let progress = Arc::new(Mutex::new(Progress {
            info,
            results: Vec::new(),
            in_progress: false,
            running: None,
        }));
        let publishing = Arc::new(Mutex::new(()));
        let (wake, woken) = mpsc::channel();
        let thread = std::thread::spawn({
            let progress = Arc::clone(&progress);
            let publishing = Arc::clone(&publishing);
            move || publish_until_stopped(&progress, &publishing, &woken, sink, signals_baseline)
        });
        LiveReport { progress, publishing, wake: Some(wake), thread: Some(thread) }
    }

    pub(super) fn update<R>(&self, f: impl FnOnce(&mut Progress) -> R) -> R {
        f(&mut lock(&self.progress))
    }

    /// `f`, which ends the test and writes its full report, once any report
    /// so far being written has landed.
    pub(super) fn publish_full<R>(&self, f: impl FnOnce(&mut Progress) -> R) -> R {
        let mut progress = lock(&self.progress);
        let _publishing = lock(&self.publishing);
        f(&mut progress)
    }

    /// Write the report now rather than at the next interval.
    pub(super) fn publish_now(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.send(());
        }
    }

    fn stop(&mut self) {
        self.wake = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Stop the thread; the results.
    pub(super) fn finish(mut self) -> Vec<TestResult> {
        self.stop();
        self.update(|p| std::mem::take(&mut p.results))
    }
}

impl Drop for LiveReport {
    fn drop(&mut self) {
        self.stop();
    }
}

fn publish_until_stopped(
    progress: &Mutex<Progress>,
    publishing: &Mutex<()>,
    woken: &Receiver<()>,
    mut sink: Sink,
    signals_baseline: u32,
) {
    loop {
        if let Err(RecvTimeoutError::Disconnected) = woken.recv_timeout(INTERVAL) {
            return;
        }
        // One report for every wake-up so far, and none once stopped, even
        // with a wake-up still queued.
        loop {
            match woken.try_recv() {
                Ok(()) => {},
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        // A stopping run has no time for it.
        if crate::signals::count() > signals_baseline {
            continue;
        }
        let progress = lock(progress);
        let Some(markdown) = progress.live_markdown() else {
            continue;
        };
        let _publishing = lock(publishing);
        drop(progress);
        sink(&markdown);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::super::{parse_tests, Group, Outcome};
    use super::*;

    /// A window of the shared format, its numbers as strings, as the
    /// harnesses write them.
    #[expect(clippy::too_many_arguments)]
    fn window(
        end_ms: u64,
        start: &str,
        ended: &str,
        messages: u64,
        latency_total: &str,
        latency_max: &str,
        cpu: &str,
        rss: &str,
    ) -> String {
        format!(
            "{{\"rss\": {{\"average\": \"{rss}\", \"max\": \"{rss}\", \"total\": \"{rss}\", \"count\": \"1\"}}, \
             \"cpu\": {{\"average\": \"{cpu}\", \"max\": \"{cpu}\", \"total\": \"{cpu}\", \"count\": \"1\"}}, \
             \"latency\": {{\"average\": \"0\", \"max\": \"{latency_max}\", \"total\": \"{latency_total}\", \
             \"count\": \"{messages}\", \"p50\": \"1\", \"p90\": \"1\", \"p99\": \"1\", \"p999\": \"1\"}}, \
             \"bytes\": {{\"average\": \"1024\", \"max\": \"1024\", \"total\": \"{}\", \"count\": \"{messages}\"}}, \
             \"messages\": {{\"average\": \"1\", \"max\": \"1\", \"total\": \"{messages}\", \"count\": \
             \"{messages}\"}}, \"window_start_ms\": \"{}\", \"window_end_ms\": \"{end_ms}\", \
             \"measurement_start_ms\": \"{start}\", \"measurement_end_ms\": \"{ended}\"}}\n",
            messages * 1024,
            end_ms - 1000
        )
    }

    fn approx(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap_or(f64::NAN);
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    #[test]
    fn reads_windows_of_the_measurement() {
        let mut text = String::new();
        // Warmup: no measurement yet, so neither its CPU nor its RSS counts.
        text += &window(1_000, "-inf", "-inf", 0, "0", "-inf", "0.0", "1");
        // Measured from 1,500 ms: 2,000 records in 2 s (Java writes large
        // totals in exponent form).
        text += &window(2_000, "1500", "-inf", 1_000, "2.0E3", "5", "40", "104857600");
        text += &window(3_000, "1500.0", "-inf", 1_000, "4000", "9.5", "60", "314572800");
        // Measurement ended at 3,500 ms: its last acks count, not its CPU.
        text += &window(4_000, "1500", "3500", 1_000, "3000", "7", "500", "1");
        // Cooldown.
        text += &window(5_000, "1500", "3500", 0, "0", "-inf", "500", "1");
        // A line still being written.
        text += "{\"rss\": {\"average\": \"1\"";
        let s = from_windows(&text).unwrap();
        approx(Some(s.seconds), 2.0);
        approx(Some(s.messages), 3_000.0);
        approx(s.msg_s(), 1_500.0);
        approx(s.mib_s(), 1_500.0 * 1024.0 / MIB);
        approx(s.latency_avg, 3.0);
        approx(s.latency_max, 9.5);
        approx(s.cpu_pct, 50.0);
        approx(s.rss_mib, 200.0);
    }

    #[test]
    fn reads_windows_still_running() {
        // The Rust harness writes an empty window's max as 0, the others as
        // -inf; neither counts without records.
        let text = window(1_000, "-inf", "-inf", 0, "0", "0", "0", "1")
            + &window(2_000, "1000", "-inf", 0, "0", "0", "10", "1048576")
            + &window(3_000, "1000", "-inf", 10, "10", "1", "20", "1048576");
        let s = from_windows(&text).unwrap();
        approx(Some(s.seconds), 2.0);
        approx(s.msg_s(), 5.0);
        approx(s.latency_avg, 1.0);
        approx(s.latency_max, 1.0);
        approx(s.cpu_pct, 15.0);
        approx(s.rss_mib, 1.0);
        // Before the measurement, or before a whole line, nothing.
        assert_eq!(from_windows(&window(1_000, "-inf", "-inf", 0, "0", "-inf", "1", "1")), None);
        assert_eq!(from_windows(""), None);
        assert_eq!(from_windows("{\"measurement_start_ms\": \"1\""), None);
    }

    #[test]
    fn reads_interval_lines() {
        // The Rust (and C) consumer's lines, then the Java consumer's, which
        // pad differently; the other lines of the log are skipped.
        let log = "\
==> t / Rust on topic x (2026-10-21T10:00:00Z)
warmup complete (100 msgs); measuring 600s now
[interval 0] t=   5.0s msgs=    500 thr=      100 msg/s  avg=  2.00 p50=2 p99=4 p999=5 max=6 ms  cpu= 10.0% rss=  50.0MB
[interval 1] t=  10.0s msgs=   1500 thr=      300 msg/s  avg=  4.00 p50=4 p99=8 p999=9 max=12 ms  cpu= 30.0% rss=  70.0MB
[interval 2] t=  20.0s msgs=      0 thr=        0 msg/s avg=  0.00 p50=0 p99=0 p999=0 max=0 ms cpu=  5.0% rss=60.0MB
[interval 3] t=  25.0s msgs=   2000 thr=      400 msg/s avg=  1.50 p50=1 p99=2 p999=3 max=3 ms cpu= 15.0% rss=60.0MB
[interval 4] t=  30.0s msgs=   9999";
        let s = from_interval_lines(log, 1024.0).unwrap();
        approx(Some(s.seconds), 25.0);
        approx(Some(s.messages), 4_000.0);
        approx(s.msg_s(), 160.0);
        approx(s.mib_s(), 160.0 * 1024.0 / MIB);
        // (500 × 2 + 1,500 × 4 + 2,000 × 1.5) / 4,000.
        approx(s.latency_avg, 2.5);
        approx(s.latency_max, 12.0);
        approx(s.cpu_pct, 15.0);
        approx(s.rss_mib, 60.0);
        assert_eq!(from_interval_lines("warmup complete (100 msgs)\n", 1.0), None);
        assert_eq!(field("max=12 ms", "max="), Some(12.0));
        assert_eq!(field("avg=-1.25", "avg="), Some(-1.25));
        assert_eq!(field("t=s", "t="), None);
    }

    fn info(tests: &[PerfTest]) -> RunInfo {
        RunInfo {
            group: Group::Rust,
            arms: vec![Arm::Rust, Arm::Librdkafka],
            tests: tests.to_vec(),
            duration_min: 10,
            warmup_min: 1,
            started: "2026-10-21T10:00:00Z".to_string(),
            facts: Vec::new(),
            ingress_mb_s: None,
        }
    }

    #[test]
    fn reports_only_while_a_test_runs() {
        let test = parse_tests("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        let dir = std::env::temp_dir().join(format!("perf-cloud-live-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("metrics.jsonl"),
            window(2_000, "1000", "-inf", 100, "200", "4", "25", "1048576"),
        )
        .unwrap();

        let mut progress = Progress {
            info: info(std::slice::from_ref(&test)),
            results: Vec::new(),
            in_progress: false,
            running: None,
        };
        assert_eq!(progress.live_markdown(), None);
        progress.begin_test(test.clone());
        progress.begin_arm(RunningArm::new(&test, Arm::Rust, &dir));
        let md = progress.live_markdown().unwrap();
        assert!(md.contains("| Throughput (msg/s) | 100 | – |"), "{md}");
        assert!(md.contains("- librdkafka has not started yet."), "{md}");
        progress.end_arm(ArmResult {
            arm: Arm::Rust,
            outcome: Outcome::Failed("interrupted".to_string()),
            metrics: None,
            load_msg_s: None,
            elapsed: Duration::from_secs(1),
        });
        progress.end_test();
        assert_eq!(progress.live_markdown(), None);
        assert_eq!(progress.results.len(), 1);
        assert_eq!(progress.results[0].arms.len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Whether `done` holds within 10 s.
    fn eventually(mut done: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        true
    }

    fn passed(arm: Arm) -> ArmResult {
        ArmResult {
            arm,
            outcome: Outcome::Passed,
            metrics: None,
            load_msg_s: None,
            elapsed: Duration::from_secs(1),
        }
    }

    #[test]
    fn writes_the_report_when_woken_and_stops() {
        let test = parse_tests("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        let dir = std::env::temp_dir().join(format!("perf-cloud-publish-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let report = dir.join("report.md");

        let live = LiveReport::start(info(std::slice::from_ref(&test)), dir.clone(), false, crate::signals::count());
        live.update(|p| {
            p.begin_test(test.clone());
            p.begin_arm(RunningArm::new(&test, Arm::Rust, &dir));
        });
        live.publish_now();
        // Renamed into place: once there, it is whole.
        assert!(eventually(|| report.exists()));
        let md = fs::read_to_string(&report).unwrap();
        assert!(md.contains("In progress"), "{md}");
        assert!(md.ends_with("\n- Measurement: 10 min per client\n"), "{md}");
        assert!(md.contains("- Rust is running and has not started measuring yet."), "{md}");
        assert!(!dir.join(".report.md.partial").exists());

        // Once the test ends, nothing replaces its report.
        live.publish_full(|p| {
            p.end_test();
            fs::write(&report, "full").unwrap();
        });
        live.publish_now();
        let results = live.finish();
        assert_eq!(fs::read_to_string(&report).unwrap(), "full");
        assert_eq!(results.len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_full_report_lands_after_the_report_so_far() {
        let test = parse_tests("t rust PARTITIONS=1 VALUE_SIZE=1024 LIMIT_RPS=0")
            .unwrap()
            .remove(0);
        let written = Arc::new(Mutex::new(Vec::<&str>::new()));
        let (entered, entered_rx) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        // A push that takes until the test releases it.
        let sink: Sink = Box::new({
            let written = Arc::clone(&written);
            move |_: &str| {
                let _ = entered.send(());
                let _ = released.recv_timeout(Duration::from_secs(10));
                written.lock().unwrap().push("so far");
            }
        });
        let live = LiveReport::start_with(info(std::slice::from_ref(&test)), sink, crate::signals::count());
        live.update(|p| {
            p.begin_test(test.clone());
            p.begin_arm(RunningArm::new(&test, Arm::Rust, Path::new("/nonexistent")));
        });
        live.publish_now();
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        std::thread::scope(|s| {
            // A client ending does not wait for the push...
            let boundary = s.spawn(|| live.update(|p| p.end_arm(passed(Arm::Rust))));
            let boundary_waited = !eventually(|| boundary.is_finished());
            // ... the full report does.
            let full = s.spawn(|| {
                live.publish_full(|p| {
                    p.end_test();
                    written.lock().unwrap().push("full");
                });
            });
            std::thread::sleep(Duration::from_millis(200));
            let full_waited = !full.is_finished();
            release.send(()).unwrap();
            assert!(!boundary_waited, "a client's end waited for the push");
            assert!(full_waited, "the full report did not wait for the push");
        });
        live.publish_now();
        drop(live);
        assert_eq!(*written.lock().unwrap(), ["so far", "full"]);
    }

    #[test]
    fn reads_each_harness_where_it_writes() {
        let dir = Path::new("/arm");
        let source = |line: &str, arm: Arm| {
            let test = parse_tests(line).unwrap().remove(0);
            match RunningArm::new(&test, arm, dir).source {
                Source::Windows(path) => (path, None),
                Source::IntervalLines { log, record_bytes } => (log, Some(record_bytes)),
            }
        };
        let consumer = "c rust,python WORKLOAD=consumer PARTITIONS=1 VALUE_SIZE=512 LIMIT_RPS=1";
        for arm in [Arm::Rust, Arm::Librdkafka, Arm::Java] {
            assert_eq!(source(consumer, arm), (dir.join("run.log"), Some(512.0)), "{arm:?}");
        }
        assert_eq!(source(consumer, Arm::PythonAsyncRust), (dir.join("metrics.jsonl"), None));
        for line in [
            "p rust PARTITIONS=1 VALUE_SIZE=512 LIMIT_RPS=1",
            "t rust WORKLOAD=transactional TXN_MODE=eos PARTITIONS=1 VALUE_SIZE=512 LIMIT_RPS=1",
        ] {
            for arm in [Arm::Rust, Arm::Librdkafka, Arm::Java, Arm::PythonSyncLibrdkafka] {
                assert_eq!(source(line, arm), (dir.join("metrics.jsonl"), None), "{line} {arm:?}");
            }
        }
    }
}
