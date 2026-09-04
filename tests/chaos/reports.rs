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

//! On-disk reports and Rust client-log capture.
//!
//! The observability analog of librdkafka's report files. Each run writes a
//! directory under `target/chaos-runs/<run-id>/` containing:
//!
//! - `verdict.txt` — the conservation / dual-key verdict (persisted).
//! - `leader-changes.txt` — before→after leader/replica diffs per action
//!   (librdkafka's `leader_changes.txt`).
//! - `client.log` — captured Rust client `log` output (librdkafka's
//!   per-consumer stderr).
//! - `summary.txt` — counts of known diagnostic signatures grepped from
//!   `client.log` (librdkafka's `summary.txt` / `metadata-trigger.txt`).
//!
//! The Rust client logs through the `log` facade (~194 sites). We install one
//! process-global [`ChaosLogger`] that tees every record to the *current
//! run's* `client.log` and tallies signature matches — the in-process analog
//! of librdkafka's "grep the debug log" reporting.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Diagnostic signatures counted for `summary.txt`. Each is a (label,
/// substring) pair matched case-insensitively against captured log lines —
/// the Rust-client analog of librdkafka's gap-signature grep.
const SIGNATURES: &[(&str, &str)] = &[
    ("transport disconnects", "disconnect"),
    ("connection resets", "connection reset"),
    ("metadata refreshes", "updating metadata"),
    ("leader-change errors", "leader"),
    ("not-coordinator errors", "coordinator"),
    ("timeouts", "timed out"),
    ("retries", "retrying"),
];

/// Shared sink the global logger writes into. Swapped per run so `client.log`
/// always points at the active run's directory.
struct LogSink {
    file: Option<File>,
    /// signature label -> match count.
    counts: Vec<u64>,
    /// Also echo to stderr (so `--nocapture` still shows client logs live).
    echo: bool,
}

static SINK: OnceLock<Mutex<LogSink>> = OnceLock::new();
static LOGGER_INSTALLED: Once = Once::new();
static CAPTURING: AtomicBool = AtomicBool::new(false);

fn sink() -> &'static Mutex<LogSink> {
    SINK.get_or_init(|| Mutex::new(LogSink { file: None, counts: vec![0; SIGNATURES.len()], echo: false }))
}

/// The process-global logger: tees each record to the current run's file and
/// tallies signature matches. Installed once; inert until a run points it at a
/// file via [`RunReports::new`].
struct ChaosLogger;

impl log::Log for ChaosLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        CAPTURING.load(Ordering::Relaxed)
    }

    fn log(&self, record: &log::Record) {
        if !CAPTURING.load(Ordering::Relaxed) {
            return;
        }
        let line = format!("[{:<5}] {}: {}\n", record.level(), record.target(), record.args());
        let mut s = sink().lock().expect("log sink poisoned");
        let lower = line.to_lowercase();
        for (i, (_, needle)) in SIGNATURES.iter().enumerate() {
            if lower.contains(needle) {
                s.counts[i] += 1;
            }
        }
        if s.echo {
            eprint!("{line}");
        }
        if let Some(f) = s.file.as_mut() {
            let _ = f.write_all(line.as_bytes());
        }
    }

    fn flush(&self) {
        if let Ok(mut s) = sink().lock()
            && let Some(f) = s.file.as_mut()
        {
            let _ = f.flush();
        }
    }
}

/// Report bundle for one chaos run. Owns the run directory and the client-log
/// capture; call [`Self::write_verdict`] / [`Self::record_leader_change`] as
/// the run proceeds, then [`Self::finish`] to flush `summary.txt`.
pub struct RunReports {
    dir: PathBuf,
    leader_log: Mutex<File>,
}

impl RunReports {
    /// Create `target/chaos-runs/<run-id>/`, install (once) and arm the
    /// client-log capture pointed at `client.log`. `echo` also mirrors client
    /// logs to stderr.
    pub fn new(run_id: &str, echo_client_logs: bool) -> Self {
        let dir = PathBuf::from("target/chaos-runs").join(run_id);
        fs::create_dir_all(&dir).expect("create chaos run dir");

        // Install the global logger exactly once; arm it for this run.
        LOGGER_INSTALLED.call_once(|| {
            // Ignore the error: another logger may already be set in this test
            // binary; capture is best-effort and must not fail the run.
            let _ = log::set_boxed_logger(Box::new(ChaosLogger));
            log::set_max_level(log::LevelFilter::Debug);
        });

        let client_log = File::create(dir.join("client.log")).expect("create client.log");
        {
            let mut s = sink().lock().expect("log sink poisoned");
            s.file = Some(client_log);
            s.counts = vec![0; SIGNATURES.len()];
            s.echo = echo_client_logs;
        }
        CAPTURING.store(true, Ordering::Relaxed);

        let leader_log = File::create(dir.join("leader-changes.txt")).expect("create leader-changes.txt");
        eprintln!("chaos: reports -> {}", dir.display());
        Self { dir, leader_log: Mutex::new(leader_log) }
    }

    /// Append a timestamped leader/replica-change line (the `leader_changes.txt`
    /// analog). Called by the actions with their before→after diffs.
    pub fn record_leader_change(&self, line: &str) {
        let mut f = self.leader_log.lock().expect("leader log poisoned");
        let _ = writeln!(f, "{} {line}", now_millis());
    }

    /// Persist the final verdict text to `verdict.txt`.
    pub fn write_verdict(&self, verdict: &str) {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.dir.join("verdict.txt"))
            .expect("create verdict.txt");
        let _ = f.write_all(verdict.as_bytes());
    }

    /// Flush the signature summary to `summary.txt` and disarm capture. The
    /// run directory path is returned for the caller to log.
    pub fn finish(self) -> PathBuf {
        log::logger().flush();
        CAPTURING.store(false, Ordering::Relaxed);

        let counts = { sink().lock().expect("log sink poisoned").counts.clone() };
        let mut summary = String::from("=== Client-log signature summary ===\n");
        for ((label, _), count) in SIGNATURES.iter().zip(counts) {
            summary.push_str(&format!("  {label:<24}: {count}\n"));
        }
        let mut f = File::create(self.dir.join("summary.txt")).expect("create summary.txt");
        let _ = f.write_all(summary.as_bytes());
        eprint!("{summary}");

        // Detach the file so the next run's logger does not write here.
        {
            let mut s = sink().lock().expect("log sink poisoned");
            s.file = None;
        }
        self.dir
    }
}

/// A run id: unix-millis plus a short random suffix, unique per run.
pub fn new_run_id() -> String {
    format!("{}-{}", now_millis(), std::process::id())
}

fn now_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}

/// A cheap handle the actions/harness can hold to append leader-change lines
/// without threading `&RunReports` everywhere. `None` when reports are off.
pub type ReportsHandle = Option<Arc<RunReports>>;
