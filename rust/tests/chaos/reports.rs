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

/// A single rotating log file: writes append to `path`; when it exceeds
/// `budget` bytes it rolls to `path.1` (dropping any previous `.1`) and starts
/// fresh — the `--log-budget-bytes` analog, one backup kept.
struct RotatingFile {
    path: PathBuf,
    file: File,
    written: u64,
    budget: u64,
}

impl RotatingFile {
    fn create(path: PathBuf, budget: u64) -> Self {
        let file = File::create(&path).expect("create rotating log file");
        Self { path, file, written: 0, budget }
    }

    fn write_line(&mut self, line: &str) {
        let bytes = line.as_bytes();
        if self.written + bytes.len() as u64 > self.budget {
            // Rotate: current -> .1 (overwriting an old .1), then fresh file.
            let backup = self.path.with_extension("log.1");
            let _ = fs::rename(&self.path, &backup);
            self.file = File::create(&self.path).expect("recreate rotated log file");
            self.written = 0;
        }
        if self.file.write_all(bytes).is_ok() {
            self.written += bytes.len() as u64;
        }
    }
}

/// Shared sink the global logger writes into. Routes each line to a
/// per-`clientId` rotating file (`client-<id>.log`), falling back to
/// `client.log` for lines with no recognizable client id. Swapped per run so
/// files always land in the active run's directory.
struct LogSink {
    /// Run directory; per-client files are created lazily under it.
    dir: Option<PathBuf>,
    /// Per-`clientId` rotating files, created on first sighting.
    per_client: std::collections::HashMap<String, RotatingFile>,
    /// Fallback file for lines carrying no `clientId=`.
    fallback: Option<RotatingFile>,
    /// Per-file rotation budget in bytes.
    budget: u64,
    /// signature label -> match count.
    counts: Vec<u64>,
    /// Also echo to stderr (so `--nocapture` still shows client logs live).
    echo: bool,
}

impl LogSink {
    /// Extract the `clientId=<id>` token from a log line, if present.
    fn client_id(line: &str) -> Option<&str> {
        let start = line.find("clientId=")? + "clientId=".len();
        let rest = &line[start..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '-' || c == '_' || c == '.'))
            .unwrap_or(rest.len());
        (end > 0).then(|| &rest[..end])
    }

    /// Route a line to its per-client file (or the fallback), creating the file
    /// on first use.
    fn route(&mut self, line: &str) {
        let Some(dir) = self.dir.clone() else { return };
        match Self::client_id(line) {
            Some(id) => {
                let id = id.to_string();
                let budget = self.budget;
                self.per_client
                    .entry(id.clone())
                    .or_insert_with(|| RotatingFile::create(dir.join(format!("client-{id}.log")), budget))
                    .write_line(line);
            },
            None => {
                let budget = self.budget;
                self.fallback
                    .get_or_insert_with(|| RotatingFile::create(dir.join("client.log"), budget))
                    .write_line(line);
            },
        }
    }
}

static SINK: OnceLock<Mutex<LogSink>> = OnceLock::new();
static LOGGER_INSTALLED: Once = Once::new();
static CAPTURING: AtomicBool = AtomicBool::new(false);

fn sink() -> &'static Mutex<LogSink> {
    SINK.get_or_init(|| {
        Mutex::new(LogSink {
            dir: None,
            per_client: std::collections::HashMap::new(),
            fallback: None,
            budget: DEFAULT_LOG_BUDGET_BYTES,
            counts: vec![0; SIGNATURES.len()],
            echo: false,
        })
    })
}

/// Default per-file rotation budget: 64 MiB.
const DEFAULT_LOG_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

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
        s.route(&line);
    }

    fn flush(&self) {
        if let Ok(mut s) = sink().lock() {
            for f in s.per_client.values_mut() {
                let _ = f.file.flush();
            }
            if let Some(f) = s.fallback.as_mut() {
                let _ = f.file.flush();
            }
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
    /// client-log capture. Each workload's log lands in its own
    /// `client-<clientId>.log` (rotating at `budget_bytes`); lines with no
    /// client id go to `client.log`. `echo` also mirrors logs to stderr.
    pub fn new(run_id: &str, echo_client_logs: bool, budget_bytes: u64) -> Self {
        let dir = PathBuf::from("target/chaos-runs").join(run_id);
        fs::create_dir_all(&dir).expect("create chaos run dir");

        // Install the global logger exactly once; arm it for this run.
        LOGGER_INSTALLED.call_once(|| {
            // Ignore the error: another logger may already be set in this test
            // binary; capture is best-effort and must not fail the run.
            let _ = log::set_boxed_logger(Box::new(ChaosLogger));
            log::set_max_level(log::LevelFilter::Debug);
        });

        {
            let mut s = sink().lock().expect("log sink poisoned");
            s.dir = Some(dir.clone());
            s.per_client.clear();
            s.fallback = None;
            s.budget = budget_bytes;
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

        // Detach files so the next run's logger does not write here.
        {
            let mut s = sink().lock().expect("log sink poisoned");
            s.dir = None;
            s.per_client.clear();
            s.fallback = None;
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
