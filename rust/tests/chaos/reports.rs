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
use std::io::{BufWriter, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// Diagnostic signatures counted for `summary.txt`. Each is a label plus the
/// substrings that identify it, matched case-insensitively against captured
/// log lines (a line counts once per label however many of its substrings it
/// contains) — the Rust-client analog of librdkafka's gap-signature grep.
///
/// The error signatures name the error, not the subject: a bare "leader" or
/// "coordinator" also matched routine lines ("Discovered group coordinator",
/// "Leader for partition ... is unknown", the leader-epoch validation chatter)
/// and inflated the counts on a quiet run. The client renders a broker error
/// either by its message (`Errors::message`, e.g. "not the current leader" /
/// "This is not the correct coordinator.") or by its variant name.
const SIGNATURES: &[(&str, &[&str])] = &[
    ("transport disconnects", &["disconnect"]),
    ("connection resets", &["connection reset"]),
    ("metadata refreshes", &["updating metadata"]),
    (
        "leader-change errors",
        &[
            "not the current leader",
            "notleaderorfollower",
            "not_leader_or_follower",
        ],
    ),
    (
        "not-coordinator errors",
        &["not the correct coordinator", "notcoordinator", "not_coordinator"],
    ),
    ("timeouts", &["timed out"]),
    ("retries", &["retrying"]),
];

/// Indices into [`SIGNATURES`] whose substrings occur in `line` (already
/// lower-cased). Computed outside the sink lock: it is the per-line work.
fn matched_signatures(lower: &str) -> impl Iterator<Item = usize> + '_ {
    SIGNATURES
        .iter()
        .enumerate()
        .filter(move |(_, (_, needles))| needles.iter().any(|needle| lower.contains(needle)))
        .map(|(i, _)| i)
}

/// A single rotating log file: writes append to `path`; when it exceeds
/// `budget` bytes it rolls to `path.1` (dropping any previous `.1`) and starts
/// fresh — the `--log-budget-bytes` analog, one backup kept.
///
/// Best-effort: a file that cannot be opened is reported once on stderr and
/// its lines are dropped. Every write happens under the process-wide sink
/// lock, and the `expect` this used to have poisoned that lock on the first
/// failure, after which every later log line in the process panicked.
struct RotatingFile {
    path: PathBuf,
    /// Buffered: the client logs thousands of lines a second under a fault,
    /// and every line is written while the process-wide sink lock is held, so
    /// one `write(2)` per line stalled every logging task behind the disk.
    /// `None` once the file could not be opened.
    file: Option<BufWriter<File>>,
    written: u64,
    budget: u64,
}

impl RotatingFile {
    /// Open `path` for appending, so a client whose file was closed early
    /// ([`close_client_log`], the open-file cap) continues it instead of
    /// truncating it.
    fn create(path: PathBuf, budget: u64) -> Self {
        let (file, written) = match Self::open(&path) {
            Some((file, len)) => (Some(file), len),
            None => (None, 0),
        };
        Self { path, file, written, budget }
    }

    fn open(path: &std::path::Path) -> Option<(BufWriter<File>, u64)> {
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(file) => {
                let len = file.metadata().map(|m| m.len()).unwrap_or(0);
                Some((BufWriter::new(file), len))
            },
            Err(err) => {
                eprintln!(
                    "chaos: WARN cannot open client log {}: {err}; its lines are dropped",
                    path.display()
                );
                None
            },
        }
    }

    fn write_line(&mut self, line: &str) {
        let bytes = line.as_bytes();
        if self.file.is_some() && self.written + bytes.len() as u64 > self.budget {
            // Rotate: shift .1 -> .2 ... (dropping the oldest), current -> .1,
            // then a fresh file. More than one backup, so the lead-up to a
            // fault survives the log storm that follows it.
            self.flush();
            self.file = None;
            for n in (1..LOG_BACKUPS).rev() {
                let _ = fs::rename(self.backup(n), self.backup(n + 1));
            }
            let _ = fs::rename(&self.path, self.backup(1));
            self.file = Self::open(&self.path).map(|(file, _)| file);
            self.written = 0;
        }
        if let Some(file) = self.file.as_mut()
            && file.write_all(bytes).is_ok()
        {
            self.written += bytes.len() as u64;
        }
    }

    fn flush(&mut self) {
        if let Some(file) = self.file.as_mut() {
            let _ = file.flush();
        }
    }

    /// The path of rotated backup `n` (`client-x.log.<n>`, 1 = newest).
    fn backup(&self, n: usize) -> PathBuf {
        self.path.with_extension(format!("log.{n}"))
    }
}

/// Rotated backups kept per client log, besides the live file.
const LOG_BACKUPS: usize = 4;

/// Most per-client log files kept open at once. A finished workload's file is
/// closed ([`close_client_log`]); this caps what is left (client ids that
/// never belonged to a workload thread) so the descriptors cannot grow without
/// bound. An evicted file is reopened for appending on its next line.
const MAX_OPEN_CLIENT_LOGS: usize = 64;

/// `HH:MM:SS.mmm` (UTC) of a Unix time in milliseconds, the prefix of every
/// captured client-log line so it can be lined up with the broker logs and
/// `leader-changes.txt`. The date is in the run directory's name.
fn utc_time_of_day(millis: u128) -> String {
    let secs = (millis / 1000) % 86_400;
    format!("{:02}:{:02}:{:02}.{:03}", secs / 3600, secs / 60 % 60, secs % 60, millis % 1000)
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
        if self.dir.is_none() {
            return;
        }
        let budget = self.budget;
        match Self::client_id(line) {
            Some(id) => {
                // Look up before allocating: the id is only owned when the
                // file is created, once per client.
                if let Some(file) = self.per_client.get_mut(id) {
                    file.write_line(line);
                } else {
                    if self.per_client.len() >= MAX_OPEN_CLIENT_LOGS
                        && let Some(evict) = self.per_client.keys().next().cloned()
                    {
                        self.close_client(&evict);
                    }
                    let Some(dir) = self.dir.as_deref() else { return };
                    self.per_client
                        .entry(id.to_string())
                        .or_insert_with(|| RotatingFile::create(dir.join(format!("client-{id}.log")), budget))
                        .write_line(line);
                }
            },
            None => {
                let Some(dir) = self.dir.as_deref() else { return };
                self.fallback
                    .get_or_insert_with(|| RotatingFile::create(dir.join("client.log"), budget))
                    .write_line(line);
            },
        }
    }

    /// Flush and close client `id`'s file, if open.
    fn close_client(&mut self, id: &str) {
        if let Some(mut file) = self.per_client.remove(id) {
            file.flush();
        }
    }

    /// Flush every open file's buffer to disk.
    fn flush_all(&mut self) {
        for file in self.per_client.values_mut() {
            file.flush();
        }
        if let Some(file) = self.fallback.as_mut() {
            file.flush();
        }
    }
}

static SINK: OnceLock<Mutex<LogSink>> = OnceLock::new();
static LOGGER_INSTALLED: Once = Once::new();
static CAPTURING: AtomicBool = AtomicBool::new(false);

/// The sink, locked. A panic while it was held (there should be none: nothing
/// under the lock panics any more) must not make every later log line panic,
/// so a poisoned lock is recovered: the sink is plain buffers and counters.
fn sink() -> MutexGuard<'static, LogSink> {
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
    .lock()
    .unwrap_or_else(PoisonError::into_inner)
}

/// Close client `client_id`'s log file. Called when the workload of that id
/// finishes: churn mints a new client id per added consumer, and each kept its
/// file open until the run ended. A late line reopens it for appending.
pub fn close_client_log(client_id: &str) {
    sink().close_client(client_id);
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
        // Every workload task logs through this one sink, so the lock is a
        // process-wide serialization point: do the formatting and the
        // signature scan first, and hold the lock only to tally and append.
        let line = format!(
            "{} [{:<5}] {}: {}\n",
            utc_time_of_day(now_millis()),
            record.level(),
            record.target(),
            record.args()
        );
        let matched: Vec<usize> = matched_signatures(&line.to_lowercase()).collect();
        let echo = {
            let mut s = sink();
            for i in matched {
                s.counts[i] += 1;
            }
            s.route(&line);
            s.echo
        };
        if echo {
            eprint!("{line}");
        }
    }

    fn flush(&self) {
        sink().flush_all();
    }
}

/// Report bundle for one chaos run. Owns the run directory and the client-log
/// capture; call [`Self::write_verdict`] / [`Self::record_leader_change`] as
/// the run proceeds, then [`Self::finish`] to flush `summary.txt`.
///
/// Best-effort throughout: a report file that cannot be written is reported on
/// stderr, never a panic, so the reports cannot take down the run (or its
/// teardown) they describe.
pub struct RunReports {
    dir: PathBuf,
    /// `None` when `leader-changes.txt` could not be created.
    leader_log: Mutex<Option<File>>,
}

impl RunReports {
    /// Create `target/chaos-runs/<run-id>/`, install (once) and arm the
    /// client-log capture. Each workload's log lands in its own
    /// `client-<clientId>.log` (rotating at `budget_bytes`); lines with no
    /// client id go to `client.log`. `echo` also mirrors logs to stderr.
    pub fn new(run_id: &str, echo_client_logs: bool, budget_bytes: u64) -> Self {
        let dir = PathBuf::from("target/chaos-runs").join(run_id);
        if let Err(err) = fs::create_dir_all(&dir) {
            eprintln!("chaos: WARN cannot create the report directory {}: {err}", dir.display());
        }

        // Install the global logger exactly once; arm it for this run.
        LOGGER_INSTALLED.call_once(|| {
            // Ignore the error: another logger may already be set in this test
            // binary; capture is best-effort and must not fail the run.
            let _ = log::set_boxed_logger(Box::new(ChaosLogger));
            log::set_max_level(log::LevelFilter::Debug);
        });

        {
            let mut s = sink();
            s.dir = Some(dir.clone());
            s.per_client.clear();
            s.fallback = None;
            s.budget = budget_bytes;
            s.counts = vec![0; SIGNATURES.len()];
            s.echo = echo_client_logs;
        }
        CAPTURING.store(true, Ordering::Relaxed);

        let leader_log = File::create(dir.join("leader-changes.txt"))
            .map_err(|err| eprintln!("chaos: WARN cannot create leader-changes.txt: {err}"))
            .ok();
        eprintln!("chaos: reports -> {}", dir.display());
        Self { dir, leader_log: Mutex::new(leader_log) }
    }

    /// Append a timestamped leader/replica-change line (the `leader_changes.txt`
    /// analog). Called by the actions with their before→after diffs.
    pub fn record_leader_change(&self, line: &str) {
        if let Some(f) = self.leader_log.lock().unwrap_or_else(PoisonError::into_inner).as_mut() {
            let _ = writeln!(f, "{} {line}", now_millis());
        }
    }

    /// Persist the final verdict text to `verdict.txt`.
    pub fn write_verdict(&self, verdict: &str) {
        let written = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.dir.join("verdict.txt"))
            .and_then(|mut f| f.write_all(verdict.as_bytes()));
        if let Err(err) = written {
            eprintln!("chaos: WARN cannot write verdict.txt: {err}");
        }
    }

    /// Flush the signature summary to `summary.txt` and disarm capture. The
    /// run directory path is returned for the caller to log. By reference, so
    /// the runner need not be the sole owner of the handle (the scenario may
    /// still hold a clone after an abort).
    pub fn finish(&self) -> PathBuf {
        CAPTURING.store(false, Ordering::Relaxed);

        // Flush the sink directly rather than through `log::logger()`: if another
        // logger won the global slot, ours is not the one `log::logger()` returns.
        let counts = {
            let mut s = sink();
            s.flush_all();
            s.counts.clone()
        };
        let mut summary = String::from("=== Client-log signature summary ===\n");
        for ((label, _), count) in SIGNATURES.iter().zip(counts) {
            summary.push_str(&format!("  {label:<24}: {count}\n"));
        }
        if let Err(err) = File::create(self.dir.join("summary.txt")).and_then(|mut f| f.write_all(summary.as_bytes())) {
            eprintln!("chaos: WARN cannot write summary.txt: {err}");
        }
        eprint!("{summary}");

        // Detach files so the next run's logger does not write here.
        {
            let mut s = sink();
            s.dir = None;
            s.per_client.clear();
            s.fallback = None;
        }
        self.dir.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_of_day_is_utc_with_millis() {
        // 2026-09-25T13:04:05.007Z
        assert_eq!(utc_time_of_day(1_790_341_445_007), "13:04:05.007");
        assert_eq!(utc_time_of_day(0), "00:00:00.000");
    }

    #[test]
    fn rotation_keeps_the_newest_backups_and_drops_the_oldest() {
        let dir = std::env::temp_dir().join(format!("chaos-rotation-{}-{}", std::process::id(), now_millis()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("client-x.log");
        // Each 4-byte line fills the 4-byte budget, so every line after the
        // first rotates.
        let mut file = RotatingFile::create(path.clone(), 4);
        for i in 0..=LOG_BACKUPS + 2 {
            file.write_line(&format!("{i:03}\n"));
        }
        file.flush();
        let read = |p: PathBuf| fs::read_to_string(p).unwrap();
        let last = LOG_BACKUPS + 2;
        assert_eq!(read(path.clone()), format!("{last:03}\n"));
        for n in 1..=LOG_BACKUPS {
            assert_eq!(read(file.backup(n)), format!("{:03}\n", last - n), "backup {n}");
        }
        assert!(!file.backup(LOG_BACKUPS + 1).exists(), "only {LOG_BACKUPS} backups are kept");
        let _ = fs::remove_dir_all(dir);
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("chaos-{name}-{}-{}", std::process::id(), now_millis()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sink_in(dir: &std::path::Path) -> LogSink {
        LogSink {
            dir: Some(dir.to_path_buf()),
            per_client: std::collections::HashMap::new(),
            fallback: None,
            budget: DEFAULT_LOG_BUDGET_BYTES,
            counts: vec![0; SIGNATURES.len()],
            echo: false,
        }
    }

    /// A log file that cannot be created drops its lines instead of panicking
    /// (which poisoned the process-wide sink lock for every later line).
    #[test]
    fn an_unopenable_log_file_drops_lines_instead_of_panicking() {
        let missing = std::env::temp_dir().join(format!("chaos-missing-{}-{}", std::process::id(), now_millis()));
        let mut file = RotatingFile::create(missing.join("client-x.log"), 4);
        file.write_line("lost\n");
        file.write_line("also lost, past the budget\n");
        file.flush();
        assert!(!missing.exists());
    }

    /// A closed client file is reopened for appending, not truncated, and the
    /// open files are capped.
    #[test]
    fn closed_and_evicted_client_files_are_continued() {
        let dir = temp_dir("client-files");
        let mut sink = sink_in(&dir);
        sink.route("[Producer clientId=producer-rust-1] first\n");
        sink.close_client("producer-rust-1");
        assert!(sink.per_client.is_empty(), "closed with its workload");
        sink.route("[Producer clientId=producer-rust-1] late\n");
        for i in 0..MAX_OPEN_CLIENT_LOGS + 5 {
            sink.route(&format!("[Consumer clientId=consumer-rust-{}] line\n", 1000 + i));
        }
        assert!(sink.per_client.len() <= MAX_OPEN_CLIENT_LOGS, "{} open", sink.per_client.len());
        sink.route("[Producer clientId=producer-rust-1] after eviction\n");
        sink.flush_all();
        assert_eq!(
            fs::read_to_string(dir.join("client-producer-rust-1.log")).unwrap(),
            "[Producer clientId=producer-rust-1] first\n[Producer clientId=producer-rust-1] late\n\
             [Producer clientId=producer-rust-1] after eviction\n"
        );
        let _ = fs::remove_dir_all(dir);
    }

    /// A panic while the global sink was locked does not make logging or
    /// `finish` panic afterwards.
    #[test]
    fn a_poisoned_sink_is_still_usable() {
        let _ = std::thread::spawn(|| {
            let _guard = sink();
            panic!("poison the log sink");
        })
        .join();
        close_client_log("nobody");
        log::Log::flush(&ChaosLogger);
        log::Log::log(
            &ChaosLogger,
            &log::Record::builder().args(format_args!("still logging")).build(),
        );
        let dir = temp_dir("poisoned-finish");
        let reports = RunReports { dir: dir.clone(), leader_log: Mutex::new(None) };
        reports.record_leader_change("no file: dropped");
        reports.write_verdict("PASS");
        assert_eq!(reports.finish(), dir);
        assert_eq!(fs::read_to_string(dir.join("verdict.txt")).unwrap(), "PASS");
        assert!(dir.join("summary.txt").exists());
        let _ = fs::remove_dir_all(dir);
    }
}
