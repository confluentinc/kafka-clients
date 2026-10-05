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

//! Keeping the chaos harness alive when a client under test misbehaves.
//!
//! The harness used to drive every workload future and the fault scenario on
//! one task. One client future that never yielded (the consumer `poll()` spin
//! while the coordinator is unknown, Sep 2026 matrix finding F1) then froze the
//! scenario too, so the broker it had stopped was never restarted and the run
//! stalled until the matrix runner killed it, with no verdict. And one stuck
//! client task kept the test runtime from shutting down, so a run that had
//! already panicked never exited (F8).
//!
//! This module holds the pieces that prevent that:
//!
//! - [`WorkloadThreads`] runs each workload on its own OS thread with its own
//!   small runtime. The workload futures are not `Send` (see
//!   [`super::workload::Workload`]), so each one is built on the thread that
//!   runs it. A client that never yields now burns only its own thread; the
//!   scenario keeps running and the watchdogs keep working.
//! - [`Heartbeat`] + [`start_heartbeat_watchdog`] catch the scenario task itself
//!   blocking. The tokio watchdog in `run_test` cannot fire then, because it
//!   runs on that same task.
//! - A process-wide panic hook ([`WorkloadThreads`] installs it) reports a
//!   panic in a client's background task (producer sender, consumer network
//!   loop, a delivery callback) as that workload's panic. Tokio catches those
//!   panics, so without the hook they never reached the verdict.
//! - [`ClusterTeardown`] removes the cluster without the harness, for the
//!   heartbeat watchdog and the signal handler to use. Every docker call it
//!   makes is bounded.
//! - [`signals`] handles Ctrl-C / SIGTERM from the very start of a run, on its
//!   own thread, so a signal is acted on even while the scenario task is
//!   blocked and a cluster is never leaked by one.
//! - [`ForcedExit`] ends a process that is still alive after its wind-down
//!   budget: armed before the reports are written and the cluster is torn
//!   down, re-armed once they are.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use super::verifier::Verifier;
use super::workload::{Role, WorkloadContext, WorkloadSpec, build_workload};

/// Worker threads in each workload's runtime. Two, so the client's background
/// task (consumer network loop, producer sender) can progress while the
/// workload's own future is busy on the other worker.
const WORKLOAD_WORKER_THREADS: usize = 2;

/// How long a finished workload's runtime may take to stop its leftover client
/// tasks before the thread gives up on them and exits anyway.
const WORKLOAD_RUNTIME_SHUTDOWN: Duration = Duration::from_secs(5);

/// Appended to a workload's label to name its runtime's worker threads, which
/// is how the panic hook maps a panicking thread back to its workload.
const RUNTIME_THREAD_SUFFIX: &str = "-rt";

/// How long one teardown `docker` command may take before it is killed.
const DOCKER_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on a whole [`ClusterTeardown::run`]: the container removal, the
/// network inspect, a few endpoint disconnects and the network removal, each
/// bounded by [`DOCKER_COMMAND_TIMEOUT`]. Paths that tear down right before
/// exiting arm a [`ForcedExit`] for this long first.
pub const TEARDOWN_BUDGET: Duration = Duration::from_secs(180);

/// Lock `mutex`, recovering the data if a thread panicked while holding it.
/// Everything this module guards is plain bookkeeping that stays consistent
/// across a panic, and a poisoned lock must not turn one failure into a
/// cascade of `expect` panics that hide it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The message of a panic payload (`panic!` with a literal or a formatted
/// string); anything else is reported generically.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// What a workload thread reports back.
#[derive(Default)]
struct ThreadState {
    /// Set as the thread's very last act, after the workload returned or
    /// panicked and its runtime was shut down.
    finished: AtomicBool,
    /// The first panic of the workload: its own future's, or one of its
    /// client's background tasks' (recorded by the panic hook). Always set
    /// before `finished`.
    panic: Mutex<Option<String>>,
}

impl ThreadState {
    /// Record `message` unless an earlier panic is already recorded: the first
    /// panic is the cause, later ones (a `close()` failing because the sender
    /// task died) are its consequences.
    fn record_panic(&self, message: impl FnOnce() -> String) {
        lock(&self.panic).get_or_insert_with(message);
    }
}

/// Workload label -> the states of the live workloads with that label, for the
/// panic hook. A label is unique within a run; a `Vec` only so that two runs
/// in one process (both `#[ignore]`d scenarios at once) cannot clobber each
/// other's entry. Weak, so a finished run's states are dropped with it.
fn panic_registry() -> &'static Mutex<HashMap<String, Vec<Weak<ThreadState>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Vec<Weak<ThreadState>>>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Register `state` as the workload labelled `label`, pruning dead entries.
fn register_for_panics(label: &str, state: &Arc<ThreadState>) {
    let mut registry = lock(panic_registry());
    registry.retain(|_, states| {
        states.retain(|s| s.strong_count() > 0);
        !states.is_empty()
    });
    registry.entry(label.to_string()).or_default().push(Arc::downgrade(state));
}

/// The workload label a runtime worker thread belongs to (`<label>-rt`).
fn workload_of_runtime_thread(thread_name: &str) -> Option<&str> {
    thread_name
        .strip_suffix(RUNTIME_THREAD_SUFFIX)
        .filter(|label| !label.is_empty())
}

/// Record a panic on thread `thread_name` as its workload's panic, if the
/// thread is one of a workload runtime's workers. `message` is only built
/// when it is.
fn record_background_panic(thread_name: Option<&str>, message: impl FnOnce() -> String) {
    let Some(label) = thread_name.and_then(workload_of_runtime_thread) else {
        return;
    };
    let states: Vec<Arc<ThreadState>> = lock(panic_registry())
        .get(label)
        .map(|states| states.iter().filter_map(Weak::upgrade).collect())
        .unwrap_or_default();
    if states.is_empty() {
        return;
    }
    let message = message();
    for state in states {
        state.record_panic(|| message.clone());
    }
}

/// Install (once per process) the hook that reports panics in a workload's
/// client background tasks. Tokio catches a panic in a spawned task and only
/// hands it to that task's `JoinHandle`, which the client drops or treats as
/// a closed channel, so a panicking sender task or delivery callback used to
/// leave nothing for the verdict. A panic hook runs on the panicking thread
/// before any of that, and the thread's name says which workload it serves.
/// The previous hook still runs, so the panic is printed as before.
fn install_background_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let thread = std::thread::current();
            record_background_panic(thread.name(), || {
                let location = info
                    .location()
                    .map_or_else(|| "an unknown location".to_string(), |l| format!("{}:{}", l.file(), l.line()));
                format!(
                    "{} (in a client background task on thread '{}', at {location})",
                    panic_message(info.payload()),
                    thread.name().unwrap_or_default()
                )
            });
            previous(info);
        }));
    });
}

/// One workload thread.
struct WorkloadThread {
    role: Role,
    label: String,
    stop: Arc<AtomicBool>,
    state: Arc<ThreadState>,
}

/// Every workload thread of a run, including consumers added mid-run.
///
/// The threads are detached: the run never joins them. A thread whose client
/// never returns is reported by [`Self::stop_and_wait`], and the process exits
/// without it.
#[derive(Default)]
pub struct WorkloadThreads {
    threads: Mutex<Vec<WorkloadThread>>,
}

impl WorkloadThreads {
    /// Build and start the workload `spec` on a new thread. Returns its stop
    /// flag once the workload is built (and has started running).
    ///
    /// Panics if the workload cannot be built (for example a gRPC backend
    /// without the `multilanguage-tests` feature), as building it on the
    /// caller's task did.
    pub async fn spawn_workload(
        &self,
        spec: WorkloadSpec,
        ctx: WorkloadContext,
        verifier: Arc<dyn Verifier>,
        broker_network: String,
    ) -> Arc<AtomicBool> {
        let label = spec.label();
        let role = spec.role;
        self.spawn_with(role, label, move |stop, built| async move {
            match build_workload(&spec, ctx, verifier, &broker_network).await {
                Ok(workload) => {
                    let _ = built.send(Ok(()));
                    workload.run(stop).await;
                },
                Err(err) => {
                    let _ = built.send(Err(err));
                },
            }
        })
        .await
    }

    /// Start `make`'s future on a new thread with its own runtime. `make`
    /// receives the stop flag and a channel on which it reports whether the
    /// workload was built: `Err` fails the spawn (this call panics); dropping
    /// the sender without sending means the thread panicked while building,
    /// which [`Self::first_panic`] then reports. A panic in any task the
    /// future spawns on its runtime is reported the same way.
    pub(super) async fn spawn_with<F, Fut>(&self, role: Role, label: String, make: F) -> Arc<AtomicBool>
    where
        F: FnOnce(Arc<AtomicBool>, tokio::sync::oneshot::Sender<Result<(), String>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        install_background_panic_hook();
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(ThreadState::default());
        // Registered before the thread exists, so not even a panic in the
        // runtime's first task can slip past the hook.
        register_for_panics(&label, &state);
        let (built_tx, built_rx) = tokio::sync::oneshot::channel();
        {
            let stop = stop.clone();
            let state = state.clone();
            let thread_label = label.clone();
            std::thread::Builder::new()
                .name(label.clone())
                .spawn(move || {
                    // The runtime is built outside `catch_unwind` so that it
                    // survives a panic in the workload and is always shut down
                    // below. A panic that unwound through it would drop it
                    // instead, which is the unbounded wait described there.
                    let runtime = match tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(WORKLOAD_WORKER_THREADS)
                        .thread_name(format!("{thread_label}{RUNTIME_THREAD_SUFFIX}"))
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(err) => {
                            state.record_panic(|| format!("failed to build the workload runtime: {err}"));
                            state.finished.store(true, Ordering::SeqCst);
                            return;
                        },
                    };
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime.block_on(make(stop, built_tx));
                    }));
                    // Not a plain drop, even after a panic: that waits for every
                    // task, and a client task that never yields would keep this
                    // thread (and `finished`, and the panic report) from ever
                    // completing.
                    runtime.shutdown_timeout(WORKLOAD_RUNTIME_SHUTDOWN);
                    if let Err(payload) = outcome {
                        state.record_panic(|| panic_message(payload.as_ref()));
                    }
                    // The client is closed; release its log file (churn mints a
                    // new client id per added consumer, so the files would
                    // otherwise pile up until the run ends).
                    super::reports::close_client_log(&thread_label);
                    state.finished.store(true, Ordering::SeqCst);
                })
                .expect("failed to spawn a workload thread");
        }
        lock(&self.threads).push(WorkloadThread { role, label: label.clone(), stop: stop.clone(), state });
        if let Ok(Err(err)) = built_rx.await {
            panic!("failed to build workload {label}: {err}");
        }
        stop
    }

    /// Tell every workload of `role` to stop (producers flush and close,
    /// consumers commit and close).
    pub fn stop_role(&self, role: Role) {
        for t in lock(&self.threads).iter() {
            if t.role == role {
                t.stop.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Tell every workload to stop.
    pub fn stop_all(&self) {
        for t in lock(&self.threads).iter() {
            t.stop.store(true, Ordering::Relaxed);
        }
    }

    /// Whether every workload of `role` (every workload, for `None`) has
    /// finished.
    pub fn all_finished(&self, role: Option<Role>) -> bool {
        lock(&self.threads)
            .iter()
            .filter(|t| role.is_none_or(|r| t.role == r))
            .all(|t| t.state.finished.load(Ordering::SeqCst))
    }

    /// The first workload that panicked, as `(label, message)`.
    ///
    /// A thread records its panic before it marks itself finished, so a caller
    /// that saw [`Self::all_finished`] return `true` and then calls this sees
    /// every panic of those workloads.
    pub fn first_panic(&self) -> Option<(String, String)> {
        lock(&self.threads)
            .iter()
            .find_map(|t| lock(&t.state.panic).clone().map(|message| (t.label.clone(), message)))
    }

    /// Labels of the workloads of `role` (every workload, for `None`) still
    /// running.
    pub fn unfinished(&self, role: Option<Role>) -> Vec<String> {
        lock(&self.threads)
            .iter()
            .filter(|t| role.is_none_or(|r| t.role == r) && !t.state.finished.load(Ordering::SeqCst))
            .map(|t| t.label.clone())
            .collect()
    }

    /// Stop every workload and wait up to `timeout` for them to finish.
    /// Returns the labels of those that did not.
    ///
    /// Used when a run is abandoned (watchdog, panic, Ctrl-C). Before this,
    /// the workloads were dropped mid-flight. That left sends in flight at
    /// verdict time, which the verdict scored as unsettled, although it was the
    /// abort that left them open.
    pub async fn stop_and_wait(&self, timeout: Duration) -> Vec<String> {
        self.stop_all();
        let deadline = Instant::now() + timeout;
        loop {
            let running = self.unfinished(None);
            if running.is_empty() || Instant::now() >= deadline {
                return running;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Proof of life from the scenario task, checked by a plain thread
/// ([`start_heartbeat_watchdog`]).
pub struct Heartbeat {
    epoch: Instant,
    /// Milliseconds since `epoch` at the last beat.
    last_ms: AtomicU64,
    armed: AtomicBool,
    /// What the drive is doing (building workloads, the scenario, a close
    /// phase, ...), so a watchdog that fires can say where the run was stuck.
    phase: Mutex<&'static str>,
}

impl Heartbeat {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            last_ms: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            phase: Mutex::new("setup"),
        })
    }

    /// Record that the scenario task is alive.
    pub fn beat(&self) {
        self.last_ms.store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Start watching (beats first, so the arming moment counts as a beat).
    pub fn arm(&self) {
        self.beat();
        self.armed.store(true, Ordering::Relaxed);
    }

    /// Stop watching: the scenario task has legitimately stopped beating.
    pub fn disarm(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }

    /// Record the phase the drive has entered.
    pub fn set_phase(&self, phase: &'static str) {
        *lock(&self.phase) = phase;
    }

    /// The phase the drive was last in.
    pub fn phase(&self) -> &'static str {
        *lock(&self.phase)
    }

    /// How long since the last beat, or `None` while disarmed.
    fn silent_for(&self) -> Option<Duration> {
        if !self.armed.load(Ordering::Relaxed) {
            return None;
        }
        let now = self.epoch.elapsed().as_millis() as u64;
        Some(Duration::from_millis(now.saturating_sub(self.last_ms.load(Ordering::Relaxed))))
    }
}

/// Watch `heartbeat` from a plain thread. If the scenario task stays silent for
/// `stale_after` while armed, print the `chaos: WATCHDOG` notice the matrix
/// runner classifies, tear the cluster down and exit the process.
///
/// This covers the one wedge the run's own (tokio) watchdog cannot: the
/// scenario task blocking outright, which stops that watchdog too.
pub fn start_heartbeat_watchdog(heartbeat: Arc<Heartbeat>, stale_after: Duration, teardown: ClusterTeardown) {
    std::thread::Builder::new()
        .name("chaos-heartbeat-watchdog".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                if let Some(silent) = heartbeat.silent_for()
                    && silent >= stale_after
                {
                    eprintln!(
                        "chaos: WATCHDOG — the scenario task made no progress for {silent:?} during the {} phase \
                         (a client or the scenario is blocking the harness runtime). No verdict; tearing the \
                         cluster down and exiting.",
                        heartbeat.phase()
                    );
                    log::logger().flush();
                    // The teardown is bounded, but this exit must not depend
                    // on it.
                    let _guard = ForcedExit::arm(
                        TEARDOWN_BUDGET,
                        101,
                        "chaos: FORCED EXIT — the watchdog's cluster teardown did not finish".to_string(),
                    );
                    teardown.run();
                    std::process::exit(101);
                }
            }
        })
        .expect("failed to spawn the heartbeat watchdog");
}

/// When a [`ForcedExit`] fires, and what it prints and exits with.
struct ForcedExitPlan {
    at: Instant,
    code: i32,
    notice: String,
}

/// Ends the process at a deadline unless it has exited by then, printing a
/// notice first. Armed when a run starts winding down: whatever still holds
/// the process open after that (a client task that never yields keeps the
/// test runtime from shutting down, P3-1MiB, Sep 2026 matrix; a report or
/// teardown step that hangs) must not turn a finished run into a stalled one.
/// [`Self::rearm`] moves the deadline and changes the outcome, for example
/// once the verdict is known.
pub struct ForcedExit {
    plan: Arc<Mutex<ForcedExitPlan>>,
}

impl ForcedExit {
    /// Exit with `code` after `after`, printing `notice`, unless re-armed.
    pub fn arm(after: Duration, code: i32, notice: String) -> Self {
        let plan = Arc::new(Mutex::new(ForcedExitPlan { at: Instant::now() + after, code, notice }));
        let watched = plan.clone();
        std::thread::Builder::new()
            .name("chaos-forced-exit".into())
            .spawn(move || {
                loop {
                    let remaining = {
                        let plan = lock(&watched);
                        let remaining = plan.at.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            eprintln!("{}", plan.notice);
                            log::logger().flush();
                            std::process::exit(plan.code);
                        }
                        remaining
                    };
                    std::thread::sleep(remaining.min(Duration::from_millis(250)));
                }
            })
            .expect("failed to spawn the forced-exit guard");
        Self { plan }
    }

    /// Replace the deadline, exit code and notice.
    pub fn rearm(&self, after: Duration, code: i32, notice: String) {
        *lock(&self.plan) = ForcedExitPlan { at: Instant::now() + after, code, notice };
    }
}

/// Run `program args`, killing it if it has not exited within `timeout`.
/// Returns its stdout once it exits, or `None` if it could not be started or
/// was killed. A docker daemon that stopped answering used to hang the
/// teardown, and with it the watchdog and the signal handler that rely on it.
fn run_bounded(program: &str, args: &[&str], timeout: Duration) -> Option<String> {
    use std::io::Read as _;
    use std::process::{Command, Stdio};
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Read stdout on a side thread: a child blocked on a full pipe would never
    // exit while this thread only polls for its exit.
    let reader = child.stdout.take().map(|mut stdout| {
        std::thread::spawn(move || {
            let mut out = String::new();
            let _ = stdout.read_to_string(&mut out);
            out
        })
    });
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Some(reader.and_then(|r| r.join().ok()).unwrap_or_default()),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                eprintln!(
                    "chaos: WARN `{program} {}` did not finish within {timeout:?}; killed it",
                    args.join(" ")
                );
                // The reader is left to end with the pipe.
                return None;
            },
        }
    }
}

/// What it takes to remove a chaos cluster: its containers and its network.
/// A plain value, so a thread can do it without the harness.
#[derive(Clone)]
pub struct ClusterTeardown {
    pub containers: Vec<String>,
    pub network: String,
}

impl ClusterTeardown {
    /// Best-effort removal of the containers (and their anonymous volumes) and
    /// the network. Every docker call is bounded by [`DOCKER_COMMAND_TIMEOUT`],
    /// so the whole teardown stays within [`TEARDOWN_BUDGET`].
    pub fn run(&self) {
        let docker = |args: &[&str]| run_bounded("docker", args, DOCKER_COMMAND_TIMEOUT);
        // `-v` also removes the anonymous volumes the broker image declares
        // (`/var/lib/kafka/data` among them); without it every run left three
        // volumes behind per broker. One call for every container: `rm -f`
        // removes the ones that exist even when another is already gone.
        if !self.containers.is_empty() {
            let mut args = vec!["rm", "-f", "-v"];
            args.extend(self.containers.iter().map(String::as_str));
            let _ = docker(&args);
        }
        // Anything still attached (the gRPC backend sidecar lives in a
        // process-wide pool and outlives this cluster) makes `network rm` fail
        // with "network has active endpoints", which used to leak one
        // `kafka-net-*` per gRPC run. Detach every endpoint first; that only
        // removes the container's membership of THIS network, nothing else.
        if let Some(out) = docker(&[
            "network",
            "inspect",
            "-f",
            "{{range .Containers}}{{.Name}}\n{{end}}",
            &self.network,
        ]) {
            for attached in out.lines().filter(|l| !l.is_empty()) {
                let _ = docker(&["network", "disconnect", "-f", &self.network, attached]);
            }
        }
        let _ = docker(&["network", "rm", &self.network]);
    }
}

/// Ctrl-C (SIGINT) and SIGTERM handling for a chaos run.
///
/// Before this, the only handler was a `tokio::signal::ctrl_c()` arm in the
/// runner's final `select!`. A signal during setup (a cluster start of up to
/// 3 × 90 s, topic creation, the offsets-topic wait) killed the process
/// without running any `Drop`, leaking the brokers, their volumes and the
/// `kafka-net-*` network; SIGTERM was never handled at all; once tokio's
/// handler was installed a second Ctrl-C did nothing; and a signal arriving
/// while the scenario task was blocked was not seen until the heartbeat
/// watchdog fired two minutes later.
///
/// [`install`] starts one thread with its own runtime listening for both
/// signals, at the very start of the run, so a signal is always seen at once.
/// What it does depends on where the run is:
///
/// - while the drive runs ([`enable_orderly_abort`]), the first signal asks for
///   an orderly abort ([`abort_requested`] completes): the runner stops the
///   workloads, writes the reports and tears down, as before. If the runner
///   does not take it up within [`ABORT_ACK_TIMEOUT`] (its task is blocked),
///   the handler tears down and exits itself;
/// - once the drive has ended ([`end_orderly_abort`]), the run is already
///   winding down in order (reports, teardown, bounded by its forced exit):
///   the first signal lets it finish;
/// - otherwise (setup), or on a second signal, it tears every registered
///   cluster down at once and exits with 130;
/// - during a cluster start, before the cluster's containers are known, the
///   first signal is remembered and the cluster is torn down as soon as it is
///   registered; a second signal exits at once (leaking that cluster).
pub mod signals {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// How long the runner has to take up an orderly abort before the handler
    /// tears the cluster down and exits itself.
    const ABORT_ACK_TIMEOUT: Duration = Duration::from_secs(10);

    /// The exit code of a run ended by a signal (128 + SIGINT, as a shell
    /// reports Ctrl-C).
    const SIGNAL_EXIT_CODE: i32 = 130;

    /// What a signal does, given the state of the run.
    #[derive(Debug, PartialEq, Eq)]
    pub(super) enum SignalAction {
        /// Ask the runner to stop the workloads, write reports, tear down.
        OrderlyAbort,
        /// Remember it and tear the cluster down once it is registered.
        ExitOnceClusterIsUp,
        /// Nothing: the run is already winding down in order.
        AlreadyWindingDown,
        /// Tear every registered cluster down now and exit.
        TearDownAndExit,
    }

    /// The handler's view of the run.
    #[derive(Default)]
    pub(super) struct SignalState {
        /// Every cluster to remove before exiting.
        pub(super) teardowns: Mutex<Vec<ClusterTeardown>>,
        /// Signals received so far.
        pub(super) received: AtomicU32,
        /// A cluster start is in progress (its containers are not known yet).
        pub(super) cluster_starting: AtomicBool,
        /// The runner is listening on [`abort_requested`].
        pub(super) orderly: AtomicBool,
        pub(super) abort_requested: AtomicBool,
        pub(super) abort_acknowledged: AtomicBool,
        /// The drive has ended; the runner is writing reports and tearing down.
        pub(super) winding_down: AtomicBool,
        /// A signal arrived during a cluster start: exit once it is registered.
        pub(super) exit_pending: AtomicBool,
        pub(super) abort: tokio::sync::Notify,
    }

    impl SignalState {
        /// Count one more signal and decide what it does.
        pub(super) fn on_signal(&self) -> SignalAction {
            let nth = self.received.fetch_add(1, Ordering::SeqCst) + 1;
            if nth > 1 {
                SignalAction::TearDownAndExit
            } else if self.orderly.load(Ordering::SeqCst) {
                self.abort_requested.store(true, Ordering::SeqCst);
                self.abort.notify_waiters();
                SignalAction::OrderlyAbort
            } else if self.winding_down.load(Ordering::SeqCst) {
                SignalAction::AlreadyWindingDown
            } else if self.cluster_starting.load(Ordering::SeqCst) && lock(&self.teardowns).is_empty() {
                self.exit_pending.store(true, Ordering::SeqCst);
                SignalAction::ExitOnceClusterIsUp
            } else {
                SignalAction::TearDownAndExit
            }
        }

        /// Completes once an orderly abort has been requested.
        pub(super) async fn aborted(&self) {
            loop {
                let notified = self.abort.notified();
                let mut notified = std::pin::pin!(notified);
                // Registered before the flag is checked, so a request between
                // the check and the await is not lost.
                notified.as_mut().enable();
                if self.abort_requested.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        }

        /// Close the orderly-abort window; returns whether an abort was
        /// requested in it (and acknowledges it).
        pub(super) fn end_orderly(&self) -> bool {
            // Winding down first: an abort requested concurrently with this
            // call is then taken up by the wind-down (see
            // `watch_abort_acknowledged`) rather than seen as ignored.
            self.winding_down.store(true, Ordering::SeqCst);
            self.orderly.store(false, Ordering::SeqCst);
            let requested = self.abort_requested.load(Ordering::SeqCst);
            if requested {
                self.abort_acknowledged.store(true, Ordering::SeqCst);
            }
            requested
        }
    }

    fn state() -> &'static SignalState {
        static STATE: OnceLock<SignalState> = OnceLock::new();
        STATE.get_or_init(SignalState::default)
    }

    /// Start listening for SIGINT and SIGTERM (once per process). Call first
    /// thing in a run, before any cluster exists.
    pub fn install() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            let spawned = std::thread::Builder::new().name("chaos-signals".into()).spawn(|| {
                let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        eprintln!("chaos: WARN no signal handling ({err}); a signal kills the run without teardown");
                        return;
                    },
                };
                runtime.block_on(listen());
            });
            if let Err(err) = spawned {
                eprintln!("chaos: WARN no signal handling ({err}); a signal kills the run without teardown");
            }
        });
    }

    #[cfg(unix)]
    async fn listen() {
        use tokio::signal::unix::{SignalKind, signal};
        let (mut interrupt, mut terminate) = match (signal(SignalKind::interrupt()), signal(SignalKind::terminate())) {
            (Ok(interrupt), Ok(terminate)) => (interrupt, terminate),
            (interrupt, terminate) => {
                eprintln!(
                    "chaos: WARN cannot listen for SIGINT/SIGTERM ({:?} / {:?}); a signal kills the run without \
                     teardown",
                    interrupt.err(),
                    terminate.err()
                );
                return;
            },
        };
        loop {
            // `recv` is cancel-safe: the losing arm loses no signal.
            let name = tokio::select! {
                Some(()) = interrupt.recv() => "SIGINT (Ctrl-C)",
                Some(()) = terminate.recv() => "SIGTERM",
                else => return,
            };
            handle(name);
        }
    }

    #[cfg(not(unix))]
    async fn listen() {
        while tokio::signal::ctrl_c().await.is_ok() {
            handle("Ctrl-C");
        }
    }

    fn handle(name: &'static str) {
        match state().on_signal() {
            SignalAction::OrderlyAbort => {
                eprintln!(
                    "chaos: {name} received — aborting the run (stopping workloads, writing reports, tearing the \
                     cluster down). Send it again to tear down immediately and exit."
                );
                watch_abort_acknowledged(name);
            },
            SignalAction::ExitOnceClusterIsUp => eprintln!(
                "chaos: {name} received during the cluster start — the cluster is torn down as soon as it is up. \
                 Send it again to exit now (that cluster's containers would leak)."
            ),
            SignalAction::AlreadyWindingDown => eprintln!(
                "chaos: {name} received — the run is already writing its reports and tearing the cluster down. \
                 Send it again to tear down immediately and exit."
            ),
            SignalAction::TearDownAndExit => tear_down_and_exit(name),
        }
    }

    /// Tear the run down from the handler if the runner does not take up the
    /// abort in time: its task is blocked and would only be ended by the
    /// heartbeat watchdog, minutes later.
    fn watch_abort_acknowledged(name: &'static str) {
        let _ = std::thread::Builder::new().name("chaos-abort-ack".into()).spawn(move || {
            let deadline = Instant::now() + ABORT_ACK_TIMEOUT;
            while Instant::now() < deadline {
                let state = state();
                if state.abort_acknowledged.load(Ordering::SeqCst) || state.winding_down.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            tear_down_and_exit(&format!(
                "{name}; the run did not respond to the abort within {ABORT_ACK_TIMEOUT:?}"
            ));
        });
    }

    /// Remove every registered cluster and exit with [`SIGNAL_EXIT_CODE`].
    fn tear_down_and_exit(reason: &str) -> ! {
        eprintln!("chaos: INTERRUPTED ({reason}) — tearing the cluster down now and exiting");
        log::logger().flush();
        let teardowns = lock(&state().teardowns).clone();
        if !teardowns.is_empty() {
            let _guard = ForcedExit::arm(
                TEARDOWN_BUDGET,
                SIGNAL_EXIT_CODE,
                "chaos: FORCED EXIT — the cluster teardown after a signal did not finish".to_string(),
            );
            for teardown in &teardowns {
                teardown.run();
            }
        }
        std::process::exit(SIGNAL_EXIT_CODE);
    }

    /// Marks a cluster start in progress until dropped (also on a panicking
    /// start). A signal in the meantime is acted on when the cluster is
    /// registered, or here if the start never produced one.
    pub struct ClusterStart(());

    impl ClusterStart {
        /// Also starts a new run as far as signals go: whatever an earlier run
        /// in this process left behind (its signal count, its wind-down) does
        /// not decide what a signal does to this one.
        pub fn begin() -> Self {
            let state = state();
            state.received.store(0, Ordering::SeqCst);
            state.abort_requested.store(false, Ordering::SeqCst);
            state.abort_acknowledged.store(false, Ordering::SeqCst);
            state.winding_down.store(false, Ordering::SeqCst);
            state.cluster_starting.store(true, Ordering::SeqCst);
            Self(())
        }
    }

    impl Drop for ClusterStart {
        fn drop(&mut self) {
            let state = state();
            state.cluster_starting.store(false, Ordering::SeqCst);
            if state.exit_pending.load(Ordering::SeqCst) {
                // Registered already when the start succeeded (which exits);
                // reaching here means it produced no cluster to remove.
                tear_down_and_exit("signal received during a cluster start that failed");
            }
        }
    }

    /// Register a running cluster for removal on a signal. If a signal arrived
    /// while it was starting, tear it down and exit now.
    pub fn register_cluster(teardown: ClusterTeardown) {
        let state = state();
        lock(&state.teardowns).push(teardown);
        if state.exit_pending.load(Ordering::SeqCst) {
            tear_down_and_exit("signal received during the cluster start");
        }
    }

    /// Forget the cluster on `network`: it has been torn down.
    pub fn unregister_cluster(network: &str) {
        lock(&state().teardowns).retain(|t| t.network != network);
    }

    /// From now on, a first signal asks for an orderly abort
    /// ([`abort_requested`]) instead of exiting.
    pub fn enable_orderly_abort() {
        state().orderly.store(true, Ordering::SeqCst);
    }

    /// Completes once a signal has asked for an orderly abort.
    pub async fn abort_requested() {
        state().aborted().await;
    }

    /// The runner has stopped listening for an orderly abort and is winding
    /// down: a first signal from now on lets it finish, a second tears down
    /// and exits. Returns whether an abort was requested (the runner is now
    /// carrying it out, so the handler stands down).
    pub fn end_orderly_abort() -> bool {
        state().end_orderly()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A workload that never yields and ignores its stop flag runs on its own
    /// thread, so the caller's runtime keeps running timers. This is the F1
    /// stall reproduced without a broker. `stop_and_wait` reports the stuck
    /// workload instead of hanging.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_workload_that_never_yields_does_not_block_the_caller() {
        let threads = WorkloadThreads::default();
        let release = Arc::new(AtomicBool::new(false));
        let spin_release = release.clone();
        threads
            .spawn_with(Role::Consumer, "consumer-spin".into(), move |_stop, built| async move {
                let _ = built.send(Ok(()));
                // No await point: this future never yields.
                while !spin_release.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
            .await;

        let started = Instant::now();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the caller's timers must keep running"
        );

        let stuck = threads.stop_and_wait(Duration::from_millis(300)).await;
        assert_eq!(stuck, vec!["consumer-spin".to_string()]);

        release.store(true, Ordering::Relaxed);
        let stuck = threads.stop_and_wait(Duration::from_secs(10)).await;
        assert!(stuck.is_empty(), "released workload must finish: {stuck:?}");
    }

    /// Stop flags reach the right role, and completion is tracked per role.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_role_stops_only_that_role_and_completion_is_tracked() {
        let threads = WorkloadThreads::default();
        let until_stopped = |stop: Arc<AtomicBool>, built: tokio::sync::oneshot::Sender<Result<(), String>>| async move {
            let _ = built.send(Ok(()));
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        threads.spawn_with(Role::Producer, "producer-1".into(), until_stopped).await;
        threads.spawn_with(Role::Consumer, "consumer-1".into(), until_stopped).await;

        threads.stop_role(Role::Producer);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !threads.all_finished(Some(Role::Producer)) {
            assert!(Instant::now() < deadline, "producer did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!threads.all_finished(Some(Role::Consumer)));
        assert!(!threads.all_finished(None));
        assert_eq!(threads.unfinished(None), vec!["consumer-1".to_string()]);
        assert!(threads.unfinished(Some(Role::Producer)).is_empty());

        assert!(threads.stop_and_wait(Duration::from_secs(10)).await.is_empty());
        assert!(threads.all_finished(None));
        assert!(threads.first_panic().is_none());
    }

    /// A panicking workload is caught on its thread and reported with its
    /// label and message, for the drive loop to re-raise.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_workload_is_reported_with_its_message() {
        let threads = WorkloadThreads::default();
        threads
            .spawn_with(Role::Producer, "producer-boom".into(), |_stop, built| async move {
                let _ = built.send(Ok(()));
                panic!("chaos producer close failed: boom");
            })
            .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while threads.first_panic().is_none() {
            assert!(Instant::now() < deadline, "panic was not reported");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            threads.first_panic(),
            Some(("producer-boom".to_string(), "chaos producer close failed: boom".to_string()))
        );
        assert!(threads.all_finished(None));
    }

    /// A workload that panics while one of its client tasks never yields is
    /// still reported, with its message, once the runtime's bounded shutdown
    /// gives up on that task. Dropping the runtime during the unwind instead
    /// waited for the task forever, so the panic was never reported and the run
    /// ended as a watchdog wedge with the message lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panic_is_reported_even_when_a_client_task_never_yields() {
        let threads = WorkloadThreads::default();
        let release = Arc::new(AtomicBool::new(false));
        let spin_release = release.clone();
        threads
            .spawn_with(Role::Consumer, "consumer-stuck".into(), move |_stop, built| async move {
                let started = Arc::new(AtomicBool::new(false));
                let spin_started = started.clone();
                tokio::spawn(async move {
                    spin_started.store(true, Ordering::SeqCst);
                    // No await point: this task never yields.
                    while !spin_release.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                });
                while !started.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                let _ = built.send(Ok(()));
                panic!("chaos consumer close failed: stuck");
            })
            .await;

        let deadline = Instant::now() + WORKLOAD_RUNTIME_SHUTDOWN + Duration::from_secs(10);
        while threads.first_panic().is_none() {
            assert!(Instant::now() < deadline, "panic was not reported past the bounded shutdown");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            threads.first_panic(),
            Some(("consumer-stuck".to_string(), "chaos consumer close failed: stuck".to_string()))
        );
        assert!(threads.all_finished(None));
        release.store(true, Ordering::Relaxed);
    }

    /// A build error fails the spawn on the caller, as building on the
    /// caller's task did.
    #[tokio::test(flavor = "multi_thread")]
    #[should_panic(expected = "failed to build workload consumer-grpc: needs the multilanguage-tests feature")]
    async fn a_build_error_fails_the_spawn() {
        let threads = WorkloadThreads::default();
        threads
            .spawn_with(Role::Consumer, "consumer-grpc".into(), |_stop, built| async move {
                let _ = built.send(Err("needs the multilanguage-tests feature".to_string()));
            })
            .await;
    }

    /// A panic in a task the workload spawned on its runtime (the producer's
    /// sender task, a delivery callback) is swallowed by tokio; the panic hook
    /// reports it as the workload's panic while the workload itself keeps
    /// running.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panic_in_a_spawned_client_task_is_reported() {
        let threads = WorkloadThreads::default();
        threads
            .spawn_with(Role::Producer, "producer-bgpanic-test".into(), |stop, built| async move {
                let _ = built.send(Ok(()));
                // Like a client's background task: tokio catches the panic and
                // the workload's own future carries on.
                let joined = tokio::spawn(async { panic!("verifier poisoned in delivery_callback") }).await;
                assert!(joined.is_err());
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while threads.first_panic().is_none() {
            assert!(Instant::now() < deadline, "background panic was not reported");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (label, message) = threads.first_panic().expect("reported");
        assert_eq!(label, "producer-bgpanic-test");
        assert!(
            message.starts_with(
                "verifier poisoned in delivery_callback (in a client background task on thread \
                                 'producer-bgpanic-test-rt'"
            ),
            "{message}"
        );
        assert!(
            !threads.all_finished(None),
            "only the hook can have reported it: the workload is still running"
        );
        assert!(threads.stop_and_wait(Duration::from_secs(10)).await.is_empty());
    }

    /// Only a workload runtime's worker threads map to a workload.
    #[test]
    fn runtime_threads_map_to_their_workload() {
        assert_eq!(workload_of_runtime_thread("consumer-rust-1-rt"), Some("consumer-rust-1"));
        assert_eq!(workload_of_runtime_thread("consumer-rust-1"), None);
        assert_eq!(workload_of_runtime_thread("-rt"), None);
        assert_eq!(workload_of_runtime_thread("tokio-runtime-worker"), None);
    }

    /// The first panic is kept: it is the cause, later ones its consequences.
    #[test]
    fn the_first_panic_is_kept() {
        let state = ThreadState::default();
        state.record_panic(|| "sender task panicked".to_string());
        state.record_panic(|| "chaos producer close failed".to_string());
        assert_eq!(lock(&state.panic).as_deref(), Some("sender task panicked"));
    }

    /// A poisoned registry lock does not cascade into more panics.
    #[test]
    fn a_poisoned_registry_is_still_usable() {
        let threads = Arc::new(WorkloadThreads::default());
        let poisoner = threads.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.threads.lock().unwrap();
            panic!("poison the registry");
        })
        .join();
        assert!(threads.threads.is_poisoned());
        assert!(threads.all_finished(None));
        assert!(threads.first_panic().is_none());
        threads.stop_all();
    }

    /// A command that outlives its timeout is killed; one that exits in time
    /// returns its stdout.
    #[cfg(unix)]
    #[test]
    fn bounded_commands_are_killed_at_their_timeout() {
        let started = Instant::now();
        assert_eq!(run_bounded("sleep", &["30"], Duration::from_millis(200)), None);
        assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
        assert_eq!(
            run_bounded("echo", &["chaos"], Duration::from_secs(10)).as_deref(),
            Some("chaos\n")
        );
        assert_eq!(run_bounded("no-such-chaos-binary", &[], Duration::from_secs(1)), None);
    }

    /// A first signal aborts the drive in order when the runner listens, and
    /// is deferred during a cluster start; a second one always exits.
    #[tokio::test]
    async fn signals_abort_in_order_only_while_the_drive_listens() {
        use signals::{SignalAction, SignalState};

        let setup = SignalState::default();
        assert_eq!(setup.on_signal(), SignalAction::TearDownAndExit, "setup: exit at once");

        let starting = SignalState::default();
        starting.cluster_starting.store(true, Ordering::SeqCst);
        assert_eq!(starting.on_signal(), SignalAction::ExitOnceClusterIsUp);
        assert!(starting.exit_pending.load(Ordering::SeqCst));
        assert_eq!(starting.on_signal(), SignalAction::TearDownAndExit, "a second signal exits");

        let driving = SignalState::default();
        driving.orderly.store(true, Ordering::SeqCst);
        let aborted = driving.aborted();
        let mut aborted = std::pin::pin!(aborted);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), aborted.as_mut()).await.is_err(),
            "no abort before a signal"
        );
        assert_eq!(driving.on_signal(), SignalAction::OrderlyAbort);
        tokio::time::timeout(Duration::from_secs(5), aborted)
            .await
            .expect("the abort is delivered");
        // Also to a listener that arrives after the signal.
        tokio::time::timeout(Duration::from_secs(5), driving.aborted())
            .await
            .expect("late listener");
        assert!(driving.end_orderly(), "the runner takes the abort up");
        assert!(driving.abort_acknowledged.load(Ordering::SeqCst));
        assert_eq!(driving.on_signal(), SignalAction::TearDownAndExit, "a second signal exits");

        let finished = SignalState::default();
        finished.orderly.store(true, Ordering::SeqCst);
        assert!(!finished.end_orderly(), "no abort was requested");
        assert_eq!(
            finished.on_signal(),
            SignalAction::AlreadyWindingDown,
            "wind-down: let it finish"
        );
        assert_eq!(finished.on_signal(), SignalAction::TearDownAndExit, "a second signal exits");
    }

    /// The heartbeat remembers the drive's phase for the watchdogs.
    #[test]
    fn heartbeat_reports_the_phase() {
        let hb = Heartbeat::new();
        assert_eq!(hb.phase(), "setup");
        hb.set_phase("producers closing");
        assert_eq!(hb.phase(), "producers closing");
    }

    /// The heartbeat reports silence only while armed.
    #[test]
    fn heartbeat_is_silent_only_while_armed() {
        let hb = Heartbeat::new();
        assert_eq!(hb.silent_for(), None, "disarmed at construction");
        hb.arm();
        std::thread::sleep(Duration::from_millis(30));
        assert!(hb.silent_for().expect("armed") >= Duration::from_millis(30));
        hb.beat();
        assert!(hb.silent_for().expect("armed") < Duration::from_millis(30));
        hb.disarm();
        assert_eq!(hb.silent_for(), None);
    }

    #[test]
    fn panic_message_reads_both_payload_kinds() {
        let literal: Box<dyn Any + Send> = Box::new("literal");
        let formatted: Box<dyn Any + Send> = Box::new(format!("formatted {}", 1));
        let other: Box<dyn Any + Send> = Box::new(7_u8);
        assert_eq!(panic_message(literal.as_ref()), "literal");
        assert_eq!(panic_message(formatted.as_ref()), "formatted 1");
        assert_eq!(panic_message(other.as_ref()), "non-string panic payload");
    }
}
