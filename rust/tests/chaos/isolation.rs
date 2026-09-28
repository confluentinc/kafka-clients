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
//! - [`ClusterTeardown`] removes the cluster without the harness, for the
//!   heartbeat watchdog to use.
//! - [`arm_forced_exit`] ends a process that is still alive after its reports
//!   are written.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    /// The panic message, if the workload panicked.
    panic: Mutex<Option<String>>,
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
    /// which [`Self::first_panic`] then reports.
    async fn spawn_with<F, Fut>(&self, role: Role, label: String, make: F) -> Arc<AtomicBool>
    where
        F: FnOnce(Arc<AtomicBool>, tokio::sync::oneshot::Sender<Result<(), String>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(ThreadState::default());
        let (built_tx, built_rx) = tokio::sync::oneshot::channel();
        {
            let stop = stop.clone();
            let state = state.clone();
            let thread_label = label.clone();
            std::thread::Builder::new()
                .name(label.clone())
                .spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let runtime = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(WORKLOAD_WORKER_THREADS)
                            .thread_name(format!("{thread_label}-rt"))
                            .enable_all()
                            .build()
                            .expect("failed to build the workload runtime");
                        runtime.block_on(make(stop, built_tx));
                        // Not a plain drop: that waits for every task, and a
                        // client task that never yields would keep this thread
                        // (and `finished`) from ever completing.
                        runtime.shutdown_timeout(WORKLOAD_RUNTIME_SHUTDOWN);
                    }));
                    if let Err(payload) = outcome {
                        *state.panic.lock().expect("workload state poisoned") = Some(panic_message(payload.as_ref()));
                    }
                    state.finished.store(true, Ordering::SeqCst);
                })
                .expect("failed to spawn a workload thread");
        }
        self.threads.lock().expect("workload registry poisoned").push(WorkloadThread {
            role,
            label: label.clone(),
            stop: stop.clone(),
            state,
        });
        if let Ok(Err(err)) = built_rx.await {
            panic!("failed to build workload {label}: {err}");
        }
        stop
    }

    /// Tell every workload of `role` to stop (producers flush and close,
    /// consumers commit and close).
    pub fn stop_role(&self, role: Role) {
        for t in self.threads.lock().expect("workload registry poisoned").iter() {
            if t.role == role {
                t.stop.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Tell every workload to stop.
    pub fn stop_all(&self) {
        for t in self.threads.lock().expect("workload registry poisoned").iter() {
            t.stop.store(true, Ordering::Relaxed);
        }
    }

    /// Whether every workload of `role` (every workload, for `None`) has
    /// finished.
    pub fn all_finished(&self, role: Option<Role>) -> bool {
        self.threads
            .lock()
            .expect("workload registry poisoned")
            .iter()
            .filter(|t| role.is_none_or(|r| t.role == r))
            .all(|t| t.state.finished.load(Ordering::SeqCst))
    }

    /// The first workload that panicked, as `(label, message)`.
    pub fn first_panic(&self) -> Option<(String, String)> {
        self.threads.lock().expect("workload registry poisoned").iter().find_map(|t| {
            t.state
                .panic
                .lock()
                .expect("workload state poisoned")
                .clone()
                .map(|message| (t.label.clone(), message))
        })
    }

    /// Labels of the workloads still running.
    pub fn unfinished(&self) -> Vec<String> {
        self.threads
            .lock()
            .expect("workload registry poisoned")
            .iter()
            .filter(|t| !t.state.finished.load(Ordering::SeqCst))
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
            let running = self.unfinished();
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
}

impl Heartbeat {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { epoch: Instant::now(), last_ms: AtomicU64::new(0), armed: AtomicBool::new(false) })
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
                        "chaos: WATCHDOG — the scenario task made no progress for {silent:?} (a client or the \
                         scenario is blocking the harness runtime). No verdict; tearing the cluster down and \
                         exiting."
                    );
                    log::logger().flush();
                    teardown.run();
                    std::process::exit(101);
                }
            }
        })
        .expect("failed to spawn the heartbeat watchdog");
}

/// End the process `after` from now, printing `notice` first, unless it has
/// exited by then. Armed once the run's reports are on disk and its cluster is
/// torn down: whatever still holds the process open at that point (a client
/// task that never yields keeps the test runtime from shutting down; P3-1MiB,
/// Sep 2026 matrix) must not turn a finished run into a stalled one.
pub fn arm_forced_exit(after: Duration, code: i32, notice: String) {
    std::thread::Builder::new()
        .name("chaos-forced-exit".into())
        .spawn(move || {
            std::thread::sleep(after);
            eprintln!("{notice}");
            std::process::exit(code);
        })
        .expect("failed to spawn the forced-exit guard");
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
    /// the network.
    pub fn run(&self) {
        use std::process::Command;
        // `-v` also removes the anonymous volumes the broker image declares
        // (`/var/lib/kafka/data` among them); without it every run left three
        // volumes behind per broker.
        for id in &self.containers {
            let _ = Command::new("docker").args(["rm", "-f", "-v", id]).output();
        }
        // Anything still attached (the gRPC backend sidecar lives in a
        // process-wide pool and outlives this cluster) makes `network rm` fail
        // with "network has active endpoints", which used to leak one
        // `kafka-net-*` per gRPC run. Detach every endpoint first; that only
        // removes the container's membership of THIS network, nothing else.
        if let Ok(out) = Command::new("docker")
            .args([
                "network",
                "inspect",
                "-f",
                "{{range .Containers}}{{.Name}}\n{{end}}",
                &self.network,
            ])
            .output()
        {
            for attached in String::from_utf8_lossy(&out.stdout).lines().filter(|l| !l.is_empty()) {
                let _ = Command::new("docker")
                    .args(["network", "disconnect", "-f", &self.network, attached])
                    .output();
            }
        }
        let _ = Command::new("docker").args(["network", "rm", &self.network]).output();
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
        assert_eq!(threads.unfinished(), vec!["consumer-1".to_string()]);

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
