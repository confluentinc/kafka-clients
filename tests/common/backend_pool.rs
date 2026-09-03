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

//! Process-global pool of gRPC backend containers used by the
//! multilanguage integration tests.
//!
//! Mirrors the existing [`cluster_pool`](super::cluster_pool) pattern:
//! the first test that needs the python or c backend triggers a Docker
//! container start; subsequent tests share the same container via a
//! `OnceCell`. An `atexit` hook forcibly removes the containers on
//! process exit because Rust does not run destructors for `LazyLock`
//! statics.
//!
//! Each container exposes a fixed internal port (50051 python / 50052 c)
//! that testcontainers maps to a **random** host port, returned via
//! `get_host_port_ipv4()`. This is what makes parallel test runs safe
//! against port collisions.
//!
//! See `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md` for
//! the full architecture.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

/// Retries for [`BackendHandle::channel`]'s connect: the shared backend is
/// already up, so a failure is almost always a transient Colima hiccup.
const CONNECT_ATTEMPTS: u32 = 5;
const CONNECT_RETRY_BACKOFF: Duration = Duration::from_millis(300);

/// The two non-native backends. The native rust backend doesn't need a
/// container; tests instantiate it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackendKind {
    Python,
    /// The asyncio-native Python backend (AsyncKafkaProducer / AsyncKafkaConsumer).
    /// Distinct image from [`BackendKind::Python`]; see `Dockerfile.grpc.async`.
    PythonAsync,
    C,
}

impl BackendKind {
    fn image_repository(self) -> &'static str {
        match self {
            BackendKind::Python => "confluent-kafka-rust/python-grpc-server",
            BackendKind::PythonAsync => "confluent-kafka-rust/python-async-grpc-server",
            BackendKind::C => "confluent-kafka-rust/c-grpc-server",
        }
    }

    /// The fixed internal port the gRPC server binds inside the container.
    /// Testcontainers maps this to a random host port at start time. The async
    /// python server binds the same 50051 as the sync one — they run in separate
    /// containers, so the internal ports don't collide.
    fn internal_port(self) -> u16 {
        match self {
            BackendKind::Python => 50051,
            BackendKind::PythonAsync => 50051,
            BackendKind::C => 50052,
        }
    }

    fn label(self) -> &'static str {
        match self {
            BackendKind::Python => "python",
            BackendKind::PythonAsync => "python_async",
            BackendKind::C => "c",
        }
    }
}

/// Handle to a running gRPC backend container. Holding this keeps the
/// container alive (testcontainers' `ContainerAsync` `Drop` removes it,
/// but in practice the atexit hook below catches LazyLock leaks).
pub struct BackendHandle {
    _container: ContainerAsync<GenericImage>,
    container_id: String,
    /// `http://127.0.0.1:<random_port>` reachable from the test process.
    endpoint: String,
}

impl BackendHandle {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    /// Build a tonic [`Channel`] pointing at this backend. Channels are
    /// cheap to clone (they share the underlying connection pool), so
    /// callers should clone rather than rebuild.
    pub async fn channel(&self) -> Channel {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .expect("backend endpoint is always a valid URI")
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10));

        // Retry transient transport errors; a genuinely dead container just
        // exhausts the attempts and still panics with its logs.
        let mut last_err = None;
        for attempt in 1..=CONNECT_ATTEMPTS {
            match endpoint.connect().await {
                Ok(channel) => return channel,
                Err(e) => {
                    last_err = Some(e);
                    if attempt < CONNECT_ATTEMPTS {
                        tokio::time::sleep(CONNECT_RETRY_BACKOFF).await;
                    }
                },
            }
        }
        panic!(
            "failed to connect to {} backend after {CONNECT_ATTEMPTS} attempts: {}\n--- docker logs {} (last 200 lines) ---\n{}",
            self.endpoint,
            last_err.expect("loop records an error before exiting"),
            self.container_id,
            self.container_logs()
        );
    }

    /// `docker logs` for this container, captured here rather than from CI
    /// afterward: the atexit hook below removes the container as soon as
    /// the test binary exits, before any CI-level `docker logs` step runs.
    fn container_logs(&self) -> String {
        match std::process::Command::new("docker")
            .args(["logs", "--tail", "200", &self.container_id])
            .output()
        {
            Ok(output) => {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
            },
            Err(e) => format!("(failed to run `docker logs`: {e})"),
        }
    }
}

type BackendCell = Arc<OnceCell<Arc<BackendHandle>>>;

/// Pool key includes the broker network — a gRPC client container is
/// pinned to one Docker network at start time, so tests using
/// different `ClusterConfig`s (which spawn brokers on different
/// networks) need their own backend container.
static BACKEND_POOL: std::sync::LazyLock<Mutex<HashMap<(BackendKind, String), BackendCell>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

static CLEANUP_REGISTERED: Once = Once::new();

/// Get or start the requested backend, returning a shared handle.
///
/// Pool entries are keyed by `(kind, broker_network)`. First caller for
/// a given pair triggers a `docker run`; subsequent callers share the
/// same `BackendHandle`. The container is attached to `broker_network`
/// so it can reach the Kafka broker via the CONTAINER listener
/// (`<broker_container_name>:9099`). The container is removed at
/// process exit by the atexit hook below.
///
/// Caller (the `multilanguage_test!` macro) must ensure the broker
/// network exists — typically by creating the `TestContext` first.
pub async fn get_or_start(kind: BackendKind, broker_network: &str) -> Arc<BackendHandle> {
    register_cleanup_hook();

    let cell = {
        let mut pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
        pool.entry((kind, broker_network.to_string()))
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };

    let network = broker_network.to_string();
    cell.get_or_init(|| async move { Arc::new(start_container(kind, network).await) })
        .await
        .clone()
}

async fn start_container(kind: BackendKind, broker_network: String) -> BackendHandle {
    let internal_port: u16 = kind.internal_port();
    let internal: ContainerPort = internal_port.tcp();

    let image = GenericImage::new(kind.image_repository(), "dev")
        .with_exposed_port(internal)
        .with_wait_for(WaitFor::message_on_stderr("listening"))
        // Join the broker's user-defined bridge network so the gRPC
        // server inside this container can reach the broker by its
        // container hostname.
        .with_network(broker_network.clone());

    let container = image.start().await.unwrap_or_else(|e| {
        panic!(
            "failed to start {} gRPC backend container (image {}:dev) on network {}: {e}\n\
             Build it with `make build-grpc-images`.",
            kind.label(),
            kind.image_repository(),
            broker_network,
        )
    });

    let host_port = match container.get_host_port_ipv4(internal).await {
        Ok(port) => port,
        Err(e) => panic!(
            "failed to read mapped host port for {} backend: {e}\n{}",
            kind.label(),
            describe_container_state(container.id())
        ),
    };
    let container_id = container.id().to_string();
    let endpoint = format!("http://127.0.0.1:{host_port}");

    BackendHandle { _container: container, container_id, endpoint }
}

/// Docker's view of `id` — status, exit code, OOM flag, port bindings and the
/// tail of its logs — for the panic message when the port lookup fails.
///
/// # Why this is worth the code
///
/// `get_host_port_ipv4` resolves through a **live** `docker inspect`
/// (`testcontainers-0.27.3/src/core/client.rs:182-193` → `network_settings.ports`)
/// and reports `PortNotExposed` whenever that map has no **IPv4** binding for
/// the port. Two very different things produce that: the container is no longer
/// running (a stopped container has no port bindings at all), or it is running
/// with a non-IPv4 binding only. The bare error distinguishes neither, which is
/// why an observed instance of this failure could not be root-caused.
///
/// Note the wait strategy does not rule out the first case:
/// `WaitFor::message_on_stderr("listening")` is satisfied by the line appearing
/// in the log stream, so a server that logs "listening" and then dies still
/// gets past `start()`.
///
/// Diagnostics only — deliberately no fallback and no retry. Falling back to the
/// IPv6 mapping would be wrong: the endpoint built below is `127.0.0.1`, so an
/// IPv6-only publish cannot serve this caller and would merely move the failure
/// to a confusing connect error. Retrying would be wrong for a container that
/// has exited, which is a crash to report rather than to wait out.
fn describe_container_state(id: &str) -> String {
    let inspect = std::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "status={{.State.Status}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} \
             error={{.State.Error}} ports={{json .NetworkSettings.Ports}}",
            id,
        ])
        .output();
    let state = match inspect {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        Ok(out) => format!("<docker inspect failed: {}>", String::from_utf8_lossy(&out.stderr).trim()),
        Err(e) => format!("<could not run docker inspect: {e}>"),
    };

    let logs = std::process::Command::new("docker").args(["logs", "--tail", "20", id]).output();
    let logs = match logs {
        Ok(out) => {
            let mut combined = String::from_utf8_lossy(&out.stdout).to_string();
            combined.push_str(&String::from_utf8_lossy(&out.stderr));
            let trimmed = combined.trim().to_string();
            if trimmed.is_empty() {
                "<no output>".to_string()
            } else {
                trimmed
            }
        },
        Err(e) => format!("<could not run docker logs: {e}>"),
    };

    format!("  container {id}\n  {state}\n  last 20 log lines:\n{logs}")
}

/// `docker rm -f` one backend container. The single shell-out used by every
/// teardown path here, so there is exactly one of them.
fn remove_container(handle: &BackendHandle) {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", handle.container_id()])
        .output();
}

/// True while a test still holds — or is in the middle of starting — a backend
/// container attached to `broker_network`.
///
/// Read by [`cluster_pool`](super::cluster_pool) before evicting a cluster: a
/// backend container is a *child* of the network its cluster owns, so evicting
/// the cluster destroys the network out from under it.
///
/// Both halves matter, and they mirror the two idle checks `cluster_pool`
/// already applies to its own entries:
///   - `Arc::strong_count(cell) > 1` — a task is inside [`get_or_start`] for
///     this key: it cloned the cell, released the pool lock, and has not
///     finished `get_or_init` yet. Removing the entry now would leave that task
///     to start a container nobody can reach or reap.
///   - `Arc::strong_count(handle) > 1` — a test is holding the handle (the
///     `multilanguage_admin_test!` arms keep it live for the whole body), so
///     the container is in use.
///
/// This cannot block eviction indefinitely: a test that holds a backend handle
/// also holds the `TestContext` that owns the cluster, which already pins the
/// cluster's own `Arc` above the idle threshold. The check makes that invariant
/// locally verifiable instead of resting on an argument about macro expansion.
pub fn has_live_handles_on_network(broker_network: &str) -> bool {
    let pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
    pool.iter().any(|((_, network), cell)| {
        network == broker_network
            && (Arc::strong_count(cell) > 1 || cell.get().is_some_and(|handle| Arc::strong_count(handle) > 1))
    })
}

/// De-pools and removes every backend container attached to `broker_network`,
/// returning how many were removed.
///
/// Called by [`cluster_pool`](super::cluster_pool) when it evicts the cluster
/// that owns `broker_network`. Without this the eviction's `docker network rm`
/// fails — Docker refuses to remove a network with active endpoints — so the
/// network leaks for the rest of the process (measured: 8 orphaned
/// `kafka-net-*` networks after a full run) and the backend container stays
/// resident pointing at brokers that no longer exist.
///
/// # Must be called from a blocking thread
///
/// Dropping the last `Arc<BackendHandle>` drops a `ContainerAsync`, whose
/// `Drop` wants a runtime handle — the same constraint that puts
/// `cluster_pool`'s teardown inside `spawn_blocking`. The entry is removed from
/// the pool here, so this call *is* the last reference.
pub fn take_and_remove_handles_on_network(broker_network: &str) -> usize {
    let removed: Vec<Arc<BackendHandle>> = {
        let mut pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
        let keys: Vec<(BackendKind, String)> =
            pool.keys().filter(|(_, network)| network == broker_network).cloned().collect();
        keys.into_iter()
            .filter_map(|key| pool.remove(&key))
            .filter_map(|cell| cell.get().cloned())
            .collect()
    };

    for handle in &removed {
        remove_container(handle);
    }
    removed.len()
}

/// `docker rm -f` every backend container in the pool, **leaving the pool
/// entries in place**.
///
/// Deliberately does not de-pool: keeping the pool's `Arc` alive means no
/// `ContainerAsync::drop` runs, which is what makes this safe to call from an
/// `atexit` handler where there is no tokio runtime to service that `Drop`.
/// Idempotent, so calling it from both `atexit` hooks is harmless.
pub fn force_remove_all_containers() {
    let pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
    let handles: Vec<_> = pool.values().filter_map(|cell| cell.get().cloned()).collect();
    drop(pool);

    for handle in &handles {
        remove_container(handle);
    }
}

/// Forcibly `docker rm -f` every backend container in the pool when the
/// process exits. The same trick `cluster_pool` uses for the broker
/// containers — `LazyLock` statics never run their `Drop`.
fn register_cleanup_hook() {
    CLEANUP_REGISTERED.call_once(|| {
        unsafe extern "C" {
            safe fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
        }

        extern "C" fn cleanup_backend_containers() {
            force_remove_all_containers();
        }

        atexit(cleanup_backend_containers);
    });
}
