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

//! Process-global pool of gRPC backends used by the multilanguage
//! integration tests.
//!
//! Mirrors the existing [`cluster_pool`](super::cluster_pool) pattern:
//! the first test that needs the python or c backend starts it; subsequent
//! tests share it via a `OnceCell`. An `atexit` hook forcibly stops the
//! backends on process exit because Rust does not run destructors for
//! `LazyLock` statics.
//!
//! # Backend modes
//!
//! [`BackendMode`] selects how a backend runs, via `MULTILANG_BACKEND_MODE`:
//!
//! - **`container`** (default on Linux) — a Docker container attached to the
//!   broker's network, exposing a fixed internal port (50051 python / 50052 c)
//!   that testcontainers maps to a **random** host port. The server reaches the
//!   broker through its CONTAINER-family listener.
//! - **`native`** (default on all other platforms, e.g. macOS) — the same gRPC
//!   server run as a child process on the host, bound to an ephemeral port on
//!   `127.0.0.1` that it reports on startup. The server connects to the broker
//!   through the host-loopback listener, as the `__rust` arm does. This mode
//!   tests the host platform's build of the bindings: containers on macOS run Linux, so container mode
//!   can only test the Linux build and cannot load the host's Mach-O artifacts.
//!
//! In both modes each backend listens on its own port, so parallel test runs
//! do not collide.
//!
//! See `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md` for
//! the full architecture.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, LazyLock, Mutex, Once, mpsc};
use std::time::Duration;

use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

/// Retries for [`BackendHandle::channel`]'s connect: the shared backend is
/// already up, so a failure is almost always a transient Colima hiccup.
const CONNECT_ATTEMPTS: u32 = 5;
const CONNECT_RETRY_BACKOFF: Duration = Duration::from_millis(300);

/// How long a native backend may take to print its "listening" line. Covers the
/// Python interpreter importing grpcio and the extension module on a cold start.
const NATIVE_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Lines of a native backend's output kept for failure messages, matching the
/// `docker logs --tail 200` the container mode reports.
const NATIVE_OUTPUT_LINES: usize = 200;

/// How the gRPC backends run. See the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendMode {
    /// A Docker container on the broker's network.
    Container,
    /// A child process on the host.
    Native,
}

/// `MULTILANG_BACKEND_MODE=container|native`; unset means `container` on Linux
/// and `native` elsewhere. Read once, so every test in the process agrees.
static BACKEND_MODE: LazyLock<BackendMode> =
    LazyLock::new(|| match std::env::var("MULTILANG_BACKEND_MODE").as_deref() {
        Ok("container") => BackendMode::Container,
        Ok("native") => BackendMode::Native,
        Ok(other) => panic!("MULTILANG_BACKEND_MODE must be `container` or `native`, got `{other}`"),
        Err(_) if cfg!(target_os = "linux") => BackendMode::Container,
        Err(_) => BackendMode::Native,
    });

/// The mode every gRPC backend in this process runs in.
pub fn backend_mode() -> BackendMode {
    *BACKEND_MODE
}

/// Whether the gRPC backends run in containers, and therefore need the
/// broker's container-internal bootstrap addresses. The gRPC factories'
/// `needs_container_bootstrap()` return this.
pub fn uses_containers() -> bool {
    backend_mode() == BackendMode::Container
}

/// The two non-native backends. The native Rust backend doesn't need a
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

    /// The command that runs this backend's server natively, and the make
    /// target that builds what it needs.
    ///
    /// Python runs the checked-in server script with the venv interpreter
    /// (`MULTILANG_PYTHON` overrides it); the generated gRPC stubs live under
    /// `target/grpc-native/python` and are put on `PYTHONPATH`. C runs the
    /// server binary built by CMake (`MULTILANG_C_GRPC_SERVER` overrides it).
    fn native_command(self) -> (Command, &'static str) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        match self {
            BackendKind::Python | BackendKind::PythonAsync => {
                let python = std::env::var_os("MULTILANG_PYTHON")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| root.join("venv/bin/python"));
                let script = match self {
                    BackendKind::Python => "grpc_server.py",
                    _ => "grpc_server_async.py",
                };
                let stubs = root.join("target/grpc-native/python");
                require_native_artifact(self, &python, "build-grpc-native-python");
                require_native_artifact(self, &stubs.join("admin_service_pb2.py"), "build-grpc-native-python");

                let mut python_path = std::ffi::OsString::from(stubs);
                if let Some(existing) = std::env::var_os("PYTHONPATH") {
                    python_path.push(":");
                    python_path.push(existing);
                }
                let mut command = Command::new(python);
                command
                    .arg(root.join("bindings/python").join(script))
                    .env("PYTHONPATH", python_path);
                (command, "build-grpc-native-python")
            },
            BackendKind::C => {
                let server = std::env::var_os("MULTILANG_C_GRPC_SERVER")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| root.join("target/grpc-native/c/kafka_grpc_server"));
                require_native_artifact(self, &server, "build-grpc-native-c");
                (Command::new(server), "build-grpc-native-c")
            },
        }
    }
}

/// Panics with a build hint when a native backend's artifact is missing, rather
/// than letting the spawn fail with an unexplained "No such file or directory".
///
/// A bare command name with no path separator (e.g. `MULTILANG_PYTHON=python3`)
/// is not checked: [`Command`] resolves it through `PATH`, and a missing
/// command still fails at spawn with the build hint.
fn require_native_artifact(kind: BackendKind, path: &Path, make_target: &str) {
    let is_bare_command = path.components().count() == 1 && !path.is_absolute();
    assert!(
        is_bare_command || path.exists(),
        "{} native gRPC backend: {} does not exist. Build it with `make {make_target}`.",
        kind.label(),
        path.display(),
    );
}

/// Handle to a running gRPC backend. The backend stays alive while this handle
/// is held. Dropping it stops the backend (testcontainers' `ContainerAsync`
/// removes the container; [`NativeProcess`] kills the process). Handles held by
/// the `LazyLock` pool are never dropped, so the atexit hook below stops those.
pub struct BackendHandle {
    runtime: BackendRuntime,
    /// `http://127.0.0.1:<port>` reachable from the test process.
    endpoint: String,
}

/// What a [`BackendHandle`] is running on, per [`BackendMode`].
enum BackendRuntime {
    Container {
        // Boxed to keep the enum small: `ContainerAsync` is roughly 870 bytes,
        // while the native variant is a few words.
        _container: Box<ContainerAsync<GenericImage>>,
        id: String,
    },
    Native(NativeProcess),
}

/// A backend server running as a child process of the test binary.
struct NativeProcess {
    child: Mutex<Child>,
    /// The last [`NATIVE_OUTPUT_LINES`] lines of the server's stdout and
    /// stderr, for failure messages (the native counterpart of `docker logs`).
    output: Arc<Mutex<VecDeque<String>>>,
}

impl NativeProcess {
    /// Kill the server and reap it. Idempotent: a second call finds it
    /// already exited.
    fn terminate(&self) {
        let mut child = self.child.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Exit status (if it has exited) plus the captured output.
    fn describe(&self) -> String {
        let status = match self.child.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).try_wait() {
            Ok(Some(status)) => format!("exited: {status}"),
            Ok(None) => "still running".to_string(),
            Err(e) => format!("<could not query process: {e}>"),
        };
        let output = self.output.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let lines: Vec<&str> = output.iter().map(String::as_str).collect();
        format!("process {status}\n{}", lines.join("\n"))
    }
}

impl Drop for NativeProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl BackendHandle {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
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
            "failed to connect to {} backend after {CONNECT_ATTEMPTS} attempts: {}\n{}",
            self.endpoint,
            last_err.expect("loop records an error before exiting"),
            self.logs()
        );
    }

    /// The backend's recent output. It is collected here because the atexit
    /// hook below stops the backend when the test binary exits, before any
    /// CI-level `docker logs` step could run.
    fn logs(&self) -> String {
        match &self.runtime {
            BackendRuntime::Container { id, .. } => {
                format!("--- docker logs {id} (last 200 lines) ---\n{}", container_logs(id))
            },
            BackendRuntime::Native(process) => {
                format!(
                    "--- native backend output (last {NATIVE_OUTPUT_LINES} lines) ---\n{}",
                    process.describe()
                )
            },
        }
    }

    /// Stop the backend: `docker rm -f` for a container, kill for a native
    /// process. Every teardown path in this module goes through this method.
    fn terminate(&self) {
        match &self.runtime {
            BackendRuntime::Container { id, .. } => {
                let _ = Command::new("docker").args(["rm", "-f", id]).output();
            },
            BackendRuntime::Native(process) => process.terminate(),
        }
    }
}

/// `docker logs --tail 200` for container `id`.
fn container_logs(id: &str) -> String {
    match Command::new("docker").args(["logs", "--tail", "200", id]).output() {
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

type BackendCell = Arc<OnceCell<Arc<BackendHandle>>>;

/// Pool key includes the broker network — a gRPC client container is
/// pinned to one Docker network at start time, so tests using
/// different `ClusterConfig`s (which spawn brokers on different
/// networks) need their own backend container. A native backend is not
/// attached to any network, but keeps the same key so it shares the
/// container's lifecycle: it is stopped when the cluster it served is evicted.
static BACKEND_POOL: std::sync::LazyLock<Mutex<HashMap<(BackendKind, String), BackendCell>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

static CLEANUP_REGISTERED: Once = Once::new();

/// Get or start the requested backend, returning a shared handle.
///
/// Pool entries are keyed by `(kind, broker_network)`. First caller for
/// a given pair starts the backend; subsequent callers share the same
/// `BackendHandle`.
///
/// In [`BackendMode::Container`] that is a `docker run` attached to
/// `broker_network`, so the server can reach the Kafka broker via the
/// protocol-matched CONTAINER listener advertised on the broker's container
/// hostname (`<broker_container_name>:9099` PLAINTEXT / `:9100` SSL / `:9101`
/// SASL_SSL, per `INTEGRATION_TEST_PROTOCOL`). In [`BackendMode::Native`] it is
/// a host process, and the tests hand it the host-loopback listener instead
/// (the gRPC factories' `needs_container_bootstrap()` follows the mode).
/// Either way the backend is stopped at process exit by the atexit hook below.
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
    cell.get_or_init(|| async move {
        let handle = match backend_mode() {
            BackendMode::Container => start_container(kind, network).await,
            // Waiting for the "listening" line blocks, so keep it off the
            // runtime's worker threads.
            BackendMode::Native => tokio::task::spawn_blocking(move || start_native(kind))
                .await
                .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic())),
        };
        Arc::new(handle)
    })
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
    let id = container.id().to_string();
    let endpoint = format!("http://127.0.0.1:{host_port}");

    BackendHandle {
        runtime: BackendRuntime::Container { _container: Box::new(container), id },
        endpoint,
    }
}

/// Start `kind`'s server as a child process on `127.0.0.1`, and wait for its
/// "listening" line — the same readiness signal the container mode waits for.
///
/// The server is started with `GRPC_PORT=0`, so the OS assigns a free port at
/// bind time and no other process can claim it first. The server reports the
/// port it bound as the last `:`-separated field of the "listening" line
/// (`... listening on 127.0.0.1:<port>`), which becomes the endpoint. Stdout
/// and stderr are drained for the lifetime of the process, so the server never
/// blocks on a full pipe.
fn start_native(kind: BackendKind) -> BackendHandle {
    let (mut command, make_target) = kind.native_command();
    let mut child = command
        .env("GRPC_HOST", "127.0.0.1")
        .env("GRPC_PORT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "failed to spawn {} native gRPC backend: {e}\nBuild it with `make {make_target}`.",
                kind.label()
            )
        });

    let output = Arc::new(Mutex::new(VecDeque::with_capacity(NATIVE_OUTPUT_LINES)));
    let (ready_tx, ready_rx) = mpsc::channel();
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    drain_output(stdout, Arc::clone(&output), None);
    drain_output(stderr, Arc::clone(&output), Some(ready_tx));

    let process = NativeProcess { child: Mutex::new(child), output };
    let listening_line = match ready_rx.recv_timeout(NATIVE_START_TIMEOUT) {
        Ok(line) => line,
        // Disconnected: stderr reached EOF before "listening", meaning the
        // server exited. Timeout: the server is still starting or has hung.
        // Both are treated as fatal.
        Err(e) => {
            let reason = match e {
                mpsc::RecvTimeoutError::Timeout => format!("did not start within {NATIVE_START_TIMEOUT:?}"),
                mpsc::RecvTimeoutError::Disconnected => "exited before it was listening".to_string(),
            };
            // Allow the drain threads time to capture the final output lines.
            std::thread::sleep(Duration::from_millis(200));
            let details = process.describe();
            process.terminate();
            panic!("{} native gRPC backend {reason}\n{details}", kind.label());
        },
    };
    let Some(port) = parse_listening_port(&listening_line) else {
        let details = process.describe();
        process.terminate();
        panic!(
            "{} native gRPC backend: no bound port in its listening line `{listening_line}` \
             (expected `listening on <host>:<port>`)\n{details}",
            kind.label()
        );
    };

    BackendHandle {
        runtime: BackendRuntime::Native(process),
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

/// The port a native server reports in its "listening" line: the last
/// `:`-separated field, which must be a non-zero `u16`.
fn parse_listening_port(line: &str) -> Option<u16> {
    let port: u16 = line.rsplit(':').next()?.trim().parse().ok()?;
    (port != 0).then_some(port)
}

/// Copy `stream`'s lines into `output`, keeping the last [`NATIVE_OUTPUT_LINES`],
/// until the stream reaches EOF or fails to read. Lines that are not valid UTF-8
/// are kept lossily rather than ending the drain, which would leave the server to
/// block on a full pipe. With `ready`, sends it the first line containing
/// "listening".
fn drain_output(
    stream: impl Read + Send + 'static,
    output: Arc<Mutex<VecDeque<String>>>,
    ready: Option<mpsc::Sender<String>>,
) {
    std::thread::spawn(move || {
        let mut ready = ready;
        let mut reader = BufReader::new(stream);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {},
            }
            let line = String::from_utf8_lossy(&buf).trim_end_matches(['\r', '\n']).to_string();
            if line.contains("listening")
                && let Some(ready) = ready.take()
            {
                let _ = ready.send(line.clone());
            }
            let mut output = output.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if output.len() == NATIVE_OUTPUT_LINES {
                output.pop_front();
            }
            output.push_back(line);
        }
    });
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
    let inspect = Command::new("docker")
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

    let logs = Command::new("docker").args(["logs", "--tail", "20", id]).output();
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

/// True while a test still holds — or is in the middle of starting — a backend
/// container attached to `broker_network`.
///
/// Read by [`cluster_pool`](super::cluster_pool) before evicting a cluster: a
/// backend container is a *child* of the network its cluster owns, so evicting
/// the cluster destroys the network out from under it. (A native backend is
/// not attached to the network, but it still serves that cluster's brokers.)
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

/// De-pools and stops every backend attached to `broker_network`, returning how
/// many were stopped.
///
/// Called by [`cluster_pool`](super::cluster_pool) when it evicts the cluster
/// that owns `broker_network`. Without this the eviction's `docker network rm`
/// fails — Docker refuses to remove a network with active endpoints — so the
/// network leaks for the rest of the process (measured: 8 orphaned
/// `kafka-net-*` networks after a full run) and the backend container stays
/// resident pointing at brokers that no longer exist. A native backend is not
/// attached to the network, but it is stopped as well; otherwise it would remain
/// idle until the process exits.
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
        handle.terminate();
    }
    removed.len()
}

/// Stop every backend in the pool (`docker rm -f` a container, kill a native
/// process), **leaving the pool entries in place**.
///
/// Deliberately does not de-pool: keeping the pool's `Arc` alive means no
/// `ContainerAsync::drop` runs, which is what makes this safe to call from an
/// `atexit` handler where there is no tokio runtime to service that `Drop`.
/// Idempotent, so calling it from both `atexit` hooks is harmless.
pub fn force_stop_all_backends() {
    let pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
    let handles: Vec<_> = pool.values().filter_map(|cell| cell.get().cloned()).collect();
    drop(pool);

    for handle in &handles {
        handle.terminate();
    }
}

/// Forcibly stop every backend in the pool when the process exits. The same trick
/// `cluster_pool` uses for the broker containers — `LazyLock` statics never run
/// their `Drop`.
fn register_cleanup_hook() {
    CLEANUP_REGISTERED.call_once(|| {
        unsafe extern "C" {
            safe fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
        }

        extern "C" fn cleanup_backends() {
            force_stop_all_backends();
        }

        atexit(cleanup_backends);
    });
}
