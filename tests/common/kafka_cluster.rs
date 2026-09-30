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

//! Kafka cluster container wrapper for integration tests.
//!
//! Wraps one or more testcontainers Kafka containers on a shared Docker
//! network. Each broker exposes all four security protocols: PLAINTEXT,
//! SSL, SASL_PLAINTEXT, and SASL_SSL.
//!
//! Created by [`super::cluster_pool`], shared across tests with the same
//! [`super::cluster_config::ClusterConfig`] — or, for a
//! [`ClusterConfig::dedicated`] config, started privately by
//! [`super::test_context::TestContext`] and removed when the test ends.
//!
//! # Topologies and broker lifecycle (Java `ClusterInstance`)
//!
//! [`KafkaCluster`] plays the role of Java's
//! `org.apache.kafka.common.test.ClusterInstance`
//! (`test-common-runtime/.../ClusterInstance.java`), with one container per
//! KRaft node instead of one in-JVM `BrokerServer`/`ControllerServer`:
//!
//! | Java `ClusterInstance`      | Rust `KafkaCluster`                  |
//! |-----------------------------|--------------------------------------|
//! | `type()`                    | [`KafkaCluster::cluster_type`]       |
//! | `brokerIds()`               | [`KafkaCluster::broker_ids`]         |
//! | `aliveBrokers().keySet()`   | [`KafkaCluster::alive_broker_ids`]   |
//! | `controllerIds()`           | [`KafkaCluster::controller_ids`]     |
//! | `brokerBoundPorts()`        | [`KafkaCluster::broker_bound_ports`] |
//! | `shutdownBroker(id)`        | [`KafkaCluster::shutdown_broker`]    |
//! | `startBroker(id)`           | [`KafkaCluster::start_broker`]       |
//! | `waitForReadyBrokers()`     | [`KafkaCluster::wait_for_ready_brokers`] |
//!
//! [`Type::CoKraft`] (Java `CO_KRAFT`, the pooled default) runs every node as
//! `broker,controller`. [`Type::Kraft`] (Java `KRAFT`, Java's default for
//! `@ClusterTest`) runs `controllers` controller-only containers plus
//! broker-only containers, with Java's `TestKitNodes` ids (brokers from 0,
//! controllers from 3000), so stopping brokers never touches the quorum.
//!
//! **Restart fidelity.** `shutdown_broker` / `start_broker` `docker stop` and
//! `docker start` the *same* held `ContainerAsync`, so a restarted broker keeps:
//!   - its node id and `advertised.listeners` (same environment);
//!   - its host ports — each advertised client port is bound by an
//!     in-process `BrokerProxy` forwarding to a pre-reserved backend host port
//!     pinned with `with_mapped_port` (which Docker re-binds on start); the
//!     proxy rebinds the advertised ports on start, so bootstrap strings handed
//!     out earlier stay valid. Stopping the proxy with the container closes
//!     every client connection and refuses new ones, as a shut-down in-JVM
//!     broker does (see [`KafkaCluster::shutdown_broker`]);
//!   - its data — the log directory lives in the container's writable layer,
//!     which survives a stop. The image's start-up script re-runs the storage
//!     format on every start but tolerates "already formatted", and
//!     [`CLUSTER_ID`] is fixed, so nothing is reformatted.
//!
//! **Deviations from Java.**
//!   - Lifecycle requires a [`ClusterConfig::dedicated`] [`Type::Kraft`]
//!     cluster. Java may stop the broker half of a `CO_KRAFT` node; a combined
//!     container cannot stop its broker without its controller. Pooled clusters
//!     are a Rust-only optimization and must never carry a stopped broker.
//!   - `start_broker` returns once the broker serves requests and is listed
//!     alive by `DescribeCluster` (Java's `startup()` returns once registered
//!     and unfenced), and `wait_for_ready_brokers` sends a `Metadata` request
//!     directly to each alive broker instead of reading metadata caches
//!     in-process, and expects the *alive* set rather than all brokers.
//!   - `CO_KRAFT` node ids stay 1-based (pre-existing; Java uses 0-based).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

use super::cluster_config::{ClusterConfig, Type};
use super::test_certs;

use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyToContainer, Image, ImageExt};

/// Kafka image tag to use for integration tests.
const KAFKA_TAG: &str = "4.2.0";

/// Container port for the PLAINTEXT listener.
const PLAINTEXT_PORT: ContainerPort = ContainerPort::Tcp(9092);
/// PLAINTEXT listener exposed only on the inter-broker network — used by
/// gRPC client containers in the multilanguage test harness so they can
/// reach the broker by container hostname rather than via the host loopback.
const CONTAINER_PORT_NUM: u16 = 9099;
/// Container port for the SASL_PLAINTEXT listener.
const SASL_PLAINTEXT_PORT: ContainerPort = ContainerPort::Tcp(9095);
/// Container port for the SSL listener.
const SSL_PORT: ContainerPort = ContainerPort::Tcp(9096);
/// Container port for the SASL_SSL listener.
const SASL_SSL_PORT: ContainerPort = ContainerPort::Tcp(9097);

/// Path for the JAAS config file inside the container.
const JAAS_CONFIG_PATH: &str = "/opt/kafka/config/kafka_server_jaas.conf";
/// Directory for SSL certificates inside the container (a Docker volume).
const SECRETS_DIR: &str = "/etc/kafka/secrets";
/// Keystore filename inside SECRETS_DIR.
const SSL_KEYSTORE_FILENAME: &str = "broker-keystore.pem";
/// Truststore filename inside SECRETS_DIR.
const SSL_TRUSTSTORE_FILENAME: &str = "broker-truststore.pem";

/// Default SASL credentials for integration tests.
pub const SASL_USERNAME: &str = "admin";
/// Default SASL password for integration tests.
pub const SASL_PASSWORD: &str = "admin-secret";

/// Static cluster ID for KRaft. All brokers in the same cluster share this.
const CLUSTER_ID: &str = "5L6g3nShT-eMCtK--X86sw";

/// Bootstrap attempts for a retryable failure (`is_retryable_start_error`):
/// a lost reserve-then-bind port race, a broker that died during startup, or
/// a transient Docker API failure. Each attempt draws fresh ports, so the
/// all-collide probability drops fast.
const MAX_START_ATTEMPTS: u32 = 6;

/// Pause before bootstrap attempt `attempt + 1`: 1 s, 2 s, 4 s, … capped at
/// [`MAX_START_RETRY_BACKOFF`].
fn start_retry_backoff(attempt: u32) -> std::time::Duration {
    let secs = 1u64 << attempt.saturating_sub(1).min(4);
    std::time::Duration::from_secs(secs).min(MAX_START_RETRY_BACKOFF)
}

/// Upper bound of [`start_retry_backoff`].
const MAX_START_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(8);

/// Deadline for a `docker` CLI call made while reaping a failed bootstrap
/// attempt, so a wedged daemon cannot hang cluster start forever.
const DOCKER_CLI_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Deadline for a broker container to report readiness. Comfortably above a
/// healthy KRaft quorum formation (seconds) but finite, so a container that
/// will never become ready fails the attempt instead of hanging the shared
/// [`super::cluster_pool`] `OnceCell` forever.
///
/// Kept short deliberately: a broker that cannot register with the quorum
/// spins here emitting ~10 log lines/second, and testcontainers retains all of
/// them for the error message (see [`truncate_container_error`]), so the
/// deadline bounds the failure log as well as the wait.
const CONTAINER_STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// Concurrent cluster bootstraps. Without a cap, libtest starts ~14 configs
/// in a burst and some brokers die under contention. macOS serializes to 1
/// (Colima's port layer makes the reserve-then-bind race far likelier);
/// Linux allows 2.
#[cfg(target_os = "macos")]
const MAX_CONCURRENT_BOOTSTRAPS: usize = 1;
#[cfg(not(target_os = "macos"))]
const MAX_CONCURRENT_BOOTSTRAPS: usize = 2;

static BOOTSTRAP_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_CONCURRENT_BOOTSTRAPS);

/// Host-mapped ports for one broker's client-facing listeners.
struct BrokerPorts {
    plaintext: u16,
    ssl: u16,
    sasl_plaintext: u16,
    sasl_ssl: u16,
}

impl BrokerPorts {
    /// Reserve four free host ports for a broker by binding to port 0.
    fn reserve() -> Self {
        Self {
            plaintext: find_available_port(),
            ssl: find_available_port(),
            sasl_plaintext: find_available_port(),
            sasl_ssl: find_available_port(),
        }
    }
}

/// `process.roles` of one KRaft node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessRoles {
    /// `broker,controller` — every node of a [`Type::CoKraft`] cluster.
    Combined,
    /// `broker` — a broker node of a [`Type::Kraft`] cluster.
    Broker,
    /// `controller` — a controller node of a [`Type::Kraft`] cluster.
    Controller,
}

impl ProcessRoles {
    fn as_str(self) -> &'static str {
        match self {
            Self::Combined => "broker,controller",
            Self::Broker => "broker",
            Self::Controller => "controller",
        }
    }

    fn is_broker(self) -> bool {
        self != Self::Controller
    }

    fn is_controller(self) -> bool {
        self != Self::Broker
    }
}

/// Everything needed to create one node's container.
struct NodeSpec {
    node_id: i32,
    roles: ProcessRoles,
    container_name: String,
    /// Host ports for the client listeners; `None` for a controller-only node.
    ports: Option<BrokerPorts>,
    /// Host ports Docker maps the client listeners to when a [`BrokerProxy`]
    /// fronts the broker (lifecycle-capable clusters only): `ports` are then
    /// bound by the proxy, which forwards to these.
    backend_ports: Option<BrokerPorts>,
}

/// Java `TestKitDefaults.BROKER_ID_OFFSET`: first broker id of a [`Type::Kraft`] cluster.
const BROKER_ID_OFFSET: i32 = 0;
/// Java `TestKitDefaults.CONTROLLER_ID_OFFSET`: first controller id of a [`Type::Kraft`] cluster.
const CONTROLLER_ID_OFFSET: i32 = 3000;

/// The nodes of a cluster of `config`'s shape, brokers first.
///
/// [`Type::CoKraft`] keeps the harness's historical 1-based combined ids;
/// [`Type::Kraft`] numbers like Java's `TestKitNodes` (brokers from
/// [`BROKER_ID_OFFSET`], controllers from [`CONTROLLER_ID_OFFSET`]).
fn node_specs(config: &ClusterConfig, suffix: &str) -> Vec<NodeSpec> {
    let brokers = i32::from(config.brokers);
    let proxied = supports_lifecycle(config);
    match config.cluster_type {
        Type::CoKraft => (1..=brokers)
            .map(|id| NodeSpec {
                node_id: id,
                roles: ProcessRoles::Combined,
                container_name: format!("kafka-{id}-{suffix}"),
                ports: Some(BrokerPorts::reserve()),
                backend_ports: None,
            })
            .collect(),
        Type::Kraft => {
            assert!(config.controllers >= 1, "A KRAFT cluster must have at least 1 controller");
            let broker_nodes = (BROKER_ID_OFFSET..BROKER_ID_OFFSET + brokers).map(|id| NodeSpec {
                node_id: id,
                roles: ProcessRoles::Broker,
                container_name: format!("kafka-broker-{id}-{suffix}"),
                ports: Some(BrokerPorts::reserve()),
                backend_ports: proxied.then(BrokerPorts::reserve),
            });
            let controller_nodes =
                (CONTROLLER_ID_OFFSET..CONTROLLER_ID_OFFSET + i32::from(config.controllers)).map(|id| NodeSpec {
                    node_id: id,
                    roles: ProcessRoles::Controller,
                    container_name: format!("kafka-controller-{id}-{suffix}"),
                    ports: None,
                    backend_ports: None,
                });
            broker_nodes.chain(controller_nodes).collect()
        },
    }
}

/// In-process TCP forwarder fronting one broker of a lifecycle-capable
/// cluster: it binds the broker's advertised host ports and forwards each
/// connection to the host ports Docker maps the container's listeners to.
///
/// It exists so [`KafkaCluster::shutdown_broker`] can give clients the view
/// Java's in-JVM brokers give once shut down — every connection closed, new
/// ones refused — which Docker's own host-port forwarder does not reliably do
/// around a container stop (see `shutdown_broker`). Stopping the proxy drops
/// its listeners and every forwarded connection; starting it rebinds the same
/// ports, so bootstrap strings handed out earlier stay valid.
struct BrokerProxy {
    /// `(advertised host port, Docker-mapped backend host port)` per listener.
    routes: Vec<(u16, u16)>,
    /// The running accept loops and connections; `None` while stopped.
    running: std::sync::Mutex<Option<ProxyRun>>,
}

/// One start..stop lifetime of a [`BrokerProxy`].
struct ProxyRun {
    cancel: tokio_util::sync::CancellationToken,
    /// Every accept loop and connection task holds a clone of the matching
    /// sender, so `recv()` yields `None` once all of them have exited.
    exited: tokio::sync::mpsc::Receiver<()>,
}

impl BrokerProxy {
    fn new(front: &BrokerPorts, backend: &BrokerPorts) -> Self {
        Self {
            routes: vec![
                (front.plaintext, backend.plaintext),
                (front.ssl, backend.ssl),
                (front.sasl_plaintext, backend.sasl_plaintext),
                (front.sasl_ssl, backend.sasl_ssl),
            ],
            running: std::sync::Mutex::new(None),
        }
    }

    /// Binds every advertised port and starts forwarding. A bind failure is
    /// reported with "address already in use" wording, so a cluster start that
    /// lost a port race is retried (`is_port_allocation_error`).
    async fn start(&self) -> Result<(), String> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let (alive, exited) = tokio::sync::mpsc::channel(1);
        for &(front, backend) in &self.routes {
            let listener = match tokio::net::TcpListener::bind(("127.0.0.1", front)).await {
                Ok(listener) => listener,
                Err(err) => {
                    // Nothing else holds `cancel` yet, so stop the routes already bound.
                    cancel.cancel();
                    return Err(format!(
                        "broker proxy failed to bind 127.0.0.1:{front} (address already in use?): {err}"
                    ));
                },
            };
            tokio::spawn(Self::accept_loop(listener, backend, cancel.clone(), alive.clone()));
        }
        let previous = self
            .running
            .lock()
            .expect("broker proxy state poisoned")
            .replace(ProxyRun { cancel, exited });
        assert!(previous.is_none(), "broker proxy started twice");
        Ok(())
    }

    /// Closes the listeners and every forwarded connection, returning once
    /// all of them are closed.
    async fn stop(&self) {
        let run = self.running.lock().expect("broker proxy state poisoned").take();
        if let Some(ProxyRun { cancel, mut exited }) = run {
            cancel.cancel();
            while exited.recv().await.is_some() {}
        }
    }

    async fn accept_loop(
        listener: tokio::net::TcpListener,
        backend: u16,
        cancel: tokio_util::sync::CancellationToken,
        alive: tokio::sync::mpsc::Sender<()>,
    ) {
        loop {
            let accepted = tokio::select! {
                _ = cancel.cancelled() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((mut client, _)) = accepted else {
                // Back off so a persistent accept error (e.g. fd exhaustion) cannot spin.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            let cancel = cancel.clone();
            let alive = alive.clone();
            tokio::spawn(async move {
                let _alive = alive;
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = async {
                        // A refused backend (container stopping) just closes
                        // the client connection, as a dead broker would.
                        if let Ok(mut server) = tokio::net::TcpStream::connect(("127.0.0.1", backend)).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    } => {},
                }
            });
        }
    }
}

/// Every port handed out by [`find_available_port`] in this process.
///
/// The OS-assigned-port trick has a TOCTOU window: the probe listener is
/// dropped (releasing the port) before Docker binds it, so the same port can
/// be handed to a second caller in between. With a single cluster that window
/// is negligible, but `tests/integration/main.rs` is one binary running ~14
/// distinct [`ClusterConfig`]s, and libtest bootstraps several concurrently —
/// four live clusters is 32 reservations, all drawn from the same ephemeral
/// range and all released before use. Collisions were observed in practice
/// (one broker stuck in `Created` with
/// `Bind for 0.0.0.0:<port> failed: port is already allocated`).
///
/// This registry closes the in-process half of the race: a port is never
/// handed out twice, even if the OS offers it again. Ports are never
/// returned — a cluster lives until the process exits, and a port that
/// failed to bind is one we specifically must not retry.
///
/// `BTreeSet` (not `HashSet`) so the `static` can be `const`-constructed.
static RESERVED_PORTS: std::sync::Mutex<std::collections::BTreeSet<u16>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// How many times [`find_available_port`] retries when the OS offers a port
/// this process has already reserved.
const PORT_PROBE_ATTEMPTS: u32 = 64;

/// Finds an available TCP port by letting the OS assign one, rejecting any
/// port already handed out in this process (see [`RESERVED_PORTS`]).
///
/// Binds `0.0.0.0`, not `127.0.0.1`: Docker publishes mapped ports on all
/// interfaces, so a loopback-only probe cannot see a conflict elsewhere.
///
/// The cross-process half of the race (another `cargo test`, or Docker's own
/// allocations) is not detectable here — [`KafkaCluster::start_with_config`]
/// handles that by retrying with fresh ports.
fn find_available_port() -> u16 {
    // Hold each rejected listener open for the whole loop so the OS cannot
    // offer the same port again on the next iteration.
    let mut rejected = Vec::new();
    for _ in 0..PORT_PROBE_ATTEMPTS {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").expect("Failed to bind to port 0");
        let port = listener.local_addr().expect("Failed to get local addr").port();
        let first_time = RESERVED_PORTS.lock().expect("reserved-port registry poisoned").insert(port);
        if first_time {
            return port;
        }
        rejected.push(listener);
    }
    panic!("Could not find an unreserved host port after {PORT_PROBE_ATTEMPTS} attempts");
}

/// Bytes kept from the head of a container-start error — enough for the error
/// kind (`container is not ready: failed to wait for container log: End of
/// stream reached before finding message: …`).
const CONTAINER_ERROR_HEAD_LEN: usize = 300;
/// Bytes kept from the tail of a container-start error — the log lines
/// immediately before the stream ended, which say why it ended.
const CONTAINER_ERROR_TAIL_LEN: usize = 1_700;

/// Maximum number of `ERROR`/`FATAL` lines rescued from the elided middle.
const MAX_RESCUED_ERROR_LINES: usize = 8;
/// Maximum length of a single rescued line.
const MAX_RESCUED_LINE_LEN: usize = 240;

/// Largest index `<= max` in `s` that is a UTF-8 char boundary.
fn floor_char_boundary(s: &str, max: usize) -> usize {
    let mut i = max.min(s.len());
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// Smallest index `>= min` in `s` that is a UTF-8 char boundary.
fn ceil_char_boundary(s: &str, min: usize) -> usize {
    let mut i = min.min(s.len());
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// Trims the middle out of a container-start error, rescuing the fatal lines.
///
/// testcontainers' `WaitLogError::EndOfStream` embeds **every** captured log
/// line in its `Display` ("Contains all the lines that were read for debugging
/// purposes" — `testcontainers::core::logs`). A broker that spins until
/// [`CONTAINER_STARTUP_TIMEOUT`] emits a `MetadataLoader` line every ~100 ms,
/// so the error reaches hundreds of kilobytes and buries the actual test
/// output.
///
/// A head+tail cut alone is not enough here. Kafka's startup-failure shape is
/// one fatal `ERROR` followed by hundreds of orderly-shutdown `INFO` lines, so
/// the signal sits *before* the noise: the retained tail shows only the clean
/// shutdown, and the cause ends up in the elided middle. Observed for real —
/// `ERROR [BrokerLifecycleManager] Shutting down because we were unable to
/// register with the controller quorum` was dropped while the surrounding
/// `closed event queue` chatter was kept.
///
/// So: keep head and tail, and additionally rescue up to
/// [`MAX_RESCUED_ERROR_LINES`] distinct `ERROR`/`FATAL` lines from between
/// them.
fn truncate_container_error(err: &str) -> String {
    if err.len() <= CONTAINER_ERROR_HEAD_LEN + CONTAINER_ERROR_TAIL_LEN {
        return err.to_string();
    }
    let head_end = floor_char_boundary(err, CONTAINER_ERROR_HEAD_LEN);
    let tail_start = ceil_char_boundary(err, err.len() - CONTAINER_ERROR_TAIL_LEN);
    let middle = &err[head_end..tail_start];

    // testcontainers `Debug`-formats the captured log, so entries are
    // separated by the two-character escape `\n`, not by real newlines.
    let mut rescued: Vec<&str> = Vec::new();
    for line in middle.split("\\n") {
        if !line.contains("ERROR") && !line.contains("FATAL") {
            continue;
        }
        let line = line.trim_start_matches(['"', ',', ' ']);
        let line = &line[..floor_char_boundary(line, MAX_RESCUED_LINE_LEN)];
        if !line.is_empty() && !rescued.contains(&line) {
            rescued.push(line);
        }
        if rescued.len() == MAX_RESCUED_ERROR_LINES {
            break;
        }
    }

    let mut out = String::with_capacity(CONTAINER_ERROR_HEAD_LEN + CONTAINER_ERROR_TAIL_LEN + 1024);
    out.push_str(&err[..head_end]);
    let elided = middle.len();
    if rescued.is_empty() {
        let _ = write!(out, "\n  … [{elided} bytes of container log elided] …\n");
    } else {
        let _ = write!(
            out,
            "\n  … [{elided} bytes of container log elided; {} ERROR/FATAL line(s) retained] …\n",
            rescued.len()
        );
        for line in &rescued {
            let _ = writeln!(out, "  {line}");
        }
        out.push_str("  …\n");
    }
    out.push_str(&err[tail_start..]);
    out
}

/// Whether `config` builds a cluster that supports broker lifecycle
/// ([`KafkaCluster::shutdown_broker`] / [`KafkaCluster::start_broker`]).
fn supports_lifecycle(config: &ClusterConfig) -> bool {
    config.dedicated && config.cluster_type == Type::Kraft
}

/// `true` when a container start failed because the host port we reserved was
/// claimed by something else before Docker could bind it — a failure that a
/// retry with fresh ports can fix.
///
/// Docker reports this as `port is already allocated` on Linux, and as
/// `address already in use` on Colima (macOS CI).
fn is_port_allocation_error(err: &str) -> bool {
    err.contains("port is already allocated") || err.contains("address already in use")
}

/// A broker container that came up but exited during startup — most commonly
/// "unable to register with the controller quorum" on a loaded CI runner,
/// which surfaces as the wait-for-log hitting end of stream. Transient by
/// nature (a fresh container on the same ports normally succeeds), so it is
/// worth spending a bounded retry on, unlike the subnet-exhaustion class the
/// no-retry rule below protects against.
fn is_transient_broker_startup_error(err: &str) -> bool {
    err.contains("End of stream reached before finding message")
        || err.contains("unable to register with the controller quorum")
}

/// Docker's default address pool is exhausted. Never retried: every attempt
/// creates a fresh network, so a retry only burns more subnets (see
/// [`KafkaCluster::start_with_config`]).
fn is_address_pool_exhausted_error(err: &str) -> bool {
    err.contains("all predefined address pools have been fully subnetted")
}

/// The Docker daemon or its API failed transiently — the request never got a
/// meaningful answer, so the same request on a fresh attempt normally
/// succeeds. Seen on Docker Desktop for macOS when the VM is busy, e.g.
/// `failed to list networks: Timeout error` while testcontainers checks
/// whether the attempt's network exists.
///
/// Matches the wording of the error types that reach us: bollard's
/// `RequestTimeoutError` ("Timeout error"), its transport errors
/// ("Error in the hyper legacy client: …", covering a daemon connection that
/// was refused or reset), a daemon socket that briefly vanished ("Socket not
/// found"), and any 5xx `DockerResponseServerError` ("Docker responded with
/// status code 5xx"). A 5xx also carries deterministic failures, notably the
/// address-pool exhaustion, which is excluded by [`is_retryable_start_error`].
fn is_transient_docker_api_error(err: &str) -> bool {
    const TRANSPORT_MARKERS: [&str; 7] = [
        "Timeout error",
        ": Timeout",
        "Error in the hyper legacy client",
        "Socket not found",
        "onnection refused",
        "onnection reset",
        "roken pipe",
    ];
    if TRANSPORT_MARKERS.iter().any(|marker| err.contains(marker)) {
        return true;
    }
    const STATUS_PREFIX: &str = "Docker responded with status code ";
    err.match_indices(STATUS_PREFIX).any(|(at, _)| {
        err[at + STATUS_PREFIX.len()..]
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|code| code.parse::<u16>().ok())
            .is_some_and(|code| (500..600).contains(&code))
    })
}

/// Whether a failed bootstrap attempt is worth another one: a lost port race,
/// a broker that died during startup, or a transient Docker API failure — but
/// never an exhausted address pool, whatever else the error says.
fn is_retryable_start_error(err: &str) -> bool {
    !is_address_pool_exhausted_error(err)
        && (is_port_allocation_error(err)
            || is_transient_broker_startup_error(err)
            || is_transient_docker_api_error(err))
}

/// Kafka image configured for one broker in a KRaft cluster.
///
/// Uses the standard `apache/kafka` Docker image entrypoint and its
/// `/etc/kafka/docker/run` startup flow. Configuration is done entirely
/// through environment variables — `KafkaDockerWrapper` converts every
/// `KAFKA_*` env var into a `server.properties` entry.
///
/// The Docker image's `/etc/kafka/docker/configure` script has an SSL
/// check that triggers when `KAFKA_ADVERTISED_LISTENERS` contains the
/// literal string `SSL://`. This check enforces Confluent-style
/// credential files that set keystore passwords, which Kafka rejects
/// for PEM format. We bypass it by using **custom listener names**
/// (`TLSONLY`, `SASLTLS`) that don't contain `SSL://`, and mapping
/// them to the real protocols via `listener.security.protocol.map`.
///
/// Uses the JVM-based `apache/kafka` image (not native) because SASL
/// requires the full JVM runtime.
#[derive(Debug, Clone)]
struct KafkaAllProtocols {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<CopyToContainer>,
}

impl KafkaAllProtocols {
    /// Create the image for one node in a cluster.
    ///
    /// - `node`: this node's id, `process.roles`, container name and — for any
    ///   node with the broker role — its pre-reserved host ports
    /// - `voters`: the `controller.quorum.voters` value shared by every node
    /// - `num_brokers`: number of broker-role nodes in the cluster
    /// - `certs`: shared SSL certificates (CA + broker cert with all broker
    ///   container hostnames in SANs)
    fn new(node: &NodeSpec, voters: &str, num_brokers: u16, certs: &test_certs::TestCertificates) -> Self {
        let mut env_vars = HashMap::new();

        // KRaft configuration
        env_vars.insert("CLUSTER_ID".into(), CLUSTER_ID.into());
        // Cap the JVM heap. The `apache/kafka` image defaults to roughly 1 GB
        // per broker, and the suite keeps around `TARGET_LIVE_CLUSTERS`
        // clusters resident at once — a target, not a ceiling, which is why
        // this per-broker bound matters: it is the only *hard* limit on total
        // residency. Measured on a 15 GB host: 24 brokers held 7.1 GiB
        // and drove free memory to ~1.2 GB, at which point brokers could not
        // answer each other's Raft vote requests in time and self-terminated
        // with "unable to register with the controller quorum".
        //
        // Integration-test clusters carry a handful of small topics, so 512 MB
        // is ample. `-Xms256m` keeps the initial commit low so idle brokers
        // cost less than active ones.
        env_vars.insert("KAFKA_HEAP_OPTS".into(), "-Xmx512m -Xms256m".into());
        env_vars.insert("KAFKA_NODE_ID".into(), node.node_id.to_string());
        env_vars.insert("KAFKA_PROCESS_ROLES".into(), node.roles.as_str().into());
        env_vars.insert("KAFKA_CONTROLLER_LISTENER_NAMES".into(), "CONTROLLER".into());
        env_vars.insert("KAFKA_CONTROLLER_QUORUM_VOTERS".into(), voters.into());

        let Some(ports) = node.ports.as_ref() else {
            // Controller-only node (Java `Type.KRAFT`): just the CONTROLLER
            // listener. The image's `configure` script rejects
            // `KAFKA_ADVERTISED_LISTENERS` on a controller, so none is set.
            env_vars.insert("KAFKA_LISTENERS".into(), "CONTROLLER://0.0.0.0:9094".into());
            env_vars.insert("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".into(), "CONTROLLER:PLAINTEXT".into());
            return Self { env_vars, copy_to_sources: Vec::new() };
        };
        let this_container = &node.container_name;
        env_vars.insert("KAFKA_INTER_BROKER_LISTENER_NAME".into(), "BROKER".into());
        env_vars.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".into(), num_brokers.min(3).to_string());
        // Disable time-based retention. Tests produce records with timestamp 0
        // (1970), as Java's do, so under the default 7-day `log.retention.ms`
        // every such segment is already expired. The broker's retention check
        // runs 30 s after startup and then every 300 s; Java's per-test fresh
        // clusters finish before the first check, but pooled clusters here live
        // for minutes, so the check deleted records mid-test (e.g.
        // `ListOffsets EARLIEST` returned 50 in `test_async_consumer_seek`).
        // Set on every broker-role node, combined nodes included; a
        // `server_properties` override from a `ClusterConfig` still wins.
        env_vars.insert("KAFKA_LOG_RETENTION_MS".into(), "-1".into());

        // Listeners — custom names avoid the configure script's SSL check.
        //
        // The check triggers on "SSL://" in KAFKA_ADVERTISED_LISTENERS.
        // Using TLSONLY (for SSL) and SASLTLS (for SASL_SSL) avoids the
        // substring match. listener.security.protocol.map maps them to
        // the actual protocols.
        // A broker-only node (Java `Type.KRAFT`) has no CONTROLLER listener; it
        // still maps the CONTROLLER listener name below because
        // `controller.listener.names` is how it reaches the quorum.
        let controller_listener = if node.roles == ProcessRoles::Combined {
            ",CONTROLLER://0.0.0.0:9094"
        } else {
            ""
        };
        env_vars.insert(
            "KAFKA_LISTENERS".into(),
            format!(
                "PLAINTEXT://0.0.0.0:9092,TLSONLY://0.0.0.0:9096,SASLPLAIN://0.0.0.0:9095,\
                 SASLTLS://0.0.0.0:9097,BROKER://0.0.0.0:9093{controller_listener},\
                 CONTAINER://0.0.0.0:{CONTAINER_PORT_NUM}"
            ),
        );

        // Advertised listeners use pre-reserved host ports for client
        // listeners (so metadata responses contain correct reachable
        // addresses) and the Docker network container name for the
        // inter-broker listener and the CONTAINER listener used by the
        // multilanguage gRPC client containers.
        env_vars.insert(
            "KAFKA_ADVERTISED_LISTENERS".into(),
            format!(
                "PLAINTEXT://127.0.0.1:{},TLSONLY://127.0.0.1:{},SASLPLAIN://127.0.0.1:{},\
                 SASLTLS://127.0.0.1:{},BROKER://{}:9093,\
                 CONTAINER://{}:{CONTAINER_PORT_NUM}",
                ports.plaintext, ports.ssl, ports.sasl_plaintext, ports.sasl_ssl, this_container, this_container
            ),
        );
        env_vars.insert(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".into(),
            "PLAINTEXT:PLAINTEXT,TLSONLY:SSL,SASLPLAIN:SASL_PLAINTEXT,\
             SASLTLS:SASL_SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,CONTAINER:PLAINTEXT"
                .into(),
        );

        // SSL configuration (PEM format)
        env_vars.insert("KAFKA_SSL_KEYSTORE_TYPE".into(), "PEM".into());
        env_vars.insert(
            "KAFKA_SSL_KEYSTORE_LOCATION".into(),
            format!("{SECRETS_DIR}/{SSL_KEYSTORE_FILENAME}"),
        );
        env_vars.insert("KAFKA_SSL_TRUSTSTORE_TYPE".into(), "PEM".into());
        env_vars.insert(
            "KAFKA_SSL_TRUSTSTORE_LOCATION".into(),
            format!("{SECRETS_DIR}/{SSL_TRUSTSTORE_FILENAME}"),
        );

        // SASL configuration
        env_vars.insert("KAFKA_SASL_ENABLED_MECHANISMS".into(), "PLAIN".into());
        env_vars.insert(
            "KAFKA_OPTS".into(),
            format!("-Djava.security.auth.login.config={JAAS_CONFIG_PATH}"),
        );

        // Files to mount into the container
        let mut copy_to_sources = Vec::new();

        // JAAS config for SASL PLAIN
        let jaas_content = format!(
            "KafkaServer {{\n    \
             org.apache.kafka.common.security.plain.PlainLoginModule required\n    \
             username=\"{SASL_USERNAME}\"\n    \
             password=\"{SASL_PASSWORD}\"\n    \
             user_{SASL_USERNAME}=\"{SASL_PASSWORD}\";\n\
             }};\n"
        );
        copy_to_sources.push(CopyToContainer::new(jaas_content.into_bytes(), JAAS_CONFIG_PATH));

        // SSL: broker keystore (private key + certificate chain)
        let keystore_pem = format!("{}{}", certs.broker_key_pem, certs.broker_cert_pem);
        copy_to_sources.push(CopyToContainer::new(
            keystore_pem.into_bytes(),
            format!("{SECRETS_DIR}/{SSL_KEYSTORE_FILENAME}"),
        ));

        // SSL: truststore (CA certificate)
        copy_to_sources.push(CopyToContainer::new(
            certs.ca_cert_pem.clone().into_bytes(),
            format!("{SECRETS_DIR}/{SSL_TRUSTSTORE_FILENAME}"),
        ));

        Self { env_vars, copy_to_sources }
    }
}

impl Image for KafkaAllProtocols {
    fn name(&self) -> &str {
        // JVM image, not native — SASL requires full JVM runtime
        "apache/kafka"
    }

    fn tag(&self) -> &str {
        KAFKA_TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Kafka Server started")]
    }

    fn env_vars(&self) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        // No random-mapped ports — we use with_mapped_port for fixed bindings
        &[]
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        self.copy_to_sources.iter()
    }
}

/// Manages a real Kafka cluster in Docker for integration tests.
///
/// One container per KRaft node — combined broker+controller nodes
/// ([`Type::CoKraft`]) or separate broker and controller nodes
/// ([`Type::Kraft`]) — connected via a shared Docker network. All brokers expose all four
/// security protocols. Tests choose which listener to connect to via
/// the protocol-specific bootstrap server accessors.
///
/// Shared across tests with the same [`ClusterConfig`].
pub struct KafkaCluster {
    /// The running container handles keyed by node id, kept alive for the
    /// lifetime of the cluster. Held (rather than only their ids) so
    /// [`Self::shutdown_broker`] / [`Self::start_broker`] can stop and restart
    /// the very same container.
    containers: BTreeMap<i32, ContainerAsync<KafkaAllProtocols>>,
    /// Ids of the nodes with the broker role — Java `ClusterInstance.brokerIds()`.
    broker_ids: BTreeSet<i32>,
    /// Ids of the nodes with the controller role — Java `ClusterInstance.controllerIds()`.
    controller_ids: BTreeSet<i32>,
    /// PLAINTEXT host port per broker id — Java `ClusterInstance.brokerBoundPorts()`.
    broker_ports: BTreeMap<i32, u16>,
    /// The [`BrokerProxy`] fronting each broker of a lifecycle-capable cluster.
    proxies: BTreeMap<i32, BrokerProxy>,
    /// Brokers stopped by [`Self::shutdown_broker`] and not yet restarted — the
    /// complement of Java's `aliveBrokers()` (`KafkaBroker.isShutdown()`).
    shutdown_brokers: std::sync::Mutex<BTreeSet<i32>>,
    /// Docker container IDs for all nodes (brokers first), used for cleanup.
    container_ids: Vec<String>,
    /// Docker network name, used for cleanup.
    network_name: String,
    /// Comma-separated `host:port` pairs for the PLAINTEXT listener.
    bootstrap_servers: String,
    /// Comma-separated `host:port` pairs for the SSL listener.
    ssl_bootstrap_servers: String,
    /// Comma-separated `host:port` pairs for the SASL_PLAINTEXT listener.
    sasl_plaintext_bootstrap_servers: String,
    /// Comma-separated `host:port` pairs for the SASL_SSL listener.
    sasl_ssl_bootstrap_servers: String,
    /// Comma-separated `container_name:port` pairs for the CONTAINER
    /// listener, reachable from sibling containers on the broker's
    /// Docker network. Used by the multilanguage test harness.
    container_bootstrap_servers: String,
    /// CA certificate PEM for SSL tests.
    ca_cert_pem: String,
    /// The config this cluster was started with.
    config: ClusterConfig,
}

impl KafkaCluster {
    /// Start a cluster matching the given config.
    ///
    /// Starts `config.brokers` containers on a shared Docker network,
    /// each with pre-reserved host ports bound via `with_mapped_port`.
    /// Advertised listeners are set to the known host ports from the
    /// start, so metadata responses contain correct reachable addresses.
    ///
    /// For multi-broker clusters, all containers start concurrently so
    /// the KRaft quorum can form (requires a majority of voters to be up).
    ///
    /// Called by [`super::cluster_pool`], not by tests directly.
    ///
    /// A bootstrap attempt that fails retryably (`is_retryable_start_error`:
    /// a reserved host port taken before Docker's bind, a broker that died
    /// during startup, or a transient Docker API failure) is retried with
    /// fresh ports after a short backoff ([`MAX_START_ATTEMPTS`]). Every failed
    /// attempt removes its containers and network before returning.
    ///
    /// # Panics
    ///
    /// Panics if the cluster cannot be started within [`MAX_START_ATTEMPTS`]
    /// attempts, or if ports cannot be retrieved.
    pub async fn start_with_config(config: &ClusterConfig) -> Self {
        let mut last_error = String::new();
        for attempt in 1..=MAX_START_ATTEMPTS {
            match Self::try_start_with_config(config).await {
                Ok(cluster) => return cluster,
                Err(err) => {
                    // Only a lost port race, a transient broker-startup
                    // failure or a transient Docker API failure is worth
                    // retrying. Every attempt also creates a fresh Docker
                    // network, and Docker's default address pool holds only
                    // ~30 of them — retrying an unrelated failure (notably
                    // "all predefined address pools have been fully
                    // subnetted") burns more subnets and makes the real
                    // problem worse.
                    if !is_retryable_start_error(&err) {
                        panic!("Failed to start Kafka cluster (not retryable) on attempt {attempt}: {err}");
                    }
                    // The failed attempt already removed its containers and
                    // network (`try_start_with_config`); the next attempt
                    // reserves a disjoint set of ports (the failed ones stay in
                    // `RESERVED_PORTS`).
                    eprintln!("WARN: Kafka cluster start attempt {attempt}/{MAX_START_ATTEMPTS} failed: {err}");
                    last_error = err;
                    if attempt < MAX_START_ATTEMPTS {
                        // Give a busy Docker daemon room to recover before the
                        // next attempt hits it with a burst of API calls.
                        tokio::time::sleep(start_retry_backoff(attempt)).await;
                    }
                },
            }
        }
        panic!("Failed to start Kafka cluster after {MAX_START_ATTEMPTS} attempts. Last error: {last_error}");
    }

    /// One bootstrap attempt. Returns `Err` instead of panicking so
    /// [`Self::start_with_config`] can retry with fresh ports.
    async fn try_start_with_config(config: &ClusterConfig) -> Result<Self, String> {
        // Cap concurrent bootstraps (see `BOOTSTRAP_PERMITS`). Held for the
        // whole attempt — including the readiness wait — so the brokers of at
        // most `BOOTSTRAP_PERMITS` clusters are ever starting at once.
        let _permit = BOOTSTRAP_PERMITS
            .acquire()
            .await
            .map_err(|err| format!("Cluster bootstrap semaphore closed: {err}"))?;

        assert!(config.brokers >= 1, "Cluster must have at least 1 broker");

        // Generate unique names to avoid collisions between concurrent test processes
        let suffix = random_suffix(8);

        let result = Self::start_attempt(config, &suffix).await;
        if result.is_err() {
            // Every container handle of the attempt has been dropped by now,
            // which removes the containers testcontainers got to start. It
            // does not remove one whose `create` succeeded but whose `start`
            // failed (e.g. a lost port race): that container stays in Docker's
            // `Created` state, still attached to the attempt's network, so the
            // network's own removal fails too. Reap both by name.
            reap_failed_attempt(&suffix).await;
        }
        result
    }

    /// Creates the attempt's network and nodes and waits for them. Every
    /// container and the network carry `suffix` in their names, which is how
    /// [`reap_failed_attempt`] finds what a failed attempt left behind.
    async fn start_attempt(config: &ClusterConfig, suffix: &str) -> Result<Self, String> {
        let num_brokers = config.brokers;
        let network_name = attempt_network_name(suffix);

        // Reserve host ports for each broker before starting containers
        // (inside `node_specs`). This lets us set correct advertised.listeners
        // from the start, which is required because KRaft does not allow
        // dynamic updates to advertised.listeners.
        let nodes = node_specs(config, suffix);

        // Quorum voters: every node with the controller role.
        let voters: String = nodes
            .iter()
            .filter(|node| node.roles.is_controller())
            .map(|node| format!("{}@{}:9094", node.node_id, node.container_name))
            .collect::<Vec<_>>()
            .join(",");

        // Generate SSL certificates with SANs covering all broker container names
        let hostname_refs: Vec<&str> = nodes
            .iter()
            .filter(|node| node.roles.is_broker())
            .map(|node| node.container_name.as_str())
            .collect();
        let certs = test_certs::generate_test_certificates(&hostname_refs);
        let ca_cert_pem = certs.ca_cert_pem.clone();

        // Create and start all containers concurrently
        let mut handles = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let kafka = KafkaAllProtocols::new(node, &voters, num_brokers, &certs);
            let net = network_name.clone();
            let name = node.container_name.clone();
            let node_id = node.node_id;
            let server_props: Vec<(String, String)> =
                config.server_properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            // Pre-reserved host ports to bind to container ports (brokers
            // only). A proxied broker maps its backend ports; the advertised
            // ports are bound by its `BrokerProxy`.
            let mapped_ports: Vec<(u16, ContainerPort)> = node
                .backend_ports
                .as_ref()
                .or(node.ports.as_ref())
                .map(|ports| {
                    vec![
                        (ports.plaintext, PLAINTEXT_PORT),
                        (ports.ssl, SSL_PORT),
                        (ports.sasl_plaintext, SASL_PLAINTEXT_PORT),
                        (ports.sasl_ssl, SASL_SSL_PORT),
                    ]
                })
                .unwrap_or_default();

            handles.push(tokio::spawn(async move {
                let mut request = testcontainers::ContainerRequest::from(kafka)
                    .with_network(&net)
                    .with_container_name(&name)
                    // Bound the readiness wait. `ready_conditions` waits for
                    // "Kafka Server started" on stdout, which a container that
                    // failed to bind its ports never emits — without a deadline
                    // that future never resolves, and because the cluster is
                    // built inside a `OnceCell` (`super::cluster_pool`) it parks
                    // every test sharing this config, forever.
                    .with_startup_timeout(CONTAINER_STARTUP_TIMEOUT);
                for (host_port, container_port) in mapped_ports {
                    request = request.with_mapped_port(host_port, container_port);
                }

                for (key, value) in &server_props {
                    request = request.with_env_var(key, value);
                }

                request.start().await.map(|container| (node_id, container)).map_err(|err| {
                    // Trim the embedded container log — see
                    // `truncate_container_error`.
                    format!(
                        "Failed to start Kafka container {name}: {}",
                        truncate_container_error(&err.to_string())
                    )
                })
            }));
        }

        // Await all container starts (quorum forms once majority is up).
        // Collect every result before propagating a failure so the successful
        // containers land in `containers` and are reaped when it is dropped.
        let mut containers = BTreeMap::new();
        let mut start_error: Option<String> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok((node_id, container))) => {
                    containers.insert(node_id, container);
                },
                Ok(Err(err)) => {
                    start_error.get_or_insert(err);
                },
                Err(join_err) => {
                    // Surface as an attempt failure (not a panic here) so the
                    // attempt is still reaped; not retryable.
                    start_error.get_or_insert(format!("Broker start task panicked: {join_err}"));
                },
            }
        }
        if let Some(err) = start_error {
            return Err(err);
        }

        // Collect container IDs and build bootstrap strings from the known ports
        let container_ids: Vec<String> = nodes.iter().map(|node| containers[&node.node_id].id().to_string()).collect();
        let mut plaintext_addrs = Vec::with_capacity(nodes.len());
        let mut ssl_addrs = Vec::with_capacity(nodes.len());
        let mut sasl_plaintext_addrs = Vec::with_capacity(nodes.len());
        let mut sasl_ssl_addrs = Vec::with_capacity(nodes.len());
        let mut container_addrs = Vec::with_capacity(nodes.len());
        let mut broker_ports = BTreeMap::new();
        let mut proxies = BTreeMap::new();
        for node in &nodes {
            let Some(ports) = node.ports.as_ref() else { continue };
            if let Some(backend) = node.backend_ports.as_ref() {
                let proxy = BrokerProxy::new(ports, backend);
                if let Err(err) = proxy.start().await {
                    // Earlier nodes' proxies are dropped with this attempt; stop them first.
                    for started in proxies.values() {
                        BrokerProxy::stop(started).await;
                    }
                    return Err(err);
                }
                proxies.insert(node.node_id, proxy);
            }
            plaintext_addrs.push(format!("127.0.0.1:{}", ports.plaintext));
            ssl_addrs.push(format!("127.0.0.1:{}", ports.ssl));
            sasl_plaintext_addrs.push(format!("127.0.0.1:{}", ports.sasl_plaintext));
            sasl_ssl_addrs.push(format!("127.0.0.1:{}", ports.sasl_ssl));
            container_addrs.push(format!("{}:{CONTAINER_PORT_NUM}", node.container_name));
            broker_ports.insert(node.node_id, ports.plaintext);
        }

        Ok(Self {
            containers,
            broker_ids: nodes
                .iter()
                .filter(|node| node.roles.is_broker())
                .map(|node| node.node_id)
                .collect(),
            controller_ids: nodes
                .iter()
                .filter(|node| node.roles.is_controller())
                .map(|node| node.node_id)
                .collect(),
            broker_ports,
            proxies,
            shutdown_brokers: std::sync::Mutex::new(BTreeSet::new()),
            container_ids,
            network_name,
            bootstrap_servers: plaintext_addrs.join(","),
            ssl_bootstrap_servers: ssl_addrs.join(","),
            sasl_plaintext_bootstrap_servers: sasl_plaintext_addrs.join(","),
            sasl_ssl_bootstrap_servers: sasl_ssl_addrs.join(","),
            container_bootstrap_servers: container_addrs.join(","),
            ca_cert_pem,
            config: config.clone(),
        })
    }

    /// Bootstrap servers for the PLAINTEXT listener (e.g., `"127.0.0.1:32781"`
    /// for single broker, or `"127.0.0.1:32781,127.0.0.1:32782"` for multi-broker).
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Bootstrap servers for the SSL listener.
    pub fn ssl_bootstrap_servers(&self) -> &str {
        &self.ssl_bootstrap_servers
    }

    /// Bootstrap servers for the SASL_PLAINTEXT listener.
    pub fn sasl_plaintext_bootstrap_servers(&self) -> &str {
        &self.sasl_plaintext_bootstrap_servers
    }

    /// Bootstrap servers for the SASL_SSL listener.
    pub fn sasl_ssl_bootstrap_servers(&self) -> &str {
        &self.sasl_ssl_bootstrap_servers
    }

    /// Bootstrap servers reachable from sibling containers attached to
    /// this cluster's Docker network — `<container_name>:9099` per
    /// broker, advertising the CONTAINER listener. Used by the
    /// multilanguage gRPC test harness.
    pub fn container_bootstrap_servers(&self) -> &str {
        &self.container_bootstrap_servers
    }

    /// CA certificate PEM for SSL tests.
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// The config this cluster was started with.
    #[allow(dead_code)]
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Docker container IDs for all brokers.
    pub fn container_ids(&self) -> &[String] {
        &self.container_ids
    }

    /// Docker network name used by this cluster.
    pub fn network_name(&self) -> &str {
        &self.network_name
    }

    // -----------------------------------------------------------------------
    // Topology and broker lifecycle — Java `ClusterInstance` (see module docs).
    // -----------------------------------------------------------------------

    /// Java `ClusterInstance.type()`.
    pub fn cluster_type(&self) -> Type {
        self.config.cluster_type
    }

    /// Java `ClusterInstance.brokerIds()`: every node with the broker role,
    /// running or not.
    pub fn broker_ids(&self) -> BTreeSet<i32> {
        self.broker_ids.clone()
    }

    /// Java `ClusterInstance.aliveBrokers().keySet()`: the brokers not stopped by
    /// [`Self::shutdown_broker`].
    pub fn alive_broker_ids(&self) -> BTreeSet<i32> {
        let shutdown = self.shutdown_brokers.lock().expect("shutdown-broker set poisoned");
        self.broker_ids.difference(&shutdown).copied().collect()
    }

    /// Java `ClusterInstance.controllerIds()`: every node with the controller
    /// role (for [`Type::CoKraft`] that is every node).
    pub fn controller_ids(&self) -> BTreeSet<i32> {
        self.controller_ids.clone()
    }

    /// Java `ClusterInstance.brokerBoundPorts()`: the PLAINTEXT host port of
    /// each broker, in broker-id order.
    pub fn broker_bound_ports(&self) -> Vec<u16> {
        self.broker_ports.values().copied().collect()
    }

    /// `127.0.0.1:<port>` of `broker_id`'s PLAINTEXT listener — a bootstrap
    /// address naming that one broker.
    ///
    /// # Panics
    ///
    /// Panics with Java's `"Unknown brokerId <id>"` if there is no such broker.
    pub fn broker_bootstrap_servers(&self, broker_id: i32) -> String {
        let port = self
            .broker_ports
            .get(&broker_id)
            .unwrap_or_else(|| panic!("Unknown brokerId {broker_id}"));
        format!("127.0.0.1:{port}")
    }

    /// The broker's container, after checking the lifecycle preconditions.
    ///
    /// Java's `findBrokerOrThrow` throws `IllegalArgumentException("Unknown
    /// brokerId " + id)`; the two extra checks are Docker-harness constraints
    /// (see module docs).
    fn lifecycle_container(&self, broker_id: i32) -> &ContainerAsync<KafkaAllProtocols> {
        assert!(self.broker_ids.contains(&broker_id), "Unknown brokerId {broker_id}");
        assert!(
            self.config.dedicated,
            "broker lifecycle requires a dedicated cluster (ClusterConfig::dedicated): \
             a stopped broker in a pooled cluster would leak into other tests"
        );
        assert_eq!(
            self.config.cluster_type,
            Type::Kraft,
            "broker lifecycle requires Type::Kraft: a CO_KRAFT container also hosts a controller, \
             and stopping it would stop that controller too"
        );
        &self.containers[&broker_id]
    }

    /// Java `ClusterInstance.shutdownBroker(brokerId)`: stops the broker and
    /// returns once its process has exited.
    ///
    /// `docker stop` delivers `SIGTERM`, on which Kafka runs the same controlled
    /// shutdown as Java's `BrokerServer.shutdown()`; the container (and with it
    /// the broker's data) is kept for [`Self::start_broker`].
    ///
    /// It then stops the broker's [`BrokerProxy`], so on return every client
    /// connection to the broker is closed and new connections are refused —
    /// what a client sees once Java's `awaitShutdown()` has returned and the
    /// broker's socket server is closed. Relying on `docker stop` alone is not
    /// equivalent: Docker's host-port forwarder (notably Docker Desktop's) can
    /// outlive the container briefly, accepting a connection it then holds for
    /// ~15 s before resetting it — long enough to stall a client whose close
    /// waits for in-flight requests.
    ///
    /// # Panics
    ///
    /// Panics on an unknown broker id, on a pooled or [`Type::CoKraft`]
    /// cluster, or if Docker fails to stop the container.
    pub async fn shutdown_broker(&self, broker_id: i32) {
        let container = self.lifecycle_container(broker_id);
        container
            .stop_with_timeout(Some(BROKER_SHUTDOWN_TIMEOUT_SECS))
            .await
            .unwrap_or_else(|err| panic!("failed to stop broker {broker_id}: {err}"));
        self.proxies[&broker_id].stop().await;
        self.shutdown_brokers
            .lock()
            .expect("shutdown-broker set poisoned")
            .insert(broker_id);
    }

    /// Java `ClusterInstance.startBroker(brokerId)`: restarts a broker stopped by
    /// [`Self::shutdown_broker`] and returns once it is serving requests.
    ///
    /// Java's `BrokerServer.startup()` blocks until the broker has registered
    /// and been unfenced; the equivalent wait here is until an admin client
    /// bootstrapped only at the restarted broker — so its bootstrap `Metadata`
    /// is answered by that broker, proving it serves requests — sees it alive
    /// in `DescribeCluster` (which may be answered by any broker). The broker comes back with the same node id, host
    /// ports and log directory — see the module docs.
    ///
    /// # Panics
    ///
    /// Panics on an unknown broker id, on a pooled or [`Type::CoKraft`]
    /// cluster, if Docker fails to start the container, or if the broker is not
    /// serving within [`BROKER_READY_TIMEOUT`].
    pub async fn start_broker(&self, broker_id: i32) {
        let container = self.lifecycle_container(broker_id);
        container
            .start()
            .await
            .unwrap_or_else(|err| panic!("failed to start broker {broker_id}: {err}"));
        self.proxies[&broker_id]
            .start()
            .await
            .unwrap_or_else(|err| panic!("failed to restart the proxy of broker {broker_id}: {err}"));
        self.shutdown_brokers
            .lock()
            .expect("shutdown-broker set poisoned")
            .remove(&broker_id);

        let expected = BTreeSet::from([broker_id]);
        wait_until_broker_sees(&self.broker_bootstrap_servers(broker_id), |alive| alive.is_superset(&expected))
            .await
            .unwrap_or_else(|last| {
                panic!("broker {broker_id} not serving within {BROKER_READY_TIMEOUT:?} of restart; last view: {last}")
            });
    }

    /// Java `ClusterInstance.waitForReadyBrokers()`: waits until every alive
    /// broker is registered and unfenced, **as seen by each alive broker**.
    ///
    /// Java waits for the controller to count the brokers as ready and then for
    /// every broker's metadata cache to hold every broker alive. Over the wire
    /// the same condition is: a `Metadata` request sent **directly to each
    /// alive broker** (its own connection, not a least-loaded pick) returns
    /// exactly [`Self::alive_broker_ids`] — a broker answers `Metadata` from its
    /// metadata cache, which lists only unfenced brokers. Java counts all
    /// brokers because its callers only wait with every broker running; the
    /// alive set makes this also usable while some broker is stopped.
    ///
    /// # Panics
    ///
    /// Panics if the brokers do not converge within [`BROKER_READY_TIMEOUT`].
    pub async fn wait_for_ready_brokers(&self) {
        let alive = self.alive_broker_ids();
        let deadline = tokio::time::Instant::now() + BROKER_READY_TIMEOUT;
        for broker_id in &alive {
            let address = self.broker_bootstrap_servers(*broker_id);
            loop {
                let last = match broker_metadata_view(&address).await {
                    Ok(view) if view == alive => break,
                    Ok(view) => format!("{view:?}"),
                    Err(err) => format!("error: {err}"),
                };
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "brokers {alive:?} not ready within {BROKER_READY_TIMEOUT:?}: broker {broker_id} last saw {last}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

/// Metadata version for [`broker_metadata_view`]: v12 is inside the range
/// every AK 4.x broker supports (v0-v3 were removed in 4.0), and with an empty
/// topic list it asks for brokers only.
const READY_CHECK_METADATA_VERSION: i16 = 12;

/// The broker ids in the metadata cache of the broker at `address`, read with
/// a `Metadata` request over a dedicated connection to that broker alone —
/// the per-broker `metadataCache` check of Java's
/// `KafkaClusterTestKit.waitForReadyBrokers`.
async fn broker_metadata_view(address: &str) -> Result<BTreeSet<i32>, String> {
    use confluent_kafka::common::network::{NetworkSend, PlaintextChannelBuilder, Selectable, Selector};
    use confluent_kafka::common::protocol::ByteBufferAccessor;
    use confluent_kafka::common::requests::{
        ConcreteResponse, MetadataRequestBuilder, RequestBuilder, RequestHeader, RequestHeaderOptionsBuilder,
    };

    const NODE: &str = "ready-check";
    const POLL_MS: i64 = 500;
    const MAX_POLLS: usize = 20;
    let buffer_size = <Selector as Selectable>::USE_DEFAULT_BUFFER_SIZE;

    let addr: std::net::SocketAddr = address.parse().map_err(|err| format!("bad address {address}: {err}"))?;
    let mut selector =
        Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(PlaintextChannelBuilder::new(None)));
    let result = async {
        selector
            .connect(NODE, addr, "localhost", buffer_size, buffer_size)
            .await
            .map_err(|err| format!("connect: {err}"))?;

        let mut builder = MetadataRequestBuilder::with_topics_allow_auto_topic_creation_version(
            Some(&[]),
            false,
            READY_CHECK_METADATA_VERSION,
        );
        let mut request = builder
            .build_version(READY_CHECK_METADATA_VERSION)
            .map_err(|err| format!("build: {err}"))?;
        let header = RequestHeader::with_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(builder.api_key())
                .set_request_version(READY_CHECK_METADATA_VERSION)
                .set_client_id("cluster-lifecycle")
                .set_correlation_id(1)
                .build()
                .map_err(|err| format!("header: {err}"))?,
        )
        .map_err(|err| format!("header: {err}"))?;
        let send = request.to_send(&header).map_err(|err| format!("serialize: {err}"))?;
        selector
            .send(NetworkSend::new(NODE, Box::new(send)))
            .map_err(|err| format!("send: {err}"))?;

        for _ in 0..MAX_POLLS {
            selector.poll(POLL_MS).await.map_err(|err| format!("poll: {err}"))?;
            if let Some(receive) = selector.completed_receives().first() {
                let payload = receive.payload().ok_or("response without payload")?.to_vec();
                let response = ConcreteResponse::parse_response(&mut ByteBufferAccessor::new(payload), &header)
                    .map_err(|err| format!("parse: {err}"))?;
                let ConcreteResponse::Metadata(metadata) = response else {
                    return Err("not a Metadata response".to_string());
                };
                return Ok(metadata.data().brokers.iter().map(|broker| broker.node_id).collect());
            }
            if !selector.disconnected().is_empty() {
                return Err("disconnected".to_string());
            }
        }
        Err("no response".to_string())
    }
    .await;
    selector.close().await;
    result
}

/// How long `docker stop` lets a broker run its controlled shutdown before
/// `SIGKILL`. Docker's default is 10 s; a controlled shutdown with a live
/// controller takes a second or two, so this only bounds a pathological hang.
const BROKER_SHUTDOWN_TIMEOUT_SECS: i32 = 60;

/// Bound for a broker to (re)join after a start, and for
/// [`KafkaCluster::wait_for_ready_brokers`]. Same as the initial
/// [`CONTAINER_STARTUP_TIMEOUT`]: a restart pays the same JVM start-up and
/// quorum registration.
pub const BROKER_READY_TIMEOUT: std::time::Duration = CONTAINER_STARTUP_TIMEOUT;

/// Polls `DescribeCluster` through an admin client bootstrapped **only** at
/// `bootstrap` until `condition` holds for the set of broker ids it reports.
///
/// Only the bootstrap `Metadata` is pinned to `bootstrap`; each
/// `DescribeCluster` goes to the least-loaded known broker. For a per-broker
/// view use [`broker_metadata_view`].
///
/// Returns `Err` with the last observed view (or error) on
/// [`BROKER_READY_TIMEOUT`].
async fn wait_until_broker_sees(bootstrap: &str, condition: impl Fn(&BTreeSet<i32>) -> bool) -> Result<(), String> {
    use confluent_kafka::admin::{Admin, AdminClientConfig, KafkaAdminClient};

    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "cluster-lifecycle".to_string()),
        ("request.timeout.ms".to_string(), "5000".to_string()),
        ("default.api.timeout.ms".to_string(), "5000".to_string()),
        ("reconnect.backoff.max.ms".to_string(), "500".to_string()),
    ]);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    let admin = KafkaAdminClient::new(config).expect("admin client");

    let deadline = tokio::time::Instant::now() + BROKER_READY_TIMEOUT;
    let outcome = loop {
        let last = match admin.describe_cluster().nodes().get().await {
            Ok(nodes) => {
                let view: BTreeSet<i32> = nodes.iter().map(|node| node.id()).collect();
                if condition(&view) {
                    break Ok(());
                }
                format!("{view:?}")
            },
            Err(err) => format!("error: {err}"),
        };
        if tokio::time::Instant::now() >= deadline {
            break Err(last);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    admin.close_with_timeout(std::time::Duration::from_secs(5)).await;
    outcome
}

/// Docker network name of the bootstrap attempt identified by `suffix`.
fn attempt_network_name(suffix: &str) -> String {
    format!("kafka-net-{suffix}")
}

/// Removes everything a failed bootstrap attempt left in Docker: every
/// container whose name carries the attempt's `suffix` — including ones that
/// were created but never started — and then the attempt's network.
///
/// Best effort: failures are logged, not returned, because the attempt's own
/// error is the one worth reporting. Containers go first, since Docker refuses
/// to remove a network that still has endpoints.
async fn reap_failed_attempt(suffix: &str) {
    let name_filter = format!("name={suffix}");
    let ids = match docker_cli(&["ps", "-aq", "--filter", &name_filter]).await {
        Ok(stdout) => stdout,
        Err(err) => {
            eprintln!("WARN: could not list containers of failed cluster attempt {suffix}: {err}");
            String::new()
        },
    };
    let ids: Vec<&str> = ids.split_whitespace().collect();
    if !ids.is_empty() {
        let mut args = vec!["rm", "-f"];
        args.extend(&ids);
        match docker_cli(&args).await {
            Ok(_) => eprintln!(
                "WARN: reaped {} leftover container(s) of failed cluster attempt {suffix}",
                ids.len()
            ),
            Err(err) => eprintln!("WARN: could not remove containers {ids:?} of failed cluster attempt: {err}"),
        }
    }

    let network = attempt_network_name(suffix);
    let network_filter = format!("name=^{network}$");
    match docker_cli(&["network", "ls", "-q", "--filter", &network_filter]).await {
        Ok(found) if found.trim().is_empty() => {},
        Ok(_) => {
            if let Err(err) = docker_cli(&["network", "rm", &network]).await {
                eprintln!("WARN: could not remove network {network} of failed cluster attempt: {err}");
            }
        },
        Err(err) => eprintln!("WARN: could not look up network {network} of failed cluster attempt: {err}"),
    }
}

/// Runs `docker <args>` off the async runtime, bounded by
/// [`DOCKER_CLI_TIMEOUT`], returning stdout on success.
async fn docker_cli(args: &[&str]) -> Result<String, String> {
    let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
    let run = tokio::task::spawn_blocking(move || std::process::Command::new("docker").args(&owned).output());
    let output = tokio::time::timeout(DOCKER_CLI_TIMEOUT, run)
        .await
        .map_err(|_| format!("`docker {}` timed out after {DOCKER_CLI_TIMEOUT:?}", args.join(" ")))?
        .map_err(|err| format!("`docker {}` task failed: {err}", args.join(" ")))?
        .map_err(|err| format!("could not run `docker {}`: {err}", args.join(" ")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(format!(
            "`docker {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Generates a random hexadecimal suffix of the given byte length
/// (producing `2 * len` hex characters).
fn random_suffix(len: usize) -> String {
    let mut buf = String::with_capacity(len * 2);
    for _ in 0..len {
        let byte: u8 = rand::random();
        let _ = write!(buf, "{byte:02x}");
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::{
        KAFKA_TAG, MAX_START_ATTEMPTS, attempt_network_name, docker_cli, is_port_allocation_error,
        is_retryable_start_error, is_transient_docker_api_error, random_suffix, reap_failed_attempt,
        start_retry_backoff,
    };

    #[test]
    fn recognizes_linux_docker_wording() {
        assert!(is_port_allocation_error(
            "Bind for 0.0.0.0:54321 failed: port is already allocated"
        ));
    }

    #[test]
    fn recognizes_colima_macos_wording() {
        assert!(is_port_allocation_error(
            "Failed to start Kafka container kafka-1-abc: failed to start a container: \
             Docker responded with status code 500: failed to set up container networking: \
             driver failed programming external connectivity on endpoint kafka-1-abc (deadbeef): \
             failed to bind host port 0.0.0.0:51460/tcp: address already in use"
        ));
    }

    #[test]
    fn does_not_retry_unrelated_errors() {
        assert!(!is_port_allocation_error(
            "all predefined address pools have been fully subnetted"
        ));
    }

    /// The error observed on Docker Desktop for macOS (PLAN Phase 20), as
    /// testcontainers formats a bollard `RequestTimeoutError` from the
    /// network-exists check, in both the observed and the full wording.
    #[test]
    fn retries_docker_api_timeouts() {
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-broker-0-abc: failed to list networks: Timeout"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-broker-0-abc: failed to list networks: Timeout error"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-controller-3000-abc: failed to create a container: Timeout error"
        ));
    }

    #[test]
    fn retries_daemon_connection_failures() {
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to create a network: Error in the hyper legacy \
             client: client error (Connect)"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to inspect a container: Connection reset by peer \
             (os error 54)"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to start a container: Connection refused (os error 61)"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to initialize a docker client: Socket not found: \
             /var/run/docker.sock"
        ));
    }

    #[test]
    fn retries_docker_api_server_errors() {
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to start a container: Docker responded with \
             status code 500: context deadline exceeded"
        ));
        assert!(is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to list networks: Docker responded with status \
             code 503: service unavailable"
        ));
        assert!(!is_transient_docker_api_error(
            "Failed to start Kafka container kafka-1-abc: failed to create a container: Docker responded with \
             status code 409: Conflict. The container name is already in use"
        ));
        assert!(!is_transient_docker_api_error(
            "failed to create a container: Docker responded with status code 404: No such image"
        ));
    }

    /// The subnet-exhaustion guard wins over every retryable class: the
    /// error arrives as a 500, which would otherwise be retried.
    #[test]
    fn never_retries_address_pool_exhaustion() {
        assert!(!is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to create a network: Docker responded with \
             status code 500: all predefined address pools have been fully subnetted"
        ));
    }

    #[test]
    fn keeps_existing_retry_classes() {
        assert!(is_retryable_start_error(
            "Bind for 0.0.0.0:54321 failed: port is already allocated"
        ));
        assert!(is_retryable_start_error(
            "container is not ready: End of stream reached before finding message: Kafka Server started"
        ));
        assert!(!is_retryable_start_error(
            "Failed to start Kafka container kafka-1-abc: failed to pull the image 'apache/kafka:4.2.0', error: \
             unauthorized"
        ));
    }

    /// Removes whatever the reap test staged, however the test ends — a
    /// leaked `kafka-net-*` network costs one of Docker's ~30 default subnets
    /// for good. Synchronous `docker` calls, since `Drop` cannot await.
    struct StagedAttemptCleanup {
        suffix: String,
    }

    impl Drop for StagedAttemptCleanup {
        fn drop(&mut self) {
            let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output().ok();
            let name_filter = format!("name={}", self.suffix);
            if let Some(out) = docker(&["ps", "-aq", "--filter", &name_filter]) {
                for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
                    let _ = docker(&["rm", "-f", id]);
                }
            }
            let _ = docker(&["network", "rm", &attempt_network_name(&self.suffix)]);
        }
    }

    /// A failed attempt can leave a container that was created but never
    /// started, still attached to the attempt's network (PLAN Phase 20).
    /// Stage exactly that with the broker image (`docker create` does not
    /// start it) and check both are gone after the reap.
    #[tokio::test]
    async fn reap_removes_created_containers_and_network() {
        let suffix = random_suffix(8);
        let network = attempt_network_name(&suffix);
        let image = format!("apache/kafka:{KAFKA_TAG}");
        // Pull up front, without `DOCKER_CLI_TIMEOUT`: a cold pull can take
        // longer than 30 s. The creates below then use `--pull never`, so
        // none of the bounded calls can turn into a pull.
        if docker_cli(&["image", "inspect", &image]).await.is_err() {
            let pull_image = image.clone();
            let pulled = tokio::task::spawn_blocking(move || {
                std::process::Command::new("docker").args(["pull", &pull_image]).status()
            })
            .await
            .expect("pull task");
            assert!(pulled.is_ok_and(|status| status.success()), "could not pull {image}");
        }

        // Created before anything is staged, so every exit path cleans up.
        let _cleanup = StagedAttemptCleanup { suffix: suffix.clone() };
        docker_cli(&["network", "create", &network]).await.expect("create network");
        for name in [
            format!("kafka-broker-0-{suffix}"),
            format!("kafka-controller-3000-{suffix}"),
        ] {
            docker_cli(&[
                "create",
                "--pull",
                "never",
                "--name",
                &name,
                "--network",
                &network,
                &image,
            ])
            .await
            .expect("create container");
        }

        reap_failed_attempt(&suffix).await;

        let name_filter = format!("name={suffix}");
        let left = docker_cli(&["ps", "-aq", "--filter", &name_filter])
            .await
            .expect("list containers");
        assert!(left.trim().is_empty(), "containers left behind: {left}");
        let network_filter = format!("name=^{network}$");
        let left = docker_cli(&["network", "ls", "-q", "--filter", &network_filter])
            .await
            .expect("list networks");
        assert!(left.trim().is_empty(), "network left behind: {left}");
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let secs: Vec<u64> = (1..=MAX_START_ATTEMPTS)
            .map(|attempt| start_retry_backoff(attempt).as_secs())
            .collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 8, 8]);
    }
}
