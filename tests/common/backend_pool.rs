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

/// The two non-native backends. The native rust backend doesn't need a
/// container; tests instantiate it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BackendKind {
    Python,
    C,
}

impl BackendKind {
    fn image_repository(self) -> &'static str {
        match self {
            BackendKind::Python => "confluent-kafka-rust/python-grpc-server",
            BackendKind::C => "confluent-kafka-rust/c-grpc-server",
        }
    }

    /// The fixed internal port the gRPC server binds inside the container.
    /// Testcontainers maps this to a random host port at start time.
    fn internal_port(self) -> u16 {
        match self {
            BackendKind::Python => 50051,
            BackendKind::C => 50052,
        }
    }

    fn label(self) -> &'static str {
        match self {
            BackendKind::Python => "python",
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
        Endpoint::from_shared(self.endpoint.clone())
            .expect("backend endpoint is always a valid URI")
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap_or_else(|e| panic!("failed to connect to {} backend: {e}", self.endpoint))
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

    let host_port = container
        .get_host_port_ipv4(internal)
        .await
        .unwrap_or_else(|e| panic!("failed to read mapped host port for {} backend: {e}", kind.label()));
    let container_id = container.id().to_string();
    let endpoint = format!("http://127.0.0.1:{host_port}");

    BackendHandle { _container: container, container_id, endpoint }
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
            let pool = BACKEND_POOL.lock().expect("backend pool lock poisoned");
            let handles: Vec<_> = pool.values().filter_map(|cell| cell.get().cloned()).collect();
            drop(pool);

            for handle in &handles {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", handle.container_id()])
                    .output();
            }
        }

        atexit(cleanup_backend_containers);
    });
}
