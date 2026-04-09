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

//! Shared cluster instance registry for integration tests.
//!
//! A process-global pool mapping [`ClusterConfig`] to a running
//! [`KafkaCluster`]. All tests requesting the same configuration share
//! one container, amortizing the Docker startup cost.
//!
//! Thread safety is achieved through `Mutex` + `tokio::sync::OnceCell`:
//! - The `Mutex` protects the pool map (held briefly, only to look up or
//!   insert a `OnceCell`).
//! - The `OnceCell` ensures that exactly one task starts the container;
//!   all other callers await its completion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};

use super::cluster_config::ClusterConfig;
use super::kafka_cluster::KafkaCluster;

use tokio::sync::OnceCell;

/// Type alias for the cluster pool's inner value: a once-cell holding a shared cluster.
type ClusterCell = Arc<OnceCell<Arc<KafkaCluster>>>;

/// Process-global pool of shared Kafka cluster instances.
///
/// Each unique `ClusterConfig` gets at most one running container.
static CLUSTER_POOL: std::sync::LazyLock<Mutex<HashMap<ClusterConfig, ClusterCell>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Ensures the atexit cleanup hook is registered exactly once.
static CLEANUP_REGISTERED: Once = Once::new();

/// Registers an `atexit` handler that forcibly removes all Docker containers
/// in the pool when the process exits.
///
/// Rust does not run destructors for `LazyLock` statics at process exit,
/// so `ContainerAsync`'s `Drop` never fires. This hook uses `docker rm -f`
/// as a synchronous fallback that works even after the tokio runtime is gone.
fn register_cleanup_hook() {
    CLEANUP_REGISTERED.call_once(|| {
        unsafe extern "C" {
            safe fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
        }

        extern "C" fn cleanup_containers() {
            let container_ids: Vec<String> = {
                let pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");
                pool.values()
                    .filter_map(|cell| cell.get().map(|c| c.container_id().to_string()))
                    .collect()
            };
            for id in &container_ids {
                let _ = std::process::Command::new("docker").args(["rm", "-f", id]).output();
            }
        }

        atexit(cleanup_containers);
    });
}

/// Get or create a shared [`KafkaCluster`] for the given config.
///
/// The first caller with a given config triggers container startup;
/// subsequent callers await the same `OnceCell` and receive a reference
/// to the already-running cluster.
pub async fn get_or_create(config: &ClusterConfig) -> Arc<KafkaCluster> {
    register_cleanup_hook();

    let cell = {
        let mut pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");
        pool.entry(config.clone()).or_insert_with(|| Arc::new(OnceCell::new())).clone()
    };

    cell.get_or_init(|| async { Arc::new(KafkaCluster::start_with_config(config).await) })
        .await
        .clone()
}
