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
use std::sync::{Arc, Mutex};

use super::cluster_config::ClusterConfig;
use super::kafka_cluster::KafkaCluster;

use tokio::sync::OnceCell;

/// Process-global pool of shared Kafka cluster instances.
///
/// Each unique `ClusterConfig` gets at most one running container.
static CLUSTER_POOL: std::sync::LazyLock<Mutex<HashMap<ClusterConfig, Arc<OnceCell<Arc<KafkaCluster>>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a shared [`KafkaCluster`] for the given config.
///
/// The first caller with a given config triggers container startup;
/// subsequent callers await the same `OnceCell` and receive a reference
/// to the already-running cluster.
pub async fn get_or_create(config: &ClusterConfig) -> Arc<KafkaCluster> {
    let cell = {
        let mut pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");
        pool.entry(config.clone()).or_insert_with(|| Arc::new(OnceCell::new())).clone()
    };

    cell.get_or_init(|| async { Arc::new(KafkaCluster::start_with_config(config).await) })
        .await
        .clone()
}
