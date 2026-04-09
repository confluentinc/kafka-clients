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
//! Wraps a single testcontainers Kafka container. Created by
//! [`super::cluster_pool`], shared across tests with the same
//! [`super::cluster_config::ClusterConfig`].

use super::cluster_config::ClusterConfig;

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::kafka::apache;
use testcontainers_modules::kafka::apache::Kafka;

/// Manages a real Kafka broker in Docker for integration tests.
///
/// Shared across tests with the same [`ClusterConfig`].
/// The container is cleaned up automatically when this struct is dropped
/// (testcontainers default behavior).
pub struct KafkaCluster {
    /// The running container handle. Kept alive for the duration of the pool entry.
    _container: ContainerAsync<Kafka>,
    /// The `host:port` connection string for this cluster.
    bootstrap_servers: String,
    /// The config this cluster was started with.
    config: ClusterConfig,
}

impl KafkaCluster {
    /// Start a cluster matching the given config.
    ///
    /// Called by [`super::cluster_pool`], not by tests directly.
    ///
    /// # Panics
    ///
    /// Panics if the container fails to start or ports cannot be retrieved.
    pub async fn start_with_config(config: &ClusterConfig) -> Self {
        // Build the container request, applying any custom server properties
        // as environment variables. `ImageExt::with_env_var` consumes the image
        // and returns a `ContainerRequest<Kafka>`, so we must convert first.
        let mut request: testcontainers::ContainerRequest<Kafka> =
            testcontainers::ContainerRequest::from(Kafka::default());

        for (key, value) in &config.server_properties {
            request = request.with_env_var(key, value);
        }

        let container = request.start().await.expect("Failed to start Kafka container");

        let host_port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Failed to get Kafka host port");

        let bootstrap_servers = format!("127.0.0.1:{host_port}");

        Self { _container: container, bootstrap_servers, config: config.clone() }
    }

    /// Bootstrap servers connection string (e.g., `"127.0.0.1:32781"`).
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// The config this cluster was started with.
    #[allow(dead_code)]
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }
}
