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

//! Per-test isolation and cleanup for integration tests.
//!
//! Each test creates a [`TestContext`] that provides unique resource names
//! and tracks created resources for cleanup. This is the key to parallel
//! test safety on a shared cluster.

use std::sync::Arc;

use super::cluster_config::ClusterConfig;
use super::cluster_pool;
use super::kafka_cluster::KafkaCluster;

/// Per-test context providing unique resource names and cleanup.
///
/// Each test creates one of these; they share the underlying
/// [`KafkaCluster`].
pub struct TestContext {
    /// The shared cluster backing this test.
    cluster: Arc<KafkaCluster>,
    /// Unique prefix for this test's resources (e.g., `"test_api_versions_a3f9"`).
    prefix: String,
    /// Topics created during this test, cleaned up on drop.
    created_topics: Vec<String>,
}

impl TestContext {
    /// Create a new context for a test, sharing the given cluster.
    ///
    /// Generates a unique prefix from the current thread name + random suffix.
    pub async fn new(config: ClusterConfig) -> Self {
        let cluster = cluster_pool::get_or_create(&config).await;
        let thread_name = std::thread::current().name().unwrap_or("test").to_string();
        // Sanitize thread name: Kafka topic names only allow [a-zA-Z0-9._-].
        // Replace invalid characters (e.g. '::' from module paths) with '_'.
        let sanitized: String = thread_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let suffix = random_suffix(4);
        let prefix = format!("{sanitized}_{suffix}");

        Self { cluster, prefix, created_topics: Vec::new() }
    }

    /// Bootstrap servers string (delegates to cluster).
    pub fn bootstrap_servers(&self) -> &str {
        self.cluster.bootstrap_servers()
    }

    /// Bootstrap servers for the secure listener (SASL/SSL port).
    /// Returns `None` for PLAINTEXT clusters.
    pub fn secure_bootstrap_servers(&self) -> Option<&str> {
        self.cluster.secure_bootstrap_servers()
    }

    /// CA certificate PEM for SSL tests.
    /// Returns `None` for non-SSL clusters.
    pub fn ca_cert_pem(&self) -> Option<&str> {
        self.cluster.ca_cert_pem()
    }

    /// Generate a unique topic name for this test.
    ///
    /// Example: `"test_api_versions_a3f9_my_topic"`.
    #[allow(dead_code)]
    pub fn topic(&mut self, base_name: &str) -> String {
        let name = format!("{}_{}", self.prefix, base_name);
        self.created_topics.push(name.clone());
        name
    }

    /// Generate a unique consumer group ID for this test.
    #[allow(dead_code)]
    pub fn group_id(&self, base_name: &str) -> String {
        format!("{}_{}", self.prefix, base_name)
    }

    /// Clean up all resources created during this test.
    ///
    /// Currently a no-op since we do not have an admin client yet.
    /// Topics auto-created during the test are ephemeral and shared-cluster
    /// isolation is achieved through unique naming, not deletion.
    #[allow(dead_code)]
    pub async fn cleanup(&mut self) {
        // Future: delete topics via admin client or docker exec
        self.created_topics.clear();
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if !self.created_topics.is_empty() {
            eprintln!(
                "WARN: TestContext dropped with {} uncleaned topics (prefix: {})",
                self.created_topics.len(),
                self.prefix
            );
        }
    }
}

/// Generates a random hexadecimal suffix of the given length (in bytes,
/// producing `2 * len` hex characters).
fn random_suffix(len: usize) -> String {
    use std::fmt::Write;
    let mut buf = String::with_capacity(len * 2);
    for _ in 0..len {
        let byte: u8 = rand::random();
        let _ = write!(buf, "{byte:02x}");
    }
    buf
}
