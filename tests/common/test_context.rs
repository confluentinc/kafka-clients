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

use std::collections::HashMap;
use std::sync::Arc;

use super::cluster_config::ClusterConfig;
use super::cluster_pool;
use super::kafka_cluster::{KafkaCluster, SASL_PASSWORD, SASL_USERNAME};

/// Security protocol the parameterized integration suite runs against.
///
/// Determined by the `INTEGRATION_TEST_PROTOCOL` environment variable, read
/// fresh on each query (unset / unknown -> [`TestProtocol::Plaintext`]). The
/// variable is fixed for the life of a test-binary process, so every query
/// yields the same protocol — one protocol per run. This
/// is how CI runs the whole functional integration suite three times — once
/// over PLAINTEXT, once over SSL, once over SASL_SSL — without any per-test
/// change: each test builds its config through [`TestContext::configure`] /
/// [`TestContext::apply_security`], which fill in the right listener port and
/// security keys for whichever protocol this run selected.
///
/// Every broker the harness starts exposes all four listeners simultaneously
/// (see [`super::kafka_cluster`]), so switching protocol is purely a
/// client-config choice — no cluster restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestProtocol {
    /// `security.protocol=PLAINTEXT` — the default; no encryption, no auth.
    Plaintext,
    /// `security.protocol=SSL` — TLS transport, server-cert trust only.
    Ssl,
    /// `security.protocol=SASL_SSL` with SASL/PLAIN — auth over TLS.
    SaslSsl,
}

impl TestProtocol {
    /// Read the protocol selected for this test-binary run from
    /// `INTEGRATION_TEST_PROTOCOL`. Accepts `plaintext` (default), `ssl`, and
    /// `sasl_ssl` (case-insensitive; `sasl-ssl` also accepted). Any unset or
    /// unrecognized value falls back to [`TestProtocol::Plaintext`], so a plain
    /// `cargo test` run behaves exactly as before this parameterization.
    pub fn from_env() -> Self {
        match std::env::var("INTEGRATION_TEST_PROTOCOL")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("ssl") => TestProtocol::Ssl,
            Some("sasl_ssl") | Some("sasl-ssl") => TestProtocol::SaslSsl,
            _ => TestProtocol::Plaintext,
        }
    }
}

/// Build a SASL/PLAIN `sasl.jaas.config` string for the given credentials.
///
/// Mirrors the broker-side `PlainLoginModule` the harness configures with
/// `admin` / `admin-secret` (see [`super::kafka_cluster`]).
pub fn plain_jaas_config(username: &str, password: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{username}\" password=\"{password}\";"
    )
}

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

    /// Bootstrap servers for the PLAINTEXT listener.
    pub fn bootstrap_servers(&self) -> &str {
        self.cluster.bootstrap_servers()
    }

    /// Bootstrap servers for the SSL listener.
    pub fn ssl_bootstrap_servers(&self) -> &str {
        self.cluster.ssl_bootstrap_servers()
    }

    /// Bootstrap servers for the SASL_PLAINTEXT listener.
    pub fn sasl_plaintext_bootstrap_servers(&self) -> &str {
        self.cluster.sasl_plaintext_bootstrap_servers()
    }

    /// Bootstrap servers for the SASL_SSL listener.
    pub fn sasl_ssl_bootstrap_servers(&self) -> &str {
        self.cluster.sasl_ssl_bootstrap_servers()
    }

    /// Bootstrap servers reachable from sibling containers attached to
    /// this cluster's Docker network. Used by the multilanguage gRPC
    /// test harness — the python and c gRPC server containers cannot
    /// reach the broker via the host's `127.0.0.1` loopback, so they
    /// use these container-internal addresses instead.
    pub fn container_bootstrap_servers(&self) -> &str {
        self.cluster.container_bootstrap_servers()
    }

    /// Docker network name used by this cluster. Sibling gRPC client
    /// containers must join this network to reach the broker via the
    /// CONTAINER listener.
    pub fn broker_network_name(&self) -> &str {
        self.cluster.network_name()
    }

    /// CA certificate PEM for SSL tests.
    pub fn ca_cert_pem(&self) -> &str {
        self.cluster.ca_cert_pem()
    }

    /// The security protocol this test-binary run targets (from
    /// `INTEGRATION_TEST_PROTOCOL`). See [`TestProtocol`].
    pub fn protocol(&self) -> TestProtocol {
        TestProtocol::from_env()
    }

    /// Host-loopback bootstrap servers for the [`protocol`](Self::protocol)
    /// selected this run: PLAINTEXT -> `:9092`, SSL -> `:9096`,
    /// SASL_SSL -> `:9097`.
    ///
    /// This is the address native (in-process Rust) clients should use.
    /// Container-backed gRPC backends must keep calling
    /// [`container_bootstrap_servers`](Self::container_bootstrap_servers)
    /// instead — their broker listener is PLAINTEXT-only and unaffected by the
    /// protocol selector.
    pub fn protocol_bootstrap_servers(&self) -> &str {
        match self.protocol() {
            TestProtocol::Plaintext => self.cluster.bootstrap_servers(),
            TestProtocol::Ssl => self.cluster.ssl_bootstrap_servers(),
            TestProtocol::SaslSsl => self.cluster.sasl_ssl_bootstrap_servers(),
        }
    }

    /// Inject the client-side security config keys for the selected
    /// [`protocol`](Self::protocol) into `cfg`.
    ///
    /// - PLAINTEXT: no-op (client defaults to `security.protocol=PLAINTEXT`).
    /// - SSL: `security.protocol=SSL`, PEM truststore from
    ///   [`ca_cert_pem`](Self::ca_cert_pem), and an empty
    ///   `ssl.endpoint.identification.algorithm` (tests connect via
    ///   `127.0.0.1`, so hostname verification is disabled — matching the
    ///   dedicated `ssl_sasl_test` / `sasl_ssl_consumer_test`).
    /// - SASL_SSL: the SSL keys plus SASL/PLAIN (`sasl.mechanism=PLAIN` and a
    ///   `sasl.jaas.config` for `admin` / `admin-secret`).
    ///
    /// Does NOT set `bootstrap.servers`; pair it with
    /// [`protocol_bootstrap_servers`](Self::protocol_bootstrap_servers), or use
    /// [`configure`](Self::configure) to do both at once.
    pub fn apply_security(&self, cfg: &mut HashMap<String, String>) {
        match self.protocol() {
            TestProtocol::Plaintext => {},
            TestProtocol::Ssl => {
                cfg.insert("security.protocol".to_string(), "SSL".to_string());
                cfg.insert("ssl.truststore.certificates".to_string(), self.ca_cert_pem().to_string());
                cfg.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
            },
            TestProtocol::SaslSsl => {
                cfg.insert("security.protocol".to_string(), "SASL_SSL".to_string());
                cfg.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
                cfg.insert("sasl.jaas.config".to_string(), plain_jaas_config(SASL_USERNAME, SASL_PASSWORD));
                cfg.insert("ssl.truststore.certificates".to_string(), self.ca_cert_pem().to_string());
                cfg.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
            },
        }
    }

    /// Set `bootstrap.servers` to [`protocol_bootstrap_servers`] and inject the
    /// matching security keys via [`apply_security`] — the one call a
    /// native-client config builder needs to become protocol-aware.
    ///
    /// [`protocol_bootstrap_servers`]: Self::protocol_bootstrap_servers
    /// [`apply_security`]: Self::apply_security
    pub fn configure(&self, cfg: &mut HashMap<String, String>) {
        cfg.insert("bootstrap.servers".to_string(), self.protocol_bootstrap_servers().to_string());
        self.apply_security(cfg);
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
