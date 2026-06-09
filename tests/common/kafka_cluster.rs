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
//! [`super::cluster_config::ClusterConfig`].

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as _;

use super::cluster_config::ClusterConfig;
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

/// Finds an available TCP port on localhost by letting the OS assign one.
///
/// There is a tiny TOCTOU race window between when we release the port
/// and when testcontainers binds to it. In practice this is the standard
/// pattern for integration tests and extremely unlikely to fail.
fn find_available_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to port 0")
        .local_addr()
        .expect("Failed to get local addr")
        .port()
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
    /// Create the image for one broker in a cluster.
    ///
    /// - `node_id`: 1-based broker/controller node ID
    /// - `container_names`: ordered list of all broker container names
    ///    (index 0 = node 1, index 1 = node 2, etc.)
    /// - `ports`: pre-reserved host ports for this broker's client listeners
    /// - `certs`: shared SSL certificates (CA + broker cert with all
    ///    container hostnames in SANs)
    fn new(
        node_id: u16,
        container_names: &[String],
        ports: &BrokerPorts,
        certs: &test_certs::TestCertificates,
    ) -> Self {
        let num_brokers = container_names.len() as u16;
        let this_container = &container_names[(node_id - 1) as usize];

        let mut env_vars = HashMap::new();

        // KRaft configuration
        env_vars.insert("CLUSTER_ID".into(), CLUSTER_ID.into());
        env_vars.insert("KAFKA_NODE_ID".into(), node_id.to_string());
        env_vars.insert("KAFKA_PROCESS_ROLES".into(), "broker,controller".into());
        env_vars.insert("KAFKA_CONTROLLER_LISTENER_NAMES".into(), "CONTROLLER".into());
        env_vars.insert("KAFKA_INTER_BROKER_LISTENER_NAME".into(), "BROKER".into());

        // Quorum voters: all brokers participate as controllers
        let voters: String = container_names
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{}@{}:9094", i + 1, name))
            .collect::<Vec<_>>()
            .join(",");
        env_vars.insert("KAFKA_CONTROLLER_QUORUM_VOTERS".into(), voters);
        env_vars.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".into(), num_brokers.min(3).to_string());

        // Listeners — custom names avoid the configure script's SSL check.
        //
        // The check triggers on "SSL://" in KAFKA_ADVERTISED_LISTENERS.
        // Using TLSONLY (for SSL) and SASLTLS (for SASL_SSL) avoids the
        // substring match. listener.security.protocol.map maps them to
        // the actual protocols.
        env_vars.insert(
            "KAFKA_LISTENERS".into(),
            format!(
                "PLAINTEXT://0.0.0.0:9092,TLSONLY://0.0.0.0:9096,SASLPLAIN://0.0.0.0:9095,\
                 SASLTLS://0.0.0.0:9097,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094,\
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
/// One or more containers, each running a KRaft broker+controller,
/// connected via a shared Docker network. All brokers expose all four
/// security protocols. Tests choose which listener to connect to via
/// the protocol-specific bootstrap server accessors.
///
/// Shared across tests with the same [`ClusterConfig`].
pub struct KafkaCluster {
    /// The running container handles. Kept alive for the duration of the pool entry.
    _containers: Vec<ContainerAsync<KafkaAllProtocols>>,
    /// Docker container IDs for all brokers, used for cleanup.
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
    /// # Panics
    ///
    /// Panics if any container fails to start or ports cannot be retrieved.
    pub async fn start_with_config(config: &ClusterConfig) -> Self {
        let num_brokers = config.brokers;
        assert!(num_brokers >= 1, "Cluster must have at least 1 broker");

        // Generate unique names to avoid collisions between concurrent test processes
        let suffix = random_suffix(8);
        let network_name = format!("kafka-net-{suffix}");
        let container_names: Vec<String> = (1..=num_brokers).map(|id| format!("kafka-{id}-{suffix}")).collect();

        // Reserve host ports for each broker before starting containers.
        // This lets us set correct advertised.listeners from the start,
        // which is required because KRaft does not allow dynamic updates
        // to advertised.listeners.
        let broker_ports: Vec<BrokerPorts> = (0..num_brokers).map(|_| BrokerPorts::reserve()).collect();

        // Generate SSL certificates with SANs covering all broker container names
        let hostname_refs: Vec<&str> = container_names.iter().map(String::as_str).collect();
        let certs = test_certs::generate_test_certificates(&hostname_refs);
        let ca_cert_pem = certs.ca_cert_pem.clone();

        // Create and start all broker containers concurrently
        let mut handles = Vec::with_capacity(num_brokers as usize);
        for node_id in 1..=num_brokers {
            let idx = (node_id - 1) as usize;
            let kafka = KafkaAllProtocols::new(node_id, &container_names, &broker_ports[idx], &certs);
            let net = network_name.clone();
            let name = container_names[idx].clone();
            let ports = &broker_ports[idx];
            let server_props: Vec<(String, String)> =
                config.server_properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            // Bind pre-reserved host ports to container ports
            let plaintext_port = ports.plaintext;
            let ssl_port = ports.ssl;
            let sasl_plaintext_port = ports.sasl_plaintext;
            let sasl_ssl_port = ports.sasl_ssl;

            handles.push(tokio::spawn(async move {
                let mut request = testcontainers::ContainerRequest::from(kafka)
                    .with_network(&net)
                    .with_container_name(&name)
                    .with_mapped_port(plaintext_port, PLAINTEXT_PORT)
                    .with_mapped_port(ssl_port, SSL_PORT)
                    .with_mapped_port(sasl_plaintext_port, SASL_PLAINTEXT_PORT)
                    .with_mapped_port(sasl_ssl_port, SASL_SSL_PORT);

                for (key, value) in &server_props {
                    request = request.with_env_var(key, value);
                }

                request.start().await.expect("Failed to start Kafka container")
            }));
        }

        // Await all container starts (quorum forms once majority is up)
        let mut containers = Vec::with_capacity(handles.len());
        for handle in handles {
            containers.push(handle.await.expect("Broker start task panicked"));
        }

        // Collect container IDs and build bootstrap strings from the known ports
        let mut container_ids = Vec::with_capacity(containers.len());
        let mut plaintext_addrs = Vec::with_capacity(containers.len());
        let mut ssl_addrs = Vec::with_capacity(containers.len());
        let mut sasl_plaintext_addrs = Vec::with_capacity(containers.len());
        let mut sasl_ssl_addrs = Vec::with_capacity(containers.len());

        let mut container_addrs = Vec::with_capacity(containers.len());
        for (i, container) in containers.iter().enumerate() {
            container_ids.push(container.id().to_string());
            let ports = &broker_ports[i];

            plaintext_addrs.push(format!("127.0.0.1:{}", ports.plaintext));
            ssl_addrs.push(format!("127.0.0.1:{}", ports.ssl));
            sasl_plaintext_addrs.push(format!("127.0.0.1:{}", ports.sasl_plaintext));
            sasl_ssl_addrs.push(format!("127.0.0.1:{}", ports.sasl_ssl));
            container_addrs.push(format!("{}:{CONTAINER_PORT_NUM}", container_names[i]));
        }

        Self {
            _containers: containers,
            container_ids,
            network_name,
            bootstrap_servers: plaintext_addrs.join(","),
            ssl_bootstrap_servers: ssl_addrs.join(","),
            sasl_plaintext_bootstrap_servers: sasl_plaintext_addrs.join(","),
            sasl_ssl_bootstrap_servers: sasl_ssl_addrs.join(","),
            container_bootstrap_servers: container_addrs.join(","),
            ca_cert_pem,
            config: config.clone(),
        }
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
