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
//!
//! Supports two image types:
//! - `Kafka` (from testcontainers-modules) for PLAINTEXT
//! - `SecureKafka` (custom) for SSL, SASL_PLAINTEXT, and SASL_SSL

use std::borrow::Cow;
use std::collections::HashMap;

use super::cluster_config::{ClusterConfig, SecurityMode};
use super::test_certs;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyToContainer, Image, ImageExt};
use testcontainers_modules::kafka::apache;
use testcontainers_modules::kafka::apache::Kafka;

/// Kafka image tag to use for integration tests.
const KAFKA_TAG: &str = "4.2.0";

/// Container port for the PLAINTEXT listener (always present).
const PLAINTEXT_PORT: ContainerPort = ContainerPort::Tcp(9092);
/// Container port for the SASL_PLAINTEXT listener.
const SASL_PLAINTEXT_PORT: ContainerPort = ContainerPort::Tcp(9095);
/// Container port for the SSL listener.
const SSL_PORT: ContainerPort = ContainerPort::Tcp(9096);
/// Container port for the SASL_SSL listener.
const SASL_SSL_PORT: ContainerPort = ContainerPort::Tcp(9097);

/// Path where the start script is written inside the container.
const START_SCRIPT: &str = "/opt/kafka/testcontainers_start.sh";
/// Path for the JAAS config file inside the container.
const JAAS_CONFIG_PATH: &str = "/opt/kafka/config/kafka_server_jaas.conf";

/// Custom Kafka image for SSL/SASL integration tests.
///
/// The standard `testcontainers_modules::kafka::apache::Kafka` only supports
/// PLAINTEXT. This custom image configures additional listeners for SSL and SASL.
///
/// Uses the JVM-based `apache/kafka` image (not native) because SASL requires
/// the full JVM runtime.
#[derive(Debug, Clone)]
struct SecureKafka {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<CopyToContainer>,
    exposed_ports: Vec<ContainerPort>,
    /// The security mode that determines which secure listener is configured.
    security_mode: SecurityMode,
}

impl SecureKafka {
    /// Create a new `SecureKafka` image for the given security mode.
    fn new(security_mode: &SecurityMode) -> Self {
        let mut env_vars = HashMap::new();
        let mut copy_to_sources = Vec::new();
        let mut exposed_ports = vec![PLAINTEXT_PORT];

        // Base KRaft configuration (same as the standard Kafka image)
        env_vars.insert("CLUSTER_ID".to_owned(), apache::DEFAULT_CLUSTER_ID.to_owned());
        env_vars.insert("KAFKA_PROCESS_ROLES".to_owned(), "broker,controller".to_owned());
        env_vars.insert("KAFKA_CONTROLLER_LISTENER_NAMES".to_owned(), "CONTROLLER".to_owned());
        env_vars.insert("KAFKA_INTER_BROKER_LISTENER_NAME".to_owned(), "BROKER".to_owned());
        env_vars.insert("KAFKA_BROKER_ID".to_owned(), apache::DEFAULT_BROKER_ID.to_string());
        env_vars.insert(
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_owned(),
            apache::DEFAULT_INTERNAL_TOPIC_RF.to_string(),
        );
        env_vars.insert(
            "KAFKA_CONTROLLER_QUORUM_VOTERS".to_owned(),
            format!("{}@localhost:9094", apache::DEFAULT_BROKER_ID),
        );

        match security_mode {
            SecurityMode::Plaintext => {
                // Should not be used -- use the standard Kafka image for PLAINTEXT.
                // But handle it defensively.
                env_vars.insert(
                    "KAFKA_LISTENERS".to_owned(),
                    "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094".to_owned(),
                );
                env_vars.insert(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                    "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
                );
            },
            SecurityMode::SaslPlaintext { username, password } => {
                exposed_ports.push(SASL_PLAINTEXT_PORT);
                env_vars.insert(
                    "KAFKA_LISTENERS".to_owned(),
                    "PLAINTEXT://0.0.0.0:9092,SASL_PLAINTEXT://0.0.0.0:9095,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094".to_owned(),
                );
                env_vars.insert(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                    "PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"
                        .to_owned(),
                );
                env_vars.insert("KAFKA_SASL_ENABLED_MECHANISMS".to_owned(), "PLAIN".to_owned());
                env_vars.insert(
                    "KAFKA_OPTS".to_owned(),
                    format!("-Djava.security.auth.login.config={JAAS_CONFIG_PATH}"),
                );
                // Create JAAS config file
                let jaas_content = format!(
                    "KafkaServer {{\n    \
                     org.apache.kafka.common.security.plain.PlainLoginModule required\n    \
                     username=\"{username}\"\n    \
                     password=\"{password}\"\n    \
                     user_{username}=\"{password}\";\n\
                     }};\n"
                );
                copy_to_sources.push(CopyToContainer::new(jaas_content.into_bytes(), JAAS_CONFIG_PATH));
            },
            SecurityMode::Ssl => {
                exposed_ports.push(SSL_PORT);
                let certs = test_certs::generate_test_certificates("localhost");
                env_vars.insert(
                    "KAFKA_LISTENERS".to_owned(),
                    "PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9096,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
                        .to_owned(),
                );
                env_vars.insert(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                    "PLAINTEXT:PLAINTEXT,SSL:SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
                );
                Self::add_ssl_env_vars(&mut env_vars, &certs);
            },
            SecurityMode::SaslSsl { username, password } => {
                exposed_ports.push(SASL_SSL_PORT);
                let certs = test_certs::generate_test_certificates("localhost");
                env_vars.insert(
                    "KAFKA_LISTENERS".to_owned(),
                    "PLAINTEXT://0.0.0.0:9092,SASL_SSL://0.0.0.0:9097,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"
                        .to_owned(),
                );
                env_vars.insert(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                    "PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
                );
                env_vars.insert("KAFKA_SASL_ENABLED_MECHANISMS".to_owned(), "PLAIN".to_owned());
                env_vars.insert(
                    "KAFKA_OPTS".to_owned(),
                    format!("-Djava.security.auth.login.config={JAAS_CONFIG_PATH}"),
                );
                Self::add_ssl_env_vars(&mut env_vars, &certs);
                // Create JAAS config file
                let jaas_content = format!(
                    "KafkaServer {{\n    \
                     org.apache.kafka.common.security.plain.PlainLoginModule required\n    \
                     username=\"{username}\"\n    \
                     password=\"{password}\"\n    \
                     user_{username}=\"{password}\";\n\
                     }};\n"
                );
                copy_to_sources.push(CopyToContainer::new(jaas_content.into_bytes(), JAAS_CONFIG_PATH));
            },
        }

        Self { env_vars, copy_to_sources, exposed_ports, security_mode: security_mode.clone() }
    }

    /// Add SSL-related environment variables to the env_vars map.
    fn add_ssl_env_vars(env_vars: &mut HashMap<String, String>, certs: &test_certs::TestCertificates) {
        env_vars.insert("KAFKA_SSL_KEYSTORE_TYPE".to_owned(), "PEM".to_owned());
        env_vars.insert("KAFKA_SSL_KEYSTORE_KEY".to_owned(), certs.broker_key_pem.clone());
        env_vars.insert("KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN".to_owned(), certs.broker_cert_pem.clone());
        env_vars.insert("KAFKA_SSL_TRUSTSTORE_TYPE".to_owned(), "PEM".to_owned());
        env_vars.insert("KAFKA_SSL_TRUSTSTORE_CERTIFICATES".to_owned(), certs.ca_cert_pem.clone());
    }

    /// Returns the container port for the secure listener.
    fn secure_port(&self) -> Option<ContainerPort> {
        match &self.security_mode {
            SecurityMode::Plaintext => None,
            SecurityMode::SaslPlaintext { .. } => Some(SASL_PLAINTEXT_PORT),
            SecurityMode::Ssl => Some(SSL_PORT),
            SecurityMode::SaslSsl { .. } => Some(SASL_SSL_PORT),
        }
    }

    /// Returns the CA certificate PEM if SSL is configured.
    fn ca_cert_pem(&self) -> Option<String> {
        self.env_vars.get("KAFKA_SSL_TRUSTSTORE_CERTIFICATES").cloned()
    }

    /// Returns the listener name for advertised listeners configuration.
    fn secure_listener_name(&self) -> Option<&str> {
        match &self.security_mode {
            SecurityMode::Plaintext => None,
            SecurityMode::SaslPlaintext { .. } => Some("SASL_PLAINTEXT"),
            SecurityMode::Ssl => Some("SSL"),
            SecurityMode::SaslSsl { .. } => Some("SASL_SSL"),
        }
    }
}

impl Image for SecureKafka {
    fn name(&self) -> &str {
        // Use JVM image, not native -- SASL requires full JVM runtime
        "apache/kafka"
    }

    fn tag(&self) -> &str {
        KAFKA_TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // Same pattern as the standard Kafka image: container will be started
        // with a custom entrypoint that waits for the start script to be created
        // in `exec_after_start`.
        vec![]
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn env_vars(&self) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!("while [ ! -f {START_SCRIPT} ]; do sleep 0.1; done; chmod 755 {START_SCRIPT} && {START_SCRIPT}"),
        ]
        .into_iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &self.exposed_ports
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        self.copy_to_sources.iter()
    }

    fn exec_after_start(&self, cs: ContainerState) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let plaintext_host_port = cs.host_port_ipv4(PLAINTEXT_PORT)?;
        let mut advertised_listeners = format!("PLAINTEXT://127.0.0.1:{plaintext_host_port},BROKER://localhost:9093");

        // Add secure listener if configured
        if let (Some(secure_port), Some(listener_name)) = (self.secure_port(), self.secure_listener_name()) {
            let secure_host_port = cs.host_port_ipv4(secure_port)?;
            advertised_listeners.push_str(&format!(",{listener_name}://127.0.0.1:{secure_host_port}"));
        }

        let script = format!(
            "#!/usr/bin/env bash\nexport KAFKA_ADVERTISED_LISTENERS={advertised_listeners}\n/etc/kafka/docker/run\n"
        );

        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("echo '{script}' > {START_SCRIPT}"),
        ];

        let ready_conditions = vec![WaitFor::message_on_stdout("Kafka Server started")];
        let exec = ExecCommand::new(cmd).with_container_ready_conditions(ready_conditions);

        Ok(vec![exec])
    }
}

/// Internal enum to hold either a standard or secure Kafka container.
enum KafkaContainer {
    Standard(ContainerAsync<Kafka>),
    Secure(ContainerAsync<SecureKafka>),
}

impl KafkaContainer {
    fn id(&self) -> &str {
        match self {
            KafkaContainer::Standard(c) => c.id(),
            KafkaContainer::Secure(c) => c.id(),
        }
    }
}

/// Manages a real Kafka broker in Docker for integration tests.
///
/// Shared across tests with the same [`ClusterConfig`].
pub struct KafkaCluster {
    /// The running container handle. Kept alive for the duration of the pool entry.
    _container: KafkaContainer,
    /// The Docker container ID, used for cleanup at process exit.
    container_id: String,
    /// The `host:port` connection string for this cluster (PLAINTEXT listener).
    bootstrap_servers: String,
    /// Bootstrap servers for the secure listener (SASL/SSL port).
    secure_bootstrap_servers: Option<String>,
    /// CA certificate PEM for SSL tests.
    ca_cert_pem: Option<String>,
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
        match &config.security_mode {
            SecurityMode::Plaintext => Self::start_plaintext(config).await,
            _ => Self::start_secure(config).await,
        }
    }

    /// Start a plaintext-only cluster using the standard Kafka image.
    async fn start_plaintext(config: &ClusterConfig) -> Self {
        let mut request: testcontainers::ContainerRequest<Kafka> =
            testcontainers::ContainerRequest::from(Kafka::default()).with_tag(KAFKA_TAG);

        for (key, value) in &config.server_properties {
            request = request.with_env_var(key, value);
        }

        let container = request.start().await.expect("Failed to start Kafka container");
        let container_id = container.id().to_string();

        let host_port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Failed to get Kafka host port");

        let bootstrap_servers = format!("127.0.0.1:{host_port}");

        Self {
            _container: KafkaContainer::Standard(container),
            container_id,
            bootstrap_servers,
            secure_bootstrap_servers: None,
            ca_cert_pem: None,
            config: config.clone(),
        }
    }

    /// Start a secure cluster using the custom SecureKafka image.
    async fn start_secure(config: &ClusterConfig) -> Self {
        let secure_kafka = SecureKafka::new(&config.security_mode);
        let secure_port = secure_kafka.secure_port();
        let ca_cert_pem = secure_kafka.ca_cert_pem();

        let mut request = testcontainers::ContainerRequest::from(secure_kafka);

        for (key, value) in &config.server_properties {
            request = request.with_env_var(key, value);
        }

        let container = request.start().await.expect("Failed to start secure Kafka container");
        let container_id = container.id().to_string();

        let plaintext_host_port = container
            .get_host_port_ipv4(PLAINTEXT_PORT)
            .await
            .expect("Failed to get PLAINTEXT host port");

        let bootstrap_servers = format!("127.0.0.1:{plaintext_host_port}");

        let secure_bootstrap_servers = if let Some(port) = secure_port {
            let host_port = container
                .get_host_port_ipv4(port)
                .await
                .expect("Failed to get secure host port");
            Some(format!("127.0.0.1:{host_port}"))
        } else {
            None
        };

        Self {
            _container: KafkaContainer::Secure(container),
            container_id,
            bootstrap_servers,
            secure_bootstrap_servers,
            ca_cert_pem,
            config: config.clone(),
        }
    }

    /// Bootstrap servers connection string for the PLAINTEXT listener
    /// (e.g., `"127.0.0.1:32781"`).
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Bootstrap servers for the secure listener (SASL/SSL port).
    /// Returns `None` for PLAINTEXT clusters.
    pub fn secure_bootstrap_servers(&self) -> Option<&str> {
        self.secure_bootstrap_servers.as_deref()
    }

    /// CA certificate PEM for SSL tests.
    /// Returns `None` for non-SSL clusters.
    pub fn ca_cert_pem(&self) -> Option<&str> {
        self.ca_cert_pem.as_deref()
    }

    /// The config this cluster was started with.
    #[allow(dead_code)]
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// The Docker container ID.
    pub fn container_id(&self) -> &str {
        &self.container_id
    }
}
