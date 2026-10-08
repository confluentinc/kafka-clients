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

//! Configuration for the `KafkaAdminClient`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`.

use std::collections::HashMap;

use crate::common::Error;
use crate::common::config::config_def::ValidList;
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::security::auth::SecurityProtocol;
use crate::{ClientDnsLookup, CommonClientConfigs};

/// Configuration for the admin client.
///
/// Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`. Unknown
/// keys are accepted silently, matching Java's `AbstractConfig` behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig")]
pub struct AdminClientConfig {
    bootstrap_servers: Vec<String>,
    client_dns_lookup: ClientDnsLookup,
    client_id: String,
    request_timeout_ms: i32,
    default_api_timeout_ms: i32,
    retries: i32,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    bootstrap_resolve_timeout_ms: i64,
    reconnect_backoff_ms: i64,
    reconnect_backoff_max_ms: i64,
    connections_max_idle_ms: i64,
    metadata_max_age_ms: i64,
    socket_connection_setup_timeout_ms: i64,
    metadata_cluster_check_enable: bool,

    // --- Security ---
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    sasl_config: SaslConfigs,

    /// SSL/TLS configuration.
    ssl_config: SslConfigs,
}

impl AdminClientConfig {
    /// `bootstrap.servers`
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str = "bootstrap.servers";
    /// `bootstrap.controllers` (KIP-919). Accepted and validated as in Java,
    /// but connecting through controllers is not implemented, so setting it
    /// makes [`Self::new`] fail (see there).
    pub const BOOTSTRAP_CONTROLLERS_CONFIG: &'static str = "bootstrap.controllers";
    /// Config key: `client.dns.lookup`. Java's `AdminClientConfig.java` declares it as its
    /// own public alias of `CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG`.
    pub const CLIENT_DNS_LOOKUP_CONFIG: &'static str = CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG;
    /// `client.id`
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";
    /// `request.timeout.ms`
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";
    /// `default.api.timeout.ms`
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str = "default.api.timeout.ms";
    /// `retries`
    pub const RETRIES_CONFIG: &'static str = "retries";
    /// `retry.backoff.ms`
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = "retry.backoff.ms";
    /// `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = "retry.backoff.max.ms";
    /// `bootstrap.resolve.timeout.ms` (KIP-909). See
    /// [`Self::bootstrap_resolve_timeout_ms`].
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG;
    /// `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";
    /// `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";
    /// `connections.max.idle.ms`
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";
    /// `metadata.max.age.ms`
    pub const METADATA_MAX_AGE_MS_CONFIG: &'static str = "metadata.max.age.ms";
    /// `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str = "socket.connection.setup.timeout.ms";
    /// `metadata.cluster.check.enable` (KIP-1242). Java's `AdminClientConfig.java:157`
    /// declares it as its own public alias of
    /// [`CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG`], whose doc
    /// describes it. See [`Self::metadata_cluster_check_enable`].
    pub const METADATA_CLUSTER_CHECK_ENABLE_CONFIG: &'static str =
        CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG;
    /// `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &'static str = CommonClientConfigs::SECURITY_PROTOCOL_CONFIG;
    /// `sasl.mechanism`
    pub const SASL_MECHANISM_CONFIG: &'static str = SaslConfigs::SASL_MECHANISM;
    /// `sasl.jaas.config`
    pub const SASL_JAAS_CONFIG: &'static str = SaslConfigs::SASL_JAAS_CONFIG;

    /// Creates a config from a property map. `bootstrap.servers` must be non-empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if `bootstrap.servers` is missing or empty, and
    /// an error if a value fails to parse or validate. Following Java's
    /// `KafkaAdminClient.determineBootstrapType`, setting both `bootstrap.servers`
    /// and `bootstrap.controllers` is an [`Error::Config`]. Setting only
    /// `bootstrap.controllers` returns an `UNSUPPORTED_VERSION` error
    /// ([`Error::unsupported_version`]): Java
    /// accepts it (KIP-919), but this client does not implement bootstrapping
    /// through controllers, and silently ignoring the key would leave the
    /// client with no bootstrap address at all.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)")]
    pub fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
        let mut config = Self::default();
        let mut bootstrap_controllers: Vec<String> = Vec::new();

        for (key, value) in props {
            match key.as_str() {
                Self::BOOTSTRAP_SERVERS_CONFIG => {
                    // `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:159`).
                    config.bootstrap_servers = ValidList::parse_any_non_duplicate_values(key, value, true)?;
                },
                Self::BOOTSTRAP_CONTROLLERS_CONFIG => {
                    // `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:162-167`).
                    bootstrap_controllers = ValidList::parse_any_non_duplicate_values(key, value, true)?;
                },
                Self::CLIENT_DNS_LOOKUP_CONFIG => {
                    config.client_dns_lookup = ClientDnsLookup::parse_config_value(value)?;
                },
                // `ConfigDef.parseType` trims every `Type.STRING` value (`ConfigDef.java:729-731`).
                Self::CLIENT_ID_CONFIG => config.client_id = value.trim().to_string(),
                Self::REQUEST_TIMEOUT_MS_CONFIG => config.request_timeout_ms = parse_i32(key, value)?,
                Self::DEFAULT_API_TIMEOUT_MS_CONFIG => config.default_api_timeout_ms = parse_i32(key, value)?,
                Self::RETRIES_CONFIG => config.retries = parse_i32(key, value)?,
                Self::RETRY_BACKOFF_MS_CONFIG => config.retry_backoff_ms = parse_i64(key, value)?,
                Self::RETRY_BACKOFF_MAX_MS_CONFIG => config.retry_backoff_max_ms = parse_i64(key, value)?,
                Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:207-212`, KAFKA-20939).
                    let v = parse_i64(key, value)?;
                    if v < 0 {
                        return Err(Error::config_name_value_message(key, v, "Value must be at least 0"));
                    }
                    config.bootstrap_resolve_timeout_ms = v;
                },
                Self::RECONNECT_BACKOFF_MS_CONFIG => config.reconnect_backoff_ms = parse_i64(key, value)?,
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG => config.reconnect_backoff_max_ms = parse_i64(key, value)?,
                Self::CONNECTIONS_MAX_IDLE_MS_CONFIG => config.connections_max_idle_ms = parse_i64(key, value)?,
                Self::METADATA_MAX_AGE_MS_CONFIG => config.metadata_max_age_ms = parse_i64(key, value)?,
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG => {
                    config.socket_connection_setup_timeout_ms = parse_i64(key, value)?;
                },
                Self::METADATA_CLUSTER_CHECK_ENABLE_CONFIG => {
                    // `Type.BOOLEAN`, default `true` (`AdminClientConfig.java:305-309`).
                    config.metadata_cluster_check_enable = parse_bool(key, value)?;
                },
                Self::SECURITY_PROTOCOL_CONFIG => {
                    config.security_protocol = SecurityProtocol::for_name(value).ok_or_else(|| {
                        Error::config_name_value_message(
                            key,
                            value,
                            format!("Valid values are: {:?}", SecurityProtocol::names()),
                        )
                    })?;
                },
                Self::SASL_MECHANISM_CONFIG => {
                    config.sasl_config.mechanism = value.to_string();
                },
                Self::SASL_JAAS_CONFIG => {
                    config.sasl_config.jaas_config = if value.is_empty() {
                        None
                    } else {
                        Some(value.to_string())
                    };
                },
                key if key.starts_with("ssl.") => {
                    SslConfigs::apply_ssl_config_key(&mut config.ssl_config, key, value)?;
                },
                // Unknown keys are accepted silently, as in Java.
                _ => {},
            }
        }

        // `KafkaAdminClient.determineBootstrapType` (KIP-909 moved it there from
        // the deleted `AdminBootstrapAddresses.fromConfig`). Java runs it in
        // `createInternal`; this crate has always run the check here, when the
        // config is built, because the config does not keep
        // `bootstrap.controllers` (no client can use it, see below).
        if crate::admin::KafkaAdminClient::determine_bootstrap_type(&config.bootstrap_servers, &bootstrap_controllers)?
        {
            // Java bootstraps through the controllers here
            // (`usingBootstrapControllers = true`); that path is not
            // implemented, so fail explicitly rather than drop the key.
            return Err(Error::unsupported_version(format!(
                "{} is not supported by this client; set {} instead",
                Self::BOOTSTRAP_CONTROLLERS_CONFIG,
                Self::BOOTSTRAP_SERVERS_CONFIG
            )));
        }
        config
            .client_dns_lookup
            .warn_if_tls_hostname_verification_affected(config.security_protocol, &config.ssl_config);
        Ok(config)
    }

    /// The `bootstrap.servers` list.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// `client.dns.lookup`.
    pub(crate) fn client_dns_lookup(&self) -> ClientDnsLookup {
        self.client_dns_lookup
    }

    /// The `client.id`.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `request.timeout.ms`.
    pub fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }

    /// `default.api.timeout.ms`.
    pub fn default_api_timeout_ms(&self) -> i32 {
        self.default_api_timeout_ms
    }

    /// `retries`.
    pub fn retries(&self) -> i32 {
        self.retries
    }

    /// `retry.backoff.ms`.
    pub fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }

    /// `retry.backoff.max.ms`.
    pub fn retry_backoff_max_ms(&self) -> i64 {
        self.retry_backoff_max_ms
    }

    /// `bootstrap.resolve.timeout.ms` (KIP-909): selects the client's bootstrap
    /// DNS resolution mode. `0` (the default) resolves `bootstrap.servers`
    /// synchronously when the client is created, and a resolution failure fails
    /// creation with a config error. A positive value resolves asynchronously,
    /// retrying for at most this long before every subsequent call fails with an
    /// unrecoverable
    /// [`BootstrapResolutionError`](crate::common::errors::BootstrapResolutionError);
    /// the client must then be closed and re-created. Setting a positive value
    /// enables an evolving feature whose compatibility may be broken in a minor
    /// release.
    pub fn bootstrap_resolve_timeout_ms(&self) -> i64 {
        self.bootstrap_resolve_timeout_ms
    }

    /// `reconnect.backoff.ms`.
    pub fn reconnect_backoff_ms(&self) -> i64 {
        self.reconnect_backoff_ms
    }

    /// `reconnect.backoff.max.ms`.
    pub fn reconnect_backoff_max_ms(&self) -> i64 {
        self.reconnect_backoff_max_ms
    }

    /// `connections.max.idle.ms`.
    pub fn connections_max_idle_ms(&self) -> i64 {
        self.connections_max_idle_ms
    }

    /// `metadata.max.age.ms`.
    pub fn metadata_max_age_ms(&self) -> i64 {
        self.metadata_max_age_ms
    }

    /// `socket.connection.setup.timeout.ms`.
    pub fn socket_connection_setup_timeout_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_ms
    }

    /// `metadata.cluster.check.enable` (KIP-1242): whether the client sends the
    /// cluster id and node id it expects in ApiVersions so the broker can detect
    /// a misrouted connection.
    ///
    /// It is passed to the `NetworkClient` as in Java
    /// (`ClientUtils.createNetworkClient`), but, also as in Java, it has no
    /// effect on the admin client: the admin's metadata updater does not
    /// override `MetadataUpdater.clusterId()` (`AdminMetadataManager.java`), so
    /// the expected cluster id is never known. This client additionally runs
    /// the admin `NetworkClient` with `metadata.recovery.strategy=none`, under
    /// which the check is skipped.
    pub(crate) fn metadata_cluster_check_enable(&self) -> bool {
        self.metadata_cluster_check_enable
    }

    /// `security.protocol`.
    pub fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub fn sasl_config(&self) -> &SaslConfigs {
        &self.sasl_config
    }

    /// SSL/TLS configuration.
    pub fn ssl_config(&self) -> &SslConfigs {
        &self.ssl_config
    }
}

impl Default for AdminClientConfig {
    /// Defaults match `AdminClientConfig`'s `ConfigDef` (Apache Kafka 4.2).
    fn default() -> Self {
        Self {
            bootstrap_servers: Vec::new(),
            client_dns_lookup: ClientDnsLookup::UseAllDnsIps,
            client_id: String::new(),
            request_timeout_ms: 30_000,
            default_api_timeout_ms: 60_000,
            retries: i32::MAX,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1_000,
            bootstrap_resolve_timeout_ms: CommonClientConfigs::DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS,
            reconnect_backoff_ms: 50,
            reconnect_backoff_max_ms: 1_000,
            connections_max_idle_ms: 300_000,
            metadata_max_age_ms: 300_000,
            socket_connection_setup_timeout_ms: 10_000,
            metadata_cluster_check_enable: true,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_config: SaslConfigs::default(),
            ssl_config: SslConfigs::default(),
        }
    }
}

fn parse_i32(key: &str, value: &str) -> Result<i32, Error> {
    value.trim().parse::<i32>().map_err(|_| Error::config_name_value(key, value))
}

fn parse_i64(key: &str, value: &str) -> Result<i64, Error> {
    value.trim().parse::<i64>().map_err(|_| Error::config_name_value(key, value))
}

fn parse_bool(key: &str, value: &str) -> Result<bool, Error> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error::config_name_value(key, value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_required_bootstrap() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092, b:9092".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.bootstrap_servers(), &["a:9092".to_string(), "b:9092".to_string()]);
        assert_eq!(config.request_timeout_ms(), 30_000);
        assert_eq!(config.default_api_timeout_ms(), 60_000);
        assert_eq!(config.retries(), i32::MAX);
        assert_eq!(config.security_protocol(), SecurityProtocol::Plaintext);
    }

    /// `bootstrap.resolve.timeout.ms` (KIP-909): `Type.LONG`, default `0`
    /// (synchronous resolution), `atLeast(0L)` since KAFKA-20939.
    #[test]
    fn bootstrap_resolve_timeout_ms() {
        assert_eq!(
            AdminClientConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            "bootstrap.resolve.timeout.ms"
        );
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        assert_eq!(AdminClientConfig::new(&props).unwrap().bootstrap_resolve_timeout_ms(), 0);

        props.insert("bootstrap.resolve.timeout.ms".to_string(), "3000".to_string());
        assert_eq!(AdminClientConfig::new(&props).unwrap().bootstrap_resolve_timeout_ms(), 3000);

        props.insert("bootstrap.resolve.timeout.ms".to_string(), "-1".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "Invalid value -1 for configuration bootstrap.resolve.timeout.ms: Value must be at least 0"
        );
    }

    /// `metadata.cluster.check.enable` (KIP-1242): `Type.BOOLEAN`, default
    /// `true` (`AdminClientConfig.java:305-309`).
    #[test]
    fn metadata_cluster_check_enable() {
        assert_eq!(
            AdminClientConfig::METADATA_CLUSTER_CHECK_ENABLE_CONFIG,
            "metadata.cluster.check.enable"
        );
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        assert!(AdminClientConfig::new(&props).unwrap().metadata_cluster_check_enable());

        props.insert("metadata.cluster.check.enable".to_string(), "false".to_string());
        assert!(!AdminClientConfig::new(&props).unwrap().metadata_cluster_check_enable());

        props.insert("metadata.cluster.check.enable".to_string(), "maybe".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "Invalid value maybe for configuration metadata.cluster.check.enable"
        );
    }

    #[test]
    fn missing_bootstrap_is_error_with_exact_message() {
        let props = HashMap::new();
        let err = AdminClientConfig::new(&props).unwrap_err();
        assert_eq!(err.message(), "You must set either bootstrap.servers or bootstrap.controllers");
    }

    /// `KafkaAdminClient.determineBootstrapType`'s branches for
    /// `bootstrap.controllers`: only controllers is an explicit unsupported
    /// error (never silently dropped), both is Java's `ConfigException`, and
    /// an empty controllers list counts as unset.
    #[test]
    fn bootstrap_controllers() {
        let props = HashMap::from([("bootstrap.controllers".to_string(), "c:9093".to_string())]);
        let error = AdminClientConfig::new(&props).unwrap_err();
        assert_eq!(error.error(), crate::common::protocol::Errors::UnsupportedVersion, "{error:?}");
        assert_eq!(
            error.message(),
            "bootstrap.controllers is not supported by this client; set bootstrap.servers instead"
        );

        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "a:9092".to_string()),
            ("bootstrap.controllers".to_string(), "c:9093".to_string()),
        ]);
        match AdminClientConfig::new(&props) {
            Err(Error::Config(e)) => {
                assert_eq!(e.message(), "You cannot set both bootstrap.servers and bootstrap.controllers")
            },
            other => panic!("Expected Config, got: {other:?}"),
        }

        let props = HashMap::from([("bootstrap.controllers".to_string(), " ".to_string())]);
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "You must set either bootstrap.servers or bootstrap.controllers"
        );
        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "a:9092".to_string()),
            ("bootstrap.controllers".to_string(), String::new()),
        ]);
        assert_eq!(
            AdminClientConfig::new(&props).unwrap().bootstrap_servers(),
            ["a:9092".to_string()]
        );
    }

    #[test]
    fn overrides_and_unknown_keys() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        props.insert("client.id".to_string(), "admin-1".to_string());
        props.insert("request.timeout.ms".to_string(), "5000".to_string());
        props.insert("some.unknown.key".to_string(), "ignored".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.client_id(), "admin-1");
        assert_eq!(config.request_timeout_ms(), 5000);
    }

    /// `client.dns.lookup` defaults to `use_all_dns_ips` and parses into the
    /// typed [`ClientDnsLookup`], as `AdminClientConfig`'s `ConfigDef` defines it.
    #[test]
    fn test_client_dns_lookup() {
        assert_eq!(AdminClientConfig::CLIENT_DNS_LOOKUP_CONFIG, "client.dns.lookup");
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap().client_dns_lookup(),
            ClientDnsLookup::UseAllDnsIps
        );

        props.insert(
            CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(),
            "resolve_canonical_bootstrap_servers_only".to_string(),
        );
        assert_eq!(
            AdminClientConfig::new(&props).unwrap().client_dns_lookup(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
        );

        props.insert(CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(), "default".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "Invalid value default for configuration client.dns.lookup: String must be one of: \
             use_all_dns_ips, resolve_canonical_bootstrap_servers_only"
        );
    }

    #[test]
    fn invalid_numeric_value_is_error() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "a:9092".to_string());
        props.insert("request.timeout.ms".to_string(), "not-a-number".to_string());
        assert!(AdminClientConfig::new(&props).is_err());
    }

    /// Translated from `ProducerConfigTest.testInvalidSecurityProtocol`, adapted
    /// to `AdminClientConfig`.
    #[test]
    fn test_invalid_security_protocol() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("security.protocol".to_string(), "abc".to_string());
        let err = AdminClientConfig::new(&props).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("security.protocol"),
            "Error message should contain config key, got: {}",
            msg
        );
    }

    /// Translated from `ProducerConfigTest.testCaseInsensitiveSecurityProtocol`,
    /// adapted to `AdminClientConfig`.
    #[test]
    fn test_case_insensitive_security_protocol() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("security.protocol".to_string(), "sasl_ssl".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.security_protocol(), SecurityProtocol::SaslSsl);
    }

    #[test]
    fn test_sasl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";"
                .to_string(),
        );
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.sasl_config().mechanism, "PLAIN");
        assert_eq!(config.sasl_config().resolve_username(), Some("alice"));
        assert_eq!(config.sasl_config().resolve_password(), Some("secret"));
    }

    #[test]
    fn test_ssl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("ssl.truststore.location".to_string(), "/path/to/truststore.pem".to_string());
        props.insert("ssl.keystore.location".to_string(), "/path/to/keystore.pem".to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(
            config.ssl_config().truststore_location.as_deref(),
            Some("/path/to/truststore.pem")
        );
        assert_eq!(config.ssl_config().keystore_location.as_deref(), Some("/path/to/keystore.pem"));
        assert_eq!(config.ssl_config().endpoint_identification_algorithm, "");
    }

    /// `bootstrap.servers` is a `Type.LIST`: `ConfigDef.parseType` trims the
    /// value and splits it on `\\s*,\\s*`, so whitespace around the commas
    /// and at the ends never reaches `ClientUtils.parseAndValidateAddresses`
    /// (which does not trim, and rejects it).
    #[test]
    fn test_bootstrap_servers_list_parsing() {
        for value in [
            "localhost:1,localhost:2",
            "localhost:1, localhost:2",
            " localhost:1 ,localhost:2 ",
        ] {
            let props = HashMap::from([("bootstrap.servers".to_string(), value.to_string())]);
            let config = AdminClientConfig::new(&props).unwrap();
            assert_eq!(
                config.bootstrap_servers(),
                ["localhost:1".to_string(), "localhost:2".to_string()],
                "{value:?}"
            );
            let addresses = crate::ClientUtils::parse_and_validate_addresses(
                config.bootstrap_servers(),
                config.client_dns_lookup(),
            )
            .unwrap();
            assert_eq!(addresses.len(), 2, "{value:?}");
        }
    }

    /// `bootstrap.servers` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:159`): an empty
    /// element is rejected with `ConfigDef`'s exact message and duplicates are removed
    /// (single-message `ConfigException`, no `Invalid value` prefix).
    #[test]
    fn test_bootstrap_servers_valid_list() {
        let error_message = |value: &str| {
            let props = HashMap::from([("bootstrap.servers".to_string(), value.to_string())]);
            match AdminClientConfig::new(&props) {
                Err(Error::Config(e)) => e.message().to_string(),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        };
        for value in ["localhost:9092,,localhost:9093", "a:1, ,b:1", "a:1,"] {
            assert_eq!(
                error_message(value),
                "Configuration 'bootstrap.servers' values must not be empty.",
                "{value:?}"
            );
        }
        // `ConfigDef.parseValue` removes duplicates (with a warning) before validating.
        let props = HashMap::from([("bootstrap.servers".to_string(), "a:1,a:1".to_string())]);
        assert_eq!(AdminClientConfig::new(&props).unwrap().bootstrap_servers(), ["a:1".to_string()]);
        assert_eq!(
            error_message(",,"),
            "Configuration 'bootstrap.servers' values must not be empty."
        );
        // Admin allows an empty list (`isEmptyAllowed = true`), so the
        // ValidList message never appears; `KafkaAdminClient.determineBootstrapType`'s
        // check reports it instead.
        for value in ["", "  "] {
            assert_eq!(
                error_message(value),
                "You must set either bootstrap.servers or bootstrap.controllers",
                "{value:?}"
            );
        }
        let props = HashMap::from([("bootstrap.servers".to_string(), "a:1,b:1".to_string())]);
        assert_eq!(
            AdminClientConfig::new(&props).unwrap().bootstrap_servers(),
            ["a:1".to_string(), "b:1".to_string()]
        );
    }
}
