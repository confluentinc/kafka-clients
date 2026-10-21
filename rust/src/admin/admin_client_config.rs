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
//! Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`: the keys
//! are defined on a `ConfigDef` in Java's order (`AdminClientConfig.java:166-319`)
//! and parsed and validated by it, and the configuration answers Java's
//! `AbstractConfig` getters.
//!
//! Not defined, and so reported by [`AdminClientConfig::unused`] and the
//! client's [`AdminClientConfig::log_unused`] line when set:
//!
//! - `metric.reporters`, a list of classes: a configuration never takes a
//!   class (CLAUDE.md §2).
//! - `security.providers`: Java's JCA security providers have no counterpart.
//! - `config.providers`: config providers are not translated, so the key is
//!   left undefined and a value set for it is reported as unused, rather than
//!   accepted and ignored.
//!
//! `AbstractConfig`'s `getPassword`, `typeOf`, `originals*()` and `values*()`
//! are not translated yet: each needs a public type the crate does not have
//! (`Password`, `ConfigDef.Type`, Java's `RecordingMap`). See `CHANGELOG.md`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;

use crate::common::Error;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap, ValuesReader, string_originals};
use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, Importance, Range, Type, ValidList, ValidString,
};
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::security::auth::SecurityProtocol;
use crate::{ClientDnsLookup, CommonClientConfigs};

/// Java's `AdminClientConfig.CONFIG`, built in its static initializer.
static CONFIG: LazyLock<ConfigDef> = LazyLock::new(|| {
    ConfigDef::new()
        .define_with_validator(
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG,
            Type::List,
            Vec::<String>::new(),
            ValidList::any_non_duplicate_values(true, false),
            Importance::High,
        )
        .define_with_validator(
            AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG,
            Type::List,
            Vec::<String>::new(),
            ValidList::any_non_duplicate_values(true, false),
            Importance::High,
        )
        .define(AdminClientConfig::CLIENT_ID_CONFIG, Type::String, "", Importance::Medium)
        .define_with_validator(
            AdminClientConfig::METADATA_MAX_AGE_CONFIG,
            Type::Long,
            5 * 60 * 1000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            CommonClientConfigs::SEND_BUFFER_CONFIG,
            Type::Int,
            128 * 1024,
            Range::at_least(CommonClientConfigs::SEND_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            CommonClientConfigs::RECEIVE_BUFFER_CONFIG,
            Type::Int,
            64 * 1024,
            Range::at_least(CommonClientConfigs::RECEIVE_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            AdminClientConfig::RECONNECT_BACKOFF_MS_CONFIG,
            Type::Long,
            50_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::RECONNECT_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            1000_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::RETRY_BACKOFF_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::RETRY_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MAX_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS,
            Range::at_least(0_i64),
            Importance::High,
        )
        .define(
            CommonClientConfigs::ENABLE_METRICS_PUSH_CONFIG,
            Type::Boolean,
            false,
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::REQUEST_TIMEOUT_MS_CONFIG,
            Type::Int,
            30000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define(
            AdminClientConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
            Importance::Medium,
        )
        .define(
            CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
            Importance::Medium,
        )
        .define(
            AdminClientConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG,
            Type::Long,
            5 * 60 * 1000_i64,
            Importance::Medium,
        )
        .define_with_validator(
            AdminClientConfig::RETRIES_CONFIG,
            Type::Int,
            i32::MAX,
            Range::between(0, i32::MAX),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::DEFAULT_API_TIMEOUT_MS_CONFIG,
            Type::Int,
            60000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            CommonClientConfigs::METRICS_SAMPLE_WINDOW_MS_CONFIG,
            Type::Long,
            30000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            CommonClientConfigs::METRICS_NUM_SAMPLES_CONFIG,
            Type::Int,
            2,
            Range::at_least(1),
            Importance::Low,
        )
        // `metric.reporters` is a list of classes (CLAUDE.md §2): not defined.
        .define_with_validator(
            CommonClientConfigs::METRICS_RECORDING_LEVEL_CONFIG,
            Type::String,
            "INFO",
            ValidString::r#in(&["INFO", "DEBUG", "TRACE"]),
            Importance::Low,
        )
        .define_with_validator(
            AdminClientConfig::CLIENT_DNS_LOOKUP_CONFIG,
            Type::String,
            ClientDnsLookup::UseAllDnsIps.config_value(),
            ValidString::r#in(&[
                ClientDnsLookup::UseAllDnsIps.config_value(),
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.config_value(),
            ]),
            Importance::Medium,
        )
        // security support; `security.providers` (JCA providers) is not defined.
        .define_with_validator(
            AdminClientConfig::SECURITY_PROTOCOL_CONFIG,
            Type::String,
            CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL,
            CaseInsensitiveValidString::r#in(&SecurityProtocol::names()),
            Importance::Medium,
        )
        .with_client_ssl_support()
        .with_client_sasl_support()
        .define_with_validator(
            CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG,
            Type::String,
            CommonClientConfigs::DEFAULT_METADATA_RECOVERY_STRATEGY,
            CaseInsensitiveValidString::r#in(&["NONE", "REBOOTSTRAP"]),
            Importance::Low,
        )
        .define_with_validator(
            CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
            Range::at_least(0),
            Importance::Low,
        )
        .define(
            AdminClientConfig::METADATA_CLUSTER_CHECK_ENABLE_CONFIG,
            Type::Boolean,
            true,
            Importance::Low,
        )
    // Java ends the chain with `config.providers`
    // (`AdminClientConfig.java:310-315`); it is left undefined until config
    // providers are translated.
});

/// The AdminClient configuration class, which also contains constants for
/// configuration entry names.
///
/// Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`. Keys it
/// does not define are accepted, as Java accepts them; the client reports
/// them, with the defined keys it does not read, in one line when it is built
/// ([`Self::log_unused`]).
///
/// Equality and hashing are Java's `equals` / `hashCode`: two configurations
/// are equal when they were built from equal properties.
#[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig")]
pub struct AdminClientConfig {
    /// The originals, the parsed values and the keys used so far.
    config: AbstractConfig,
    /// The keys the typed fields below were read from, which the client marks
    /// used when it is built from this configuration (`ValuesReader`).
    read_keys: Vec<&'static str>,
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
    socket_connection_setup_timeout_max_ms: i64,
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

impl fmt::Debug for AdminClientConfig {
    /// The typed settings, whose secrets are `Password`s
    /// rendered as `[hidden]`, and the keys of the properties, never their
    /// values: they also hold keys this configuration does not define, whose
    /// values may be secrets, and Java never prints a raw original value.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructured exhaustively: a field added to the struct fails to
        // compile until it is bound here, and a bound but unrendered field is
        // an unused variable, which the lint rejects.
        let Self {
            config,
            read_keys: _,
            bootstrap_servers,
            client_dns_lookup,
            client_id,
            request_timeout_ms,
            default_api_timeout_ms,
            retries,
            retry_backoff_ms,
            retry_backoff_max_ms,
            bootstrap_resolve_timeout_ms,
            reconnect_backoff_ms,
            reconnect_backoff_max_ms,
            connections_max_idle_ms,
            metadata_max_age_ms,
            socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms,
            metadata_cluster_check_enable,
            security_protocol,
            sasl_config,
            ssl_config,
        } = self;
        let originals: BTreeSet<&str> = config.originals_map().keys().map(String::as_str).collect();
        f.debug_struct("AdminClientConfig")
            .field("bootstrap_servers", bootstrap_servers)
            .field("client_dns_lookup", client_dns_lookup)
            .field("client_id", client_id)
            .field("request_timeout_ms", request_timeout_ms)
            .field("default_api_timeout_ms", default_api_timeout_ms)
            .field("retries", retries)
            .field("retry_backoff_ms", retry_backoff_ms)
            .field("retry_backoff_max_ms", retry_backoff_max_ms)
            .field("bootstrap_resolve_timeout_ms", bootstrap_resolve_timeout_ms)
            .field("reconnect_backoff_ms", reconnect_backoff_ms)
            .field("reconnect_backoff_max_ms", reconnect_backoff_max_ms)
            .field("connections_max_idle_ms", connections_max_idle_ms)
            .field("metadata_max_age_ms", metadata_max_age_ms)
            .field("socket_connection_setup_timeout_ms", socket_connection_setup_timeout_ms)
            .field("socket_connection_setup_timeout_max_ms", socket_connection_setup_timeout_max_ms)
            .field("metadata_cluster_check_enable", metadata_cluster_check_enable)
            .field("security_protocol", security_protocol)
            .field("sasl_config", sasl_config)
            .field("ssl_config", ssl_config)
            .field("originals", &originals)
            .finish()
    }
}

impl PartialEq for AdminClientConfig {
    /// Java's `AbstractConfig.equals`: the originals are equal.
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl Eq for AdminClientConfig {}

impl Hash for AdminClientConfig {
    /// Java's `AbstractConfig.hashCode`, over the originals.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.config.hash(state);
    }
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
    /// `bootstrap.resolve.timeout.ms` (KIP-909): `0` (the default) resolves
    /// `bootstrap.servers` synchronously when the client is created; a
    /// positive value resolves asynchronously, retrying for at most this long.
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG;
    /// `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";
    /// `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";
    /// `connections.max.idle.ms`
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";
    /// `metadata.max.age.ms`
    pub const METADATA_MAX_AGE_CONFIG: &'static str = "metadata.max.age.ms";
    /// `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str = "socket.connection.setup.timeout.ms";
    /// `metadata.cluster.check.enable` (KIP-1242).
    ///
    /// Whether the client should send cluster and node information when
    /// connecting to a broker to enable it to check for a misrouted
    /// connection. This configuration is ignored if rebootstrapping is
    /// disabled by setting the configuration `metadata.recovery.strategy=none`.
    /// If the client is connecting to a broker older than Apache Kafka 4.4, no
    /// checking is performed and this configuration has no effect.
    ///
    /// Java's `AdminClientConfig.java:157` declares it as its own public alias
    /// of `CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_CONFIG`, whose doc
    /// (`CommonClientConfigs.java:268-271`) is the text above.
    pub const METADATA_CLUSTER_CHECK_ENABLE_CONFIG: &'static str =
        CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG;
    /// `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &'static str = CommonClientConfigs::SECURITY_PROTOCOL_CONFIG;

    /// Creates a config from a property map: Java's `AdminClientConfig(Map)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if a value fails to parse or validate, with
    /// `ConfigDef`'s message, or if a SASL `security.protocol` has an empty
    /// `sasl.mechanism`. Following Java's
    /// `KafkaAdminClient.determineBootstrapType`, setting neither or both of
    /// `bootstrap.servers` and `bootstrap.controllers` is an [`Error::Config`].
    /// Setting only `bootstrap.controllers` returns a
    /// [`LocalUnsupportedOperationError`](crate::common::LocalUnsupportedOperationError)
    /// ([`Error::local_unsupported_operation`]): Java
    /// accepts it (KIP-919), but this client does not implement bootstrapping
    /// through controllers, and silently ignoring the key would leave the
    /// client with no bootstrap address at all.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig#AdminClientConfig(Map)")]
    pub fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
        let config = Self::build(string_originals(props), true)?;
        config
            .client_dns_lookup
            .warn_if_tls_hostname_verification_affected(config.security_protocol, &config.ssl_config);
        Ok(config)
    }

    /// The configuration of one more client given this one: the same
    /// properties, with its own record of the keys used, as [`Self::new`]
    /// leaves it. It is not `Clone` (CLAUDE.md §3).
    ///
    /// Java's `Admin.create` builds one `AdminClientConfig` and hands it to
    /// one client, which logs nothing about it. The copy therefore logs none
    /// of the warnings [`Self::new`] logs: it makes the same parse and the
    /// same reads, marking the same keys used and failing the same way, but
    /// logs neither the parse's warning that a list's duplicate values are
    /// removed, nor the post-processing's warnings (the reconnect and retry
    /// backoff, the connection setup timeout), nor `new`'s TLS
    /// hostname-verification warning.
    ///
    /// Unlike the consumer's and producer's, an admin configuration derives
    /// no `client.id`: `KafkaAdminClient.createInternal` does, taking one
    /// number per client (`KafkaAdminClient.generateClientId`), so the copy
    /// leaves `client.id` as given.
    ///
    /// # Errors
    ///
    /// Fails as [`Self::new`] does; this configuration's properties already
    /// passed it, so it does not in practice.
    #[cfg_attr(
        not(any(feature = "ffi", test)),
        expect(dead_code, reason = "only the C config handle copies a configuration")
    )]
    pub(crate) fn copy_for_client(&self) -> Result<Self, Error> {
        Self::build(self.config.originals_map().clone(), false)
    }

    /// Java's `AdminClientConfig(Map)` without the TLS hostname-verification
    /// warning [`Self::new`] adds: parses `originals` and post-processes them,
    /// logging the parse's and the post-processing's warnings only when
    /// `log_warnings` is true.
    fn build(originals: ConfigMap, log_warnings: bool) -> Result<Self, Error> {
        let config = AbstractConfig::with_hooks_and_log_warnings(
            &CONFIG,
            originals,
            false,
            log_warnings,
            "AdminClientConfig",
            |originals| originals,
            // Java's `postProcessParsedConfig`.
            |config| {
                CommonClientConfigs::post_validate_sasl_mechanism_config(config)?;
                CommonClientConfigs::post_validate_supported_sasl_mechanism(config)?;
                CommonClientConfigs::warn_disabling_exponential_backoff(config, log_warnings)?;
                Ok(CommonClientConfigs::post_process_reconnect_backoff_configs(
                    config,
                    config.parsed_values(),
                    log_warnings,
                ))
            },
        )?;
        let mut values = ValuesReader::new(&config);
        let bootstrap_servers = values.list(Self::BOOTSTRAP_SERVERS_CONFIG)?.to_vec();
        let bootstrap_controllers = values.list(Self::BOOTSTRAP_CONTROLLERS_CONFIG)?;
        // `KafkaAdminClient.determineBootstrapType` (KIP-909 moved it there from
        // the deleted `AdminBootstrapAddresses.fromConfig`). Java runs it in
        // `createInternal`; this crate has always run the check here, when the
        // config is built, because no client can use `bootstrap.controllers`
        // (see below).
        if crate::admin::KafkaAdminClient::determine_bootstrap_type(&bootstrap_servers, bootstrap_controllers)? {
            // Java bootstraps through the controllers here
            // (`usingBootstrapControllers = true`); that path is not
            // implemented, so fail explicitly rather than drop the key.
            return Err(Error::local_unsupported_operation(format!(
                "{} is not supported by this client; set {} instead",
                Self::BOOTSTRAP_CONTROLLERS_CONFIG,
                Self::BOOTSTRAP_SERVERS_CONFIG
            )));
        }
        let client_dns_lookup = ClientDnsLookup::parse_config_value(values.string(Self::CLIENT_DNS_LOOKUP_CONFIG)?)?;
        let client_id = values.string(Self::CLIENT_ID_CONFIG)?.to_string();
        let request_timeout_ms = values.int(Self::REQUEST_TIMEOUT_MS_CONFIG)?;
        let default_api_timeout_ms = values.int(Self::DEFAULT_API_TIMEOUT_MS_CONFIG)?;
        let retries = values.int(Self::RETRIES_CONFIG)?;
        let retry_backoff_ms = values.long(Self::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = values.long(Self::RETRY_BACKOFF_MAX_MS_CONFIG)?;
        let bootstrap_resolve_timeout_ms = values.long(Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG)?;
        let reconnect_backoff_ms = values.long(Self::RECONNECT_BACKOFF_MS_CONFIG)?;
        let reconnect_backoff_max_ms = values.long(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)?;
        let connections_max_idle_ms = values.long(Self::CONNECTIONS_MAX_IDLE_MS_CONFIG)?;
        let metadata_max_age_ms = values.long(Self::METADATA_MAX_AGE_CONFIG)?;
        let socket_connection_setup_timeout_ms = values.long(Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let socket_connection_setup_timeout_max_ms =
            values.long(CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        let metadata_cluster_check_enable = values.boolean(Self::METADATA_CLUSTER_CHECK_ENABLE_CONFIG)?;
        let protocol = values.string(Self::SECURITY_PROTOCOL_CONFIG)?;
        // The validator accepted `protocol` case-insensitively.
        let security_protocol = SecurityProtocol::for_name(protocol)
            .ok_or_else(|| Error::local_illegal_state(format!("Unknown security protocol {protocol}")))?;

        // The `ssl.*` and `sasl.*` keys are read by Java's channel builder only
        // for a protocol that uses them (`ChannelBuilders.clientChannelBuilder`),
        // so only then are they marked used.
        let mut ssl_values = ValuesReader::new(&config);
        let ssl_config = SslConfigs::from_values(&mut ssl_values)?;
        if matches!(security_protocol, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl) {
            values.append(ssl_values);
        }
        let mut sasl_values = ValuesReader::new(&config);
        let sasl_config = SaslConfigs::from_values(&mut sasl_values)?;
        if matches!(security_protocol, SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl) {
            values.append(sasl_values);
        }
        let read_keys = values.into_read_keys();

        Ok(Self {
            config,
            read_keys,
            bootstrap_servers,
            client_dns_lookup,
            client_id,
            request_timeout_ms,
            default_api_timeout_ms,
            retries,
            retry_backoff_ms,
            retry_backoff_max_ms,
            bootstrap_resolve_timeout_ms,
            reconnect_backoff_ms,
            reconnect_backoff_max_ms,
            connections_max_idle_ms,
            metadata_max_age_ms,
            socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms,
            metadata_cluster_check_enable,
            security_protocol,
            sasl_config,
            ssl_config,
        })
    }

    /// The `SHORT` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] with Java's `Unknown configuration '<key>'` when
    ///   `key` is not a configuration this class defines.
    /// - [`Error::LocalIllegalArgument`] when `key` is a configuration of
    ///   another type, where Java's cast fails with a `ClassCastException`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getShort")]
    pub fn get_short(&self, key: &str) -> Result<Option<i16>, Error> {
        self.config.get_short(key)
    }

    /// The `INT` configuration `key`, `None` for Java's `null`. Reading a key
    /// marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getInt")]
    pub fn get_int(&self, key: &str) -> Result<Option<i32>, Error> {
        self.config.get_int(key)
    }

    /// The `LONG` configuration `key`, `None` for Java's `null`. Reading a key
    /// marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getLong")]
    pub fn get_long(&self, key: &str) -> Result<Option<i64>, Error> {
        self.config.get_long(key)
    }

    /// The `DOUBLE` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getDouble")]
    pub fn get_double(&self, key: &str) -> Result<Option<f64>, Error> {
        self.config.get_double(key)
    }

    /// The `LIST` configuration `key`, borrowed from the parsed values, `None`
    /// for Java's `null`. Reading a key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getList")]
    pub fn get_list(&self, key: &str) -> Result<Option<&[String]>, Error> {
        self.config.get_list(key)
    }

    /// The `BOOLEAN` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getBoolean")]
    pub fn get_boolean(&self, key: &str) -> Result<Option<bool>, Error> {
        self.config.get_boolean(key)
    }

    /// The `STRING` configuration `key`, trimmed, `None` for Java's `null`
    /// (also what an unset `PASSWORD` key such as `ssl.keystore.key` answers,
    /// as Java's cast of `null` succeeds). Reading a key marks it used
    /// ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`]; a set `PASSWORD` key is not a `STRING`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getString")]
    pub fn get_string(&self, key: &str) -> Result<Option<&str>, Error> {
        self.config.get_string(key)
    }

    /// The keys of the properties this configuration was built from that were
    /// never read: through a getter, or by the client built from it.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#unused")]
    pub fn unused(&self) -> HashSet<String> {
        self.config.unused()
    }

    /// Marks the configuration `key` used without reading it.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#ignore")]
    pub fn ignore(&self, key: &str) {
        self.config.ignore(key);
    }

    /// Info level log for any unused configurations: one line,
    /// `These configurations '[a, b]' were supplied but are not used yet.`,
    /// and nothing when every key was used.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#logUnused")]
    pub fn log_unused(&self) {
        self.config.log_unused();
    }

    /// The names of the configurations this class defines.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig#configNames")]
    pub fn config_names() -> HashSet<String> {
        CONFIG.names()
    }

    /// Marks used the keys the typed settings were read from, as the client
    /// does where Java's reads them through the getters.
    pub(crate) fn mark_read_keys_used(&self) {
        for key in &self.read_keys {
            self.config.ignore(key);
        }
    }

    /// Java's `logAll()`, which Java runs when `Admin.create` builds the
    /// configuration with `doLog = true`: every parsed value at INFO.
    pub(crate) fn log_values(&self) {
        self.config.log_all("AdminClientConfig");
    }

    /// The `bootstrap.servers` list.
    pub(crate) fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// `client.dns.lookup`.
    pub(crate) fn client_dns_lookup(&self) -> ClientDnsLookup {
        self.client_dns_lookup
    }

    /// The `client.id`.
    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `request.timeout.ms`.
    pub(crate) fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }

    /// `default.api.timeout.ms`.
    pub(crate) fn default_api_timeout_ms(&self) -> i32 {
        self.default_api_timeout_ms
    }

    /// `retries`.
    pub(crate) fn retries(&self) -> i32 {
        self.retries
    }

    /// `retry.backoff.ms`.
    pub(crate) fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }

    /// `retry.backoff.max.ms`.
    pub(crate) fn retry_backoff_max_ms(&self) -> i64 {
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
    pub(crate) fn bootstrap_resolve_timeout_ms(&self) -> i64 {
        self.bootstrap_resolve_timeout_ms
    }

    /// `reconnect.backoff.ms`.
    pub(crate) fn reconnect_backoff_ms(&self) -> i64 {
        self.reconnect_backoff_ms
    }

    /// `reconnect.backoff.max.ms`.
    pub(crate) fn reconnect_backoff_max_ms(&self) -> i64 {
        self.reconnect_backoff_max_ms
    }

    /// `connections.max.idle.ms`.
    pub(crate) fn connections_max_idle_ms(&self) -> i64 {
        self.connections_max_idle_ms
    }

    /// `metadata.max.age.ms`.
    pub(crate) fn metadata_max_age_ms(&self) -> i64 {
        self.metadata_max_age_ms
    }

    /// `socket.connection.setup.timeout.ms`.
    pub(crate) fn socket_connection_setup_timeout_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_ms
    }

    /// `socket.connection.setup.timeout.max.ms`.
    pub(crate) fn socket_connection_setup_timeout_max_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_max_ms
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
    pub(crate) fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub(crate) fn sasl_config(&self) -> &SaslConfigs {
        &self.sasl_config
    }

    /// SSL/TLS configuration.
    pub(crate) fn ssl_config(&self) -> &SslConfigs {
        &self.ssl_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::types::Password;

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

        // `ConfigDef.parseType(BOOLEAN)` is case-insensitive.
        props.insert("metadata.cluster.check.enable".to_string(), " TRUE ".to_string());
        assert!(AdminClientConfig::new(&props).unwrap().metadata_cluster_check_enable());

        props.insert("metadata.cluster.check.enable".to_string(), "maybe".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "Invalid value maybe for configuration metadata.cluster.check.enable: Expected value to be either true or false"
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
        assert!(error.is_local_unsupported_operation_error(), "{error:?}");
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
        let err = AdminClientConfig::new(&props).unwrap_err();
        assert!(err.is_config_error());
        assert_eq!(
            err.message(),
            "Invalid value not-a-number for configuration request.timeout.ms: Not a number of type INT"
        );
    }

    fn bootstrap_props() -> HashMap<String, String> {
        HashMap::from([("bootstrap.servers".to_string(), "a:9092".to_string())])
    }

    /// Translated from `AdminClientConfigTest.testDefaultMetadataRecoveryStrategy`.
    /// Rust needs `bootstrap.servers`: `determineBootstrapType` runs when the
    /// config is built here (see [`AdminClientConfig::new`]).
    #[test]
    fn test_default_metadata_recovery_strategy() {
        let config = AdminClientConfig::new(&bootstrap_props()).unwrap();
        assert_eq!(
            config
                .get_string(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG)
                .unwrap(),
            Some("rebootstrap")
        );
    }

    /// Translated from `AdminClientConfigTest.testInvalidMetadataRecoveryStrategy`.
    #[test]
    fn test_invalid_metadata_recovery_strategy() {
        let mut props = bootstrap_props();
        props.insert(
            CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG.to_string(),
            "abc".to_string(),
        );
        let err = AdminClientConfig::new(&props).unwrap_err();
        assert!(err.is_config_error());
        assert!(
            err.message().contains(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG),
            "{}",
            err.message()
        );
    }

    /// The numeric validators of Java's define chain
    /// (`AdminClientConfig.java:166-319`), each value just below its bound.
    #[test]
    fn test_numeric_validators() {
        let cases = [
            ("metadata.max.age.ms", "-1", "Value must be at least 0"),
            ("send.buffer.bytes", "-2", "Value must be at least -1"),
            ("receive.buffer.bytes", "-2", "Value must be at least -1"),
            ("reconnect.backoff.ms", "-1", "Value must be at least 0"),
            ("reconnect.backoff.max.ms", "-1", "Value must be at least 0"),
            ("retry.backoff.ms", "-1", "Value must be at least 0"),
            ("retry.backoff.max.ms", "-1", "Value must be at least 0"),
            ("bootstrap.resolve.timeout.ms", "-1", "Value must be at least 0"),
            ("request.timeout.ms", "-1", "Value must be at least 0"),
            ("retries", "-1", "Value must be at least 0"),
            ("default.api.timeout.ms", "-1", "Value must be at least 0"),
            ("metrics.sample.window.ms", "-1", "Value must be at least 0"),
            ("metrics.num.samples", "0", "Value must be at least 1"),
            ("metadata.recovery.rebootstrap.trigger.ms", "-1", "Value must be at least 0"),
        ];
        for (key, value, reason) in cases {
            let mut props = bootstrap_props();
            props.insert(key.to_string(), value.to_string());
            let err = AdminClientConfig::new(&props).unwrap_err();
            assert!(err.is_config_error(), "{key}");
            assert_eq!(
                err.message(),
                format!("Invalid value {value} for configuration {key}: {reason}"),
            );
        }
        // The bounds themselves are accepted, and keys without a validator
        // take any value, as in Java.
        let mut props = bootstrap_props();
        props.insert("send.buffer.bytes".to_string(), "-1".to_string());
        props.insert("metrics.num.samples".to_string(), "1".to_string());
        props.insert("socket.connection.setup.timeout.ms".to_string(), "-5".to_string());
        props.insert("connections.max.idle.ms".to_string(), "-5".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.socket_connection_setup_timeout_ms(), -5);
        assert_eq!(config.connections_max_idle_ms(), -5);
    }

    #[test]
    fn test_metrics_recording_level_and_recovery_strategy_values() {
        let mut props = bootstrap_props();
        props.insert("metrics.recording.level".to_string(), "info".to_string());
        assert_eq!(
            AdminClientConfig::new(&props).unwrap_err().message(),
            "Invalid value info for configuration metrics.recording.level: String must be one of: INFO, DEBUG, TRACE"
        );
        let mut props = bootstrap_props();
        props.insert("metadata.recovery.strategy".to_string(), "none".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.get_string("metadata.recovery.strategy").unwrap(), Some("none"));
    }

    /// `AbstractConfig`'s typed getters, on the admin client's keys.
    #[test]
    fn test_typed_getters() {
        let mut props = bootstrap_props();
        props.insert("client.id".to_string(), " admin-1 ".to_string());
        props.insert("ssl.keystore.key".to_string(), "secret".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.get_int("request.timeout.ms").unwrap(), Some(30_000));
        assert_eq!(config.get_long("metadata.max.age.ms").unwrap(), Some(300_000));
        assert_eq!(config.get_boolean("enable.metrics.push").unwrap(), Some(false));
        assert_eq!(config.get_string("client.id").unwrap(), Some("admin-1"));
        assert_eq!(config.get_list("bootstrap.servers").unwrap(), Some(&["a:9092".to_string()][..]));
        assert_eq!(config.get_list("bootstrap.controllers").unwrap(), Some(&[][..]));
        assert_eq!(config.get_string("sasl.jaas.config").unwrap(), None);

        let err = config.get_int("no.such.key").unwrap_err();
        assert!(err.is_config_error());
        assert_eq!(err.message(), "Unknown configuration 'no.such.key'");
        let err = config.get_long("request.timeout.ms").unwrap_err();
        assert!(err.is_local_illegal_argument_error());
        assert_eq!(err.message(), "Configuration 'request.timeout.ms' is of type INT, not LONG");
        assert!(config.get_short("request.timeout.ms").is_err());
        assert!(config.get_double("request.timeout.ms").is_err());
        assert!(config.get_string("ssl.keystore.key").is_err());
    }

    #[test]
    fn test_config_names() {
        let names = AdminClientConfig::config_names();
        for key in [
            "bootstrap.servers",
            "bootstrap.controllers",
            "client.id",
            "metadata.recovery.strategy",
            "metadata.cluster.check.enable",
            "ssl.truststore.type",
            "sasl.mechanism",
        ] {
            assert!(names.contains(key), "{key}");
        }
        for key in [
            "metric.reporters",
            "security.providers",
            "config.providers",
            "ssl.cipher.suites",
            "ssl.keystore.password",
            "ssl.key.password",
            "ssl.truststore.password",
        ] {
            assert!(!names.contains(key), "{key}");
        }
    }

    /// A key the user reads is used; what the client reads it marks used
    /// when it is built, and an undefined key is never used.
    #[test]
    fn test_unused_and_ignore() {
        let mut props = bootstrap_props();
        props.insert("request.timeout.ms".to_string(), "5000".to_string());
        props.insert("send.buffer.bytes".to_string(), "1000".to_string());
        props.insert("some.unknown.key".to_string(), "x".to_string());
        props.insert("ssl.keystore.location".to_string(), "/k".to_string());
        let config = AdminClientConfig::new(&props).unwrap();
        let all: HashSet<String> = props.keys().cloned().collect();
        assert_eq!(config.unused(), all);

        config.get_int("request.timeout.ms").unwrap();
        config.ignore("some.unknown.key");
        assert_eq!(
            config.unused(),
            HashSet::from([
                "bootstrap.servers".to_string(),
                "send.buffer.bytes".to_string(),
                "ssl.keystore.location".to_string(),
            ])
        );

        // The client reads `bootstrap.servers`; the `ssl.*` keys only for an
        // SSL protocol, and `send.buffer.bytes` not at all.
        config.mark_read_keys_used();
        assert_eq!(
            config.unused(),
            HashSet::from(["send.buffer.bytes".to_string(), "ssl.keystore.location".to_string()])
        );
        // A copy for one more client has its own used set, as each client
        // built from one C handle does.
        assert_eq!(config.copy_for_client().unwrap().unused(), all);

        props.insert("security.protocol".to_string(), "SSL".to_string());
        props.insert("ssl.keystore.password".to_string(), "p".to_string());
        props.insert("ssl.keystore.type".to_string(), "JKS".to_string());
        let ssl = AdminClientConfig::new(&props).unwrap();
        ssl.mark_read_keys_used();
        // `ssl.keystore.password` is not defined (`SslConfigs` module docs), so
        // it stays unused under TLS too.
        assert_eq!(
            ssl.unused(),
            HashSet::from([
                "send.buffer.bytes".to_string(),
                "some.unknown.key".to_string(),
                "ssl.keystore.password".to_string(),
            ])
        );
    }

    /// `new` logs each configuration warning once (the parse's duplicate
    /// values, the post-processing's, the TLS
    /// hostname verification); a copy for one more client logs none of them,
    /// as Java's client logs nothing about the config it is given, but makes
    /// the same reads, so it equals `new`'s and its used set is `new`'s.
    #[test]
    fn test_copy_for_client_logs_no_configuration_warning() {
        use log::Level;

        use crate::test_log_capture::capture_logs;

        let props: HashMap<String, String> = [
            (AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9092"),
            (AdminClientConfig::SECURITY_PROTOCOL_CONFIG, "SSL"),
            (
                AdminClientConfig::CLIENT_DNS_LOOKUP_CONFIG,
                "resolve_canonical_bootstrap_servers_only",
            ),
            (AdminClientConfig::RETRY_BACKOFF_MS_CONFIG, "2000"),
            (AdminClientConfig::RETRY_BACKOFF_MAX_MS_CONFIG, "1000"),
            (AdminClientConfig::RECONNECT_BACKOFF_MS_CONFIG, "100"),
            (AdminClientConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "40000"),
            (CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "30000"),
            ("some.unknown.key", "x"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let (config, new_lines) = capture_logs(|| AdminClientConfig::new(&props).unwrap());
        let warnings = |lines: &[(Level, String)]| {
            lines
                .iter()
                .filter(|(level, _)| *level == Level::Warn)
                .map(|(_, line)| line.split(' ').take(3).collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            warnings(&new_lines),
            [
                "Configuration key \"bootstrap.servers\"",
                "Configuration 'retry.backoff.ms' with",
                "Configuration 'socket.connection.setup.timeout.ms' with",
                "Disabling exponential reconnect",
                "client.dns.lookup=resolve_canonical_bootstrap_servers_only with security.protocol=SSL:",
            ],
            "{new_lines:?}"
        );
        let (copy, copy_lines) = capture_logs(|| config.copy_for_client().unwrap());
        assert!(copy_lines.is_empty(), "{copy_lines:?}");
        assert_eq!(copy, config);
        assert_eq!(copy.bootstrap_servers(), ["localhost:9092".to_string()]);
        let new_unused = config.unused();
        assert_eq!(copy.unused(), new_unused);
        copy.mark_read_keys_used();
        assert_eq!(copy.unused(), HashSet::from(["some.unknown.key".to_string()]));
        // The copy's used set is its own.
        assert_eq!(config.unused(), new_unused);
    }

    /// Java's `AbstractConfig.equals` / `hashCode`: equal originals.
    #[test]
    fn test_equals_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        let hash = |c: &AdminClientConfig| {
            let mut h = DefaultHasher::new();
            c.hash(&mut h);
            h.finish()
        };
        let a = AdminClientConfig::new(&bootstrap_props()).unwrap();
        let b = AdminClientConfig::new(&bootstrap_props()).unwrap();
        b.get_int("retries").unwrap();
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
        let mut props = bootstrap_props();
        props.insert("retries".to_string(), "3".to_string());
        let c = AdminClientConfig::new(&props).unwrap();
        assert_ne!(a, c);
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
        // Only `PLAIN` is accepted under a SASL protocol (`post_validate_supported_sasl_mechanism`).
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
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

    /// Rust-only (`CommonClientConfigs::post_validate_supported_sasl_mechanism`):
    /// a SASL `security.protocol` needs `sasl.mechanism=PLAIN`; any other
    /// mechanism, the `GSSAPI` default included, fails the build with
    /// `LocalUnsupportedOperationError`, while an empty one keeps Java's
    /// `ConfigError`. Under `PLAINTEXT` the mechanism is inert, as in Java.
    #[test]
    fn test_unsupported_sasl_mechanism_rejected() {
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";";
        let build = |extra: &[(&str, &str)]| {
            let mut props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
            for (k, v) in extra {
                props.insert((*k).to_string(), (*v).to_string());
            }
            AdminClientConfig::new(&props)
        };

        let err = build(&[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "SCRAM-SHA-256"),
            ("sasl.jaas.config", jaas),
        ])
        .unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "LocalUnsupportedOperationError: SASL mechanism 'SCRAM-SHA-256' is not supported by this client; only \
             PLAIN is"
        );

        // The default mechanism is Java's `GSSAPI`.
        let err = build(&[("security.protocol", "SASL_SSL")]).unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "LocalUnsupportedOperationError: SASL mechanism 'GSSAPI' is not supported by this client; only PLAIN is"
        );

        // Case-sensitive, as Java's mechanism names are.
        let err = build(&[("security.protocol", "SASL_PLAINTEXT"), ("sasl.mechanism", "plain")]).unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");

        build(&[("security.protocol", "PLAINTEXT")]).unwrap();
        build(&[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.jaas.config", jaas),
        ])
        .unwrap();

        // An empty mechanism fails Java's check first.
        let err = build(&[("security.protocol", "SASL_PLAINTEXT"), ("sasl.mechanism", "")]).unwrap_err();
        assert!(err.is_config_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "ConfigError: Invalid value null for configuration sasl.mechanism: When the security.protocol \
             configuration enables SASL, mechanism must be non-null and non-empty string."
        );
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
    /// `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:170`): an empty
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

    /// `{:?}` of a config built from every `Type.PASSWORD` key the client
    /// parses (`sasl.jaas.config` and the three SSL keys) renders none of their
    /// values. The derived `Debug` is safe because the embedded `SaslConfigs`
    /// and `SslConfigs` hold those values as `Password`s. The three SSL
    /// password keys this client does not define render as key names only.
    #[test]
    fn test_debug_redacts_password_configs() {
        let secrets = [
            (
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t\";",
            ),
            ("ssl.truststore.certificates", "TRUSTSTORE-CERTIFICATES-PEM"),
            ("ssl.keystore.key", "KEYSTORE-KEY-PEM"),
            ("ssl.keystore.certificate.chain", "KEYSTORE-CERTIFICATE-CHAIN-PEM"),
        ];
        // Not defined (`SslConfigs` module docs), so held only in the originals.
        let undefined = [
            ("ssl.truststore.password", "truststore-S3cr3t"),
            ("ssl.keystore.password", "keystore-S3cr3t"),
            ("ssl.key.password", "key-S3cr3t"),
        ];
        let mut props: HashMap<String, String> = secrets
            .iter()
            .chain(&undefined)
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        props.insert("bootstrap.servers".to_string(), "visible-host:9092".to_string());
        props.insert("client.id".to_string(), "visible-admin".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert("ssl.truststore.location".to_string(), "/visible/truststore.pem".to_string());
        let config = AdminClientConfig::new(&props).unwrap();

        // The secrets did reach the config; they are only hidden from `Debug`.
        assert_eq!(config.sasl_config().resolve_password(), Some("jaas-S3cr3t"));
        assert_eq!(
            config.ssl_config().keystore_key.as_ref().map(Password::value),
            Some("KEYSTORE-KEY-PEM")
        );

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for (key, value) in secrets.iter().chain(&undefined) {
                assert!(!rendered.contains(value), "{key} leaked: {rendered}");
            }
            for fragment in ["jaas-S3cr3t", "jaas-user", "PlainLoginModule", "S3cr3t", "-PEM"] {
                assert!(!rendered.contains(fragment), "{fragment:?} leaked: {rendered}");
            }
            assert_eq!(rendered.matches(Password::HIDDEN).count(), secrets.len(), "{rendered}");
            for visible in [
                "visible-host:9092",
                "visible-admin",
                "\"PLAIN\"",
                "/visible/truststore.pem",
            ] {
                assert!(rendered.contains(visible), "{visible} missing: {rendered}");
            }
        }
    }
}
