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

use std::collections::{BTreeSet, HashMap};
use std::fmt;

use crate::common::Error;
use crate::common::config::config_def::{ConfigDef, Range, ValidList};
use crate::common::config::types::Password;
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::security::auth::SecurityProtocol;
use crate::{ClientDnsLookup, CommonClientConfigs, MetadataRecoveryStrategy};

/// Configuration for the admin client.
///
/// Corresponds to `org.apache.kafka.clients.admin.AdminClientConfig`. Unknown
/// keys are accepted silently, matching Java's `AbstractConfig` behavior.
///
/// Equality compares the parsed values and the user's map, as Java's
/// `AbstractConfig.equals` compares `originals`.
#[derive(Clone, PartialEq, Eq)]
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
    socket_connection_setup_timeout_max_ms: i64,
    send_buffer_bytes: i32,
    receive_buffer_bytes: i32,
    metadata_cluster_check_enable: bool,
    metadata_recovery_strategy: MetadataRecoveryStrategy,
    metadata_recovery_rebootstrap_trigger_ms: i64,

    // --- Security ---
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    sasl_config: SaslConfigs,

    /// SSL/TLS configuration.
    ssl_config: SslConfigs,

    /// The user-supplied configuration map, verbatim: Java's
    /// `AbstractConfig.originals()`, with no defaults mixed in. Read through
    /// [`Self::user_configured`] where Java asks
    /// `originals().containsKey(..)`: by
    /// `CommonClientConfigs.postProcessReconnectBackoffConfigs` and by
    /// `KafkaAdminClient.configureDefaultApiTimeoutMs`.
    originals: HashMap<String, String>,
}

/// Renders every field in declaration order, as the derive it replaces did,
/// but never a secret.
///
/// The typed secrets are `Password`s inside `sasl_config` / `ssl_config` and
/// render as `Password::HIDDEN` on their own. `originals`, the raw user map,
/// renders as its sorted key set, with no values at all, because Java never
/// prints a raw `originals` value: `AbstractConfig.logAll()` prints only the
/// values parsed for the keys the `ConfigDef` defines, with each
/// `Type.PASSWORD` value rendered as `[hidden]`, and `logUnused()` prints key
/// names only. The map also holds keys this client does not parse, such as
/// Java's OAuth `Type.PASSWORD` keys, so hiding values by key could not match
/// Java. This is the same rendering as `ProducerConfig`'s.
impl fmt::Debug for AdminClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructured exhaustively: a field added to the struct fails to
        // compile until it is bound here, and a bound but unrendered field is
        // an unused variable, which the lint rejects.
        let Self {
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
            send_buffer_bytes,
            receive_buffer_bytes,
            metadata_cluster_check_enable,
            metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger_ms,
            security_protocol,
            sasl_config,
            ssl_config,
            originals,
        } = self;
        // The keys only, never a value (see above). Sorted so the rendering is
        // deterministic, as Java's `logAll()` sorts; borrowed, so no key is
        // copied.
        let originals: BTreeSet<&str> = originals.keys().map(String::as_str).collect();
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
            .field("send_buffer_bytes", send_buffer_bytes)
            .field("receive_buffer_bytes", receive_buffer_bytes)
            .field("metadata_cluster_check_enable", metadata_cluster_check_enable)
            .field("metadata_recovery_strategy", metadata_recovery_strategy)
            .field(
                "metadata_recovery_rebootstrap_trigger_ms",
                metadata_recovery_rebootstrap_trigger_ms,
            )
            .field("security_protocol", security_protocol)
            .field("sasl_config", sasl_config)
            .field("ssl_config", ssl_config)
            .field("originals", &originals)
            .finish()
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
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG;
    /// `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = CommonClientConfigs::RETRY_BACKOFF_MAX_MS_CONFIG;
    /// `bootstrap.resolve.timeout.ms` (KIP-909). See
    /// [`Self::bootstrap_resolve_timeout_ms`].
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG;
    /// `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG;
    /// `reconnect.backoff.max.ms`. When only `reconnect.backoff.ms` is set, it
    /// defaults to that value, which disables the exponential reconnect backoff
    /// (see [`Self::reconnect_backoff_max_ms`]).
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG;
    /// `connections.max.idle.ms`
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";
    /// `metadata.max.age.ms`
    pub const METADATA_MAX_AGE_MS_CONFIG: &'static str = "metadata.max.age.ms";
    /// `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    /// `socket.connection.setup.timeout.max.ms`. Java's `AdminClientConfig.java`
    /// declares it as its own public alias of
    /// `CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG`.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &'static str =
        CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    /// `send.buffer.bytes`. Java's `AdminClientConfig.java` declares it as its own
    /// public alias of `CommonClientConfigs.SEND_BUFFER_CONFIG`.
    pub const SEND_BUFFER_CONFIG: &'static str = CommonClientConfigs::SEND_BUFFER_CONFIG;
    /// `receive.buffer.bytes`. Java's `AdminClientConfig.java` declares it as its own
    /// public alias of `CommonClientConfigs.RECEIVE_BUFFER_CONFIG`.
    pub const RECEIVE_BUFFER_CONFIG: &'static str = CommonClientConfigs::RECEIVE_BUFFER_CONFIG;
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
    /// `sasl.mechanism`
    pub const SASL_MECHANISM_CONFIG: &'static str = SaslConfigs::SASL_MECHANISM;
    /// `sasl.jaas.config`
    pub const SASL_JAAS_CONFIG: &'static str = SaslConfigs::SASL_JAAS_CONFIG;
    /// `metadata.recovery.strategy`. Java's `AdminClientConfig.java` declares it as
    /// its own public alias of `CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG`.
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &'static str = CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG;
    /// The default `metadata.recovery.strategy` (`rebootstrap`).
    pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &'static str =
        CommonClientConfigs::DEFAULT_METADATA_RECOVERY_STRATEGY;
    /// `metadata.recovery.rebootstrap.trigger.ms`. Java's `AdminClientConfig.java` declares it as
    /// its own public alias of `CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG`.
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &'static str =
        CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG;
    /// The default `metadata.recovery.rebootstrap.trigger.ms` (5 minutes).
    pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 =
        CommonClientConfigs::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS;

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
        // Java's `AbstractConfig` keeps the user's map as `originals()`.
        let mut config = Self { originals: props.clone(), ..Self::default() };
        let mut bootstrap_controllers: Vec<String> = Vec::new();

        for (key, value) in props {
            match key.as_str() {
                Self::BOOTSTRAP_SERVERS_CONFIG => {
                    // `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:170`).
                    config.bootstrap_servers = ValidList::parse_any_non_duplicate_values(key, value, true)?;
                },
                Self::BOOTSTRAP_CONTROLLERS_CONFIG => {
                    // `ValidList.anyNonDuplicateValues(true, false)` (`AdminClientConfig.java:173-178`).
                    bootstrap_controllers = ValidList::parse_any_non_duplicate_values(key, value, true)?;
                },
                Self::CLIENT_DNS_LOOKUP_CONFIG => {
                    config.client_dns_lookup = ClientDnsLookup::parse_config_value(value)?;
                },
                // `ConfigDef.parseType` trims every `Type.STRING` value (`ConfigDef.java:707-709`,
                // `:731-733`), with Java's `String.trim`.
                Self::CLIENT_ID_CONFIG => config.client_id = ConfigDef::trim(value).to_string(),
                // Each numeric key is parsed by `ConfigDef.parseType`, then checked by
                // the validator its `define(..)` gives (`AdminClientConfig.java:167-315`),
                // if any.
                Self::REQUEST_TIMEOUT_MS_CONFIG => {
                    // `Type.INT`, `atLeast(0)` (`AdminClientConfig.java:218-223`).
                    config.request_timeout_ms = ConfigDef::parse_int(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.request_timeout_ms)?;
                },
                Self::DEFAULT_API_TIMEOUT_MS_CONFIG => {
                    // `Type.INT`, `atLeast(0)` (`AdminClientConfig.java:245-250`).
                    config.default_api_timeout_ms = ConfigDef::parse_int(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.default_api_timeout_ms)?;
                },
                Self::RETRIES_CONFIG => {
                    // `Type.INT`, `between(0, Integer.MAX_VALUE)` (`AdminClientConfig.java:239-244`).
                    config.retries = ConfigDef::parse_int(key, value)?;
                    Range::between(0, i32::MAX).ensure_valid(key, config.retries)?;
                },
                Self::RETRY_BACKOFF_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:195-200`).
                    config.retry_backoff_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.retry_backoff_ms)?;
                },
                Self::RETRY_BACKOFF_MAX_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:201-206`).
                    config.retry_backoff_max_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.retry_backoff_max_ms)?;
                },
                Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:207-212`, KAFKA-20939).
                    config.bootstrap_resolve_timeout_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.bootstrap_resolve_timeout_ms)?;
                },
                Self::RECONNECT_BACKOFF_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:183-188`).
                    config.reconnect_backoff_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.reconnect_backoff_ms)?;
                },
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0L)` (`AdminClientConfig.java:189-194`).
                    config.reconnect_backoff_max_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.reconnect_backoff_max_ms)?;
                },
                // `Type.LONG`, no validator (`AdminClientConfig.java:234-238`).
                Self::CONNECTIONS_MAX_IDLE_MS_CONFIG => {
                    config.connections_max_idle_ms = ConfigDef::parse_long(key, value)?
                },
                Self::METADATA_MAX_AGE_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0)` (`AdminClientConfig.java:180`).
                    config.metadata_max_age_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.metadata_max_age_ms)?;
                },
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG => {
                    // `Type.LONG`, no validator (`AdminClientConfig.java:224-228`).
                    config.socket_connection_setup_timeout_ms = ConfigDef::parse_long(key, value)?;
                },
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG => {
                    // `Type.LONG`, no validator (`AdminClientConfig.java:229-233`).
                    config.socket_connection_setup_timeout_max_ms = ConfigDef::parse_long(key, value)?;
                },
                Self::SEND_BUFFER_CONFIG => {
                    // `Type.INT`, `atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND)`
                    // (`AdminClientConfig.java:181`).
                    config.send_buffer_bytes = ConfigDef::parse_int(key, value)?;
                    Range::at_least(CommonClientConfigs::SEND_BUFFER_LOWER_BOUND)
                        .ensure_valid(key, config.send_buffer_bytes)?;
                },
                Self::RECEIVE_BUFFER_CONFIG => {
                    // `Type.INT`, `atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND)`
                    // (`AdminClientConfig.java:182`).
                    config.receive_buffer_bytes = ConfigDef::parse_int(key, value)?;
                    Range::at_least(CommonClientConfigs::RECEIVE_BUFFER_LOWER_BOUND)
                        .ensure_valid(key, config.receive_buffer_bytes)?;
                },
                Self::METADATA_CLUSTER_CHECK_ENABLE_CONFIG => {
                    // `Type.BOOLEAN`, default `true` (`AdminClientConfig.java:305-309`).
                    config.metadata_cluster_check_enable = parse_bool(key, value)?;
                },
                Self::SECURITY_PROTOCOL_CONFIG => {
                    // A trimmed `Type.STRING` (`ConfigDef.java:731-733`), checked by
                    // `CaseInsensitiveValidString` (`AdminClientConfig.java:283-289`). The
                    // validator sees the trimmed value, so its error prints that one.
                    let trimmed = ConfigDef::trim(value);
                    config.security_protocol = SecurityProtocol::for_name(trimmed).ok_or_else(|| {
                        Error::config_name_value_message(
                            key,
                            trimmed,
                            format!("Valid values are: {:?}", SecurityProtocol::names()),
                        )
                    })?;
                },
                Self::METADATA_RECOVERY_STRATEGY_CONFIG => {
                    config.metadata_recovery_strategy = parse_metadata_recovery_strategy(key, value)?;
                },
                Self::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG => {
                    // `Type.LONG`, `atLeast(0)` (`AdminClientConfig.java:299-304`).
                    config.metadata_recovery_rebootstrap_trigger_ms = ConfigDef::parse_long(key, value)?;
                    Range::at_least(0).ensure_valid(key, config.metadata_recovery_rebootstrap_trigger_ms)?;
                },
                Self::SASL_MECHANISM_CONFIG => {
                    // A trimmed `Type.STRING` (`ConfigDef.java:731-733`).
                    config.sasl_config.mechanism = ConfigDef::trim(value).to_string();
                },
                Self::SASL_JAAS_CONFIG => {
                    // `Type.PASSWORD`: `parseType` wraps the trimmed value
                    // (`ConfigDef.java:724-728`). An empty value means unset here.
                    let trimmed = ConfigDef::trim(value);
                    config.sasl_config.jaas_config = if trimmed.is_empty() {
                        None
                    } else {
                        Some(Password::new(trimmed))
                    };
                },
                key if key.starts_with("ssl.") => {
                    // `parseType` trims a `String` value whatever the key's type
                    // (`ConfigDef.java:707-709`), so every `ssl.*` key
                    // `SslConfigs.addClientSslSupport` defines (`SslConfigs.java:128-148`)
                    // is trimmed before its parser or validator sees it.
                    SslConfigs::apply_ssl_config_key(&mut config.ssl_config, key, ConfigDef::trim(value))?;
                },
                // Unknown keys are accepted silently, as in Java.
                _ => {},
            }
        }

        config.post_process_parsed_config()?;

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

    /// Java's `AbstractConfig.postProcessParsedConfig` hook as
    /// `AdminClientConfig` overrides it (`AdminClientConfig.java:318-323`), run
    /// once every key is parsed, before the bootstrap check:
    /// `postValidateSaslMechanismConfig`, `warnDisablingExponentialBackoff`,
    /// then `postProcessReconnectBackoffConfigs`, whose override replaces the
    /// parsed `reconnect.backoff.max.ms`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when a SASL `security.protocol` has an empty
    /// `sasl.mechanism`.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfig#postProcessParsedConfig")]
    fn post_process_parsed_config(&mut self) -> Result<(), Error> {
        CommonClientConfigs::post_validate_sasl_mechanism_config(self.security_protocol, &self.sasl_config.mechanism)?;
        CommonClientConfigs::warn_disabling_exponential_backoff(
            self.retry_backoff_ms,
            self.retry_backoff_max_ms,
            self.socket_connection_setup_timeout_ms,
            self.socket_connection_setup_timeout_max_ms,
        );
        if let Some(reconnect_backoff_max_ms) =
            CommonClientConfigs::post_process_reconnect_backoff_configs(&self.originals, self.reconnect_backoff_ms)
        {
            self.reconnect_backoff_max_ms = reconnect_backoff_max_ms;
        }
        Ok(())
    }

    /// Whether the user set `key` explicitly.
    ///
    /// Java's `originals().containsKey(key)`: a key is user configured iff it
    /// is in the user's map, not merely because it has a value here.
    pub(crate) fn user_configured(&self, key: &str) -> bool {
        self.originals.contains_key(key)
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

    /// `default.api.timeout.ms`, as configured. The client checks it against
    /// `request.timeout.ms` when it is created, as Java's
    /// `KafkaAdminClient.configureDefaultApiTimeoutMs` does: an explicit value
    /// below the request timeout fails creation, and an unset one below it is
    /// raised to the request timeout.
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

    /// `reconnect.backoff.max.ms`. When the user set `reconnect.backoff.ms` and
    /// not this key, this is the `reconnect.backoff.ms` value, as in Java
    /// (`CommonClientConfigs.postProcessReconnectBackoffConfigs`).
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

    /// `socket.connection.setup.timeout.ms`: the initial connection setup
    /// timeout, which grows exponentially for each consecutive failure up to
    /// [`Self::socket_connection_setup_timeout_max_ms`].
    pub fn socket_connection_setup_timeout_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_ms
    }

    /// `socket.connection.setup.timeout.max.ms`: the most the connection setup
    /// timeout grows to. Default: 30 seconds.
    pub fn socket_connection_setup_timeout_max_ms(&self) -> i64 {
        self.socket_connection_setup_timeout_max_ms
    }

    /// `send.buffer.bytes`: the TCP send buffer (SO_SNDBUF) size, `-1` for the
    /// OS default. Default: 128 KiB.
    pub fn send_buffer_bytes(&self) -> i32 {
        self.send_buffer_bytes
    }

    /// `receive.buffer.bytes`: the TCP receive buffer (SO_RCVBUF) size, `-1`
    /// for the OS default. Default: 64 KiB for the admin client (the producer
    /// and consumer default to 32 KiB and 64 KiB respectively).
    pub fn receive_buffer_bytes(&self) -> i32 {
        self.receive_buffer_bytes
    }

    /// `metadata.cluster.check.enable` (KIP-1242): whether the client sends the
    /// cluster id and node id it expects in ApiVersions so the broker can detect
    /// a misrouted connection.
    ///
    /// It is passed to the `NetworkClient` as in Java
    /// (`ClientUtils.createNetworkClient`), but, also as in Java, it has no
    /// effect on the admin client: the admin's metadata updater does not
    /// override `MetadataUpdater.clusterId()` (`AdminMetadataManager.java`), so
    /// the expected cluster id is never known.
    pub(crate) fn metadata_cluster_check_enable(&self) -> bool {
        self.metadata_cluster_check_enable
    }

    /// `metadata.recovery.strategy`.
    pub(crate) fn metadata_recovery_strategy(&self) -> MetadataRecoveryStrategy {
        self.metadata_recovery_strategy
    }

    /// `metadata.recovery.rebootstrap.trigger.ms`.
    pub fn metadata_recovery_rebootstrap_trigger_ms(&self) -> i64 {
        self.metadata_recovery_rebootstrap_trigger_ms
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
            socket_connection_setup_timeout_ms: CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
            socket_connection_setup_timeout_max_ms: CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
            // `128 * 1024` and `64 * 1024` (`AdminClientConfig.java:181-182`).
            send_buffer_bytes: 128 * 1024,
            receive_buffer_bytes: 64 * 1024,
            metadata_cluster_check_enable: true,
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger_ms: Self::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_config: SaslConfigs::default(),
            ssl_config: SslConfigs::default(),
            originals: HashMap::new(),
        }
    }
}

/// Parses a string value as `bool`.
///
/// Java's `ConfigDef.parseType(BOOLEAN)` (`ConfigDef.java:703-737`) trims the
/// value, then accepts `equalsIgnoreCase("true")` / `("false")`; anything else
/// throws `ConfigException(name, value, "Expected value to be either true or
/// false")`, where `value` is the **original, untrimmed** string. Same rule as
/// the producer and consumer configs.
fn parse_bool(key: &str, value: &str) -> Result<bool, Error> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(Error::config_name_value_message(
            key,
            value,
            "Expected value to be either true or false",
        ))
    }
}

/// `metadata.recovery.strategy`: a trimmed `Type.STRING` (`ConfigDef.java:731-733`)
/// validated by `CaseInsensitiveValidString.in(Utils.enumOptions(MetadataRecoveryStrategy.class))`
/// (`AdminClientConfig.java:292-297`), then read through
/// `MetadataRecoveryStrategy.forName` as `KafkaAdminClient`'s constructor does.
///
/// The valid strings are the upper-cased enum constant names, held in a
/// `HashSet`; the message lists them in that set's iteration order, which for
/// these two names is `REBOOTSTRAP, NONE`.
fn parse_metadata_recovery_strategy(key: &str, value: &str) -> Result<MetadataRecoveryStrategy, Error> {
    // Java's `String.trim`, not `str::trim`: it strips control characters and
    // keeps Unicode spaces such as U+00A0.
    let trimmed = ConfigDef::trim(value);
    MetadataRecoveryStrategy::for_name(trimmed).map_err(|_| {
        Error::config_name_value_message(key, trimmed, "String must be one of (case insensitive): REBOOTSTRAP, NONE")
    })
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

    /// A value `ConfigDef.parseType` cannot parse is reported with its
    /// `NumberFormatException` message, naming the key's type and printing
    /// the original, untrimmed value (`ConfigDef.java:788-789`). An `int` key
    /// rejects a value beyond `Integer.MAX_VALUE` the same way.
    #[test]
    fn invalid_numeric_value_is_error() {
        for (key, type_name) in [
            ("request.timeout.ms", "INT"),
            ("default.api.timeout.ms", "INT"),
            ("retries", "INT"),
            ("retry.backoff.ms", "LONG"),
            ("retry.backoff.max.ms", "LONG"),
            ("bootstrap.resolve.timeout.ms", "LONG"),
            ("reconnect.backoff.ms", "LONG"),
            ("reconnect.backoff.max.ms", "LONG"),
            ("connections.max.idle.ms", "LONG"),
            ("metadata.max.age.ms", "LONG"),
            ("socket.connection.setup.timeout.ms", "LONG"),
            ("metadata.recovery.rebootstrap.trigger.ms", "LONG"),
        ] {
            let err = AdminClientConfig::new(&with_bootstrap(&[(key, " not-a-number ")])).unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{key}: got {err:?}");
            assert_eq!(
                err.message(),
                format!("Invalid value  not-a-number  for configuration {key}: Not a number of type {type_name}"),
            );
        }
        let err = AdminClientConfig::new(&with_bootstrap(&[("retries", "2147483648")])).unwrap_err();
        assert_eq!(
            err.message(),
            "Invalid value 2147483648 for configuration retries: Not a number of type INT"
        );
    }

    /// The `atLeast(0)` / `between(0, Integer.MAX_VALUE)` validators of
    /// `AdminClientConfig`'s numeric keys (`AdminClientConfig.java:180-250`)
    /// reject a negative value with `Range.ensureValid`'s message, and accept
    /// the bounds themselves. `socket.connection.setup.timeout.ms` and
    /// `connections.max.idle.ms` have no validator in Java, so `-1` stays
    /// accepted for them.
    #[test]
    fn test_negative_values_are_rejected() {
        for key in [
            "retries",
            "request.timeout.ms",
            "default.api.timeout.ms",
            "metadata.max.age.ms",
            "retry.backoff.ms",
            "retry.backoff.max.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
        ] {
            let err = AdminClientConfig::new(&with_bootstrap(&[(key, "-1")])).unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{key}: got {err:?}");
            assert_eq!(
                err.message(),
                format!("Invalid value -1 for configuration {key}: Value must be at least 0")
            );
            AdminClientConfig::new(&with_bootstrap(&[(key, "0")])).unwrap_or_else(|e| panic!("{key}=0: {e:?}"));
        }
        let config = AdminClientConfig::new(&with_bootstrap(&[("retries", "2147483647")])).unwrap();
        assert_eq!(config.retries(), i32::MAX);
        // The value in the message is the parsed number, not the raw string.
        let err = AdminClientConfig::new(&with_bootstrap(&[("retry.backoff.ms", " -07 ")])).unwrap_err();
        assert_eq!(
            err.message(),
            "Invalid value -7 for configuration retry.backoff.ms: Value must be at least 0"
        );

        let config = AdminClientConfig::new(&with_bootstrap(&[
            ("socket.connection.setup.timeout.ms", "-1"),
            ("connections.max.idle.ms", "-1"),
        ]))
        .unwrap();
        assert_eq!(config.socket_connection_setup_timeout_ms(), -1);
        assert_eq!(config.connections_max_idle_ms(), -1);
    }

    /// `send.buffer.bytes` / `receive.buffer.bytes` are `Type.INT` with
    /// `atLeast(-1)`, defaulting to 128 KiB and 64 KiB
    /// (`AdminClientConfig.java:181-182`); `-1` selects the OS default.
    #[test]
    fn test_socket_buffer_sizes() {
        assert_eq!(AdminClientConfig::SEND_BUFFER_CONFIG, "send.buffer.bytes");
        assert_eq!(AdminClientConfig::RECEIVE_BUFFER_CONFIG, "receive.buffer.bytes");
        let config = AdminClientConfig::new(&with_bootstrap(&[])).unwrap();
        assert_eq!((config.send_buffer_bytes(), config.receive_buffer_bytes()), (131_072, 65_536));

        let config = AdminClientConfig::new(&with_bootstrap(&[
            ("send.buffer.bytes", "262144"),
            ("receive.buffer.bytes", "32768"),
        ]))
        .unwrap();
        assert_eq!((config.send_buffer_bytes(), config.receive_buffer_bytes()), (262_144, 32_768));

        let config =
            AdminClientConfig::new(&with_bootstrap(&[("send.buffer.bytes", "-1"), ("receive.buffer.bytes", "-1")]))
                .unwrap();
        assert_eq!((config.send_buffer_bytes(), config.receive_buffer_bytes()), (-1, -1));

        for key in ["send.buffer.bytes", "receive.buffer.bytes"] {
            let err = AdminClientConfig::new(&with_bootstrap(&[(key, "-2")])).unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{key}: got {err:?}");
            assert_eq!(
                err.message(),
                format!("Invalid value -2 for configuration {key}: Value must be at least -1")
            );
            let err = AdminClientConfig::new(&with_bootstrap(&[(key, "64k")])).unwrap_err();
            assert_eq!(
                err.message(),
                format!("Invalid value 64k for configuration {key}: Not a number of type INT")
            );
        }
    }

    /// `socket.connection.setup.timeout.max.ms` is a `Type.LONG` without a
    /// validator, defaulting to 30 seconds (`AdminClientConfig.java:229-233`).
    #[test]
    fn test_socket_connection_setup_timeout_max_ms() {
        let key = "socket.connection.setup.timeout.max.ms";
        assert_eq!(AdminClientConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, key);
        let config = AdminClientConfig::new(&with_bootstrap(&[])).unwrap();
        assert_eq!(config.socket_connection_setup_timeout_ms(), 10_000);
        assert_eq!(config.socket_connection_setup_timeout_max_ms(), 30_000);

        let config = AdminClientConfig::new(&with_bootstrap(&[(key, "45000")])).unwrap();
        assert_eq!(config.socket_connection_setup_timeout_max_ms(), 45_000);
        assert_eq!(config.socket_connection_setup_timeout_ms(), 10_000);

        let err = AdminClientConfig::new(&with_bootstrap(&[(key, "abc")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value abc for configuration socket.connection.setup.timeout.max.ms: Not a number of type LONG"
        );
    }

    /// Java checks the SASL mechanism in the config constructor
    /// (`AdminClientConfig.postProcessParsedConfig`), before
    /// `KafkaAdminClient.determineBootstrapType` runs, so with no bootstrap
    /// servers either the mechanism error is the one reported.
    #[test]
    fn test_sasl_mechanism_is_checked_before_the_bootstrap_servers() {
        let props = HashMap::from([
            ("security.protocol".to_string(), "SASL_SSL".to_string()),
            ("sasl.mechanism".to_string(), String::new()),
        ]);
        let err = AdminClientConfig::new(&props).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value null for configuration sasl.mechanism: When the security.protocol \
             configuration enables SASL, mechanism must be non-null and non-empty string."
        );
    }

    /// `reconnect.backoff.ms` alone sets the max to its value whether it is
    /// above or below the default max of 1000, and an explicit max is kept.
    #[test]
    fn test_reconnect_backoff_alone_sets_the_max() {
        let max = |extra: &[(&str, &str)]| {
            AdminClientConfig::new(&with_bootstrap(extra))
                .unwrap()
                .reconnect_backoff_max_ms()
        };
        assert_eq!(max(&[("reconnect.backoff.ms", "5000")]), 5000);
        assert_eq!(max(&[("reconnect.backoff.ms", "200")]), 200);
        assert_eq!(
            max(&[("reconnect.backoff.ms", "200"), ("reconnect.backoff.max.ms", "2000")]),
            2000
        );
        assert_eq!(max(&[("reconnect.backoff.max.ms", "2000")]), 2000);
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
    /// parses (`sasl.jaas.config` and the six SSL keys) renders none of their
    /// values: the hand-written `Debug` renders `originals` as its keys only,
    /// and the embedded `SaslConfigs` and `SslConfigs` hold the values as
    /// `Password`s.
    #[test]
    fn test_debug_redacts_password_configs() {
        let secrets = [
            (
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t\";",
            ),
            ("ssl.truststore.password", "truststore-S3cr3t"),
            ("ssl.truststore.certificates", "TRUSTSTORE-CERTIFICATES-PEM"),
            ("ssl.keystore.password", "keystore-S3cr3t"),
            ("ssl.keystore.key", "KEYSTORE-KEY-PEM"),
            ("ssl.keystore.certificate.chain", "KEYSTORE-CERTIFICATE-CHAIN-PEM"),
            ("ssl.key.password", "key-S3cr3t"),
        ];
        let mut props: HashMap<String, String> = secrets
            .iter()
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
            config.ssl_config().key_password.as_ref().map(Password::value),
            Some("key-S3cr3t")
        );

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for (key, value) in secrets {
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

    /// `originals` is the user's map verbatim (Java's `originals()`), which
    /// `user_configured` reads; `Debug` renders it as its sorted key set and
    /// never a value, including the value of a key this client does not parse.
    /// Equality compares it, as Java's `AbstractConfig.equals` compares
    /// `originals`.
    #[test]
    fn test_originals() {
        let props = with_bootstrap(&[
            ("request.timeout.ms", "5000"),
            ("sasl.oauthbearer.client.credentials.client.secret", "oauth-S3cr3t"),
            ("some.unknown.key", "unknown-value"),
        ]);
        let config = AdminClientConfig::new(&props).unwrap();
        assert_eq!(config.originals, props);
        assert!(config.user_configured("request.timeout.ms"));
        assert!(config.user_configured("some.unknown.key"));
        assert!(!config.user_configured("default.api.timeout.ms"));

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!rendered.contains("oauth-S3cr3t"), "{rendered}");
            assert!(!rendered.contains("unknown-value"), "{rendered}");
        }
        assert!(
            format!("{config:?}").ends_with(
                "originals: {\"bootstrap.servers\", \"request.timeout.ms\", \
                 \"sasl.oauthbearer.client.credentials.client.secret\", \"some.unknown.key\"} }"
            ),
            "{config:?}"
        );
        assert!(format!("{:?}", AdminClientConfig::default()).ends_with("originals: {} }"));

        let same_values = AdminClientConfig::new(&with_bootstrap(&[("request.timeout.ms", "5000")])).unwrap();
        assert_eq!(same_values.request_timeout_ms(), config.request_timeout_ms());
        assert_ne!(same_values, config);
        assert_eq!(AdminClientConfig::new(&props).unwrap(), config);
    }

    fn with_bootstrap(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        props
    }

    /// `ConfigDef.parseType` trims every `String` value before it parses or
    /// validates it, whatever the key's type (`ConfigDef.java:707-709`). These
    /// tests cover the `Type.STRING` key `security.protocol`.
    #[test]
    fn test_security_protocol_is_trimmed() {
        let config = AdminClientConfig::new(&with_bootstrap(&[("security.protocol", " SSL")])).unwrap();
        assert_eq!(config.security_protocol(), SecurityProtocol::Ssl);
        let config = AdminClientConfig::new(&with_bootstrap(&[
            ("security.protocol", " sasl_ssl "),
            ("sasl.mechanism", "PLAIN"),
        ]))
        .unwrap();
        assert_eq!(config.security_protocol(), SecurityProtocol::SaslSsl);
    }

    /// The `Type.STRING` keys `sasl.mechanism` and `client.id` are trimmed
    /// with Java's `String.trim`, which strips every character up to U+0020
    /// and nothing else (so not U+00A0). `client.dns.lookup` was already
    /// trimmed; it is checked here alongside them.
    #[test]
    fn test_string_keys_are_trimmed_with_java_trim() {
        let config = AdminClientConfig::new(&with_bootstrap(&[
            ("sasl.mechanism", " PLAIN "),
            ("client.id", "\u{1}admin-1\t"),
            ("client.dns.lookup", " resolve_canonical_bootstrap_servers_only "),
        ]))
        .unwrap();
        assert_eq!(config.sasl_config().mechanism, "PLAIN");
        assert_eq!(config.client_id(), "admin-1");
        assert_eq!(
            config.client_dns_lookup(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
        );
        let config = AdminClientConfig::new(&with_bootstrap(&[("client.id", "\u{A0}admin-1 ")])).unwrap();
        assert_eq!(config.client_id(), "\u{A0}admin-1");
    }

    /// The `Type.PASSWORD` key `sasl.jaas.config` is trimmed
    /// (`ConfigDef.java:724-728`); a blank value trims to empty, which this
    /// client reads as unset.
    #[test]
    fn test_sasl_jaas_config_is_trimmed() {
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required \
                    username=\"alice\" password=\"secret\";";
        let config = AdminClientConfig::new(&with_bootstrap(&[("sasl.jaas.config", &format!("  {jaas}\n"))])).unwrap();
        assert_eq!(config.sasl_config().jaas_config.as_ref().map(Password::value), Some(jaas));
        let config = AdminClientConfig::new(&with_bootstrap(&[("sasl.jaas.config", "  ")])).unwrap();
        assert_eq!(config.sasl_config().jaas_config, None);
    }

    /// Every `ssl.*` value is trimmed before `SslConfigs` sees it, whatever
    /// the key's type: `Type.STRING` locations and store types, `Type.PASSWORD`
    /// passwords.
    #[test]
    fn test_ssl_values_are_trimmed() {
        let config = AdminClientConfig::new(&with_bootstrap(&[
            ("ssl.truststore.location", " /p.pem "),
            ("ssl.keystore.type", "\tPKCS12 "),
            ("ssl.key.password", " key-secret "),
            ("ssl.endpoint.identification.algorithm", " "),
        ]))
        .unwrap();
        assert_eq!(config.ssl_config().truststore_location.as_deref(), Some("/p.pem"));
        assert_eq!(config.ssl_config().keystore_type, "PKCS12");
        assert_eq!(
            config.ssl_config().key_password.as_ref().map(Password::value),
            Some("key-secret")
        );
        assert_eq!(config.ssl_config().endpoint_identification_algorithm, "");
    }

    /// `CaseInsensitiveValidString` validates the trimmed `security.protocol`,
    /// so its error prints the value without the padding
    /// (`ConfigDef.java:1141-1145`).
    #[test]
    fn test_invalid_security_protocol_error_prints_the_trimmed_value() {
        let err = AdminClientConfig::new(&with_bootstrap(&[("security.protocol", " abc ")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value abc for configuration security.protocol: \
             Valid values are: [\"PLAINTEXT\", \"SSL\", \"SASL_PLAINTEXT\", \"SASL_SSL\"]"
        );
    }

    /// Translated from `AdminClientConfigTest.testDefaultMetadataRecoveryStrategy`
    /// (Rust's constructor also requires `bootstrap.servers`, which Java checks
    /// later, in `AdminBootstrapAddresses.fromConfig`). Also pins the trigger
    /// default (`CommonClientConfigs.java:265`).
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfigTest#testDefaultMetadataRecoveryStrategy")]
    fn test_default_metadata_recovery_strategy() {
        let config = AdminClientConfig::new(&with_bootstrap(&[])).unwrap();
        assert_eq!(config.metadata_recovery_strategy(), MetadataRecoveryStrategy::Rebootstrap);
        assert_eq!(
            AdminClientConfig::DEFAULT_METADATA_RECOVERY_STRATEGY,
            MetadataRecoveryStrategy::Rebootstrap.name()
        );
        assert_eq!(config.metadata_recovery_rebootstrap_trigger_ms(), 300_000);
        assert_eq!(AdminClientConfig::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS, 300_000);
        assert_eq!(
            AdminClientConfig::METADATA_RECOVERY_STRATEGY_CONFIG,
            "metadata.recovery.strategy"
        );
        assert_eq!(
            AdminClientConfig::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
            "metadata.recovery.rebootstrap.trigger.ms"
        );
    }

    /// Translated from `AdminClientConfigTest.testInvalidMetadataRecoveryStrategy`,
    /// asserting Java's whole message rather than only the key it contains.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClientConfigTest#testInvalidMetadataRecoveryStrategy")]
    fn test_invalid_metadata_recovery_strategy() {
        let err = AdminClientConfig::new(&with_bootstrap(&[("metadata.recovery.strategy", "abc")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value abc for configuration metadata.recovery.strategy: \
             String must be one of (case insensitive): REBOOTSTRAP, NONE"
        );
    }

    /// `CaseInsensitiveValidString` on a trimmed `Type.STRING`: any case, and
    /// surrounding whitespace, is accepted.
    #[test]
    fn metadata_recovery_strategy_is_case_insensitive_and_trimmed() {
        for (value, expected) in [
            ("none", MetadataRecoveryStrategy::None),
            ("NoNe", MetadataRecoveryStrategy::None),
            (" none ", MetadataRecoveryStrategy::None),
            ("REBOOTSTRAP", MetadataRecoveryStrategy::Rebootstrap),
            ("rebootstrap", MetadataRecoveryStrategy::Rebootstrap),
            // Java's `String.trim` strips every character up to U+0020.
            ("\u{1}none", MetadataRecoveryStrategy::None),
        ] {
            let config = AdminClientConfig::new(&with_bootstrap(&[("metadata.recovery.strategy", value)])).unwrap();
            assert_eq!(config.metadata_recovery_strategy(), expected, "value {value:?}");
        }
        // ... and nothing else: a no-break space stays, so the value is invalid.
        let err = AdminClientConfig::new(&with_bootstrap(&[("metadata.recovery.strategy", "none\u{A0}")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value none\u{A0} for configuration metadata.recovery.strategy: \
             String must be one of (case insensitive): REBOOTSTRAP, NONE"
        );
    }

    /// `metadata.recovery.rebootstrap.trigger.ms` is a `Type.LONG` with
    /// `atLeast(0)`; the messages are Java's.
    #[test]
    fn metadata_recovery_rebootstrap_trigger_ms_is_validated() {
        let key = "metadata.recovery.rebootstrap.trigger.ms";
        let config = AdminClientConfig::new(&with_bootstrap(&[(key, " 0 ")])).unwrap();
        assert_eq!(config.metadata_recovery_rebootstrap_trigger_ms(), 0);
        let config = AdminClientConfig::new(&with_bootstrap(&[(key, "1000")])).unwrap();
        assert_eq!(config.metadata_recovery_rebootstrap_trigger_ms(), 1000);

        let err = AdminClientConfig::new(&with_bootstrap(&[(key, "-1")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value -1 for configuration metadata.recovery.rebootstrap.trigger.ms: Value must be at least 0"
        );

        let err = AdminClientConfig::new(&with_bootstrap(&[(key, "abc")])).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value abc for configuration metadata.recovery.rebootstrap.trigger.ms: Not a number of type LONG"
        );
    }
}
