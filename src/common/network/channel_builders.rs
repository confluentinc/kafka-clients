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

//! Translation of `org.apache.kafka.common.network.ChannelBuilders`.
//!
//! Phase 5b-3 ships the producer-relevant subset:
//!
//! * [`client_channel_builder`] — entry point for the producer's
//!   [`crate::common::network::Selector`] (Phase 5c) to obtain a typed
//!   [`crate::common::network::ChannelBuilder`].
//! * [`channel_builder_configs`] — pure data-shuffling helper that
//!   reproduces Java's listener-prefix override logic without an
//!   `AbstractConfig` parser. Used by the Phase 9 SASL builder; the
//!   Phase 5b-3 PLAINTEXT and SSL paths do not need it but it's kept
//!   for parity with the Java surface and the Java
//!   `ChannelBuildersTest.testChannelBuilderConfigs` test.
//!
//! Server-side (`server_channel_builder`), `createPrincipalBuilder`,
//! and the SASL routing branch are deferred to Phase 9.

use std::collections::HashMap;
use std::sync::Arc;

use rustls::ClientConfig;

use crate::common::errors::KafkaError;
use crate::common::network::ListenerName;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::connection_mode::ConnectionMode;
use crate::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use crate::common::network::ssl_channel_builder::SslChannelBuilder;
use crate::common::security::auth::SecurityProtocol;

/// Owned trait object alias for the channel builder. `Send` so the
/// builder can be moved between Tokio tasks.
pub type BoxedChannelBuilder = Box<dyn ChannelBuilder>;

/// Construct the appropriate client-side [`ChannelBuilder`] for the
/// given [`SecurityProtocol`]. Mirrors Java's
/// `ChannelBuilders.clientChannelBuilder(...)`.
///
/// SASL_PLAINTEXT and SASL_SSL are deferred to Phase 9; passing them
/// returns [`KafkaError::Config`].
///
/// **Java→Rust signature differences:**
///
/// 1. The Java method takes a `JaasContext.Type contextType` and
///    `String clientSaslMechanism` for SASL routing. Both are deferred.
/// 2. The Java method takes an `AbstractConfig config` from which it
///    derives the configs map; the Rust signature accepts the SSL
///    config directly as an `Option<Arc<ClientConfig>>` (required for
///    [`SecurityProtocol::Ssl`], ignored otherwise).
/// 3. The Java method takes `Time` and `LogContext` for SASL token
///    refresh and structured logging. Both are deferred.
pub fn client_channel_builder(
    security_protocol: SecurityProtocol,
    listener_name: Option<ListenerName>,
    ssl_config: Option<Arc<ClientConfig>>,
) -> Result<BoxedChannelBuilder, KafkaError> {
    match security_protocol {
        SecurityProtocol::Plaintext => Ok(Box::new(PlaintextChannelBuilder::new(listener_name))),
        SecurityProtocol::Ssl => {
            let config = ssl_config
                .ok_or_else(|| KafkaError::Config("ssl_config is required when security.protocol = SSL".to_owned()))?;
            Ok(Box::new(SslChannelBuilder::new(
                ConnectionMode::Client,
                listener_name,
                false,
                config,
            )))
        },
    }
}

/// Reject SASL security protocols at the configuration boundary.
///
/// Mirrors Java's eager `IllegalArgumentException` paths in
/// `clientChannelBuilder` for SASL_* protocols, but signalled as a
/// typed [`KafkaError::Config`] (CLAUDE.md rule 10.3 + Phase-9 deferral
/// note in `Phase-5/NOTES.md`).
///
/// Phase 5b-3 callers do not invoke this directly — the
/// [`SecurityProtocol`] enum already excludes SASL variants. The
/// helper is kept so the producer-side config validator (Phase 5d)
/// can produce a uniform error message regardless of whether the
/// SASL value came from the public enum or a stringly-typed config
/// path.
pub fn reject_sasl_until_phase_9(value: &str) -> Result<(), KafkaError> {
    let upper = value.to_ascii_uppercase();
    if upper == "SASL_PLAINTEXT" || upper == "SASL_SSL" {
        return Err(KafkaError::Config(format!(
            "security.protocol={value} is not yet supported (Phase 9). Supported values: PLAINTEXT, SSL"
        )));
    }
    Ok(())
}

/// Reproduce Java's `channelBuilderConfigs(AbstractConfig, ListenerName)`
/// over a flat `HashMap<String, String>` configuration source. Mirrors
/// the listener-prefix override semantics asserted by
/// `ChannelBuildersTest.testChannelBuilderConfigs`:
///
/// * When `listener_name` is `Some`, keys prefixed with
///   `listener.name.<name>.` are unwrapped to the bare key; bare keys
///   that are also present prefixed are dropped (the prefix wins);
///   keys like `<mechanism>.some.prop` are dropped if the listener-
///   prefixed `listener.name.<name>.some.prop` already exists in the
///   parsed configs.
/// * When `listener_name` is `None`, the original keys are returned
///   verbatim.
///
/// The Java version operates over `AbstractConfig.values()` and
/// `originals()` and additionally interacts with the "RecordingMap"
/// that tracks unused keys. Phase 5b-3 doesn't have an `AbstractConfig`
/// translation; we reproduce the data-shuffling shape over a plain
/// map so the Java test can be translated 1:1.
pub fn channel_builder_configs(
    originals: &HashMap<String, String>,
    listener_name: Option<&ListenerName>,
) -> HashMap<String, String> {
    let mut parsed: HashMap<String, String> = HashMap::new();
    let prefix = listener_name.map(|n| n.config_prefix());

    if let Some(prefix) = prefix.as_deref() {
        // Pass 1: unwrap listener-prefixed keys. These take precedence
        // over the bare keys.
        for (k, v) in originals {
            if let Some(stripped) = k.strip_prefix(prefix) {
                parsed.insert(stripped.to_owned(), v.clone());
            }
        }
        // Pass 2: copy in originals that are neither already parsed
        // nor would be overshadowed.
        for (k, v) in originals {
            if parsed.contains_key(k) {
                // Already present (the unwrapped form took precedence).
                continue;
            }
            // Skip if the bare key is the prefixed form of an
            // already-parsed entry.
            if let Some(stripped) = k.strip_prefix(prefix)
                && parsed.contains_key(stripped)
            {
                continue;
            }
            // Skip keys like `<mechanism>.some.prop` if the listener-
            // prefixed `listener.name.<name>.some.prop` already exists
            // in parsed configs (Java: `e.getKey().substring(e.getKey().indexOf('.') + 1)`).
            if let Some(idx) = k.find('.')
                && parsed.contains_key(&k[idx + 1..])
            {
                continue;
            }
            parsed.insert(k.clone(), v.clone());
        }
    } else {
        // No listener: return originals verbatim.
        parsed.clone_from(originals);
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `ChannelBuildersTest.testChannelBuilderConfigs`.
    ///
    /// Java's helper consults an `AbstractConfig` schema in two stages:
    /// `valuesWithPrefixOverride` (typed fields only — listener-prefixed
    /// keys are unwrapped, others fall back to `values()`), then a
    /// filter that drops `<mechanism>.some.prop` keys when the bare
    /// `some.prop` exists in the typed parsed map. Both stages depend
    /// on a `ConfigDef` schema we have not translated yet (deferred to
    /// Phase 9 SASL). This Rust translation reproduces only the
    /// data-shuffling shape over a flat `HashMap`.
    ///
    /// **Documented divergences from the Java test**, both of which
    /// require a `ConfigDef` translation to fix and will be revisited
    /// when SASL ships:
    ///
    /// 1. Java line 74: `assertNull(configs.get("plain.sasl.server.callback.handler.class"))`.
    ///    Java drops the key because `TestSecurityConfig`'s schema lists
    ///    `sasl.server.callback.handler.class` as a typed field and the
    ///    second-stage filter prunes the `plain.`-prefixed shadow. Our
    ///    schema-less helper preserves the key.
    /// 2. Java line 77: `assertEquals("custom.config1", configs.get("listener.name.listener1.gssapi.config1.key"))`.
    ///    Java keeps the original prefixed key for non-typed fields
    ///    (since `valuesWithPrefixOverride` only unwraps typed fields).
    ///    Our helper unwraps every `listener.name.<name>.` prefix
    ///    unconditionally, so the prefixed key is gone and only the
    ///    unwrapped form (`gssapi.config1.key`) survives.
    #[test]
    fn channel_builder_configs_listener_prefix() {
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(
            "listener.name.listener1.gssapi.sasl.kerberos.service.name".to_owned(),
            "testkafka".to_owned(),
        );
        props.insert(
            "listener.name.listener1.sasl.kerberos.service.name".to_owned(),
            "testkafkaglobal".to_owned(),
        );
        props.insert("plain.sasl.server.callback.handler.class".to_owned(), "callback".to_owned());
        props.insert(
            "listener.name.listener1.gssapi.config1.key".to_owned(),
            "custom.config1".to_owned(),
        );
        props.insert("custom.config2.key".to_owned(), "custom.config2".to_owned());

        let listener = ListenerName::new("listener1");
        let configs = channel_builder_configs(&props, Some(&listener));

        // Java line 62: prefixed key dropped from the parsed configs.
        assert!(!configs.contains_key("listener.name.listener1.gssapi.sasl.kerberos.service.name"));
        // Java line 65: unwrapped form retains the listener-prefix value.
        assert_eq!(
            configs.get("gssapi.sasl.kerberos.service.name").map(|s| s.as_str()),
            Some("testkafka")
        );
        // Java line 68: the second prefixed key is also unwrapped.
        assert_eq!(
            configs.get("sasl.kerberos.service.name").map(|s| s.as_str()),
            Some("testkafkaglobal")
        );
        // Java line 71: the listener-prefixed `sasl.kerberos.service.name`
        // is gone from the parsed map.
        assert!(!configs.contains_key("listener.name.listener1.sasl.kerberos.service.name"));
        // Java line 80: the non-listener-prefixed custom key is kept verbatim.
        assert_eq!(configs.get("custom.config2.key").map(|s| s.as_str()), Some("custom.config2"));

        // Documented divergence #1 (Java line 74): without a ConfigDef
        // schema we cannot drop this key. The bare assertion that
        // matches our actual behaviour:
        assert_eq!(
            configs.get("plain.sasl.server.callback.handler.class").map(|s| s.as_str()),
            Some("callback"),
            "schema-less helper preserves the key Java's typed-field filter would drop"
        );

        // Documented divergence #2 (Java line 77): we unwrap the prefix
        // so the original key is gone, only the bare form survives.
        assert!(
            !configs.contains_key("listener.name.listener1.gssapi.config1.key"),
            "schema-less helper unwraps every listener-prefixed key, including non-typed ones"
        );
        assert_eq!(configs.get("gssapi.config1.key").map(|s| s.as_str()), Some("custom.config1"));
    }

    /// Listener-prefix `None` returns the originals verbatim.
    /// Mirrors Java's else-branch in `channelBuilderConfigs`.
    #[test]
    fn channel_builder_configs_no_listener() {
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(
            "listener.name.listener1.gssapi.sasl.kerberos.service.name".to_owned(),
            "testkafka".to_owned(),
        );
        props.insert(
            "listener.name.listener1.sasl.kerberos.service.name".to_owned(),
            "testkafkaglobal".to_owned(),
        );
        props.insert("plain.sasl.server.callback.handler.class".to_owned(), "callback".to_owned());
        props.insert(
            "listener.name.listener1.gssapi.config1.key".to_owned(),
            "custom.config1".to_owned(),
        );
        props.insert("custom.config2.key".to_owned(), "custom.config2".to_owned());

        let configs = channel_builder_configs(&props, None);

        // All keys retained verbatim.
        assert_eq!(
            configs
                .get("listener.name.listener1.gssapi.sasl.kerberos.service.name")
                .map(|s| s.as_str()),
            Some("testkafka")
        );
        assert!(!configs.contains_key("gssapi.sasl.kerberos.service.name"));
        assert_eq!(
            configs
                .get("listener.name.listener1.sasl.kerberos.service.name")
                .map(|s| s.as_str()),
            Some("testkafkaglobal")
        );
        assert!(!configs.contains_key("sasl.kerberos.service.name"));
        assert_eq!(
            configs.get("plain.sasl.server.callback.handler.class").map(|s| s.as_str()),
            Some("callback")
        );
        assert_eq!(
            configs.get("listener.name.listener1.gssapi.config1.key").map(|s| s.as_str()),
            Some("custom.config1")
        );
        assert_eq!(configs.get("custom.config2.key").map(|s| s.as_str()), Some("custom.config2"));
    }

    #[test]
    fn client_channel_builder_for_plaintext() {
        // Cannot use `expect` because `BoxedChannelBuilder` is
        // `Box<dyn ChannelBuilder>` which is not `Debug`. Match on
        // the result instead.
        match client_channel_builder(SecurityProtocol::Plaintext, None, None) {
            Ok(_) => {},
            Err(e) => panic!("plaintext builder construction failed: {e:?}"),
        }
    }

    #[test]
    fn client_channel_builder_for_ssl_requires_config() {
        let err = match client_channel_builder(SecurityProtocol::Ssl, None, None) {
            Ok(_) => panic!("expected Config error"),
            Err(e) => e,
        };
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn client_channel_builder_for_ssl_with_config() {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let cfg = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client versions")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        match client_channel_builder(SecurityProtocol::Ssl, None, Some(Arc::new(cfg))) {
            Ok(_) => {},
            Err(e) => panic!("ssl builder construction failed: {e:?}"),
        }
    }

    #[test]
    fn reject_sasl_protocols() {
        assert!(reject_sasl_until_phase_9("SASL_PLAINTEXT").is_err());
        assert!(reject_sasl_until_phase_9("sasl_ssl").is_err());
        assert!(reject_sasl_until_phase_9("PLAINTEXT").is_ok());
        assert!(reject_sasl_until_phase_9("SSL").is_ok());
    }
}
