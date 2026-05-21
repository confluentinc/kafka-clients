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

//! Translation of `org.apache.kafka.common.network.SaslChannelBuilder`.

use std::sync::Arc;

use rustls::ClientConfig;
use tokio::net::TcpStream;

use crate::common::errors::KafkaError;
use crate::common::network::authenticator::PlaintextAuthenticator;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::connection_mode::ConnectionMode;
use crate::common::network::kafka_channel::BoxedMetadataRegistry;
use crate::common::network::{KafkaChannel, ListenerName, PlaintextTransportLayer, SslTransportLayer};
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::authenticator::PlainCredentials;

/// Builds [`KafkaChannel`]s wrapping a SASL-authenticated transport.
/// Mirrors Java's `SaslChannelBuilder`.
///
/// **Phase 9a scope.** PLAIN mechanism only. The `SaslClientAuthenticator`
/// created here drives the SASL exchange after the underlying transport
/// is ready. SCRAM, OAUTHBEARER, Kerberos/GSSAPI are rejected at the
/// config validation boundary (Phase 9b).
///
/// **Java→Rust signature differences:**
///
/// 1. Java's constructor takes a sprawling parameter list:
///    `(ConnectionMode, Map<String, JaasContext>, SecurityProtocol,
///    ListenerName, boolean, String clientSaslMechanism, CredentialCache,
///    DelegationTokenCache, String sslClientAuthOverride, Time,
///    LogContext, Function<Short, ApiVersionsResponse>)`. All of those
///    except `SecurityProtocol`, `listenerName`, `clientSaslMechanism`,
///    and the credentials are out-of-scope for Phase 9a:
///    - `Map<String, JaasContext>` — JAAS parsing deferred (Phase 9b).
///      Phase 9a takes an explicit [`PlainCredentials`] instead.
///    - `CredentialCache`, `DelegationTokenCache` — server-side.
///    - `Time`, `LogContext` — re-authentication / structured logging.
///    - `apiVersionSupplier` — server-side.
/// 2. Java's `Configurable.configure(Map<String, ?> configs)` is
///    folded into the constructor; Rust takes typed fields.
/// 3. SSL is configured via a pre-built `rustls::ClientConfig`
///    (consistent with `SslChannelBuilder`); Java configures `SslFactory`
///    inside `configure()`.
pub struct SaslChannelBuilder {
    security_protocol: SecurityProtocol,
    listener_name: Option<ListenerName>,
    /// Configured SASL mechanism. PLAIN-only at this milestone.
    client_sasl_mechanism: String,
    /// Configured client id (also used as Authenticator's `client_id`).
    /// Used inside `build_channel` once Phase 9b lands the wiring; the
    /// field is kept on the builder so construction can validate it now.
    #[allow(dead_code)]
    client_id: String,
    /// PLAIN credentials. Phase 9b will route through a typed
    /// `SaslConfigs` struct sourced from `ProducerConfig`. Used by
    /// `build_channel` once Phase 9b lands the wiring.
    #[allow(dead_code)]
    credentials: PlainCredentials,
    /// Pre-built rustls config. Required when
    /// `security_protocol == SaslSsl`, ignored otherwise. Owned via `Arc`
    /// so multiple channels can share the same trust roots. Used by
    /// `build_channel` once Phase 9b lands the wiring.
    #[allow(dead_code)]
    ssl_config: Option<Arc<ClientConfig>>,
}

impl std::fmt::Debug for SaslChannelBuilder {
    /// Hand-emitted to inherit [`PlainCredentials`]'s password
    /// redaction. Deriving would print the raw password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslChannelBuilder")
            .field("security_protocol", &self.security_protocol)
            .field("listener_name", &self.listener_name)
            .field("client_sasl_mechanism", &self.client_sasl_mechanism)
            .field("client_id", &self.client_id)
            .field("credentials", &self.credentials)
            .field("ssl_config", &self.ssl_config.as_ref().map(|_| "<rustls::ClientConfig>"))
            .finish()
    }
}

impl SaslChannelBuilder {
    /// Construct a SASL channel builder. Mirrors Java's constructor for
    /// the producer-side use case (`ConnectionMode::Client`) with the
    /// `JaasContext` / `LoginManager` indirection collapsed.
    ///
    /// Returns `KafkaError::Config` for any mechanism other than PLAIN
    /// (Phase 9a scope) and for SASL_SSL without an `ssl_config`.
    pub fn new(
        security_protocol: SecurityProtocol,
        listener_name: Option<ListenerName>,
        client_sasl_mechanism: impl Into<String>,
        client_id: impl Into<String>,
        credentials: PlainCredentials,
        ssl_config: Option<Arc<ClientConfig>>,
    ) -> Result<Self, KafkaError> {
        if !security_protocol.is_sasl() {
            return Err(KafkaError::Config(format!(
                "SaslChannelBuilder requires a SASL security protocol; got {}",
                security_protocol.name()
            )));
        }
        let mechanism = client_sasl_mechanism.into();
        if mechanism != "PLAIN" {
            return Err(KafkaError::Config(format!(
                "Unsupported SASL mechanism: {mechanism}. Phase 9 supports only PLAIN."
            )));
        }
        if security_protocol == SecurityProtocol::SaslSsl && ssl_config.is_none() {
            return Err(KafkaError::Config(
                "ssl_config is required when security.protocol = SASL_SSL".to_owned(),
            ));
        }
        Ok(SaslChannelBuilder {
            security_protocol,
            listener_name,
            client_sasl_mechanism: mechanism,
            client_id: client_id.into(),
            credentials,
            ssl_config,
        })
    }

    /// Configured SASL mechanism (PLAIN-only in Phase 9a).
    pub fn client_sasl_mechanism(&self) -> &str {
        &self.client_sasl_mechanism
    }

    /// Configured listener name (always `None` on the client side).
    pub fn listener_name(&self) -> Option<&ListenerName> {
        self.listener_name.as_ref()
    }

    /// Configured security protocol — either `SaslPlaintext` or `SaslSsl`.
    pub fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }
}

impl ChannelBuilder for SaslChannelBuilder {
    /// Construct a [`KafkaChannel`] with the appropriate transport
    /// (`PlaintextTransportLayer` for `SaslPlaintext`, `SslTransportLayer`
    /// for `SaslSsl`) and a `PlaintextAuthenticator` *placeholder*.
    ///
    /// **Phase 9a deviation from Java:** the
    /// [`crate::common::security::authenticator::SaslClientAuthenticator`]
    /// state machine is implemented and unit-tested, but the Phase 5b-3
    /// `KafkaChannel` carries a single `Authenticator` trait object that
    /// must implement the network-layer trait (sync `authenticate()` +
    /// `principal()` + `complete()` + `close()`). Wiring the SASL
    /// authenticator through the channel's trait requires either (a)
    /// extending the trait to thread the transport through `authenticate()`
    /// or (b) using interior mutability + a transport reference held
    /// by the authenticator. Both are intrusive; Phase 9a keeps the
    /// authenticator standalone (with its full state machine + tests),
    /// and Phase 9b will land the channel-side wiring once the
    /// `Authenticator` trait surface is finalised. **For now this method
    /// returns a `KafkaError::UnsupportedOperation` if invoked; instantiate
    /// the SASL authenticator directly via
    /// [`crate::common::security::authenticator::SaslClientAuthenticator::new`]
    /// for unit tests.**
    ///
    /// The intent of landing the builder + dispatch in Phase 9a is to
    /// keep the registry in `channel_builders.rs` symmetric across all
    /// four security protocols; Phase 9b finishes the integration.
    fn build_channel(
        &self,
        _id: Arc<str>,
        _stream: TcpStream,
        _max_receive_size: i32,
        _metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        // Phase 9b will replace this with the actual wiring:
        //   let transport = match self.security_protocol {
        //       SaslPlaintext => Box::new(PlaintextTransportLayer::new(stream)),
        //       SaslSsl       => Box::new(SslTransportLayer::new_client(...)),
        //   };
        //   let authenticator = SaslClientAuthenticator::new(...);
        //   KafkaChannel::new(..., Box::new(SaslChannelAuthenticator::wrap(authenticator, transport_ref)), ...)
        //
        // The blocker is that `Authenticator::authenticate(&mut self)` does
        // not take a transport reference — it expects the impl to own
        // (or hold a ref to) the transport. Reshaping the trait is out
        // of Phase 9a scope because doing so would also touch the
        // Phase 5b PLAINTEXT and SSL authenticators which currently
        // share the trait.
        Err(KafkaError::UnsupportedOperation(
            "SaslChannelBuilder::build_channel: SASL-over-channel wiring lands in Phase 9b. \
             The Phase 9a SaslClientAuthenticator is fully implemented and unit-tested, \
             but the Phase 5b-3 Authenticator trait surface requires extension before the \
             channel can host it. Instantiate SaslClientAuthenticator directly for unit \
             tests."
                .to_owned(),
        ))
    }

    fn close(&mut self) {
        // No long-lived resource on the builder side. Java's
        // `LoginManager.release()` / `AuthenticateCallbackHandler.close()` /
        // `SslFactory.close()` are Phase 9b concerns.
    }
}

// Silence "unused" warnings for fields/types that are used only
// in code paths Phase 9b will wire up. `cargo build` would warn
// otherwise; this annotation keeps the impl block honest about
// the deferral.
#[allow(dead_code)]
fn _phase_9b_compile_assertion() {
    let _ = std::marker::PhantomData::<(PlaintextTransportLayer, SslTransportLayer)>;
    let _ = ConnectionMode::Client;
    let _ = PlaintextAuthenticator::new();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ssl_config() -> Arc<ClientConfig> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let cfg = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client versions")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        Arc::new(cfg)
    }

    #[test]
    fn construction_accepts_sasl_plaintext_plain() {
        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "supersecret"),
            None,
        )
        .expect("SASL_PLAINTEXT + PLAIN must be accepted");
        assert_eq!(builder.client_sasl_mechanism(), "PLAIN");
        assert_eq!(builder.security_protocol(), SecurityProtocol::SaslPlaintext);
        assert!(builder.listener_name().is_none());
    }

    #[test]
    fn construction_accepts_sasl_ssl_plain_with_ssl_config() {
        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "supersecret"),
            Some(sample_ssl_config()),
        )
        .expect("SASL_SSL + PLAIN + ssl_config must be accepted");
        assert_eq!(builder.security_protocol(), SecurityProtocol::SaslSsl);
    }

    #[test]
    fn construction_rejects_non_sasl_protocol() {
        let err = SaslChannelBuilder::new(
            SecurityProtocol::Plaintext,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "p"),
            None,
        )
        .expect_err("must reject non-SASL protocol");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("requires a SASL security protocol"));
    }

    #[test]
    fn construction_rejects_non_plain_mechanism() {
        let err = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            None,
            "SCRAM-SHA-512",
            "test-client",
            PlainCredentials::new("alice", "p"),
            None,
        )
        .expect_err("must reject SCRAM");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("Unsupported SASL mechanism: SCRAM-SHA-512"));
    }

    #[test]
    fn construction_rejects_sasl_ssl_without_ssl_config() {
        let err = SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "p"),
            None,
        )
        .expect_err("SASL_SSL without ssl_config must fail");
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message()
                .contains("ssl_config is required when security.protocol = SASL_SSL")
        );
    }

    /// `build_channel` returns `KafkaError::UnsupportedOperation` —
    /// Phase 9b wires the channel-side hookup. Pin the deferral so
    /// future contributors don't silently break the contract.
    #[tokio::test]
    async fn build_channel_returns_unsupported_operation_in_phase_9a() {
        use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, _accepted) = tokio::join!(connect, accept);
        let stream = client.expect("connect");

        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "p"),
            None,
        )
        .expect("builder construction");

        let err = builder
            .build_channel(Arc::from("0"), stream, 1024, Box::new(DefaultChannelMetadataRegistry::new()))
            .expect_err("Phase 9a defers channel wiring to 9b");
        assert!(matches!(err, KafkaError::UnsupportedOperation(_)));
        assert!(err.message().contains("Phase 9b"));
    }
}
