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
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;

use crate::common::errors::KafkaError;
use crate::common::network::authenticator::ChannelAuthenticator;
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::kafka_channel::BoxedMetadataRegistry;
use crate::common::network::{KafkaChannel, ListenerName, PlaintextTransportLayer, SslTransportLayer};
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::authenticator::{PlainCredentials, SaslClientAuthenticator};

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
    /// Configured client id, threaded into each
    /// [`SaslClientAuthenticator`] for correlation-id labeling.
    client_id: String,
    /// PLAIN credentials, cloned into each `SaslClientAuthenticator`
    /// for the lifetime of the channel.
    credentials: PlainCredentials,
    /// Pre-built rustls config. Required when
    /// `security_protocol == SaslSsl`, ignored otherwise. Owned via `Arc`
    /// so multiple channels can share the same trust roots.
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

    /// Borrow the configured client id.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Build a SASL_SSL channel against a specific `server_name` (used
    /// for SNI and certificate verification). Mirrors
    /// [`crate::common::network::SslChannelBuilder::build_ssl_channel`]
    /// in shape — the trait's [`ChannelBuilder::build_channel`] cannot
    /// take a `server_name` (the producer's `Selector` knows it from the
    /// resolved bootstrap address), so SASL_SSL callers must use this
    /// typed entry point.
    ///
    /// Returns `KafkaError::IllegalState` if invoked on a
    /// `SaslPlaintext` builder (which has no SSL config); use
    /// [`ChannelBuilder::build_channel`] for that case.
    pub fn build_sasl_ssl_channel(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        server_name: ServerName<'static>,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        if self.security_protocol != SecurityProtocol::SaslSsl {
            return Err(KafkaError::IllegalState(format!(
                "build_sasl_ssl_channel called on a {} builder; use build_channel instead",
                self.security_protocol.name()
            )));
        }
        let ssl_config = self.ssl_config.as_ref().ok_or_else(|| {
            // Construction validated this — reaching here would be a bug.
            KafkaError::IllegalState("SaslSsl builder must have an ssl_config".to_owned())
        })?;
        let transport = SslTransportLayer::new(id.as_ref(), stream, Arc::clone(ssl_config), server_name)
            .map_err(|e| KafkaError::Network(e.to_string()))?;
        let authenticator = self.build_sasl_authenticator(id.as_ref())?;
        Ok(KafkaChannel::new(
            id,
            Box::new(transport),
            ChannelAuthenticator::sasl(authenticator),
            max_receive_size,
            metadata_registry,
        ))
    }

    /// Construct a fresh [`SaslClientAuthenticator`] using this
    /// builder's mechanism + credentials + client id. The `node` value
    /// is the channel's connection id (used by the authenticator only
    /// for logging).
    fn build_sasl_authenticator(&self, node: &str) -> Result<SaslClientAuthenticator, KafkaError> {
        SaslClientAuthenticator::new(node, &self.client_id, &self.client_sasl_mechanism, self.credentials.clone())
    }
}

impl ChannelBuilder for SaslChannelBuilder {
    /// Construct a [`KafkaChannel`] wrapping a SASL-authenticated
    /// transport. Mirrors Java's `SaslChannelBuilder.buildChannel()`:
    ///
    /// - For `SaslPlaintext`: wraps a [`PlaintextTransportLayer`] over
    ///   the TCP stream + a fresh [`SaslClientAuthenticator`].
    /// - For `SaslSsl`: the trait method cannot carry an SNI server
    ///   name (the [`ChannelBuilder`] trait is shared with the
    ///   plaintext/SSL builders which have the same signature shape),
    ///   so this returns
    ///   `KafkaError::IllegalState("...use build_sasl_ssl_channel
    ///   instead")` — callers must use
    ///   [`Self::build_sasl_ssl_channel`] which takes the
    ///   `ServerName`. This mirrors the same shape used by
    ///   [`crate::common::network::SslChannelBuilder::build_ssl_channel`].
    fn build_channel(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        match self.security_protocol {
            SecurityProtocol::SaslPlaintext => {
                let transport = PlaintextTransportLayer::new(stream);
                let authenticator = self.build_sasl_authenticator(id.as_ref())?;
                Ok(KafkaChannel::new(
                    id,
                    Box::new(transport),
                    ChannelAuthenticator::sasl(authenticator),
                    max_receive_size,
                    metadata_registry,
                ))
            },
            SecurityProtocol::SaslSsl => Err(KafkaError::IllegalState(
                "SaslChannelBuilder requires a server name for SASL_SSL; \
                     call build_channel_with_server_name(...) instead"
                    .to_owned(),
            )),
            // Phase 9b: construction-time validation ensures the
            // builder is only created with a SASL protocol, so this
            // arm is unreachable — but defensive code keeps the
            // exhaustive match honest.
            other => Err(KafkaError::IllegalState(format!(
                "SaslChannelBuilder constructed with non-SASL protocol {other:?} — should not happen"
            ))),
        }
    }

    /// SNI-aware variant. Dispatches by inner security protocol:
    /// * `SaslPlaintext` ignores `server_name` and delegates to
    ///   [`Self::build_channel`].
    /// * `SaslSsl` requires `Some(server_name)` and calls
    ///   [`Self::build_sasl_ssl_channel`].
    fn build_channel_with_server_name(
        &self,
        id: Arc<str>,
        stream: TcpStream,
        server_name: Option<ServerName<'static>>,
        max_receive_size: i32,
        metadata_registry: BoxedMetadataRegistry,
    ) -> Result<KafkaChannel, KafkaError> {
        match self.security_protocol {
            SecurityProtocol::SaslPlaintext => self.build_channel(id, stream, max_receive_size, metadata_registry),
            SecurityProtocol::SaslSsl => {
                let server_name = server_name.ok_or_else(|| {
                    KafkaError::IllegalState(
                        "SaslChannelBuilder for SASL_SSL requires a server name; \
                         build_channel_with_server_name called with None"
                            .to_owned(),
                    )
                })?;
                self.build_sasl_ssl_channel(id, stream, server_name, max_receive_size, metadata_registry)
            },
            other => Err(KafkaError::IllegalState(format!(
                "SaslChannelBuilder constructed with non-SASL protocol {other:?} — should not happen"
            ))),
        }
    }

    fn close(&mut self) {
        // No long-lived resource on the builder side. Java's
        // `LoginManager.release()` / `AuthenticateCallbackHandler.close()` /
        // `SslFactory.close()` are server-side / re-auth concerns
        // beyond Milestone-1 scope.
    }
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

    /// `build_channel` on a `SaslPlaintext` builder yields a working
    /// channel: connection id matches, channel is *not* yet ready
    /// (SASL authenticator is still in `SendApiVersionsRequest` state),
    /// principal lookup works.
    #[tokio::test]
    async fn build_channel_constructs_sasl_plaintext_channel() {
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
            PlainCredentials::new("alice", "supersecret"),
            None,
        )
        .expect("builder construction");

        let channel = builder
            .build_channel(Arc::from("0"), stream, 1024, Box::new(DefaultChannelMetadataRegistry::new()))
            .expect("Phase 9b: build_channel must succeed for SASL_PLAINTEXT");
        assert_eq!(channel.id(), "0");
        // SASL authenticator starts at SendApiVersionsRequest — until
        // it drives through, the channel is NOT ready. Mirrors Java's
        // `channel.ready() == false` until `prepare()` advances.
        assert!(!channel.ready(), "channel must not be ready before SASL handshake completes");
        // Principal lookup defers to authenticator; SASL returns
        // anonymous on the client side (matches PlaintextAuthenticator).
        let _ = channel.principal();
    }

    /// `build_channel` on a `SaslSsl` builder must reject — callers
    /// must use the typed `build_sasl_ssl_channel(server_name, ...)`
    /// entry point that carries the SNI hostname.
    #[tokio::test]
    async fn build_channel_rejects_sasl_ssl_without_server_name() {
        use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, _accepted) = tokio::join!(connect, accept);
        let stream = client.expect("connect");

        let builder = SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            None,
            "PLAIN",
            "test-client",
            PlainCredentials::new("alice", "p"),
            Some(sample_ssl_config()),
        )
        .expect("builder construction");

        let err = builder
            .build_channel(Arc::from("0"), stream, 1024, Box::new(DefaultChannelMetadataRegistry::new()))
            .expect_err("SASL_SSL must reject build_channel without server name");
        assert!(matches!(err, KafkaError::IllegalState(_)));
        assert!(err.message().contains("build_channel_with_server_name"));
    }

    /// `build_sasl_ssl_channel` on a `SaslPlaintext` builder must
    /// reject — the typed entry point is exclusively for `SaslSsl`.
    #[tokio::test]
    async fn build_sasl_ssl_channel_rejects_sasl_plaintext() {
        use crate::common::network::channel_metadata_registry::DefaultChannelMetadataRegistry;
        use rustls::pki_types::ServerName;
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

        let server_name = ServerName::try_from("localhost").expect("server name");
        let err = builder
            .build_sasl_ssl_channel(
                Arc::from("0"),
                stream,
                server_name,
                1024,
                Box::new(DefaultChannelMetadataRegistry::new()),
            )
            .expect_err("SASL_PLAINTEXT must reject build_sasl_ssl_channel");
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }
}
