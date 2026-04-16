# Milestone 3: SSL/TLS + SASL PLAIN Authentication

## Overview

Add SSL/TLS encryption and SASL PLAIN authentication to the Kafka client, supporting the `SASL_PLAINTEXT` and `SASL_SSL` security protocols. This enables connecting to Kafka clusters that require authentication.

## Scope

**In scope:**
- `PLAINTEXT` (already done), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` security protocols
- SASL PLAIN mechanism only (username/password)
- TLS transport using `tokio-rustls` (pure Rust, no OpenSSL dependency)
- Client-side only (no server authenticator)

**Out of scope (future milestones):**
- SASL SCRAM-SHA-256/512
- SASL GSSAPI (Kerberos)
- SASL OAUTHBEARER
- Mutual TLS (client certificate authentication)
- Re-authentication
- Delegation tokens

## Rust-Specific Adaptations

| Java | Rust |
|------|------|
| `javax.net.ssl.SSLEngine` | `tokio-rustls` (wraps `rustls`) |
| `javax.security.sasl.SaslClient` | Direct SASL PLAIN implementation (protocol is trivial: `\0username\0password`) |
| JAAS config files + `LoginModule` | Simple `SaslConfig` struct with `username`/`password` fields |
| `javax.security.auth.Subject` | Not needed — credentials passed directly |
| `LoginManager` + `LoginContext` | Not needed — no JAAS framework in Rust |
| `SaslClientCallbackHandler` | Not needed — credentials resolved directly from config |

The Java SASL architecture has many layers of indirection (JAAS, LoginModules, CallbackHandlers, Subjects) because it must support pluggable security frameworks. In Rust, we keep the same external behavior and wire protocol but simplify the internal plumbing.

## Dependencies

New crate dependencies:
- `tokio-rustls` — async TLS for Tokio (wraps `rustls`)
- `rustls` — pure-Rust TLS implementation
- `rustls-pemfile` — PEM file parsing for certificates/keys
- `webpki-roots` or `rustls-native-certs` — system CA certificate store

## Implementation Phases

### Phase 1: Security Protocol & Configuration

**Goal:** Define the security protocol enum and configuration types.

#### Classes to translate:

| Java Class | Rust Module | Notes |
|---|---|---|
| `SecurityProtocol` | `common/security/security_protocol.rs` | Enum: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL |
| `SslConfigs` | `common/config/ssl_configs.rs` | SSL configuration constants and defaults |
| `SaslConfigs` | `common/config/sasl_configs.rs` | SASL configuration constants |
| `SslClientAuth` | `common/config/ssl_client_auth.rs` | Enum: REQUIRED, REQUESTED, NONE |

**Simplifications:**
- `SslConfigs` in Java is a class with static string constants. In Rust, define a `SslConfig` struct with typed fields (truststore path, keystore path, protocol versions, etc.)
- `SaslConfigs` → `SaslConfig` struct with `mechanism: String`, `username: String`, `password: String` instead of JAAS config parsing
- Skip `JaasConfig`, `JaasContext`, `JaasUtils` — parse `sasl.jaas.config` string directly into username/password for PLAIN

#### Estimated classes: 4

---

### Phase 2: SSL/TLS Transport Layer

**Goal:** Implement TLS transport using `tokio-rustls`, enabling the `SSL` security protocol.

#### Classes to translate:

| Java Class | Rust Module | Notes |
|---|---|---|
| `SslTransportLayer` | `common/network/ssl_transport_layer.rs` | TLS transport using `tokio-rustls::TlsStream` |
| `SslFactory` | `common/security/ssl/ssl_factory.rs` | Creates `rustls::ClientConfig` from `SslConfig` |
| `DefaultSslEngineFactory` | (merged into `SslFactory`) | In Java this is a pluggable factory; in Rust, inline the default behavior |
| `SslChannelBuilder` | `common/network/ssl_channel_builder.rs` | Creates `KafkaChannel` with `SslTransportLayer` |

**Key design decisions:**
- `SslTransportLayer` wraps `tokio_rustls::client::TlsStream<TcpStream>` and implements our `TransportLayer` trait
- `SslFactory` builds a `rustls::ClientConfig`:
  - Loads CA certs from truststore (PEM or system roots)
  - Optionally loads client cert + key from keystore (for mutual TLS, future)
  - Configures TLS protocol versions and cipher suites
- `SslChannelBuilder` implements `ChannelBuilder` trait, creates channels with TLS
- The TLS handshake is performed during `KafkaChannel` connect, before the channel is marked as ready

**Rust implementation of `SslTransportLayer`:**
```rust
pub struct SslTransportLayer {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
    // ... position tracking for partial reads/writes
}

#[async_trait]
impl TransportLayer for SslTransportLayer {
    async fn ready(&self) -> bool { true } // TLS handshake done at creation
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { ... }
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> { ... }
    // ...
}
```

#### Estimated classes: 3 (SslTransportLayer, SslFactory, SslChannelBuilder)

---

### Phase 3: SASL Request/Response Types

**Goal:** Add the SASL handshake and authenticate request/response builders (same pattern as ApiVersions/Metadata).

#### Classes to translate:

| Java Class | Rust Module | Notes |
|---|---|---|
| `SaslHandshakeRequest` | `common/requests/sasl_handshake_request.rs` | Builder + request wrapper |
| `SaslHandshakeResponse` | `common/requests/sasl_handshake_response.rs` | Response wrapper |
| `SaslAuthenticateRequest` | `common/requests/sasl_authenticate_request.rs` | Builder + request wrapper |
| `SaslAuthenticateResponse` | `common/requests/sasl_authenticate_response.rs` | Response wrapper |

**Notes:**
- Generated message types (`SaslHandshakeRequestData`, etc.) are already done
- Follow the same pattern as `ApiVersionsRequest`/`MetadataRequest`
- Update `ConcreteRequest`/`ConcreteResponse` enums to include SASL variants

#### Estimated classes: 4

---

### Phase 4: SASL Client Authenticator

**Goal:** Implement the SASL authentication state machine for the client side.

#### Classes to translate:

| Java Class | Rust Module | Notes |
|---|---|---|
| `SaslClientAuthenticator` | `common/security/authenticator/sasl_client_authenticator.rs` | Core state machine |
| `SaslChannelBuilder` | `common/network/sasl_channel_builder.rs` | Creates channels with SASL authenticator |

**SASL PLAIN wire protocol:**
The SASL PLAIN token is simply: `\0<username>\0<password>` (RFC 4616). No challenge-response needed.

**State machine (simplified for PLAIN):**
```
SEND_API_VERSIONS_REQUEST          ← sent by the authenticator itself, not NetworkClient
  → RECEIVE_API_VERSIONS_RESPONSE  ← determines SaslAuthenticate version
  → SEND_HANDSHAKE_REQUEST         ← sends mechanism name "PLAIN"
  → RECEIVE_HANDSHAKE_RESPONSE     ← confirms mechanism is supported
  → SEND_AUTHENTICATE_REQUEST      ← sends PLAIN token: \0username\0password
  → RECEIVE_AUTHENTICATE_RESPONSE  ← success or error
  → COMPLETE
```

**Important:** The ApiVersions exchange happens *inside* the authenticator during channel
setup, before the channel is marked ready. The NetworkClient's own ApiVersions handshake
(used for version negotiation of application requests) happens separately after the
channel is ready. This means the broker sees two ApiVersions requests: one from the
authenticator and one from the client.

**Java simplifications:**
- Skip `javax.security.sasl.SaslClient` — implement PLAIN token generation directly
- Skip `LoginManager`, `AbstractLogin`, `DefaultLogin`, `LoginContext` — credentials come from config
- Skip `SaslClientCallbackHandler` — no callback indirection needed
- Skip `PlainLoginModule` — username/password from `SaslConfig` struct directly
- Skip `JaasContext`, `JaasConfig` — parse `sasl.jaas.config` if provided, else use direct config

**Rust implementation:**
```rust
pub struct SaslClientAuthenticator {
    state: SaslState,
    mechanism: String,      // "PLAIN"
    username: String,
    password: String,
    node_id: String,
    // ... transport layer reference for sending/receiving
}

impl Authenticator for SaslClientAuthenticator {
    async fn authenticate(&mut self) -> Result<(), KafkaError> {
        // State machine: ApiVersions → Handshake → Authenticate → Complete
    }
    fn complete(&self) -> bool { self.state == SaslState::Complete }
    fn principal(&self) -> &KafkaPrincipal { ... }
}
```

**`SaslChannelBuilder`:**
- For `SASL_PLAINTEXT`: creates `PlaintextTransportLayer` + `SaslClientAuthenticator`
- For `SASL_SSL`: creates `SslTransportLayer` + `SaslClientAuthenticator`
- The authenticator runs AFTER the transport layer is connected (and after TLS handshake for SASL_SSL)

#### Estimated classes: 2

---

### Phase 5: Integration & Wiring

**Goal:** Wire everything together — update ChannelBuilder selection, Selector, and NetworkClient.

#### Changes to existing code:

| File | Change |
|---|---|
| `common/network/channel_builder.rs` | Already a trait; add `SslChannelBuilder` and `SaslChannelBuilder` as implementations |
| `common/network/kafka_channel.rs` | Update to run authenticator after connect (currently only PlaintextAuthenticator which is a no-op) |
| `common/network/selector.rs` | No changes needed — already uses `ChannelBuilder` trait |
| `clients/network_client.rs` | Pass `SecurityProtocol` and credentials to channel builder construction |

**Channel builder selection logic:**
```rust
fn create_channel_builder(protocol: SecurityProtocol, config: &ClientConfig) -> Box<dyn ChannelBuilder> {
    match protocol {
        SecurityProtocol::Plaintext => Box::new(PlaintextChannelBuilder::new()),
        SecurityProtocol::Ssl => Box::new(SslChannelBuilder::new(ssl_config)),
        SecurityProtocol::SaslPlaintext => Box::new(SaslChannelBuilder::new(sasl_config, None)),
        SecurityProtocol::SaslSsl => Box::new(SaslChannelBuilder::new(sasl_config, Some(ssl_config))),
    }
}
```

---

### Phase 6: Integration Tests

**Goal:** Verify SSL and SASL PLAIN against a real Kafka broker in Docker.

#### Test matrix:

| Test | Protocol | Mechanism | What it verifies |
|---|---|---|---|
| `test_ssl_connection` | SSL | — | TLS handshake, ApiVersions over TLS |
| `test_sasl_plaintext_connection` | SASL_PLAINTEXT | PLAIN | SASL handshake, auth, metadata fetch |
| `test_sasl_ssl_connection` | SASL_SSL | PLAIN | TLS + SASL combined |
| `test_sasl_wrong_credentials` | SASL_PLAINTEXT | PLAIN | Authentication failure handling |
| `test_sasl_unsupported_mechanism` | SASL_PLAINTEXT | — | Error when requesting unsupported mechanism |

#### Docker setup:
- Configure testcontainers Kafka with SASL_PLAINTEXT listener
- Use environment variables to enable PLAIN mechanism with test credentials
- For SSL tests, generate self-signed CA + broker certificate

---

## Class Summary

| Phase | New Classes | Modified Classes |
|---|---|---|
| Phase 1: Config | 4 | 0 |
| Phase 2: SSL Transport | 3 | 0 |
| Phase 3: SASL Requests | 4 | 2 (ConcreteRequest, ConcreteResponse) |
| Phase 4: SASL Authenticator | 2 | 0 |
| Phase 5: Wiring | 0 | 2-3 (kafka_channel, network_client) |
| Phase 6: Tests | 5 tests | 2 (cluster_config, kafka_cluster) |
| **Total** | **~13 classes** | **~6 modified** |

## Excluded Java Classes (not needed in Rust)

These Java classes exist in the dependency graph but are not needed because Rust's simpler architecture replaces them:

| Java Class | Why excluded |
|---|---|
| `LoginManager` | JAAS lifecycle management — credentials from config struct |
| `AbstractLogin` / `DefaultLogin` | JAAS login abstraction — not needed |
| `LoginContext` | javax.security.auth — not applicable |
| `JaasContext` / `JaasConfig` / `JaasUtils` | JAAS config parsing — simplified to direct config |
| `SaslClientCallbackHandler` | Callback indirection — credentials resolved directly |
| `PlainLoginModule` | JAAS LoginModule — credentials from config struct |
| `PlainAuthenticateCallback` | Server-side only |
| `KafkaPrincipalBuilder` / `DefaultKafkaPrincipalBuilder` | Server-side principal resolution |
| `SaslServerAuthenticator` / `SaslServerCallbackHandler` | Server-side only |
| `PlainSaslServer` / `PlainServerCallbackHandler` | Server-side only |
| `CredentialCache` | Server-side credential storage |
| `SaslInternalConfigs` | Internal server configs |
| `SecurityManagerCompatibility` | Java SecurityManager — deprecated, not applicable |
| `SecurityConfig` / `BrokerSecurityConfigs` | Server/broker configuration |
| All Kerberos classes | Out of scope |
| All OAuth classes | Out of scope |
| All SCRAM classes | Out of scope |
| All delegation token classes | Out of scope |

## Java Source Reference

- Network: `kafka/clients/src/main/java/org/apache/kafka/common/network/`
- Security: `kafka/clients/src/main/java/org/apache/kafka/common/security/`
- Authenticator: `kafka/clients/src/main/java/org/apache/kafka/common/security/authenticator/`
- PLAIN: `kafka/clients/src/main/java/org/apache/kafka/common/security/plain/`
- SSL: `kafka/clients/src/main/java/org/apache/kafka/common/security/ssl/`
- Requests: `kafka/clients/src/main/java/org/apache/kafka/common/requests/`
