# Phase 2: SSL/TLS Transport Layer — Implementation Plan

## Context

Milestone 3 adds SSL/TLS and SASL PLAIN authentication. Phase 1 (complete) implemented `SecurityProtocol`, `SslConfig`, `SaslConfig`, `SslClientAuth`. Phase 2 adds TLS transport using `tokio-rustls`, enabling the `SSL` security protocol.

The Java `SslTransportLayer` has complex buffer management (3 buffers, NEED_WRAP/NEED_UNWRAP state machine) because Java's `SSLEngine` is low-level. In Rust, `rustls`/`tokio-rustls` handles buffer management internally, making the implementation much simpler.

---

## Interface Changes Required

### 1. `ChannelBuilder::build_channel` — accept `TcpStream` instead of `Box<dyn TransportLayer>`

The Selector currently hardcodes `PlaintextTransportLayer::connected(stream)` and passes `Box<dyn TransportLayer>` to the builder. For SSL, the builder needs the raw `TcpStream` to wrap with TLS. This change aligns with Java's design where the builder gets the socket.

**Also add `peer_host: &str`** — needed for TLS SNI and hostname verification.

**File**: `src/common/network/channel_builder.rs`
```rust
// Before:
fn build_channel(&self, id: &str, transport_layer: Box<dyn TransportLayer>, ...) -> io::Result<KafkaChannel>;
// After:
fn build_channel(&self, id: &str, stream: TcpStream, peer_host: &str, ...) -> io::Result<KafkaChannel>;
```

### 2. `Selectable::connect` — add `peer_host: &str`

**File**: `src/common/network/selectable.rs` — add `peer_host: &str` parameter.

### 3. Cascade updates

| File | Change |
|------|--------|
| `src/common/network/plaintext_channel_builder.rs` | Create `PlaintextTransportLayer::connected(stream)` internally |
| `src/common/network/selector.rs` | Pass `stream` + `peer_host` to builder; ~13 test call sites add `"localhost"` |
| `src/common/network/mock_selector.rs` | Add `_peer_host: &str` parameter |
| `src/clients/network_client.rs` | Pass `node.host()` to `selector.connect()` |
| `tests/integration/*.rs` | ~8 call sites add `"localhost"` |

---

## New Files

### 1. `src/common/security/ssl/ssl_factory.rs` — SslFactory

Builds `Arc<rustls::ClientConfig>` from `SslConfig`. Rust equivalent of `SslFactory.java` + `DefaultSslEngineFactory.java` (simplified).

```rust
pub struct SslFactory {
    client_config: Arc<rustls::ClientConfig>,
    hostname_verification: bool,
}
```

**Methods**:
- `new(ssl_config: &SslConfig) -> io::Result<Self>` — Load CA certs (PEM file/inline/system roots via `webpki-roots`), optionally client cert+key for mTLS, configure TLS versions
- `create_tls_connector(&self) -> TlsConnector`
- `create_server_name(peer_host: &str) -> io::Result<ServerName<'static>>`

**Hostname verification**: When `endpoint_identification_algorithm` is empty, use a custom `ServerCertVerifier` (via `rustls::client::danger`) that validates cert chain but skips hostname matching.

**Format support**: PEM only (default). Return clear error for JKS/PKCS12 (Java-specific formats, can add PKCS12 later).

### 2. `src/common/network/ssl_transport_layer.rs` — SslTransportLayer

```rust
enum SslState {
    Handshaking { stream: Option<TcpStream>, connector: TlsConnector, domain: ServerName<'static> },
    Ready(tokio_rustls::client::TlsStream<TcpStream>),
    Closed,
}

pub struct SslTransportLayer {
    state: SslState,
    connected: bool,
    interest_ops: InterestOps,
}
```

**Key behavior**:
- `handshake()` → calls `connector.connect(domain, stream).await`, transitions Handshaking → Ready
- `ready()` → true only in Ready state
- `read()`/`write()` → delegate to `TlsStream` via `AsyncReadExt`/`AsyncWriteExt`
- `has_bytes_buffered()` → false initially (rustls handles internally; the selector retries next poll)
- `has_pending_writes()` → check `tls_stream.get_ref().1.wants_write()`
- `close()` → TLS shutdown + drop stream

**I/O pattern difference from Plaintext**: `TlsStream` doesn't expose `readable()`/`try_read()`. Use `AsyncReadExt::read()` / `AsyncWriteExt::write()` directly. The selector's existing poll flow handles non-blocking semantics.

### 3. `src/common/network/ssl_channel_builder.rs` — SslChannelBuilder

```rust
pub struct SslChannelBuilder {
    ssl_factory: SslFactory,
    listener_name: Option<ListenerName>,
}
```

- `build_channel()`: Creates `SslTransportLayer::new(stream, connector, domain)` + `PlaintextAuthenticator` (SSL auth is at transport layer level, matching Java's design)

### 4. `src/common/security/ssl/mod.rs` — module declaration

---

## Module Wiring

| File | Addition |
|------|----------|
| `src/common/security/mod.rs` | `pub mod ssl;` |
| `src/common/network/mod.rs` | `pub mod ssl_transport_layer;` + `pub mod ssl_channel_builder;` + re-exports |

---

## Dependencies (Cargo.toml)

```toml
rustls = { version = "0.23", features = ["logging", "std", "tls12"] }
tokio-rustls = "0.26"
rustls-pemfile = "2"
webpki-roots = "0.26"
```

Using `aws-lc-rs` crypto backend (default) for FIPS compliance.

---

## Implementation Order

1. Add crate dependencies → `cargo build`
2. Change `ChannelBuilder` + `Selectable` signatures
3. Update `PlaintextChannelBuilder`, `Selector`, `MockSelector`, `NetworkClient`
4. Update all test call sites (~21 total)
5. **Commit**: interface changes, verify all existing tests pass
6. Implement `SslFactory` with unit tests
7. Implement `SslTransportLayer` with unit tests
8. Implement `SslChannelBuilder`
9. Wire into module exports
10. **Commit**: SSL transport implementation
11. Format + lint checks

---

## Tests

### SslFactory unit tests (~11 tests)
- Build with system roots, PEM truststore (file + inline), client cert, unsupported format error
- Hostname verification enabled/disabled, server name parsing (DNS + IP)
- TLS version configuration, invalid PEM handling

### SslTransportLayer unit tests (~7 tests)
- Initial state not ready, read/write before handshake errors
- Close from handshaking/closed states, interest ops management
- Peer addr available before handshake

### SslChannelBuilder tests
- Build channel creates non-ready transport

Integration tests against real Kafka with TLS are Phase 6 scope.

---

## Risks

1. **`TlsStream` I/O pattern**: No `readable()`/`try_read()` — must use `AsyncReadExt` directly. Selector poll flow handles this.
2. **Handshake blocking poll**: `prepare()` is called without timeout, so TLS handshake blocks the poll loop for that cycle. Matches Java behavior — acceptable.
3. **Hostname verification disable**: Requires custom `ServerCertVerifier` via `rustls::client::danger` — most complex part of SslFactory.
4. **Test certificates**: Embed self-signed PEM certs as constants in test module.

---

## Java Source Reference

- `SslTransportLayer.java`: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java`
- `SslChannelBuilder.java`: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslChannelBuilder.java`
- `SslFactory.java`: `kafka/clients/src/main/java/org/apache/kafka/common/security/ssl/SslFactory.java`
- `DefaultSslEngineFactory.java`: `kafka/clients/src/main/java/org/apache/kafka/common/security/ssl/DefaultSslEngineFactory.java`

---

## Verification

1. `cargo build` — compiles with new dependencies
2. `cargo test` — all existing tests pass with interface changes + new SSL tests pass
3. `cargo xtask format-check` — properly formatted
4. `cargo xtask lint` — no clippy warnings