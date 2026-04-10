# Phase 4: SASL Client Authenticator — Implementation Plan

## Context

Milestone 3 adds SSL/TLS and SASL PLAIN authentication. Phases 1-3 are complete (SecurityProtocol/configs, SSL transport, SASL request/response types). Phase 4 implements the SASL authentication state machine and SaslChannelBuilder — the final code needed before Phase 5 wires everything together and Phase 6 adds integration tests.

## Scope

Translate 2 Java classes (SaslClientAuthenticator, SaslChannelBuilder) into Rust, with a prerequisite refactoring of the Authenticator trait. No Java test files exist for these classes.

## Part 1: Prerequisite — Refactor Authenticator Trait

### Problem
`Authenticator::authenticate()` is currently synchronous with no transport access:
```rust
fn authenticate(&mut self) -> io::Result<()>;  // line 47 of authenticator.rs
```
The SASL authenticator must read/write through the transport layer during authentication.

### Solution
Change signature to accept transport and return a boxed future:
```rust
fn authenticate<'a>(
    &'a mut self,
    transport: &'a mut (dyn TransportLayer + Send),
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
```

In `KafkaChannel::prepare()`, use split borrowing (separate struct fields):
```rust
let auth = &mut *self.authenticator;
let transport = &mut *self.transport_layer;
auth.authenticate(transport).await?;
```

### Files to Modify

**`src/common/network/authenticator.rs`**
- Add imports: `Future`, `Pin`, `TransportLayer`
- Change trait method signature
- Update `PlaintextAuthenticator`: add `_transport` param, return `Box::pin(async { Ok(()) })`

**`src/common/network/kafka_channel.rs`**
- Update `prepare()` at line 185: split borrow + `.await`
- Update `MockAuthenticator` in test module (line ~805)

### Verification
`cargo test` — all 454 existing tests must pass. No behavioral change.

---

## Part 2: SaslClientAuthenticator

**New file:** `src/common/security/authenticator/sasl_client_authenticator.rs`

**Java source:** `SaslClientAuthenticator.java` (712 lines, translating client-only, PLAIN-only subset)

### State Machine

```
SendApiVersionsRequest → ReceiveApiVersionsResponse
  → SendHandshakeRequest → ReceiveHandshakeResponse
  → Initial (send PLAIN token) → Intermediate (receive response)
  → ClientComplete (if using SaslAuthenticate header) → Complete
```

Preserves Java's state names (Initial/Intermediate/ClientComplete) for future SCRAM/challenge-response extensibility.

### Struct
```rust
pub struct SaslClientAuthenticator {
    state: SaslState,
    mechanism: String,
    username: String,
    password: String,
    node: String,
    host: String,
    client_id: String,
    correlation_id: i32,
    sasl_handshake_version: i16,
    sasl_authenticate_version: i16,  // -1 = no SaslAuthenticate header (legacy)
    current_request_header: Option<RequestHeader>,
    net_out_buffer: Option<Box<dyn KafkaSend>>,   // Pending outbound
    net_in_buffer: Option<NetworkReceive>,          // Pending inbound
    pending_sasl_state: Option<SaslState>,          // Deferred state when flushing
}
```

### Constants
```rust
const DISABLE_KAFKA_SASL_AUTHENTICATE_HEADER: i16 = -1;
pub const MAX_RESERVED_CORRELATION_ID: i32 = i32::MAX;
pub const MIN_RESERVED_CORRELATION_ID: i32 = i32::MAX - 7;
```

### Key Methods (translated from Java)

| Method | Java lines | Purpose |
|--------|-----------|---------|
| `authenticate()` | 240-326 | State machine main loop |
| `send_api_versions_request()` | 246-249 | Send ApiVersions v0 |
| `set_sasl_authenticate_and_handshake_versions()` | 393-404 | Extract SASL versions from ApiVersionsResponse |
| `send_handshake_request()` | 328-331 | Send SaslHandshake with mechanism |
| `handle_sasl_handshake_response()` | 601-618 | Validate mechanism support |
| `send_initial_token()` | 333-335 | Send PLAIN token via `send_sasl_client_token` |
| `send_sasl_client_token()` | 432-451 | Send token (raw or wrapped in SaslAuthenticate) |
| `create_sasl_token()` | 527-559 | Generate PLAIN token: `\0username\0password` |
| `receive_response_or_token()` | 474-485 | Read size-delimited message via NetworkReceive |
| `receive_kafka_response()` | 568-599 | Parse response with header + correlation ID check |
| `receive_token()` | 505-524 | Receive + validate SaslAuthenticateResponse |
| `flush_net_out_buffer()` | 561-566 | Write pending data to transport |
| `next_request_header()` | 373-384 | Create header with reserved correlation ID |
| `next_correlation_id()` | 367-371 | Allocate from reserved range |
| `set_sasl_state()` | 406-425 | State transition (with pending support) |
| `is_reserved()` | 137-139 | Check if correlation ID is in reserved range |

### Skipped Java Code
- All re-authentication methods/states
- Server-side logic
- Java SASL client framework (SaslClient, CallbackHandler, LoginModule)
- Non-PLAIN mechanisms (SCRAM, Kerberos, OAuth)
- `ReauthInfo` class

### PLAIN Token Format (RFC 4616)
```
\0<username>\0<password>
```
No challenge-response needed — single token exchange.

### Non-blocking Design
`authenticate()` is called repeatedly by `KafkaChannel::prepare()`. Each call:
1. Flushes pending outbound data
2. Attempts one state transition
3. Returns `Ok(())` if I/O would block (partial read/write)
4. Stores partial reads in `net_in_buffer` for next call

---

## Part 3: SaslChannelBuilder

**New file:** `src/common/network/sasl_channel_builder.rs`

**Java source:** `SaslChannelBuilder.java` (414 lines, client-only subset)

### Struct
```rust
pub struct SaslChannelBuilder {
    security_protocol: SecurityProtocol,
    sasl_config: SaslConfig,
    ssl_factory: Option<SslFactory>,
    listener_name: Option<ListenerName>,
    client_id: String,
}
```

### ChannelBuilder Implementation
- `SASL_PLAINTEXT` → `PlaintextTransportLayer` + `SaslClientAuthenticator`
- `SASL_SSL` → `SslTransportLayer` + `SaslClientAuthenticator`
- Constructor validates: PLAIN requires username+password; SASL_SSL requires ssl_factory

---

## Part 4: Module Wiring

**New file:** `src/common/security/authenticator/mod.rs`
- `pub mod sasl_client_authenticator;`
- `pub use sasl_client_authenticator::SaslClientAuthenticator;`

**Modify:** `src/common/security/mod.rs`
- Add `pub mod authenticator;`

**Modify:** `src/common/network/mod.rs`
- Add `pub mod sasl_channel_builder;`
- Add `pub use sasl_channel_builder::SaslChannelBuilder;`

---

## Part 5: Tests

### MockTransportLayer (in sasl_client_authenticator.rs test module)
Captures writes, returns pre-programmed reads. Implements `TransportLayer` trait.

### Test Helpers
- `build_api_versions_response_bytes(...)` — serialize ApiVersionsResponse with SASL version info
- `build_sasl_handshake_response_bytes(...)` — serialize SaslHandshakeResponse
- `build_sasl_authenticate_response_bytes(...)` — serialize SaslAuthenticateResponse

Each builds size-prefixed bytes (4-byte size + ResponseHeader + body) matching `NetworkReceive` format. Response correlation IDs must match the request's reserved IDs.

### SaslClientAuthenticator Tests (~10 tests)
1. `test_plain_token_generation` — `\0alice\0secret` format
2. `test_initial_state` — state=SendApiVersionsRequest, complete()=false
3. `test_correlation_id_management` — reserved range, incrementing
4. `test_is_reserved` — boundary conditions
5. `test_version_negotiation` — extracts SASL versions from ApiVersionsResponse
6. `test_successful_authentication` — full happy path with mock transport
7. `test_unsupported_mechanism` — UNSUPPORTED_SASL_MECHANISM error
8. `test_auth_failure` — SASL_AUTHENTICATION_FAILED error
9. `test_raw_token_mode` — legacy path (sasl_authenticate_version == -1)
10. `test_handle_sasl_handshake_illegal_state` — ILLEGAL_SASL_STATE error

### SaslChannelBuilder Tests (~4 tests)
1. `test_build_channel_sasl_plaintext` — creates channel, not ready
2. `test_build_channel_sasl_ssl` — creates channel with SSL transport
3. `test_missing_credentials_error` — constructor rejects missing username/password
4. `test_invalid_security_protocol` — rejects non-SASL protocol

---

## Implementation Order

1. Refactor Authenticator trait + PlaintextAuthenticator + KafkaChannel::prepare()
2. `cargo test` — verify no regressions
3. **Commit**: "Refactor Authenticator trait to accept transport layer for SASL support"
4. Create module scaffolding (security/authenticator/mod.rs)
5. Implement SaslClientAuthenticator (struct, constants, constructor, Authenticator impl, all helper methods)
6. Add MockTransportLayer and unit tests
7. Implement SaslChannelBuilder with tests
8. Wire into module exports
9. `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`
10. **Commit**: "Implement SASL client authenticator and SaslChannelBuilder"

## Verification

1. `cargo build` — compiles without errors
2. `cargo test` — all existing + new tests pass
3. `cargo xtask format-check` — properly formatted
4. `cargo xtask lint` — no clippy warnings

## Java Source Reference

- `SaslClientAuthenticator.java`: `kafka/clients/src/main/java/org/apache/kafka/common/security/authenticator/SaslClientAuthenticator.java`
- `SaslChannelBuilder.java`: `kafka/clients/src/main/java/org/apache/kafka/common/network/SaslChannelBuilder.java`
- `Authenticator.java`: `kafka/clients/src/main/java/org/apache/kafka/common/network/Authenticator.java`
