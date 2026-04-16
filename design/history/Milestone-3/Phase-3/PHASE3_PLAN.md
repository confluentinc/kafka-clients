# Phase 3: SASL Request/Response Types — Implementation Plan

## Context

Milestone 3 adds SSL/TLS encryption and SASL PLAIN authentication. Phase 1 (SecurityProtocol, SslConfig, SaslConfig, SslClientAuth) and Phase 2 (SslTransportLayer, SslFactory, SslChannelBuilder) are complete. Phase 3 adds the SASL handshake and authenticate request/response wrapper types, following the same pattern as the existing ApiVersions and Metadata request/response types. These types are needed by Phase 4 (SaslClientAuthenticator state machine).

## Scope

Translate 4 Java classes into 4 new Rust files, modify 3 existing files.

**No Java test files exist** for these classes. Unit tests will follow the patterns established in `api_versions_request.rs`.

## Prerequisites Verified

- Generated message specs exist: `generator/messages/SaslHandshakeRequest.json` (apiKey=17, v0-1, non-flexible), `SaslHandshakeResponse.json` (v0-1, non-flexible), `SaslAuthenticateRequest.json` (apiKey=36, v0-2, flexible v2+), `SaslAuthenticateResponse.json` (v0-2, flexible v2+)
- `ApiKeys::SASL_HANDSHAKE` and `ApiKeys::SASL_AUTHENTICATE` constants already exist
- `Errors::message()` method exists for error message strings
- `ErrorMessage` in `SaslAuthenticateResponseData` is `Option<String>` (nullable in all versions)
- SaslHandshake is non-flexible (no tagged fields), SaslAuthenticate v2 is flexible

## New Files

### 1. `src/common/requests/sasl_handshake_request.rs`

**Java source**: `SaslHandshakeRequest.java` (77 lines)

- `SaslHandshakeRequest` struct: `data: SaslHandshakeRequestData`, `version: i16`
- Methods: `new()`, `data()`, `version()`, `api_key()` -> `&ApiKeys::SASL_HANDSHAKE`, `get_error_response()`, `parse()`
- `get_error_response`: creates `SaslHandshakeResponseData` with error code set, ignores `throttle_time_ms` (not in schema)
- `SaslHandshakeRequestBuilder`: wraps data + version range from `ApiKeys::SASL_HANDSHAKE`
- Implements `RequestBuilder` trait, `build_version()` returns `ConcreteRequest::SaslHandshake(...)`
- Standard `Display` impl

### 2. `src/common/requests/sasl_handshake_response.rs`

**Java source**: `SaslHandshakeResponse.java` (76 lines)

- `SaslHandshakeResponse` struct: `data: SaslHandshakeResponseData`
- Methods: `new()`, `data()`, `api_key()` -> `&ApiKeys::SASL_HANDSHAKE`, `error()`, `error_counts()`, `enabled_mechanisms()`, `parse()`
- `throttle_time_ms()` -> returns `DEFAULT_THROTTLE_TIME` (0), not in schema
- `maybe_set_throttle_time_ms()` -> no-op
- `should_client_throttle()` -> `false` (non-throttled API key)
- Standard `Display` impl

### 3. `src/common/requests/sasl_authenticate_request.rs`

**Java source**: `SaslAuthenticateRequest.java` (87 lines)

- `SaslAuthenticateRequest` struct: `data: SaslAuthenticateRequestData`, `version: i16`
- Methods: `new()`, `data()`, `version()`, `api_key()` -> `&ApiKeys::SASL_AUTHENTICATE`, `get_error_response()`, `parse()`
- `get_error_response`: creates `SaslAuthenticateResponseData` with error code AND error message set
- `SaslAuthenticateRequestBuilder`: wraps data + version range from `ApiKeys::SASL_AUTHENTICATE`
- **Redacted `Display`**: clones data, sets `auth_bytes` to empty `Vec`, then prints (security: never log credentials)
- Builder `Display`: returns `"(type=SaslAuthenticateRequest)"` matching Java

### 4. `src/common/requests/sasl_authenticate_response.rs`

**Java source**: `SaslAuthenticateResponse.java` (90 lines)

- `SaslAuthenticateResponse` struct: `data: SaslAuthenticateResponseData`
- Methods: `new()`, `data()`, `api_key()` -> `&ApiKeys::SASL_AUTHENTICATE`, `error()`, `error_counts()`, `error_message()`, `session_lifetime_ms()`, `sasl_auth_bytes()`, `parse()`
- `throttle_time_ms()` -> returns `DEFAULT_THROTTLE_TIME` (0)
- `maybe_set_throttle_time_ms()` -> no-op
- `should_client_throttle()` -> `false`
- **Redacted `Display`**: clones data, clears `auth_bytes` before printing

## Modified Files

### 5. `src/common/requests/mod.rs`

Add 4 module declarations and 4 re-export lines (alphabetical order):
- `pub mod sasl_authenticate_request;` / `pub mod sasl_authenticate_response;`
- `pub mod sasl_handshake_request;` / `pub mod sasl_handshake_response;`
- Re-export: `SaslAuthenticateRequest`, `SaslAuthenticateRequestBuilder`, `SaslAuthenticateResponse`, `SaslHandshakeRequest`, `SaslHandshakeRequestBuilder`, `SaslHandshakeResponse`

### 6. `src/common/requests/abstract_request.rs`

- Add imports: `SaslHandshakeRequestData`, `SaslAuthenticateRequestData`, `SaslHandshakeRequest`, `SaslAuthenticateRequest`
- Add `ConcreteRequest` variants: `SaslHandshake(SaslHandshakeRequest)`, `SaslAuthenticate(SaslAuthenticateRequest)`
- Add match arms in ALL 8 dispatch methods: `version()`, `api_key()`, `to_send()`, `serialize_with_header()`, `serialize()`, `get_error_response()`, `do_parse_request()`, `Display`

### 7. `src/common/requests/abstract_response.rs`

- Add imports: `SaslHandshakeResponse`, `SaslAuthenticateResponse`
- Add `ConcreteResponse` variants: `SaslHandshake(SaslHandshakeResponse)`, `SaslAuthenticate(SaslAuthenticateResponse)`
- Add match arms in ALL 9 dispatch methods: `api_key()`, `to_send()`, `serialize_with_header()`, `serialize()`, `error_counts()`, `throttle_time_ms()`, `maybe_set_throttle_time_ms()`, `should_client_throttle()`, `parse()`, `Display`
- SASL response parsing uses standard pattern (like Metadata), NOT the ApiVersions special-case pattern

## Implementation Order

1. Create all 4 new files
2. Wire into `mod.rs`
3. Update `abstract_request.rs` and `abstract_response.rs` with new variants
4. `cargo build` to verify compilation
5. Add unit tests to each new file
6. `cargo test`, `cargo xtask format-check`, `cargo xtask lint`
7. Commit

## Unit Tests

**sasl_handshake_request.rs** (~5 tests):
- Builder version range, builder build, get_error_response (verify error code), parse roundtrip, display format

**sasl_handshake_response.rs** (~7 tests):
- error(), error_counts(), throttle_time_ms() == 0, maybe_set_throttle_time_ms() no-op, should_client_throttle() == false, enabled_mechanisms(), parse roundtrip

**sasl_authenticate_request.rs** (~5 tests):
- Builder version range, builder build, get_error_response (verify error code + error_message), **display_redacted** (auth_bytes not in output), parse roundtrip

**sasl_authenticate_response.rs** (~8 tests):
- error(), error_message(), session_lifetime_ms(), sasl_auth_bytes(), error_counts(), throttle_time_ms() == 0, should_client_throttle() == false, **display_redacted** (auth_bytes not in output)

## Verification

1. `cargo build` — compiles without errors
2. `cargo test` — all existing + new tests pass
3. `cargo xtask format-check` — properly formatted
4. `cargo xtask lint` — no clippy warnings
