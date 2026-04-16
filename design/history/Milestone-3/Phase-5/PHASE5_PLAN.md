# Phase 5: Integration & Wiring — `channel_builders.rs` Factory

## Context

Phases 1-4 are complete. Three channel builders exist (`PlaintextChannelBuilder`, `SslChannelBuilder`, `SaslChannelBuilder`) but callers must manually construct the right one. Phase 5 translates `ChannelBuilders.java`'s factory pattern into a Rust module that selects the appropriate `ChannelBuilder` based on `SecurityProtocol`.

## Scope

Translate `ChannelBuilders.clientChannelBuilder()` and its private `create()` method (lines 110-190 of `ChannelBuilders.java`) into a single Rust factory function. No Java test files exist for `ChannelBuilders` — tests are new.

## Java Source Reference

- `kafka/clients/src/main/java/org/apache/kafka/common/network/ChannelBuilders.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java` (lines 117-122, `createChannelBuilder()`)

## Java Pattern

In Java:
1. `ClientUtils.createChannelBuilder(config, time, logContext)` reads `SECURITY_PROTOCOL_CONFIG` from config, extracts SASL mechanism, calls `ChannelBuilders.clientChannelBuilder()`
2. `ChannelBuilders.clientChannelBuilder()` validates SASL params, calls private `create()`
3. `create()` switches on `SecurityProtocol`:
   - `PLAINTEXT` -> `PlaintextChannelBuilder`
   - `SSL` -> `SslChannelBuilder`
   - `SASL_SSL` / `SASL_PLAINTEXT` -> `SaslChannelBuilder` (with JAAS contexts, credentials)
4. Calls `channelBuilder.configure(configs)` (Java-specific Map<String, Object> config)

## Rust Adaptation

In Rust, the channel builders already accept typed config structs (`SaslConfig`, `SslFactory`) in their constructors, so there is no separate `configure()` step. The factory function takes the relevant configs directly.

Excluded Java code:
- `JaasContext` loading — simplified to direct `SaslConfig` struct
- `channelBuilderConfigs()` — Java's config extraction from `AbstractConfig` is not applicable
- `serverChannelBuilder()` — server-side, out of scope
- `createPrincipalBuilder()` — server-side, out of scope
- `requireNonNullMode()` — replaced by Rust exhaustive match

---

## Step 1: Create `src/common/network/channel_builders.rs`

**New file.** Translates `ChannelBuilders.create()`.

### Public API

```rust
/// Creates a client-side ChannelBuilder for the given security protocol.
///
/// Translated from `ChannelBuilders.clientChannelBuilder()` (Java).
///
/// # Arguments
///
/// * `security_protocol` - The security protocol to use
/// * `ssl_config` - Required for `SSL` and `SASL_SSL` protocols
/// * `sasl_config` - Required for `SASL_PLAINTEXT` and `SASL_SSL` protocols
/// * `listener_name` - Optional listener name (server-side only, `None` for clients)
/// * `client_id` - The Kafka client ID
///
/// # Errors
///
/// Returns an error if required configs are missing for the given protocol.
pub fn client_channel_builder(
    security_protocol: SecurityProtocol,
    ssl_config: Option<&SslConfig>,
    sasl_config: Option<&SaslConfig>,
    listener_name: Option<ListenerName>,
    client_id: &str,
) -> io::Result<Box<dyn ChannelBuilder>>
```

### Match Logic

```rust
match security_protocol {
    SecurityProtocol::Plaintext => {
        Ok(Box::new(PlaintextChannelBuilder::new(listener_name)))
    }
    SecurityProtocol::Ssl => {
        let ssl_config = ssl_config.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SSL protocol requires ssl_config")
        })?;
        let ssl_factory = SslFactory::new(ssl_config)?;
        Ok(Box::new(SslChannelBuilder::new(ssl_factory, listener_name)))
    }
    SecurityProtocol::SaslPlaintext => {
        let sasl_config = sasl_config.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SASL_PLAINTEXT protocol requires sasl_config")
        })?;
        Ok(Box::new(SaslChannelBuilder::new(
            SecurityProtocol::SaslPlaintext,
            sasl_config.clone(),
            None,
            listener_name,
            client_id,
        )?))
    }
    SecurityProtocol::SaslSsl => {
        let ssl_config = ssl_config.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL protocol requires ssl_config")
        })?;
        let sasl_config = sasl_config.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SASL_SSL protocol requires sasl_config")
        })?;
        let ssl_factory = SslFactory::new(ssl_config)?;
        Ok(Box::new(SaslChannelBuilder::new(
            SecurityProtocol::SaslSsl,
            sasl_config.clone(),
            Some(ssl_factory),
            listener_name,
            client_id,
        )?))
    }
}
```

### Dependencies (imports within the crate)

```rust
use std::io;
use crate::common::config::{SaslConfig, SslConfig};
use crate::common::network::channel_builder::ChannelBuilder;
use crate::common::network::listener_name::ListenerName;
use crate::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use crate::common::network::sasl_channel_builder::SaslChannelBuilder;
use crate::common::network::ssl_channel_builder::SslChannelBuilder;
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::ssl::SslFactory;
```

### Design Decisions

- Takes `Option<&SslConfig>` and `Option<&SaslConfig>` rather than a single config struct. This matches the Java pattern where each builder extracts its own configs, and avoids forcing callers to construct configs they don't need.
- `SaslConfig` is cloned when passed to `SaslChannelBuilder` since the builder stores it. This is acceptable since credentials are small strings.
- Returns `io::Result` because `SslFactory::new()` and `SaslChannelBuilder::new()` can fail with config validation errors.
- No `configure()` step — Rust builders accept typed configs in their constructors.

---

## Step 2: Modify `src/common/network/mod.rs`

Add 2 lines:

```rust
pub mod channel_builders;
```

and in the re-exports section:

```rust
pub use channel_builders::client_channel_builder;
```

---

## Step 3: Unit Tests

In `channel_builders.rs` `#[cfg(test)] mod tests`:

### Test 1: `test_plaintext_builder`
- Call `client_channel_builder(Plaintext, None, None, None, "test")`
- Assert `Ok` — no configs needed for PLAINTEXT

### Test 2: `test_ssl_builder`
- Create a valid `SslConfig::default()`
- Call `client_channel_builder(Ssl, Some(&ssl_config), None, None, "test")`
- Assert `Ok`

### Test 3: `test_ssl_missing_config`
- Call `client_channel_builder(Ssl, None, None, None, "test")`
- Assert `Err` with message containing "ssl_config"

### Test 4: `test_sasl_plaintext_builder`
- Create `SaslConfig` with mechanism=PLAIN, username, password
- Call `client_channel_builder(SaslPlaintext, None, Some(&sasl_config), None, "test")`
- Assert `Ok`

### Test 5: `test_sasl_ssl_builder`
- Create both `SslConfig` and `SaslConfig`
- Call `client_channel_builder(SaslSsl, Some(&ssl_config), Some(&sasl_config), None, "test")`
- Assert `Ok`

### Test 6: `test_sasl_ssl_missing_ssl_config`
- Create `SaslConfig` only
- Call `client_channel_builder(SaslSsl, None, Some(&sasl_config), None, "test")`
- Assert `Err` with message containing "ssl_config"

### Test 7: `test_sasl_plaintext_missing_sasl_config`
- Call `client_channel_builder(SaslPlaintext, None, None, None, "test")`
- Assert `Err` with message containing "sasl_config"

---

## Implementation Order

1. Create `src/common/network/channel_builders.rs` with function + tests
2. Add module + re-export to `src/common/network/mod.rs`
3. `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`
4. **Commit**: `"Add channel_builders factory for security protocol dispatch (Phase 5)"`

## Verification

1. `cargo build` — compiles without errors
2. `cargo test` — all existing + 7 new tests pass
3. `cargo xtask format-check` — properly formatted
4. `cargo xtask lint` — no clippy warnings

## Files Changed

| File | Action |
|---|---|
| `src/common/network/channel_builders.rs` | **Create** |
| `src/common/network/mod.rs` | Modify (2 lines) |
