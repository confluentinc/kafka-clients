# Phase 1: Security Protocol & Configuration

## Goal

Define the `SecurityProtocol` enum and the SSL/SASL configuration types needed by subsequent phases.

## Classes to Implement

### 1. SecurityProtocol (`common/security/auth/security_protocol.rs`)

**Java:** `org.apache.kafka.common.security.auth.SecurityProtocol`

Enum with 4 variants determining the channel builder and transport layer to use.

```rust
/// Security protocol for Kafka connections.
///
/// Determines the combination of encryption (TLS) and authentication (SASL)
/// used for broker connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    /// Un-authenticated, non-encrypted channel.
    Plaintext = 0,
    /// SSL/TLS encrypted channel (no SASL).
    Ssl = 1,
    /// SASL authenticated, non-encrypted channel.
    SaslPlaintext = 2,
    /// SASL authenticated, SSL/TLS encrypted channel.
    SaslSsl = 3,
}
```

**Public API:**
- `fn id(&self) -> i16` — permanent protocol ID
- `fn name(&self) -> &'static str` — wire name ("PLAINTEXT", "SSL", etc.)
- `fn for_id(id: i16) -> Option<SecurityProtocol>` — lookup by ID
- `fn for_name(name: &str) -> Option<SecurityProtocol>` — case-insensitive lookup
- `fn names() -> &'static [&'static str]` — all protocol names
- `impl Display` — displays the name
- `impl FromStr` — parses from name string

**Tests to translate:** No dedicated test class in Java — tested indirectly. Add basic unit tests for:
- Round-trip `for_id` / `id()` for all variants
- Round-trip `for_name` / `name()` for all variants
- Case-insensitive `for_name` ("sasl_ssl" → SaslSsl)
- `for_id` with invalid ID returns None
- `for_name` with invalid name returns None

---

### 2. SslConfig (`common/config/ssl_configs.rs`)

**Java:** `org.apache.kafka.common.config.SslConfigs`

In Java, this is a class of static string constants used as config keys with `AbstractConfig`.
In Rust, we define a **typed struct** holding the actual config values the client needs.

```rust
/// SSL/TLS configuration for Kafka connections.
///
/// Maps to Java's `SslConfigs` — only the client-relevant subset.
#[derive(Debug, Clone)]
pub struct SslConfig {
    /// Path to the trust store file (CA certificates).
    /// Corresponds to `ssl.truststore.location`.
    pub truststore_location: Option<String>,

    /// Password for the trust store file.
    /// Corresponds to `ssl.truststore.password`.
    pub truststore_password: Option<String>,

    /// Trusted certificates in PEM format (alternative to truststore_location).
    /// Corresponds to `ssl.truststore.certificates`.
    pub truststore_certificates: Option<String>,

    /// Trust store format: "JKS", "PKCS12", or "PEM".
    /// Corresponds to `ssl.truststore.type`. Default: "PEM".
    pub truststore_type: String,

    /// Path to the key store file (client certificate for mTLS).
    /// Corresponds to `ssl.keystore.location`.
    pub keystore_location: Option<String>,

    /// Password for the key store file.
    /// Corresponds to `ssl.keystore.password`.
    pub keystore_password: Option<String>,

    /// Private key in PEM format (alternative to keystore_location).
    /// Corresponds to `ssl.keystore.key`.
    pub keystore_key: Option<String>,

    /// Certificate chain in PEM format (alternative to keystore_location).
    /// Corresponds to `ssl.keystore.certificate.chain`.
    pub keystore_certificate_chain: Option<String>,

    /// Key store format: "JKS", "PKCS12", or "PEM".
    /// Corresponds to `ssl.keystore.type`. Default: "PEM".
    pub keystore_type: String,

    /// Password for the private key.
    /// Corresponds to `ssl.key.password`.
    pub key_password: Option<String>,

    /// Endpoint identification algorithm for hostname verification.
    /// "https" enables hostname verification (default). Empty string disables it.
    /// Corresponds to `ssl.endpoint.identification.algorithm`.
    pub endpoint_identification_algorithm: String,

    /// Enabled TLS protocol versions.
    /// Corresponds to `ssl.enabled.protocols`. Default: ["TLSv1.2", "TLSv1.3"].
    pub enabled_protocols: Vec<String>,
}
```

**Notes on Rust adaptation:**
- Java defaults to JKS keystores. Rust/rustls works natively with PEM, so default `truststore_type` and `keystore_type` to `"PEM"`.
- Skip `ssl.provider`, `ssl.secure.random.implementation`, `ssl.engine.factory.class` — Java-specific, not applicable to rustls.
- Skip `ssl.keymanager.algorithm`, `ssl.trustmanager.algorithm` — Java JSSE-specific.
- Skip `ssl.cipher.suites` for now — rustls has sensible defaults. Can add later.
- Skip `ssl.protocol` — rustls handles protocol negotiation automatically based on `enabled_protocols`.
- Provide `Default` impl with sensible defaults (PEM format, TLSv1.2+1.3, https endpoint verification).

**Config key constants** (for compatibility with Java config strings):
```rust
pub const SSL_TRUSTSTORE_LOCATION: &str = "ssl.truststore.location";
pub const SSL_TRUSTSTORE_PASSWORD: &str = "ssl.truststore.password";
// ... etc, matching Java SslConfigs constant values
```

**Tests:** Unit tests for `Default` impl defaults and builder/setter patterns.

---

### 3. SaslConfig (`common/config/sasl_configs.rs`)

**Java:** `org.apache.kafka.common.config.SaslConfigs`

In Java, this has 40+ constants, most for Kerberos/OAuth (out of scope).
For SASL PLAIN, we only need a minimal subset.

```rust
/// SASL configuration for Kafka connections.
///
/// Maps to the client-relevant subset of Java's `SaslConfigs`.
/// Currently supports PLAIN mechanism only.
#[derive(Debug, Clone)]
pub struct SaslConfig {
    /// SASL mechanism. Default: "PLAIN".
    /// Corresponds to `sasl.mechanism`.
    pub mechanism: String,

    /// JAAS configuration string for embedded credentials.
    /// Corresponds to `sasl.jaas.config`.
    /// Example: "org.apache.kafka.common.security.plain.PlainLoginModule required
    ///           username=\"alice\" password=\"secret\";"
    /// If set, `username` and `password` are extracted from this string.
    pub jaas_config: Option<String>,

    /// Username for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub username: Option<String>,

    /// Password for PLAIN authentication.
    /// Convenience field — used when `jaas_config` is not set.
    pub password: Option<String>,
}
```

**JAAS config parsing:**
For compatibility with Java clients, support parsing the `sasl.jaas.config` format:
```
org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";
```

Provide a method to extract username/password:
```rust
impl SaslConfig {
    /// Resolve the effective username, checking `username` field first,
    /// then parsing from `jaas_config` if present.
    pub fn resolve_username(&self) -> Option<&str> { ... }

    /// Resolve the effective password, checking `password` field first,
    /// then parsing from `jaas_config` if present.
    pub fn resolve_password(&self) -> Option<&str> { ... }
}
```

**Config key constants:**
```rust
pub const SASL_MECHANISM: &str = "sasl.mechanism";
pub const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";
pub const DEFAULT_SASL_MECHANISM: &str = "GSSAPI";
```

**Excluded Java constants (out of scope):**
- All `SASL_KERBEROS_*` — Kerberos not supported
- All `SASL_OAUTHBEARER_*` — OAuth not supported
- All `SASL_LOGIN_REFRESH_*` — Token refresh not supported
- `SASL_CLIENT_CALLBACK_HANDLER_CLASS` — No pluggable callback handlers
- `SASL_LOGIN_CLASS` — No pluggable login implementations

**Tests:**
- `resolve_username`/`resolve_password` from direct fields
- `resolve_username`/`resolve_password` parsed from `jaas_config`
- Direct fields take precedence over `jaas_config`
- Malformed `jaas_config` handling

---

### 4. SslClientAuth (`common/config/ssl_client_auth.rs`)

**Java:** `org.apache.kafka.common.config.SslClientAuth`

Simple enum for server-side client auth policy. Primarily server-side but used in shared config types.

```rust
/// Whether the server requires or requests client TLS authentication.
///
/// This is primarily a server-side setting, but included for config
/// compatibility with Java's `SslClientAuth`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslClientAuth {
    /// Server requires client certificate.
    Required,
    /// Server requests but does not require client certificate.
    Requested,
    /// Server does not request client certificate.
    #[default]
    None,
}
```

**Public API:**
- `fn for_config(key: Option<&str>) -> Self` — case-insensitive lookup, None → `SslClientAuth::None`
- `impl Display` — lowercase string representation
- `impl FromStr`

**Tests:** Basic round-trip and case-insensitive parsing.

---

## Module Structure

```
src/common/
├── config/
│   ├── mod.rs              (new module)
│   ├── ssl_configs.rs      (SslConfig struct + constants)
│   ├── sasl_configs.rs     (SaslConfig struct + constants)
│   └── ssl_client_auth.rs  (SslClientAuth enum)
└── security/
    └── auth/
        ├── mod.rs           (new module)
        └── security_protocol.rs (SecurityProtocol enum)
```

Update `src/common/mod.rs` to add:
```rust
pub mod config;
pub mod security;
```

---

## Implementation Order

1. `SecurityProtocol` — no dependencies, foundation for everything
2. `SslClientAuth` — no dependencies, simple enum
3. `SslConfig` — depends on nothing, needed by Phase 2
4. `SaslConfig` — depends on nothing, needed by Phase 4

---

## Definition of Done

Per project rules:
1. All methods from Java classes implemented (client-relevant subset)
2. Tests translated (unit tests for enums, config parsing, defaults)
3. `cargo build` succeeds
4. `cargo test` passes
5. `cargo xtask format-check` passes
6. `cargo xtask lint` passes
