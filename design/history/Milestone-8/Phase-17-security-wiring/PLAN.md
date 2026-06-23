# Phase 17 — SASL_SSL (full security-protocol) wiring into `AsyncKafkaConsumer`

**Milestone-8 / Phase-17** · Agent number **N = 17** ·
`design/history/Milestone-8/Phase-17-security-wiring/`

Make `AsyncKafkaConsumer` select its channel builder from `security.protocol` +
`ssl.*` / `sasl.*` config instead of hardcoding `PlaintextChannelBuilder`.
Headline target: **SASL_SSL with PLAIN** against the Docker cluster on `:9097`.
All four protocols wired via the existing `client_channel_builder` factory.

## The template: the producer (just merged from master, PR #10)

The master merge (`47b8579`) wired the **producer** end-to-end. The consumer
must **mirror it exactly** — this is a working, cluster-proven reference in the
same tree. Do NOT invent a different shape.

Producer reference points:
- `src/producer/producer_config.rs`:
  - fields: `security_protocol: SecurityProtocol` (enum), `sasl_config: SaslConfig`,
    `ssl_config: SslConfig` (defaults: `Plaintext` / `SaslConfig::default()` /
    `SslConfig::default()`).
  - parsing: `security.protocol` → `SecurityProtocol::for_name(value)` (error with
    `SecurityProtocol::names()` in the message on unknown); `sasl.mechanism` →
    `sasl_config.mechanism`; `sasl.jaas.config` → `sasl_config.jaas_config`
    (empty → `None`); all `ssl.*` keys via the `parse_ssl_config(&mut SslConfig,
    key, value)` helper.
- `src/producer/kafka_producer.rs` (~line 279):
  ```rust
  let channel_builder = channel_builders::client_channel_builder(
      config.security_protocol,
      Some(&config.ssl_config),
      Some(&config.sasl_config),
      None,                 // listener_name
      &config.client_id,
      log_context.clone(),
  ).map_err(|e| KafkaError::illegal_argument(
      format!("Failed to create channel builder: {}", e)))?;
  let selector = Selector::with_defaults_and_log_context(
      config.connections_max_idle_ms, channel_builder, log_context.clone());
  ```

`client_channel_builder` signature (post-merge):
`(SecurityProtocol, Option<&SslConfig>, Option<&SaslConfig>, Option<ListenerName>,
&str, LogContext) -> io::Result<Box<dyn ChannelBuilder>>`.

## Java contract
Mirrors `ClientUtils.createChannelBuilder` / `ChannelBuilders.clientChannelBuilder`,
which `KafkaConsumer` calls to build the `Selector`'s channel builder from
`security.protocol`. The Rust equivalent is `client_channel_builder`. SASL
mechanisms implemented: **PLAIN only** (SCRAM/OAUTHBEARER/GSSAPI not implemented —
out of scope; `SaslChannelBuilder` validates and errors otherwise).

## Work items (Actor 17)

1. **`consumer_config.rs`** — mirror `ProducerConfig`:
   - Change `security_protocol: String` → `security_protocol: SecurityProtocol`
     (default `Plaintext`). Add `sasl_config: SaslConfig` and
     `ssl_config: SslConfig` fields (defaults).
   - Keep the public accessor `security_protocol()` working — return the enum's
     `.name()` (`&str`) or change return type to `SecurityProtocol`; preserve the
     existing behavior so current callers/tests still compile. Update the
     existing test asserting `"PLAINTEXT"`.
   - Parse: `security.protocol` via `SecurityProtocol::for_name` (replace the
     current string-validation arm; keep the same error semantics —
     `illegal_argument` with the valid names listed); `sasl.mechanism`,
     `sasl.jaas.config`; all `ssl.*` keys.
   - **DRY (DoD §6):** the `ssl.*` parsing currently lives as a private
     `parse_ssl_config` on `ProducerConfig`. Extract it to a shared
     `pub(crate)` helper in `common::config::ssl_configs`
     (e.g. `apply_ssl_config_key(ssl: &mut SslConfig, key: &str, value: &str)`)
     and call it from BOTH producer and consumer. Do not copy-paste the body.
   - Keep silent-accept for classic-protocol-only keys (scope §20).

2. **`async_kafka_consumer.rs:717-718`** — replace
   `Box::new(PlaintextChannelBuilder::new(None))` + `Selector::with_defaults(...)`
   with the producer's `client_channel_builder(...)` + `with_defaults_and_log_context`
   pattern. Use the `log_context` already in scope in the consumer ctor (the merge
   added a `[Consumer clientId=..., groupId=...]` context). Surface the builder
   error as a `KafkaError` from the ctor (no panic). Confirm there is no second
   hardcoded builder in the `NetworkClientDelegate` supplier path.

3. License headers / rustdoc intact; no TODO/FIXME (DoD §5/§8).

## Tests (the "test it well" mandate)

- **Unit (no Docker), `consumer_config.rs`:** each of the 4 `security.protocol`
  values parses to the right `SecurityProtocol`; invalid value →
  `illegal_argument` with asserted message content (DoD §3); `ssl.truststore.*`/
  `ssl.keystore.*`/`ssl.endpoint.identification.algorithm` land on `ssl_config`;
  `sasl.mechanism`/`sasl.jaas.config` land on `sasl_config`; empty jaas → `None`.
- **Shared helper unit test:** `apply_ssl_config_key` sets each field (mirror the
  producer's existing `parse_ssl_config` coverage so the extraction is tested).
- **Builder-selection unit test:** `security.protocol=SASL_SSL` with valid
  ssl+sasl config → `client_channel_builder` returns `Ok`; `SASL_SSL` missing
  `ssl_config` content → error (mirror `channel_builders.rs` validation).
- **Integration (Docker, gated like the existing `plaintext_consumer_*` tests),
  new `tests/integration/sasl_ssl_consumer_test.rs`:** build `AsyncKafkaConsumer`
  with `security.protocol=SASL_SSL`, PLAIN `admin`/`admin-secret`, CA cert, against
  `:9097`; subscribe → produce a few records → `poll` returns them. Plus a
  **wrong-credentials** test asserting auth failure surfaces as an error (not a
  hang), mirroring `ssl_sasl_test.rs::test_sasl_wrong_credentials`. Follow the
  `ssl_sasl_test.rs` helpers for `SslConfig`/`SaslConfig` construction and the
  `tests/common/kafka_cluster.rs` cluster (SASL_SSL on :9097, admin/admin-secret).

## DoD
`cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check` green
(whole workspace — the shared-helper extraction touches the producer too).

## Out of scope
SCRAM/OAUTHBEARER/GSSAPI mechanisms; JKS/PKCS12 keystores (PEM only); any change
to the just-merged producer wiring beyond the shared `apply_ssl_config_key`
extraction.

## Critic 17 focus
Behavior-parity with the producer template and Java `createChannelBuilder`;
config-key coverage vs `ssl_configs`/`sasl_configs` constants; error surfaced not
panicked; no hardcoded-PLAINTEXT left; `SASL_SSL`-missing-ssl validation; PLAIN-only
scope stated; the `apply_ssl_config_key` extraction is behavior-identical for the
producer (no regression); tests assert message content and cover the auth-failure
path.
