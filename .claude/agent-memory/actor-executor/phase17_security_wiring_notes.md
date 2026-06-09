---
name: phase17-security-wiring-notes
description: Milestone-8 Phase 17 — consumer channel-builder selection from security.protocol; shared ssl-key helper; producer send() shadowing gotcha
metadata:
  type: project
---

Phase 17 wired `AsyncKafkaConsumer` to pick its channel builder from
`security.protocol`/`ssl.*`/`sasl.*` (all 4 protocols, PLAIN only), mirroring
the producer wired by master PR #10. Closed by commit 78204d2.

**Why:** consumer hardcoded `PlaintextChannelBuilder`; producer already had the
end-to-end SASL_SSL path. Goal was behavior-parity with producer + Java
`ClientUtils.createChannelBuilder`.

**How to apply (reusable patterns):**
- Shared ssl-key parsing lives in `common::config::ssl_configs::apply_ssl_config_key(&mut SslConfig, key, value)` (`pub(crate)`). Both `ProducerConfig` and `ConsumerConfig` call it from their `key if key.starts_with("ssl.")` arm. Do NOT re-add a private `parse_ssl_config` to either config (DoD §6).
- `ConsumerConfig.security_protocol` is `SecurityProtocol` (Copy enum), default `Plaintext`; `security_protocol()` accessor returns `.name()` (canonical UPPERCASE), so case-insensitive input ("sasl_ssl") reads back as "SASL_SSL". The pre-Phase-17 impl stored the raw string — `consumer_config_test::test_case_insensitive_security_protocol` was updated to assert the canonical name.
- Consumer ctor: build `log_context` BEFORE the channel builder (it was created after the old hardcoded builder); then `channel_builders::client_channel_builder(config.security_protocol, Some(&config.ssl_config), Some(&config.sasl_config), None, config.client_id(), log_context.clone())` mapped to `KafkaError::illegal_argument` + `Selector::with_defaults_and_log_context(...)`. Identical shape to `kafka_producer.rs:278`.
- SASL_SSL with default `SslConfig` (no truststore) does NOT error — `load_root_certs` falls back to webpki system roots. To test the "missing ssl" error path, pass `None` as the `ssl_config` arg to `client_channel_builder` (exercises its own `requires ssl_config` validation), not an empty SslConfig.

**Producer `send()` shadowing gotcha (cost me several compile cycles):**
After PR #10, `impl KafkaProducer<Vec<u8>,Vec<u8>>` has an inherent zero-copy
`send(record: ProducerRecord<&[u8],&[u8]>, callback)` that SHADOWS the
`Producer` trait's `send(record: ProducerRecord<K,V>)`. Any test holding a
concrete `KafkaProducer<Vec<u8>,Vec<u8>>` and calling `.send(record)` with a
`Vec<u8>` record fails to compile (wrong arg count + type). This silently broke
ALL `plaintext_consumer_*` integration tests at the merge commit (pre-existing,
not introduced by Phase 17). Fix: call the trait method via fully-qualified
syntax `<KafkaProducer<Vec<u8>,Vec<u8>> as Producer<Vec<u8>,Vec<u8>>>::send(&producer, record)`.
Plain `Producer::send(producer, record)` is NOT enough — UFCS doesn't infer the
trait's K,V params and doesn't auto-ref `&self` (add explicit `&` for owned
producers, omit for already-`&` params).

**Integration test gating:** `tests/integration/main.rs` is `#![cfg(feature = "integration-tests")]`. `cargo xtask lint`/`test` do NOT pass that feature, so the integration target's clippy warnings are outside DoD scope. Verify integration compiles with `cargo test --features integration-tests --test integration --no-run`. New SASL_SSL e2e test: `tests/integration/sasl_ssl_consumer_test.rs` (cluster on :9097, admin/admin-secret, ca via `ctx.ca_cert_pem()` + `ssl.endpoint.identification.algorithm=""`).

**Could NOT run live:** Docker cluster not available in this env; integration test was verified compile-only.
