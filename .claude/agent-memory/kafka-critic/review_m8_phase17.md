---
name: review-m8-phase17
description: Phase 17 security-wiring review — consumer channel-builder from security.protocol; producer send-shadowing API smell
metadata:
  type: project
---

Phase 17 (commit 78204d2) wired AsyncKafkaConsumer's channel builder from
security.protocol + ssl.*/sasl.* config, mirroring the producer (PR#10).
Reviewed clean — no issues filed.

**Verification approach that worked:** diff consumer wiring char-for-char
against `src/producer/kafka_producer.rs:279` client_channel_builder call;
confirm arg order/values identical, error map_err not panic. Confirm shared
`apply_ssl_config_key` is byte-identical to the removed producer
`parse_ssl_config` body. Run `cargo test --lib producer_config::` to prove the
extraction is regression-free, and `--features integration-tests --test
integration --no-run` to prove the gated target compiles.

**security_protocol() accessor case-normalization is Java-faithful, NOT a
contract break:** Java stores raw string via CaseInsensitiveValidString +
getString() (case-preserved) but every *usage* goes through
SecurityProtocol.forName (uppercases) — see CommonClientConfigs.java:298. The
Rust accessor now returns .name() (canonical UPPERCASE). Only callers are
tests; production usage is via the enum. Safe.

**Producer send-shadowing API smell (introduced by master PR#10, NOT Phase
17):** `impl KafkaProducer<Vec<u8>,Vec<u8>>` has an inherent
`send(record: ProducerRecord<&[u8],&[u8]>, callback: Option<Callback>)`
(zero-copy FFI path) that SHADOWS the trait
`Producer::send(record: ProducerRecord<K,V>)` for the bytes specialization.
Method resolution prefers the inherent method, so `producer.send(record)` with
a 1-arg trait-style call no longer compiles for KafkaProducer<Vec<u8>,Vec<u8>>.
Phase 17's integration-test fix (fully-qualified
`<KafkaProducer<..> as Producer<..>>::send(...)`) is correct and weakens no
assertion — same trait method, just disambiguated. Worth a separate comment
against the merge if a producer-API cleanup phase is opened (e.g. rename
inherent to `send_bytes`). Not Phase 17's bug.
