# Phase 6: Consumer orchestration + public API + Mock

## Goal

Wire the share managers and event family onto the existing
`ConsumerNetworkThread` / `ApplicationEventHandler` engine via `ShareConsumerImpl`,
and expose the public share-consumer API. This is the phase where the share
consumer becomes a usable type.

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`,
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `internals/ShareConsumerImpl.java` (~1359 LOC)
- `ShareConsumer.java` (interface), `KafkaShareConsumer.java`,
  `MockShareConsumer.java`, `ShareConsumerConfig.java`
- Share-event integration into the app-side `ApplicationEvent` enum + processor.

Tests: `ShareConsumerImplTest`.

## Rust output

- `src/consumer/internals/share_consumer_impl.rs` (`ShareConsumerImpl`, §31
  callback drain)
- `src/consumer/share_consumer.rs` (`ShareConsumer<K,V>` `#[async_trait]` trait:
  async `poll`/`commit_sync`/`commit_async`/`close`; sync
  `acknowledge`/`subscription`/`wakeup`)
- `src/consumer/kafka_share_consumer.rs` (`KafkaShareConsumer` facade +
  `new_share_consumer` factory)
- `src/consumer/mock_share_consumer.rs` (`MockShareConsumer` — mock-specific
  config as inherent methods)
- `src/consumer/share_consumer_config.rs`
- Share-event wiring in `src/consumer/internals/events/application_event.rs` +
  `application_event_processor.rs`, slot registration in `request_managers.rs`

Metrics omitted (KIP-714).

## Design notes

- RENEW re-delivery: `acknowledge(_, RENEW)` captures a *clone* of the renewed
  record (rare path only; hot ACCEPT/RELEASE/REJECT records only the offset, §27/
  §11-compliant). This introduces a share-only `K/V: Clone` bound on
  `ShareConsumerImpl<K,V>` — an accepted Rust-specific divergence, documented in
  rustdoc.
- `RequestManagers::entries()` polls `share_heartbeat` BEFORE `share_consume`,
  matching Java's share order `shareHeartbeat → shareMembership → shareConsume`.
- A `share_membership: Option<Arc<ShareMembershipManager>>` slot is Arc-shared
  with `share_heartbeat` and skipped from `entries()` (a `&mut dyn RequestManager`
  cannot be produced from a shared `Arc`); its standalone reconcile driving is
  done in Phase 7.

## Commits

- `3153002` — `ShareConsumer` trait + `MockShareConsumer` + `ShareConsumerConfig`
- `fff6f69` — wire share events into `ApplicationEvent` enum + processor
- `ff3950f` — blocker 1: `ShareInFlightBatch` offset-level in-flight tracking
- `ac983c2` — blocker 2: awaitable acknowledge-on-close bridge
- `373f307` — `ShareConsumerImpl` + §31 callback drain + `ShareConsumerImplTest`
- `7623a72` — blocker 3: wire `ShareHeartbeatRequestManager` into `RequestManagers`
- `8e118f0` — `KafkaShareConsumer` facade + `new_share_consumer` factory
- Fixups: `225dd26` (blocker 1 / RENEW re-delivery), `b637a27` (blocker 3 /
  `entries()` order), `daf4c47` (acquisition-lock-timeout empty-poll regression)

## Verification

- `cargo build`, `cargo test --lib`, `cargo xtask format-check`,
  `cargo xtask lint` — all green (2251 pass / 1 ignore / 0 fail after fixups).
- Trait-surface check (DoD §11): `Box<dyn ShareConsumer<K,V>>` from the factory,
  no enum dispatch, per-record `Deserializer` stays sync.
