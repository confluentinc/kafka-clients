# Phase 7: Production pipeline wiring + integration tests

## Goal

Wire `new_share_consumer` into a real end-to-end background pipeline (so a share
consumer can JOIN → fetch → ack → commit → close), and translate the end-to-end
share-consumer integration tests (re-scoped in from the original deferral).

## Branch

`milestone9-share-consumer`.

## Java sources

All paths relative to `kafka/`, submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- Production wiring drawn from `ShareConsumerImpl` / `ConsumerNetworkThread` /
  `RequestManagers` share paths.
- `core/.../ShareConsumerTest.scala` and `ShareConsumerRackAwareTest.scala`
  (integration).

## Rust output

- `src/consumer/mod.rs` — `new_share_consumer` production pipeline wiring
  (`set_share_membership`, `for_share` factory)
- `src/consumer/internals/consumer_network_thread.rs` — bg-loop Phase 2.4s/2.5s
  share membership reconcile + member-id propagation
- `src/consumer/internals/share_consume_request_manager.rs` — share response
  routing (`propagate_share_member_id`, `member_id_for_test`)
- `src/consumer/internals/request_managers.rs` — `for_share`,
  `propagate_share_member_id`, `share_consume_member_id`
- `src/consumer/internals/consumer_metadata.rs` — share metadata support
- `tests/integration/share_consumer_test.rs`,
  `tests/integration/share_consumer_rack_aware_test.rs` (+ `main.rs` wiring)

## Design notes

- The bg loop drives `share_membership.reconcile(now)` each `run_once` iteration
  (the Arc-shared membership slot that `entries()` cannot poll as `&mut`), then
  propagates the member id into the `ShareConsumeRequestManager`.
- Response routing: the spawned forwarder captures the real
  `response_completion_time_ms` for BOTH the success and failure ack paths.

## Commits

- `0f3e02b` — `ShareConsumeRequestManager` response routing
- `e50dc7e` — bg-loop share membership reconcile + `for_share` factory
- `543de7c` — wire `new_share_consumer` production pipeline
- `3ca255d` — `ShareConsumerImplTest` group-id ctor cases
- `981f3c0` — share consumer integration tests
- `6add01d` — update stale `KafkaShareConsumer` factory test
- Fixups: `e1f8a8e` (ack-failure completion time), `89e15f4` (bg-loop reconcile
  test with teeth)

## Known deferrals (documented, none blocking the client working)

- `KafkaShareConsumerTest` full-pipeline MockClient round-trips — the Rust
  `MockClient` is FIFO / node-based with no request-body matchers (same gap that
  defers the sibling `AsyncKafkaConsumer` MockClient tests).
- Consume/ack + rack-aware integration tests `#[ignore]`-gated — need an
  `AdminClient` (`alterShareAutoOffsetReset` on a GROUP `ConfigResource`; none
  exists in the Rust tree) and, for rack awareness, a 3-broker cluster.
- The production join-wiring's end-to-end group-JOIN effect has no automated
  regression guard (needs the deferred MockClient matcher or a live broker); the
  bg-loop *mechanism* it feeds IS guarded by the teeth-having `run_once` test.
- `new_share_consumer` / `KafkaShareConsumer::new` require `K/V: Clone` (RENEW
  retention).

## Verification

- `cargo build`, `cargo test --lib`, `cargo xtask format-check`,
  `cargo xtask lint` — all green (final: 2257 lib tests pass, 1 ignored).
