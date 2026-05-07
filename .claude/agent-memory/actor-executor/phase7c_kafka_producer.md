---
name: Phase 7c — KafkaProducer skeleton
description: Construction-path translation; deferred public ctor; +Send on KafkaClient::poll; Drop pattern
type: project
---

Phase 7c lands the `KafkaProducer` construction path only.

## Public ctor deferral pattern

`KafkaProducer::new(props)` and `with_serializers(props, ...)` currently
return `KafkaError::UnsupportedOperation` because the production
`NetworkClient` requires `DefaultMetadataUpdater`, which is not yet
translated. The deferred-error message contains "DefaultMetadataUpdater"
so callers get a clear pointer at the gap.

**Why**: CLAUDE.md rule 5 — "fail with an explicit `KafkaError`, not a
silent stub or hang" — applies here. Phase 7d/8 will lift the deferral.

**How to apply**: when a Phase translates a class that depends on a
not-yet-translated dep, prefer `Err(UnsupportedOperation("…name…"))`
over panic/hang. Tests pin the deferred message so future contributors
know what to remove.

## `KafkaProducer<K, V, C: KafkaClient>` is generic over the client

Matches `Sender<C>` (Phase 6e). Tests use the generic surface with a
local `StubKafkaClient` mock; production wiring will pick a concrete
`C` once `DefaultMetadataUpdater` lands.

## Cross-module change: +Send on poll futures

The Sender's run loop calls `client.poll(timeout, now).await`. For the
spawned task to be `Send`, the future returned by `poll` must be `Send`.
Async-fn-in-trait without an explicit bound desugars to a non-`Send`
future. Fix:

```rust
// Before:
async fn poll(&mut self, timeout_ms: i64) -> Result<(), KafkaError>;

// After:
fn poll(&mut self, timeout_ms: i64) -> impl Future<Output = Result<(), KafkaError>> + Send;
```

Applied to both `KafkaClient::poll` and `Selectable::poll`. Existing
impls required no change (their futures are already Send). The
cascading constraint follows the call chain — every `poll` in the
graph needs `+ Send` if any caller spawns its future.

## Drop pattern for spawned Tokio tasks

`Drop` is a sync context — cannot `.await` a JoinHandle. The pattern:

1. Capture `Arc<AtomicBool>` handles for `running` / `force_close`
   BEFORE moving the worker into `tokio::spawn`.
2. In `Drop`: flip both atomics, then call `JoinHandle::abort()`. The
   worker's `while running.load(Acquire)` exits on the next iteration.
3. The worker's `poll` (or whatever it awaits) must `.await` something
   sleep-like so the loop yields back to the runtime — otherwise
   `force_close=true` is never observed.

Phase 7e will add async `close()` that awaits the JoinHandle gracefully.

## "Spawn LAST" discipline

`tokio::spawn(sender.run_loop())` is the very last statement in
`new_for_test`. Every error-returning path BEFORE the spawn returns
`Err` without leaking a background task. This mirrors Java's
"throw in constructor → close partial state" idiom.

## Test mock: local `StubKafkaClient` vs sibling `MockClientImpl`

`sender::tests::MockClientImpl` is `pub(super)` — only visible in
`sender.rs`. Cross-file tests in `kafka_producer.rs` would require
either lifting the visibility (invasive) or building a local mock.
Chose the local mock — narrow surface (just enough to spawn+drop), no
cross-file coupling. Pattern: implement KafkaClient with no-op or
trivial returns; `poll` does a brief `tokio::time::sleep` so the
spawned loop yields back.

## Field choices that matter for 7d

- `accumulator: Arc<RecordAccumulator>` — pub(crate) on the field, so
  Phase 7d's `send` body can call `accumulator.append(...)`.
- `partitioner: Option<Arc<dyn Partitioner>>` — Phase 7c always sets
  `None`. Phase 7d needs to thread a config-injected partitioner
  through; Java uses reflection (`getConfiguredInstance(...)`), Rust
  will need a builder pattern.
- `compression: Box<dyn Compression>` — held only for type identity.
  The accumulator stores `CompressionType` directly (Phase 6d).

## ByteArraySerializer vs ByteArrayOwnedSerializer

`ByteArraySerializer: Serializer<[u8]>` (unsized parameter — used on
the hot path with `&[u8]`). `ByteArrayOwnedSerializer: Serializer<Vec<u8>>`
exists in `serdes.rs` for the case where K=Vec<u8> (which is what
`ProducerRecord<K, V>` requires since `K: Sized`). Same split for
`StringSerializer<str>` vs `StringOwnedSerializer<String>`. Tests
that need `KafkaProducer<Vec<u8>, String, _>` use the *Owned variants.
