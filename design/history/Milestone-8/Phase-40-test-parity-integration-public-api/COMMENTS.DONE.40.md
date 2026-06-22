# Critic 40 — Phase 40 review — RESOLVED

All items from the round-1 review (`COMMENTS.40.md`) addressed by the Actor.
Resolutions below; each references the original commits `4ad5a11`
(PlaintextConsumerTest) / `a061a2e` (ConsumerTopicCreationTest).

---

## UNSAFE WAKEUP HELPER — `fire_wakeup_during` — RESOLVED (option a: safe API added)

**Resolution: deleted the unsafe helper and added a safe, shareable public
wakeup handle (the Critic's preferred option a).**

The UB was real and non-negotiable: a `&mut AsyncKafkaConsumer` (held across
`position_timeout`'s await) and a `&AsyncKafkaConsumer` to the same object
were live simultaneously on two tasks via a `*const Box` round-tripped through
`usize` — UB under Stacked/Tree Borrows regardless of which fields `wakeup()`
touches.

### API addition

- New public type `WakeupHandle` (`async_kafka_consumer.rs`), `Clone + Send +
  'static`. Captures ONLY the internally-synchronized, `Arc`-backed wakeup
  state:
  - `Async { wakeup_trigger: WakeupTrigger, bg_wakeup: Arc<dyn Fn()+Send+Sync> }`
    — fires the rotating-token trigger AND pokes the bg-task `select!`, exactly
    as `AsyncKafkaConsumer::wakeup()` does.
  - `Mock { flag: Arc<AtomicBool> }` — sets the shared flag the next `poll()`
    observes.
- New method `Consumer::wakeup_handle(&self) -> WakeupHandle`, mirrored on the
  trait (`mod.rs`), `AsyncKafkaConsumer` (inherent + trait impl), and
  `MockConsumer` (trait impl). Re-exported `pub use
  async_kafka_consumer::WakeupHandle` from `consumer::mod`.
- Supporting change: `NetworkThreadCloseHandle.wakeup_fn` changed
  `Box<dyn Fn>` → `Arc<dyn Fn>` so the bg-wakeup closure can be cloned into the
  handle (constructors still take `Box` and convert via `Arc::from`, so all
  call sites + test seams are unchanged). `MockConsumer.wakeup` changed
  `AtomicBool` → `Arc<AtomicBool>` so the flag can be shared.

### Java line + perf justification

- Java parity: `CompletableFuture.runAsync(() -> { sleep(1s); consumer.wakeup();
  })` at `PlaintextConsumerTest.java:1501` / `1535`. Java's `Consumer` reference
  is freely shareable across threads; this restores that expressiveness in
  Rust without `unsafe`.
- **Perf-neutral:** off the hot path entirely. Cost is one `Arc`/`watch` clone
  at handle creation, only when a user opts in. No per-record / per-poll cost.
  Confirmed mirrored on the `Consumer` trait.

### Tests

- Deleted `fire_wakeup_during` (the unsafe helper). ZERO `unsafe` / ZERO UB
  remaining in the file (`grep unsafe` → none).
- Rewrote both `test_async_consumer_position_respects_wakeup` and
  `..._with_error_connection_respects_wakeup`: `let handle =
  consumer.wakeup_handle();` is taken BEFORE the `&mut position_timeout(...)`
  borrow; the handle is moved into a spawned task that sleeps 1s then fires
  `handle.wakeup()`. Sound — no reference to the consumer crosses the task
  boundary.
- New unit tests prove the handle fires the same wakeup state:
  `async_kafka_consumer::tests::wakeup_handle_cancels_current_token` and
  `mock_consumer::tests::wakeup_handle_wakes_next_poll` (both pass).

## Issue 1 — `test_async_consumer_headers` asserted the provisioner record — FIXED

The provisioner writes a header-less record at offset 0; the test seeked to a
hard `0` and read `records[0]` (the provisioner), so `.expect("headerKey")`
would panic. Fixed by computing `base = end_offset(...)` (the offset the
headers record actually lands at, after the provisioner) and `seek(tp, base)`,
matching every other provisioned test. The 3-header order assertions
(`headerKey`/`headerKey2`/`headerKey3`) and the `lastHeader("headerKey") ==
"headerValue"` assertion are unchanged (faithful to
`PlaintextConsumerTest.java:257-289`).

## Issue 2 — `test_async_consumer_fetch_offsets_for_time` resolved to the provisioner — FIXED

The provisioner's offset-0 record carries a broker wall-clock `CreateTime`
timestamp, so `offsets_for_times(ts=0/20)` resolved to it instead of the
produced records. Fixed by applying the in-code comment's described fix: the
test no longer provisions. The 100 timestamped records are sent directly — the
first send to partition 0 auto-creates the topic with `KAFKA_NUM_PARTITIONS=2`,
so offset 0 on each partition IS a real `ts==0` record. `base0 = base1 = 0`.
Assertions now pin `r0.offset()==0 / ts==0` and `r1.offset()==20 / ts==20`
(plus `leader_epoch==Some(0)`), faithful to
`PlaintextConsumerTest.java:1428-1456`.

## Issue 3 — live `TODO` in production code (CLAUDE.md §5) — FIXED

The `TODO(milestone-N-interceptors)` at `async_kafka_consumer.rs` (the
interceptor-loader gap) was converted to a precise DOCUMENTED-LIMITATION
comment (no TODO/FIXME token): config-based `interceptor.classes` loading is
untranslated; `new` builds an EMPTY `ConsumerInterceptors` for both consumer
and invoker (behaviorally identical today); the seam that can carry a non-empty
chain is `new_with_components` (`pub(crate)`). `grep -n "TODO" ` on the new line
→ none.

## Issue 4 — `offsets_for_times` lossy mapping undocumented — FIXED

Added a contract note (no behavior change) on:
- `Consumer::offsets_for_times` and `Consumer::offsets_for_times_timeout`
  rustdoc (`mod.rs`): unresolved partitions are OMITTED (key absent) rather
  than present-with-null (Java's behavior); callers porting Java `keySet()`
  iteration must treat absent as "no offset".
- The `OffsetAndTimestamp` type rustdoc (`offset_and_timestamp.rs`), cross-
  linking to the trait method.

---

## Verification (all green)

- `cargo build` — clean.
- `cargo test --lib` — 2005 passed; 0 failed (incl. the 2 new wakeup-handle
  unit tests).
- `cargo test --features integration-tests --test integration --no-run` —
  compiles.
- `cargo clippy --features integration-tests --test integration` — ZERO
  warnings in the touched file `plaintext_consumer_test.rs` (remaining warnings
  are pre-existing in other integration files, not in scope).
- `cargo xtask lint` — clean.
- `cargo xtask format-check` — clean.
