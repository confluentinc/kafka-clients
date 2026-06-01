# Phase 12 Review — Resolved Comments

## Issue 3: `max_time_to_wait_ms` slot in `AsyncKafkaConsumer` is disconnected from the bg-task's `cached_max_time_to_wait_ms` — accessor returns 0 forever

**Commit**: `d3e9e15` (Phase 12 commit 3/N)
**File**: `src/consumer/async_kafka_consumer.rs:988` (creates fresh `Arc<AtomicI64>`), `src/consumer/internals/consumer_network_thread.rs:201, 252, 513` (bg-task creates its own `Arc<AtomicI64>` in `Self::new`).
**Severity**: **blocking** (called out by Actor — confirming it as a real bug)
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ApplicationEventHandler.java` — `maximumTimeToWait()` reads from a shared `AtomicLong` updated by the bg thread.

### Description

The production ctor creates `let max_time_to_wait_ms: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));` (line 988) and stuffs it into `AsyncKafkaConsumerComponents`. The consumer's accessor `maximum_time_to_wait_ms()` (line 1324) reads from this slot.

Meanwhile, `ConsumerNetworkThread::new` (consumer_network_thread.rs:252) creates its OWN `cached_max_time_to_wait_ms: Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` and writes to it after every `run_once` iteration (line 513). The two `Arc<AtomicI64>` instances point to different `AtomicI64` cells.

Result: the app-side `maximum_time_to_wait_ms()` accessor always returns the initial `0` (and even if it weren't 0, it would never observe bg-task updates).

### Resolution

Fixed by making `ConsumerNetworkThread::new` accept an `Arc<AtomicI64>` parameter (`cached_max_time_to_wait_ms`) — the production ctor in `AsyncKafkaConsumer::new` now constructs `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` and passes the SAME Arc into both `ConsumerNetworkThread::new` AND the consumer struct (via `AsyncKafkaConsumerComponents.max_time_to_wait_ms`). The bg ctor seeds the slot with `MAX_POLL_TIMEOUT_MS` (Java's `Long.MAX_VALUE` default is dropped to the safer "wake at least this often" bound — explicit comment in the ctor). All five Phase-10 test callsites updated to pass `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))`.

Fixup commit references `d3e9e15`.

---

## Issue 2: Dual `ConsumerStateNotifier` instances — bg-task writes to a slot that no app-side accessor reads

**Commit**: `143f30a` (Phase 12 commit 2/N) + `d3e9e15` (Phase 12 commit 3/N)
**File**: `src/consumer/async_kafka_consumer.rs:903-925` (registers notifier #A), `src/consumer/async_kafka_consumer.rs:1093-1144` (`new_with_components` builds notifier #B)
**Severity**: **blocking**
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java:289` (the single `AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata`), `343-353` (the single `memberStateListener` that writes to it), `447` (`groupMetadata.set(initializeGroupMetadata(...))`), `462` (the same `memberStateListener` passed to `RequestManagers.supplier`).

### Description

Java keeps a **single** `AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata` field and a **single** `MemberStateListener` instance. The Rust translation built **two** `ConsumerStateNotifier` instances against two distinct `Arc<Mutex<Option<ConsumerGroupMetadata>>>` slots — bg-task writes via the membership-registered notifier never reached the slot the app-side `consumer.group_metadata()` reads.

### Resolution

Applied **option (a) — consolidate via components**: the production ctor now builds the `group_metadata`, `group_assignment_snapshot`, and `state_notifier` Arcs ONCE (Java `AsyncKafkaConsumer.java:289, 343-353` parity) and threads them through `AsyncKafkaConsumerComponents` to `new_with_components`. The same `state_notifier` Arc is registered on `ConsumerMembershipManager` AND stored on the consumer struct — single source of truth.

`AsyncKafkaConsumerComponents` now requires `group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>`, `group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>`, and `state_notifier: Arc<ConsumerStateNotifier>` as mandatory fields — making the single-source-of-truth contract enforced at the type level. `new_with_components` consumes the three fields directly (no longer constructs new notifier instances).

The test fixture `make_test_consumer_with_channels` builds the slots locally and threads the same Arcs through, mirroring the production pattern. A new regression test `issue_2_state_notifier_writes_visible_through_consumer_after_registration` proves the wiring: register the consumer's `state_notifier()` Arc on a listener registration site, dispatch `on_member_epoch_updated`, and verify the app-side `group_metadata()` observes the change.

Issue 5 (orphaned-Arc smell at `let _ = (group_metadata, group_assignment_snapshot);`) is structurally resolved by this same fix — the orphan-drop block and its rationale comment are gone, because the Arcs are now properly threaded through components instead of being dropped locally.

Fixup commit references `143f30a`.

---

## Issue 5: Listener-registered `ConsumerStateNotifier`'s backing `Arc<Mutex<Option<ConsumerGroupMetadata>>>` is unreachable — writes will succeed but go to an orphaned slot

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/async_kafka_consumer.rs:903-925`
**Severity**: nit (subsumed by Issue 2's root cause)

### Resolution

Structurally subsumed by Issue 2's fix (option (a) — consolidate via components). The `let _ = (group_metadata, group_assignment_snapshot);` line and the "intentionally dropped" comment have been removed; the Arcs are now part of the `AsyncKafkaConsumerComponents` flow and reach both the membership listener registration site AND the consumer struct's `group_metadata` field. No separate fixup is required — the same commit that closes Issue 2 closes this.

Fixup commit references `143f30a`.

---

## Issue 1: `CommitRequestManager` is never polled in production — pending commit/offset-fetch requests will sit forever in `unsent_offset_commits` / `unsent_offset_fetches`

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/internals/consumer_network_thread.rs` (bg-task skips commit), `src/consumer/internals/commit_request_manager.rs` (dyn-trait `poll` returns empty)
**Severity**: **blocking**
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java:181-209` (`poll(currentTimeMs)`); `kafka/.../RequestManagers.java:91-101` (entries includes commit)

### Description

The slot-sharing refactor changed `RequestManagers.commit` to `Option<Arc<CommitRequestManager>>` and SKIPPED it from `entries()`. The bg-task's `run_once` polled `coordinator` explicitly but never `commit` — `commit_sync().await`, `committed().await`, async-commit, and auto-commit all hung in production because `unsent_offset_commits` / `unsent_offset_fetches` were never drained into `UnsentRequest`s for the network client delegate.

### Resolution

Two changes:

1. **Method signature**: `CommitRequestManager::poll_with_coordinator` now takes `&self` (was `&mut self`) — all mutation flows through interior mutability on `Arc<CommitRequestManagerInner>`. This lets the bg-task call into the method through the shared `Arc<CommitRequestManager>` handle returned by `RequestManagers::commit_handle()` without needing exclusive ownership.

2. **Bg-task wire-up**: `ConsumerNetworkThread::run_once` now invokes `commit.poll_with_coordinator(coord, now)` between the coordinator poll and the heartbeat poll — mirroring Java's `requestManagers.entries()` walk order
(`coordinator → commit → heartbeat`). The returned `PollResult` is folded into the `try_connect` + `add_all_from_poll_result` machinery alongside the other before-membership entries. `cleanup()` also drives `commit.drain_pending_offset_commit_requests()` (Java `CommitRequestManager.pollOnClose`) so close-path commits are shipped.

Regression test `run_once_drives_commit_poll_with_coordinator` enqueues a commit via `manager.commit_sync(...)`, drives one `thread.run_once().await`, and asserts the delegate's `unsent + inflight` count is ≥ 1 — proves the wire-up.

Fixup commit references `143f30a`.

---

## Issue 4: `FetchRequestManager::is_unavailable` / `maybe_throw_auth_failure` no-ops swallow auth errors silently — observable behavior gap from Java

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/async_kafka_consumer.rs` (closure construction in fetch wire-up)
**Severity**: nit (in Phase 12 PLAINTEXT-only scope) → upgrade to **blocking** for any phase that wires SSL/SASL

### Resolution

Applied option (b) per the issue's resolution guidance: closures kept as no-ops with an updated inline comment that:
- Explicitly characterizes the `maybe_throw_auth_failure` no-op as a **correctness gap** (not just a "one extra round-trip" performance hit, as the original Phase-12 commit message had framed it).
- Adds a `FIXME(phase-9-sasl)` marker calling for delegate-backed snapshots of node-availability + auth-failure maps before SSL/SASL wiring lands.
- References the Java contract (`AbstractFetch.java:452-457`) and Phase 9's SASL milestone.

Phase 12 is PLAINTEXT-only and does not exercise this path; the deferral is documented in code per the Critic's preferred option.

Fixup commit references `143f30a`.

## Issue 6: stale comment in `AsyncKafkaConsumer::new` contradicts the Issue 3 fix

**Commit**: `6114cb0` (Phase 12 commit 3/N Issue-3 fixup)
**File**: `src/consumer/async_kafka_consumer.rs:1092-1098`
**Severity**: Nit (doc-only; no behavior bug)

### Description

After the Issue 3 fixup, the `max_time_to_wait_ms` slot is seeded with `MAX_POLL_TIMEOUT_MS` and the bg-task writes to the SAME `Arc<AtomicI64>` cell every iteration. The stale comment block claimed the slot stays at `0` with no copy-back, misleading future maintainers.

### Resolution

Rewrote the comment block at `async_kafka_consumer.rs:1092-1100` to describe the actual wiring: the slot is seeded with `MAX_POLL_TIMEOUT_MS` at ctor time and the bg task writes to the shared `Arc<AtomicI64>` cell every `run_once` iteration. The app-side `maximum_time_to_wait_ms()` accessor reads the bg-task's current value through this shared Arc. Includes a reference to Java's single `cachedMaximumTimeToWait` long field at `AsyncKafkaConsumer.java:354` to anchor the parity argument.

Folded into Phase 12 commit (4/N) — `Phase 12 (4/N): factory swap in new_consumer + smoke test + Issue 6`.
