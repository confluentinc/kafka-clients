---
name: phase12_5_commits_1-2_notes
description: Milestone-8 Phase 12.5 (1-2/N) — Arc<Inner> response routing for coordinator + topic_metadata RMs; trait dispatch via dyn doesn't need trait import in scope; tokio::spawn in poll forces tests to #[tokio::test]
metadata:
  type: project
---

Phase 12.5 commits (1)/(2) wire response routing for `CoordinatorRequestManager` and `TopicMetadataRequestManager` using the canonical `Arc<Inner>` interior-mutability pattern from `commit_request_manager.rs:1410-1429`.

**Why:** Phase-12 audit (`design/history/Milestone-8/Phase-12/RESPONSE-ROUTING-AUDIT.md`) identified 4 BROKEN RMs whose `UnsentRequest` build sites dropped the `oneshot::Receiver` paired with each request — broker replies arrived into closed channels. Phase 12.5 wires response routing for all 4; this entry covers (1) coordinator and (2) topic_metadata, the two that fit the `Arc<Inner>` pattern (heartbeat + fetch use mpsc channel-back).

**How to apply (carry-overs to Phase 12.5 (3)/(4) and future RM refactors):**

1. **`Arc<Mutex<RM>>` → `Arc<RM>` migration cascade.** When promoting an RM to interior mutability, the slot type changes from `Option<Arc<Mutex<RM>>>` to `Option<Arc<RM>>`. Cascade callsites:
   - `RequestManagers::*_handle()` return type
   - Fields on dependent RMs (`AbstractHeartbeatRequestManager::coordinator_request_manager`)
   - Ctor in `async_kafka_consumer.rs`
   - All `Arc::new(Mutex::new(...))` in tests become `Arc::new(...)`
   - `set_coordinator_for_test` becomes `&self` (interior mutex). Test code drops `.lock().unwrap()` indirection.

2. **Trait dispatch via `dyn` doesn't require trait import.** After refactoring `coord_arc.lock().poll(now)` to `coord_arc.poll_shared(now)`, the `use super::request_manager::RequestManager` import becomes unused in `consumer_network_thread.rs` and `events/application_event_processor.rs`. **Why:** Rust resolves methods on `dyn Trait` via the vtable; the trait import is only needed when calling trait methods on *concrete* types. Calls through `entries() → Vec<&mut dyn RequestManager>` still work. **Watch:** `cargo build` will surface the unused import via `#![deny(warnings)]`.

3. **`#[test]` → `#[tokio::test]` when `poll` spawns.** After the build site spawns the response forwarder via `tokio::spawn`, **every** test that calls `manager.poll(...)` needs a tokio runtime — even tests that don't depend on the forwarder's effect. The spawn itself requires a reactor. Conversion is mechanical: `#[test] fn name()` → `#[tokio::test] async fn name()`. Helper functions called from tests can stay sync.

4. **Provide `poll_shared(&self)` + delegate from `RequestManager::poll(&mut self)`.** The `RequestManager` trait requires `&mut self` but `Arc<RM>` only hands out `&RM`. Pattern: add a public `poll_shared(&self)` that does the work via interior mutability, then `impl RequestManager for RM { fn poll(&mut self, now) -> PollResult { self.poll_shared(now) } }`. Same for `signal_close` → `signal_close_shared`. The bg task (`consumer_network_thread.rs::run_once`) calls `coord_arc.poll_shared(now)` through the `Arc`.

5. **`fatal_error()` returns owned value, not borrowed.** Before: `pub fn fatal_error(&self) -> Option<&KafkaError>` (lent from field). After interior mutability: `pub fn fatal_error(&self) -> Option<KafkaError>` (clones via `.lock().clone()`). Callers stop calling `.cloned()` — the type change does it for them. **Reason:** `MutexGuard` borrows cannot outlive the guard.

6. **Drop the guard before firing oneshot completions.** Inside `on_response_inner` / `on_failure_inner` / `poll_shared` (expire-stale pass), acquire the inflight guard, mutate, then **drop the guard** before calling `state.complete(...)`. User-facing futures may run continuations that themselves touch the manager (e.g. application code that calls `consumer.partitions_for(...)` from a previous `complete()` continuation). Holding the guard across `state.complete(...)` risks re-entry deadlock on `std::sync::Mutex`. Pattern:
   ```rust
   let mut state = guard.remove(idx);
   drop(guard);
   state.complete(Err(...));
   ```

7. **Test-only accessors when `Vec<...>` is behind a Mutex.** Before: `pub(crate) fn inflight_requests(&self) -> &[TopicMetadataRequestState]` lent a slice directly from the field. After: `Mutex<Vec<...>>` cannot lend a slice across the guard boundary. Replace with three `#[cfg(test)]` accessors that pull the small shape tests actually need:
   - `inflight_count(&self) -> usize`
   - `inflight_snapshot(&self) -> Vec<(u64, Option<String>)>` (id + topic)
   - `inflight_remaining_backoff_ms(&self, idx, now) -> i64`
   Tests previously calling `manager.inflight_requests()[0].id()` become `manager.inflight_snapshot()[0].0`.

8. **Delete the old "manual harness" test.** When a test exists only to demonstrate the manual handshake pattern (call `unsent.handler().on_complete(...)` then `manager.on_response(...)` directly, sidestepping production routing), and the new production wire-up makes that path automatic, **delete the test**. Add a new regression test that drives the same scenario through the spawned forwarder (`yield_now().await` x2 after firing). The audit explicitly flagged `test_client_response_route` at `topic_metadata_request_manager.rs:872` as this category.

9. **Two `yield_now().await` after `on_complete` is the minimum.** The forwarder pattern `match response_rx.await { Ok(Ok(mut cr)) => match cr.take_response_body() { Some(...) => Self::on_response_inner(...), ... } }` requires:
   - Yield 1: the forwarder task wakes from `response_rx.await`.
   - Yield 2: leaves headroom for `on_response_inner` to fully execute (which acquires Mutex slots).
   One yield is sometimes enough; two is the safe default.

10. **Empty-string vs `None` group ID check belongs in `assert!`, not Option.** `CoordinatorRequestManager::new` takes `impl Into<String>` and asserts `!group_id.is_empty()`. Translated `testNullGroupIdShouldThrow` becomes `#[should_panic(expected = "group_id must not be empty")]` and passes an empty string. Pattern survives the refactor.
