---
name: phase5-design-notes
description: Milestone-8 Phase-5 event-layer translation patterns (ApplicationEvent, completable event identity, AsyncPollState, reaper)
metadata:
  type: project
---

# Milestone-8 Phase-5 — event-passing layer

Lessons from the Phase-5 review round that are reusable for Phase-6 /
Phase-10 work in the same module
(`src/consumer/internals/events/`).

**Why these are worth remembering:** five of the Critic-1 findings stem
from one root cause — confusing Java's `CompletableApplicationEvent<T>`
with bare `ApplicationEvent`. Future event translations in Phase-6
need the same checklist.

**How to apply:** before translating any Java event class, locate its
`extends` clause and `implements` list, then read the bullets below.

## CompletableApplicationEvent<T> mapping

Every Java event extending `CompletableApplicationEvent<T>` (or
`CompletableBackgroundEvent<T>`) MUST become a Rust variant carrying
`handle: CompletableEventHandle<T>` (and any other Java fields).
Conversely, events that extend bare `ApplicationEvent` /
`BackgroundEvent` MUST NOT carry a `handle` — they are non-completable
by design.

Sub-issues seen in Phase 5:
  - `AsyncPollEvent` extends bare `ApplicationEvent`. Translating it as
    `CompletableEventHandle<()>` loses the two-stage state machine
    (see `AsyncPollState` below).
  - Some abstract bases carry secondary futures (e.g. `CommitEvent` has
    BOTH `CompletableFuture<Map<...>> future` AND
    `CompletableFuture<Void> offsetsReady`). Both must be translated.
  - Fields on the abstract base (e.g. `Optional<Map<...>> offsets` on
    `CommitEvent`) must be preserved with the same nullability — Java
    `Optional<Map>` → Rust `Option<HashMap<...>>`, NOT `HashMap<...>`.

## AsyncPollState pattern (non-blocking volatile fields)

Java's `volatile` field clusters on non-`CompletableFuture` events map
to a `Arc<...State>` wrapper around `AtomicBool` / `Mutex<Option<T>>`.

`AsyncPollState` exposes:
  - `is_complete()` / `complete_successfully()` — backed by `AtomicBool`
  - `is_validate_positions_complete()` / `mark_validate_positions_complete()`
  - `complete_exceptionally(KafkaError)` — sets both flags via
    `Mutex<Option<KafkaError>>`
  - `error()` — returns `Option<KafkaError>` (cloned out of the mutex)

The variant carries `state: Arc<AsyncPollState>` so app side + bg task
share the same memory. App side polls `state.is_complete()` /
`state.error()` between iterations; bg task drives state forward.

## MetadataErrorNotifiable dispatch

Java's `MetadataErrorNotifiableEvent` interface (one method,
`onMetadataError(Exception)`) maps to a free-function dispatch on the
enum: `ApplicationEvent::on_metadata_error(&self, KafkaError) -> bool`.
Returns `true` for notifiable variants (`AsyncPoll`,
`CheckAndUpdatePositions`, `ListOffsets`, `TopicMetadata`,
`AllTopicsMetadata`) so the caller can match Java's "do not subsequently
process the event" contract.

A `pub(crate) trait MetadataErrorNotifiable` would have been equivalent
but required either trait objects or a separate match-arm dispatch
anyway, given everything lives in one enum.

## Completable-event identity across erased() recreation

`CompletableEventHandle::erased()` produces a fresh `Arc<dyn
CompletableEventErasedHandle>` per call. `Arc::ptr_eq` between two
calls returns `false` even though both wrap the same `HandleInner<T>`.

Fix: add `inner_id(&self) -> *const ()` on the trait, returning
`Arc::as_ptr(&self.inner) as *const ()`. Comparison uses
`inner_id()` equality.

Rejected alternative: caching the `Arc<dyn ...>` in
`OnceLock<Arc<dyn CompletableEventErasedHandle>>` on `HandleInner`
creates a cycle (`HandleInner` → cached `Arc` → `ErasedHandle` →
`HandleInner`), keeping it alive forever.

## CompletableEventReaper Java contract

  - `reap(currentTimeMs) -> u64`: increment counter BEFORE
    `fail_with_timeout` (Java line 115). The count means "events that
    were not done at observation time", not "events we successfully
    failed". Conditional increment diverges under concurrent completion.
  - `reap_on_close(&mut Vec<Arc<...>>) -> u64`: take `&mut Vec` (NOT
    `impl IntoIterator`) and clear the collection at the end (Java
    line 150). This is a public contract — Phase-10 callers expect it.

## Display impl on enum-of-Java-class-events

Java prints `ClassName{type=TYPE, enqueuedMs=N}` where `ClassName`
(e.g. `AsyncPollEvent`) and the `Type` enum (e.g. `ASYNC_POLL`) are
distinct concepts. In Rust the variant name plays both roles, so the
`type=` field is redundant. Print `<variant>{}` for the bare event and
`<variant>{enqueued_ms=N}` for the envelope.

## Test patterns for variant-shape changes

When reshaping a variant payload (e.g. adding a `current_time_ms`
field), add a small dedicated test that pattern-matches on the variant
and asserts each new field. Critical so the variant shape doesn't
silently regress under future refactors.

`assert_eq!(rx.try_recv().unwrap(), Ok(()))` does NOT compile because
`Result<(), KafkaError>` lacks `PartialEq` (KafkaError doesn't derive
it). Use `assert!(matches!(rx.try_recv().expect("sender used"),
Ok(())))` instead — the `expect` confirms the slot was consumed and
the `matches!` confirms the Ok payload.

## Phase-10 wiring contract (for the next Actor)

  * `AsyncPoll` is non-completable: the consumer's `poll()` loop polls
    `state.is_complete()` between iterations, NOT awaits a future.
  * `CommitAsync` / `CommitSync` need BOTH receivers (the typed
    `HashMap<...>` for the actual commit result AND the `()` for
    `offsets_ready`). The app side awaits `offsets_ready` before
    returning control if the user passed `offsets: None`.
  * `BackgroundEventHandler` is sender-only — drain the
    `mpsc::UnboundedReceiver` directly on the consumer struct.
  * The reaper's `reap_on_close(&mut Vec<...>)` clears the supplied
    vec. The wiring code should drain the application-event channel
    into a `Vec`, pass `&mut vec` to the reaper, then drop the (empty)
    vec.
