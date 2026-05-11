---
name: Phase 8.0 Round 1 review-fix patterns
description: Patterns learned during Phase 8.0 Round 1 actor fixups (Critic 8 review of DefaultMetadataUpdater + KafkaProducer wiring)
type: project
---

Patterns from Round 1 fixups for Critic 8's Phase 8.0 review.

## Java inner-class field capture → Rust struct field

**Pattern:** Java's `DefaultMetadataUpdater` is an inner class on `NetworkClient` and reads `metadataRecoveryStrategy` via implicit field-capture (`NetworkClient.java:1297` accesses the enclosing instance's field). The Rust translation cannot model inner-class semantics, so the strategy must travel into the updater somehow.

**Why:** Threading the field through every trait method (e.g. `handle_successful_response`) pollutes the wider `MetadataUpdater` trait surface for the rare consumer that doesn't need it (`ManualMetadataUpdater`). The Critic explicitly approved capturing it at construction.

**How to apply:** When translating an inner-class field-capture, prefer adding the captured value as a struct field on the translated class at construction time, NOT threading it through the trait method. Update the constructor signature.

## `MetadataUpdaterContext::send_internal_metadata_request` returns Result

**Pattern:** A trait method that previously returned `()` and dispatched callbacks internally now returns `Result<(), KafkaError>`, leaving the failure handling to the caller.

**Why:** The Rust take/put-back window for `Option<M>` cannot do callbacks back to the temporarily-moved-out value. Java's inner-class direct field access has no equivalent. Propagating the error up to the caller (the updater itself, alive on the stack) lets it call `handle_failed_request(now, Some(err))` cleanly.

**How to apply:** When you find a callback hole caused by the take/put pattern, the fix isn't a side-channel buffer — it's making the trait method return the error and letting the caller (the owner of the take/put dance) handle it locally.

## Panic-safe take/put via raw-pointer Drop guard

**Pattern:** To make `take()` → `call(&mut self)` → `put_back` safe against a mid-call panic, use a stack-allocated guard struct with `slot_ptr: *mut Option<M>`. The guard's `Drop` writes `None` (or restores the value) on unwind. Use `mem::forget` (via `.disarm()`) on the happy path.

**Why:** A safe `&mut Option<M>` reference inside the guard would conflict with the `&mut self` we pass into the call (the slot is a sub-borrow of self). The raw pointer is opaque to the borrow checker. The unsafe is justified because the slot pointer is captured before the take/put dance and used only by Drop after the call returns or unwinds.

**How to apply:** For any single-slot Option<T> take/put dance where the call between can panic, write a small `Guard<T>` struct with raw-pointer slot and `disarm()` for happy path. Hand-rolling beats adding `scopeguard` as a new dep.

## Pre-test cleanup of state side-effects

**Pattern:** In tests that drive a `MockSelector`-backed `NetworkClient` through `client.ready()` then assert behavior after a manual `maybe_update`, the connect loop's intermediate `client.poll()` calls may have already triggered a metadata dispatch (setting `in_progress = Some(...)`), which short-circuits subsequent `maybe_update` calls.

**Why:** `client.poll` runs `maybe_update` internally. With `discover=false` and a ready node, the second poll iteration sees `time_to_next_update == 0` and dispatches. MockSelector accepts the bytes but never replies, so `in_progress` stays Some indefinitely.

**How to apply:** When testing `DefaultMetadataUpdater`-driven state machines, expose a `clear_in_progress_for_test()` helper (`#[cfg(test)] pub(crate)`) to reset state between the connect-priming phase and the test-specific phase. Alternative: skip `client.ready()` entirely and inject the connection-ready state directly via internal helpers. The cleanup helper is cleaner.

## `client.api_versions.update(node_id, ...)` works ONLY against the same NetworkClient instance

**Pattern:** `ApiVersions` does NOT derive `Clone` and is held inline (`api_versions: ApiVersions` on `NetworkClient`, not `Arc<ApiVersions>`). Updates are seen by the same instance via its internal Mutex.

**Why:** Producer-side `Arc<ApiVersions>` and NetworkClient-side `ApiVersions` are distinct instances. Updates to one don't propagate to the other. (See `phase8_0_kafka_producer_new.md` for the rationale on why the producer holds its own copy.)

**How to apply:** When pinning api-version ranges for a test, always target `client.api_versions.update(...)` on the same `NetworkClient` you're testing — NOT the producer-side `Arc<ApiVersions>` field. They diverge by design.
