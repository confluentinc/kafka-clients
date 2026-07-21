---
name: milestone11-phase2-ack-core
description: M11 Phase 2 KIP-932 acknowledgement core types — callback trait, handler, ShareInFlightBatch move semantics
metadata:
  type: project
---

Milestone 11 (KIP-932 share consumer) Phase 2 translated the acknowledgement core types.

**Why:** These are dependency-closure types for `ShareConsumeRequestManager` (Phase 5). Callback invocation wiring is deferred to Phase 5/6.

**How to apply / patterns established:**
- `AcknowledgementCommitCallback` (public, `acknowledgement_commit_callback.rs`): `#[async_trait]` trait, `async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>)`. Mirrors `OffsetCommitCallback` precedent exactly (per-commit notification, not per-record → `#[async_trait]` allowed by CLAUDE.md §11). Returns `()` because Java `onComplete` is `void`.
- `AcknowledgementCommitCallbackHandler` (internal): catches callback **panics** via `futures_util::FutureExt::catch_unwind` + `AssertUnwindSafe` (Java catches `Exception`; Rust callback can only panic), logs `error!`, always resets `entered_callback` (Java `finally`).
- `ShareInFlightBatch<K,V>`: `ConsumerRecord` is NOT `Clone` (only PartialEq/Eq gated). So: `get_in_flight_records()` returns `Vec<&ConsumerRecord>`; `merge` consumes `other` by value; `take_acknowledged_records`/`renew`/`take_renewals` use single-pass move (remove-from-in_flight, route RENEW→renewing) instead of Java's copy-refs-then-remove. Net state identical — documented as deviations in rustdoc.
- `ShareAcknowledgementMode`: `from_string` is **case-sensitive** (Java `Utils.enumOptions` returns lowercased names, so "IMPLICIT"/"EXPLICIT" are rejected). `Display` = `ShareAcknowledgementMode{mode=implicit}`. `Validator` (ConfigDef.Validator) deferred to config-wiring phase — same pattern as `ShareAcquireMode` (Phase 1).

**Tests:** `ShareAcknowledgementModeTest` + `AcknowledgementCommitCallbackHandlerTest` fully translated (the latter drops Java's `retryOnExceptionWithTimeout` — Rust callback fires synchronously on caller task per §31). ShareInFlightBatch/Exception get smoke tests; full coverage arrives with `ShareConsumeRequestManagerTest` (Phase 5).

**Gotcha:** clippy `collapsible_if` is deny-by-default here; use let-chains (`if let ... && ... && let ...`) — Rust 1.96 / edition supports them.
