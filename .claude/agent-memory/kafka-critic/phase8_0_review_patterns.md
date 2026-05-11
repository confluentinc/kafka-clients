---
name: phase8_0_review_patterns
description: DefaultMetadataUpdater translation review — inner-class → free struct + take/put-back Option<M> trap, missing-context-on-trait-method silent-divergence
type: project
---

Phase 8.0 review of `NetworkClient.DefaultMetadataUpdater` (Java inner class) translated as a `pub(crate) struct DefaultMetadataUpdater` implementing the existing `MetadataUpdater` trait, plus the `KafkaProducer::new` / `with_serializers` / `from_config` lift from `Err(UnsupportedOperation)` stubs to working public constructors.

## The two recurring Java-inner-class translation traps

### Trap A — Trait method that doesn't accept a context cannot access enclosing-class state

`DefaultMetadataUpdater::maybe_update` was correctly given `&mut dyn MetadataUpdaterContext` as a parameter. But `handle_successful_response` (and `handle_failed_request`) were not — they only see `&mut self` + the response. In Java, the inner-class `handleSuccessfulResponse` can read `metadataRecoveryStrategy` directly off the enclosing `NetworkClient`. The Rust trait surface doesn't expose it.

The actor "solved" this by dropping the `metadataRecoveryStrategy == REBOOTSTRAP` gate in front of the REBOOTSTRAP_REQUIRED arm and justifying it inline as "the gate is enforced by the caller". **It is not.** The caller-side gate (`NetworkClient::handle_rebootstrap` returning early when strategy != REBOOTSTRAP) prevents the *teardown* but doesn't undo the **side effects** — the `info!` log, the `initiate_rebootstrap()` call that mutates `metadata_attempt_start_ms`, and most importantly the **skipped `metadata.failed_update(now)` call** that Java would otherwise hit in the `brokers.isEmpty()` fallback arm.

**Lesson for future inner-class translations**: any time the actor adds a comment that says "the gate is enforced by the caller" or "we accept this divergence because X", trace the actual behavior on both sides of the gate end-to-end. The state-machine pollution is the giveaway — when the divergence is benign, the answer is "we don't touch any state outside the gate"; when the divergence is real, you'll see "we touch X but the gate prevents Y from acting on X".

### Trap B — Take/put-back `Option<M>` silently drops trait-callback re-entry

`NetworkClient::poll` does:
```rust
let mut updater = self.metadata_updater.take().expect(...);
let timeout = updater.maybe_update(self, now);  // `self` becomes `&mut dyn MetadataUpdaterContext`
self.metadata_updater = Some(updater);
```

Inside `maybe_update`, the updater calls `context.send_internal_metadata_request(...)` which calls `self.do_send(...)`. `do_send` has an UnsupportedVersion arm that, in Java, calls `metadataUpdater.handleFailedRequest(...)`. The Rust translation guards this with `if let Some(updater) = self.metadata_updater.as_mut()` — which is `None` during the take/put window — so the callback is silently dropped.

In `DefaultMetadataUpdater::maybe_update_for_node`, `self.in_progress = Some(InProgressData(...))` is assigned **before** `context.send_internal_metadata_request(...)` runs. If the send hits UnsupportedVersion, `in_progress` is never cleared (the only clearers are `handle_failed_request` and `handle_successful_response`, neither of which fires for a request that never made it to the wire). Result: the updater is permanently wedged thinking a fetch is in progress, and `is_update_due` returns `false` forever (because `has_fetch_in_progress` short-circuits).

**Diagnostic pattern**: when a Java method uses a "set state, then call sibling that may call back to clear state" pattern, the Rust translation needs either (a) move the state assignment to AFTER the sibling returns, propagating the sibling's failure back, or (b) buffer the failure callback for replay AFTER the take/put-back window closes.

**Test gap pattern**: the actor's "production fix" test (`do_send_unsupported_version_internal_metadata_fires_failed_request`) called `do_send` directly, not via `maybe_update`. That bypasses the take/put window — the test passes but doesn't pin the bug. **When reviewing a "fixed in Phase N" claim, verify the production fix is reached through the production path the test would actually hit.**

## Public-surface drift in serialiser-marker traits

`SupportsDefaultSerializer` is a new `pub` trait + `impl for Vec<u8>` providing default `Box<dyn Serializer<T>>` for the no-args `KafkaProducer::new()`. It short-circuits Java's `key.serializer` / `value.serializer` FQCN reflection — meaning if the user passes those keys in `props`, they're silently ignored. This is an unavoidable Rust deviation but the user has no diagnostic.

**Pattern**: any time the actor introduces a marker-trait to fill in for Java reflection, check whether the corresponding Java config keys are silently dropped without a `log::warn!`. Phase 7e established the precedent (partitioner.class factory warns on unsupported FQCNs). Apply the same standard.

## `from_config` as an internal lift point — keep it `pub(crate)`

`from_config(ProducerConfig, key_ser, value_ser)` is a Rust-only entry point with no Java analogue. The actor made it `pub` to support `tests/integration/performance_test.rs:367`. But the integration test is in the same crate (`tests/integration/` compiles with the crate), so `pub(crate)` would be sufficient. **Pattern**: lift-points introduced for test wiring should default to `pub(crate)` unless an external consumer is documented.

## Test count baseline verification

Always check the baseline test count by `git checkout` to the pre-phase commit and `cargo test --lib | tail -3`. Phase 8.0 baseline at `7a0da54` was 1201; after 8.0 commits at `2a7ef65` was 1217 (+16). Actor's claim verified.

## Docstring stale claims

The docstring at `default_metadata_updater.rs:404-408` says "Tests that *do* require the full `NetworkClient` (testRebootstrap, testInflightRequestsDuringRebootstrap) live in `network_client.rs::tests`". A `grep -n "test_rebootstrap" src/network_client.rs` returns nothing for translated `testRebootstrap`. These tests are scheduled for Phase 8a but the rustdoc claims they exist now. **Pattern**: any "tests live in X" or "covered by test Y" docstring claim should be verified by grep before accepting the actor's PR.

## Quick verification commands

```sh
# Baseline test count at pre-phase commit:
git stash; git checkout <pre-phase-commit>; cargo test --lib | tail -3
git checkout <branch>; git stash pop

# Confirm every translated method has a direct test (catches Blocking 1 type bugs):
grep -n "fn handle_\|fn maybe_update" src/default_metadata_updater.rs
grep -n "handle_successful_response\|handle_failed_request\|handle_server_disconnect" src/default_metadata_updater.rs

# Find take/put-back windows that span trait callbacks:
grep -n "\.take()\b" src/network_client.rs

# Find `if let Some(updater) = self.metadata_updater.as_mut()` re-entry guards
# — every one of these is a potential silent-drop bug:
grep -n "metadata_updater.as_mut\|metadata_updater\.as_ref" src/network_client.rs
```
