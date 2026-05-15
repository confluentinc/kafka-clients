---
name: Phase-8.0 Round-2 verification patterns
description: Verified-good fix shapes for Phase 8.0 Round 1; *mut-vs-&mut analysis for take/put-back guards; constructor-field-capture as inner-class translation
type: feedback
---

Verified-good fix shapes and audit techniques learned during the
Phase 8.0 Round 2 verification of Actor 8's fixups.

## Inner-class field-capture → struct field at construction

Java's inner classes (e.g. `DefaultMetadataUpdater` inside
`NetworkClient`) freely reference enclosing-class fields like
`metadataRecoveryStrategy`. The "correct" Rust translation for a
config-like field captured at construction is to **add it as a
field on the inner struct**, mirroring Java's inner-class
synthetic-field semantics — not to thread it through every trait
method.

**Why:** the alternative (thread it through `MetadataUpdater::handle_successful_response(&mut self, ..., strategy: MetadataRecoveryStrategy)`) pollutes sibling impls (`ManualMetadataUpdater`) with a parameter they never use, and changes the trait surface for a value that's invariant per `NetworkClient` instance.

**How to apply:** when a Java inner class reads an enclosing field, prefer the struct-field-at-construction option (b) over the trait-method-parameter option (a). The exception is when the value can change at runtime — then the trait-method parameter is required.

## `*mut Option<M>` is justified over `&mut Option<M>` for take/put-back guards

For a guard that needs to put back into a slot during Drop while
the slot's owner (`&mut self`) is simultaneously being passed
elsewhere:

- `&mut Option<M>` held in the guard struct **cannot coexist** with `&mut self` passed to `maybe_update`. The borrow checker doesn't model field-disjoint borrowing across method calls.
- `*mut Option<M>` sidesteps this. The raw pointer is created from `&mut self.metadata_updater` once, then `take()` runs through `&mut self`, then `&mut self` is passed to `maybe_update`. The pointer is dereferenced only in Drop — and only on unwind, when `&mut self` is gone.

**Soundness checklist for this pattern:**
1. **Single call site** — confirm the helper is called exactly once per outer call, so no nested guard could alias the slot.
2. **Drop fires only on unwind** — confirm `.disarm()` is called on every happy path via `mem::forget`. Linear flow, no early-return between assignment and disarm.
3. **Never crosses `.await`** — the guard is `!Send` because of `*mut`. Verify the future's `Send` bound (Phase 7c required `+ Send` on `Selectable::poll`) by confirming the guard's lifetime is bounded by a synchronous helper that returns *before* any `.await`.
4. **No concurrent access during `maybe_update`** — audit which methods are reachable through `&mut self` and confirm none read `self.metadata_updater`. For `MetadataUpdaterContext`, the `is_connecting`/`fetch_nodes`/`send_internal_metadata_request` callbacks don't touch the slot.

`catch_unwind` is the safe alternative but introduces `UnwindSafe` bounds on `M` and double-panic risk. For Rust→Java translations where the trait surface is small and the slot is field-local, the hand-rolled `*mut`-based guard is the cleaner choice.

## Test seam: `#[cfg(test)] pub(crate) fn clear_in_progress_for_test`

When a MockSelector accepts bytes but never replies, the first
ready-iteration `poll()` leaves stale `in_progress = Some(...)`
state that the regression test must clear before pinning the bug
condition. JUnit's `MockClient.prepareResponse(...)` provides
the same affordance synchronously; Rust's `MockSelector` doesn't.

**Pattern:**
- `#[cfg(test)]` gate → no production-API exposure.
- `pub(crate)` visibility → callable from sibling test modules without `pub`.
- Rustdoc explains why JUnit doesn't need the equivalent.

This is a legitimate test seam — not a smell. The bug is in `send_internal_metadata_request`, not in the connect loop, so clearing the connect-loop residue is preparation, not bug avoidance.

## Verifying that a regression test actually pins the bug

For a fix to a multi-step bug (Java's "set state, call dispatch,
expect dispatch to clean up state on failure"), the regression
test must exercise **all** the steps the production code does,
not a strict subset:

1. **Wire-up step** must use the real production constructor / wiring (not a unit-test stub).
2. **Trigger step** must invoke the integrated path (e.g. `updater.maybe_update(&mut client, now)`), not the lower-level helper that bypasses the take/put window.
3. **Both side-effects** of the Java contract must be asserted. For a "clear state on failure" fix, that's typically (a) state is cleared AND (b) backoff/counter was advanced. A test that only checks one is an incomplete pin.

Tight, unconditional assertions are required. A `conditional assertion` (`if state.is_some() { assert!(...) }`) is the anti-pattern — under the buggy code, the precondition is false and the test silently passes.

## Verification of "could this be `&mut` instead of `*mut`?"

When reviewing new `unsafe` blocks that use `*mut` for field access:

1. **Construct the safe alternative**: write out `struct Guard<'a, M> { slot: &'a mut Option<M> }`.
2. **Trace the borrow checker**: does the guard's `&mut` reference into a field of `self` coexist with `&mut self` passed elsewhere? If yes → borrow-check failure → `*mut` is justified.
3. **Consider `catch_unwind`**: alternative is `std::panic::catch_unwind(AssertUnwindSafe(...))` — introduces `UnwindSafe` bound costs.
4. **Verdict**: file as Nit only if a safe alternative is clearly cheaper; otherwise accept the `unsafe` block with explicit SAFETY-comment audit.

Per CLAUDE.md `unsafe` should be a last resort but is legitimate when the safe alternative would require generic `UnwindSafe` bounds across the trait hierarchy.
