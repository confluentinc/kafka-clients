---
name: m11-bindings-b6-notes
description: M11 admin bindings B6 (producers + transactions, the final slice) — the void-result D2 row, choosing Java's richest result view, rebuilding a value Java only projects, and two environment traps that make a teeth run meaningless
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B6 (`describeProducers`,
`describeTransactions`, `abortTransaction`, `forceTerminateTransaction`,
`listTransactions`, `fenceProducers`), landed on `dev/admin-bindings`. Completes
all **46** admin RPCs on both surfaces. Builds on [[m11_bindings_b5b_notes]].

## A Java result carrying only `KafkaFuture<Void>` gets no C result handle

New fifth D2 row (now in `PLAN-bindings.md` §7). `AbortTransactionResult` exposes
exactly one method, `all() -> KafkaFuture<Void>`; `TerminateTransactionResult`
exposes `result() -> KafkaFuture<Void>`. Neither carries data, and neither has
per-key granularity a caller can reach (the abort result's per-partition map is
private and the RPC takes one spec). "One opaque handle per RPC" exists to carry
per-key data/errors across a boundary with no `KafkaFuture`; with nothing to
carry, a handle whose only method is `_destroy` is ceremony plus a leak.

**Do not confuse this with B5b's two `KafkaFuture<Long>` results**, which *do*
have a value and correctly got a handle each. The discriminator is "does the Java
result carry anything at all", not "is it a single future".

Shape to reuse: `admin_sync_value_op` / `admin_async_value_op` with `T = ()` plus
an error-only callback typedef — **not** `admin_async_void_op`, which runs the
submit inside the spawned task and so breaks the module's "the RPC is enqueued on
the calling thread" invariant. (`admin_async_void_op` is right for `close`, where
there is no submit/await split.)

## Pick the Java result view that keeps the most error granularity

`ListTransactionsResult` has three views. `all()` and `allByBrokerId()` fail
wholesale; only `byBrokerId()` keeps a **per-broker** future, hence a per-broker
error. Drive the FFI from `byBrokerId()`: a listing that succeeded on broker 1 and
failed on broker 2 then reports both, and Java's `all()` is one flatten away for a
caller who wants it. Generalise: **when a Java result offers several views over
the same futures, bind the one that loses the least, and document the flatten.**

## Rebuilding a value Java only ever projects — without inventing a combinator

Java never exposes `ProducerIdAndEpoch`: `producerId(id)` and `epochId(id)` are
two `thenApply` projections of one per-id future, and `fencedProducers()` is a
third that discards both. One C row needs both scalars. Join **each projection**
over the requested key set and merge; both resolve from the same future so
neither join can observe a state the other cannot, and `KafkaFuture::get` is
re-callable so awaiting twice is not a second request.

Resist adding `KafkaFuture::zip` — Java's `KafkaFuture` has none, so it would be a
type the Java client does not have (DoD #7). Make the unreachable merge arm an
explicit `illegal_state`, not a silent drop.

## Same-name enums, opposite case sensitivity

`TransactionState.parse` is **case-sensitive** (`NAME_TO_ENUM.getOrDefault`);
`GroupState.parse` upper-cases first. `read_group_states`' rustdoc advertises
case-insensitivity, so copying that helper silently accepts names Java rejects.
Check `parse` in the Java source for every enum crossing as a name, do not assume
the previous one's behaviour.

## Two environment traps that make a green teeth run meaningless

Both bit this slice, and each made a mutation report "all passed":

  1. **A stale in-place `_confluentkafka*.so` in `bindings/python/`** (left by an
     earlier `make`) shadows the freshly `pip install`ed extension, because
     pytest's cwd precedes site-packages. Symptom: `AttributeError: module
     '_confluentkafka' has no attribute 'X_drain'` right after a *successful*
     build. Delete the in-place `.so` **and** `bindings/python/build/` before a
     docker test run. (`build/` is separately dangerous: setuptools decides by
     mtime.)
  2. **`__pycache__` survives a same-size mutation.** A pure column *swap* keeps
     `admin.py` byte-identical in length; `shutil.copy` does not preserve mtime,
     but the pyc validity check is second-resolution, so a rewrite inside the
     same second reuses the stale bytecode. Clear `__pycache__` before every run
     in the teeth harness. Same family as [[workflow_teeth_check_mtime]], one
     layer up.
  3. Corollary: `ckr-pytest:dev` bakes `/venv` into the **image**, so each
     `docker run --rm` starts from the image's extension. A teeth loop must do
     the `pip install` inside the *same* container as the pytest runs, then
     mutate only pure-Python files.

## Optionals inside a flattened second index

`ProducerState` has an `OptionalInt` and an `OptionalLong` at `(i, j)`. Two small
helpers close the gap: `indexed_optional_at(Option<&[Option<T>]>, i32)` (an
out-of-range index and an empty `Optional` are both "absent") feeding the existing
`write_optional`. On the Python side each optional must be built first and handed
to `Py_BuildValue` with **`N`** (steals even on failure); an `O` leaks the fresh
`PyLong`. `offset_map_to_py` is the precedent.

Accidental luck worth noticing: the two `ProducerState` Optionals have different
widths and so do `ProducerIdAndEpoch`'s two scalars, so those transpositions do
not compile — a better guarantee than a test. Prefer parameter orders where
same-typed neighbours are impossible.

## The residual the suites cannot reach

Swapping `state_count` and `producer_id_count` where the two `listTransactions`
entry points forward to `list_transactions_options` passes **everything**: the
Rust test calls the builder directly, and the mock fails the whole call so no
suite observes the filters end to end. Mitigation is ordering (each count
immediately follows its array) plus hand verification; report it rather than
claiming full teeth coverage.
