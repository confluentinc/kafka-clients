---
name: review-m11-dosend-deadlock-manual-suite
description: Critic 49 patterns — auditing a carve-out by enumerating the callee's error set; verdict-can't-fail traps in the examples/txn_* manual suite; set-X-then-assert-X tautologies
metadata:
  type: project
---

Range `e147a1e..21343fe` (Milestone 11): the `do_send_bytes` / `maybe_add_partition`
guard-in-scrutinee deadlock fix plus the `examples/txn_*` manual suite. Four findings,
**all in test / example code**; the production fix was correct.

**Auditing an escape-hatch carve-out: enumerate the callee's error set, don't reason
about intent.** The fix routes on `is_api_exception() && error.error() != UnknownServerError`
(the crate spells Java's bare `KafkaException` as `UnknownServerError`). The way to
settle whether such a carve-out is safe is to list every error the called function can
produce and map each to the Java `catch` block Java would have taken. Here:
`maybe_fail_with_error` yields exactly four shapes and never returns `last_error`
itself, so a broker `UnknownServerError` cannot reach the arm wearing that code. Note
`KafkaError::error()` returns `UnknownServerError` for *every* variant lacking a
`KafkaGenericError` (`Timeout`, `RecordTooLarge`, `TransactionAborted`, …), all of which
`is_api_exception()` accepts — so the carve-out is broader than "bare KafkaException".
Unreachable today, so not a finding; check reachability before flagging.

**New review surface: `examples/txn_*` are ✅/❌ verification programs, and their
verdicts have their own failure modes.** Three of my four findings were the same shape —
*a verdict line that cannot print ❌ for the failure its own label names*:

  - **Derived-count expectations degenerate at zero.** `sequence_check(got, base, runs)`
    builds `expected = repeated(base, runs)`; with `runs` derived from the observed data
    (`runs_for` = "how many copies of the first expected value are on this topic"),
    `runs == 0` makes `expected` empty, an empty topic compares equal, and it prints
    "0 records, exactly as expected". Grep for any `expected = f(observed)` comparison
    and ask what happens at zero. Worse, the guard the code *claims* to rely on may
    inspect a different topic than the one at risk — read the gate, don't trust its
    comment.
  - **A label naming a specific error while the helper accepts any error.** Check
    whether the client actually produces the named error before assuming the label is
    right: here the label said `InvalidTransactionTimeout`, but Java wraps it in a bare
    `KafkaException` (`TransactionManager.java:1535-1536`), so the *label* was the
    defect and a "fix" toward it would break Java parity.
  - **A helper conflating the two Java surfaces.** `send_expect_failure` returns
    `Ok(error)` for a sync `Err` *and* for a failed future — exactly the distinction
    `KafkaProducer.doSend`'s catch chain draws, and the half of the fix that suite is
    billed as the tripwire for.

**Tautology pattern: set state X, operate, assert state X.** A test that seeds
`FATAL_ERROR` before the call and then asserts `has_fatal_error()` proves nothing about
the side effect it claims. The fix is a seed state the operation must *change*
(`ABORTABLE_ERROR` + `ProducerFenced` last_error → the ApiException path drives it to
`FATAL_ERROR`; the rethrow path would not). Watch for this whenever a test's setup and
its assertion name the same predicate.

**Pass 2 — a new discriminator can overclaim what it discriminates.** The fix for the
sync-vs-future finding added `SendFailure::{Synchronous, ViaFuture}`, and one call site
justified `ViaFuture` as proving "the record reached the broker". It does not:
`ensureValidRecordSize` throws `RecordTooLargeException`, an `ApiException`, so Java's
`catch (ApiException)` turns a **local** rejection into a `FutureFailure` too — and this
crate mirrors that (`ensure_valid_record_size` → `handle_api_exception` →
`Ok(failed future)`). Sync-vs-future is *not* local-vs-broker. When a fixup introduces a
two-way split, enumerate the scenarios each branch admits before accepting the
rationale; and check whether the case asserts the error *code*, which is usually the
discriminator that actually works (a broker `MessageTooLarge` vs a local
`RecordTooLarge`, whose `error()` degrades to `UnknownServerError`).

**Pass 3 — how to verify a discriminator claim, rather than accept it.** The fix
asserted `error.error() == Errors::MessageTooLarge` as proof the record reached the
broker. To check that, grep every *constructor* of the asserted code and walk each
caller's reachability: here the only client-side producer is
`KafkaError::record_batch_too_large` (`kafka_error.rs:438`), whose sole caller is
`producer_batch.rs`'s `finalize_split_batches`, reachable only from the split path that
`sender.rs` gates on a broker `MESSAGE_TOO_LARGE` response *and* `record_count > 1`. So
every observable `Generic(MessageTooLarge)` is downstream of a broker response and the
claim holds. An assertion on an error code is only as strong as the set of sites that
can mint that code.

**Sweep technique.** For guard-in-scrutinee re-locks, use a multi-line regex
(`rg -U '(?s)(if let|while let|match)[^;{]{0,400}?\.lock\(\)[^;{]{0,400}?\{'`) —
scrutinees span lines and a line grep under-reports. Then read each arm: log-only /
bind-or-return arms are safe.

Verified with: full `cargo test --lib` (2516), `cargo xtask lint` (clippy
`--all-targets` — examples *are* covered), and four manual examples against the live
broker. Running the self-contained examples (`txn_lso_demo`, `txn_eos_pipeline`, which
mint run-unique topics) is safe; running a *paired* producer appends a run to fixed
topics and shifts the arithmetic its consumer relies on.
