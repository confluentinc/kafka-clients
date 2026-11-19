# Fix plan: calls that never settle around `close()`

Status: **approved** by the owner, 2026-11-18, with §8's recommended answers: the (root conflict) reading, every `_cb` call counted, Q49 left out, and the order of §6 with the force push of #333. The work started once the new tests commit was made.

**Source.** A behavioural parity audit of the producer against Java 4.4.0. It reproduced each case against a real broker, ran the same scenario in Java, Python and .NET, and kept the repros and their outputs.

**Rules.** Root `CLAUDE.md` at PR #327's head, `05abb9564`. The copies on #312 and #333 are identical to it.

## 1. The bug: one cause, two findings

- **What creates the producer's hold on Node.** The producer's threadsafe function (tsfn) is created referenced, so an open client keeps Node running.
- **What drops it.** The first close completion calls `cbq_closed()` (`native/producer.c` `close_on_complete`), which unreferences the tsfn (`native/callback_queue.c:116`).
- **Why calls go missing.** From then on, nothing holds Node for a completion the core queues later. Such a completion is drained only if something else keeps the event loop alive.
- **When it came in.** PR #312, commit `654eee903`.

The two findings:
- **A call on a closed producer (high).** An awaited `close()`, `close(timeout)`, `[Symbol.asyncDispose]()`, `flush()` or `partitionsFor()` never settles on a closed `KafkaProducer` or `MockProducer`. Node exits with code 0, and the code after the `await` never runs.
- **Two closes at once (medium).** Two `close()` calls on an open producer: the second's completion arrives after the first unreferenced the tsfn, and is lost. This happened in 3 of 5 runs.

**Every other async call has the same exposure.** A completion that arrives after the close completion is affected the same way:
- a `partitionsFor()` still waiting for metadata;
- a record future's `get()` whose wait completes after the close;
- a send whose hand-over completes after the close.

## 2. The bar: Java

The audit ran each case with a keep-alive timer, so that something else held Node. In every case the core already gives Java's answer (Java 4.4.0 runs: `AfterClose.java`, `MockAfterClose.java`, `ConcurrentClose.java`). So the fix only has to make the calls settle, and no answer changes.

| Call | Java 4.4.0 | JS today, nothing else holding Node | JS with a keep-alive (= after the fix) |
|---|---|---|---|
| `KafkaProducer`, after `close()`: `close()`, `close(0)`, `await using` disposal | return | never settles, exit 0 | resolves |
| `close(-1)` after `close()` | `IllegalArgumentException` "The timeout cannot be negative." | rejects (answered inside the call) | the same |
| `flush()` after `close()` | returns | never settles | resolves |
| `partitionsFor(known)` after `close()` | the cached partitions | never settles | resolves with them |
| `partitionsFor(unknown)` after `close()` | `KafkaException` "Requested metadata update after close" | never settles | rejects `KafkaError`, same message |
| `send()` after `close()` | `IllegalStateException` "Cannot perform operation after producer has been closed" | rejects (inside the call) | the same |
| `MockProducer`, after `close()`: `close()`, `close(5)`, `await using` disposal | return | never settles | resolve |
| mock `flush()` after `close()` | `IllegalStateException` "MockProducer is already closed." | never settles | rejects `IllegalStateError`, same message |
| mock `partitionsFor()` after `close()` | `[]` | never settles | resolves `[]` |
| two `close()` calls at once on an open producer | both return | the first is lost in 3 of 5 runs | both resolve |

Python and .NET settle every case. Where they differ from Java, the difference is one their own rules document: their `flush` and `partitionsFor` after close raise `IllegalStateError` / `IllegalStateException`. After the fix, the JavaScript client matches Java exactly, with no deviation to record.

## 3. The fix

### 3.1 The queue counts the calls waiting for an answer (`native/callback_queue.c`, `native/util.h`)

1. **Two new fields on `CallbackQueue`.**
   - `pending`: the client's `_cb` calls whose completion hasn't run yet.
   - `held`: whether the tsfn is referenced right now. It starts at 1, because Node-API creates the tsfn referenced.
2. **One function decides the hold**: `cbq_hold_update(env, q)`.
   - The rule: hold while the client is open (`self_strong`), or while `pending > 0`.
   - It calls `napi_ref_threadsafe_function` or `napi_unref_threadsafe_function` only when that answer changes.
   - Neither call is counted by Node-API (a uv handle's ref is a flag), so the count must be the binding's own.
   - It does nothing once the destroy began (`stopped`), while the handle is finalized (`finalizing`), or once the tsfn is gone (`!tsfn_alive`).
3. **Two calls around each request.**
   - `cbq_call_begin(env, q)` runs right before each `_cb` call: `pending++`, then the hold update. That re-holds Node for a call made after the close.
   - `cbq_call_end(env, q)` is the last statement of each completion: `pending--`, then the hold update.
   - So the unref happens only after the JS callback ran. The awaiting code's continuation then runs, as it does today.
4. **`cbq_closed` changes in one place.** It keeps making the handle reference weak, and still sets `self_strong = 0`. It then calls the hold update, instead of unreferencing unconditionally.
5. **Why both findings are fixed.**
   - Two closes at once: the first completion of two closes finds `pending == 1`, so the tsfn stays held until the second completion ran.
   - A call on a closed producer: a call after the close takes `pending` from 0 to 1, which re-holds Node until its answer.
6. **A completion that never runs JS** (one run by the destroy, from the handle's finalizer) still decrements `pending`. The hold update is skipped there, because the tsfn is being released anyway.

### 3.2 Every `_cb` call site is counted (`native/producer.c`)

**On #312:**
- `send_with_callback_cb`, ended in `send_on_handover`;
- `flush_cb`, ended in `flush_on_complete`;
- `close_cb` and `close_with_timeout_cb`, ended in `close_on_complete`, after the JS callback;
- `KafkaFuture_get_cb` and `get_with_timeout_cb`, ended in `future_on_wait`.

**On #333, after the rebase:**
- `partitions_for_cb`, ended in `partitions_for_on_complete`.
- `MockProducer` reaches all of these through the same `p->view` sites, so it needs nothing of its own.

Counting the sends' hand-overs too costs two integer operations per send, on the JS thread only. One rule then covers every call: an outstanding `_cb` call holds Node, whatever order the core answers in.

As built on #312, no scenario found answers a hand-over or a record future's wait after the close's completion:
- a send waiting for metadata across `close(0)`, and the waits of lingering records across `close(0)`, are answered before the close completes, while the open producer still holds Node;
- a send made after the close is answered inside the call.

So on #312 those two counts are guarded by `check-tsfn-lifetime` and by the test of a late `close()` and `flush()` after a record's callback ran (§4, item 5), not by a test of a late answer.

### 3.3 The rule text and its linter

The root's Lifetime rule says:

> The binding leaves the client's thread-safe function referenced, as Node-API creates it, and calls `napi_unref_threadsafe_function` once `close()` has closed the client, so nothing is counted.

Read literally, that is today's code, and it produces both findings. Those findings break:
- the root's own "an async one rejects with it" (Implementation over the FFI);
- the same rule's Why: "this way no pending send or poll is cut off";
- Java parity.

So the code follows the rule's intent: a closed client holds Node only while a call made on it is still waiting for its answer. On #333 this is recorded as a **(root conflict)**, under `DECISIONS.md` entry 4's exception for root text that, read literally, breaks the client, with a wording proposal for the rules' owner. #312 predates both the mark and `DECISIONS.md`, so there `javascript/CLAUDE.md` states the reading in plain words, and #333 adds the mark and entry 33 on the rebase. The proposal: "…calls `napi_unref_threadsafe_function` once `close()` has closed the client and every call made on it has had its answer."

Three places change with it:
- **`javascript/CLAUDE.md` "Threads and liveness"**: the bullet on the threadsafe function, marked (root conflict) with the reason.
- **`javascript/CLAUDE.md` rule 10, "Closed state and teardown"**: one sentence, that the close completion lets Node go once no call waits.
- **The linter rule `check-tsfn-lifetime`** (`scripts/lint/`, with its tests and the mutation test). Today it bans `napi_ref_threadsafe_function` in `native/` and allows the unref only in `cbq_closed`. It becomes:
  1. `napi_ref_threadsafe_function` and `napi_unref_threadsafe_function` appear only in `cbq_hold_update`;
  2. every `kafka_*_cb(` call in `native/` is preceded, in its function, by `cbq_call_begin`;
  3. every completion function passed to a `_cb` call ends with `cbq_call_end`.

  Points 2 and 3 are checked by function, as the other `native/` lints read the C sources. The mutation test gains one mutant per point.

## 4. Tests

The tests use Java's results from §2 as the expected values. A **child process with nothing else holding Node** is the only setup that reproduces the bug, because Jest's workers keep the event loop alive. So each test spawns `node` on a small script and checks three things:
- the script printed the marker it prints after the `await`;
- the outcome (the class and Java's message);
- exit code 0.

These patterns already exist in `test/unit/producer/kafka_producer.test.js` and `test/integration/producer.test.js`.

**On #312, with the fix:**
1. **After `close()` (unit, unreachable bootstrap, no broker).** For `close()`, `close(0)`, `close(-1)`, `await using` disposal, `flush()` and `send()`: each settles with Java's outcome from §2, and the code after it runs.
2. **Two calls at once on an open producer (unit, unreachable bootstrap).** The pairs `close()+close()`, `close()+close(0)`, `close()+flush()` and `flush()+close()`, each run 25 times in one child. Every Promise settles, and the child prints "all settled". At 3 failures in 5 runs today, 25 iterations catch the bug every time.
3. **A record future's wait across `close(0)` (unit, unreachable bootstrap).**
   - The setup: a lingering record (`linger.ms` 60000), its `get()` already waiting natively, then `close(0)`.
   - The expected result: `get()` rejects with `ExecutionError`, its `cause` the `KafkaError` "Producer is closed forcefully.", as Java's `get()` throws `ExecutionException`, and the child exits.
   - As built on #312: an integration test, since a record lingers only with a broker (with an unreachable bootstrap its send waits for metadata until `max.block.ms`). It guards Java's answer and the exit: both waits are answered before the close completes (§3.2).
4. **The hold still ends (unit).**
   - A closed producer with no call waiting lets the child exit promptly.
   - A closed producer whose late call settled lets it exit right after the answer.
   - An open producer still keeps it running.
   - This guards the Lifetime rule against the fix.
5. **A late call after a record's callback ran (unit, unreachable bootstrap).** Added on review. The record's callback fires with the metadata timeout, then `close()`, then a late `close()` and `flush()`: both resolve, as Java's return, and the code after them runs. A `cbq_call_end` in the delivery callback, which ends a count it did not begin, makes the late calls go unheld; no other test shows that.

As built on #312, these new tests pass on the unfixed addon too, as guards: `close(-1)` and `send()` after `close()` (answered inside the call), the `close()+flush()` and `flush()+close()` pairs (flush is answered first), a send waiting for metadata across `close(0)` (answered before the close completes), item 3, and two checks of item 4: a closed producer with no call waiting exits, and an open producer keeps running.

**On #333, with the `partitionsFor` site:**
1. **After `close()` (integration, broker):** `partitionsFor(known)` resolves with the cached partitions, and `partitionsFor(unknown)` rejects with `KafkaError` "Requested metadata update after close". Both happen in a child.
2. **`MockProducer` after `close()` (unit):**
   - `close()`, `close(5)` and `await using` disposal resolve;
   - `flush()` rejects `IllegalStateError` "MockProducer is already closed.";
   - `partitionsFor()` resolves `[]`;
   - `send()` rejects with the same message;
   - two concurrent `close()` calls both resolve.
   - All of it runs in a child.

**Teeth.** Each new test is run against the unfixed addon, to show it fails there. The site-level mutants (one missing `cbq_call_begin`, one `cbq_call_end` before the JS callback) must fail the matching test or the linter.

## 5. Docs

- **`native/callback_queue.c` and `native/producer.c` header comments**, on liveness and on the closed state.
- **`javascript/CLAUDE.md`**, §3.3.
- **`README.md`**, lifetime section. "A producer keeps Node running until `close()` closes it" gains: "a call made on a closed producer keeps it running until that call settles, as Java's call returns".
- **`CHANGELOG.md`**, a Fixed entry for both findings.
- **`MIGRATION.md`**, the "close every producer" row, if its wording needs it.
- **`DECISIONS.md`**, entry 33: the (root conflict) reading of the Lifetime rule, and why.

## 6. Branches, order and gates

1. **Fix #312 in its own worktree** (`dev/javascript-producer-bindings`), not the checkout holding #333's local tests commit:
   - the queue and the #312 call sites;
   - the rule and linter changes;
   - the #312 tests and docs.

   That makes one commit, or two: the fix with its tests, and the rule and linter change. #312 has no `DECISIONS.md`, so there the decision is recorded in `javascript/CLAUDE.md` and in this plan; `DECISIONS.md` entry 33 comes with #333.
2. **Critic review**, then fixes.
3. **Full gate at #312's head**, in that worktree:
   - `format-check`, `lint`, `lint-custom`, `doc-check`, `make -k verify`;
   - the JavaScript integration and verifiable-client tests.

   The JavaScript integration tests run with `KAFKA_CONTAINER` and `KAFKA_PORT` overridden, so another checkout's `kafka-js-test` is never removed.
4. **Push #312**, a fast-forward.
5. **Rebase #333 onto it.**
   - Rebase #333's commits onto the new #312 head, in a worktree, with the tests commit carried on top.
   - #333 rewrote much of `producer.c`, so the conflicts are resolved there by hand.
   - Add the #333 commit: the `partitions_for_cb` site, the `MockProducer` and `partitionsFor` tests, and the docs that #333 owns.
6. **Full gate at #333's head**, at the commit to be pushed, so without the local tests commit.
7. **Force-push #333 with a lease, up to the commit below the tests commit.** The tests commit stays local, as agreed.
8. **Update both PR descriptions.**

The new tests commit runs again on the rebased head before it is reviewed. It may include liveness cases that this fix changes.

## 7. Out of scope

- **Keeping a closed client reachable while one of its calls is pending.** That is the root's "Future improvements" entry Q49, about garbage collection with an un-awaited call queued. The rules say an agent builds none of these until a rule takes the entry's place. The new counter would make it a small change, and it's listed for the owner.
- **The audit's excluded items.** They're the core's or the FFI's: the timed-out `get()` message, the metric count, NUL bytes in C strings, the two counters of entry 31, and the FFI mock's `flush()` loop.
- **Transactions**, as for the whole producer.

## 8. Decisions for the owner

1. **The reading of the Lifetime rule (§3.3):** count pending calls after `close()`, recorded as a (root conflict), with the wording proposal for the rules' owner. Approve?
2. **Count every `_cb` call** (§3.2), sends and record-future waits included, or only the calls the audit hit? Recommended: every call.
3. **Q49 stays out (§7).** Confirm.
4. **The order (§6):** #312 is fixed and pushed first; then #333 is rebased onto it and force-pushed, with the new tests commit kept local. Approve the force push of #333?
