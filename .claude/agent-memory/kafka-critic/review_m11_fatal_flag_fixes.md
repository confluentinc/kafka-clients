---
name: review-m11-fatal-flag-fixes
description: Critic 52 on 3 bug-fix commits (P3 topic grouping, B3 is_fatal stamping, B1 dropped responses) — the choke-point-only enrichment defeated by a rebuild-fresh error surface. CLOSED 2→0.
metadata:
  type: project
---

**CLOSED after one fix cycle (2 findings → 0).** Issue 1 fixed by stamping
`into_fatal()` on `maybe_fail_with_error`'s fatal `else` branch + making
`into_fatal()` **promote** the poison path's payload-only `IllegalState` to a fatal
`Generic(UnknownServerError)` (recursion terminates because `with_message` builds a
`Generic`; `error()`==UnknownServerError and message preserved for both variants, so
only `is_fatal()` flips). Issue 2 fixed by changing `await_ready` to return
`(Vec<ClientResponse>, io::Result<bool>)` — Vec rides *outside* the Result so it
survives both error returns; caller dispatches before `let ready = result?;`.
Re-review: both root-caused, tests discriminating, no new defects.
**Untestable-but-disclosed gap accepted:** the auth-error return can't be exercised
because `MockClient::authentication_error` is hardcoded `None` — same code shape as
the tested connection-failed return, disclosed per CLAUDE.md §5, not a finding.
**One non-blocking residual left open (Manager's discretion):** the poison branch
(transition_to:2374) still stores an *unstamped* `IllegalState` in `last_error`, and
`run_once`→`maybe_abort_batches` (sender.rs:1018-1021) fails record futures with that
raw error — same class as Issue 1 but only on the rare Sender-side-invalid-transition
path; normal fatal paths are stamped by `transition_to_fatal_error`. Was cited only as
supporting evidence in Issue 1, never filed separately, so not re-litigated as a blocker.

Critic 52 reviewed three fix commits on `milestone11-producer-transactions`:
P3 `9d483533` (topic grouping `||`→`&&`), B3 `50fac788` (`is_fatal()` stamping),
B1 `b1f9e2c9` (dispatch responses polled during node-readiness). **2 findings** (1
high/blocking on B3, 1 medium on B1's self-flagged residual). P3 fully sound.

**The headline lesson — a flag-enrichment fix that stamps at "choke points" is
defeated by any method that REBUILDS the error fresh from `last_error.error()`.**
B3 stamped `into_fatal()` at the four state-transition choke points so the awaited
handler result, the stored `last_error`, and the pending-transition slot all
report fatal. But `TransactionManager::maybe_fail_with_error` (the surface every
`begin_transaction`/`begin_commit`/`begin_abort`/`send_offsets`/`maybe_add_partition`
[= the `send()` path] hits when the manager is *already* in an error state) does
NOT return `last_error` — it `match`es on `last_error.error()` (the wire code) and
constructs a **brand-new** `KafkaError`, discarding the stamp, then returns it
unstamped on the fatal branch (only the abortable branch stamps, with
`with_txn_requires_abort()`). So `producer.send()`/`begin_transaction()` after a
fence still return `is_fatal()==false` — the exact "retry a dead producer forever"
bug the commit claimed to fix. **When auditing any is_fatal/requires_abort/is_retriable
enrichment, enumerate the app-visible surfaces and check whether each RETURNS the
stored error or REBUILDS one; a choke-point strategy only covers the former.**

**How the gap hid, and how to catch it:** an existing production-path test
(`kafka_producer.rs:3511 test_send_after_fatal_error_returns_error`) drove
`send()` after `transition_to_fatal_error` and asserted the code + message but
**not `is_fatal()`** — so the fix's new is_fatal assertions all landed on the
covered paths (handler/last_error/pending) and none on the rebuild surface.
Reusable move: for a flag fix, grep the flag's readers and the methods that
`return Err(...)` on the target state; a nearby test asserting `.error()`/
`.message()` but not the new flag is where the hole is.

**Two compounding sub-traps on the same finding:**
- The commit's "transition_to_fatal_error is the **sole entry** to
  State::FatalError" is false: `transition_to`'s poison path (rules §1,
  Java 1124-1127) sets `current_state = FatalError` + stores an `IllegalState`
  `last_error` directly. A "sole entry / cannot be missed" claim = grep every
  writer of the target state, not just the named one.
- `into_fatal()` **no-ops on the string-payload variants** (`IllegalState`,
  `Timeout`, `RecordTooLarge`, ...). So even a naive "add `into_fatal()` to
  `maybe_fail_with_error`'s else branch" leaves the poison-path `IllegalState`
  arm non-fatal. A consuming mutator that silently no-ops on some variants must
  be checked against **what the fatal paths actually construct**, not assumed
  total.
- Watch for a disjointness comment used as a non-sequitur: "fatal and
  requires-abort are disjoint, so I won't stamp fatal" — disjointness argues for
  stamping fatal-but-not-requires_abort, i.e. the opposite.

**B1 residual (medium, self-flagged, real):** `network_client_utils::await_ready`
now returns collected responses on success, but its two `Err` early-returns
(`connection_failed` :100, `authentication_error` :117) drop the accumulated
`Vec` — the `io::Result<(bool,Vec)>` shape can't carry it in `Err`. Java's
`NetworkClientUtils.awaitReady` doesn't lose them because `client.poll()`
self-dispatches BEFORE the throw (Java 43/85 vs 70-71/86-87). The first
`is_ready` poll (:93) runs before the connection_failed check, so a response for
an UNRELATED healthy in-flight request gets dropped when the awaited coordinator
node's connection fails → that batch hangs to `delivery.timeout.ms`. Same class
as the primary bug, narrower window.

**Verified-sound, don't re-flag:**
- P3 `&&`: Java `TopicProduceDataCollection.find(name, topicId)` matches on the
  conjunction of both `mapKey` fields (`ProduceRequest.json` Name+TopicId both
  `mapKey:true`; generated `elementKeysAreEqual` fails on first differing key);
  `topic_ids_for_partitions` = one id per topic-name (HashMap), so same-topic
  entries always merge, different-topic never — no wrong-split. Java
  `Sender.java:907` / `topicIdsForBatches` :950.
- B1 success path: `handle_client_responses` is sync (`sender.rs:866`), called at
  1408 with no `.await` after → no guard-across-await; no double-dispatch because
  `poll()` drains what it returns; only free-fn callers are is_ready←await_ready←
  await_node_ready, all updated; `network_client.rs:1884` `await_ready` is a
  separate 2-arg local test helper. Test at sender.rs:3257 is discriminating.
- B3 direct choke-points (`fatal_error` 3607, `transition_to_fatal_error` 2016,
  `authentication_failed` 2255, `close` 2331) correct; abortable NOT over-stamped
  (`fail_pending_requests` 2211 leaves it unstamped) — disjointness holds on
  touched paths.
