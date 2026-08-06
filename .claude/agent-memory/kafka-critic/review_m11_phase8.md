---
name: review-m11-phase8
description: M11 Phase 8 (TransactionManagerTest parity sweep + broker integration + consumer ControlRecordType fix) — 11 findings pass 1; new-harness-surface-is-inert, unreachable discriminating property, and audit populations that exclude what the phase created
metadata:
  type: project
---

Phase 8 (N=48), the milestone's last phase: 47 `TransactionManagerTest` + 10 `SenderTest`
methods, `MockClient` matcher/late-response surface, 4 broker integration tests, and the
one production change — the consumer `ControlRecordType` / `containsAbortMarker` fix.
**11 findings on pass 1**, breaking the four-phase run of clean-on-arrival: two are
substantive (a new harness surface that does nothing; an integration test whose stated
discriminating property is unreachable), five are records, four trivial.

**The phase's own core reasoning was right every time; its descriptions of scope and cause
were wrong five times.** Same signature as 5a/5b/6/7 — verify the *sentence*, not just the
conclusion.

## 1. Highest-yield check of the phase: "does the new harness surface actually do anything?"

`MockClient::disconnect_by_id_with_late_responses(node, true)` was added *specifically* so
`testReceiveFailedBatchTwiceWithTransactions` could deliver a late response. It cannot.
Chain to re-run on any future "retained request / late response / out-of-order response"
harness addition:

  - the Rust `Sender` sends produce requests with **`None` as the callback**
    (`sender.rs:2330`, `// No callback -- we process responses after poll() returns`);
  - responses are routed by `pending_produce_responses.remove(&correlation_id)`;
  - `ClientRequest::make_header` reuses `self.correlation_id`, so the disconnect response
    and the late response share an id;
  - the disconnect already `remove`d the entry → the late response falls through to
    `Ok(())`.

So the "twice" never happens; the retention only stops
`send_idempotent_producer_response`'s `requests.front_mut().expect(..)` from panicking.
**Method: for any harness feature justified by one test, trace the delivery path end to end
and ask which assertion in that test would flip.** Here none would.

Bundled false Java fact worth remembering: **`ClientRequest.callback()` is a getter**
(`ClientRequest.java:104-105`), not a move. Java's retained `ClientRequest` keeps its
callback and `MockClient.respond` (`:408`) hands the *same* handler to a second
`ClientResponse`, which `onComplete()` invokes again. Any Rust comment claiming Java's
retained request "carries no callback" is wrong. `take_callback()` is `Option::take` and is
not the Java semantics.

## 2. A broker integration test can be real and still not test what its doc says

`test_idempotent_produce_survives_a_forced_epoch_bump` forces the bump for real (second
`InitProducerId` on the same `transactional.id`) — but the second producer is a **fresh
`KafkaProducer`**, so its sequences are 0 because they were never anything else. The doc's
"a client that failed to reset its sequences after the bump would be rejected with
`OutOfOrderSequenceNumber`" names `start_sequences_at_beginning` /
`bump_idempotent_producer_epoch`, neither of which is on the path. Test is valuable
(fencing + exactly-once across incarnations); the attribution is not.

**Reusable: when a test claims to cover a recovery path, ask which object holds the state
being recovered.** A new instance has no state to recover. (Also: the epoch-bump-with-reset
path *is* reachable via abort-after-abortable-error, so "the only way from the client API"
was too strong.)

## 3. Negative integration assertions need a *liveness* gate, not just a budget

The Actor guarded the duration axis ("too short and the test passes vacuously") and missed
the readiness axis: `drain_for(read_committed_reader, 8s)` asserts nothing about the
consumer having fetched at all. `assign` + `auto.offset.reset=earliest` needs a
`ListOffsets` round trip first; a slow container start turns the assertion into a no-op.
The sibling test's `drain_for` **is** gated (that consumer already delivered 2 records) —
that contrast is the tell. Cheap fix shape: seed one non-transactional record and require
the negative drain to return exactly it.

Note the pairing (`read_uncommitted` sees 3 / `read_committed` sees 0) proves *records are
on the broker*, which the awaited `send_all` acks already proved. It does not prove reader
liveness. Don't accept a pairing as a gate without asking what each half establishes.

## 4. A "rarity" justification's rebuttal can still under-state the trigger

Phase 7a deferred `containsAbortMarker` with "readers will hit it only if their producers
reuse producer IDs after an abort, which is rare". Phase 8 rebutted it correctly (producer
ids are stable per incarnation) — but then described the trigger as *abort-then-commit*, in
four artifacts. The real trigger is narrower to state and far broader in effect: the
removed guard was `is_control_batch && aborted_producer_ids.contains(producer_id)` placed
**after** `consume_aborted_transactions_up_to`, and the ABORT marker batch is itself a
control batch carrying that same producer id. So it fired on **any** aborted transaction
read under `read_committed` — including one with nothing after it, and an empty aborted
transaction whose marker is its only batch. `read_committed` was unusable on any partition
that had ever had an abort.

**Reusable: when a phase rebuts a predecessor's frequency claim, re-derive the trigger from
the deleted guard rather than from the predecessor's framing.** Accepting the old premise
("reuse is needed") and merely showing it always holds gets the conclusion right and the
blast radius wrong.

Verified-clean parts of that fix, don't re-check: `ControlRecordType` vs the Java enum
(5 members, 4 translated, `recordKey()` accounted with a reproduced grep); Java's order at
`CompletedFetch.java:210-218`; `records_bytes.is_empty()` ≡ `!batchIterator.hasNext()`;
per-batch + control-batch-only so §27's per-record budget is untouched; the abort
integration test *does* discriminate (revert → `unsupported_version` → `poll()`'s
`.expect`).

## 5. `?` before the retry decision means the batch is *dropped*, not retried

§9.25 (filed-not-fixed, correctly) said the empty batch pool means "the producer simply
carries the stale sequence state into the retry". There is no retry:
`if self.can_retry(batch, response, now)?` (`sender.rs:1896`) short-circuits before
`BatchAction::Reenqueue` exists, and `handle_produce_response_for`'s `?` (`:942`) drops the
local `HashMap<TopicPartition, ProducerBatch>` that already owns the batch. No `impl Drop`
anywhere in `src/producer/`, so: futures never resolve, pooled buffer accounting leaks,
`run_once` returns `Ok` with only a log line.

**Reusable: for any "the error is swallowed" claim, follow the `?` to the frame that owns
the object and ask what happens to it.** "Swallowed" and "harmless" are different claims;
here it is a hang-and-leak. Same family as Phase 4 lesson 1 ("deallocate later needs a
named second owner").

## 6. An audit's population must include what the phase created (third instance)

The `sender.rs` block re-ran and re-reported its header sweep: "**52 headers, 0
mismatches**". Reproduced exactly with an independent implementation. But the population is
`` Translated from `SenderTest.<name>` `` only, while the *same file* carries **50**
`` `TransactionManagerTest.<name>` `` headers under the same stated convention — 41 conform,
**9 do not, 6 added by this phase**, one (`testMultipleAddPartitionsPerForOneProduce`,
1932-1976 vs 1932-1970) running six lines into the next test's `@EnumSource` list.

Third time in M11 (after Phase 6 P4.1 and P4.2, both mine). The question to ask of any
"N of M": **what does M exclude, and did this phase add to it?**

**False-positive trap I avoided and should not re-open:** `transaction_manager.rs` carries
**78** bare-name range citations (`` `testFoo` (Java A-B) ``) and *all 78* differ from
`sender.rs`'s declaration→closing-brace convention — because they use a different, unstated
one (annotation line → one past the closing brace). Internally consistent, pre-existing,
out of scope. **78/78 mismatching is the signature of a wrong yardstick, not 78 defects.**
When a sweep flags an entire population, suspect the convention before the code.

## 7. Verify the *cause* attached to a correction, not only the correction

"…caught two of Phase 8's own ranges off by one at the end, **both** because the body is
wrapped in a `try (Metrics m = ..)`". True for
`testAwaitPendingRecordsBeforeCommittingTransaction` (2830 opens it, 2870 closes it); false
for `testUnresolvedSequencesAreNotFatal`, whose body (1534-1572) has **no inner braces at
all** — a plain last-statement slip. `grep -n "try (Metrics"` returns exactly 5 sites and
1534 is not one. Also the wrapper is not the main producer of the shape: **11** translated
methods have `        }` immediately before their closing `    }`, only 3 from that wrapper.
And it hides no third live instance (`testRecordsFlushedImmediatelyOnTransactionCompletion`
was cited 2771-2826 correctly from its first commit).

`git show <first-commit>:<file> | grep -o '(Java 2829-[0-9]*'` settles what the pre-fix
value was in one command. **zsh gotcha:** `git show $c:src/...` breaks — `:s` is a history
modifier. Use `git show "${c}:src/..."`.

## 8. Extraction / reproduction notes for this repo's shipped derivations

- Both `TransactionManagerTest` and `SenderTest` derivations reproduce exactly:
  `140/122/18/33/107`, then `0/107/0` with an empty named-OWED listing; and `53/55`,
  `comm -23` empty, `comm -13` = the two out-of-scope names, `comm -12` = 53.
- Extract `scope.awk` by locating its first line with `grep -n 'void \[a-zA-Z0-9_\]'`
  (9844) — a regex built from the *rendered* program, not from the prose around it.
- The shell block contains a stray `    //` line from the comment stripping; drop
  `^    //$` lines before `bash -n`.
- `cargo test`'s trailing "4 passed; 5 ignored" target is **doc-tests**, not `#[ignore]`d
  unit tests. Don't read it as an ignored-test discrepancy (`status.md`'s "3" is right).
- The `threads.h` claim is directly reproducible: `cc -c` a two-line probe →
  `fatal error: 'threads.h' file not found`; and `verify: build … test` →
  `build: … build-python` / `test: … test-python`, so the chain really is blocked.

## 9. Other non-findings worth not re-deriving

- **§9.26's deferral is the right call and its record is executable** (owner, missing
  fixture, three Java tests with lines, the extra owed literal, interim broker cover). Its
  "two further tests there" is accurate under "in those two files" — enumerated:
  `test_read_committed_with_aborted_transaction` and
  `test_consumer_position_updated_when_skipping_aborted_transactions`;
  `test_read_committed_with_compacted_topic` carries no such note.
- **Decompression-before-skip** in `load_next_batch` (Java's `streamingIterator` is created
  *after* the skip decision, so Java never decompresses a skipped aborted batch) is
  pre-existing and unchanged; marker batches aren't compressed, so the fix doesn't worsen
  it.
- **`MockClient` double-`build()`**: no path builds a builder twice; the matcher is not used
  to *select* which future response matches (Java doesn't either — `:249-250` filters on
  node only, then throws), so FIFO semantics hold.
- `testSenderShouldCloseWhenTransactionManagerInErrorState`'s unblocking is sound: the real
  ABORTABLE_ERROR state satisfies both of Java's mock stubs, and `force_close` +
  `close_call_count() == 1` are two independent pins on `Sender.java:274-278`'s catch arm.
- `run_a_few_more_times` reproduces Java's `numRuns.incrementAndGet() >= 4` exactly
  (3 `run_once`, 5 increments) — verified against `ProducerTestUtils.java:33-44`.
- Matchers do assert at Java strength: `prepare_produce_response` →
  `MockClient::send`'s `assert!(matcher(&built))` → `assert_eq!(batch.producer_epoch(), epoch)`.

## 10. A rustdoc insert can silently steal the next item's attributes

`close_call_count` was inserted between `current_state`'s doc comment + `#[cfg(test)]` and
its `fn`. Rust allows interleaved docs/attrs, so it compiles: `close_call_count` gets a
nonsense two-paragraph doc, `current_state` is undocumented **and loses its
`#[cfg(test)]`**, and only the file-level `#![allow(dead_code)]` keeps `cargo xtask lint`
green. **Check the item *after* any inserted method for orphaned attributes** — an insert
between an attribute list and its `fn` is invisible to build, test, format and lint here.
