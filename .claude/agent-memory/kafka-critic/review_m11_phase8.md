---
name: review-m11-phase8
description: M11 Phase 8 (TransactionManagerTest parity sweep + broker integration + consumer ControlRecordType fix) — 11→3→1; inert harness surface, unreachable discriminating property, audit populations excluding what the phase created, and one bullet that missed three rounds running
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

---

## Pass 2 — 11 → 3. Nine fixes clean; all three findings are in the fixes' own records, two created by the fix round

Fixes across `a49d060`, `fe253e0`, `1bdd846`; nothing disputed, and the Actor re-derived
both flagged analyses before conceding (its numbers matched mine exactly, including the
nine header deviations).

### P2.1 A fix for a doc-hygiene finding repeated the defect, in a shape the sweep shipped in the same round cannot see

Pass-1 issue 8 was "an inserted method stole the next item's doc + `#[cfg(test)]`". The fix
was right *and* shipped a crate-wide sweep: **doc line → one or more `#[..]` lines → doc
line**, 0 suspects (I reproduced it). Meanwhile the fix for issue 1 deleted
`disconnect_by_id`'s body, moved its `pub fn` below a rewritten doc block, and left the old
doc block above — two summaries and two `Translated from` lines on one item, no `///`
separator, so rustdoc runs them together. The sweep's shape requires an attribute between
the two doc runs, so it structurally cannot see it.

**The discriminating sweep, worth keeping:** join each contiguous `///` block and count
`^Translated from` openers; ≥2 is the hit. One hit across all of `src/`
(`mock_client.rs:288`). More generally — when a round ships a mechanical check for a defect
family, ask which *members of that family* the check's shape excludes, then run one for the
excluded shape. Two rounds in a row here the fix commit was the new defect surface.

### P2.2 Verifying a "harness surface is inert" fix: check the *substitute* mechanism, not just the deletion

The Actor removed the inert overload and re-translated the test through a different
mechanism. What made that verifiable in three greps:

  - `pending_produce_responses` has **exactly one insert** (at send) and **exactly one
    removal** (keyed on the response's correlation id);
  - `fail_expired_batches(expired_inflight, now, **false**)` → `retain = true` → the batch
    is pushed to `batches_awaiting_response` **without** touching the routing map;
  - `batches_awaiting_response`'s only other drain is in `run`'s shutdown tail.

Those three facts make the four new assertions non-vacuous *and* prove the mutation claim in
both directions: delete the response and assertion 1 fails; the previous revision's
disconnect pushed a `ClientResponse` with the **same** correlation id, so it would have
passed. **Single-writer/single-remover counts are what turn an assertion into a pin** —
check them before crediting or faulting any "drains only by X" claim.

Also: Java's own sequence here is expiry-fails-the-batch (inside `sendProducerData`, before
`client.poll`) then *two* response handlings; the port has expiry plus *one*. The property
(a response for an already-failed batch stays ABORTABLE) survives; the doubling does not.
Documented in three places, so accepted — but note that "receive … twice" tests can lose
their doubling and still keep their property.

### P2.3 An appeal to CLAUDE.md §11 is checkable against §11's own scope note — and this one failed it

§9.28 dismissed the faithful alternative with "CLAUDE.md §11 warns against per-message
callbacks on the hot path". §11 has no callback bullet (it has `Arc<str>` identifiers,
atomics over `Mutex<i64>`, no per-call `Pin<Box<dyn Future>>`, no per-message
`tokio::spawn`), and its **"Hot path" definition** explicitly excludes "per-RPC or per-batch
top-level API surfaces". A `RequestCompletionHandler` on the produce path is **one per
produce request** — per-RPC by construction, exactly the carve-out. So the rule cited does
not reach the case, and calling it a "per-message callback" mis-states the alternative.

The conclusion was still right, for a reason the same file already documents three times:
**a Rust `RequestCompletionHandler` cannot capture `&mut self`** (`sender.rs:182`, `:348`,
`:374`) — ownership, not performance. **Whenever a record cites a CLAUDE.md rule to close a
design question, read the rule *and its scope paragraph*; §11's carve-out is the one most
likely to be skipped.** Fourth instance this milestone of a justification proving a weaker
or different claim than it states.

### P2.4 A corrected taxonomy is as checkable as the corrections it describes

Pass-1 issue 10 fixed a mis-attributed cause; the rewritten bullet then mis-classified a
*different* entry twice. `testFailedInflightBatchAfterEpochBump` was cited `3727-3810`
against `3726-3816`, so it was (a) not "±1 or ±2 at one end" — both ends wrong, the end by
**six**, landing mid-body — making it seven of nine in that class and **two**
large-deviation entries, not one; and (b) not an annotation-line slip — 3727 is the first
*body comment* line, one past the declaration, whereas the other two pre-existing slips
really did cite `@ParameterizedTest` / `@ValueSource`.

**Method: after a taxonomy sentence is rewritten, re-classify every member from the raw
data rather than reading the sentence.** The sweep output already had the numbers; the
sentence summarising it was written from memory of "the interesting one".

### P2.5 Ruling on "exact figure" vs "rounded + shipped derivation" — the derivation wins

I asked for the exact `47 649`; the Actor rounded all line counts to the nearest hundred,
kept file counts exact, and shipped the `find | wc -l` command. **Ruled in the Actor's
favour and recorded as the general rule: ask for exactness only where the number cannot be
re-derived.** An exact figure that a later commit in the same phase invalidates is a trap;
a rounded display plus a shipped command is true and stays true, and it is the same move I
have endorsed for accounting blocks since Phase 4 pass 3. Re-ran the command: every rounded
value is the correct nearest hundred and `structure.md` agrees.

### P2.6 Cheap verifications that settled adjudications this pass

  - **"A different `ClusterConfig` would fork the pooled container"** — true:
    `cluster_pool::get_or_create` keys its `OnceCell` map on `config.clone()`, i.e. the whole
    `ClusterConfig` including `server_properties`. One grep settles any "we'd need a separate
    cluster" argument in this repo.
  - **"Named unit tests cover the path"** — open each and check the *instance identity*, not
    just the assertion: all three named for the client-side sequence reset keep **one**
    manager across the bump, which is what makes the reset observable (the integration test's
    fresh producer is what made it unobservable there).
  - **`fe253e0` supersedes an immutable commit message in its own body** ("the original
    commit message cannot be rewritten, so this message carries the correction and supersedes
    it") — the right handling when a finding lands on a published commit message, and worth
    accepting rather than pressing for something impossible.

### P2.7 Housekeeping observation

`1bdd846` **deleted** `COMMENTS.48.md` where all seven sibling `COMMENTS.4x.md` files exist
as 0-byte placeholders. Not filed, but a Manager loop that reads the file to test emptiness
would error on a missing path. Recreating it is the Critic's job on the next pass anyway.

---

## Pass 3 — 3 → 1. Two fixes clean; the third is the same bullet's third consecutive miss

### P3.1 Test a shipped gate by *running the binary against a synthetic tree*, not by reading it

The round added `cargo xtask doc-hygiene` (wired into `lint`). Verifying it without touching
the repo: the check does `rust_sources("src")` relative to CWD, so `mkdir scratch/src`,
write probe files, `cd scratch`, run `target/debug/xtask doc-hygiene`. End-to-end proof of
the shipped code path, zero repo mutation — and it belongs in the Critic's toolkit for any
future path-relative xtask.

Probe set worth reusing: both defect shapes; `//!` module docs with the trigger string
(must stay silent); doc → `#[cfg(test)]` + `#[allow]` → item; doc → `#[derive]` → struct;
two separate doc blocks each with one opener; a `#[cfg_attr(docsrs, doc = "..")]` between
doc lines; a multi-line `#[cfg(all(` between doc lines.

Result: both shapes caught, all the "must stay silent" cases silent, **one FP**
(`cfg_attr(.., doc = ..)` — the standard docs.rs idiom) and **one FN** (multi-line
attributes, because the skip loop only consumes lines *starting* `#[`).

**Neither filed, and the reason is the rule:** exposure was checkable and nil —
`grep -rn cfg_attr src/` → 0, `#\[doc` → 0, `docsrs` repo-wide → 0, and `cargo doc` is in
no gate. Two multi-line attribute sites exist (`consumer_group_metadata.rs:54`, `:67`, both
`#[deprecated(`) and both are ordinary doc→attr→fn. Disclosing a demonstrated limitation
with its repro is the honest middle between filing a theoretical defect and saying "looks
fine".

Wiring audit that made "no blast radius" checkable: `lint_fix` does **not** call it
(correct — unfixable automatically); xtask already assumes repo-root CWD everywhere
(`generator/Cargo.toml`, `target/`), so the relative path adds no constraint; `verify` and
`verify-sandbox` inherit it through `lint` with no Makefile change.

### P3.2 The same bullet missed three rounds running — each rewrite derived what it was faulted on and hand-wrote what it added

  - Pass 1: one cause given for two corrections; false for one.
  - Pass 2: "eight of the nine" / "three cited the annotation"; both wrong for the same entry.
  - Pass 3: the Δstart/Δend numbers are finally all correct (verified against my sweep), the
    two exceptions correctly named — and the **new** classification column is wrong in 5 of
    10 cells, all saying "(body statement)" where four cited a **blank line after the closing
    brace** and one cited an argument inside the *next* method's `@EnumSource`.

**The reusable check:** when a fix converts prose to a table, classify every cell from the
raw data, not just the columns the finding named. And treat a self-certifying label —
here "The table is pasted derivation output so it cannot drift again" — as the *strongest*
reason to verify, not a reason to trust: the Δ column plainly was derived and the added one
plainly was not, and the label is what would stop the next reader checking.

Substantive point buried in the error: the four blank-line rows are the set's **largest
class** and have a one-line detector ("does the cited end line have content?"). A wrong
taxonomy does not just misdescribe — it hides the pattern the table exists to surface.

### P3.3 Two disclosures rather than findings, and why the bar sat there

  - **"The one other §11 citation in the PLAN"** — there are two (§10.6's per-record
    `Arc<str>`, line 2861; and line 973's "sequence assignment runs per batch on the drain
    path"). Both *are* correct appeals, so the conclusion survives and only the count is off,
    in a review-ledger sentence that nothing cross-references. Disclosed with the correction.
    Worth keeping the adjudication: §11's hot-path definition names "**batch drain**"
    explicitly while its exclusion is scoped to per-batch *top-level API surfaces*, so
    drain-path sequence assignment is **inside** the rule and a per-produce-request
    completion handler is **outside** it — the two PLAN citations sit on opposite sides of
    the same carve-out.
  - **The gitignore observation** the coordinator wanted made durable already is, twice, in
    Actor memory predating this milestone (`phase39_critic_round1_patterns.md:23`,
    `phase13a_fetch_test_notes.md:88`). Before asking for a fact to be recorded, grep the
    memory tree for it — it may already be there under an older phase's name.
