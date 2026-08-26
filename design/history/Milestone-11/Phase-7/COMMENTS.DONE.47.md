# Critic 47 — Milestone 11 Phase 7: CLOSED on a clean pass

Two passes, **4 → 0** — the milestone's fastest closure. Four findings, all real,
all records; **zero behavioural defects in production code at any point**, the
fourth consecutive phase clean on arrival. The Critic verified all 22 production
entry points statement-by-statement against `MockProducer.java` on pass 1.

| Pass | Findings | Character |
|---|---|---|
| 1 | 4 | records: a membership-vs-count disclosure error, an invented mechanism, a correct fix falsifying a published C doc contract, a quoted line number off by one |
| 2 | **0** | closes the phase |

## What Phase 7 delivered

`MockProducer`'s full transactional surface: the in-memory state machine
(`transaction_initialized` / `transaction_in_flight`), Java's uncommitted-record and
uncommitted-offset staging (commit publishes via the `mem::take` ≡ Java's
append-then-reassign pair; abort clears — the asymmetry is Java's own),
`fence_producer`, misuse errors with Java's literal messages, and **43 of the 44
named tests** (the 44th proven not-applicable: its NPE fires inside the Java test's
own lambda before the mock is entered — verified by the Critic that no other throw
is reachable). 69 tests in the module, up from 23.

## Beyond translation

  - **Two pre-existing defects in `Completion::complete` fixed** against Java
    567-581: the error path passed no metadata to the callback where Java passes a
    −1-filled `RecordMetadata`, and completion fired before the callback instead of
    after.
  - That correct fix **falsified a published C header contract** ("the other
    argument is null") — resolved doc-side with per-path truth, after establishing
    Java itself is inconsistent across its three callback paths and each Rust site
    mirrors its own Java counterpart exactly.
  - A **known behavioural divergence honestly recorded** (§10.10): a callback that
    re-enters the mock deadlocks where Java's reentrant monitor allows it —
    disclosed with the two hazards (async-runtime vs reentrancy) separated, rather
    than papered over or "fixed" worse.

## Lessons minted

  - **Membership, never count**: two artifact lists agreeing on "3" are invisible to
    every count-level check while naming different threes; and a *renamed* test is
    invisible even to a name-keyed body diff without an explicit rename map (19
    bodies differ raw, 6 after one normalisation, 4 after both).
  - The **`cargo:rerun-if-changed` footgun** documented in passing: emitting any
    such directive disables cargo's default re-run heuristics, so `build.rs:106`'s
    `src/` line is the only thing keeping the generated C header fresh.
  - The Actor **falsified its own claim before shipping** (a guard justification
    tested and found not-yet-load-bearing) — the record discipline is now
    preemptive rather than Critic-driven.

## Verified at closure

Gate exit 0 on every commit; 2605 tests passing (2449 lib); format-check/lint 0;
Docker clean throughout, zero §9.17 incidents; the entire phase diff confined to
`mock_producer.rs`, one FFI doc, and PLAN.

---

# Pass-by-pass reports and resolutions

# Critic 47 — Milestone 11 Phase 7 (`MockProducer` transactional surface): CLOSED on pass 1

All four pass-1 findings are fixed in the fixup commit accompanying this file. All four
were **records**, matching the Critic's own verdict that the production translation is
clean across all 22 entry points. Verification per finding is in the Actor's report.

## What each fix was

1. **The weakened-test set is four, and the two artifacts named two different threes.**
   Upheld in full, and worse than a miscount: the in-file block and PLAN named
   `testMetadataOnException` + the two closed-producer tests; the commit message and the
   memory note named `testManualCompletion` + the same two. Neither three was the set.
   Re-derived from the diff rather than from either list — comparing every pre-Phase-7
   test body at `82aa2da` against its current form, with this phase's *two* mechanical
   swaps normalised away (the Critic named one; there is also
   `make_record("topic", "keyN", "valueN")` → `recordN()`, which is why a first pass
   reported six). Exactly four remain, the Critic's set. All four are now named in the
   in-file accounting block, PLAN §Phase-7 and the memory note, with the lesson recorded
   at each: carry such a set by **membership**, never by count — two lists that agree on
   `3` are invisible to every count-level check.

2. **`test_metadata_on_exception`'s premise was false.** Upheld. Nothing on
   `error_next` → `Completion::complete` → `cb(..)` catches unwinds (the only
   `catch_unwind` in `producer/` is at `producer_batch.rs:909`, inside the `#[cfg(test)]`
   module that starts at `:845`), and the closure's own `expect` depends on panics
   propagating. Java does not swallow here either — verified that
   `MockProducer.Completion.complete` has no try/catch while
   `ProducerBatch.completeFutureAndFireCallbacks` does, at `ProducerBatch.java:318-320`.
   The rustdoc now states the true reason: an assertion that only runs *inside* the
   callback cannot fail when the callback never runs, which is a hole Java's inline
   `assertNotNull(md)` (`MockProducerTest.java:727`) has and this form closes — so the
   translation is strictly stronger, which is the point worth stating.

3. **The C contract sentence was falsified — doc fixed, code left alone.** Upheld.
   Confirmed all three Rust callback sites mirror their own Java counterparts faithfully
   (`producer_batch.rs:433` ≡ `ProducerBatch.java:315`, null metadata;
   `kafka_producer.rs:1156` ≡ `KafkaProducer.java:1060-1061`, and
   `mock_producer.rs:343` ≡ `MockProducer.java:578`, both non-null), so Java's own
   inconsistency is faithfully reproduced and the prose was the only thing claiming
   uniformity. `kafka_producer_Producer_send_async`'s doc now states the per-path truth,
   that `metadata` and `error` are **not** mutually exclusive, and that the caller must
   free every non-null handle testing them independently rather than in an `if`/`else`.
   `send_batch_async` shares the bridge (`make_record_callback`) and its existing
   cross-reference was widened to cover the same rule.

   The header is generated by `build.rs` via cbindgen under `#[cfg(feature = "ffi")]`, so
   it was regenerated rather than hand-edited: `cargo build --features ffi` exit 0, after
   which `grep -c "other argument is null" target/include/confluent_kafka.h` is `0` and
   the corrected prose appears at `:2418` and `:2473`. The header is untracked (it lives
   under `target/`), so the committed change is the Rust doc alone. `make test-c` exit 0.

4. **`ConsumerGroupMetadata.java:41` → `:42`.** Upheld; `:41` is the last parameter line.
   All three sites corrected, and the delegation the Critic offered as optional is
   included because it makes the claim self-checking: the test calls the one-arg
   constructor at `:52`, which delegates to the four-arg one declared at `:38`, whose
   first statement is the `requireNonNull` at `:42`. Verified by `grep -n` — one hit, 42.

## Both disclosed near-misses, actioned as advised

- **§10.10 deviation 2's reason did mis-answer**, and is fixed. "No `.await`, so nothing
  is held across a suspend point (CLAUDE.md §9.6.2)" is true but answers the
  *async-runtime* hazard, not the *reentrancy* hazard the deviation is about. The entry
  now separates the two and names the residual the Critic identified: the user callback
  fired from `Completion::complete` runs **with the guard held**, so a callback that
  re-enters the mock deadlocks where Java's reentrant monitor allows it. Behaviour left
  alone as disclosed — it predates Phase 7, no Java test exercises it, and releasing the
  guard first would trade away the atomicity Java's `synchronized` provides.
- **The 2451-vs-2449 delta** is read and agreed: `298a430` counted tests run, `fb3da54`
  tests passed, with 2 ignored between them. No artifact changed; the durable records all
  say 2449 passed.

Original review follows verbatim.

---

# Critic 47 — Milestone 11 Phase 7 (`MockProducer` transactional surface), pass 1

Range reviewed: `82aa2da..bd7975f` (6 commits). Diff confined to
`src/producer/mock_producer.rs` and `design/history/Milestone-11/PLAN.md`, plus the
Actor's memory notes and the Phase-6 `COMMENTS.DONE.46.md` archive.

**Production translation is clean.** All 22 production entry points were compared
statement-by-statement against `MockProducer.java` (guard order, error-knob position,
staging branch, `mem::take` vs `clear()`, flush placement, message text) and I found no
behavioural divergence. Independently verified: 40 declared / 30 present / 10 absent,
55 `@Test` / 53 translated / 2 not applicable, all three shipped extraction guards, all
53 rustdoc `(Java N)` citations, all 43 in-file `MockProducer.java:NN` citations, 69
tests passing, `format-check` and `lint` clean, no TODO/FIXME, one lock per method with
zero re-locks. Details of what was checked and deliberately **not** filed are at the end.

**Four findings, all records.** Three are wrong or incomplete claims in shipped prose;
one is a published C contract that the (correct) production fix falsifies.

---

## Issue 1: The "three landed translations were weakened" disclosure undercounts — it is four, and the artifacts name two different threes

- **File**: `src/producer/mock_producer.rs:2519` (test accounting block),
  `design/history/Milestone-11/PLAN.md:559-562` (§Phase-7),
  commit `fb3da54` message,
  `.claude/agent-memory/actor-executor/phase7_mock_producer_notes.md` (final paragraph)
- **Severity**: Missing Requirement (`definition-of-done.md` §3 — the block's own
  "what the derivation cannot see" disclosure is the only thing standing in for a
  present-but-weakened test, and it is short by one)
- **Java Reference**: `MockProducerTest.java:107` / `:122`
  (`testManualCompletion`, `assertEquals(e, err.getCause())`)

**Description.** The accounting block says *"Three others were weakened and are no
longer: `testMetadataOnException` (724), `shouldThrowOnSendIfProducerIsClosed` (624) and
`shouldThrowOnFlushProducerIfProducerIsClosed` (673)"*. PLAN §Phase-7 says the same
three. Commit `fb3da54` and the Actor's memory note both say *three* as well, but name a
**different** set: `testManualCompletion` plus the two closed-producer tests, dropping
`testMetadataOnException`.

Four were strengthened, not three. I diffed every one of the 23 pre-Phase-7 test bodies
against its post-Phase-7 form (comments stripped, whitespace normalised); every other
change in the module is the mechanical `MockProducer::with_auto_complete(x)` →
`build_mock_producer(x)` swap. The four with changed assertions are:

| Test | Before (`82aa2da`) | After |
|---|---|---|
| `test_manual_completion` | `assert!(result2.is_err(), ..)` (`:564`) | `assert_eq!("blah", error.message())` |
| `test_metadata_on_exception` | `assert!(result.is_err(), "Expected error")` (`:714`) | four callback values + `assert_eq!("dummy exception", ..)` |
| `should_throw_on_send_if_producer_is_closed` | `.message().contains(..)` (`:650`) | `assert_illegal_state` (variant + exact message) |
| `should_throw_on_flush_if_producer_is_closed` | `.message().contains(..)` (`:660`) | `assert_illegal_state` (variant + exact message) |

`test_manual_completion` is squarely in the category the disclosure exists for: it was a
landed translation of a Java test whose only error assertion (`assertEquals(e,
err.getCause())`, Java 122) had been reduced to a bare `is_err()`. It is missing from the
two durable artifacts (the in-file block and PLAN) and present only in the two
non-durable ones.

**Expected.** One number, four names, in all four places — or, at minimum, the in-file
block and PLAN §Phase-7 agreeing with each other and with the diff.

**Actual.** Both durable artifacts say "three" and omit `test_manual_completion`; both
non-durable ones say "three" and omit `test_metadata_on_exception`. Neither three is the
set.

**Note for the fix.** This is the milestone's signature shape (a fix's own justification
is the new defect surface) applied to a *set* rather than a count: each artifact was
written next to the commit that made its own subset visible. When correcting, sweep with
`grep -rn "Three others were weakened\|three landed translations\|Three were ("
src/ design/ .claude/` — and re-derive the set from the diff rather than from either list.

---

## Issue 2: `test_metadata_on_exception`'s justifying premise is false — a panic in the callback is not swallowed, in Rust or in Java

- **File**: `src/producer/mock_producer.rs:1265-1266`
- **Severity**: Missing Requirement (false supporting fact in shipped rustdoc; the
  conclusion it supports is correct and independently justified)
- **Java Reference**: `MockProducer.java:567-581` (`Completion.complete`, **no**
  try/catch) vs `ProducerBatch.java:306-321` (which **does** try/catch and log)

**Description.** The rustdoc reads:

> Java asserts on the metadata handed to the send callback. **A panic inside the callback
> would be swallowed by the mock**, so the four values are captured and asserted
> afterwards rather than in the closure — otherwise a callback that never fires, or fires
> with no metadata, would pass silently.

The emphasised clause is false. The call path is
`MockProducer::error_next` → `MockProducerInner::error_next` →
`Completion::complete` → `cb(..)`, entirely synchronous, with no `catch_unwind`
anywhere (`grep -rn catch_unwind src/` returns nothing on this path — the producer hits
are all `#[cfg(test)]` helpers). A panic in the callback unwinds straight back out of
`producer.error_next(e)` in the test body, poisoning `MockProducer::inner` on the way,
and fails the test. It is propagated, not swallowed.

Two things make this worth correcting rather than ignoring:

1. **The code beside it contradicts it.** The closure keeps
   `metadata.expect("the callback must receive metadata on the error path")` *inside*
   itself — i.e. it relies on a panic in the callback being observable for the
   no-metadata case. If panics were swallowed, that `expect` would be dead.
2. **Java does not swallow here either.** `MockProducer.Completion.complete`
   (`MockProducer.java:567-581`) has no try/catch, so a Java `AssertionError` thrown by a
   mock send callback also propagates out of `errorNext`. The place in the Java producer
   that *does* swallow is `ProducerBatch.completeFutureAndFireCallbacks`
   (`ProducerBatch.java:318-320`, `catch (Exception e) { log.error(..) }`) — a different
   class, and a plausible source of the intuition.

**Expected.** Drop the false clause and keep the true justification, which is sufficient
on its own: capturing outside the closure is what makes *"the callback never fired"* a
failure rather than a silent pass (Java's inline `assertNotNull(md)` at
`MockProducerTest.java:727` cannot detect that either, so the Rust form is strictly
stronger — that is the point worth stating).

**Actual.** A false premise ships as the stated reason for a correct design choice.

---

## Issue 3: the published C callback contract "(the other argument is null)" is now false for the mock's error path

- **File**: `src/ffi/producer.rs:1140-1143` (ships as
  `target/include/confluent_kafka.h:2416-2418`)
- **Severity**: Behavior Mismatch (doc vs code, in a generated public C header)
- **Java Reference**: `MockProducer.java:578`, and `KafkaProducer.java:1060-1061`
  vs `ProducerBatch.java:315`

**Description.** `kafka_producer_Producer_send_async`'s documented contract is:

> `callback` is invoked … with a non-null `kafka_producer_RecordMetadata_t` on success or
> a non-null `kafka_common_KafkaError_t` on failure **(the other argument is null)**. The
> caller owns whichever handle is non-null and must free it with the matching `*_destroy`.

Phase 7 changed `Completion::complete` so the error path passes
`Some(&null_metadata)` (`mock_producer.rs:341-344`) — which is **correct**: it is exactly
Java 578, and it matches the real producer's `handle_api_exception`
(`kafka_producer.rs:1155-1156` ≡ `KafkaProducer.java:1060-1061`). But
`make_record_callback` (`src/ffi/producer.rs:463-484`) boxes whichever halves are
`Some`, so a C caller of `send_async` against a `MockProducer` now receives **both**
pointers non-null on the error path, contradicting the sentence above.

The sentence was already false for one path before this phase — `KafkaProducer`'s
`ApiException` path, reachable through the same `send_async`. Phase 7 adds the second.
What makes it insidious rather than academic is that it is *true* on the most common
path: `ProducerBatch::complete_future_and_fire_callbacks`
(`producer_batch.rs:432`) passes `callback(None, exception)`, faithfully mirroring
`ProducerBatch.java:315`. So Java itself is inconsistent across its three callback sites,
the Rust mirrors each one faithfully, and only the C prose claims a uniformity that never
existed.

Consequence for a C caller written strictly to the contract — `if (error) { …
destroy(error); } else { destroy(metadata); }` — is a leaked `RecordMetadata_t` on every
failed record. Our own harness does not catch it: `on_record` in
`bindings/c/tests/test_mock_producer.c:68-88` frees the two handles under independent
`if`s, and no C test drives an async mock send into `error_next` (all
`test_send_async_*` use `auto_complete=true`), which is what the commit-1 message
correctly observed about *assertions* — but the observation was not carried through to
the documented contract.

**Expected.** The doc is what should change, not the code. Something like: on failure
`error` is non-null and `metadata` may also be non-null, carrying `-1` in every unknown
field (Java's `RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)`); the caller owns and
must free **every** non-null handle. Worth stating that the three producer paths differ
and that this mirrors Java. A one-line addition to §10.10 deviation 1 recording that the
mock's error path now matches `KafkaProducer`'s on the C surface would close it.

**Actual.** A generated public C header states a mutual exclusivity that two of three
callback paths violate, one of them newly.

---

## Issue 4: `ConsumerGroupMetadata.java:41` is off by one, in the phase's load-bearing skip justification

- **File**: `src/producer/mock_producer.rs:2491`,
  `design/history/Milestone-11/PLAN.md:547` (§Phase-7),
  `design/history/Milestone-11/PLAN.md:3172` (§10.10 deviation 4)
- **Severity**: Missing Requirement (wrong citation supporting the not-applicable ruling
  on the 44th test)
- **Java Reference**: `ConsumerGroupMetadata.java:42`

**Description.** The ruling that
`shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction` is not applicable
*and carries no missing surface* rests on where the `NullPointerException` is raised. The
substance is right, and I verified it independently: with
`Collections.emptyMap()` and a producer that is open, unfenced, initialized and
in-flight, every guard in `sendOffsetsToTransaction` passes and the method would
`return` at `MockProducer.java:195`, so no other throw is reachable — the NPE can only
come from evaluating `new ConsumerGroupMetadata(null)` as an argument, before the mock is
entered. The three artifacts attribute the throw to `ConsumerGroupMetadata.java:41`.

`:41` is `Optional<String> groupInstanceId) {` — the last parameter line. The quoted
statement `Objects.requireNonNull(groupId, "group.id can't be null")` is at **`:42`**
(`grep -n "group.id can't be null"` gives exactly one hit, 42). The constructor actually
invoked by `new ConsumerGroupMetadata(null)` is the one-arg one at **`:52`**, which
delegates to the four-arg one declared at `:38`.

**Expected.** `ConsumerGroupMetadata.java:42` for the `requireNonNull`; optionally
`:52 → :38-42` to show the delegation, since the test calls the one-arg form.

**Actual.** All three sites cite `:41`, which contains neither the entered constructor
nor the quoted statement.

**Why this is filed when I have twice declined to file ±1-3 citation slop.** My standing
bar (recorded in Phase 5b and re-affirmed in Phase 6) is for `(Java A-B)` *range* headers
in "Translated from" rustdocs: a range still brackets its own method, so slop is a
locator imprecision. This is not a range — it is a single line cited as the location of a
statement that is quoted verbatim beside it, and it is the stated evidence for the
phase's one substantive test-skip. I have not moved the bar; this is a different shape of
claim. (For the record, the 53 range headers in this file are all **exact** — see below.)

---

## Checked, and deliberately not filed

Recorded so the next pass does not re-derive them.

1. **Every shipped derivation reproduces, byte for byte.** Extracted from the file (with
   guards: non-empty, balanced braces, `bash -n` / `compile()` parses, required tokens
   present) rather than retyped. Method accounting → `40 declared, 30 present, 10 absent`
   with the same ten names and lines. Test accounting → `rows=55 atTest=55`,
   `TRANSLATED=53`, `NOT_APPLICABLE=2`, and **both** "want empty" checks empty.
2. **All three extraction guards behave exactly as documented.** Deleting
   `w && /^    @/{next}` keeps `rows=55` and changes exactly one row (`689
   shouldThrowClassCastException` → `688 SuppressWarnings`). Relaxing the anchor to
   `^ *//` makes exactly the two NOT APPLICABLE names score in both columns and nothing
   else. And the self-falsified claim is honest: the prefix-nesting probe prints `[]`, and
   dropping the closing backtick changes no row's count — the block says the backtick
   buys nothing today and ships the check that demonstrates it.
3. **Ground truth re-derived independently, not just re-run.** All 4-space method
   declarations in `MockProducerTest.java` = 58; minus `buildMockProducer`, `cleanup`
   (`@AfterEach`) and `isError` = 55, and the name set is *identical* to the shipped TSV.
   `MockProducer.java`'s distinct class-level method names, enumerated by hand = 40.
   Every one of the 55 shipped line numbers lands on a `void <name>(` declaration line.
4. **Both ends of every convention this file imposes.** Wrap-tolerant sweep (joining runs
   of `///`): 53 of 53 headers carry a `(Java N)`, **all 53 numbers equal the
   declaration line**, no header names a non-`@Test` method, and the only two `@Test`
   names without a header are the two NOT APPLICABLE entries. Separately, all 20
   `MockProducerInner` field citations and all 23 `Corresponds to Java's … (\`MockProducer.java:NN\`)`
   citations resolve to the right Java declaration. This is the field-by-field check that
   found 1-of-55 in Phase 6; here it is clean.
5. **The "10 absent block zero tests" claim is not what the artifacts say**, and what they
   do say is true. The block claims **nine** of the ten appear zero times in
   `MockProducerTest.java` (verified: nine zeros), and calls out `partition` separately as
   the reason `testPartitioner` is adapted and `shouldThrowClassCastException` is not
   applicable. PLAN §Phase-7's sentence attaches "blocking zero of the 55" to the
   telemetry/metrics group, not to all ten. `producer_trait.rs` declares none of
   `client_instance_id` / `metrics` / `register_metric_for_subscription` /
   `unregister_metric_from_subscription`, so §9.23's "the gap is in the trait" is right.
6. **The two `Completion::complete` fixes are both correct against Java 567-581**, and
   the `done()`-last one is genuinely load-bearing rather than cosmetic:
   `ProduceRequestResult::set` deliberately does not notify and `done()` publishes through
   the watch channel (`produce_request_result.rs:122-148`), and `FutureRecordMetadata::is_done`
   reads `result.completed()` → `*self.rx.borrow()` — so a concurrent waiter really could
   have observed completion before the callback returned. The `let _ = topic_partition;`
   the Actor cites as the tell was real (removed in the diff). No existing test's semantics
   changed except the four strengthenings of Issue 1.
7. **Staging fidelity.** `mem::take` + `push` is exactly Java's
   `consumerGroupOffsets.add(field)` at 218 followed by `field = new HashMap<>()` at 221,
   and abort's `clear()` at 241-242 is faithful because nothing retains those collections
   — pinned by
   `should_preserve_offsets_from_commit_by_group_metadata_on_abort_if_transactions_are_enabled`
   (Java 583), which fails if commit `clear()`s the published map. `clear()` now matches
   Java 490-497 line for line, in order, and still does **not** reset `offsets` (Java does
   not either — this was a real bug once, and the guard against its return is
   `test_clear_preserves_offsets`). `ConsumerGroupOffsets` is a `type` alias, so DoD §7 is
   satisfied.
8. **The reentrancy refactor is clean as posed, with one residual worth a sentence.** No
   public-surface change; every method locks at most once; the four moved
   `MockProducerInner` methods never lock. What §10.10 deviation 2 does not say is that a
   residual divergence remains: the user callback fired from `Completion::complete` runs
   **with the guard held**, so a callback that calls back into the mock
   (`producer.flushed()`, `producer.history()`) deadlocks where Java's reentrant monitor
   allows it. I am not filing this — the hazard predates Phase 7 (both `send_with_callback`
   under `auto_complete` and `error_next` already fired callbacks under the guard), no Java
   test exercises it, and the obvious "fix" (release before the callback) would trade
   Java's atomicity for it, which is worse. But deviation 2's reason as written ("no
   `.await`", CLAUDE.md §9.6.2) answers a different hazard than the one the deviation is
   about, and one sentence naming the residual would make the record complete.
9. **`shouldThrowClassCastException`'s standing justification holds, including the new
   half.** Rust generics are monomorphised, so `MockProducer<i32, String>` cannot accept a
   `ProducerRecord<String, String>`; and the addition is correct — the Rust mock holds no
   serializer fields at all, so there is nothing to mis-apply even in principle.
10. **The exception-collapse deviation is adjudicated in the Actor's favour.** Java's
    fenced `send` (`:293-295`) throws `KafkaException(msg, ProducerFencedException("Fenced"))`
    while `verifyNotFenced` (`:256`) throws the bare exception; the single flattened
    `KafkaError` keeps both halves `shouldThrowOnSendIfProducerGotFenced` actually asserts
    (the cause's *type* → `Errors::ProducerFenced`, and the wrapper's *message* →
    `"MockProducer is fenced."`), and `assert_producer_fenced` checks both. §10.10
    deviation 1 even records the one thing that is lost (`is_api_exception()` now reports
    `true` where Java's outer bare `KafkaException` is not an `ApiException`) — which is the
    right level of honesty about a flattening.
11. **Message fidelity.** All six Java misuse strings appear verbatim
    (`grep -F` against `MockProducer.java`): `"MockProducer is already closed."`,
    `"MockProducer is fenced."`, `"MockProducer hasn't been initialized for transactions."`,
    `"There is no open transaction."`,
    `"MockProducer has already been initialized for transactions."`,
    `"Transaction already started"` (no period, as Java writes it). Guard order matches Java
    in all five transactional methods, including `beginTransaction`'s knob-before-in-flight
    ordering (Java 167-173) and `sendOffsetsToTransaction`'s knob-before-empty-map
    (`:190-196`). Note the *test* does not pin either ordering — at the point
    `test_set_transactional_errors` installs the begin knob no transaction is in flight, so
    both orderings return the same error. Java pins neither (no Java test touches the
    knobs), so this is not a parity gap; the rustdoc's sentence describes the production
    placement, which is correct.
12. **The 12 mutations are plausible and the three I traced hold.** `putAll` → insert-if-absent
    is caught by `should_publish_latest_and_cumulative_…` (partition 1 must move 73 → 101);
    `flush` gaining `verifyNotFenced` by `should_not_throw_on_flush_producer_if_producer_is_fenced`;
    and the dropped commit-time `isEmpty()` guard by
    `should_drop_consumer_group_offsets_on_abort_…` (Java 529), whose post-abort
    `begin + commit` would otherwise publish an empty map — which is the pairing the commit
    message corrects itself on. The strengthened assertion I spot-checked,
    `test_metadata_on_exception`, is what catches mutation 8 (the no-metadata error callback)
    via the `expect` inside the closure.
13. **`2451` vs `2449` lib tests across two commit messages is not a discrepancy.**
    `cargo test --lib` reports `2449 passed; 2 ignored` — `298a430` counted the 2451 run,
    `fb3da54` the 2449 passed. Both true, different conventions. (I nearly filed this;
    check the ignored count before treating an apparent regression in a comment-only commit
    as one.) Likewise "clear gains the four collections" in `0d52292` counts four *added
    lines*, three of which are collections and one a flag — the durable in-file text
    ("the four collections and the flag … beyond `sent`") is exactly right.
14. **DoD, eleven clauses.** §1 rules-consistent; §2 covered by the method accounting;
    §3 by the test accounting plus the message assertions; §4 no blockers; §5 `cargo test --lib`
    = 2449 passed / 0 failed, `producer::mock_producer` = 69 passed; §6 no duplication; §7 the
    only new type is an alias, disclosed; §8 no TODO/FIXME; §9 `lint` and `format-check` clean
    (I did not re-run Docker-dependent suites — the Actor reports `make verify-sandbox` exit 0
    per commit); §10 skippable, the mock is not on the send path; §11 consumer-only, N/A.
15. **§9.14 correctly not re-filed** — no new wire type, and this phase writes no bytes.
16. **Leaving the pre-existing broken intra-doc links is right for this phase.** The brief
    put the count at 20; the measured figure today is higher —
    `cargo doc --no-deps` emits **43** rustdoc errors before the `could not document`
    summary, at 43 sites across `consumer/`, `producer/`, `common/network/`,
    `common/protocol/`, `common/requests/`, `fetch_session_handler.rs`,
    `kafka_client.rs` and one generated file (mix of unresolved links, private-item
    links and redundant explicit link targets). Exactly one is in this file —
    `mock_producer.rs:365`, *"public documentation for `new` links to private item
    `FutureRecordMetadata`"* — and it is present verbatim at `82aa2da`, so the phase
    introduced none. `cargo doc` is in neither CLAUDE.md's Development Workflow nor
    `make verify` (`build format-check lint test`), so no DoD clause is in play.
    One observation rather than a finding: this project-wide gap is disclosed only in the
    Actor's private `phase1_design_notes.md` ("treat the doc gate as *doesn't make it
    worse*"), whereas its sibling — §9.14, also project-wide, also found mid-phase — got a
    numbered PLAN §9 entry. If the team wants the doc gate tracked, §9 is where a reviewer
    would look for it.

---

## Suggested rule / process updates

None to `CLAUDE.md` or the rules files. `producer-transactions.md` did not govern this
phase (no `Caller`, no lock topology, no `TransactionalRequestResult`, no wire
encoding), and nothing in Phase 7 falsified one of its premises — the first M11 phase
where that is true.

`COMMENTS.FP.md` and `COMMENTS.FN.md`: `COMMENTS.FP.md` exists and contains no entry for
Critic 47 or Phase 7; `COMMENTS.FN.md` does not exist in the tree. Nothing to fold in.

---

# Pass 2 (clean — closes Phase 7, 2026-08-06)

# Critic 47 — Milestone 11 Phase 7, pass 2

Range reviewed: `bd7975f..bd19e89` (`fixup!` addressing all four pass-1 findings).

# No findings

All four pass-1 findings are correctly fixed, both disclosed near-misses are actioned,
and nothing in the fix commit introduced a new defect. **Phase 7 closes.** Loop: 4 → 0.

The Actor's refinement of my finding 1 is **upheld and reproduced** — and it corrects a
supporting sentence of mine, not just an omission of its own. Detail below.

---

## Per-fix verification

**Issue 1 — the weakened set, by membership.** All four are now named in all three
artifacts, with the same members and the same Java lines:

| Artifact | Members named |
|---|---|
| `mock_producer.rs:2531-2550` | `testManualCompletion` (107), `testMetadataOnException` (724), `shouldThrowOnSendIfProducerIsClosed` (624), `shouldThrowOnFlushProducerIfProducerIsClosed` (673) |
| PLAN §Phase-7 (`:560-570`) | the same four, same lines |
| `phase7_mock_producer_notes.md` | `testManualCompletion`, `testMetadataOnException`, "both closed-producer tests" |

The memory note identifies the pair collectively rather than by name; unambiguous in
context (the set has exactly two closed-producer members), and it additionally records
*which* artifact dropped *which* — the part that makes the lesson usable. All four cited
lines are declaration lines; `122` (`assertEquals(e, err.getCause())`) and `727-731`
(the callback body) both check out.

**The two-swaps refinement is right, and my pass-1 supporting sentence was wrong.** I
wrote that "every other change in the module is the mechanical `with_auto_complete` →
`build_mock_producer` swap". There were two swaps, and re-running the normalisation at
all three stages confirms the Actor's numbers exactly:

    no normalisation                  → 19 of 23 bodies differ
    constructor swap only             →  6
    constructor + make_record→recordN →  4   ← the set, and only the set

The sixth body — the one I could not have guessed from my own output — is
`should_be_flushed_after_flush`, which this phase *renamed* to Java's
`shouldNotBeFlushedAfterFlush` and whose body also took the fixture swap; a body-diff
keyed on the function name reports it as missing rather than changed, so it needs a
rename mapping to be seen at all. My table was right because I classified
`test_auto_complete_mock`'s fixture swap as mechanical by eye — but the prose I wrote
around the table named only one swap, so a reader following my sentence would have
reported six. Third time this milestone the Actor has corrected a supporting fact of
mine; correctly, and the fixed text carries both swaps explicitly.

**Issue 2 — the rustdoc's new reason.** Verified against Java, and it is the right
reason:

- Java asserts inline in the callback body (`MockProducerTest.java:727-731`) ✓.
- The hole is real. `assertTrue(producer.errorNext(e))` at `:734` proves a `Completion`
  was popped, not that the callback ran; reintroduce a "callback never fired" bug and
  Java's four assertions simply never execute, `errorNext` still returns `true`, and
  `metadata.get()` still throws the injected error — the Java test passes. Capturing
  outside and asserting after `error_next` returns is what turns that into a failure,
  so "strictly stronger" is accurate, and `assertNotNull(md)` at `:727` indeed does not
  close it (it is inside the same closure).
- The propagation claim is correctly **scoped to the path**: `error_next` →
  `Completion::complete` → `cb(..)` is synchronous with no `catch_unwind`. Verified by
  reading the path, not just by grep — and the closure's own `expect` does depend on it.
- `ProducerBatch.java:318-320` is exactly `} catch (Exception e) { log.error(..); }`, and
  `MockProducer.Completion.complete` (567-581) has no try/catch ✓. Naming the class that
  *does* swallow is the useful half; it is almost certainly where the original intuition
  came from.

On the coordinator's phrasing — "no `catch_unwind` exists outside `producer_batch.rs`'s
test module" — that is not what holds, and it is not what the shipped rustdoc claims. See
disclosure 2 below.

**Issue 3 — the C contract.** The doc now states the per-path truth, and each of the
three equivalences the Actor asserts is exact:

| Rust | Java | Metadata on error |
|---|---|---|
| `producer_batch.rs:433` `callback(None, exception.as_ref())` | `ProducerBatch.java:315` `onCompletion(null, exception)` | null |
| `kafka_producer.rs:1156` `cb(Some(&null_metadata), Some(&error))` | `KafkaProducer.java:1060-1061` | non-null, −1-filled |
| `mock_producer.rs:343` `cb(Some(&null_metadata), Some(&e))` | `MockProducer.java:578` | non-null, −1-filled |

`grep "other argument is null"` is 0 in `src/` and 0 in the generated header. The
replacement tells C callers to test the handles independently rather than in an
`if`/`else`, which is the operative instruction — `bindings/c/tests/test_mock_producer.c`'s
`on_record` already does exactly that, so the harness matches the new contract.
`send_batch_async`'s cross-reference widening is right and for the right reason: both
paths are built by the same `make_record_callback`. §10.10 deviation 1's new paragraph
records the whole thing, including that Java's own three sites disagree.

**Committing the Rust doc alone is the correct call.** Confirmed rather than assumed:

- No `.h` is tracked anywhere (`git ls-files | grep '\.h$'`, minus vendored unity → empty).
- `build.rs:103-121` generates `target/include/confluent_kafka.h` with cbindgen under
  `#[cfg(feature = "ffi")]`, and `build.rs:106` emits `cargo:rerun-if-changed=src/`, so
  any change under `src/` regenerates it — this is systematic, not incidental. (Worth
  noting because emitting *any* `rerun-if-changed` disables cargo's default
  re-run-on-any-change; the `src/` entry is what keeps the header from going stale.)
- `bindings/c/CMakeLists.txt:11` points every C test at `${RUST_PROJECT_ROOT}/target/include`.

So the Rust doc comment is the single source of truth and the header is a pure build
artifact; hand-editing it would be overwritten on the next `cargo build --features ffi`.
Verified regenerated in my tree: the new prose is present, `:2418` and `:2473` both land
inside the two new blocks.

**Issue 4 — the citation.** `:41` → the self-checking chain `:52` (one-arg constructor the
test calls) → `:38` (four-arg declaration) → `:42` (`Objects.requireNonNull(groupId,
"group.id can't be null")`, one `grep -n` hit). All three sites corrected; zero `:41`
survivors in `src/`, `design/` or the Actor's memory. The only remaining occurrence
tree-wide is inside *my own* `review_m11_phase7.md`, where the wrong citation is the
subject of the note — correct to leave.

**§10.10 deviation 2 matches what I disclosed, and improves on it.** It now separates the
async-runtime hazard from the reentrancy hazard, states that only the second is why the
body moved, keeps the `.await` fact on its own footing, and names the residual in the
terms I used: the user callback fired from `Completion::complete` runs with the guard
held, so re-entering the mock from a callback deadlocks where Java's reentrant monitor
would allow it — predating Phase 7, untested by Java, and deliberately not "fixed" by
releasing the guard, which would trade away the atomicity Java's `synchronized` provides.
That is now the durable statement of a known behavioural divergence, which is what I
wanted from it. The added claim "no call sequence that worked before can deadlock now"
also holds: every public method locks exactly once and the four moved inner methods never
lock.

## Re-verification of what the fix could have broken

The fix rewrote prose inside the accounting block, which is the block's own blind spot,
so I re-ran the machine-checkable parts against the *new* file rather than trusting the
pass-1 results:

- Test-accounting derivation re-extracted from the current text (guards: parses under
  `bash -n`, required tokens present) → `rows=55 atTest=55`, `TRANSLATED=53`,
  `NOT_APPLICABLE=2`, both "want empty" checks empty. The new prose names the four tests
  *without* the `MockProducerTest.` prefix, so it is invisible to the classifier — which
  is what keeps 53/2 intact. Confirmed empirically, not by inspection.
- Guard 1 re-run: deleting the intervening-annotation skip still changes exactly row 50
  (`689 shouldThrowClassCastException` → `688 SuppressWarnings`).
- Wrap-tolerant header sweep: **53** headers, **0** line-number mismatches, and the only
  two unheadered `@Test` names are still the two NOT APPLICABLE entries.
- All 20 `MockProducerInner` field citations and all 23 `Corresponds to Java's …
  (\`MockProducer.java:NN\`)` citations still resolve to the right Java declaration.
- `cargo test --lib` → 2449 passed, 0 failed, 2 ignored; `cargo xtask format-check` and
  `cargo xtask lint` both clean.

## Checked, and deliberately not filed

1. **"Six bodies differ before that normalisation" (`mock_producer.rs:2540-2541`) reads as
   *before both swaps*, where the figure is 19.** Six is what a *one*-swap normalisation
   gives; the commit message says it precisely ("a first pass reported six bodies"), and
   the in-file sentence compresses that to "before". I am not filing it, on the same bar I
   used in pass 1 to decline `0d52292`'s "four collections": the block's machine-checkable
   contract is its two embedded programs plus their pasted outputs, and both reproduce
   exactly; this paragraph narrates a one-off diff, its load-bearing content (the four
   names) is complete and correct in all three artifacts, and no other artifact
   cross-references the six. Filing an imprecise antecedent would be applying a threshold
   here that I have not applied to range-header slop or to the earlier commit-message
   miscount. Recording all three numbers instead, so the next reader can reproduce any of
   them: **19** un-normalised, **6** after the constructor swap alone, **4** after both.
   If the sentence is ever touched for another reason, "before completing that
   normalisation" is the one-clause repair.
2. **The commit message's "the only `catch_unwind` in `producer/` is
   `producer_batch.rs:909`" is off by one; the shipped rustdoc is not.** `src/producer/`
   has two: that one (inside the `#[cfg(test)]` module starting at `:845`) and
   `built_in_partitioner.rs:638`, which is also test-only — a `#[test] fn
   test_sticky_batch_size_more_than_zero` asserting a constructor panic. Neither is on the
   mock's callback path, so the substance ("all test-only, none on this path") stands, and
   the durable rustdoc claims only "no `catch_unwind`" *on that path*, which is exactly
   right. Commit-message-only, and correctly scoped where it matters. For the record the
   crate has non-test `catch_unwind` sites elsewhere (`network_client.rs:844`,
   `consumer_interceptors.rs` ×3, `deserializers.rs` ×2, `abstract_fetch.rs`), so a
   crate-wide "no `catch_unwind`" claim would have been wrong — scoping it to the path is
   what makes it true.
3. **"dropping all four values Java checks at 727-731"** — the four `assertEquals` are at
   728-731 and `:727` is the `assertNotNull`. The range is a superset, not a wrong
   citation, and the same paragraph cites `:727` exactly for the not-null assertion. No
   defect.
4. **The 2451-vs-2449 delta** is agreed as run-vs-passed (2 ignored); no artifact changed,
   which is right — both numbers were true under their own convention.
5. **Everything I upheld in pass 1 still holds.** I did not re-audit the 22 production
   entry points, since the fix commit touches no production logic — only doc comments in
   `mock_producer.rs` and `ffi/producer.rs`, plus PLAN and memory. `git diff` confirms no
   executable line changed.

## Suggested rule / process updates

None. `COMMENTS.FP.md` still carries only the Critic 41 entry — nothing for Critic 47 or
Phase 7 — and `COMMENTS.FN.md` does not exist in the tree.

One observation for the process rather than a rule change: this phase's single most
productive check was a **membership** diff where every artifact agreed on a **count**.
Two lists that both say "three" are invisible to every count-level sweep, including the
ones this milestone has built up. `.claude/rules/definition-of-done.md` §3 asks whether
all tests are translated; it does not ask whether a *present-but-weakened* test is named.
Both the Actor's memory note and my own now carry the lesson, which is probably the right
place for it — but if a future phase wants it enforced, "record such a set by membership,
never by count" is the sentence.
