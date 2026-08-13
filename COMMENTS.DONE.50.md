# Critic 50 — resolved findings (review of `ee1a72ec..9acbd6a3`)

All four findings were real and none was disputed. The Critic found **no behavioural
defect**: the central claim of `a02e9a7f` ("Java's `build()` is idempotent, so
`records()` must be `build()`") was verified link by link and holds. Every fix below is
documentation or test-completeness.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 1 | `mock_client.rs`'s conditional-build comment now names `ProduceRequestBuilder::build_version`'s `std::mem::replace` (`produce_request.rs:300`) as the cause and drops the §9.18 citation, which was both the wrong object and — after `a02e9a7f` — a resolved section. The cost argument is kept. The underlying divergence is filed as **PLAN §9.30** with a reproducer. | `a02e9a7f` | reproducer `produce_request::tests::test_build_is_repeatable` fails on exactly the claimed assertion: first build carries 1 topic, second carries 0 |
| 2 | The accounting block's arithmetic is now **derived** rather than restated: a shipped `awk` program prints per-group placements, and the block carries its real output. 57 placements over 55 distinct entries, the gap being two names each placed in one group and cited in another. A loop-50 clause is added to the change log, and the "the total is the check" claim is retracted — a re-listing now offsets a move, so only the decomposition checks. | `9f47980a` | derivation re-run after `cargo xtask format`: 53 in scope, 55 entries, `comm -23` empty, `comm -13` = the two documented out-of-scope names, 57/55 decomposition unchanged |
| 3 | The three `destination()` → `Node` → `isReady` triples (Java 2441-2447, 2462-2466, 2479-2483) are translated, via a helper mirroring Java's thrice-repeated block. The doc's "two deviations" claim is corrected and now states why these were never a forced omission. | `9f47980a` | probed for vacuity: perturbing the derived node id by +7 fails both tests on `Client ready status should be true`; reverted, both pass |
| 4 | `design/current/status.md`'s ignored-test list is corrected — `test_too_large_batches_are_safely_removed` is out, §9.30's reproducer is in — and `PLAN.md`'s present-tense "the same treatment §9.18 gives its own" is rewritten. | `9f47980a` | `cargo test --lib` reports `3 ignored`, matching the list name for name |

Two further corrections were made that the review did not ask for, both found by
re-running the block's own sweeps rather than by reading:

  - **Loop 50's own Java range citations were wrong.** Its three new
    ``Translated from `SenderTest.<name>` `` headers cited the `@Test` **annotation**
    line as the range start (2371 / 2384 / 3604) where the shipped convention is the
    **declaration** line (2372 / 2385 / 3605), and the driver header stopped at the
    `try`-block brace rather than the method's (2496 → 2497). The entry citations in
    the accounting block had the right numbers all along, so the file disagreed with
    itself. Fixed; the both-ends sweep now reports 105 headers, 0 mismatches.
  - **Both sweep counts in the block were stale.** "55 pairs" is now 60 (occurrences,
    not distinct names — loop 50 re-cites some entries) and "102 headers" is now 105.
    Both re-run, both stated with what moved them.

And one strengthening the Manager flagged from the Critic's "Examined" section, now
recorded at `memory_records_builder.rs`'s `build()` and in PLAN §9.18: Java has no
`closed` field — its `isClosed()` **is** `builtRecords != null`
(`MemoryRecordsBuilder.java:885-887`). `take_built_records` broke that correspondence
by leaving `closed == true` with `built_records == None`, which is precisely why the
`built_size` shadow field had to exist. With the accessor gone,
`closed ⇔ built_records.is_some()` holds at every point, so the collapse of
`estimated_size_in_bytes()` to Java's two-arm form is a consequence of the invariant
rather than a coincidence.

**Nothing was rejected**, so `COMMENTS.FP.md` is unchanged.

Note on scope, following the `COMMENTS.DONE.49.md` precedent: the Critic's two
**rules-change suggestions** are deliberately NOT applied and remain in
`COMMENTS.50.md`. `CLAUDE.md` and the files under `.claude/rules/` are to be avoided by
automatic agents; those are for the human to accept or reject. The Actor's position on
each is recorded there.

---

## The open question the Manager attached to Issue 1

**Is `std::mem::replace(&mut self.data, ProduceRequestData::new())` at
`produce_request.rs:300` itself a defect?**

**Answer: it is a real divergence from the Java contract, but a latent one — not
reachable in production today.** Filed as **PLAN §9.30**, not fixed here, per the
scope instruction. The derivation:

  - **Java does not do this.** `ProduceRequest.Builder.build(short version)`
    (`ProduceRequest.java:68-74`) validates and then returns
    `new ProduceRequest(data, version)`, sharing the reference. The builder is
    unchanged and may be built any number of times. (Java *does* null out a
    `ProduceRequest`'s `data` — `clearPartitionRecords`, `:94-97` — but that is on the
    **request**, server-side, after the response is queued. Different object, different
    lifecycle.)
  - **It is the only one of 53.** Every other `RequestBuilder` impl in this crate
    clones its data (e.g. `sasl_authenticate_request.rs:142`). One command shows it:

        for f in $(grep -rln "fn build_version" src/); do \
          awk '/fn build_version/,/^    }/' "$f" | grep -q "mem::replace\|\.take()" && echo "$f"; \
        done
        # -> src/common/requests/produce_request.rs, and nothing else

    An outlier of 1-in-53 is the strongest available evidence that it was not a
    considered design choice.
  - **Same shape as PLAN §9.12 defect 2**, which was ruled a real defect and fixed:
    there, `write` drained a `records` field so *serialising mutated the message*;
    here, `build` drains `data` so *building mutates the builder*. Both were justified
    as zero-copy ownership transfer and neither needs to be —
    `ProduceRequestData` is `Clone` and its `records` are `bytes::Bytes`, whose clone
    is a refcount bump.
  - **Why it is not live.** `network_client.rs:502` is the only production caller of
    `build_version`; it builds once per `ClientRequest`, and `do_send_with_request`
    consumes the request immediately after. Retries do not rebuild — the `Sender`
    constructs a fresh `ProduceRequestData` per send, the same property §9.12 relied
    on. The three test-side callers are each reachable at most once per request:
    `MockClient::send` builds only for a matched *future* response, after which the
    request never enters `self.requests`; `respond_with_matcher` builds only what is in
    `self.requests`; and `send_idempotent_producer_response` answers with `respond`,
    which does not build.
  - **But it has already distorted the port**, which is why it is worth a section
    rather than a shrug. `MockClient::send` builds *conditionally* where
    `MockClient.java:259` builds unconditionally, and that deviation exists solely to
    keep a second build out of reach of `respond_with_matcher`. Anyone restoring Java's
    shape there breaks the matcher path — silently, with empty `topic_data`.

**Reproducer:** `produce_request.rs`'s `test_build_is_repeatable`, `#[ignore]`d with
§9.30 cited, following the treatment §9.18 and §9.25 gave theirs. It asserts Java's
contract — two builds, both carrying the topic — and fails today with `left: 0,
right: 1` on the second.

**Fix direction (in §9.30, not attempted):** `self.data.clone()`, matching the other
52. That puts a per-request clone on the send path — a `Vec` spine plus one `String`
per topic, no record bytes — so it needs its own DoD §10 measurement rather than an
argument, which is exactly why it is filed rather than folded in here.

---

## Original review, preserved

See `COMMENTS.50.md` for the retained rules-change proposal; the four findings and the
Critic's "Examined, judged not findings" derivation are reproduced below unchanged.

---

# Critic 50 — review of `ee1a72ec..HEAD`

Range: `a02e9a7f`, `9f47980a`, `9f5b46bb`, `9acbd6a3` (PLAN §9.18, the
split-on-`MESSAGE_TOO_LARGE` panic).

**Verdict on the central claim: it holds.** Every link in the "Java's `build()`
is idempotent, so `records()` must be `build()`" chain was checked against
`kafka/` 4.2 and against this repo's history; all of them check out, and the
change makes the Rust semantics *closer* to Java than a local patch would have.

No behavioural defect found. Four findings, all documentation/test-completeness,
none blocking.

Local verification: `cargo test --lib` → 3520 passed, 0 failed, 2 ignored;
`cargo xtask format-check` clean; `cargo xtask lint` clean. The two split tests
run 6× at `--test-threads=8` without a failure. The shipped `SenderTest`
accounting derivation reproduces exactly (53 in scope, 55 entries, `comm -23`
empty, `comm -13` = the two documented out-of-scope entries).

## Issue 1 — RESOLVED

`mock_client.rs:673-685` blamed PLAN §9.18 for a workaround whose real cause is
`ProduceRequestBuilder::build_version`'s `mem::replace` at `produce_request.rs:300`.
Two things wrong: the move described was not §9.18's (they are independent — the
`mem::replace` alone drains the builder), and §9.18 is now marked FIXED, so a reader
following the citation lands on a resolved section and can reasonably conclude the
conditional build is obsolete. It is not.

## Issue 2 — RESOLVED

The `SenderTest` accounting block's arithmetic decomposition ("33 + 3 + 16 + 3
blocked = 55") and its change-log sentence ("Phase 8 changed no group's membership
… which is itself the check") both describe the pre-loop-50 shape. The total is
still right, but only by cancellation, which is the failure mode the block itself
was rewritten to prevent.

## Issue 3 — RESOLVED

`drive_split_batch_and_send` drops Java's three `destination()` → `Node` → `isReady`
triples while its doc claims exactly two deviations, and the omission is not forced:
`MockClient::is_ready` exists and the idiom is already used four times in the file.

## Issue 4 — RESOLVED

`design/current/status.md:56-70` still lists `test_too_large_batches_are_safely_removed`
as an open-defect reproducer, and `PLAN.md:2473` says §9.25's reproducer gets "the same
treatment §9.18 gives its own" in the present tense.
