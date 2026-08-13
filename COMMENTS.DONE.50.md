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

# Critic 50 pass 2 — resolved findings (review of `9acbd6a3..599f05e4`)

Two findings, both low, both real, neither behavioural, neither disputed.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 5 | The header-range regression gate now iterates `now` (every header in the working tree) instead of `cited` (the header set as it stood at `PREFIX`), so headers written after `PREFIX` are checked. Its result and the entry-citation sweep's are both **printed by the program**, so all four of the block's sweeps are shipped as code rather than stated. The generalisation is written down beside them. | `9f47980a` | reproduced the blind spot (`cited` 101, `now` 105, the 4 uncovered are exactly the 4 that were wrong); then proved the fixed gate bites — reintroducing `2371-2382` fails with `AssertionError: ranges wrong: [('SenderTest', 'testIdempotentSplitBatchAndSend')]`, reverted, clean |
| 6 | §9.30's outlier sentence is re-derived: **52** impls, **exactly one drains**. The false universal ("every other clones its data") is removed rather than restated, because any finer split is a judgement call. Same correction in the `mock_client.rs` copy, and the "all 53 impls" in the fix-direction paragraph. | `6f9c0267` | three greps, three defensible answers — 53 files (includes the trait declaration), 51 `impl RequestBuilder for` (misses `fetch_request.rs`'s qualified impl), **52** tolerant of qualification. All three are now in the section with why they differ |

**On Issue 6 I did not take either count on trust, and neither was quite right.** My own
`grep -c "impl RequestBuilder for" src/` gives **51**, agreeing with the Manager and not
with the Critic's 52. The discrepancy resolves in the Critic's favour on the substance:
`fetch_request.rs:517` writes `impl crate::common::requests::RequestBuilder for
FetchRequestBuilder`, which a literal `impl RequestBuilder for` grep cannot see. The
tolerant form `grep -rlE "impl [A-Za-z_:]*RequestBuilder for"` gives 52, and that is the
number now in the section — together with all three counts and the reason each differs,
because a figure two reviewers count differently should be shipped as a command.

The clone/construct sub-split is **deliberately not stated**. The Critic's 48/3 and my
classifier's 47/4 disagree on `offset_fetch_request.rs`, which reaches `self.data.clone()`
one call away inside `maybe_downgrade(&self)` — it does not drain, but it does not read as
a direct clone either. Rather than pick a number, the section now says only what the
command proves: exactly one drains. Restating a universal with a different denominator
would repeat the shape of the finding.

**Also folded in, from the pass-2 "Examined" section** — the two paths §9.30 did not
close, each re-verified here rather than copied:

  - The `Err` arm of the same `match` (`network_client.rs:505-536`) does not rebuild: it
    uses the immutable `request_builder()` for the api-key name and version bound, and
    `make_header`. No branch reaches `build_version` twice.
  - `Display for ClientRequest` (`client_request.rs:147-161`) prints scalars only, so no
    log site and no panic format — including `MockClient::send`'s not-ready `panic!` —
    can trigger a build.

**One clause the Critic raised and declined to file, fixed anyway.** The taxonomy
program's extraction instruction says to strip the `    //     ` prefix; bare `    //`
separator lines survive that strip and Python rejects them with an `IndentationError`.
Loop 50 added two more such separators, so the instruction was getting worse rather than
better. It now says to map them to empty lines.

**Nothing rejected in pass 2 either**, so `COMMENTS.FP.md` remains unchanged.

### The lesson from Issue 5, recorded at the source

A regression gate keyed on a historical snapshot checks the past. The block's assertion
ran over `cited` — the header set at `PREFIX` — so the four headers loop 50 added were
never compared, and they were the four that were wrong. Running the check verbatim would
have passed. **A gate's domain must be the current artifact; only its taxonomy may be
keyed on history.** The corollary is the one the two stale counts demonstrate: a sweep
that is *stated* is a sweep that is not run, so a bare "N, 0 mismatches" in a comment is
an unverified claim however rigorous the prose around it. Both are now fixed at the
source — the assertion iterates `now`, and both counts are program output.

This is the third instance of the class the block already records (Critic 46 issue 3;
Critic 48 issue 9) and the worst, because this block is *self-verifying* and its
self-verification was blind exactly where new work lands.

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

# Critic 50 — pass 2, review of `9acbd6a3..HEAD`

Range: `6f9c0267`, `599f05e4`, `50a6aaf5`.

**All four pass-1 findings are genuinely resolved, not moved.** Verified individually
below. The Actor's two self-found corrections are right, and I confirmed each number.
The reachability claim under PLAN §9.30 — the one I was asked to distrust — is
**established**, not asserted; I re-derived it independently and it holds.

Two new findings, both low, both in the same family: an audit whose denominator is
narrower than its claim. Neither affects behaviour and neither invalidates a
conclusion.

Verification: `cargo test --lib` → 3520 passed, 0 failed, **3 ignored**, and the three
match `design/current/status.md`'s list name for name. `format-check` and `lint` clean.
The `#[ignore]`d §9.30 reproducer fails on the *second*-build assertion with
`left: 0, right: 1`, having passed the first — so it fails for the stated reason and
not on `validate_records`.

---

## Issue 5: the shipped header-range check cannot see the headers loop 50 added — its denominator is 101, the file has 105

- **File**: `src/producer/internals/sender.rs:8397` (the assertion), claim at `:8295-8299`
- **Severity**: Missing Requirement (audit integrity)

**Description.** The accounting block says the rustdoc-header convention is "swept
mechanically rather than asserted", and reports **"105 headers carrying a range …
0 mismatches."** The claim is *true* — I ran a full sweep over every header in the
working tree and got 0 mismatches — but it is **not what the shipped program checks.**

The program's regression assertion is:

```python
assert all(now[(c, n)] == true_range(c, n) for c, n in cited), 'a range regressed'
```

`cited` comes from `git show 8356e80:sender.rs` (`PREFIX` at `:8340`), so the
assertion's domain is the header set as it existed *before* loop 50. I extracted the
program out of the comment, ran it (it reproduces the nine-row table exactly, "9 rows,
10 cells", and the assertion passes), and then probed its own variables:

```
headers in `cited` (from PREFIX 8356e80): 101
headers in `now` (working tree):          105
headers present now but NOT covered by the assertion: 4
    SenderTest testIdempotentSplitBatchAndSend
    SenderTest testNoBufferReuseWhenBatchExpires
    SenderTest testSplitBatchAndSend
    SenderTest testTransactionalSplitBatchAndSend
FULL sweep over all current headers -> mismatches: 0
```

The four headers outside the assertion's domain are **exactly** the four whose ranges
were wrong. The one runnable check in this block could not have caught loop 50's slip,
by construction — not because it was not re-run, but because a header added after
`PREFIX` is not in `cited` and so is never compared.

This is the third instance of the same failure in this block, and the block records the
first two immediately below the defect: Critic 46 issue 3 ("both sides shared the
filter's assumption, so a method the Java program never emits cannot surface as
unplaced") and Critic 48 issue 9 ("before asserting an 'N of M', ask what M excludes").
Here M excludes every header written after `8356e80`.

- **Expected**: the final assertion iterates the *current* header set —
  `for c, n in now` (or `set(now) | set(cited)`) — so every header in the file is
  range-checked on every run. The table generation legitimately stays keyed on `cited`;
  it is a historical taxonomy of the nine. One line, and the "105 headers, 0 mismatches"
  claim then becomes something the program produces rather than something beside it.
- **Actual**: 101 of 105 headers are checked; the 4 excluded were the 4 that were wrong.

**This also answers the Manager's question about my pass 1.** Half of it is my miss and
half is the gap above, and the two are separable:

  - What I ran was the **scope** derivation — the `awk`/`comm` name-set program — which
    is shipped, which I reproduced exactly, and which compares *name sets*. It is
    structurally blind to line numbers; no run of it could ever have surfaced
    2371-vs-2372.
  - The check that does see line numbers is the header sweep. I did not extract and run
    it, which was my error. But had I run it as shipped, it would have passed anyway,
    because of the denominator above.

The generalisable lesson, and the reason I am filing this rather than only confessing:
**"I ran the shipped derivation" is only as strong as which derivations are shipped.**
This block ships two of its four sweeps as code (the scope program, the taxonomy
program) and states the other two as results. The two stated ones — "55 pairs" and
"102 headers" — were *both* stale, which is direct evidence they were not being re-run;
loop 50 corrected them to 60 and 105 but left them stated. A future Critic should treat
a bare "N, 0 mismatches" in a comment as an unverified claim regardless of how rigorous
the surrounding prose is.

---

## Issue 6: PLAN §9.30's outlier claim has an off-by-one denominator and a false universal

- **File**: `design/history/Milestone-11/PLAN.md:2717-2719`; same sentence in
  `src/mock_client.rs:689-691`
- **Severity**: Design Flaw (documentation)

**Description.** §9.30 argues, correctly, that the drain is unintended because it is an
outlier. The supporting sentence is:

> Of this crate's 53 `RequestBuilder` impls, this is the only one that drains; every
> other clones its data (e.g. `sasl_authenticate_request.rs:142`).

Both halves of the framing are slightly wrong; the conclusion is not.

- **53 is a file count, not an impl count.** `grep -c "impl RequestBuilder for" src/`
  → **52**. The 53rd file with a `fn build_version` is
  `src/common/requests/abstract_request.rs:169`, which is the trait *declaration*
  (`fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest>;`), not an
  impl. The shipped survey command greps files (`grep -rln "fn build_version" src/`),
  and the prose relabels that number as impls.
- **"every other clones its data" is false for three.**
  `ElectLeadersRequestBuilder` (`elect_leaders_request.rs:208-213`),
  `FetchRequestBuilder` (`fetch_request.rs:530`) and `LeaveGroupRequestBuilder`
  (`leave_group_request.rs:155`) have **no `data` field at all** — they hold typed
  fields and construct a fresh `*RequestData` at build time. The accurate split of the
  52 impls is 48 clone `self.data`, 3 construct, 1 drains.

The reason this is worth a line rather than nothing: the shipped survey proves only the
*drain* half — I ran it verbatim and it prints `src/common/requests/produce_request.rs`
and nothing else, exactly as claimed — while the *clone* half is an unchecked universal
sitting in the sentence that carries the section's whole rhetorical weight. That is the
same shape as Issue 2, one section away and freshly fixed.

- **Expected**: "Of this crate's **52** `RequestBuilder` impls, this is the only one
  that drains: 48 clone `self.data` and three (`elect_leaders`, `fetch`, `leave_group`)
  build a fresh `*RequestData` from typed fields." The `mock_client.rs` copy needs the
  same correction.
- **Actual**: 53 impls, every other clones.

---

## Examined, judged not findings

### The four pass-1 findings are resolved, not relocated

- **Issue 1.** `mock_client.rs:676-701` now names
  `ProduceRequestBuilder::build_version`'s `mem::replace`
  (`common/requests/produce_request.rs:300`) as the cause, keeps the cost argument as a
  separate bullet, and adds a note recording that the earlier revision had the wrong
  object *and* the wrong section. It also states the consequence explicitly ("Building
  here unconditionally would put a second build in reach of `respond_with_matcher`"),
  which the original did not. Resolved, and stronger than what I asked for.
- **Issue 2.** The prose decomposition is replaced by a shipped `awk` program plus its
  real output. I ran it: **57 placements**, groups `1 / 16 / 33 / 4 / 3`, duplicates
  exactly `testIdempotentInitProducerIdWithMaxInFlightOne` and
  `testTransactionalSplitBatchAndSend`, and `cut -f2 | sort -u | wc -l` → **55**. Every
  number in the block reproduces. The loop-50 clause is present and the "the total is
  the check" claim is explicitly retracted with the reason (a re-listing now offsets a
  move). *On the Manager's question — has the trust relocated?* Mostly no: the 57, the
  per-group counts, the two duplicate names and the 55 distinct are all mechanical. One
  interpretive step remains and is stated rather than hidden: the program cannot tell
  which of a duplicate's two occurrences is the *placement* and which the *citation*,
  which is why the Phase-5a heading reads `(3)` against 4 rows. That residue is
  irreducible without marking the citations syntactically, and the prose names both
  cases. Good enough, and much better than a sum.
- **Issue 3.** All three `destination()` → `Node` → `isReady` triples are translated
  through `assert_produce_in_flight_and_ready`, called exactly 3 times — matching Java's
  three sites at 2441-2447, 2462-2466, 2479-2483. The doc no longer claims the deviation
  list covers this, and explains why the omission was never forced. Resolved.
- **Issue 4.** `status.md` now lists three reproducers (§9.25, §9.1, §9.30) and says
  what became of the old third; `cargo test --lib -- --ignored --list` prints exactly
  those three names. `PLAN.md:2483` is rewritten to past tense. Resolved.

### Issue 3's fix is discriminating — verified from the implementation, not from the probe

The Actor's +7 probe is sound, and I did not need to take it on trust, because
`MockClient::is_ready` settles it statically:

```rust
fn is_ready(&self, node: &Node, now: i64) -> bool {
    if let Some(state) = self.connections.get(node.id_string()) { state.is_ready(now) } else { false }
}
```

`connections` is keyed on `node.id_string()` (`node.rs:68`), the rig registers nodes 0
and 1, so a perturbed id is a key miss and the `else { false }` arm fires the
`"Client ready status should be true"` assertion. The assertion therefore genuinely
reads its input; it is not satisfiable by construction.

Separately: it *is* a weak assertion in the sense that it cannot fail in this scenario
(the connection is Connected and unthrottled whenever a request is in flight) and cannot
detect a wrong-broker send, since the node is derived from the request's own
destination. That is equally true of Java's — `ConnectionState.isReady` is
`state == READY && !isThrottled` — so faithfulness is the right standard and the Actor
says so at the site. Not a finding.

The helper moves `isReady` ahead of the two `inflightBatch` assertions that Java
interleaves at 2445-2446. Independent assertions, no ordering semantics. Not a finding.

### The Actor's two self-found corrections are right

Checked each cited line against `SenderTest.java`:

| line | content |
|---|---|
| 2371 / 2372 | `@Test` / `public void testIdempotentSplitBatchAndSend()` |
| 2384 / 2385 | `@Test` / `public void testTransactionalSplitBatchAndSend()` |
| 3604 / 3605 | `@Test` / `public void testNoBufferReuseWhenBatchExpires()` |
| 2406 | `private void testSplitBatchAndSend(TransactionManager txnManager,` |
| 2496 / 2497 | `        }` (try-with-resources) / `    }` (method) |

So the declaration lines are 2372 / 2385 / 3605 and the driver's method brace is 2497.
All four corrections are exact. The two sweep counts also check out: 60 entry-citation
occurrences over 55 distinct names, and 105 headers carrying a range (55 `SenderTest` +
50 `TransactionManagerTest`) — the latter confirmed by the probe in Issue 5, which
reports `len(now) == 105`.

### PLAN §9.30, claim by claim

- **Java's `Builder.build(short)` shares the reference — confirmed.**
  `ProduceRequest.java:68` is the declaration and `:73` is
  `return new ProduceRequest(data, version);`, with `data` a `private final` field on
  the Builder (`:57`) that `build` never writes. `:74` is the closing brace, so the
  cited `68-74` is exact.
- **`clearPartitionRecords` is a different object and server-side — confirmed.** The
  field it nulls is `ProduceRequest`'s own `private volatile ProduceRequestData data`
  (`:97`), whose comment at `:94-96` says it is nulled "when a produce request is put in
  the purgatory (due to client throttling …)". The method itself is at `:205-209` and
  sets `data = null` on the request. Nothing touches the Builder.
- **Same shape as §9.12 defect 2 — accepted.** §9.12 defect 2 was `write` draining a
  `records` field so that *serialising mutated the message*; this is `build` draining
  `data` so that *building mutates the builder*. Same justification ("zero-copy
  ownership transfer"), same unnecessity (`ProduceRequestData` is `Clone` and its
  `records` are `bytes::Bytes`). The analogy holds.
- **Outlier claim — see Issue 6.** The conclusion is right and the shipped survey
  reproduces; only the denominator and the "every other clones" universal are off.
- **"Latent, not live" — ESTABLISHED, and I re-derived it rather than accepting it.**
  I classified every `.build()` / `.build_version(` call site in `src/` against each
  file's `#[cfg(test)]` boundary. Outside test cfg there are six, and four are not
  callers in the relevant sense: `abstract_request.rs:161` is the trait's default
  `build()` delegating to `build_version`; `fetch_request.rs:347` is
  `FetchRequestBuilder`'s own inherent `build()`; and `mock_client.rs:383-384` /
  `:703` are the two `MockClient` sites (`MockClient` lives in `src/` rather than behind
  `cfg(test)`, so a mechanical classifier calls them production — they are test
  infrastructure, as the Actor characterises them). That leaves
  **`network_client.rs:502` as the sole production build site**, exactly as claimed.
  Two further checks the section does not make and I did:
    - `network_client.rs:501-536`: the `Err` arm does **not** rebuild. It formats an
      error, calls `make_header` (which reads `latest_allowed_version`, not the data),
      and pushes an aborted send. No second build on any branch.
    - `impl fmt::Display for ClientRequest` (`client_request.rs:147-161`) prints scalar
      fields only, so the `panic!("Cannot send {} …")` at `mock_client.rs:658` and every
      log site that formats a request cannot trigger a build.
  On the test side, `mock_client`'s `send` builds only for a *matched future response*,
  after which the request never enters `self.requests`; `respond_with_matcher` builds
  only what is in `self.requests`; and `sender.rs`'s
  `send_idempotent_producer_response` builds the front request once and then answers
  with `respond`, which pops without building. Mutually exclusive, one build per
  request. The claim holds.
  *One imprecision I am deliberately not filing:* "The three test-side callers" is
  unqualified, and there are roughly ten more build sites under `#[cfg(test)]` in
  `admin/`. They build admin builders and can never hold a `ProduceRequestBuilder`, and
  a reader inside a section titled `ProduceRequestBuilder::build_version…` supplies that
  scope. Worth knowing, not worth a finding.
- **The reproducer fails for the right reason — confirmed by running it.** It clears
  `assert_eq!(first.data().topic_data.len(), 1)` and then fails at `:457` with
  `left: 0, right: 1` on the second build. It does not fail early on
  `validate_records`, which is what the hand-built magic-v2 batch in the test exists to
  avoid.

### The `built_size` / `isClosed` strengthening is correctly recorded

`MemoryRecordsBuilder.java` has no `boolean closed` field (`grep` finds none);
`isClosed()` at `:885-887` is exactly `return builtRecords != null;`. Both the
`build()` call-site comment (`memory_records_builder.rs:312-325`) and the new PLAN
paragraph state it accurately, including that `built_size` existed only to answer
`estimated_size_in_bytes()` in a state Java cannot reach. Citation exact.

### Scope

Clean. Production source changes in this range are `mock_client.rs` (comment only) and
`memory_records_builder.rs` (comment only); everything else is a new `#[ignore]`d test
in `produce_request.rs`, test-only edits in `sender.rs` (all below `mod tests` at
:2414), and docs. **PLAN §9.1 (`:999`) is untouched** — the three PLAN hunks are at
2068, 2481 and 2694. **§9.25 was touched, and correctly so**: the single edit is the
one-sentence past-tense rewrite at `:2483` that my own Issue 4 asked for. No
substantive change to either section.

### One clause about the extraction instruction

`:8324` says to recover the taxonomy program by stripping the `    //     ` prefix from
each line. Blank separator lines inside the block are bare `    //`, which that strip
leaves intact and Python rejects with an `IndentationError`. Mapping bare `    //` to an
empty line makes it run and reproduce the table exactly. Mentioning it only so the next
person does not conclude the program is broken; not worth changing.
