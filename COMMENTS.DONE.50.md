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

# Critic 50 pass 3 — resolved findings (review of `599f05e4..d46cdd13`)

Two findings, both real, neither disputed. Issue 7 carried five live defects.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 7 | `headers()`'s regex made the class prefix **optional**, so the five bare-method headers are in the population; a bare name is resolved by searching the Java files (`resolve`, which raises unless exactly one declares it) rather than by assuming a class. All five citations corrected. `len(now)` was distinct keys, not headers — the program now prints both, and asserts a repeated header cites the *same* range rather than silently overwriting. | `599f05e4` | population goes 105 → **111 headers over 110 distinct methods**; the taxonomy grows 9 → 14 rows because all five were wrong at `PREFIX` too; pasted transcript verified byte-identical to program stdout |
| 8 | §9.30's fix direction no longer says "matching the other 52 builders" — it reintroduced the universal the section had just deleted, and mis-counted it (with 52 impls "the other" is 51). | `6f9c0267` | `git log -S` confirms `6f9c0267` introduced it, i.e. the commit that fixed the count added the restatement |

## The third axis, probed rather than waited for

The Manager asked for the *new* domain to be probed before declaring this closed. All
eight axes of `headers()` were measured; the full result is now in the block, and this is
the summary:

| axis | result |
|---|---|
| `///`-block collector | 0 non-doc blocks carry a `Translated from … (Java N-M)` header |
| the `Translated from` literal | 56 blocks say "translating"/"mirroring" with a range — every one cites a **body region** of a helper or production method, which the declaration-to-brace rule does not govern. A deliberate boundary, not a hole |
| **class prefix** | **the defect — 5 bare-method headers, all 5 wrong** |
| class alternation | only `Sender`, `SenderTest`, `TransactionManager`, `TransactionManagerTest` are ever named; 0 production-class headers carry a range |
| `(Java N-M)` range shape | 1 single-line `(Java N)` citation, a field reference, not a test |
| dict keying | the count bug: 111 blocks → 110 keys |
| non-greedy `.*?` pairing | 0 blocks where a tight `` [^`]{0,120}? `` bound disagrees |
| file scope | 0 such headers in any other file under `src/` |

So: one coverage hole, one count bug, six clean. Issue 5 was the domain's **recency**;
issue 7 was its **shape**. Fixing one never touched the other, and probing one proved
nothing about the others — which is why the axes are now enumerated in the block rather
than discovered one pass at a time.

**One further self-correction the probe forced.** The shape-analysis prose after the
table ("seven of the nine were ±1 or ±2… four cited the blank line") was hand-written and
went stale the instant five rows were added — the same failure as the two stale sweep
counts, in the same block, one paragraph away. It is now **derived**: the program prints
`12 of 14 are ±1 or ±2 at one end; 2 large: …` and the label tally, and the prose points
at the transcript instead of repeating it. The dominant shape also changed with the new
rows, from "blank line" to "annotation", which no one would have noticed by reading.

## The `offset_fetch` question, settled — and the Manager's answer is the right one

Neither classifier was wrong; **the taxonomy was.**
`OffsetFetchRequestBuilder::maybe_downgrade` (`offset_fetch_request.rs:309-312`) returns
`self.data.clone()` on one version path and constructs a downgraded copy on the other, so
it is in both categories at once and no count of "clones vs constructs" can be right. The
predicate that settles it, and which neither the Critic nor I had tested: it takes
**`&self`**, so it cannot drain whatever else it does. That generalises — a
`build_version` reaching its data only through `&self` borrows is safe by signature — and
§9.30 now says so instead of quantifying a non-partition.

## Where I disagree: §9.17's volumes are the same cause, not a second one

The Manager asked me to record the volume accumulation as **its own mechanism with its
own fix**, on the grounds that "volumes accumulate on *every* run, because the harness
declares one at `kafka_cluster.rs:53` with no auto-remove", so a `Drop` fix "will not fix
the volume half". I checked both halves of that and neither holds:

  - **The harness does not declare the volume.** `kafka_cluster.rs:53-54` is a path
    constant (`SECRETS_DIR = "/etc/kafka/secrets"`) used to place keystore files. The
    *image* declares three volumes — `docker image inspect apache/kafka:4.2.0` gives
    `/etc/kafka/secrets`, `/mnt/shared/config`, `/var/lib/kafka/data` — so every broker
    container creates three anonymous volumes no matter what the harness does, and
    editing `:53` cannot change that.
  - **They do not accumulate on a clean run.** `ContainerAsync::drop`
    (`testcontainers-0.27.3/src/core/containers/async_container.rs:277`) calls
    `client.rm(&id)`, and `Client::rm` (`core/client.rs:227-237`) builds its options with
    `.force(true).v(true)` — `v` is "remove anonymous volumes". A normally-dropped
    container takes its three with it.

So volumes survive on exactly the condition containers do: the process dying without
running drops. That is §9.17's own cause, and §9.17's proposed fix covers both halves.
Recorded there as a second *symptom* rather than a second mechanism.

Two caveats I have stated in the section rather than glossed: `env::Command::Keep`
(`TESTCONTAINERS_COMMAND=keep`) disables removal entirely, and "a clean run leaves zero
volumes" is derived from the testcontainers source, **not measured** — Docker was wedged
and measuring it would have orphaned more containers. That measurement is owed.

**Two things from the Manager's note I did adopt**, because they are right and are new
information:

  - `docker volume prune -f` now sits beside `docker rm -f -v` in §9.17's playbook.
  - The **hang** is recorded as a distinct symptom. A partial KRaft quorum answers TCP
    and never becomes usable, so clients block on metadata rather than failing fast —
    strictly worse than the port collision the section was filed for, because a hang
    mimics a client defect and invites bisecting the code.

**Nothing rejected as a finding in any pass**, so `COMMENTS.FP.md` remains unchanged; the
disagreement above is with a framing in the coordinator's note, not with a Critic finding.

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

# Critic 50 — pass 3, review of `b9e75717..HEAD`

Range: `05134e8c`, `6130ab78`.

Both pass-2 findings are fixed at the source, and the §9.30 rework is the strongest
artifact this loop has produced — three greps shipped as commands with the reason each
differs, and a universal *deleted* rather than restated. **The gate fix is correct and I
reproduced its bite.** But probing its domain the way I probed the broken one found a
hole one level down, and this time the hole contains **five live defects**.

Two findings. Issue 7 is the substantive one.

Non-Docker gate re-run by me on HEAD: `cargo test --lib` 3520 / 0 / 3 ignored,
`format-check` clean, `lint` clean, `check-generated` clean (199 files). Per the
instruction I ran nothing needing Docker.

---

## Issue 7: the fixed gate's domain is still narrower than the artifact, and all five headers outside it are wrong

- **File**: `src/producer/internals/sender.rs:8367-8372` (the `headers()` classifier),
  claim at `:8296-8300`; the five defective headers at `:4369`, `:4428`, `:4466`,
  `:4526`, `:4873`
- **Severity**: Bug (five wrong Java citations) + Missing Requirement (the sweep that
  exists to catch them cannot see them)

**Description.** Pass 2's fix moved the gate's domain from *history* (`cited`, keyed on
`PREFIX`) to the *current artifact* (`now`). That was the right move and I verified it
bites — see below. But `now` is not the artifact; it is whatever the `headers()` regex
matches:

```python
pat = re.compile(r'Translated from\s+`(SenderTest|TransactionManagerTest)\.'
                 r'([A-Za-z0-9_]+)`.*?\(Java\s+(\d+)\s*[-–]\s*(\d+)')
```

The class prefix inside the backticks is mandatory. `sender.rs` contains **five further
rustdoc headers that carry a `(Java a-b)` range** but name the method alone, with the
class given in prose — e.g. `sender.rs:4428`:

```rust
/// Translated from `testDisconnectAndRetry` (Java 1080-1091): a disconnected
/// `FindCoordinator` response leaves the coordinator unknown …
```

I resolved all five against `TransactionManagerTest.java`:

| `sender.rs` | method | cited | true | error |
|---|---|---|---|---|
| 4369 | `testLookupCoordinatorOnDisconnectAfterSend` | 1260-1290 | **1261**-1290 | start −1 |
| 4428 | `testDisconnectAndRetry` | 1080-1091 | **1081**-1091 | start −1 |
| 4466 | `testLookupCoordinatorOnDisconnectBeforeSend` | 1292-1321 | **1293**-1321 | start −1 |
| 4526 | `testUnsupportedInitTransactions` | 1117-1134 | **1118**-1134 | start −1 |
| 4873 | `testUnsupportedFindCoordinator` | 1100-1115 | **1101**-1115 | start −1 |

Verified line by line: 1080 / 1117 / 1100 / 1260 / 1292 are `@Test`, and
1081 / 1118 / 1101 / 1261 / 1293 are the `public void …()` declarations.

**Five of five are wrong, in exactly the way this convention exists to catch** — the
`@Test` annotation line cited where the convention is the declaration line. That is
character for character the defect loop 50 committed at 2371 / 2384 / 3604 and was
faulted for in pass 2. Every instance of it in this file that predates loop 50 is
outside the gate's denominator.

So the block's **"105 headers carrying a range … 0 mismatches"** understates its
population. The file has **110** headers carrying a range; 105 are checked and clean,
and the 5 excluded are all defective. The denominator excludes precisely the defective
members — for the fourth time in this block's history, after Critic 46 issue 3, Critic
48 issue 9, and pass 2's issue 5.

The distinction from issue 5 is worth stating, because the pass-2 fix was right and this
is not a regression of it: issue 5 was a *temporal* hole (the domain was a past
snapshot); this is a *shape* hole (the domain is whatever one regex matches). Fixing the
first does not touch the second. The block's own general rule — "a gate's domain must be
the current artifact" — is the correct rule; it just is not yet satisfied, because
`now` is a projection of the artifact through a classifier, not the artifact.

**A second, minor face of the same root cause.** `headers()` returns a dict keyed on
`(cls, name)`, so `len(now)` counts distinct keys, not headers. There are **106**
matching blocks against **105** keys:
`('SenderTest', 'testTransactionShouldTransitionToAbortableForSenderAPI')` appears at
`sender.rs:13314` and `:13322`. Both cite `3051-3101`, so nothing is wrong today — but a
second block citing a *different* range would be silently dropped by the dict and the
printed count would still read "105 headers".

- **Expected**: make the class prefix optional in the classifier and resolve a bare name
  against both Java files (which is what the file's own prose does), and key the
  collection on the header's source line rather than on `(cls, name)`. Then the five
  mismatches fail the gate, and the printed denominator is the artifact's rather than
  the regex's. The five citations then need correcting to 1261 / 1081 / 1293 / 1118 /
  1101.
- **Actual**: five wrong Java citations, invisible to the sweep that exists to find
  them, under a "0 mismatches" claim.

*Context, not a defect:* the file also carries **16** test headers in a third shape,
`Translated from Java \`SenderTest.testX()\``, none of which carries a range
(`grep -c` → 16, of which 0 contain `Java <digits>`). "Carrying a range" honestly
excludes them and a header with no range has nothing to be wrong. I note it only because
it is the reason a shape-keyed denominator is fragile here: this file uses three header
conventions and the sweep knows one.

---

## Issue 8: §9.30's fix-direction paragraph reintroduces the universal the section deletes, with an off-by-one

- **File**: `design/history/Milestone-11/PLAN.md:2795-2796`
- **Severity**: Design Flaw (documentation)

**Description.** §9.30 now says, correctly and carefully:

> **What the 51 non-draining impls do is deliberately left unquantified.** […] Any finer
> split is a judgement call that two readers will count differently […] the load-bearing
> claim is that **exactly one drains**, which the command above settles.

Three paragraphs later:

> **Fix direction:** replace the `mem::replace` with `self.data.clone()`, **matching the
> other 52 builders**.

Two problems in four words. There are 52 impls in total, so "the other" ones number
**51** — the figure the same section uses correctly above. And "matching the other N
builders" re-asserts that all of them clone `self.data`, which is the universal the
section just deleted as false. `git log -S "matching the other 52 builders"` returns
`6f9c0267`: the sentence previously read "the other 53 builders" and this commit
corrected the numeral without noticing the claim underneath it. (The *other* count in
the same sentence, "a `RequestBuilder` trait change across all 52 impls", is right.)

- **Expected**: drop the comparative or weaken it to what is true — e.g. "replace the
  `mem::replace` with `self.data.clone()`, which is what most of the other 51 do".
- **Actual**: an off-by-one plus a restatement of a deleted universal, inside the
  section that deletes it.

---

## Examined, judged not findings — including every question put to me

### The gate fix is correct, and I reproduced its bite independently

Extracted the program (lines 8362-8444, stripping `    //     ` and mapping bare
`    //` to blank) and ran it. It reproduces the shipped transcript with no content
differences beyond my slice boundaries:

```
9 rows, 10 cells: 4 blank line, 2 annotation, 2 body statement, 1 comment, 1 next-method token
105 headers carrying a range (55 SenderTest + 50 TransactionManagerTest), 0 mismatches
60 entry citations over 55 distinct names, 0 mismatches
every one of the nine is correct in the working tree
```

Both promoted numbers therefore **are** program output now, not prose — the corollary
that mattered most from issue 5. Then I made the gate's input a perturbed copy of
`sender.rs` in memory (`2372-2382` → `2371-2382`) and got:

```
AssertionError: ranges wrong: [('SenderTest', 'testIdempotentSplitBatchAndSend')]
```

verbatim what the Actor reported. The gate bites, and it names the offender.

### The `cited`-keyed taxonomy is genuinely immune — the blind spot has not moved into it

This was the sharper half of the question and the answer is clean. The taxonomy loop
builds `rows` from `cited` and prints them, and *then* the gate runs
`bad = [... for (c, n) in now ...]; assert not bad`. So a deviation introduced after
`PREFIX` cannot be hidden by the table's keying: it crashes the program before the final
line prints. The table stays a historical record of the nine; the gate covers the
present. Keying a *taxonomy* on history while keying the *gate* on the artifact is the
right split, and the block states it as a general rule. Issue 7 is not a counterexample
to that rule — it is the rule not yet fully satisfied, because `now` is the artifact
seen through a regex.

### The grep reconciliation is right

Reproduced all three:

| command | count | why |
|---|---|---|
| `grep -rl "fn build_version" src/` | **53** | includes `abstract_request.rs:169`, the trait declaration |
| `grep -rl "impl RequestBuilder for" src/` | **51** | misses `fetch_request.rs:517` |
| `grep -rlE "impl [A-Za-z_:]*RequestBuilder for" src/` | **52** | correct |

`fetch_request.rs:517` is indeed
`impl crate::common::requests::RequestBuilder for FetchRequestBuilder`. **52 is the real
number.** (One attribution nit, not worth filing: the section says the 51 form is "what
I get and what the Manager got"; my pass-2 grep was
`"^impl RequestBuilder for\|^impl .*RequestBuilder for"`, whose second alternative
catches the qualified impl, which is how I also reached 52. The fact is right either
way.) The re-scoped drain survey — over `fn build_version(&mut self` bodies of those 52
— runs and prints `src/common/requests/produce_request.rs` and nothing else, which also
correctly excludes `fetch_request.rs`'s *inherent* `build_version` at `:351`.

### `offset_fetch_request.rs`: neither classifier is wrong — the taxonomy is

`build_version` calls `maybe_downgrade(&self, version)` (`:309-312`):

```rust
fn maybe_downgrade(&self, version: i16) -> OffsetFetchRequestData {
    if version >= BATCH_MIN_VERSION || self.data.groups.is_empty() {
        return self.data.clone();
    }
    // …otherwise build a downgraded OffsetFetchRequestData from self.data…
}
```

So on one version path it is *literally* `self.data.clone()` and on the other it
constructs — the same builder is in both categories depending on the version argument.
My 48/3 counted the file because it contains the clone; the Actor's 47/4 counted the
`build_version` body because the call is one frame away. Both are defensible readings of
a binary split that the code does not obey. The property that actually settles the
question, and that neither classifier tested, is the receiver: `maybe_downgrade` takes
**`&self`**, so it cannot drain regardless of path. That is the predicate the section
cares about, and it is the one the shipped command tests.

### Deleting the universal was the right call

The section's argument is "it is an outlier, therefore unintended". "Exactly one of 52
drains" carries that argument by itself; the clone/construct split was decoration on it.
Restating a corrected universal would have been the third revision of a sentence twice
found wrong, which is the shape of the finding it was fixing. And nothing a reader needs
is lost: the fix direction still names `self.data.clone()` as the target, so the "clone
is the house pattern" signal survives — the only problem is *how* it survives, which is
Issue 8.

### PLAN §9.17 and the 6687 volumes — yes, it belongs there, but as a distinct mechanism

§9.17 as written is entirely about *containers*: the cause (containers created in
`tokio::spawn`ed tasks at `kafka_cluster.rs:361-375` and owned by `_containers` only
after the collect loop, with no `impl Drop`), the symptom (a deterministic host-port
collision that "mimics a code regression"), the fix (register teardown as each container
starts), and the remedy (`docker ps`, `docker rm -f`). Volumes appear nowhere.

It belongs in §9.17 because that is where anyone with a wedged Docker environment will
look, and because the remedy line is incomplete without it — `docker rm -f` reclaims
containers and not their anonymous volumes. But it must be written as a **second,
independent mechanism**, not folded into the existing cause, for three reasons:

  - The stated cause is abort-specific. Anonymous volumes accumulate on **every** run,
    clean ones included, because nothing removes them; the harness declares a volume for
    the SSL certificate directory (`kafka_cluster.rs:53`) and configures no auto-remove.
    So the growth is not a consequence of the missing `Drop`.
  - Therefore **the proposed fix does not fix it.** Registering teardown as each
    container starts reclaims containers; the volume count keeps climbing. Folding the
    two together would let a reader believe one fix closes both.
  - The symptoms differ, and the second one is the more dangerous. The container leak
    surfaces as `address already in use` — recognisably infrastructural. The volume
    accumulation surfaced here as brokers failing to form a quorum ("Node 1
    disconnected", "Node 3 disconnected") after ~20 minutes, which mimics a *client*
    defect and is exactly the kind of signal this loop has twice had to argue was
    environmental.

So: a new subsection under §9.17 with its own cause, its own fix (auto-remove or an
explicit volume teardown), and a "Meanwhile" that adds `docker volume prune` beside
`docker rm -f`.

### The `--no-verify` reasoning holds, and I closed the one gap in it by measurement

`6130ab78`'s only source change is a two-line comment in `src/mock_client.rs` (53 → 52
inside a `//` block); everything else is markdown. Comments cannot change codegen, so
"identical artifact to `05134e8c`" is sound and the Docker half of the gate would
necessarily have reproduced `05134e8c`'s green result.

One refinement rather than an objection: "identical artifact" would not, on its own,
cover `format-check` or `check-generated`, which read *source* and can fail on a comment
edit — rustfmt does reflow comments. So the argument has a small gap. I closed it by
running them: `format-check` clean, `check-generated` clean on 199 files, plus `lint` and
`cargo test --lib` 3520/0/3, all on this HEAD. Treating the unrun Docker half as
environmental debt is correct on the evidence, and I would carry it the same way.

### Scope

Clean. **§9.1 (`PLAN.md:999`) and §9.25 (`:2431`) are untouched** — the three PLAN hunks
are at 2715, 2764 and 2794, all inside §9.30 (which begins at 2697). The only source
changes in this range are the two-line `mock_client.rs` comment and the comment block in
`sender.rs` at 8286-8465, which is inside `mod tests` (line 2414). No behavioural code
changed in pass 3 at all.
