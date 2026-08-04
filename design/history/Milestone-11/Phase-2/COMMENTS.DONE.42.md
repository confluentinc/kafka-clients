# Critic 42 — Milestone 11 Phase 2: REVIEW LOOP CLOSED

Three Critic passes per `agent-roles.md` steps 2-6. Converged on pass 3 with **no
Phase 2 defects**.

| Pass | Findings | Outcome |
|---|---|---|
| 1 | 5 (1 functional) | Fixed — `1391c69` |
| 2 | 2 (both low, neither behavioural) | Fixed — `b62e218` |
| 3 | 1, entirely inside the new rules §12 prose | Fixed inline; **zero Phase 2 defects** |

## Pass 1 — the functional finding

`InitProducerIdRequestBuilder` offered v6 where Java caps at v5. Java's
`AbstractRequest.Builder(ApiKeys)` delegates to `Builder(apiKey, false)` →
`latestVersion(false)` — "any supported and *released* version"
(`AbstractRequest.java:46-51`). The spec sets `latestVersionUnstable: true` on
`validVersions: 0-6`, and Rust's `latest_version()` hardwires the
unstable-inclusive accessor.

Compounding factor the Critic identified: v6 is the KIP-939 2PC version Phase 5
implements, and `Enable2Pc`/`KeepPreparedTxn` are v6+ **non-ignorable**, so at a
negotiated v5 Rust would silently drop them where Java throws — interacting with
the §9.1 generator gap.

The Actor then over-extended the finding to `EndTxn` and `AddPartitionsToTxn`,
having grepped for the *presence* of `latestVersionUnstable` rather than its value.
Both are `false`; the Critic's original scope was correct. The Actor's own new
regression test caught the error before it landed.

## Passes 2 and 3

Both found the same shape of issue: a fix reaching some of the sites it applied to
rather than all. Pass 2 — the version-cap convention applied to 2 of 4 builders,
and a PLAN correction *appended* to a stale claim instead of replacing it. Pass 3 —
three prose imprecisions in the rules §12 written to fix pass 2, including a line
number the Critic had itself got wrong in pass 1 and which was propagated verbatim.

## Verdicts carried forward

  - **All four scoping calls independently confirmed** — the omitted members of
    `AddPartitionsToTxnRequest`, the static `TxnOffsetCommitRequest.getErrorResponse`,
    `TxnOffsetCommitResponse.Builder`, and the single-arg
    `getErrorResponse(Throwable)` are all broker-side with zero
    `clients/src/main` callers.
  - **The deterministic-sort deviation is behaviour-preserving** at every version;
    all four sites feed array fields the broker treats as sets, and no
    `HashMap`-into-`write()` site was left unsorted.
  - **All 18 dispatch match sites** inspected arm by arm; none delegates to the
    wrong variant.
  - **The `ignorable` distinction verified on both halves** —
    `CommittedLeaderEpoch` dropping silently is correct, `InitProducerId.ProducerId`
    remains a genuine gap.
  - **Scope answer:** rules §12 implicates nine pre-existing builders outside
    Phase 2. Tracked as PLAN §9.7, deliberately not folded in.

---

# Full pass-3 report

# Critic 42 — Milestone 11 Phase 2, third pass

Reviewed `b62e218` ("fixup! Phase 2: address Critic 42 second-pass findings 6-7").
Nothing else in scope.

**Issues 6 and 7 are both correctly and completely resolved. There are no Phase 2
defects.** Build, full lib suite (2196 passed / 0 failed / 1 ignored),
`format-check` and `lint` all clean. The three changed production lines are
confirmed behavioural no-ops.

One finding remains, and it is entirely inside the new rules §12 text — docs-only,
three one-line corrections, no code. It should be applied inline; it does **not**
warrant a fourth review round. Separately, §12 reaches beyond Phase 2 and needs a
scoping decision from you, which I set out below rather than treating as a defect.

---

## Issue 8: rules §12 has three imprecisions that will cost a round in Phase 3 if left

- **File**: `.claude/rules/producer-transactions.md:488-532`
- **Severity**: Missing Requirement (documentation precision; docs-only, no code)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/requests/OffsetCommitRequest.java:55`;
  `AbstractRequest.java:42-67`

The rule's table, its reasoning, and its five-API list are all **correct** — see
the verification section below. These are three narrow defects in the surrounding
prose, each of which would cause a Critic applying §12 to flag correct code.

**(a) The retroactive scope is unstated, and §12 is violated by five existing
builders on the day it lands.**

§12:530 lists as an anti-pattern "`latest_version()` in a builder whose Java
counterpart calls `super(apiKey)`". Five builders outside Phase 2 match that
description right now:

| Rust site | Java builder `super(...)` |
|---|---|
| `src/common/requests/metadata_request.rs:191` | `super(ApiKeys.METADATA)` |
| `src/common/requests/find_coordinator_request.rs:192` | `super(ApiKeys.FIND_COORDINATOR)` |
| `src/common/requests/sasl_handshake_request.rs:116` | `super(ApiKeys.SASL_HANDSHAKE)` |
| `src/common/requests/sasl_authenticate_request.rs:122` | `super(ApiKeys.SASL_AUTHENTICATE)` |
| `src/common/requests/consumer_group_heartbeat_request.rs:138` | `super(ApiKeys.CONSUMER_GROUP_HEARTBEAT, enableUnstableLastVersion)` — §12 row 2 says "pass the same flag through"; the Rust builder does not model the flag at all |

All five are behaviourally correct today (every one of those APIs has
`latestVersionUnstable: false`, so the two accessors return the same value), and
all five are Milestone-8-or-earlier code. But §12 as written does not say whether
it governs new translations only or the whole crate, so the next Critic to read it
has no basis for deciding, and will reasonably raise all five.

*Fix*: one sentence stating the scope — e.g. "applies to builders translated from
this rule onward; the five pre-existing sites listed in follow-up N are correct
today and tracked separately."

**(b) `OffsetCommitRequest.java:56` should be `:55`.**

§12:521 cites `OffsetCommitRequest.java:56` for Java's deliberate
unstable-inclusive use. The `latestVersion()` call is on **line 55**
(`forTopicIdsOrNames`); line 56 is the closing brace. `OffsetFetchRequest.java:64`
in the same bullet is correct. This is my own error from the first-pass report,
propagated verbatim — flagging it because §12 is a navigation aid and a Critic
will follow the citation.

**(c) Anti-pattern §12:531-532 contradicts "How to apply" bullet 2 on one
reading.**

    - An explicit-range Java `super(apiKey, oldest, latest)` translated as an
      accessor call instead of the literal bound.

For four sites the Java "literal bound" *is* an accessor call —
`ApiKeys.X.latestVersion()` at `ApiVersionsRequest.java:43`,
`OffsetCommitRequest.java:55`, `OffsetFetchRequest.java:64`,
`OffsetsForLeaderEpochRequest.java:57` — so read strictly, this anti-pattern
forbids `latest_version()` at exactly the four places §12:520-523 says it is
required. The intended target is clearly `AddPartitionsToTxn`, where Java passes
the constant `LAST_CLIENT_VERSION` and substituting an accessor would be wrong.
The word "literal" is doing two jobs: "verbatim" and "constant".

*Fix*: "…translated as a bound other than the one Java passes (e.g. an accessor
where Java passes a constant such as `LAST_CLIENT_VERSION`)."

- **Why it matters**: low, and nothing is broken. But §12's whole purpose is to
  stop this being a judgement call, and (a) plus (c) leave it ambiguous in the two
  directions that produced findings 1 and 6 in the first place. Three lines now
  versus a round of churn in Phase 3.

---

## Scoping decision you asked for: §12 reaches beyond Phase 2

You asked me to name this rather than let the phase widen silently. I swept every
`RequestBuilder` in the crate against its Java counterpart's `super(...)` form.
Nine builders remain on `latest_version()`; they split cleanly:

**Correct per §12 row 3 — Java's explicit range literally passes `latestVersion()`
(4 sites, no action needed):**

  - `api_versions_request.rs:169` ← `ApiVersionsRequest.java:43`
  - `offset_commit_request.rs:183` ← `OffsetCommitRequest.java:55`
  - `offset_fetch_request.rs:286` ← `OffsetFetchRequest.java:64`
  - `offsets_for_leader_epoch_request.rs:160` ← `OffsetsForLeaderEpochRequest.java:57`

  These four are right, but none carries the "say so at the site" marker §12:522
  requires — including the two §12 names by line number. So the
  deliberate-vs-not-yet-reached distinguishability §12 exists to create is absent
  at precisely the sites §12 cites as the exemplars.

**§12 implicates (5 sites, all behaviourally correct today, all outside Phase 2):**
the table in Issue 8(a).

**Recommendation**: open one follow-up covering both groups — add the four markers
and switch the five call sites — and do **not** fold it into Phase 2. It is nine
files across the common/consumer surface with zero behaviour change, which is a
poor fit for a transactions phase and would make a regression ambiguous between
the two. Narrowing §12 to producer transactions is the alternative, but I would
not: the rule is general and correct, and the reason it lives in
`producer-transactions.md` is only that Phase 2 is where the defect surfaced.
Consider moving it to a shared rules location when the follow-up lands.

---

## What I verified as correct

**Q1 — rules §12 is correct, and its three-row table is complete for this
codebase.** I initially suspected a gap: `AbstractRequest.Builder` has **four**
constructors, not three — `Builder(ApiKeys, boolean)` (`:42`),
`Builder(ApiKeys)` (`:49`), `Builder(ApiKeys, short allowedVersion)` (`:56`,
"allows only a specific version"), and `Builder(ApiKeys, short, short)` (`:63`).
§12's table omits the third, and since the two arity-2 forms differ only by
argument *type*, a table keyed on shape could in principle be mis-applied.

**It is not a gap.** I enumerated every builder `super(...)` call in
`clients/src/main/.../requests/` by brace-tracking into each
`extends AbstractRequest.Builder` body. The distribution is:

    77×  super(apiKey)
    11×  super(apiKey, oldest, latest)     [3-arg range, 4 spellings]
     5×  super(apiKey, enableUnstableLastVersion)
     0×  super(apiKey, allowedVersion)     [the single-version short form]

The single-version constructor has **no builder callers anywhere in the client
module** — the `super(ApiKeys.X, (short) 0)` and `super(ApiKeys.X, version)` hits
that look like it are all `AbstractRequest(ApiKeys, short version)` on the
*request* class, not the builder (e.g. `DescribeTopicPartitionsRequest.java:63`,
`CreatePartitionsRequest.java:52`). So the table covers every form that can
actually occur, the arity collision cannot arise, and omitting the fourth row is
right rather than incomplete. Recording the reasoning because the omission looks
like a defect until you check reachability.

Also verified in §12: the delegation claim and its `AbstractRequest.java:46-51`
citation are accurate (`:49-51` is `Builder(ApiKeys)` → `this(apiKey, false)`, and
the "any supported and *released* version" javadoc is at `:46-48`); the five-API
flag list is exactly right (`OFFSET_COMMIT`, `OFFSET_FETCH`, `INIT_PRODUCER_ID`,
`STREAMS_GROUP_HEARTBEAT`, `STREAMS_GROUP_DESCRIBE` — re-derived from the
generated `latest_version_unstable()` table, unchanged); and §12 correctly does
**not** forbid `latest_version()` where Java wants it (bullet `:520-523`, plus row
3 of the table independently yields the right answer for those sites).

**Q2 — all five Phase 2 builders are now consistent, and the
`AddPartitionsToTxn` exclusion is still right.**

  - `init_producer_id_request.rs:141`, `end_txn_request.rs:151`,
    `add_offsets_to_txn_request.rs:114`, `txn_offset_commit_request.rs:319` and
    `:360` — all `latest_version_with_unstable(false)`, each with a comment citing
    rules §12.
  - `add_partitions_to_txn_request.rs` keeps `LAST_CLIENT_VERSION`, correct
    because Java uses the explicit-range
    `super(ApiKeys.ADD_PARTITIONS_TO_TXN, minVersion, maxVersion)`
    (`AddPartitionsToTxnRequest.java:73`) with `forClient` passing
    `LAST_CLIENT_VERSION` literally.

**Q3 — PLAN §10.2 reads correctly top-down.** The stale clause is deleted, not
appended to; the surviving sentence ("All five construct or inspect a *received*
v4+ request, which only a broker does.") is grammatical and complete, and the
verified-callers paragraph follows with no contradiction. No orphaned text. The
parenthetical "a third file the original write-up omitted (Critic 42 finding 5)"
now refers to a write-up no longer quoted in the section, but it reads as a
historical note with its source cited, which is coherent — not worth changing.

**Q4 — the improved `EndTxn` test is right and does fail in the flag-flip
scenario.** `assert_eq!(ApiKeys::END_TXN.latest_version(), released)` compares
`highest_supported_version(true)` against `highest_supported_version(false)`. Today
both are 5 (flag `false`), so it passes. If `EndTxnRequest.json` ever gained
`latestVersionUnstable: true`, they become 5 and 4 and the assertion fails — which
is the intended tripwire. The message is accurate on both counts: the explicit
`false` form would indeed become load-bearing (capping at 4, matching Java), and
production would stay correct with no change, so failing the *test* rather than
shipping a divergence is the right outcome. The first assertion remains a
restatement of the production expression, but it is now paired with one that has
independent teeth, which was the point.

**Q5 — nothing regressed.** Exactly three production lines changed
(`add_offsets_to_txn_request.rs:114`, `txn_offset_commit_request.rs:319`, `:360`),
all from `latest_version()` to `latest_version_with_unstable(false)`.
`ADD_OFFSETS_TO_TXN` and `TXN_OFFSET_COMMIT` both have `latestVersionUnstable:
false`, so both accessors return the same value (4 and 5 respectively) and the
change is a provable no-op. Test count is unchanged from the second pass (2196
passed / 1 ignored) — consistent with the `EndTxn` test being modified in place
rather than added, and with no test removed or weakened.
