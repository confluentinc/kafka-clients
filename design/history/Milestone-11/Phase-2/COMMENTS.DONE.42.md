# Critic 42 — Milestone 11 Phase 2: CLOSED on a clean pass

Six Critic passes. **Pass 6 returned zero findings** — that is what closes this
phase, per `agent-roles.md` §2. Nothing was substituted for it.

| Pass | Findings | Nature | Fixes |
|---|---|---|---|
| 1 | 5 | 1 functional, 4 docs/test-strength | `1391c69` |
| 2 | 2 | consistency + docs, no behaviour | `b62e218` |
| 3 | 1 | **docs only** — rules §12 prose | `e8a91e7` |
| 4 | 2 | **docs only** — §9.7 justification, §12 citation | `8264450` |
| 5 | 3 | **docs only** — flag anchoring in §9.7/§12, PLAN structure | `e7442fe` |
| 6 | **0** | — | closes the loop |

## Pass 5's findings (all three confirmed by the Actor, independently)

  1. **§9.7's flag claim was anchored to the wrong corpus.** The 9-vs-10 divergence
     is real, but `"latestVersionUnstable": true` for `OFFSET_COMMIT` /
     `OFFSET_FETCH` is a property of **`generator/messages/`**, which `build.rs:44`
     compiles — **not** of `kafka/` 4.2, where only `InitProducerIdRequest.json`
     sets it. A reviewer checking `kafka/` as CLAUDE.md directs would have declared
     the warning false and switched all nine, causing the exact regression it
     exists to prevent. Same for rules §12's "five APIs carry the flag".
  2. **The §9 reorder achieved ascending order by pushing §9.7/§9.8 past the
     `## 10` boundary**, nesting them under "Recorded translation deviations".
     §12 says the nine-builder work is "tracked as PLAN §9.7"; anyone reading §9 to
     its end never reached it.
  3. Rule §12's Java citations — **all correct**, including pass 4's fix.

Fixed by naming the corpus at every claim site, moving §9.7-§9.8 back inside §9,
and adding **§9.9** for the root cause: `generator/messages/` is a pre-4.2 snapshot,
36 of 197 specs differ, never refreshed since `6cd275c`.

## Pass 6 (clean)

Verified `e7442fe` is documentation-only (`git diff --stat e8a91e7 HEAD -- '*.rs'`
empty; no non-`.md` file changed at all), re-derived every figure in §9.9
independently, and confirmed §9.1-§9.9 are ascending inside `## 9` with all 17
`§9.x` cross-references resolving.

Went beyond its brief on one point: rather than accept §9.9's "no live defect", it
audited **every** reader of the flag in the tree. `latest_version_unstable()` has no
caller outside the generated file; the only production
`latest_version_with_unstable(false)` sites are the five txn builders, whose specs
are flag-identical across corpora; `is_version_enabled` and
`to_api_version_internal` are reached only from `api_versions_response.rs` and every
caller passes `true`. For the Streams pair `validVersions` is identical in both
corpora, so the live accessor returns Java's value. The claim holds.

Recorded three items it examined and deliberately did not raise, so they read as
considered rather than missed — including that §9.8's pass-5 row could not cite the
SHA of the commit containing it.

## What the closure rests on

**Phase 2's code was last found defective in pass 1** and has been clean across
passes 2, 3, and 4. Passes 3 and 4 found nothing in `src/` at all — both were
entirely about documentation, and specifically about the *follow-up item* (§9.7) and
the *rule* (§12) that pass 1's fix produced.

**A clean pass exists.** Six passes; the sixth found nothing. Pass 4's two
findings were fixed in `8264450` and re-verified **mechanically by the Actor**, not
by an independent reviewer:

  - Issue 9: `OFFSET_COMMIT` and `OFFSET_FETCH` confirmed `latestVersionUnstable:
    true` in the generated table, so the warning block naming them is correct.
  - Issue 10: `AddPartitionsToTxnRequest.java:58` confirmed as the line passing
    `LAST_CLIENT_VERSION`.
  - The §9 heading reorder: 8 sections, 0 duplicates, ascending.

These are factual checks, not judgement. A reviewer might still find a reader could
misact on §9.7 — that is precisely what passes 3 and 4 each found, so the
possibility is not hypothetical.

## The pattern across both phases

Of seven completed Critic passes across Phases 1 and 2, **six found something
real**, and in each phase later passes found defects in *fixes* rather than in the
original work. Phase 2's own translation was sound; every round after the first was
repairing repairs.

Two of those were the same Actor error in opposite directions:

  - Pass 2: checked a spec flag's **presence** instead of its value → wrongly
    reported two classes as divergent.
  - Pass 4: asserted a flag's **value** across a set without checking each member →
    wrongly told §9.7's executor that switching all nine builders was inert, which
    would have capped `OffsetCommit`/`OffsetFetch` at 9 instead of 10.

## Verdicts carried forward from the passes

  - **All four broker-side scoping omissions independently confirmed** — zero
    `clients/src/main` callers for any of them.
  - **The deterministic-sort deviation is behaviour-preserving** at every version;
    no `HashMap`-into-`write()` site left unsorted.
  - **All 18 dispatch match arms** inspected individually; none delegates to the
    wrong variant.
  - **Both halves of the `ignorable` distinction verified.**
  - **§12 implicates nine pre-existing builders** outside Phase 2 → tracked as
    §9.7, deliberately not folded in.

---

# Pass 4 report (the last completed pass)

# Critic 42 — Milestone 11 Phase 2, fourth pass

Reviewed the rules-file and PLAN portions of `e8a91e7` — the Issue 8 fix that no
pass had checked. Archival bookkeeping (`COMMENTS.DONE.42.md`, §9.8) reviewed for
correctness only; nothing wrong found there.

**This pass is not clean.** Two findings. Fixes (b) and (c) are correct and
complete; fix (a) is correct in `.claude/rules/producer-transactions.md` but the
follow-up it points at, PLAN §9.7, carries a false justification that would make a
mechanical execution of that follow-up introduce a real behaviour regression.

Both are docs-only. Neither is a Phase 2 code defect — Phase 2's code remains
clean, as pass 3 found.

---

## Issue 9: PLAN §9.7's justification is false for two of its nine builders, and it invites a regression

- **File**: `design/history/Milestone-11/PLAN.md:1032-1063` (§9.7), the sentence at
  `:1038-1041`
- **Severity**: Bug (latent — a docs claim that would induce a code defect if the
  follow-up is executed as written)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/requests/OffsetCommitRequest.java:55`;
  `OffsetFetchRequest.java:64`

- **Description**:

  §9.7 justifies the whole follow-up with:

      **All are behaviourally correct today** — every affected API has
      `latestVersionUnstable: false`, so the two accessors agree — so this is
      faithfulness and future-proofing, not a bug.

  The clause after the dash is **false for two of the nine**. Measured from the
  generated `api_message_type.rs` tables:

  | API | `latestVersionUnstable` | `latest_version()` | `latest_version_with_unstable(false)` |
  |---|---|---|---|
  | `API_VERSIONS` | false | 4 | 4 |
  | **`OFFSET_COMMIT`** | **true** | **10** | **9** |
  | **`OFFSET_FETCH`** | **true** | **10** | **9** |
  | `OFFSET_FOR_LEADER_EPOCH` | false | 4 | 4 |
  | `METADATA` | false | 13 | 13 |
  | `FIND_COORDINATOR` | false | 6 | 6 |
  | `SASL_HANDSHAKE` | false | 1 | 1 |
  | `SASL_AUTHENTICATE` | false | 2 | 2 |
  | `CONSUMER_GROUP_HEARTBEAT` | false | 1 | 1 |

  For `OFFSET_COMMIT` and `OFFSET_FETCH` the two accessors differ by one. The
  *conclusion* ("all are behaviourally correct today") still holds, but for a
  different reason in those two cases: they are correct because Java **deliberately**
  passes `ApiKeys.X.latestVersion()` — `OffsetCommitRequest.java:55`,
  `OffsetFetchRequest.java:64` — not because the flag is false.

- **Why it matters — this is not cosmetic.** The item is titled "Bring nine
  pre-existing `RequestBuilder`s in line with rules §12". Someone executing it
  after reading "every affected API has the flag false, so the accessors agree"
  will reasonably conclude the change is inert everywhere and switch all nine to
  `latest_version_with_unstable(false)`. For `offset_commit_request.rs:183` and
  `offset_fetch_request.rs:286` that caps the builder at **9 instead of 10**,
  diverging from Java on the consumer's offset-commit and offset-fetch paths — the
  mirror image of the original finding 1, and on a hotter path.

  The §9.7 table itself is right: the action for that group is "add the
  'deliberate' marker", not switch the call. So the item is internally recoverable.
  But the justifying sentence is the one a reader uses to decide how much care the
  work needs, and it says the opposite of the truth for the two builders where care
  is actually required.

  It is also the same reasoning slip as pass 2, inverted: there the flag's
  *presence* was checked instead of its *value*; here the value is asserted as
  `false` for a set in which two are `true`.

  Note `.claude/rules/producer-transactions.md:521-532` (the §12 Scope block) does
  **not** make this error — it says "All are behaviourally correct today; four are
  faithful (Java's own bound is `latestVersion()`) and five match the anti-pattern
  above", giving the correct per-group reason. The defect is confined to PLAN §9.7.

- **Expected**: replace the blanket clause with the per-group reason §12 already
  uses, and state the hazard explicitly — e.g. "All are behaviourally correct today,
  for two different reasons: the five implicated APIs have
  `latestVersionUnstable: false`, so the accessors agree; the four faithful ones are
  correct because Java's own bound is `latestVersion()`. Note `OFFSET_COMMIT` and
  `OFFSET_FETCH` *do* carry the flag (`latest_version()` = 10 vs 9), so switching
  those two would be a behaviour regression — they need the marker, not the call
  change."

- **Actual**: a blanket claim that is false for the two builders it most matters for.

---

## Issue 10: §12's `AddPartitionsToTxnRequest.java:73` citation points at a line that does not contain the named constant

- **File**: `.claude/rules/producer-transactions.md:553-555` (the reworked
  anti-pattern)
- **Severity**: Missing Requirement (documentation precision)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/requests/AddPartitionsToTxnRequest.java:42` (declaration), `:58` (passed), `:73` (the cited line)

- **Description**:

  The reworked anti-pattern reads:

      An explicit-range Java `super(apiKey, oldest, latest)` where Java passes a
      **constant** (e.g. `AddPartitionsToTxnRequest.java:73`'s `LAST_CLIENT_VERSION`)
      but Rust substitutes an accessor call.

  Line 73 is `super(ApiKeys.ADD_PARTITIONS_TO_TXN, minVersion, maxVersion);` — it
  contains no `LAST_CLIENT_VERSION`. The constant is declared at `:42` and passed at
  `:58`, inside `Builder.forClient`.

  A generous reading works ("the `LAST_CLIENT_VERSION` that reaches line 73"), but
  the same sentence carries three sibling citations —
  `ApiVersionsRequest.java:43`, `OffsetCommitRequest.java:55`,
  `OffsetFetchRequest.java:64` — and **all three point at the line where the bound
  expression literally appears**, not at the `super(...)` call. So the one citation
  a reader must follow to see a *constant* bound is the one that doesn't show it,
  and it is stylistically inconsistent with its neighbours.

- **Expected**: `AddPartitionsToTxnRequest.java:58` (or `:42`), matching how the
  other three are cited.

- **Actual**: `:73`.

- **Why it matters**: low, and I raise it only because fix (b) in this very commit
  was an off-by-one in exactly this kind of navigation citation, accepted on the
  grounds that "§12 is a navigation aid and a Critic will follow the citation". The
  same standard applies here. One character.

---

## What I verified as correct

**Fix (a) — the §12 Scope block is right.** The nine-builder split is correct, and
I re-derived every line number against the current tree rather than the previous
report:

  - Faithful (Java's own bound is `latestVersion()`, values match):
    `api_versions_request.rs:169` ← `ApiVersionsRequest.java:43`;
    `offset_commit_request.rs:183` ← `OffsetCommitRequest.java:55`;
    `offset_fetch_request.rs:286` ← `OffsetFetchRequest.java:64`;
    `offsets_for_leader_epoch_request.rs:160` ← `OffsetsForLeaderEpochRequest.java:57`.
  - Implicated, all five line numbers confirmed to land on the
    `latest_allowed_version: ApiKeys::X.latest_version(),` line:
    `metadata_request.rs:191`, `find_coordinator_request.rs:192`,
    `sasl_handshake_request.rs:116`, `sasl_authenticate_request.rs:122`,
    `consumer_group_heartbeat_request.rs:138`.

  The instruction is unambiguous: "**Do NOT flag these in a transactions-phase
  review**" plus "A Critic reviewing a *new* translation should apply this rule
  fully; a Critic reviewing pre-existing code should cite §9.7 instead." The second
  sentence generalises past transactions phases, so it holds for any future
  reviewer, not just Phase 3-8. I would not raise the nine under it.

**Fix (b) — `:55` is correct, and the rest of §12's citations check out.**
`OffsetCommitRequest.java:55` holds
`ApiKeys.OFFSET_COMMIT.latestVersion()` inside `forTopicIdsOrNames`; `:56` is the
closing brace. Also re-verified: `AbstractRequest.java:46-51` (javadoc "any
supported and *released* version" at `:46-48`, `Builder(ApiKeys)` →
`this(apiKey, false)` at `:49-51`); `OffsetFetchRequest.java:64`;
`ApiVersionsRequest.java:43`; `OffsetsForLeaderEpochRequest.java:57`. All accurate.
The one exception is Issue 10.

**Fix (c) — the ambiguity is resolved and the rule still forbids what it should.**
Row 3 now reads "translate the `latest` **expression** verbatim — a constant where
Java passes a constant, `latest_version()` where Java passes `latestVersion()`",
which removes the double duty the word "literal" was doing. The anti-pattern is
scoped to the constant case and explicitly permits the four accessor-bound sites,
so it no longer contradicts the "How to apply" bullet. It still catches the real
error (Java passes `LAST_CLIENT_VERSION`, Rust substitutes an accessor). The
reverse substitution — Java passes `latestVersion()`, Rust hardcodes a number — is
not in the anti-pattern list, but row 3 of the table covers it, and the
anti-pattern lists elsewhere in this file are not exhaustive either.

**§9.7 is otherwise accurate and actionable.** The two-group table, the per-file
actions, the "deliberately not folded into Milestone 11" rationale, and the
relocation note are all correct. The `consumer_group_heartbeat_request.rs` caveat
is captured well and is the right call: Java's
`ConsumerGroupHeartbeatRequest.Builder` does take `enableUnstableLastVersion` as a
parameter (`super(ApiKeys.CONSUMER_GROUP_HEARTBEAT, enableUnstableLastVersion)`),
the Rust builder models no such flag, and "decide whether to thread it through or
document why not" is the correct instruction rather than a mechanical switch. Only
the justification sentence is wrong (Issue 9).

**§9.8 and the archival bookkeeping are accurate.** The three-row pass table
matches what each pass actually returned (5 / 2 / 1); the functional finding is
described correctly including the §9.1 interaction; "both later passes found the
same shape of defect" is a fair characterisation; and the independent
re-derivations it credits are the ones I actually performed. No corrections.

**Non-finding observation.** §9's headings now run 9.1, 9.2, 9.3, 9.4, 9.5, **9.8,
9.7**, 9.6 — §9.8 and §9.7 were inserted above §9.6 in a file that was otherwise
ascending. Pure document organisation, trivially navigable by search; noting it
only so it can be tidied if §9 is ever touched again.

---

## Sign-off

**Phase 2's code is clean and has been for two passes.** But this pass is not
clean, so the loop does not close on it. Issue 9 is a latent bug rather than a
prose nit: left as written, §9.7 tells its executor that switching all nine
builders is inert, and for two of them it is not.

Fix Issues 9 and 10 — both single-sentence edits to files no code depends on — and
run one more pass. I expect it to come back clean; there is nothing left in Phase 2
itself to find.

---

# Passes 1-3 (archived earlier)

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

---

# Pass 5 report

# Critic 42 — Milestone 11 Phase 2, fifth pass

Reviewed `8264450` (`fixup! Phase 2: address Critic 42 fourth-pass findings 9-10`) —
the Issue 9 / Issue 10 fixes plus the §9 heading reorder in the same commit. Then
re-checked the areas earlier passes cleared, looking only for damage a *fix* could
have done.

**This pass is not clean. Three findings.**

Issue 10's fix is correct. Issue 9's fix is correct in its *instruction* and in its
*per-group reasons* — but the flag value it rests on is a property of this repo's
spec copy, not of Apache Kafka 4.2, and that is not stated (Issues 11 and 12).
The heading reorder moved two subsections out of their parent section (Issue 13).

**Phase 2's ten translated files remain clean.** `git diff --stat e8a91e7 HEAD --
'*.rs' 'Cargo.toml'` is empty; no production code has changed since the pass-3
verification. All five Phase 2 APIs are built from specs byte-identical to Kafka
4.2 (`InitProducerIdRequest.json` differs by one comment typo only), so finding
1's fix is correct against the Java contract.

---

## Issue 11: Rust's generated `latest_version_unstable` table disagrees with Kafka 4.2 on four APIs — `generator/messages/` is a pre-4.2 spec snapshot

- **File**: `generator/messages/OffsetCommitRequest.json:43`,
  `generator/messages/OffsetFetchRequest.json:45`,
  `generator/messages/StreamsGroupHeartbeatRequest.json:26`,
  `generator/messages/StreamsGroupDescribeRequest.json:26`
- **Severity**: Behavior Mismatch (latent — no production call site consults the
  flag for these APIs today)
- **Java Reference**:
  `kafka/clients/src/main/resources/common/message/OffsetCommitRequest.json`,
  `OffsetFetchRequest.json`, `StreamsGroupHeartbeatRequest.json`,
  `StreamsGroupDescribeRequest.json` (Apache Kafka 4.2.0, `gradle.properties`
  `version=4.2.0`); `kafka/.../common/protocol/ApiKeys.java:222-224`;
  `kafka/generator/src/main/java/org/apache/kafka/message/ApiMessageTypeGenerator.java:427-438`

- **Description**:

  The Rust build reads its message specs from `generator/messages/`, not from
  `kafka/`. For four APIs that corpus sets `"latestVersionUnstable": true` where
  Kafka 4.2 does not set the property at all:

  | API | `generator/messages` | Kafka 4.2 (`kafka/…/message/`) |
  |---|---|---|
  | `OFFSET_COMMIT` | `true` (`:43`) | absent → `false` |
  | `OFFSET_FETCH` | `true` (`:45`) | absent → `false` |
  | `STREAMS_GROUP_HEARTBEAT` | `true` (`:26`) | absent → `false` |
  | `STREAMS_GROUP_DESCRIBE` | `true` (`:26`) | absent → `false` |
  | `INIT_PRODUCER_ID` | `true` (`:32`) | `true` (`:32`) — agrees |

  In Kafka 4.2 **exactly one** spec sets the flag true:
  `InitProducerIdRequest.json:32`. Verified by
  `grep -rn '"latestVersionUnstable": true' kafka/clients/src/main/resources/common/message/`
  → one hit. The same grep over `generator/messages/` → five hits.

  For `OffsetCommitRequest.json` and `OffsetFetchRequest.json` the flag line is the
  **only** difference between the two copies (`diff` reports `43d42` and `45d44`
  respectively, nothing else). So this is not a version-range divergence that
  happens to carry a flag — it is precisely the flag.

  Both generators compute the same expression
  (`highest - 1` when the flag is set and unstable versions are disabled —
  `ApiMessageTypeGenerator.java:436` vs `generator/src/lib.rs:287-291`), so the
  arithmetic is faithful. Only the input differs. The consequence:

  | Accessor | Rust today | Java 4.2 |
  |---|---|---|
  | `OFFSET_COMMIT` `latest_version_with_unstable(false)` | **9** | **10** |
  | `OFFSET_FETCH` `latest_version_with_unstable(false)` | **9** | **10** |
  | `STREAMS_GROUP_HEARTBEAT` same | **-1** ("no enabled versions") | **0** |
  | `STREAMS_GROUP_DESCRIBE` same | **-1** | **0** |

  `latest_version()` is unaffected (it passes `true`), which is why nothing is
  broken today: `grep -rn latest_version_with_unstable src/` finds call sites only
  in the five Phase 2 txn builders plus two of their tests, and
  `latest_version_unstable()` has no caller outside the generated file. All five
  txn APIs have matching flags in both corpora, so **Phase 2 itself is correct**.
  The divergence is live only if a future builder or an ApiVersions-negotiation
  path asks for the released ceiling of `OFFSET_COMMIT` / `OFFSET_FETCH` — which is
  exactly what PLAN §9.7 contemplates doing.

- **Why it matters, and why it is untracked**:

  The drift is not confined to these four. 36 of 197 specs in `generator/messages/`
  differ from `kafka/`, and the direction is consistently *older*:
  `ListOffsetsRequest.json` has `validVersions: "1-10"` against 4.2's `"1-11"`
  (KIP-1023 v11 missing), and `InitProducerIdRequest.json` still carries the
  `Verison` typo that 4.2 fixed. `git log -- generator/messages/OffsetCommitRequest.json`
  shows a single commit, `6cd275c Initial branch (#1)`, so the corpus has never
  been refreshed.

  Nothing tracks this. PLAN §9.2 covers migrating the **Java source** base 4.2.0 →
  4.3.1 and is explicit about line-number citations, but says nothing about the
  spec corpus being *behind* 4.2.0. `design/current/design.md:261` asserts the
  opposite — "197 Kafka protocol definitions from Apache Kafka 4.2" — so the
  condition is currently invisible to anyone reading the docs.

  I am raising this in a transactions-phase review only because §9.7's warning and
  rules §12 both rest on it (Issue 12), and because a reviewer who checks that
  warning against the CLAUDE.md-designated Source Reference will reach the wrong
  conclusion. Fixing the corpus is not Phase 2 work.

- **Expected**: a tracked follow-up to reconcile `generator/messages/` with
  `kafka/` 4.2 (or, if the corpus is deliberately pinned to an earlier release,
  a recorded decision saying so and naming the version). Either way the four flag
  divergences should be named, since they are the ones with an observable accessor
  effect.

- **Actual**: undocumented, and contradicted by `design/current/design.md:261`.

---

## Issue 12: §9.7's warning and rules §12 present a repo-local flag value as a property of the Kafka spec, which hides Issue 11 and invites the regression the warning exists to prevent

- **File**: `design/history/Milestone-11/PLAN.md:1106-1114` (the ⚠ block added by
  `8264450`); `.claude/rules/producer-transactions.md:510-511`
- **Severity**: Bug (latent — a docs claim that, checked against the designated
  source reference, reads as false and licenses the wrong code change)
- **Java Reference**: `kafka/clients/src/main/resources/common/message/OffsetCommitRequest.json`,
  `OffsetFetchRequest.json` (neither sets `latestVersionUnstable`);
  `kafka/.../requests/OffsetCommitRequest.java:55`; `OffsetFetchRequest.java:64`

- **Description**:

  The new warning reads:

      Two of them — `OFFSET_COMMIT` and `OFFSET_FETCH` — carry
      `latestVersionUnstable: true` with `highest = 10`, so the accessors differ
      (10 vs 9). … They are correct today because **Java deliberately passes
      `latestVersion()`** (`OffsetCommitRequest.java:55`,
      `OffsetFetchRequest.java:64`), not because any flag is false.

  and rules §12:510-511 says:

      Only five APIs currently carry the flag (`OFFSET_COMMIT`, `OFFSET_FETCH`,
      `INIT_PRODUCER_ID`, `STREAMS_GROUP_HEARTBEAT`, `STREAMS_GROUP_DESCRIBE`)

  Both sentences are true of `generator/messages/` and **false of Apache Kafka
  4.2**, where only `INIT_PRODUCER_ID` carries the flag (Issue 11). Neither names
  the artifact it measured; both read as statements about the Kafka message spec,
  in a rules file and a plan whose entire subject is Java fidelity, and in a
  repository whose CLAUDE.md "Source Reference" section designates `kafka/`.

  The last clause is the sharpest one: "not because any flag is false" is correct
  about the Rust build and wrong about Java — in Java 4.2 the flag *is* false for
  both APIs, so `latestVersion()` and `latestVersion(false)` agree at 10 and it
  makes no difference which Java passes. The reason `latest_version()` is
  load-bearing at `offset_commit_request.rs:183` / `offset_fetch_request.rs:286` is
  the stale flag in this repo, nothing else.

- **Why it matters — the failure mode is the same one three passes running**:

  Issues 8, 9 and 10 were each found by opening the cited artifact and checking
  the value. A reviewer or Actor doing that here opens
  `kafka/clients/src/main/resources/common/message/OffsetCommitRequest.json`, finds
  no flag, and has every reason to conclude what pass 4 concluded about the
  *previous* justification: that the claim is false and the accessors agree after
  all. From there the natural action is the one the warning was written to
  prevent — treat all nine builders as inert and switch them — capping
  `OFFSET_COMMIT` / `OFFSET_FETCH` at 9 instead of 10. Issue 9's fix is therefore
  self-undermining in the hands of the reader most likely to verify it.

  Second effect: the warning is phrased as a permanent property of those two APIs.
  If `generator/messages/` is ever refreshed to 4.2 the flag becomes false, both
  accessors return 10, the hazard disappears — and nothing in §9.7 would signal
  that, because the warning does not say what it depends on.

- **Expected**: attribute the flag to the artifact it comes from and state the Java
  position, in both places. For §9.7, something like: "Two of them —
  `OFFSET_COMMIT` and `OFFSET_FETCH` — carry `latestVersionUnstable: true` in
  **this repo's spec corpus** (`generator/messages/OffsetCommitRequest.json:43`,
  `OffsetFetchRequest.json:45`), with `highest = 10`, so the Rust accessors differ
  (10 vs 9). Note Kafka 4.2 does **not** set the flag on either — see follow-up
  §9.N — so this hazard is repo-local; do not expect to find it in `kafka/`. They
  are correct today because Java passes `latestVersion()`
  (`OffsetCommitRequest.java:55`, `OffsetFetchRequest.java:64`) and Rust's
  `latest_version()` matches it at 10." §12:510-511 needs the same qualification.

- **Actual**: both stated as unqualified facts about the spec.

---

## Issue 13: the §9 heading reorder moved §9.7 and §9.8 out of §9 and under §10, so §9.7 is unreachable by reading §9

- **File**: `design/history/Milestone-11/PLAN.md:1097` (§9.7), `:1148` (§9.8)
- **Severity**: Missing Requirement (a tracked open follow-up is no longer
  reachable from the list that is supposed to track it)
- **Java Reference**: n/a (plan document)

- **Description**:

  Before `8264450`, §9.8 and §9.7 sat **inside** section 9, between §9.5 and §9.6 —
  out of numeric order, which is what pass 4 noted as organisation-only. The fix
  achieved ascending order by moving both blocks to the **end of the file**, after
  `### 10.4 Phase 1 deviations` at `:1082`. Section 9 now ends at `### 9.6` (`:999`)
  with a `---` / `## 10. Recorded translation deviations` boundary at `:1008-1010`,
  and the file reads:

      ## 9. Follow-ups (deferred work, tracked)
      ### 9.1 … ### 9.6                    ← §9 ends here (:999)
      ---
      ## 10. Recorded translation deviations   (:1010)
      ### 10.1 … ### 10.4                      (:1082)
      ### 9.7 Bring nine pre-existing RequestBuilders in line with rules §12  (:1097)
      ### 9.8 Critic review of Phase 2                                        (:1148)

  Both are `###` headings under `## 10`, so structurally they are now subsections
  of "Recorded translation deviations". The numbers ascend locally; the containment
  is broken.

- **Why it matters — this is the positioning question, one level up**:

  Within §9.7 the ⚠ block is well placed: immediately after the one-line status and
  the framing sentence, before the per-group reasons and the action table. Someone
  reading §9.7 top-to-bottom cannot miss it. But **§9.7 is now outside the section
  that lists it.** Rules §12:541 says "Bringing them in line is tracked as
  `design/history/Milestone-11/PLAN.md` §9.7", and §12:546 tells a Critic reviewing
  pre-existing code to "cite §9.7 instead" of raising nine findings. Anyone who
  follows that pointer by opening §9 — "Follow-ups (deferred work, tracked)" — and
  reading to its end stops at the `---` / `## 10` boundary, having seen §9.1
  through §9.6 and never reached §9.7. Of the four open follow-ups in §9 (§9.1,
  §9.3, §9.4, §9.7), it is the only one not reachable that way.

  Secondary: §10's preamble frames its contents as "every deviation from the Java
  source … justified. Gathered here so a reviewer has one list to check." §9.7 is
  an open work item and §9.8 is a review record; both now sit in that list and are
  mis-framed by it. §9.7 following `### 10.4`'s bullet list at the same heading
  level also reads visually as another deviation entry.

  No stale cross-reference and nothing dropped or duplicated: §9.1-§9.8 each appear
  exactly once, and the only external pointers (`.claude/rules/producer-transactions.md:541`
  and `:546`) still name a section that exists.

- **Expected**: place §9.7 and §9.8 after §9.6's content and **before** the `---` /
  `## 10` boundary at `:1008-1010`. That yields ascending order *and* keeps them
  inside §9, which is what the reorder was trying to achieve.

- **Actual**: appended after `### 10.4` at the end of the file.

---

## What I verified as correct

**Issue 10's fix is right.** `AddPartitionsToTxnRequest.java:58` is
`return new Builder(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), LAST_CLIENT_VERSION,`
— the bound expression appears literally on that line, matching how the three
sibling citations are cited. `:42` is the `LAST_CLIENT_VERSION` declaration, `:73`
the `super(apiKey, minVersion, maxVersion)` call, as the previous report said.

**Every other Java citation in §12 checks out**, re-derived line by line against
the current `kafka/` tree rather than against the previous report:

  - `AbstractRequest.java:46-51` — javadoc "any supported and *released* version"
    at `:46-48`; `Builder(ApiKeys)` → `this(apiKey, false)` at `:49-51`. ✓
  - `OffsetCommitRequest.java:55` — `ApiKeys.OFFSET_COMMIT.latestVersion()` inside
    `forTopicIdsOrNames`. ✓
  - `OffsetFetchRequest.java:64` — `ApiKeys.OFFSET_FETCH.latestVersion()`. ✓
  - `ApiVersionsRequest.java:43` — `ApiKeys.API_VERSIONS.latestVersion());`. ✓
  - `OffsetsForLeaderEpochRequest.java:57` —
    `new Builder((short) 3, ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), data)`. ✓

**§9.7's per-group claims are each true, checked per member — not asserted across
the set.**

  - *Faithful group* ("correct because Java's own bound is `latestVersion()`"):
    true for all four, at the four Java lines above. `offset_commit_request.rs` and
    `offset_fetch_request.rs` additionally model **both** Java factories —
    `for_topic_ids_or_names` uses `latest_version()` and `for_topic_names` caps at 9
    (`TOPIC_ID_MIN_VERSION - 1` for OffsetFetch), matching
    `OffsetCommitRequest.java:59-60` / `OffsetFetchRequest.java:67+`. So §9.7's
    "keep `latest_version()`, add the marker" is the right action and is not
    cementing a wrong bound.
  - *Implicated group* ("correct only because their APIs carry
    `latestVersionUnstable: false`"): true for all five, and in **both** corpora —
    `METADATA` 0-13, `FIND_COORDINATOR` 0-6, `SASL_HANDSHAKE` 0-1,
    `SASL_AUTHENTICATE` 0-2, `CONSUMER_GROUP_HEARTBEAT` 0-1, none setting the flag
    in either `generator/messages/` or `kafka/`. This group is unaffected by
    Issue 11.
  - All nine cited line numbers land on the right line:
    `offset_commit_request.rs:183`, `offset_fetch_request.rs:286`,
    `api_versions_request.rs:169`, `offsets_for_leader_epoch_request.rs:160`,
    `metadata_request.rs:191`, `find_coordinator_request.rs:192`,
    `sasl_handshake_request.rs:116`, `sasl_authenticate_request.rs:122`,
    `consumer_group_heartbeat_request.rs:138`.

**Could a reader still mechanically switch all nine and break
OffsetCommit/OffsetFetch?** Not by reading §9.7 — the ⚠ block, the per-group
bullets and the action table now agree, and the table's action for the faithful
group is "add the marker", not "switch". The two residual routes are Issue 12
(verify the flag against `kafka/`, find it absent, conclude the warning is wrong)
and Issue 13 (never reach §9.7 at all).

**Nothing regressed in the code, and there is no code to regress.**
`git diff --stat e8a91e7 HEAD -- '*.rs' 'Cargo.toml'` is empty; `8264450` touches
two `.md` files.

**The dispatch arms are still right.** All ten txn arms re-inspected individually
(`abstract_request.rs:448-467`, `abstract_response.rs:442-461`): each `ApiKeys`
value reads its own `*Data` / calls its own `*Response::parse` and wraps it in the
matching variant. No cross-wiring. `OFFSET_COMMIT` / `OFFSET_FETCH` neighbours
checked too, since Issue 11 touches those APIs — both correct.

**The deterministic-sort sites are intact and still behaviour-preserving.** Seven
`sort_unstable*` calls across the four files that group through a `HashMap`
(`add_partitions_to_txn_request.rs:238`, `add_partitions_to_txn_response.rs:181,187`,
`txn_offset_commit_request.rs:185,191`, `txn_offset_commit_response.rs:87,93`).
`build_txn_topic_collection` sorts topic names only, which is correct and not an
omission: Java's per-topic partition list is an `ArrayList` appended in the
caller's `List<TopicPartition>` iteration order
(`AddPartitionsToTxnRequest.java:78-90`), so it is already deterministic given the
input, and the Rust `Vec` push order matches it. Only the `HashMap` key iteration
was unspecified, and that is what is sorted.

---

## Suggested rule / CLAUDE.md updates

`COMMENTS.FP.md` and `COMMENTS.FN.md`: no `COMMENTS.FN.md` exists;
`COMMENTS.FP.md` contains nothing about this loop. Nothing to fold in from either.

One suggestion, arising from Issue 12 rather than from any false positive:

  - **CLAUDE.md "Source Reference"** currently names only `kafka/`. Add that the
    generated wire code is built from `generator/messages/`, a separate copy, and
    that a claim about a message spec must name which of the two it was measured
    against. Three of the five findings in this loop (Issues 9, 11, 12) are the
    same error — a spec-flag claim not tied to a verifiable artifact — and the
    per-corpus distinction is the missing piece that would have caught all three.
    Rules §12:503-514 is the natural place for the concrete version of this, since
    it is the rule that reasons about the flag.

---

# Pass 6 report (clean — closes the loop)

# Critic 42 — Milestone 11 Phase 2, sixth pass

Reviewed `e7442fe` (`fixup! Phase 2: address Critic 42 fifth-pass findings 11-13`).

**No findings.**

`git diff --stat e8a91e7 HEAD -- '*.rs'` is empty, and
`git diff --name-only e8a91e7 HEAD | grep -v '\.md$'` returns nothing — the commit is
documentation-only, as stated.

## Pass 5's three findings: all fixed correctly

**Issues 11 / 12 (corpus anchoring) — fixed, and every claim verified against the
corpus it now names.**

  - `grep -rl '"latestVersionUnstable": true' generator/messages/` → **5** files
    (`InitProducerIdRequest`, `OffsetCommitRequest`, `OffsetFetchRequest`,
    `StreamsGroupHeartbeatRequest`, `StreamsGroupDescribeRequest`).
    Same grep over `kafka/clients/src/main/resources/common/message/` → **1**
    (`InitProducerIdRequest.json`). Both counts as documented.
  - Generated table
    (`target/debug/build/confluent-kafka-rust-*/out/generated/api_message_type.rs`):
    `OFFSET_COMMIT`/`OFFSET_FETCH` highest = 10, `latest_version_unstable()` = true,
    and `highest_supported_version` returns `highest - 1` when the flag is set and
    unstable is disabled (`:621-625`) — so 10 vs 9, exactly as §9.7's warning says.
  - §9.7's cited sites are right: `offset_commit_request.rs:183` and
    `offset_fetch_request.rs:286` are both the
    `latest_allowed_version: ApiKeys::X.latest_version(),` line.
  - §9.7's parenthetical "(`METADATA`, `FIND_COORDINATOR`, `SASL_HANDSHAKE`,
    `SASL_AUTHENTICATE` and `CONSUMER_GROUP_HEARTBEAT` set no flag at all, which
    defaults to false)" — verified absent in **both** corpora for all five. This is a
    presence-vs-value claim stated correctly.
  - §12:515-517's "the other four had their latest versions released by 4.2" holds:
    for all four, `validVersions` is byte-identical between the corpora, so the only
    4.2 change was dropping the flag.

**§9.9 — every number checked and correct.** 197 specs in each tree; `diff -rq`
reports **36** differing files (plus `README.md` present only in `kafka/`); exactly
**2** of the 36 differ by a single line, and in both that line is
`"latestVersionUnstable": true` (`OffsetCommitRequest.json` `43d42`,
`OffsetFetchRequest.json` `45d44`); `ListOffsetsRequest.json` is `1-10` vs `1-11`;
`build.rs:44` is the
`generator::generate_messages(Path::new("generator/messages"), …)` call;
`git log --oneline -- generator/messages/` returns exactly one commit,
`6cd275c Initial branch (#1)`.

**§9.9's "no known live defect" is sound, including for the Streams APIs.** I swept
every consumer of the flag rather than only the txn builders:

  - `latest_version_unstable()` — no caller outside the generated file.
  - `latest_version_with_unstable(false)` in production — only the five Phase 2 txn
    builders (`init_producer_id_request.rs:141`, `end_txn_request.rs:151`,
    `add_offsets_to_txn_request.rs:114`, `txn_offset_commit_request.rs:319`, `:360`).
    All five txn request specs and all five response specs are byte-identical
    between the corpora except one comment typo in `InitProducerIdRequest.json:29`
    (`Verison` → `Version`), and its flag is `true` in both. So Phase 2's generated
    code does match 4.2.
  - The other two paths that reach the flag —
    `ApiKeys::is_version_enabled(v, enable_unstable)` (`api_keys.rs:290`) and
    `to_api_version_internal` (`:347`) — are reached only from
    `filter_apis` / `collect_apis` / `intersect_forwardable_apis` /
    `default_api_versions_response*` in `api_versions_response.rs`. Every caller in
    the tree passes `true`; `collect_apis` has no caller; `is_version_enabled` has no
    production caller (two `API_VERSIONS` asserts only, which short-circuit at
    `:291`).
  - `NodeApiVersions` (`node_api_versions.rs:57`, `:326`) goes through
    `ApiVersionsResponse::to_api_version`, which uses `latest_version()` — the
    unstable-**inclusive** accessor — matching Java's
    `ApiVersionsResponse.toApiVersion(ApiKeys)`. Unaffected by the flag.
  - Streams specifically: `STREAMS_GROUP_HEARTBEAT` / `STREAMS_GROUP_DESCRIBE` have
    highest = 0 with identical `validVersions` in both corpora, so `latest_version()`
    returns 0 = Java. Only `with_unstable(false)` would yield -1, and nothing calls
    it for them. The assertion holds for Streams as it does for
    OffsetCommit/OffsetFetch.

**Issue 13 (structure) — fixed.** §9.1 … §9.9 are all `###` headings inside
`## 9. Follow-ups`, ascending, with `## 10. Recorded translation deviations` at
`:1164` after them. Every one of `### 9.1`–`### 9.9` and `### 10.1`–`### 10.4`
appears exactly once. Diffing §9.7's body between `8264450` and HEAD shows only the
intended content edits — nothing truncated or lost in the move. §9.8's body is
complete.

**Every `§9.x` cross-reference resolves and says what the citing text implies** —
`PLAN.md` `:1033`, `:1082-1083`, `:1091`, `:1094`, `:1102`, `:1104`, `:1107`,
`:1117`, `:1126`, `:1145`, `:1146`, `:1153`, `:1158-1159`; and
`.claude/rules/producer-transactions.md:472` (§9.1), `:518` (§9.9), `:551` and `:556`
(§9.7). No dangling numbers.

**§9.8's corrected status is accurate and does not overclaim.** "Five Critic 42
passes; none has yet returned zero findings" matches the archive (5 / 2 / 1 / 2 / 3);
every fix SHA in the table is the right commit; "every round after the first found a
defect in a **fix**, never in the translation" holds for passes 2-5; and the
"declared closed without a clean pass" admission matches `e8a91e7`'s own commit
message ("close the Phase 2 Critic loop after a clean third pass").

## Phase 2's code is untouched and intact

Ten txn wrapper files present; the five txn `ApiKeys` arms in each of
`abstract_request.rs` and `abstract_response.rs` unchanged; all seven
`sort_unstable*` sites still in place
(`add_partitions_to_txn_request.rs:238`, `add_partitions_to_txn_response.rs:181,187`,
`txn_offset_commit_request.rs:185,191`, `txn_offset_commit_response.rs:87,93`).

## Considered and deliberately not raised

Recorded so a future reader knows these were looked at, not missed:

  1. §9.8's table row for pass 5 shows its fix as `pending`, but the commit writing
     that row **is** the fix. A commit cannot cite its own SHA, and the section body
     immediately below describes §9.9 as added, so the fix state is unambiguous in
     context. Not a defect.
  2. §9.8's "common root" sentence attributes all three flag-related findings to the
     two-corpus disagreement. Strictly that root applies only to pass 5's; pass 2's
     was presence-vs-value (provably corpus-independent — the four txn specs involved
     are byte-identical across both trees) and pass 4's was a value asserted across a
     set without per-member checking. But this is the Critic's own pass-5 framing
     ("the per-corpus distinction is the missing piece that would have caught all
     three") implemented faithfully, the operative guard is §12's "check the Java
     `super(...)` call, not the spec flag", and nothing actionable turns on the
     attribution.
  3. "Phase 2's `src/` has been clean since pass 1" is supported by the pass-2/3
     verdicts rather than by the `e8a91e7..HEAD` diff it cites; pass 2's three
     production-line changes were provable no-ops. Reads correctly in context.

## Suggested rule / CLAUDE.md updates

`COMMENTS.FP.md` contains nothing about this loop; no `COMMENTS.FN.md` exists.
Nothing to fold in. Pass 5's CLAUDE.md "Source Reference" suggestion is already
recorded in `COMMENTS.DONE.42.md` for the `agent-roles.md` §2 process; I have no
further suggestion.

## Sign-off

Clean pass — **no findings**. Phase 2 can close on this.
