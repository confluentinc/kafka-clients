# M15 / P5 — Groups & group offsets (.NET binding)

> **Status:** ✅ **APPROVED 2026-09-17 — IMPLEMENTATION AUTHORIZED.**
> All four escalated design items were **ruled on 2026-09-17** and are applied
> throughout (§0.1.1). **No design question is open**, and the plan itself is
> approved as written. See §12.
> **Agent number:** **N = 75** (single number; see §0.2)
> **Mode:** **A** — purely additive C# binding work. Zero Rust-core change, zero
> C-ABI change, zero generated-header change. Re-verified against the **expanded
> fidelity-first surface** mandated by directive #4 (§0.1) — see §0.3.
> **Branch:** work continues on `prashah_dev_dotnet_binding`. Do **NOT** create a
> branch, do **NOT** merge, do **NOT** rebase, do **NOT** squash.
> **Supersedes:** the two-plan packaging `P5a-groups-listing-and-describing/PLAN.md`
> and `P5b-group-offsets-and-members/PLAN.md`. Both directories are deleted; this
> file is the only P5 plan on disk.

---

## §0 — Packaging, ledger, and mode

### §0.1 The five binding constraints this plan implements

A maintainer review rejected the earlier P5a/P5b two-plan packaging while
**accepting its substantive findings**. The constraints, restated here so they
are not re-litigated mid-phase:

1. **P5 ships as ONE phase.** One directory, one agent number, one plan —
   matching the P3 and P4 precedent. The old D21 ("split P5a/P5b") is **moot and
   withdrawn**; it is not renumbered into the decision list below, it is gone.
2. **No internal sub-stages either.** P3 used "three internally-green stages",
   P4 used two. That is **not** repeated here: P5 is not subdivided, not handed
   off at an internal green point, and not partially accepted.
   ⚠ **AMENDED 2026-09-17 — read §3.A.** The original text here required the
   Actor to implement all nine RPCs as *one continuous pass*, and called that
   strictly stronger than #1. After three measured context-exhaustion crashes the
   maintainer **relaxed exactly that clause**: the implementation may now span
   multiple **resumable checkpoint sessions**. Everything else in this list is
   unchanged — #1's one-phase/one-number rule and #3's single-Critic-pass rule
   both stand, and a checkpoint triggers no review.
3. **The Critic runs exactly once, after the Actor has completely finished.**
   Never interim, never per-RPC, never per-file. See §3.
4. **Java-shape completeness governs every ship-vs-omit call.** Verbatim: *"we
   need to maintain Java shape and have all relevant APIs that are currently
   there in Java."* If Java has it and it is reachable through the nine in-scope
   RPCs' result / option / payload types, **represent it**. Genuinely
   unreachable surface stays out of scope — but *simplicity alone is never a
   reason to omit*. This directive is what resolves D24 and D25 below, and it is
   why the surface in §4 is materially larger than the earlier drafts proposed.
5. **Keep the substance, drop the packaging.** The mechanism-seam finding, the
   throw-vs-defer analysis, the stored-vs-published trap, and the
   zero-mock-coverage risk all survive into this plan (§1, §2, §6.0). The
   decisions are consolidated and renumbered into one section (§2).

### §0.1.1 Maintainer rulings, 2026-09-17 — all four open items closed

The consolidated plan escalated four items. All four are now ruled. **No design
question in this plan is open**; what remains is plan *approval* itself (§12).

| # | Item | Ruling | Where applied |
|---|---|---|---|
| 1 | `GroupState.groupStatesForType` | **SHIP IT**, duplicating Java's hardcoded lookup tables in C#. Recorded as a **deliberate, named exception** to `bindings/CLAUDE.md` §2.6 — a conscious tradeoff of the "no logic in the binding" rule for Java-surface fidelity, **not** a silent violation. | **D33**; §4.1; §6.2 items 13–14; §7.8 |
| 2 | Deprecated `inStates` / `states()` options projection | **APPROVED** — replicate **exactly** the lossy conversion Java performs internally. A sanctioned, **named exception to D21**, documented at the site. | **D34**; §4.2; §6.2 item 10 |
| 3 | The nine deprecated constructors | **SKIP THEM.** Return-only objects users never construct — dead surface not worth the fidelity cost. Ship live public ctors + `internal` native-data ctors. | **D25.1**; §4.3 |
| 4 | The shape-4 two-list callable | **DELEGATED** to the Actor/Critic as a normal implementation decision during the round. No pre-commitment: internal code shape, no public-API impact. | **D22** |

Rulings 1 and 2 **reverse** the withdrawn drafts' omission lean; ruling 3
confirms it; ruling 4 removes a pre-commitment. Ruling 1 is the one with
downstream reach — it adds surface, tests, and a DoD item, and it corrected a
factual error in the escalation itself (the method has **four** sets and a throw,
not five sets — see D33).

### §0.2 Agent number — ledger recomputed from the filesystem, not from the roadmap

The roadmap's §8.1 ledger (`design/current/PLAN-M15-admin-client.md`) is **stale**.
It reads `71→P5, 72→P6, 73→P7, 74→P8, 75→P9`, but 71–74 were consumed by
sibling-branch work that landed after that ledger was written. Evidence, gathered
by unfiltered enumeration with control-positives rather than by reading the
roadmap:

| N | Actually consumed by | Evidence |
|---|---|---|
| 69 | M15 / P3 | `bindings/dotnet/COMMENTS.69.md` (unarchived, at binding root) |
| 70 | M15 / P4 | `bindings/dotnet/COMMENTS.70.md` (unarchived, at binding root) |
| 71 | M11 / P3.2 producer-send-ordering-parity | archived under `design/history/M11/` |
| 72 | M11 / P3.3 producer-send-admission-bound | archived |
| 73 | M11 / P3.4 producer-append-first-admission | archived |
| 74 | M16 / P1 soak-client | archived; `COMMENTS.74.md` + `COMMENTS.DONE.74.md` both present (control-positive: 2 files) |
| **75** | **FREE — claimed by this phase** | repo-wide `find -name 'COMMENTS*.md'` returns **no 75 and no 76 anywhere** |

Cross-space check: the repo-root (Rust-side) maximum is 64 and `design/history`'s
maximum is 48, so 75 is free in both numbering spaces.

**Therefore: P5 = 75, P6 = 76, P7 = 77, P8 = 78, P9 = 79.** The maintainer's
proposed arithmetic is confirmed by independent recomputation. Because P5 is one
phase and not two, it consumes exactly one number and nothing downstream shifts
beyond the single step.

> **Method note — a measurement error worth recording.** The withdrawn P5a draft
> justified its number with *"0 occurrences of `N = 75` in tracked STATUS.md
> against control-positive 1 for `N = 70`"*. That grep could not have matched
> anything: STATUS.md writes `N=73` with no spaces, so **both** the claim and its
> own control were vacuous. Re-run with the real format (`N=<n>`, word-bounded):
> N=70→1, N=71→1, N=72→1, N=73→1, **N=74→0**, N=75–80→0. Since 74 is
> demonstrably consumed yet absent from STATUS.md, **STATUS.md is not a complete
> ledger** and filesystem enumeration is the authoritative source. This is the
> COUNT DISCIPLINE rule biting a plan author, not a Critic — see §10 trap 13.

**Roadmap correction — ✅ ALREADY DONE (verified by the Manager 2026-09-17, before
the Actor was spawned).** `bindings/dotnet/design/current/PLAN-M15-admin-client.md`
§8.1 now carries the corrected ledger on disk (header *"CORRECTED 2026-09-17"*,
rows `66→P1 … 70→P4, 71–74 → M11/M16 sibling work, 75→M15/P5, 76→P6, 77→P7,
78→P8, 79→P9`), plus the "re-derive from `find -name 'COMMENTS*.md'`, never from
this table alone" warning and the P5-ships-as-one-phase ruling at `:847-854`.
**The Actor does NOT need to redo this.** The file is untracked **but not
gitignored** — do **NOT** `git add` it. (Note the path: it is
`bindings/dotnet/design/current/...`, not repo-root `design/current/...`.)

### §0.3 Mode A re-verified against the *expanded* surface

Directive #4 enlarges the surface well beyond what the withdrawn drafts proposed.
Mode A is therefore re-verified, not inherited. Every Java accessor across the
three richest payload types maps to an already-exported C entry point:

| Java type | Java accessors | All exported? | Spot-check cites (`target/include/confluent_kafka.h`) |
|---|---:|---|---|
| `ConsumerGroupDescription` | 11 | ✅ 11/11 | `group_id` :5640, `group_type` :5696, **`state` :5708**, `group_state` :5719, `coordinator` :5730, `group_epoch` :5781, `target_assignment_epoch` :5794 |
| `ClassicGroupDescription` | 8 | ✅ 8/8 | `protocol` :5815, `protocol_data` :5826, `state` :5868, `authorized_operation` :5916 |
| `MemberDescription` | 9 | ✅ 9/9 | `consumer_id` :5535, `rack_id` :5558, `assignment` :5590, `target_assignment` :5603, `member_epoch` :5615, `upgraded` :5629 |
| `GroupListing` | 5 | ✅ 5/5 | `group_id` :5384, `group_type` :5399, `protocol` :5410, `group_state` :5424, `is_simple_consumer_group` :5435 |
| `ConsumerGroupListing` | 5 | ✅ 5/5 | `group_id` :5445, `group_state` :5466, **`state` :5481**, `group_type` :5492 |

All nine RPCs have both a sync and an async declaration (`sync_decl=1
async_decl=1` for each). Control-positive: `create_topics` → 1/1.
Control-negative: `describe_share_groups` → **0/0** (no such export; see D24).

**The decisive Mode-A finding, and the one that settles D25.** The C ABI *already
exports the deprecated axis on exactly the types where Java deprecates it, and
nowhere else*:

- `ConsumerGroupListing` exports **both** `_group_state` (:5466) **and** `_state`
  (:5481) — Java has both, `state()` deprecated at `:141`.
- `GroupListing` exports **only** `_group_state` (:5424), **no** `_state` — and
  Java's `GroupListing` correspondingly has **no** `state()` axis at all.
- `ConsumerGroupDescription` exports **both** `group_state` (:5719) and `state`
  (:5708) — Java has both, `state()` deprecated at `:189`.
- `ClassicGroupDescription` exports **only** `state` (:5868) — matching Java,
  which has `state()` and no `groupState()`.

The Rust core already made the fidelity-first choice, per type, with no
exceptions. A C# binding that omitted the deprecated axis would **discard
information the ABI deliberately carries**. That is not a simplification; it is a
lossy binding.

### §0.4 Scope — the nine RPCs

`listGroups`, `listConsumerGroups`, `describeConsumerGroups`,
`describeClassicGroups`, `listConsumerGroupOffsets`,
`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`,
`deleteConsumerGroups`, `removeMembersFromConsumerGroup`.

Fifteen net-new C# types; six existing types reused. Measured:

**Reused (already in `src/Confluent.Kafka/`):** `TopicPartition`, `Node`, `Uuid`,
`OffsetAndMetadata`, `OffsetSpec`, `AclOperation`.

**Net-new (all absent today; control-negative `ZZZControlNegative` → ABSENT
confirms the sweep discriminates):** `MemberAssignment`, `MemberDescription`,
`GroupListing`, `ConsumerGroupListing`, `ConsumerGroupDescription`,
`ClassicGroupDescription`, `GroupType`, `GroupState`, `ConsumerGroupState`,
`ListGroupsOptions`, `ListConsumerGroupsOptions`, `DescribeConsumerGroupsOptions`,
`ListConsumerGroupOffsetsSpec`, `RemoveMembersFromConsumerGroupOptions`,
`MemberToRemove` — plus the nine `*Result` types and the remaining `*Options`
types.

---

## §1 — The ABI read

### §1.0 The nine RPCs, and what each one actually publishes

The table's rightmost column is the phase. **The accessor set names one shape;
Java publishes another — for seven of the nine.**

| RPC | C accessor set | Shape the accessors *name* | What Java's result type *publishes* |
|---|---|---|---|
| `listGroups` | `valid_count`, `get_valid`, `error_count`, `get_error`, `destroy` | **shape 4** | `valid()`, `errors()`, `all()` — two **independent, non-parallel** lists |
| `listConsumerGroups` | same as above | **shape 4** | same, over the deprecated `ConsumerGroupListing` |
| `describeConsumerGroups` | `count`, `get_group_id`, `get_value`, `get_error`, `destroy` | shape 1 | genuine shape 1: `describedGroups()` map + `all()` |
| `describeClassicGroups` | `count`, `get_group_id`, `get_value`, `get_error`, `destroy` | shape 1 | genuine shape 1 |
| `listConsumerGroupOffsets` | `count`, `get_group_id`, `get_error`, `get_value`, `destroy` | shape 1 | **NOT a map.** Zero-arg `partitionsToOffsetAndMetadata()`, keyed overload, `all()` |
| `alterConsumerGroupOffsets` | `count`, `get_topic`, `get_partition`, `get_error`, `destroy` | shape 2 | **one aggregate future**; `partitionResult(tp)` **mints** per call |
| `deleteConsumerGroupOffsets` | `count`, `get_topic`, `get_partition`, `get_error`, `destroy` | shape 2 | one aggregate future + a **retained request set**; `partitionResult` **throws** |
| `deleteConsumerGroups` | `count`, `get_group_id`, `get_error`, `destroy` | shape 6 | genuine shape 6 |
| `removeMembersFromConsumerGroup` | `count`, `get_group_instance_id`, `get_error`, `destroy` | shape 2-ish | one aggregate future; `memberResult` **mints**, with **two** distinct throws |

> ⚠ **The `alter` and `delete` group-offsets accessor sets are byte-identical to
> each other AND to P4's `AlterPartitionReassignmentsResult`.** Three RPCs, one
> accessor signature, three different Java shapes. This is the phase's central
> hazard and the reason §1.4 exists.

### §1.1 Headline A — shape 4's error list is **not parallel** to the listing list

`listGroups` and `listConsumerGroups` expose two independently-indexed lists.
From `confluent_kafka.h:6051-6059`, verbatim:

> *"**This list is not parallel to the listings.** … Index this list with
> `…_error_count`, never with `…_valid_count`. A non-empty error list plus a
> non-empty listing list is Java's normal partial-success outcome, which is
> exactly what `all()` would have thrown on."*

Consequences the Actor must honour:

- Two separate walk loops, each bounded by its **own** count. A single loop
  bounded by `valid_count` reading both is a silent truncation bug that passes
  every round-trip test.
- `all()` is **not** `valid()`. `all()` fails if `errors()` is non-empty;
  `valid()` returns the partial list regardless. Both must exist (directive #4).
- Requires one net-new `KeyedResultMarshal` callable — see §1.5.

### §1.2 Headline B — the minting trio: throw-vs-defer is decided by **what the constructor retained**

Three result types store **one aggregate future** and **mint a fresh per-key
future on every call**. Whether the accessor can validate its argument depends
entirely on whether the Java constructor kept the request:

| Java type | Stored field | Ctor takes | Can validate? | Throws |
|---|---|---|---|---|
| `AlterConsumerGroupOffsetsResult` | aggregate future `:33` | future only `:35` | **No** — nothing to check against | none; mints at `:42-43` |
| `DeleteConsumerGroupOffsetsResult` | aggregate future `:31` | future **+ `Set<TopicPartition>`** `:35` | **Yes** | `IllegalArgumentException` `:45` — *"Partition … was not included in the original request"*; mints `:47` |
| `RemoveMembersFromConsumerGroupResult` | aggregate future `:35` | future + members `:38` | **Yes** | **two** — `:83` removeAll-mode: *"The method: memberResult is not applicable in 'removeAll' mode"*; `:86` not-requested; mints `:89` |

Consequences:

- Publish the **method** form (`PartitionResult(tp)`, `MemberResult(m)`), **not**
  `IReadOnlyDictionary`. A dictionary cannot express "mints on demand" or "throws
  for an unrequested key", and would force materialising a key set that
  `AlterConsumerGroupOffsetsResult` provably does not have.
- **D18 does not apply.** D18 governs *unforced zero-argument getters* on admin
  value types. These take an argument; they are methods in Java and stay methods.
- Mint **fresh per call** (D27). Do not cache. Java does not; caching changes
  reference identity, which `Task`-returning APIs make observable.
- Three tests asserting the **exact** message strings (DoD §3).

### §1.3 Headline C — `ListConsumerGroupOffsetsResult` stores a map it never publishes

`ListConsumerGroupOffsetsResult` stores the shape-1
`Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>>` at `:36-38` —
and that field is **package-private and never published**. The public surface is:

- zero-arg `partitionsToOffsetAndMetadata()` `:49` — throws
  `IllegalStateException` `:51` when there is **more than one** group (not when
  there are zero);
- keyed `partitionsToOffsetAndMetadata(String groupId)` `:62` — throws
  `IllegalArgumentException` `:64`;
- `all()` `:72-73`.

This is the **second sighting** of the `createTopics` trap, which makes it a
family of two and promotes it to a rule (§9):

> **For a ROUTING decision, read the STORED FIELD — never the public accessor.**

Note the older detection heuristic — grepping the Java result type for
`thenApply` — is **structurally blind** to this variant: there is no `thenApply`,
there is a private field plus hand-written accessors. The sweep in §6.3 replaces
it.

### §1.4 The mechanism seam — accessor-set identity is evidence of *nothing*

`bindings/dotnet/CLAUDE.md` §1.1.1 already states the principle: *"The header
defines the mechanics, Java defines the shape"*, and *"accessor-set identity is
not evidence in either direction."* P5 is the phase that proves it three ways at
once (`alter`, `delete`, and P4's `AlterPartitionReassignments` share one
signature and have three shapes).

**The seam is a real finding and it survives the merge.** What did *not* survive
is the conclusion that it warrants a phase boundary. The mitigation is
procedural, not structural:

- For **every** one of the nine RPCs, the Actor writes a one-line shape
  justification in the commit body citing the **Java file and line of the stored
  field**, not the accessor set. Nine such lines, nine cites.
- The Critic's single terminal pass (§3) verifies all nine cites against
  `kafka/clients/src/main/java/org/apache/kafka/clients/admin/`.
- The wiring guard (§6.3, finding 70.12) is the mechanical backstop: sibling
  readers built from a shared factory are layout-compatible, so a mis-wired
  reader **returns a plausible answer**. Every reader added in this phase gets an
  `[InlineData]` row plus a guard entry **in the same commit**, and the Actor
  must confirm a deliberate cross-wire injection goes RED before committing.

### §1.5 Walker seam — exactly one net-new callable

`KeyedResultMarshal.cs` is **484 lines** (freshly measured; P4's plan cites
`:211/:283/:326/:398` and those line numbers are **stale — do not trust them**).
Current callables:

| Callable | Line |
|---|---:|
| `Complete<TKey, TValue>` | **:269** |
| `Complete<TKey>` | **:341** |
| `CompleteAggregate<TKey, TValue>` | **:397** |
| `CompleteList<TValue>` | **:469** |

Delegates: `internal delegate int CountAccessor(IntPtr result);` :177;
`internal delegate IntPtr IndexedAccessor(IntPtr result, int index);` :184.

**Net-new: one callable** for headline A — a two-list completer that walks a
valid-list and an independently-counted error-list and completes
`valid` / `errors` / `all` from them. Everything else in P5 reuses an existing
callable, including the composite `(topic, partition)` reader from P4 — which is
precisely why the wiring guard is mandatory here (§6.3).

---

## §2 — Design decisions (consolidated)

### §2.0 Renumbering map

Directive #5 asked for one decisions section with renumbering where cleaner. D21
(the split) is **withdrawn as moot** and is not reassigned. The remainder
renumber contiguously from the last ruled decision (D20), and two new decisions
appear that neither withdrawn draft recorded.

| Old | New | Topic |
|---|---|---|
| D21 | — | *withdrawn: P5a/P5b split. Moot.* |
| D22 | **D21** | Dual state axes — read both, derive neither |
| D23 | **D22** | The shape-4 two-list callable |
| D24 | **D23** | Namespace placement |
| D25 | **D24** | `GroupType` Share/Streams reach |
| D26 | **D25** | Deprecated surfaces |
| D27 | **D26** | Shared `MemberDescription` across two describe RPCs |
| D28 | **D27** | Minting trio published as methods |
| D29 | **D28** | `ListConsumerGroupOffsetsSpec` null = ALL partitions |
| D30 | **D29** | `RemoveMembersFromConsumerGroupOptions`' four convention breaks |
| D31 | **D30** | `OffsetAndMetadata` tri-state |
| D32 | **D31** | Degenerate inputs → whole-request errors |
| D33 | **D32** | Ragged 2-level input encoding |
| — | **D33** | *(new)* `GroupState.groupStatesForType` — **RULED 2026-09-17: SHIP** |
| — | **D34** | *(new)* Deprecated **options** projection vs D21 — **RULED 2026-09-17: APPROVED** |

### §2.1 Status summary — nothing is open

| | Decisions |
|---|---|
| **Resolved by directive #4 + evidence** | **D24**, **D25** |
| **Resolved by shipped in-repo precedent** | **D25**'s mechanism (see below), **D27**, **D30** |
| **Settled by ABI/Java reading, recorded for the record** | D21, D23, D26, D28, D29, D31, D32 |
| **Resolved by maintainer ruling, 2026-09-17** (§0.1.1) | **D33** (ship), **D34** (approved), the **deprecated-constructor** sub-question inside **D25** (skip) |
| **Delegated to the Actor/Critic during the round** | **D22** — internal code shape only, no public-API impact |
| **STILL NEEDING A RULING** | **None.** Every decision in this section is closed. |

---

**D21 — dual state axes: read both, derive neither.**
Where a type has both `GroupState` and the deprecated `ConsumerGroupState`, the
binding reads **both** from their own ABI entry points and derives neither from
the other. `GroupState` has **9** constants (it adds `NOT_READY("NotReady")`);
`ConsumerGroupState` has **8**. Deriving would need a lossy mapping, and the
ABI exports both axes precisely so the binding does not have to guess. Note
`GroupState.parse` **upper-cases with no null guard**, whereas `GroupType.parse`
is **null-safe** (returns `UNKNOWN`) and lower-cases — mirror each exactly; the
asymmetry is Java's, not a typo. `ConsumerGroupState.parse` returns `UNKNOWN` on
a miss.
*(Re-confirmed under directive #4; unchanged from the withdrawn draft.)*

**Scope — D21 governs READERS.** It applies where the binding reads a value the
ABI already exports on both axes. It does **not** govern
`ListConsumerGroupsOptions`' deprecated **writer/getter** pair, where Java itself
stores one field and derives the other: that is **D34**, a maintainer-ruled,
sanctioned exception (2026-09-17). Read D21 and D34 together before filing a
derivation as a violation.

**D22 — the shape-4 two-list callable: DELEGATED to the Actor/Critic
(maintainer ruling, 2026-09-17).** Where the callable lives, whether it is a new
member of `KeyedResultMarshal` or an extension of the existing
`CompleteList<TValue>` (:469), and whether it reuses or rescans — all of it is
internal code shape with **no public-API impact**, and is settled during the
round rather than pre-committed here. The earlier draft's pre-commitment
("a new callable, never an overload") is **withdrawn**.

**One invariant is NOT delegated**, because it is correctness rather than shape:
a shape-4 result's two lists are **independent and non-parallel** (§1.1), so each
must be walked by **its own count**. Any signature that permits one count to
serve both lists reintroduces the truncation bug — and an overload in particular
would resolve to it silently. The Actor may pick any shape that makes that
mis-call unrepresentable; §6.2 item 1 is the test that pins it either way.

**D23 — namespace.** Group types live in `Confluent.Kafka.Admin`, matching Java's
`org.apache.kafka.clients.admin`. The three enums `GroupType`, `GroupState`,
`ConsumerGroupState` live in `Confluent.Kafka` (root), matching Java's
`org.apache.kafka.common`. Existing precedent: `AclOperation.cs` and `Node.cs`
are at the root; `OffsetSpec.cs` is under `Admin/`.

**D24 — `GroupType` Share/Streams: SHIP. RESOLVED by directive #4.**
Ship all five `GroupType` constants (`UNKNOWN`, `CONSUMER`, `CLASSIC`, `SHARE`,
`STREAMS`) **and** all three `ListGroupsOptions` static factories, read verbatim
from `ListGroupsOptions.java`:

- `forConsumerGroups()` `:38` → `.withTypes(Set.of(CLASSIC, CONSUMER)).withProtocolTypes(Set.of("", ConsumerProtocol.PROTOCOL_TYPE))`
- `forShareGroups()` `:48` → `.withTypes(Set.of(SHARE))`
- `forStreamsGroups()` `:57` → `.withTypes(Set.of(STREAMS))`

All three are `public static` on an **in-scope** options type and all three are
expressible through the ABI's `types` filter array
(`kafka_admin_AdminClient_list_groups` :6549 takes `types, type_count`). Under
directive #4 they are reachable Java surface and they ship.

**The scope boundary is nonetheless genuine:** the control-negative
`describe_share_groups` returns **0/0** — there is no share-group *describe*
export to bind. Filtering *for* share groups is in scope; *describing* them is
not, because the ABI does not carry it. That is an unreachability boundary, not a
simplification.

**D25 — deprecated surfaces: SHIP THEM, with `[Obsolete]`. RESOLVED — and the
mechanism is already shipped in this repo.**

Two independent lines of evidence:

*(a) The ABI already made this choice* (§0.3): the deprecated `state` axis is
exported on exactly the two types Java deprecates it on, and omitted on exactly
the two it doesn't. Omitting it in C# discards carried information.

*(b) P3 already shipped this exact pattern, under `TreatWarningsAsErrors=true`.*
Java deprecates `listClientMetricsResources` plus its listing, options, and
result types; the binding mirrored **all four** with message-form `[Obsolete]`:

- `Admin/IAdmin.cs:327` (the RPC), `Admin/ClientMetricsResourceListing.cs:41`,
  `Admin/ListClientMetricsResourcesOptions.cs:33`,
  `Admin/ListClientMetricsResourcesResult.cs:43`, plus the two implementations
  `Admin/KafkaAdminClient.cs:89` and `Admin/MockAdminClient.cs:100`.
- Message idiom: `"Deprecated in Kafka since <ver>. Use <replacement> instead."`
- `error: false` (single-argument form) throughout — the surface stays callable.

`Directory.Build.props:17` sets `TreatWarningsAsErrors=true`, so the binding's own
internal use of its own obsolete surface must be suppressed. P3 established the
idiom: **narrow** `#pragma warning disable CS0618` / `restore` pairs carrying a
justification comment — 3 sites in `src` (`Internal/NativeAdminClient.cs:1277`,
`Internal/Interop/AdminCallbacks.cs:720` and `:1540`) and 5 in `tests`. The
comment text to copy is literally *"Java deprecates X; mirrored, not avoided."*
For a test that asserts the deprecation itself, P3 writes *"the deprecation is
what is asserted"* (`PublicAdminP3ShapeParityTests.cs:229`).

P5's deprecated set, each with its Java cite:

| Surface | Java cite | Deprecation |
|---|---|---|
| `Admin.listConsumerGroups(options)` | `Admin.java:890` | `:889` since 4.1, forRemoval |
| `Admin.listConsumerGroups()` default | `:902` | `:901` since 4.1, forRemoval |
| `ConsumerGroupListing` (whole class) | `:32` | since 4.1, forRemoval |
| `ListConsumerGroupsOptions` (whole class) | `:33` | since 4.1, **no forRemoval**; `@SuppressWarnings("removal")` `:34` |
| `ConsumerGroupState` (whole class) | `:30` | since 4.0, forRemoval |
| `ConsumerGroupDescription.state()` | `:190` | `:189` — **member on a live class** |
| `ConsumerGroupListing.state()` | `:142` | `:141` |
| `ListConsumerGroupsOptions.inStates(...)` | `:57` | deprecated |
| `ListConsumerGroupsOptions.states()` | `:85` | deprecated |

Mirror the `since` version and the `forRemoval` distinction in the message text.
`ListConsumerGroupsOptions` is deprecated **without** `forRemoval`, which is a
different promise from the rest — say so in its message rather than flattening
all nine into one sentence.

#### D25.1 — Deprecated **constructors**: SKIP THEM. **RULED 2026-09-17.**

Directive #4 resolves the deprecated *accessors* (above). Java additionally
publishes **deprecated constructors** on three payload types:
`MemberDescription` ×4 (`:66` since 4.2; `:93`/`:117`/`:138` since 4.0) plus 1
live ctor `:39`; `ConsumerGroupDescription` ×3 (`:54`/`:68`/`:83`) plus 1 live
`:103`; `ConsumerGroupListing` ×2 (`:58`/`:72`) plus 3 live
(`:45`/`:88`/`:104`).

**The maintainer ruled these nine are NOT shipped**, agreeing with the Manager's
lean: *"these are return-only objects, never constructed by binding users, so the
deprecated constructors are dead surface not worth the fidelity cost."*

**What ships, concretely:**

- **Public** constructors mirroring Java's **live** (non-deprecated) ctors:
  `MemberDescription` `:39`, `ConsumerGroupDescription` `:103`,
  `ConsumerGroupListing` `:45`, `:88`, `:104`.
- **`internal`** constructors for the native-handle construction paths the reader
  uses. This is exactly the `ConfigEntry.cs` split precedent — **public** `:88`
  for Java's public ctor, **internal** `:121` / `:150` for the richer native
  paths.
- **Nothing** for the nine deprecated ctors: no `[Obsolete]` public overload, and
  therefore no `#pragma warning disable CS0618` at an internal construction site
  for them.

**Scope of this ruling — read it narrowly.** It closes the *constructor*
question only. It does **not** narrow D25 itself: every deprecated **accessor**,
**option method**, **result type**, **listing type**, and the deprecated
`listConsumerGroups` RPC still ship with `[Obsolete]`, exactly as the table above
requires. No *information* is lost by omitting the nine ctors — only a
construction path no binding user takes.

A surface-set assertion (§6.2 item 11, D20) will therefore see **fewer** public
ctors than Java on these three types. That gap is intended; the test must encode
the live-ctor set as the expectation and cite this sub-section, so a future
reviewer reads it as a ruling rather than as an omission.

**D26 — `MemberDescription` is shared by both describe RPCs.** One C# type, built
by one reader, consumed by both `ConsumerGroupDescription` (:5671 `get_member`)
and `ClassicGroupDescription` (:5856 `get_member`). Java shares the type too.
This makes the wiring guard (§6.3) load-bearing: two sibling readers over one
layout is exactly the finding-70.12 shape.
Note the accessor is `consumerId()` `:169` — **not** `memberId`. Do not
"correct" it.
Ctors: the live `:39` ships public; the four deprecated (`:66`, `:93`, `:117`,
`:138`) do **not** ship (D25.1). One `internal` ctor serves the native path for
both RPCs.

**D27 — the minting trio ships as methods, minting fresh per call.** See §1.2.
Precedent for method-not-property on argument-taking accessors is D18's own
carve-out.

**D28 — `ListConsumerGroupOffsetsSpec.topicPartitions()` defaults to NULL, and
null means ALL PARTITIONS.** Field default `:30`, javadoc `:34-35`, accessor
`:46-47`. The ABI models this with an explicit `all_partitions` flag at
h:6830-6834. **A C# empty-collection default inverts the semantics** — empty
would mean "no partitions", null means "every partition". The C# type must carry
a nullable collection and map null → `all_partitions = true`. A test must cover
null, empty, and populated as **three distinct** outcomes.

**D29 — `RemoveMembersFromConsumerGroupOptions` breaks four .NET conventions, and
all four are preserved.** (i) A **throwing constructor** `:34-36`; (ii) a
**non-fluent** `void reason(String)` `:47` amid fluent siblings; (iii)
`removeAll()` `:59` changes *result* behaviour — it makes `count == 0` at
h:6468-6469 and it is what arms the `memberResult` removeAll-mode throw (§1.2);
(iv) **no no-options overload** at `Admin.java:1269`, unlike every sibling RPC.
Additionally `MemberIdentity` equality **omits `Reason`**
(`KafkaAdminClient.java:4233`) — the C# `MemberToRemove` equality must omit it
too, or two members differing only by reason will wrongly deduplicate/differ.

**D30 — `OffsetAndMetadata` tri-state, against an already-existing type.**
`OffsetAndMetadata` **already exists** at `src/Confluent.Kafka/OffsetAndMetadata.cs`
with ctor `(long offset, string? metadata = null, int? leaderEpoch = null)` `:80`
and properties `Offset` `:97`, `Metadata` `:104`, `LeaderEpoch` `:110`. The ABI
carries presence flags separately: `has_offset` h:5955-5961, `has_leader_epoch`
h:6926-6929, and `OffsetAndMetadataMap_get_leader_epoch` h:9625. The reader must
consult the flags and map absence to `null` / the documented sentinel — **not**
read the value unconditionally and let a garbage default through. No change to
the existing type is expected; if one proves necessary, that is a scope
escalation to report, not to absorb.

**D31 — degenerate inputs return WHOLE-REQUEST errors, not empty successes.**
h:6911-6914, h:7000-7002, h:7135-7138. An empty group list / empty partition list
produces a request-level error. The C# layer must surface it as a faulted task,
not as an empty successful result. One test per cite.

**D32 — ragged two-level input encoding.** `listConsumerGroupOffsets` takes a
ragged 2-level array (h:6812-6815, `const char *const *const *topics`);
`alterConsumerGroupOffsets` takes **seven parallel arrays**. Also:
`Node_host` and `Node_rack` return a `(ptr, len)` **pair that is NOT
NUL-terminated** (h:11287-11288), and rack returns `(null, -1)` when absent
(h:11309). Marshalling these with a NUL-terminated string helper reads past the
buffer. Use the length-carrying path.

**D33 — `GroupState.groupStatesForType(GroupType)`: SHIP IT. **RULED 2026-09-17**,
as a deliberate, named exception to `bindings/CLAUDE.md` §2.6.**

`GroupState.java:76`, `public static`, `Set<GroupState> groupStatesForType(GroupType)`.
It has **no ABI export** — it is the one piece of P5 surface with no C-ABI
counterpart at all, which is why it was escalated.

**The ruling and its framing.** The maintainer **explicitly prioritized full
Java-surface fidelity over the "no business logic in the binding" rule for this
one utility.** It is recorded here as a **deliberate, named exception** to
`bindings/CLAUDE.md` §2.6 ("Shape, not logic — bindings add no Kafka behaviour;
all logic stays in the Rust core") — **not** as a silent violation, and **not**
as a case where §2.6 happens to be satisfied. Future reviewers need to see this
was a conscious tradeoff, ruled by the maintainer, not an oversight.

> **Note — this supersedes the withdrawn draft's reasoning, which was wrong.**
> That draft argued §2.6 *was satisfied* because the method is "pure client-side
> table lookup". That reasoning is not adopted. Duplicating Kafka's
> state-per-type table in C# **is** carrying a piece of Kafka's domain knowledge
> in the binding: if a future Kafka release adds a state to a type, the C# table
> silently disagrees with the broker until someone edits it. The exception is
> justified by the maintainer's fidelity ruling, not by a claim that no rule is
> being bent. Do not restate the "§2.6 is satisfied" argument.

**What to implement — read the Java verbatim; the withdrawn draft miscounted it.**
`GroupState.java:76-88` returns **four** hardcoded sets and **throws** on
everything else. It is not "one of five sets":

| `GroupType` | Result (`GroupState.java`) |
|---|---|
| `CLASSIC` | `:78` — `{PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY}` (5) |
| `CONSUMER` | `:80` — `{PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY, ASSIGNING, RECONCILING}` (7) |
| `STREAMS` | `:82` — `{STABLE, DEAD, EMPTY, ASSIGNING, RECONCILING, NOT_READY}` (6) |
| `SHARE` | `:84` — `{STABLE, DEAD, EMPTY}` (3) |
| `UNKNOWN` **and `null`** | `:86` — **throws** `IllegalArgumentException("Group type not known")` |

Both the fifth row's cases matter and both are easy to get wrong:
`GroupType.UNKNOWN` is a **real, shipped constant** (D24) that this method
**rejects**, and `null` falls through the same `else` because every branch is a
reference comparison. In C# both map to `ArgumentException` with the message
**exactly** `"Group type not known"` — asserted as a string per DoD §3, not as a
type check. Do **not** "improve" this into returning an empty set.

Duplicate the four sets **literally from the Java lines cited above**, not from
the javadoc table at `:30-46`. The javadoc table lists `UNKNOWN` as valid for all
four types; the **code does not include `UNKNOWN` in any set**. The code wins.

**It is not dead surface.** The withdrawn draft's "zero other call sites" claim
was scoped to `kafka/clients/src/main/java` (true — the only hit there is the
declaration). Repo-wide, Kafka itself calls it from three CLI tools —
`ConsumerGroupCommand.java:156`, `ShareGroupCommand.java:115`,
`StreamsGroupCommand.java:173` — each to build the state filter it then hands to
`listGroups`. That is precisely RPC 4.1, in scope. So this is the canonical way a
caller populates `ListGroupsOptions.inGroupStates(...)`, which strengthens the
ruling rather than merely permitting it.

**D34 — the deprecated **options** projection: IMPLEMENT IT. **RULED 2026-09-17**,
as a sanctioned, named exception to D21.**

`ListConsumerGroupsOptions` stores **one** field (`Set<GroupState> groupStates`),
and its deprecated pair is a **pure lossy projection over that one field**,
performed by Java itself:

- `inStates(Set<ConsumerGroupState>)` `:57` → `states.stream().map(s -> GroupState.parse(s.toString())).collect(toSet())`
- `states()` `:85` → `groupStates.stream().map(g -> ConsumerGroupState.parse(g.toString())).collect(toSet())`

**The ruling.** The maintainer approved the exception: *support the deprecated
filter option by replicating **exactly** the lossy conversion Java performs
internally.* Replicate it — do not improve it, do not widen it, do not add a
lossless side-channel.

**This is a named exception to D21**, which is the phase's "read both, derive
neither" rule, and it must be documented **at the site** as such — same standard
as D33: an explicit ruling, not a gap. A code comment plus XML-doc on both
members, citing `ListConsumerGroupsOptions.java:57` / `:85` as the source of the
conversion and naming D21 as the rule being excepted. Without that comment a
Critic will correctly flag the derivation as a D21 violation, and the exception
will look like an oversight.

The asymmetry has a principled basis worth stating in the comment: on the
**read** path the ABI exports both axes, so deriving one from the other would be
gratuitously lossy — hence D21. On the **write** path Java itself stores only one
field and derives the other, so mirroring Java *requires* the derivation. D21
governs readers; D34 governs this one writer pair.

**The loss is real and in both directions** (9 ↔ 8 constants): `NOT_READY` has no
`ConsumerGroupState` counterpart and maps to `UNKNOWN`, after which set
deduplication collapses `{UNKNOWN, NOT_READY}` to size 1. §6.2 item 10 asserts
that collapse explicitly so it is pinned behaviour rather than an accident of the
mapping.

---

## §3 — Coordination (this section replaces staging; directives #2 and #3)

**P5 is ONE phase, with ONE plan, ONE agent number, and ONE scheduled Critic
pass. It has no sub-phases and no interim reviews. Its *implementation* may span
multiple resumable Actor sessions — see §3.1 and the amendment below.**

1. **The Actor implements all nine RPCs as one phase, across as many resumable
   sessions as it needs.** P3's "three internally-green stages" and P4's two
   stages are **not** repeated: P5 is not subdivided into sub-phases, is not
   handed off at an internal green point, and does not request an interim
   review. What it *may* do is checkpoint: a session commits the work it
   finished, appends a progress note to §11, and the next session resumes from
   git state plus that log. A checkpoint is a **resume point, not a stage** — it
   changes nothing about the phase's scope, its agent number, or its review
   schedule. Incremental commits are still expected and required (§8).
2. **The Critic runs EXACTLY ONCE, after the Actor has completely finished.**
   Not per-RPC, not per-file, not at any internal green point. The Critic is
   spawned only after the Actor reports the full nine-RPC surface complete with
   the Definition of Done satisfied (§7).
3. **This is stated here so it is not re-litigated mid-phase.** If the Actor or
   the Critic proposes an interim review, the answer is no, and this section is
   the citation.
4. **Agent numbers:** Actor = `dotnet-actor` with **N = 75**; Critic =
   `dotnet-critic` with **N = 75**. Comments land in
   `bindings/dotnet/COMMENTS.75.md`; resolved items move to
   `bindings/dotnet/COMMENTS.DONE.75.md`. Neither file is ever `git add`ed.
5. **Personas:** `dotnet-actor` / `dotnet-critic` — **never** `actor-executor` or
   `kafka-critic`, which are Rust-shaped and will review against the wrong ground
   truth (`bindings/CLAUDE.md` §2.7: review against the **C ABI header** and the
   **Java public API**, not Rust internals).
6. **Fix cycles are unbounded and unstaged.** After the single Critic pass, the
   Actor fixes and the Critic re-reviews as many times as needed until
   `COMMENTS.75.md` is empty. The "exactly once" constraint governs the *first*
   review's timing — it does not cap fix-cycle re-reviews.

### §3.A — Amendment: resumable checkpoints authorized (maintainer ruling, 2026-09-17)

**This is a narrow, measured relaxation of §3.1 ONLY. §3.2–§3.6 are untouched
and remain in force exactly as written.** Read this section before concluding
that "no internal stages" was abandoned — it was not.

**What changed.** §3.1 originally read *"The Actor implements all nine RPCs in
ONE CONTINUOUS PASS"* and §12 listed *"one continuous Actor pass over all nine
RPCs, no internal stages or checkpoints"* among the things approval does not
relax. Actor 75 then died three times with zero commits (§11, rows 4–8). The
third death was forensically measured rather than guessed at: 69 tool calls, 27
Reads — **every one ranged, zero duplicated** by file+offset+limit, ~2,300 lines
total — and **no `dotnet build` and no `dotnet test` anywhere**, because the
Actor never got far enough to build. Tool results were only ~29% of the consumed
window; ~48 extended-thinking blocks were roughly two-thirds of it.

So the binding constraint was **not** an unbounded input the briefing could cap.
It was **reasoning volume**, which scales with how much unresolved design
surface the agent must hold simultaneously — and "one continuous pass over nine
RPCs" is precisely an instruction to hold all nine at once. No further briefing
discipline could have fixed it; the three crashes were three attempts to do so.

**The ruling.** The maintainer authorized the Actor to work in **resumable
checkpoints**: the implementation may span multiple agent sessions, each
committing its progress and appending a §11 note, with the next session resuming
from git state plus that log.

**What did NOT change — enumerated, because this is the part a future reader
will get wrong:**

- P5 is still **one phase**, with **one `PLAN.md`** and **one agent number
  (N = 75)**. Checkpoints do not create P5a/P5b, do not fork the plan, and do
  not consume new agent numbers.
- The **Critic still runs exactly once**, only after all nine RPCs are complete
  and green (§3.2). **There are no per-checkpoint reviews.** A checkpoint
  boundary is not a review trigger, and §3.3 remains the citation for refusing
  one.
- Fix cycles after that single first review remain unbounded and unstaged
  (§3.6).
- Scope, the §4 per-RPC surface, the §6 rules, and the §7 Definition of Done are
  all unchanged. A checkpoint is a **save point in one continuous phase**, not a
  deliverable, not a hand-off, and not a partial acceptance.

**Standing briefing carry-overs (orthogonal to this ruling, still required).**
The crash-1 and crash-2 fixes remain in force for every session: grep rather
than read the 15,175-line generated header, `bindings/dotnet/CLAUDE.md`, and
`ffi-marshalling.md`; range-read this plan, never whole-file it; defer per-RPC
Java-source reads until that RPC is being written; build with
`cargo build --features ffi` (a bare `cargo build` exits 0 while producing a
symbol-less dylib and overwriting the good header); and bound every test
invocation's output.

---

## §4 — Per-RPC surface (flat; no stage subdivision)

Nine RPCs, listed in Java's `Admin.java` order. Each entry states the published
shape and cites the **stored field** that determines it (§1.4's discipline).

### 4.1 `listGroups` → `ListGroupsResult`
Shape **4** (§1.1). `ListGroupsResult` publishes `Valid()`, `Errors()`, `All()`.
Payload `GroupListing` — 5 accessors (`groupId` :54, `type` :68, `protocol` :77,
`groupState` :90, `isSimpleConsumerGroup` :97), **no `state()` axis**, **zero
`@Deprecated`**. Options `ListGroupsOptions` — zero `@Deprecated`; three static
factories (D24), fluent `inGroupStates` :67, `withProtocolTypes` :76, `withTypes`
:85, getters `groupStates` :93, `protocolTypes` :100, `types` :107.
ABI: result `_get_valid` :6035; RPC :6549 with params
`(admin, group_states, group_state_count, protocol_types, protocol_type_count, types, type_count, timeout_ms, out_result)`.

**Also lands here (D33, ruled):** the `public static`
`GroupState.groupStatesForType(GroupType)` helper — four hardcoded sets plus an
exact-message throw. It is not an RPC and has no ABI export, but it belongs to
this RPC's filter story: Kafka's own CLI tools call it precisely to build the set
they pass to `inGroupStates(...)` before calling `listGroups`. Ship it as a
static member on the C# `GroupState` type, matching Java's home.

**`forConsumerGroups()` needs a bare string literal.** D24 ships it, and Java's
body is `.withProtocolTypes(Set.of("", ConsumerProtocol.PROTOCOL_TYPE))`. That
constant is `"consumer"` (`ConsumerProtocol.java:45`) and lives in Java's
**`clients.consumer.internals`** package. The binding has no `ConsumerProtocol`
type today (measured: 0 hits in `bindings/dotnet/src`; control-positive
`AclOperation` → 3 files) and **must not introduce one** — it is internal Java
surface. Inline the literal `""` and `"consumer"` with a comment citing
`ConsumerProtocol.java:45`.

### 4.2 `listConsumerGroups` → `ListConsumerGroupsResult` **[deprecated]**
Shape **4**. Whole RPC + result + options + listing deprecated (D25).
Payload `ConsumerGroupListing` — `groupId` :119, `isSimpleConsumerGroup` :126,
`groupState` :133, **`state` :142 [deprecated :141]**, `type` :151.
Options `ListConsumerGroupsOptions` — one stored field; `inGroupStates` :45,
**`inStates` :57 [dep]**, `withTypes` :68, `groupStates()` :76, **`states()` :85
[dep]**, `types()` :92. **No `withProtocolTypes`** — and the ABI matches exactly:
`kafka_admin_AdminClient_list_consumer_groups` :6625 takes
`(admin, group_states, group_state_count, types, type_count, timeout_ms, out_result)`
with **no `protocol_types`**. D34 governs the deprecated projection — **ruled
approved**: replicate Java's lossy conversion exactly, with the D21-exception
comment at the site.
Ctors: `:45`, `:88`, `:104` ship public; `:58` and `:72` are deprecated and do
**not** ship (D25.1).
ABI: result `_get_valid` :6098.

### 4.3 `describeConsumerGroups` → `DescribeConsumerGroupsResult`
Genuine shape **1**: `DescribedGroups()` map + `All()`.
Payload `ConsumerGroupDescription` — 11 accessors (§0.3); class **not**
deprecated, `state()` :190 is (D25). Ctors: **only** the live `:103` ships
public; the three deprecated ctors `:54`/`:68`/`:83` do **not** ship (D25.1). Add
an `internal` ctor for the native-handle path.
ABI: `_get_value` :6167; `group_epoch` :5781 is a **bool + out-param** pair, as
is `target_assignment_epoch` :5794 — model as nullable, not as a sentinel.

### 4.4 `describeClassicGroups` → `DescribeClassicGroupsResult`
Genuine shape **1**. Payload `ClassicGroupDescription` — 8 accessors, two live
ctors, **zero `@Deprecated`**. Has `state()` and **no** `groupState()` — mirror
exactly; the asymmetry with 4.3 is Java's.
ABI: `_get_value` :6223.

### 4.5 `listConsumerGroupOffsets` → `ListConsumerGroupOffsetsResult`
**Headline C** (§1.3). Publishes zero-arg `PartitionsToOffsetAndMetadata()`
(throws on **> 1** group), the keyed overload (throws on unknown group), and
`All()`. The stored map `:36-38` is package-private and **never published** —
route from the stored field, not the accessor set.
Input spec `ListConsumerGroupOffsetsSpec` — **null means ALL partitions** (D28).
Ragged 2-level encoding (D32). `OffsetAndMetadata` tri-state (D30).

### 4.6 `alterConsumerGroupOffsets` → `AlterConsumerGroupOffsetsResult`
Minting trio member; **cannot validate** (D27, §1.2) — the ctor `:35` takes only
the future. `PartitionResult(tp)` mints, never throws. Seven parallel input
arrays (D32).

### 4.7 `deleteConsumerGroupOffsets` → `DeleteConsumerGroupOffsetsResult`
Minting trio member; **validates** against the retained `Set<TopicPartition>`
`:35`. `PartitionResult(tp)` throws `:45` with the exact message
*"Partition … was not included in the original request"*.
⚠ Accessor set is byte-identical to 4.6 and to P4's
`AlterPartitionReassignmentsResult` — §1.4 applies with full force.

### 4.8 `deleteConsumerGroups` → `DeleteConsumerGroupsResult`
Genuine shape **6** — all futures are `KafkaFuture<Void>`. The straightforward
one; do not let that make it the unreviewed one.

### 4.9 `removeMembersFromConsumerGroup` → `RemoveMembersFromConsumerGroupResult`
Minting trio member with **two** throws (`:83` removeAll-mode, `:86`
not-requested). Options break four conventions (D29). `MemberToRemove` equality
omits `Reason` (D29). No no-options overload (`Admin.java:1269`).

---

## §5 — Mode-B gaps

**No net-new Mode-B gap.** P5 introduces no requirement for a Rust-core or C-ABI
change — see §0.3's accessor sweep, in which all 38 measured Java accessors
across the five payload types already have exports.

P5 inherits the two pre-existing entries from P3 §15 unchanged. If the Actor
discovers a missing export mid-phase, that is a **scope escalation to report
immediately**, not something to work around with a binding-side reconstruction —
`bindings/CLAUDE.md` §2.6: bindings add no Kafka behaviour.

> **`groupStatesForType` is NOT a Mode-B gap, and is NOT a precedent.** It has no
> ABI export (D33), which normally reads as exactly the escalation this paragraph
> describes. It is not one: the maintainer ruled it is implemented **in C#**, so
> no Rust-core or C-ABI change is required and **Mode A holds**. That ruling
> covers this one utility only. Any *other* missing export is still an
> escalation, and "D33 did it" is not an argument for reconstructing behaviour
> binding-side.

---

## §6 — Tests

### §6.0 ⚠ KNOWN RISK — mock coverage is largely absent, and one mock actively lies

**This risk was the withdrawn P5a draft's argument for a stage boundary. Stages
are forbidden (§3), so it is recorded here as a stated, accepted risk rather than
mitigated structurally.** It is not lost in the merge; it is the reason §6.2
exists in its current form.

Measured mock coverage in the Rust core's `MockAdminClient`:

| RPC | Mock | Cite |
|---|---|---|
| `list_groups` | ⚠ **present but IGNORES ALL FILTERS** — always returns `Consumer`/`Stable` | :1492-1511 |
| `list_consumer_groups` | ✅ | :1514-1526 |
| `describe_consumer_groups` | ❌ stub | :1540 |
| `describe_classic_groups` | ❌ stub | :1558 |
| `list_consumer_group_offsets` | ⚠ partial — **stubs when `groups.len() != 1`** | :1564-1620, stub at :1573-1583 |
| `alter_consumer_group_offsets` | ❌ stub | :1634 |
| `delete_consumer_group_offsets` | ❌ stub | :1649 |
| `delete_consumer_groups` | ❌ stub | :1665 |
| `remove_members_from_consumer_group` | ❌ stub | :1681 |

Three consequences the Actor must plan around:

1. **`list_groups`' mock is a live false-pass generator.** It ignores the filter
   arrays entirely and always answers `Consumer`/`Stable`. A filter test written
   against it passes no matter what the binding sends. Every `ListGroupsOptions`
   filter assertion — including all three D24 factories — **must** assert at the
   **submit seam** (D19: the mock ignores `_options`, so assert what crossed the
   boundary, not what came back).
2. **The richest type surface has zero mock-reachable path.**
   `describeConsumerGroups` / `describeClassicGroups` carry 11 + 8 accessors plus
   9 more through `MemberDescription` — 28 accessors — behind a hard stub. These
   must be covered by **direct result-type construction and reader-level unit
   tests**, not by round-tripping through the mock.
3. **The `> 1 group` `IllegalStateException` (§1.3) is NOT mock-reachable** — the
   mock stubs out exactly that path at :1573-1583. It must be tested by
   constructing the result type directly.

> **Do not "fix" the mock.** The stub message on `alter` reads *"Not implement
> yet"* — that is **Java's own typo**, faithfully translated. Do not correct it,
> and do not extend the Rust mock: that would be a Mode-B change and P5 is Mode A.

### §6.1 Inherited test obligations (11 items from P3/P4)

All eleven carry forward unchanged. **Item 11 — KEY-SET vs ROW-SET — bites hardest
in this phase**, because P5 has three inputs where a caller-supplied key yields
zero rows: `listConsumerGroupOffsets` with an unknown group,
`deleteConsumerGroupOffsets` with an unrequested partition, and
`removeMembersFromConsumerGroup` in removeAll mode (`count == 0`, h:6468-6469).
Each needs its key-set and row-set asserted **separately**.

### §6.2 P5-specific required tests

1. **Shape-4 non-parallelism** — a result with a non-empty valid list **and** a
   non-empty error list; assert `Valid()` returns the partial list, `Errors()`
   returns the errors, and `All()` faults. Assert each list is walked by its own
   count.
2. **Three exact-message throw tests** (§1.2, DoD §3) — the `deleteConsumerGroupOffsets`
   message, and both `removeMembersFromConsumerGroup` messages, asserted as
   strings.
3. **The `> 1 group` `IllegalStateException`** via direct construction (§6.0.3).
4. **Minting identity** — `PartitionResult(tp)` called twice returns two distinct
   task objects (D27), proving no caching crept in.
5. **D28 tri-state** — null / empty / populated `topicPartitions` produce three
   distinct submitted encodings, asserted at the submit seam.
6. **D29 equality** — two `MemberToRemove` differing only by `Reason` compare
   **equal**.
7. **D30 presence flags** — absent offset and absent leader-epoch map to null,
   asserted against the `has_*` flags rather than a value sentinel.
8. **D31 degenerate inputs** — one test per cite (h:6911-6914, 7000-7002,
   7135-7138) asserting a faulted task, not an empty success.
9. **D32 non-NUL-terminated `Node_host`/`Node_rack`** — a host string whose
   buffer is not NUL-terminated marshals correctly; absent rack `(null, -1)` maps
   to null.
10. **D34 lossy collapse** — `inStates({UNKNOWN, NOT_READY})` collapses to size 1.
    Assert the round-trip both ways (`inStates` → `GroupStates`, and
    `InGroupStates` → `States`), since Java projects in both directions.
11. **Surface-set assertions** filter by `BindingFlags` + `CompilerGeneratedAttribute`,
    **never** a name predicate (D20). **Constructor sets are asserted against
    Java's LIVE ctors only** — the nine deprecated ctors are intentionally absent
    (D25.1); the expectation encodes that and cites D25.1 so the gap reads as a
    ruling, not an omission.
12. **`RunContinuationsAsynchronously` on every TCS** — including every *minted*
    future (§1.2). A minted future is easy to construct without it.
13. **D33 table fidelity** — `GroupStatesForType` returns the **exact** set for
    each of the four accepted types, asserted by full set equality (not `Contains`),
    against the four rows tabulated in D33. Sizes 5 / 7 / 6 / 3. In particular
    **no set contains `UNKNOWN`** — the javadoc table at `GroupState.java:30-46`
    says otherwise and the code is authoritative; a test written from the javadoc
    passes for the wrong reason.
14. **D33 exact-message throw** — `GroupStatesForType(GroupType.Unknown)` **and**
    the `null`/default path both throw with the message **exactly**
    `"Group type not known"`, asserted as a string (DoD §3), not as a type check.
    `Unknown` is a shipped constant that this method rejects; that is the easy
    miss.
15. **`ListGroupsOptionsTest` translation** — Java has a dedicated test file
    (`kafka/clients/src/test/java/org/apache/kafka/clients/admin/ListGroupsOptionsTest.java`)
    covering the three D24 factories (`:32`, `:47`, `:62`), the group-state and
    protocol-type setters, and — at `:90-97` — `groupStatesForType` feeding
    `inGroupStates`. Translate it; DoD §3 forbids skipping a Java test of a
    translated class. Note `:84` uses `Set.of(GroupState.values())`, i.e. **all
    nine** constants, which doubles as a completeness check on the C# enum.

### §6.3 Cross-cutting sweeps

- **Wiring guard (finding 70.12)** — mandatory for every reader added this phase.
  Sibling readers from a shared factory are layout-compatible, so a mis-wired
  reader returns a plausible answer. `[InlineData]` row **plus** guard entry in
  the **same commit**, and the Actor confirms a deliberate cross-wire injection
  goes **RED** before committing. D26's shared `MemberDescription` is the highest-risk
  instance.
- **Stored-field sweep (replaces the `thenApply` grep)** — for each of the nine
  result types, read the Java **field declarations** and record what is stored,
  then separately read the public accessors. Where they disagree, the stored
  field wins for routing (§9). The old `thenApply` heuristic is structurally
  blind to §1.3's variant and must not be relied on.
- **Two-pass prose sweep** — text ADDED **or FALSIFIED** (maintainer ruling
  2026-09-10, finding 70.1). P5 falsifies prose in at least **three** places: any
  existing text implying shape-4 lists are parallel; any text implying the
  `alter`/`delete` accessor-set identity implies shape identity; and — added by
  the 2026-09-17 rulings — any binding-side text stating the "no logic in the
  binding" rule or D21's "derive neither" as **absolute**. Both now have exactly
  one ruled exception each (D33, D34). Qualify such text where it exists in the
  binding's own docs/XML-doc; do **NOT** edit `bindings/CLAUDE.md` or any
  `.claude/rules/` file to do it (§8) — if the shared rulebook needs the
  carve-out recorded, that is a `RULE-DRAFT-*.md` for the maintainer.
- **COUNT DISCIPLINE** — a filtered grep is not a count. Every claimed-zero needs
  a control-positive **in the same command**. See §10 traps 13–15 for three
  distinct ways this failed during planning.

---

## §7 — Definition of Done

Standard binding DoD, plus:

1. `dotnet build` and `dotnet test` green — **0 warnings, 0 errors**, under
   `TreatWarningsAsErrors=true` (`Directory.Build.props:17`). Every `[Obsolete]`
   self-use carries a **narrow** `#pragma warning disable CS0618` / `restore` pair
   with a justification comment (D25).
2. All nine RPCs present on `IAdmin`, `KafkaAdminClient`, **and** `MockAdminClient`.
3. Every commit body carries its RPC's shape justification citing the **Java file
   and line of the stored field** (§1.4). Nine such cites.
4. Every net-new reader has its `[InlineData]` row and guard entry, with the
   cross-wire injection confirmed RED (§6.3).
5. Mode A holds — zero diff under `src/` (Rust), zero diff to
   `target/include/confluent_kafka.h`. If this is violated, **stop and report**.
6. The §6.3 sweeps are run and their results recorded in §11.
7. `COMMENTS.75.md` empty.
8. **Both ruled exceptions are documented AT THE SITE, in the code** — not only
   in this plan (§0.1.1):
   - `GroupStatesForType` carries a comment naming it a **maintainer-ruled
     exception to `bindings/CLAUDE.md` §2.6** (D33), stating that the Java table
     is duplicated deliberately for surface fidelity, and citing
     `GroupState.java:76-88`.
   - The deprecated `InStates` / `States` pair carries a comment naming it a
     **sanctioned exception to D21**, citing `ListConsumerGroupsOptions.java:57`
     and `:85` as the source of the conversion (D34).

   A Critic finding either site undocumented should file it; a Critic finding
   either *implemented* should **not** file it as a §2.6 or D21 violation — both
   are ruled, and this item is the citation.
9. **The nine deprecated constructors are absent** (D25.1), and the surface-set
   test encodes their absence as the expectation rather than tolerating it.

---

## §8 — Commit hygiene

- Incremental commits throughout; a commit is **not** a stage (§3.1).
- **Never `git add -A`.** Stage named paths only.
- **Never `git add`:** `COMMENTS.75.md`, `COMMENTS.DONE.75.md`, the repo-root
  `.claude/agents/dotnet-*.md`, anything under `.claude/agent-memory/`, or
  `design/current/PLAN-M15-admin-client.md` (untracked but **not** gitignored).
- **Never** edit `CLAUDE.md` or anything under any `.claude/rules/`. A rule
  candidate goes in a separate `RULE-DRAFT-*.md` for the maintainer.
- No squash, no rebase, no branch creation, no merge.
- This plan file stays **uncommitted** while in draft.

---

## §9 — The rule this phase establishes

> **For a ROUTING decision, read the STORED FIELD — never the public accessor.**

Now a family of two: `createTopics` (P3) and `ListConsumerGroupOffsetsResult`
(§1.3). Both store one shape and publish another; in both, the accessor set is
the misleading signal and the field declaration is the truth. The corollary,
already in `bindings/dotnet/CLAUDE.md` §1.1.1 and proven three ways in §1.4:
**accessor-set identity is evidence of nothing, in either direction.**

Candidate for promotion into the binding rulebook after P5 closes — as a
`RULE-DRAFT-*.md`, never by editing the rulebook directly.

**A second promotion candidate, created by the 2026-09-17 rulings.** D33 is the
first ruled carve-out to `bindings/CLAUDE.md` §2.6 across any binding. The
generalizable form is worth drafting once P5 closes:

> **When Java publishes a pure lookup/utility on an in-scope public type that the
> C ABI does not export, surface fidelity may outweigh §2.6 — but only by an
> explicit ruling, recorded at the call site, naming §2.6 as the rule being
> excepted.**

Draft it as a `RULE-DRAFT-*.md` with D33 as its worked example. Two things the
draft must carry, because both were live errors in this phase: the exception is
**per-utility, not per-category** (§5's note), and it must not be justified by
claiming §2.6 is satisfied (D33's superseded reasoning).

---

## §10 — Traps

Traps 1–12 carry forward from the withdrawn drafts (shape-4 parallelism,
accessor-set identity, minting-vs-caching, `AdminClient_destroy` having no
refcount and no drain → span-the-op `DangerousAddRef` with a differential check,
the hookless one-shot `GCHandle` family, `AbandonBeforeSubmit` on submit throw,
borrowed-vs-owned `kafka_common_KafkaError_t*` — per-index `get_error(i)` is
`const`/borrowed → `FromBorrowedHandle`, the async callback's `error` is owned →
`FromHandle` (this direction flipped **three times** across P3; verify, do not
recall), `RunContinuationsAsynchronously`, D19's options-ignoring mock, D20's
`BindingFlags` filtering, and the D18 property-vs-method carve-out).

**Three environment/measurement traps added from this planning round:**

13. **An `&&`/`||` chain fabricates a false negative.** The construct
    `[ -n "$p" ] && grep -n PATTERN "$p" || echo "(file not found)"` prints
    *"(file not found)"* whenever **grep exits 1 on no-match** — not only when the
    file is absent. This wrongly reported five existing Java files
    (`ClassicGroupDescription`, `ListGroupsOptions`, `DescribeConsumerGroupsOptions`,
    `MemberAssignment`, `GroupListing`) as missing. **Split existence from
    matching into two commands.** A bare `find` per class, with a control-negative
    (`ZZZNotAClass` → empty), then a separate `grep -c`.
14. **A control-positive can itself fail, and then it proves nothing.**
    `kafka_common_AclOperation` was used to control an "is this enum exported as a
    type?" grep and returned **0** — because `AclOperation` crosses as `int32_t`
    while `GroupState`/`GroupType` cross as `const char *`. The control was
    invalid, so the result it was guarding was unsupported. **Verify the control
    actually fires before trusting the measurement it guards.**
15. **Grep the file's real format, not the format you expect.** The withdrawn P5a
    draft searched STATUS.md for `N = 75` when the file writes `N=73` — so both
    the claim **and its control** matched nothing (§0.2). Confirm the pattern
    matches a known-present instance first.
16. **zsh expands unquoted glob arguments.** `--include=*.props` fails with
    *"no matches found"* before grep ever runs. **Quote every glob**:
    `--include='*.props'`.
17. **Directory names: it is `tests/`, not `test/`.** A sweep over `src test`
    silently searched one real directory and one nonexistent one, producing a
    zero that looked like a finding.

**Two content traps added by the 2026-09-17 rulings:**

18. **The javadoc table and the code disagree — the code wins.**
    `GroupState.java:30-46` documents `UNKNOWN` as a valid state for all four
    group types; `groupStatesForType` `:76-88` puts `UNKNOWN` in **no** set and
    *throws* when handed `GroupType.UNKNOWN`. A C# table transcribed from the
    javadoc compiles, reads plausibly, and is wrong in five places. Transcribe
    from the four `Set.of(...)` expressions, and diff the resulting sizes against
    D33's table (5 / 7 / 6 / 3) before committing.
19. **A "zero call sites" claim is only as wide as the path you searched.**
    The D33 escalation reported `groupStatesForType` had zero callers — true of
    `kafka/clients/src/main/java`, false of the repo: Kafka calls it from three
    CLI tools and one test. The narrow result was reported without its scope, and
    it pointed the analysis at "dead surface" when the method is in fact the
    canonical way to build a `listGroups` filter. **State the search path with
    every count**, and widen it once before concluding something is unused.

### §10.1 False-fail traps

A green build under `TreatWarningsAsErrors=true` does **not** prove the
deprecated surface is correctly marked — a missing `[Obsolete]` produces no
warning at all. Assert deprecation presence explicitly in the shape-parity tests,
following `PublicAdminP3ShapeParityTests.cs:229` (*"These types being obsolete is
exactly the assertion"*).

---

## §11 — Execution log

*(One phase, one agent number, one scheduled review. Checkpoint rows below are
**resume points, not stages** — see §3.1 and §3.A. No row here is ever a review
trigger.)*

| Date | Event | Notes |
|---|---|---|
| 2026-09-17 | Plan approved | maintainer approved as written, all four rulings included (§0.1.1); authorization is per-phase — P6 is **not** authorized |
| 2026-09-17 | Actor 75 spawned | `dotnet-actor`, N=75, full nine-RPC scope, **one continuous pass** (§3.1) |
| 2026-09-17 | Actor 75 died — infra, **zero output** | context-exhaustion ("autocompact thrashing"); crashed on its first reads, produced no commits and no code; HEAD still `4c6cd673`, nothing staged |
| 2026-09-17 | Actor 75 **re-spawned** (attempt 2) | clean restart, not a resume — nothing to reconcile. ⚠ **NOT a stage and NOT a checkpoint**: this is infra recovery, and §3.1's single-continuous-pass constraint is unbroken. Cause was the *brief*, not the Actor: it mandated ≈8,800 lines of reading before any code. Restart brief replaces that with a ranged reading protocol (pass-1 ≈350 lines) and bans whole-file reads of the ABI header (**15,175 lines** after the 2026-09-17 regeneration; earlier briefs said 14,651, which was the stale figure) |
| 2026-09-17 | Actor 75 died again — infra | same autocompact-thrashing signature, but reached "Orientation complete. Starting RPC 1." — the ranged reading protocol held; the crash moved into the RPC-1 work. Repo clean again: HEAD `4c6cd673`, nothing staged |
| 2026-09-17 | **Root cause found + environment repaired** | Proximate culprit is **tool-call output size**, not document reads: a default `dotnet test` emitted **7,931 lines / ~1.16 MB in a single tool call**. Underlying cause: `target/debug/libconfluent_kafka.dylib` had been built **without `--features ffi`**, so 3 symbols (`kafka_common_Error_new` / `_code` / `_destroy`, added by `a8205c5c`) were missing → 765 `EntryPointNotFoundException`s → 287 test failures. `cargo build --features ffi` restored the dylib (771 `kafka_*` exports) and regenerated the header (15,175 lines / 844 fn decls). Baseline is now **1387/1387 passing on net10.0 and net8.0**. `net462` aborts on missing `mono` — environmental, pin `-f net10.0` |
| 2026-09-17 | **P5 ABI ground truth verified unaffected** | Diffed the group ABI surface before vs after header regeneration: **no change**, 120 symbols both times, all nine P5 RPCs present with their `_async` + `_callback_t`. The 13 newly-added header functions are entirely the `kafka_common_Error_*` family. Neither crashed Actor was working from wrong ground truth, and **Mode A remains viable** — there was never a false Mode-B escalation risk for the group RPCs |
| 2026-09-17 | Actor 75 **re-spawned** (attempt 3) | clean restart, not a resume. ⚠ **NOT a stage and NOT a checkpoint** (§3.1 unbroken). Brief adds mandatory output bounds on every Bash command, the green baseline, and per-RPC commit + §11 logging so a further crash loses at most one RPC |
| 2026-09-17 | Actor 75 died a **third** time — infra | same signature. Forensics measured rather than assumed: 69 tool calls; 27 Reads, **every one ranged, zero duplicated** by file+offset+limit, ~2,300 lines total; **no `dotnet build` and no `dotnet test` anywhere** (it died before building, so the crash-2 output-bound fix was never even exercised); tool results only ~29% of the window; ~48 extended-thinking blocks ≈ two-thirds of it. Conclusion: the sink is **reasoning volume**, not any remaining input to bound — and §3.1's single-continuous-pass constraint was what forced all nine RPCs to be held at once. Left 4 uncommitted prerequisite files; HEAD still `4c6cd673` |
| 2026-09-17 | **Maintainer ruling: resumable checkpoints authorized** | §3.1 relaxed to permit multi-session resumable execution; **§3.2–§3.6 untouched**. Recorded in full at **§3.A**, with §12's first "does not relax" bullet rewritten to match. Single phase, single agent number, **single Critic pass at the end** all preserved |
| 2026-09-17 | Checkpoint 1 **salvaged, does not yet build** | the 4 prerequisite types from the third attempt — `GroupType.cs`, `GroupState.cs`, `GroupMarshal.cs`, `GroupListing.cs` (561 lines) — reviewed and kept: D33 framed as a **named exception** to `bindings/CLAUDE.md` §2.6 (not compliance), D34's separate-deprecated-axis framing present, no deprecated constructors, Mode A held, both ABI-facing claims verified against the regenerated header and `Utf8Marshal.cs:54`. **But the salvage review read the files without compiling them, and they do not build:** `GroupState.cs:40` has `<see cref="ConsumerGroupState"/>`, a forward reference to the D34 deprecated-axis type that no P5 RPC has authored yet → `CS1574` × 3 TFMs. The other three files compile clean; this is the only defect. Left uncommitted at HEAD `4c6cd673` |
| 2026-09-17 | Actor 75 **re-spawned** (attempt 4) — first session under §3.A | tasked narrowly: (A) fix the `CS1574` and commit the four salvaged files, then (B) start the first §4 RPC, committing per RPC. Brief carried the crash-1/crash-2 discipline forward (grep-don't-read the 15,175-line header / `CLAUDE.md` / `ffi-marshalling.md`, range-read this plan, defer per-RPC Java reads, `cargo build --features ffi`, bounded output) |
| 2026-09-17 | ✅ **CHECKPOINT 1 COMMITTED — `79e1fabb`** | **Task A complete.** Fixed `GroupState.cs:40` (`cref` → `<c>ConsumerGroupState</c>`) and committed the four prerequisite types by explicit path: 4 files, **561 insertions**. Verified independently: `dotnet build src/Confluent.Kafka/Confluent.Kafka.csproj` → **Build succeeded, 0 Warning(s), 0 Error(s)** across all three TFMs; `bindings/dotnet/src` working tree clean. ⚠ **A resume point, not a stage** — no review triggered (§3.2, §3.A) |
| 2026-09-17 | Actor 75 died a **fourth** time — infra, **but with work banked** | same autocompact-thrashing signature, during Task B (first RPC). Produced **no RPC code and no §11 row** (this row written by the Manager); no new production `.cs` files, working tree clean, nothing lost. **This is the first of four sessions to leave durable output** — §3.A's checkpoint model did the job it was introduced to do: the crash cost the session, not the work. Standing finding for the next session: Task A plus orientation was itself close to a full window, so **a session must be scoped to ONE RPC and nothing else** |
| 2026-09-17 | Actor 75 **re-spawned** (attempt 5) — **scope: `listGroups` (§4.1) and nothing else** | Maintainer refinement of §3.A: one RPC per session, not "one RPC or a small group". §4.1 chosen because its entire payload closure is **already committed** at `79e1fabb` (`GroupListing`, `GroupType`, `GroupState` incl. D33's `GroupStateExtensions.GroupStatesForType`, `GroupMarshal`) — the cheapest orientation of the nine — and because it lands §1.5's **single net-new `KeyedResultMarshal` callable**, which §4.2 then reuses. Net-new in this session: `ListGroupsOptions`, `ListGroupsResult`, the two-list walker, the `IAdminClient`/`AdminClient` method, P/Invoke decls, and §6.2's tests for this RPC. **Not** in scope: any other RPC, and `ConsumerGroupState` (that is §4.2's) |
| 2026-09-17 | Attempt 5 died — infra, no commit | reached orientation + precedent reads for `listGroups` and was compacted out before writing code. Nothing staged, HEAD still `79e1fabb`. Retried as-is per the maintainer's "just retry, don't analyse" directive |
| 2026-09-17 | Actor 75 **re-spawned** (attempt 6) — `listGroups` (§4.1) again | same one-RPC scope; brief trimmed to orientation facts + trap list, with the `listGroups` ABI signatures pre-supplied so the session does not spend reads rediscovering them, and the memory-index read skipped |
| 2026-09-17 | ✅ **RPC 1/9 — `listGroups` (§4.1) COMPLETE** | committed across five sub-slices after two more whole-RPC sessions were lost to context exhaustion: `3f4007b7` `ListGroupsOptions` · `1fb670c3` `ListGroupsResult` · `ac44ee4e` P/Invoke decls · `865892e6` two-list walker (`KeyedResultMarshal.CompleteTwoLists`) · `7a853986` `AdminClient.ListGroups` + 36 tests. **1441/1441 green on net10.0** (was 1405), 0 warnings across all TFMs, Mode A held (zero Rust `src/` and zero header diff). Sub-RPC slicing is what finally worked — a whole RPC is too large for one session |
| 2026-09-18 | ✅ **RPC 2/9 — `listConsumerGroups` (§4.2) COMPLETE** | committed across seven sub-slices: `a9130733` `ConsumerGroupState` · `4dbf731e` `ConsumerGroupListing` · `5cffe24a` `ListConsumerGroupsOptions` · `3405ea4d` `ListConsumerGroupsResult` · `4b50aac2` interop · `21579f40` `NativeAdminClient.ListConsumerGroups` · `b760ee2f` `IAdmin`/`KafkaAdminClient`/`MockAdminClient` · `325bceb4` tests. **1514/1514 green on net10.0** (was 1441), 0 warnings across all TFMs, Mode A held. `KeyedResultMarshal.CompleteTwoLists` reused unchanged — still six callables, no seam growth. Two axes only (states, types) — no protocol-type axis. Deprecated `States` and `ConsumerGroupListing.State` are both **projections** over the non-deprecated backing field, not independent axes; the core mock leaves state/type absent (faithful to `MockAdminClient.java:743`'s two-arg ctor), so the happy-path test pins `null`, deliberately not `Unknown` |
| 2026-09-18 | ✅ **RPC 3/9 — `describeConsumerGroups` (§4.3) COMPLETE** | nine sub-slices: `6ee73df0` `MemberAssignment` · `30bbb14f` `MemberDescription` · `5257d4a6` `ConsumerGroupDescription` · `39501c87` its tests · `e720e53d` format-gate fix · `dc33ab40` options · `0c2c87cf` result · `0b6e6bad` interop · `0e735aa5` `NativeAdminClient.DescribeConsumerGroups` · `35da7e73` `IAdmin` wiring · `b4c80c0a` tests. **1600/1600 green on net10.0** (was 1514), 0 warnings all TFMs, Mode A held. Keyed shape — `KeyedResultMarshal.Complete<TKey,TValue>` reused unchanged, still six callables. `State` is a projection of `GroupState` (only `group_state` declared); `AuthorizedOperations` absent ≠ empty (`has_authorized_operations` bool; the two hash identically, so only `Equals`/`ToString` separate them); four `bool`+out-param presence pairs → nullable, never sentinels. Deprecated ctors do not ship (3 on `ConsumerGroupDescription`, 4 on `MemberDescription`). The core mock refuses every key with `unsupported_version("Not implemented yet")` — faithful to `MockAdminClient.java:735` — so happy-path and mixed-result coverage is driven at the marshal layer, not end-to-end |
| | **Gate discovered red** | `dotnet format --verify-no-changes` sat red for three commits (IDE1006, `s_` prefix) because `dotnet build` does not surface IDE1006 and the gate was read through a pipe, which masks dotnet's exit status. Fixed in `e720e53d`; every brief since requires reading dotnet's own status |
| 2026-09-18 | ✅ **RPC 4/9 — `describeClassicGroups` (§4.4) COMPLETE** | eight sub-slices: `630279a2` `ClassicGroupState` · `5d0703c2` `ClassicGroupDescription` · `7a12d725` its tests · `48c2410a` options · `682a435f` result · `654c0818` interop · `1882e616` `NativeAdminClient.DescribeClassicGroups` · `ec2d74ce` `IAdmin` wiring · `6122b482` tests. **1670/1670 green on net10.0** (was 1600), 0 warnings all TFMs, Mode A held. Keyed shape — `Complete<TKey,TValue>` reused unchanged, still six callables. Nothing here is deprecated, so **both** ctors ship (the D25.1 hold-back covers deprecated forwarders only) and no `[Obsolete]`/`CS0618` appears. `state` is a **stored field** (not a projection, unlike §4.2/§4.3) and there is no `groupState()` — Java's asymmetry, mirrored. `isSimpleConsumerGroup()` is the inverse case: a **projection over `protocol`** (`ClassicGroupDescription.java:113`) despite the ABI exporting a dedicated bool, so `_is_simple_consumer_group` is deliberately **not** declared. The 6-arg ctor forwards `Set.of()` (`:48`), i.e. present-but-empty — so the copy-out uses the 7-arg form and passes `null` for absence. Core mock refuses every key (`mock_admin_client.rs:1546`, citing `MockAdminClient.java:1478`), so coverage is at the marshal layer. One session lost to context exhaustion; its uncommitted submit method + tests were salvaged and committed unchanged |
| 2026-09-18 | ✅ **RPC 5/9 — `listConsumerGroupOffsets` (§4.5) COMPLETE** | seven sub-slices: `e46cff93` `ListConsumerGroupOffsetsSpec` · `4abf8f17` options · `d0a05206` result · `b3fd7a38` interop · `d4bde799` `NativeAdminClient.ListConsumerGroupOffsets` · `d9cf956f` `IAdmin` wiring · `137a23d2` tests. **1722/1722 green on net10.0** (was 1670), 0 warnings all TFMs, Mode A held. Keyed shape — `Complete<TKey,TValue>` reused unchanged, still six callables. `TopicPartition`/`OffsetAndMetadata` already shipped; reused. **Three shape departures from §4.3/§4.4, all Java's:** (a) the result has **no map getter** — Java spells three methods (`partitionsToOffsetAndMetadata()`, `(String)`, `all()`), so the per-key-map-property convention does not apply and all three are methods; (b) its ctor is **package-private** → `internal`, unlike the two public sibling results; (c) the no-arg form rejects `futures.size() != 1`, i.e. **zero as well as many**. `Spec.TopicPartitions` is **nullable**: null → `all_partitions = true` ("every committed partition"), empty → `false` with count 0 — the flag exists so the two cannot collapse. Per-partition `has_offset` false means "listed but no committed offset", distinct from absent; the `-1`/null/`false` fillers behind a false gate are never read as data, and reading `-1` would fault the whole group's future via `OffsetAndMetadata`'s negative-offset rejection. `get_leader_epoch` is a presence pair → nullable. **First jagged submit in the phase** (`const char* const* const* topics`, `const int32_t* const* partitions`): two indirection levels pinned into a `List<PinnedUtf8String>` + `List<GCHandle>`, all released in `finally`; duplicate group ids rejected, not collapsed. **First RPC in the phase whose core mock implements the happy path** (`mock_admin_client.rs:1564`, single-group only; `group_specs.len() != 1` still refuses) |
| 2026-09-18 | ⏸ **PHASE HALTED AFTER RPC 5 by maintainer instruction** | RPCs 4.6–4.9 (`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`, `deleteConsumerGroups`, `removeMembersFromConsumerGroup`) not started. **Critic 75 not spawned** — the single scheduled review still runs once, after all nine RPCs, whenever the phase resumes. Resume point: HEAD `137a23d2`, tree clean, 1722 green |
| | Checkpoint *n* | one RPC per session; commit + a row here, then the next session resumes from git state + this log |
| | Actor 75 complete | all nine RPCs, DoD §7 satisfied |
| | Critic 75 spawned | **first and only scheduled review** (§3.2) |
| | Fix cycle(s) | unbounded; not stages |
| | `COMMENTS.75.md` empty | phase closes |

> **On the earlier rows' wording.** Rows for attempts 2 and 3 read
> "⚠ **NOT a stage and NOT a checkpoint**". Those statements were accurate when
> written *and remain factually true of those events* — both were crash restarts
> that committed nothing, so there was no checkpoint to speak of. But they cite
> a §3.1 that has since been amended, so do not read them as evidence that
> checkpoints are still forbidden. §3.A governs; checkpoints are authorized, and
> the one thing those rows were really asserting — that no interim review is
> triggered — is still true of checkpoints too.

Sweep results (§6.3) to be recorded here by the Actor.

---

## §12 — AUTHORIZATION

**Status: APPROVED — 2026-09-17.** The maintainer approved this plan **as
written**, with all four rulings of §0.1.1 included as updated. Implementation is
authorized and `dotnet-actor` N=75 is spawned under the §3 coordination
directives.

**Authorization is per-phase, not milestone-wide.** Approving P5 does **not**
authorize P6. P6 requires its own plan and its own approval.

**No design item is open.** The four escalated questions were ruled on
2026-09-17 and are applied throughout — see §0.1.1 for the ruling record and
§2.1 for the per-decision status.

**What the approval does not relax.** The §3 constraints are part of the approved
plan, not preconditions that expire with it:

- P5 stays **one phase** — one plan, one agent number (N = 75), one scheduled
  review — covering all nine RPCs. Its *implementation* may span multiple
  resumable Actor sessions (§3.1 as amended by §3.A, maintainer ruling
  2026-09-17); a checkpoint is a **resume point, not a stage**, and does not
  subdivide the phase, fork the plan, or consume a new agent number. Incremental
  commits are still required (§8);
- the Critic runs **exactly once** after the Actor reports the whole phase
  complete and green (§3.2); an interim review proposed by either agent is
  refused, and §3.3 is the citation. **This is unchanged by §3.A** — a
  checkpoint boundary is explicitly *not* a review trigger;
- fix cycles after that first review are unbounded and unstaged until
  `COMMENTS.75.md` is empty (§3.6).

> **Prose-sweep note.** This section previously read "NOT AUTHORIZED" and
> asserted that no agent would be spawned and no code written. The approval
> **falsified** that text, and it was rewritten rather than left standing — the
> §6.3 two-pass sweep covers prose that is added *or falsified*, and this section
> is its worked example inside the plan itself.
>
> **It happened a second time, and the same rule applied.** The 2026-09-17
> resumable-checkpoint ruling (§3.A) falsified the first bullet above, which read
> "one continuous Actor pass over all nine RPCs, no internal stages or
> checkpoints (§3.1)", along with §3's heading lead and §11's subtitle. All were
> **rewritten**, not annotated-around, and §3.A states precisely which clause
> moved and which did not — so the record shows a *narrow* amendment rather than
> the wholesale abandonment a reader might otherwise infer from the softened
> language.
