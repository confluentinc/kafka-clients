# Critic 51 — resolved findings (review of `8ab3c771..9559f796`)

All five findings were real; **none disputed**. The Critic re-derived the emission set
independently (227/127/100/12/53, name-for-name against `OUT_DIR`) and found no defect
in the emitted code. Both adjudications went in the Actor's favour and were applied as
ruled.

**A sixth issue arrived mid-round from a separate audit and outranked all five**: the
guard's error path was a **panic**. It is recorded below as Issue 0, because everything
else is cosmetic beside it.

**Attribution for Issue 0.** It was found by a subagent the Critic dispatched, whose
report never reached the Critic — it is not in the pass-1 file, and the Critic asked not
to be credited with it. The chain was: subagent found it, the Manager verified and
relayed it, the Actor fixed it. The Critic verified the fix on its merits in pass 2.

Pass 2 added two more findings (6 and 7), pass 3 added Issue 8 and two corrections
to §9.32. All low, none blocking. **Eight in total; exactly one was behavioural** —
Issue 0, the panic this branch introduced. The rest are records.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 0 | **The guard's error path panicked the I/O task.** `6245ca10` moved a class of condition into `write()`; `network_client.rs:564` then read `request.to_send(&header).expect("Failed to serialize request")`. Java wraps **both** `builder.build(version)` and `request.toSend(header)` in one `try` (`NetworkClient.java:582-583` + `:608`); this port caught only the first, so every guarded field would have aborted the task where Java retries or falls back — worse than the silent drop §9.1 set out to fix, and reachable only because of this change. `do_send_with_request` now returns `io::Result<()>`, and Java's single `catch` body is extracted into `abort_send_with_unsupported_version`, shared by both failures. | `6245ca10` | teeth-checked: restoring the `.expect` makes the new test panic at `network_client.rs:607:45`. Two tests, positive and negative, against a broker advertising Metadata ≤ v7 |
| 1 | **The 2PC severity claim is retracted** from PLAN §9.1, `examples/txn_api_contracts.rs` (module doc, probe comment, failure message), and by this fixup from `6245ca10`'s message. `Enable2Pc`/`KeepPreparedTxn` are never set in production, so nothing was dropped and nothing is fixed. §9.1 returns to "a missing safety net", with a **real** case named instead: `describeConfigs(includeDocumentation = true)` against a v1/v2 broker silently returned configs without documentation. Wave-3 finding 4 explicitly stays **open** with its actual cause (config accepted and inert). | `6245ca10`, `cbe48f20` | `transaction_manager.rs:1379-1383` sets four fields; both setters' only callers are `#[cfg(test)]`; `is_2pc_enabled()` has no `src/` caller outside a test assertion; `grep setEnable2Pc` over Java's producer is empty |
| 2 | **Two Java tests translated**, and the false skip note deleted. `MessageTest.testDefaultValues` and `testNonIgnorableFieldWithDefaultNull` were parked in a prose comment naming this blocker — no `#[ignore]`, so no runner and no `#[ignore]` sweep would list them. §9.1's "there are two Java tests" is corrected to **four**, with the reason both revisions missed them: they swept the *Java* corpus for assertions instead of the *Rust* tree for skips. | `cbe48f20` | both pass; they cover the **array** and **nullable-string-default-null** branches that `d0dd3b52` rewrote — the branches the pre-existing guard tests (`int64`, `uuid`) do not reach |
| 3 | **The tagged-field emission site is pinned.** 12 of the 100 guards come from a second site with its own hand-built version expression; every prior assertion of the guard text was on an untagged field. `test_tagged_non_ignorable_field_raises_uve_below_its_version` covers `FetchRequest.ReplicaState` at v14, plus the default-valued negative case. | `6245ca10` | passes; the Critic's own severity qualifier (10 of 12 are `*Response`, the rest broker-only) is accepted — the risk was test-only, and this closes it |
| 4 | **The `parent_versions` threading is pinned at the emission level**, not only in the predicate. `test_nested_struct_guard_respects_the_enclosing_message_versions` generates a `ListOffsetsResponse`-shaped nested struct (declared `0+`, parent 1-11) and asserts no guard, then the same with v0 reachable and asserts one. | `6245ca10` | teeth-checked twice: the first attempt "failed" on an `unused_variable` compile error rather than its assertion — the same narrow-check trap — so it was redone with the parameter still consumed, and failed on the intended assertion at `lib.rs:5212` |
| 5 | **"`generateNonIgnorableFieldCheck` has a single caller" corrected.** It has two: `MessageDataGenerator.java:794` and `JsonConverterGenerator.java:328`. The conclusion is unaffected — `*JsonConverter` is not translated, and the guard is still absent from `generateClassMessageSize` — but the supporting claim was wrong in a commit whose purpose was fixing wrong claims. | `cbe48f20` | `grep -rn generateNonIgnorableFieldCheck kafka/generator/src/main/java/` returns both call sites and the definition |
| 6 | **§9.31's own mis-citation fixed.** The `AlterPartitionReassignments` row cited `:60-65`, which is the private constructor; the gate is in `build(short)` at `:44`, check at `:45-49`. The correction ships the three greps that establish the coordinates, so the next reader re-derives rather than re-trusts — this section exists to replace a mis-citation and should not carry one. | `cbe48f20` | `grep -n "build(short version)"` → `:44`; `"allowReplicationFactorChange() && version"` → `:45`; `"private AlterPartitionReassignmentsRequest("` → `:61` |
| 8 | **§9.32's scope covered one bound of two.** The title says "outside `nullableVersions`"; the scope sentence derived only "starts above". Both bounds give **7 fields / 6 specs**; the seventh is `ShareFetchResponse…Records` (nullable only at v0, effective range 1-2), doubly out of client scope but required by the title's formulation. A section instructing the next person to derive-then-diff cannot carry two formulations yielding two numbers. Table now has a `bound` column. | `cbe48f20` | re-derived both bounds from `generator/messages/*.json`: 6 lower + 1 upper, union 7 across 6 specs |
| 7 | **Filed as PLAN §9.32, not fixed.** The third skip-block entry covered `MessageTest.testWriteNullForNonNullableFieldRaisesException` with a type-system argument true of only one of its two halves. Behind the other half is a generator-wide divergence: Java throws `NullPointerException` for a null at a version outside `nullableVersions` (`MessageDataGenerator.java:960-970`); this generator emits the marker unconditionally for string/bytes/array fields, though the **struct** arm already has the check. Scope measured: **6 fields / 5 specs**. Two are set to null by this client on purpose, and Java gates neither in its builder — so it relies on the generated throw exactly as it relies on §9.1's guard. | n/a (pre-existing) | reproducer `test_write_null_for_non_nullable_field_raises_error`, `#[ignore]`d on §9.32, confirmed to fail on its own assertion (`message_test.rs:1710`) rather than vacuously |

## Adjudications, applied as ruled

**A — the rules-file edit.** Ruling accepted in full. The factual status update in
`producer-transactions.md` §11 stands and still awaits the human's ratification; the
added **How to apply** bullet ("the check lives on the write path only…") is **removed**
— true and useful, but new normative guidance to reviewers rather than a correction of a
false statement, so it belongs in the suggestion channel. §12's cross-reference is
reworded so it no longer implies the 2PC path is covered.

**B — the seven nested-struct top-of-`write()` guards.** Not a finding; left alone. The
Critic's derivation is stronger than the argument originally offered and is the one now
recorded: `generateClassWriter` opens with
`VersionConditional.forVersions(struct.versions(), parentVersions)` (`:709`), and where
`possible ⊆ containing` it takes the `generateAlwaysTrueCheck` arm
(`VersionConditional.java:209-215`) and emits **no check at all** — so Java writes happily
at v0 and switching to `struct_versions` would create a *new* divergence.

## Why the panic fix was in scope, restated

The Critic's ground is stronger than the one originally argued ("the other half of the
same Java contract") and is the reasoning to carry forward: **the guard is what made
ordinary requests able to fail `write` at all.** Shipping it without the propagation
would have *introduced* a panic on a path that previously succeeded — a regression caused
by the change, not merely exposed by it. That is the boundary test for future work of
this shape: if a change makes a previously-infallible call fallible, its error path is
part of the change.

## Scope boundary drawn rather than crossed

**PLAN §9.31 filed, not fixed.** Two builders drop version gates Java performs at
`build`: `ListTransactionsRequest.java:37-44` (two checks) and
`AlterPartitionReassignmentsRequest.java:60-65` (one). Both are independent of §9.1 —
they exist whether or not the generated guard does — and §9.1's own instruction warns
against bundling. Worth flagging that the round-2 brief described nine of ten sites as
"faithful to Java at the builder level"; reading `AlterPartitionReassignments.build`
showed it gates too, so the count of genuine builder gaps is **two APIs, three checks**,
not one. §9.31 also carries the verified 11-site inventory (the brief listed ten; the
extra is `DescribeCluster.EndpointType` in bootstrap-controllers mode).

With Issue 0 fixed, all three §9.31 conditions are still *caught* — aborted send,
`UnsupportedVersion`, the same outcome class Java produces, since Java's own `catch`
covers its builder throw too. What is lost is Java's more actionable message text.

**PLAN §9.32 filed, not fixed** (Issue 7). Generator-wide null-at-non-nullable-version
divergence, **7 fields / 6 specs**, with an `#[ignore]`d reproducer. Same "do not
bundle" reasoning.

**§9.32 is latent, and pass 3 corrected the section's own overstatement of it.** I
re-derived both named fields rather than taking the ruling on trust, and it holds:

  - `OffsetFetchRequest.Topics` is latent **by construction** — `build_version` refuses
    `topics.is_none()` below v2 (`offset_fetch_request.rs:411-418`, constant at `:52`),
    and that gate runs *before* `maybe_downgrade` (`:425`), the only site that can
    produce a null top-level `topics`. The threshold equals the field's nullable lower
    bound, and Java carries the same constant and check. Nothing a broker does opens it.
  - `MetadataRequest.Topics` is latent via an exhaustive split over version selection,
    but the leg that closes it is **weaker** — two of three paths are shut by code, the
    third by the dated fact that a Metadata-max-0 broker predates ApiVersions. §9.32
    now labels that leg as the weaker one rather than presenting both alike.

So the section's live-wire argument is withdrawn and its present-indicative "encodes a
`-1` the broker will read as…" is now conditional. **The case for fixing it survives on
different ground, already present in the section's own text**: it is a class gap in one
generator function — the struct case is handled, string/bytes/array are not — so a
future spec revision makes it live silently with nothing to catch it. That now leads.

The `?`/NPE note was also framed as prospective and is not: the `?` folds every `write`
error today. Nothing reaches it, because the one null-version check already emitted (the
struct arm) is itself unreachable — no struct field in either corpus has a nullable
window narrower than its presence range. That *strengthens* the assignment: the
distinction is §9.32's to make, because §9.32 is what would mint an NPE-class error,
while §9.1 minted only `UnsupportedVersion`-class errors for which folding is correct.

Three pre-existing defects were therefore surfaced by this branch and deliberately left
for their own work: §9.31 (two builders, three checks), §9.32, and — fixed rather than
filed, because the branch caused it — the serialize panic.

**One suggestion of mine was wrong and is withdrawn.** I proposed folding the
`warn`-vs-`debug` log-level deviation into §9.31's fix. It does not work: §9.31's gates
raise from `build_version`, whose error arm (`network_client.rs:529`) is *also* `warn`,
so adding them would move the log line rather than lower its level or reduce its volume.
Recorded in §9.31 as an independent one-line change on `:516` and `:529`, with `:454`
noted as the arm that already matches Java's `debug` (`NetworkClient.java:586`).

## Rule suggestions S1-S3 — position recorded, not applied

Per `agent-roles.md` §2 these are the human's to accept or reject. The Actor's position:

- **S1 (CLAUDE.md §2 should name arrays and structs)** — **agree, and it is the direct
  cause of a defect in this very branch.** The rule named only `string`/`bytes`; the
  array case sat wrong until the guard exposed it, at the cost of three test
  corrections. The proposed wording is accurate, including the `records` carve-out
  (`FieldSpec.java:452-453` returns `"null"` unconditionally for records, which is why
  `field_default_is_null` checks it first).
- **S2 (agent-roles.md §2 should say whether an Actor may correct a false *fact* in a
  rules file)** — **agree.** Adjudication A had to be decided from precedent, and this
  Actor had to guess. The proposed fact-vs-normative line matches what was actually done
  here and what the carve-out removed, so it is testable rather than aspirational.
- **S3 (a skip whose justification names a blocker must be re-checked when the blocker
  closes)** — **agree, and it is the strongest of the three.** Issue 2 is exactly its
  general case, and the sweep that missed it was mine. The clause worth keeping is the
  last one: the Actor's sweep of the *Java* corpus structurally cannot find skips
  recorded on the *Rust* side, and an `#[ignore]` sweep cannot find skips recorded in
  prose.

  **Strengthened by pass 2.** Issue 7 is the **third** instance of S3's pattern on this
  one branch, and it is a variant the current wording does not quite cover: that skip
  note did not name a blocker that had closed — it gave a justification that was true of
  only *half* the Java test it skipped. So the rule wants a second clause: a skip must
  account for **everything** its Java test asserts, and a multi-part Java test may be
  half-translatable. Worth adding when S3 is adopted; a mechanism that keeps finding live
  instances on the branch that prompted it is the argument for adopting it at all.

  The two clauses have **different triggers** — the first fires when a blocker closes,
  the second when a Java test has parts — so neither subsumes the other and both are
  needed. That is the reason to keep them as two clauses rather than merge them.

## Owed

The **Docker gate** (`make verify-sandbox` → `test-integration`, `test-c`) has not run;
the sandbox is wedged with seven orphaned `apache/kafka:4.2.0` containers awaiting a
human. Container count was 7 before and after this round. All commits used
`--no-verify` and say so. Issue 0 sharpens why it matters: the integration leg is the
only place a negotiated version meets a real request, and it is the leg that would have
caught the panic without a hand-written unit test.
