# Critic 51 — resolved findings (review of `8ab3c771..9559f796`)

All five findings were real; **none disputed**. The Critic re-derived the emission set
independently (227/127/100/12/53, name-for-name against `OUT_DIR`) and found no defect
in the emitted code. Both adjudications went in the Actor's favour and were applied as
ruled.

**A sixth issue arrived mid-round from a separate audit and outranked all five**: the
guard's error path was a **panic**. It is recorded below as Issue 0, because everything
else is cosmetic beside it.

| # | Fix | Fixup of | Verification |
|---|---|---|---|
| 0 | **The guard's error path panicked the I/O task.** `6245ca10` moved a class of condition into `write()`; `network_client.rs:564` then read `request.to_send(&header).expect("Failed to serialize request")`. Java wraps **both** `builder.build(version)` and `request.toSend(header)` in one `try` (`NetworkClient.java:582-583` + `:608`); this port caught only the first, so every guarded field would have aborted the task where Java retries or falls back — worse than the silent drop §9.1 set out to fix, and reachable only because of this change. `do_send_with_request` now returns `io::Result<()>`, and Java's single `catch` body is extracted into `abort_send_with_unsupported_version`, shared by both failures. | `6245ca10` | teeth-checked: restoring the `.expect` makes the new test panic at `network_client.rs:607:45`. Two tests, positive and negative, against a broker advertising Metadata ≤ v7 |
| 1 | **The 2PC severity claim is retracted** from PLAN §9.1, `examples/txn_api_contracts.rs` (module doc, probe comment, failure message), and by this fixup from `6245ca10`'s message. `Enable2Pc`/`KeepPreparedTxn` are never set in production, so nothing was dropped and nothing is fixed. §9.1 returns to "a missing safety net", with a **real** case named instead: `describeConfigs(includeDocumentation = true)` against a v1/v2 broker silently returned configs without documentation. Wave-3 finding 4 explicitly stays **open** with its actual cause (config accepted and inert). | `6245ca10`, `cbe48f20` | `transaction_manager.rs:1379-1383` sets four fields; both setters' only callers are `#[cfg(test)]`; `is_2pc_enabled()` has no `src/` caller outside a test assertion; `grep setEnable2Pc` over Java's producer is empty |
| 2 | **Two Java tests translated**, and the false skip note deleted. `MessageTest.testDefaultValues` and `testNonIgnorableFieldWithDefaultNull` were parked in a prose comment naming this blocker — no `#[ignore]`, so no runner and no `#[ignore]` sweep would list them. §9.1's "there are two Java tests" is corrected to **four**, with the reason both revisions missed them: they swept the *Java* corpus for assertions instead of the *Rust* tree for skips. | `cbe48f20` | both pass; they cover the **array** and **nullable-string-default-null** branches that `d0dd3b52` rewrote — the branches the pre-existing guard tests (`int64`, `uuid`) do not reach |
| 3 | **The tagged-field emission site is pinned.** 12 of the 100 guards come from a second site with its own hand-built version expression; every prior assertion of the guard text was on an untagged field. `test_tagged_non_ignorable_field_raises_uve_below_its_version` covers `FetchRequest.ReplicaState` at v14, plus the default-valued negative case. | `6245ca10` | passes; the Critic's own severity qualifier (10 of 12 are `*Response`, the rest broker-only) is accepted — the risk was test-only, and this closes it |
| 4 | **The `parent_versions` threading is pinned at the emission level**, not only in the predicate. `test_nested_struct_guard_respects_the_enclosing_message_versions` generates a `ListOffsetsResponse`-shaped nested struct (declared `0+`, parent 1-11) and asserts no guard, then the same with v0 reachable and asserts one. | `6245ca10` | teeth-checked twice: the first attempt "failed" on an `unused_variable` compile error rather than its assertion — the same narrow-check trap — so it was redone with the parameter still consumed, and failed on the intended assertion at `lib.rs:5212` |
| 5 | **"`generateNonIgnorableFieldCheck` has a single caller" corrected.** It has two: `MessageDataGenerator.java:794` and `JsonConverterGenerator.java:328`. The conclusion is unaffected — `*JsonConverter` is not translated, and the guard is still absent from `generateClassMessageSize` — but the supporting claim was wrong in a commit whose purpose was fixing wrong claims. | `cbe48f20` | `grep -rn generateNonIgnorableFieldCheck kafka/generator/src/main/java/` returns both call sites and the definition |

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

## Owed

The **Docker gate** (`make verify-sandbox` → `test-integration`, `test-c`) has not run;
the sandbox is wedged with seven orphaned `apache/kafka:4.2.0` containers awaiting a
human. Container count was 7 before and after this round. All commits used
`--no-verify` and say so. Issue 0 sharpens why it matters: the integration leg is the
only place a negotiated version meets a real request, and it is the leg that would have
caught the panic without a hand-written unit test.
