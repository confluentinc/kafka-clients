---
name: review-m11-phase2-docs-fix-loop
description: Milestone 11 Phase 2 review patterns — six passes, four finding a defect in a FIX rather than the translation; heading-reorder containment, warning-block attribution, per-member verification; closed clean on pass 6
metadata:
  type: project
---

Phase 2 (ten txn request/response wrappers) had **one** code defect, found in
pass 1. Passes 2-5 each found a defect in the *previous fix*, all in docs
(`design/history/Milestone-11/PLAN.md` §9.7 and
`.claude/rules/producer-transactions.md` §12). **Pass 6 was clean** — closed after
six passes. Patterns worth carrying forward:

**A "fix" that reorders headings can break section containment.** Pass 4 noted
§9's headings ran 9.1-9.5, 9.8, 9.7, 9.6. The fix made them ascend by appending
§9.7/§9.8 to the **end of the file** — past the `## 10` boundary — so two `###`
subsections of §9 became subsections of §10. Numbers ascended; the open
follow-up that rules §12 points at became unreachable by reading §9. When
reviewing a reorder, check the *parent heading level* each block now sits under,
not just the numeric sequence. Cheap mechanical checks that caught/cleared this:
`grep -o '^### \(9\|10\)\.[0-9]' PLAN.md | sort | uniq -c` (one each, no dups), and
`diff` of the moved section's body between the old and new commit (proves nothing
was truncated in transit).

**A warning block is only as good as its verifiable anchor.** §9.7's ⚠ block was
correct in instruction but grounded its reason in a flag value that holds in
`generator/messages/` and not in `kafka/` — see
[[review-spec-corpus-two-sources]]. A reviewer verifying it against the
designated source reference finds it false and can conclude the opposite of what
the warning intends. Judge a hazard warning by "what happens when someone checks
this", not "is the instruction right".

**Per-member vs across-the-set verification.** Two findings in this loop were
themselves wrong for symmetric reasons: the pass-1 fix checked a flag's *presence*
instead of its value; pass 4 asserted a value across a nine-member set without
checking each member. Enumerate members explicitly in the report — a table with
one row per API is cheap and makes the error impossible to hide.

The presence-vs-value trap is concretely visible in the txn specs: the flag is
written explicitly (`false`) in `EndTxnRequest.json` and
`AddPartitionsToTxnRequest.json` but **absent** from `AddOffsetsToTxnRequest.json`
and `TxnOffsetCommitRequest.json`. A presence-grep therefore over-reaches on the
first two and under-reaches on the last two — which is exactly what happened, and
it is corpus-independent (all four are byte-identical in both trees).

**Beware narrowing a Critic's own root-cause framing.** Pass 5 wrote "a spec-flag
claim not tied to a verifiable artifact"; the Actor recorded it as "not tied to the
**corpus** it was read from" and named the two-corpus disagreement as the common
root of three findings. Only one of the three was actually a corpus error. Pass 6
judged this not worth a finding (the Actor implemented the Critic's own wording
faithfully, and the operative guard — §12's "check the Java `super(...)` call, not
the spec flag" — is correct), but when writing a root-cause sentence, state the
*narrowest* claim the evidence supports.

**Cleared and stable across passes 2-6** (do not re-derive from scratch):
the 10 txn dispatch arms in `abstract_request.rs` / `abstract_response.rs`; the
seven deterministic-sort sites; the four broker-side scoping omissions; both
halves of the `ignorable` distinction. `build_txn_topic_collection` sorting only
topic names is **correct** — Java's per-topic partition list is an `ArrayList`
in caller order, already deterministic; only the `HashMap` key iteration needed
sorting.

**Lock mechanics** (`agent-roles.md` §2): the lock is a `mkdir`-style directory
`COMMENTS.<N>.md.lock/` holding an `owner` file with holder/pid/timestamp;
`*.md.lock` is gitignored. Check the recorded pid with `ps -p` before taking over —
stale locks from interrupted passes are common. `rm`/`rmdir` may be denied by the
permission system, in which case overwrite `owner` with a `RELEASED` block instead
of leaving a live-looking claim.
