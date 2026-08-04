---
name: review-m11-phase2-docs-fix-loop
description: Milestone 11 Phase 2 review patterns — five passes, four of them finding a defect in a FIX rather than the translation; heading-reorder containment, warning-block attribution, per-member verification
metadata:
  type: project
---

Phase 2 (ten txn request/response wrappers) had **one** code defect, found in
pass 1. Passes 2-5 each found a defect in the *previous fix*, all in docs
(`design/history/Milestone-11/PLAN.md` §9.7 and
`.claude/rules/producer-transactions.md` §12). Patterns worth carrying forward:

**A "fix" that reorders headings can break section containment.** Pass 4 noted
§9's headings ran 9.1-9.5, 9.8, 9.7, 9.6. The fix made them ascend by appending
§9.7/§9.8 to the **end of the file** — past the `## 10` boundary — so two `###`
subsections of §9 became subsections of §10. Numbers ascended; the open
follow-up that rules §12 points at became unreachable by reading §9. When
reviewing a reorder, check the *parent heading level* each block now sits under,
not just the numeric sequence.

**A warning block is only as good as its verifiable anchor.** §9.7's ⚠ block was
correct in instruction but grounded its reason in a flag value that holds in
`generator/messages/` and not in `kafka/` — see
[[review-spec-corpus-two-sources]]. A reviewer verifying it against the
designated source reference finds it false and can conclude the opposite of what
the warning intends. Judge a hazard warning by "what happens when someone checks
this", not "is the instruction right".

**Per-member vs across-the-set verification.** Two findings in this loop were
themselves wrong for symmetric reasons: pass 2 checked a flag's *presence*
instead of its value; pass 4 asserted a value across a nine-member set without
checking each member. Enumerate members explicitly in the report — a table with
one row per API is cheap and makes the error impossible to hide.

**Cleared and stable across passes 2-5** (do not re-derive from scratch):
the 10 txn dispatch arms in `abstract_request.rs` / `abstract_response.rs`; the
seven deterministic-sort sites; the four broker-side scoping omissions; both
halves of the `ignorable` distinction. `build_txn_topic_collection` sorting only
topic names is **correct** — Java's per-topic partition list is an `ArrayList`
in caller order, already deterministic; only the `HashMap` key iteration needed
sorting.
