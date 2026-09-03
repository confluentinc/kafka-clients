---
name: review-m13-phase0
description: Milestone-13 Phase 0 (AK 4.2.0→4.3.1 reference bump) reviewed CLEAN by Critic 60; verification recipe for submodule/corpus/errata bookkeeping phases
metadata:
  type: project
---

Milestone-13 = translate the AK 4.2.0→4.3.1 clients delta. Branch
`milestone-12-ak-4.3.1`. Phase 0 (agent 60) = reference bump + corpus sync +
bookkeeping only; reviewed **CLEAN, 0 findings** (COMMENTS.60.md).

**Why:** captures how a "no new translation" bookkeeping phase is audited, so a
future corpus/version-bump phase reuses the recipe instead of re-deriving it.

**How to apply — verification recipe for a corpus/version-bump phase:**

  - Submodule: `git ls-tree HEAD kafka` must show the target commit gitlink;
    `git diff <base>..HEAD -- .gitmodules` must be empty (only the gitlink moves).
    4.3.1 tag = `26b251a451ce941d3d7a55e6487bcb7f16b5ad48`; 4.2.0 = `a18251bae0`.
  - Corpus exactness: `diff -rq generator/messages
    kafka/clients/src/main/resources/common/message` must be empty (catches
    missed/extra/modified specs in one shot). `generator/messages/` is a full
    1:1 copy of the kafka message dir (198 files incl. broker-only + README).
    `build.rs:44` compiles `generator/messages/`, NOT the kafka dir.
  - Generated types land in `$OUT_DIR/generated/*.rs` (not git-tracked, not in
    `src/`). Find with `find target -name "<snake>_data.rs"`.
    `ControlRecordTypeSchema.json` (`"type":"data"`) → `ControlRecordTypeSchemaData`
    with `r#type: i16`, versions 0.
  - Errata survey (`design/history/Milestone-13/rules-errata.md`): only
    `producer-transactions.md` carries `File.java:NNN` citations;
    consumer-threading.md / admin-client.md have none. 4.3.1 shifts:
    TransactionManager uniform **+18** (KAFKA-14831 block near :205),
    Sender.java uniform **+1**. Verified anchors:
    shouldPoisonStateOnInvalidTransition :305-307, lookupCoordinator :987,
    handleCachedTransactionRequestResult :1279-1301, instanceof dispatch
    :817/:824, MDG ignorable guard :776, TRR `isAcked=true` :58 (set before error
    check, volatile field :30). All rule *behavioral* claims still hold at 4.3.1.

**§28/§31 amendment is a Phase-4 deliverable** (rebalance handshake reshape:
PartitionsAssignedEvent + ApplyAssignmentEvent three-leg, PartitionsRemovedEvent
rename, AsyncPollEvent gating, skipAssignmentEvents). Phase 0 correctly left it a
placeholder. See [[review-spec-corpus-two-sources]] for the corpus-naming rule.
