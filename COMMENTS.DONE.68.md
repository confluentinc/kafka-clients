# Critic review of Phase E (admin per-key async callbacks, Consumer groups family) — resolved

## 1. [Bug — doc accuracy, low severity but real] `remove_members_from_consumer_group_async`'s doc comment is wrong about the empty-input case — FIXED

`src/ffi/admin.rs`'s doc comment above
`kafka_admin_AdminClient_remove_members_from_consumer_group_async` states the
callback "runs synchronously on the calling thread, before this function
returns, for every key, when the RPC cannot be submitted at all (a NULL admin
handle, a NULL group_id, **or remove_all false with no group instance id
supplied**)."

That third condition is exactly the case where `keys` (built independently,
before `remove_members_options` even runs) is **empty**.
`admin_async_per_key_op`'s "submission failed" fallback is
`for key in keys { complete(key, Err(e), ..) }` — with zero keys this fires
**zero times**, not "for every key." The actor's own test
(`bindings/c/tests/test_mock_admin.c`, using `wait_briefly_for_nothing`)
asserts exactly zero callback fires for this input, directly contradicting
the doc comment a few hundred lines away in the same commit.

This matters beyond wording: Java's `RemoveMembersFromConsumerGroupOptions(Collection)`
throws `IllegalArgumentException("Invalid empty members has been provided")`
**synchronously** for this exact input — a real, well-defined error (unlike
`alterConsumerGroupOffsets`'s empty-map case, which Java treats as trivially
successful). Since the async C entry point is `void`-returning and the
per-key callback is the *only* error channel, a raw C caller who doesn't
pre-validate client-side (Python already avoids this by validating in
`_remove_members_from_consumer_group_keys_and_spec` before ever reaching C)
gets **no callback and no error return** for this input — silent nothing,
indistinguishable from "nothing to do." The doc comment as written would
mislead a C caller into believing they'll be told about it.

Note this was isolated, not systemic: the sibling functions in the same
commit (`alter_consumer_group_offsets_async`, `delete_consumer_group_offsets_async`)
each correctly append a closing sentence ("When count is 0, the callback is
never invoked") overriding the earlier "fires synchronously" list for their
own empty-input case. `remove_members_from_consumer_group_async` was just
missing the equivalent correction.

**Resolution**: split the doc comment's "runs synchronously ... for every
key" bullet so it no longer lists "remove_all false with no group instance id
supplied" as a synchronous-fan-out case, and added a new closing paragraph
carving that case out explicitly:

> When `remove_all` is false and no group instance id is supplied, there is
> no per-member key at all (the same "no per-key slot" case
> `alter_consumer_group_offsets_async`/`delete_consumer_group_offsets_async`
> document), so the callback is never invoked — not even to report that the
> request could not be submitted. It does **not** fall into the
> "synchronously, for every key" case above, because there is no key for it
> to fire for.

Comment-only fix, no behavior change (the actual behavior was already
correct and tested — only the doc comment was wrong about what a raw C
caller observes). `cargo build --features ffi` and `cargo xtask format-check`
re-verified clean after the edit.

The optional suggestion (giving a raw C caller a way to detect this specific
synchronous-Java-error case through the async entry point) was left
unaddressed, as the critic noted was acceptable — the corrected doc comment
alone closes this finding.

## 2. [Informational, no action required this phase] Stale comment in `src/common/kafka_future.rs` — acknowledged, deferred

`register_completion`'s default-impl doc comment (~line 63) says "nothing
currently calls `when_complete` on one of them [combinator futures]." Phase
E's shared-future family (`then_apply_try`-derived `ThenApplyFuture`s
registered via `admin_async_per_key_op`) is the first real caller of
`when_complete` on a combinator future, so this comment is now stale. Low
severity, no behavioral impact (the default spawn-based path works
correctly). This file was not touched by Phase E's diff. Per the critic's own
framing this needs no action in this phase — left for whoever next touches
`src/common/kafka_future.rs`.

## What checked out clean (no action needed)

- The review brief's own framing of which 3 RPCs are "genuinely
  independent" vs. "shared-future" was WRONG, not the actor's work — verified
  directly against Java source. The correct split is 4 genuine independent
  (describeConsumerGroups, describeClassicGroups, deleteConsumerGroups,
  listConsumerGroupOffsets) vs. 3 shared-future (alterConsumerGroupOffsets,
  deleteConsumerGroupOffsets, removeMembersFromConsumerGroup). The actor's
  own commit messages and implementation already reflect this correctly.
- `then_apply_try`-derived per-key views for the 3 shared-future RPCs
  correctly propagate the source future's error unchanged to every view on
  failure, while computing success per-key — internal consistency preserved,
  no fake independence introduced.
- `admin_async_per_key_op` reused byte-for-byte verbatim, no fork.
- Nested value handles (`ConsumerGroupDescriptionInner`/
  `ClassicGroupDescriptionInner`/`OffsetAndMetadataMapInner`) reused verbatim
  across sync/async provenances.
- `RemoveMembersFromConsumerGroupOptions::new`'s empty-check matches Java's
  `RemoveMembersFromConsumerGroupOptions.java:34` exactly, correctly scoped
  to only this one RPC.
- Dedup direction correct for all applicable RPCs (first-occurrence-wins or
  an equivalent no-independence-violation mechanism).
- MockAdminClient translations for `UnsupportedOperationException`-throwing
  Java methods are faithful (per-key exceptional futures via
  `complete_with_error`), including matching Java's own typo
  ("Not implement yet").
- A suspected independence violation in `alterConsumerGroupOffsets`'s
  fail-whole-batch-on-first-bad-offset parsing was investigated and ruled
  out — this is the correct translation of Java's own map-literal
  construction throwing before the RPC is ever called, not a shared-future
  bug.
- Scope discipline clean.
- Commit-granularity deviation (all 7 RPCs' Rust changes in one commit
  instead of the requested split) — noted, not blocking.
- Full build/test suite re-run and matches all claimed numbers (4152 Rust-ffi
  tests, 177/177 `test_mock_admin`, 370/2 Python).
