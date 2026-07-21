---
name: review-m9-phase1-patterns
description: M11 KIP-932 share wire layer review — test-fidelity gaps vs faithful impl; loop-restructuring equivalence; omitted nested Validator
metadata:
  type: project
---

Milestone 11 Phase 1 (share wire protocol layer) review findings and heuristics.

**Where the defects actually were (Phase 1): test fidelity, not runtime logic.**
The wire wrappers (ShareFetch/ShareAcknowledge Request/Response, ShareRequestMetadata),
the ShareSessionHandler add/forget/replace/epoch machine, and the Acknowledgements
batch-optimisation were all correct and wire-faithful. Generated JSON specs were
byte-identical to vendored `kafka/.../common/message/Share*.json` (verify with a diff
loop; the Actor's commit claims this — confirm it). `build()` → `latest_allowed_version`
matches Java `build()`→`build(latestVersion())`.

**Finding pattern 1 — Actor writes its own weaker tests instead of translating the
Java test vectors.** `AcknowledgementsTest.java` (20 methods with EXACT
firstOffset/lastOffset/size assertions on the optimise-split boundaries) was replaced by
~10 hand-written Rust tests using loose `batches.iter().any(...)` checks. The impl was
correct, but such tests would not catch an off-by-one in the split arithmetic they exist
to guard. **How to apply:** when a Java `*Test.java` exists, map each Java `@Test` method
name to a Rust `fn test_*` and confirm the ASSERTIONS match (exact values, and repeated
second-call idempotency checks like Java's `ackList2`), not just that "some tests exist."
DoD §3 requires faithful translation or explicit skip rationale.

**Finding pattern 2 — nested Java class member silently dropped.** `ShareAcquireMode`
has a nested `Validator implements ConfigDef.Validator` (ensureValid + toString →
`[batch_optimized, record_limit]`); the Rust enum omitted it, dropping `testValidator`
and `testValidatorToString`. The project has no `ConfigDef::Validator` trait at all
(grep finds none), so full parity needs missing infra — deferral defensible but must be
documented. Config keys like `share.acquire.mode` are stored as raw String in
consumer_config.rs and only validated later in `from_consumer_config` (Java validates at
config-construction). Flag undocumented member/test drops under DoD §2/§3.

**Equivalence heuristic that saved a false positive.** Java `for(int i=1;i<n;i++)` with a
fall-through `i++` after an inner `while(i++)` was translated to a Rust `while` with
per-branch `i += 1` and NO increment in the exceeds-limit branch. Looks divergent. Trace
it: after `current_start_index = i`, the Rust form takes one extra iteration at
`i == current_start_index`, but `types[i] != types[i-1]` is guaranteed at that boundary
(the inner while broke there), so it always hits the else arm → no-op, then advances.
Net state identical to Java. Restructured loops need a boundary-state trace, not a
textual-shape rejection.

**Non-finding to remember:** `ShareAcquireMode::of()` message repeats the value string in
both "value" and "configuration" slots — this MATCHES Java `of(String)` which passes its
single arg into both positions. Not a bug.
