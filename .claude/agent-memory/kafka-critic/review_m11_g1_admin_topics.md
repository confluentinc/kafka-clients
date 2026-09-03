---
name: review-m11-g1-admin-topics
description: M11 G1 admin multilanguage review — FFI-collapses-a-Java-distinction class, guess_variant blast radius, prove-non-change-by-waiting, Java-citation-must-exist
metadata:
  type: project
---

Slice G1 of the admin multilanguage gRPC harness (`37b6f41a..17f28136`, six
topic/partition RPCs, 15 scenarios × 4 backends). Review appended to
`COMMENTS.1.md` round 13; G0 closures in `COMMENTS.DONE.1.md`.

**Why:** these are recurring defect *classes* on this branch, not one-off bugs —
each was found twice or more, so future admin/harness slices should be probed for
them first.

**How to apply:**

1. **"The C FFI collapses a Java-legal input distinction" is a class, now 3 deep.**
   `NewTopic.replicas_assignments` (harmless, wire non-nullable, *disclosed*),
   `NewPartitions.newAssignments` (harmful, wire nullable, *not* disclosed), and
   the `KafkaError` discriminator itself. The tell: an `Option`/`.map()` in
   `tests/common/multilanguage_admin.rs` faithfully encoding presence, plus an
   `if x.is_empty() { non_assignment_ctor() }` in a `src/ffi/admin.rs` `*Builder::build()`.
   Decide reachability from `generator/messages/*.json` `nullableVersions`, not
   from the Java type — a nullable `List` whose wire field is non-nullable is a
   distinction that does not exist. Both bindings funnel through the same FFI
   builder, so one collapse = 3-vs-1, never 2-vs-2.

2. **Broadening a heuristic has a blast radius the commit will not mention.**
   `60a9334f` hoisted a 2-site message-text variant guesser to all 34
   `fill_proto_error` sites. The fix was correct and Python-identical (verified by
   differential fuzz), but `kafka_error_from_proto`
   (`tests/common/multilanguage_producer.rs`) discards `p.code` for every
   non-Generic variant, so every error whose *default message* matches a pattern
   (7 of them, incl. `Errors::RequestTimedOut` = "The request timed out.") now
   loses its code on the C backend. Check both directions: does the encoder now
   guess more, and does the decoder drop what it used to keep?

3. **A single immediate negative probe does not prove non-creation.** The
   `validate_only` scenario asserts `UNKNOWN_TOPIC_OR_PARTITION` right after the
   call, on a surface the same file documents twice as eventually consistent.
   Java proves it with `waitForAllPartitionsMetadata(..., expected = original)`.
   Any "X did not happen" assertion on metadata needs a bounded wait on
   *unchanged* state.

4. **Java citations on added (non-converted) tests are checkable claims.** One of
   three added-scenario citations was fabricated
   (`PlaintextAdminIntegrationTest.testCreateTopicsWithValidateOnly` does not
   exist; only `ReplicationControlManagerTest.testCreateTopicsWithValidateOnlyFlag`
   does). Grep every cited method name — the citation is the DoD #3 evidence the
   test is in scope.

5. **Read the reviewed commit, not the working tree.** HEAD had already advanced
   two slices (G2, then G3 mid-review, by a concurrently-running Actor). Extract
   with `git show <commit>:<path>` before citing line numbers, and re-diff
   `<commit>..HEAD` for each file to confirm which findings are attributable.
   Subagents will silently read the working tree unless told otherwise.

6. **`flock` does not exist on macOS.** Use `python3 -c "import fcntl; ..."` to
   take the exclusive lock on `COMMENTS.<N>.md` that agent-roles.md requires.

7. **Guard-pattern asymmetry is worth reporting even when dead.** `5351b238` added
   three "impossible null" guards; two report an error, the third (`listTopics`)
   `continue`s — a false success with reduced cardinality — and the commit message
   claims all three report. The `continue` form then propagated into G2. Fix
   patterns in the slice that introduces them.

8. **Conversion audits: compare assertion-by-assertion against
   `git show <base>:<file>`.** G1 replaced 9 committed tests with generic
   scenarios; nothing was lost (verified), but the check is cheap and it is the
   single highest-value thing in a conversion slice. Watch for
   `assert!(helper(..).await, "msg")` becoming a bare `helper(..).await` — verify
   the helper panics internally and keeps the same bound.
