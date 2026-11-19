# Critic 75 — M15/P5 (groups and consumer-group offsets)

Reconstructed retroactively at phase close-out (2026-09-18) for archive
consistency with P1/P2a/P2b. Both review passes reported findings inline
per maintainer instruction rather than through the `COMMENTS.75.md` loop;
this file records what was found and fixed, not a live working log.

## Pass 1 — full-phase review (all nine RPCs), against commit `1ae8efbc`

**75.1 (Low) — `ListConsumerGroupsOptions.States` silently narrowed an
undefined `ConsumerGroupState` to `Unknown` instead of throwing.**
Fixed: now throws `ArgumentOutOfRangeException`, matching the
reject-an-undefined-cast contract `GroupStates`/`Types` get from
`NativeAdminClient.FilterNames` at submit time. The existing test was
split into a throw test (write direction) and a kept
projects-to-`Unknown` test (read direction via `GroupStates`, unaffected).

**75.2 (Low) — two tests asserted an asynchronous throw via
`Assert.ThrowsAsync(() => call())`, a pattern that also passes for a
synchronous throw** (`PublicAdminDeleteConsumerGroupOffsetsTests`,
`PublicAdminRemoveMembersFromConsumerGroupTests`). Fixed: both now
capture the `Task` reference before asserting, so a regression to a
synchronous throw surfaces at the capture line instead of being
tolerated.

Both fixed in `518eb270`.

## Pass 2 — quick logic-only follow-up

**(Low) — `ListConsumerGroupsOptions`'s undefined-enum guard threw with
`ParamName == "states"`**, inconsistent with sibling patterns
(`NativeAdminClient.FilterNames`, `ListGroupsOptions.CopyOfProtocolTypes`),
which name the parameter the *caller* passed rather than an internal
helper's own parameter. Fixed: renamed to `value`. Fixed in `01457176`.

Status: all findings closed. Both fixes are folded into the squashed
phase commit `feb47f04`.
