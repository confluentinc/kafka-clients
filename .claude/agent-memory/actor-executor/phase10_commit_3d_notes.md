---
name: phase10-commit-3d-notes
description: Phase 10 (3d/N) — OffsetsRequestManager `tryConnect` plumbing via `PollResult::try_connect`; bg task drains the slot in commit 7
metadata:
  type: project
---

Phase 10 commit 3d translated the one Java `tryConnect(node)` site in
`OffsetsRequestManager.sendOffsetsForLeaderEpochRequestsAndValidatePositions`
(Java line 739). The pattern is reusable for any future request manager
that needs the `tryConnect` side-effect.

## Pattern: emit connection hint on `PollResult`

Java's request managers hold a direct `NetworkClientDelegate` handle and
call `networkClientDelegate.tryConnect(node)` synchronously inside their
`poll(...)`. The Rust bg task owns the delegate, so the manager cannot
make that call directly.

**Solution**: add a `pub try_connect: Vec<Node>` slot to
`NetworkClientDelegate::PollResult` (default empty). When the manager
discovers a node without `NodeApiVersions`, it pushes the node onto a
manager-owned `Mutex<Vec<Node>>` (e.g. `OffsetsManagerShared::try_connect_queue`)
and at `poll(now)` time drains it into the returned `PollResult::try_connect`.

The bg task (Phase 10 commit 7) drains `try_connect` BEFORE
`add_all_from_poll_result` and calls `delegate.try_connect(node, now).await`
per entry.

## Why a manager-side queue and not direct emit-on-build

`sendOffsetsForLeaderEpochRequestsAndValidatePositions` is called from
within `validate_positions_if_needed` (which holds `&mut self`), but the
listener-replay path (`replay_retries_after_metadata_update`) is invoked
via `&Arc<OffsetsManagerShared>` from inside `poll()` BEFORE the manager
collects `requests_to_send`. Putting the queue on `OffsetsManagerShared`
matches the `requests_to_send` / `requests_to_retry` ownership shape
and is uniformly drainable from any code path.

## Test parity

Translated `testValidatePositionsAbortIfNoApiVersionsToCheckAgainstThenRecovers`
from `OffsetsRequestManagerTest.java`. Java's assertion is only on
`requestManager.requestsToSend()` (count) plus the negative `verify(...)
.setNextAllowedRetry(...)`; it does NOT verify the `tryConnect` mock
interaction. The Rust translation does observe `PollResult::try_connect`
because we now have a visible artifact for the previously-invisible
side effect — strictly stronger test, behavior-faithful to Java.

Java helpers `subscriptionState.partitionsNeedingValidation(...)` and
`subscriptionState.position(...)` are mocked; the Rust translation uses
the real `SubscriptionState`: assign the partition + `seek_unvalidated`
with a `FetchPosition::with_leader(..)` to push it into
`AWAITING_VALIDATION`.

## Java-vs-Rust API note: `add_all_from_poll_result`

`NetworkClientDelegate::add_all_from_poll_result(poll_result, now)`
ignores the `try_connect` slot — bg task is expected to call
`delegate.try_connect(node, now).await` BEFORE `add_all_from_poll_result`
(this is the slot's consumer; commit 7 wires it).

## Deferred to commit 7

- Actual `delegate.try_connect(node, now).await` calls from the
  ConsumerNetworkThread `runOnce` skeleton.
- Per-iteration ordering of try_connect drain vs. unsent_requests dispatch
  (Java order: `tryConnect` then `addAll`; preserve in commit 7).
