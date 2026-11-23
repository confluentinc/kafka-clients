# Milestone 18 — Java share-consumer changes after 4.4.0 (P11)

Checked 2026-11-23 for PLAN §4 P11 and §5 risk 5. Nothing here is ported: each change is listed
with whether it applies to the Rust translation and what porting it would take. The user decides
what to port, and when.

Refs fetched into `kafka/`'s object store; the submodule's checkout stays at `4.4.0`
(`1156b2752a`):

- `apache/4.4` = `9a50f30f92`: 89 commits after `4.4.0`, **one** touching the share closure
  (test-only on the client side), none touching the shared consumer files the share code uses.
- `apache/trunk` = `2919c28394` (branch point with 4.4: `3afec2a52d`): 30 commits touching the
  closure; 6 of them were already cherry-picked into 4.4.0. `git diff 4.4.0 apache/trunk` over the
  closure is fully explained by the commits below.

Closure searched: `consumer/internals/Share*`, `ShareSession*`, `ShareFetch*`, `Acknowledg*`,
`NodeAcknowledgements`, `events/Share*`, `metrics/*Share*`, `consumer/*ShareConsumer*`,
`AcknowledgeType`, `AcknowledgementCommitCallback`, `common/requests/Share*` and `*ShareGroup*`,
`admin/*Share*`, `admin/internals/*Share*`, the share message specs, the
`clients-integration-tests` share suites, and the shared files with share arms
(`RequestManagers`, `ApplicationEventProcessor`, `AbstractMembershipManager`,
`AbstractHeartbeatRequestManager`, `SubscriptionState`, `WakeupTrigger`, `ConsumerNetworkThread`,
`NetworkClientDelegate`, `ConsumerUtils`, `ConsumerConfig`, `KafkaAdminClient`, `MockAdminClient`).

## 1. Correctness fixes that apply (trunk only, none on the 4.4 branch yet)

| # | Commit | Change | Rust today | Port |
|---|---|---|---|---|
| F1 | `5e15b16468` KAFKA-21558 *Resilience improvements on leadership change* (2026-11-17) — **high** | Three bugs in `ShareConsumeRequestManager`: (a) acknowledgements piggy-backed for a node that lost its share session were never completed, so their callback never ran (the exactly-once callback obligation, CLAUDE.md §11.5); (b) after a redirect whose leader epoch did not advance, the consumer kept fetching from the old leader; (c) a leader change while the cached leader was unreachable but still in metadata never replaced the cached leader. | `share_consume_request_manager.rs` has the 4.4.0 behaviour: `maybe_update_leader_cache` compares with a strict `>` and returns nothing; no `forgetLeadersOnNode` / `failAcknowledgementsForNodesWithoutSession` counterparts; partition errors without leader info and `UNKNOWN_TOPIC_*` request no metadata update; the result of `update_partition_leadership` is ignored | ~150 lines of main code: `resolveShareSessionLeaders`, `failAcknowledgementsForNodesWithoutSession` at the end of the fetch `poll`, `forgetLeadersOnNode` in the ShareFetch and ShareAcknowledge failure handlers, `maybeUpdateLeaderCache` with `>=` returning `bool`, a private `updatePartitionLeadership` wrapper requesting a metadata update when the redirect is refused, `requestUpdate(false)` on the leaderless per-partition errors on both paths, and the dead top-level `UNKNOWN_TOPIC_ID` refresh removed. Tests: `ShareConsumeRequestManagerTest` changed by 883 lines, about 12 new or renamed tests (`testDisconnectWithoutLeadershipChangeRetriesSameNode`, `testShareFetchFollowsNewerLeaderEpochInMetadata`, `testPiggybackAcksQueuedAfterNodeDisappearsAreFailed`, `testLeaderChangeWithUnchangedEpochUpdatesShareSessionLeader`, …; `testUnknownTopicIdError` removed) |
| F2 | `99b940733a` MINOR *Accumulate renew ack checks from all nodes* (2026-10-01) — medium | For a `commitSync` or `close` spanning several nodes, whether renew acknowledgements are notified was decided by the node that answered last, so they could go un-notified. | `ResultHandler::complete` passes the per-call flag: the pre-fix behaviour | ~10 lines: an accumulated flag on `ResultHandler`, set on every non-`CommitAsync` completion and passed to `maybeSendShareAcknowledgementEvent`; one test, `testCommitSyncRenewOnOneNodeAcceptOnAnother(boolean)` |
| F3 | `43c014d5df` KAFKA-19804 *Improve heartbeat request manager initial HB interval* (2026-10-13) — low/medium | A busy loop at start-up while the first heartbeat is in flight: the initial heartbeat interval is now `request.timeout.ms`, not 0. | `abstract_heartbeat_request_manager.rs` still builds `HeartbeatRequestState` with an initial interval of 0. **Shared:** the consumer and the share consumer both have it | ~5 lines and a comment; the `testInitialHeartbeatIntervalUsesRequestTimeout` tests (share and consumer) and the `AbstractHeartbeatRequestManagerTest` rework. A consumer behaviour change too, so it would go in its own labelled commit |

Found alongside, not a share fix: `d9be0ece7d` KAFKA-21455 adds `notifyAssignmentChange(emptySet)`
to `AbstractMembershipManager#clearAssignmentAndLeaveGroup`; the Rust base lacks it. Only
`AsyncKafkaConsumer` listens to `onGroupAssignmentUpdated`, so it is a consumer follow-up.

## 2. The 4.4 branch

| Commit | Change | Verdict |
|---|---|---|
| `7413fd803f` KAFKA-21197 *Preserve original record headers in share group DLQ records* (cherry-pick of trunk `2d35188650`) | Broker fix (`ShareGroupDLQStateManager`); adds `ShareConsumerDLQTest#testDlqCopiesOriginalRecordHeaders` and `#testDlqOriginalHeaderCollidingWithDlqHeaderIsOverwritten` | Test-only for the client. The two tests could join `share_consumer_dlq_test.rs`, gated on a 4.4.1+ broker, once that image exists |

## 3. Trunk commits that do not apply

- **4.5 features**: KIP-1313 request header v3 and the client instance id (`0606659cc1`,
  `515a8c50c8`, `e90d6f42c2`; inter-broker share-state RPCs `b235414c8f`, `eab19942aa`); the
  `SubscriptionState` split into `ConsumerSubscriptionState` / `ShareSubscriptionState`
  (`bfb092bbc0`, `aec3926e7b`; checked: no share behaviour change); KIP-1388 (`e74cae57d0`); KIP-1332
  preparation (`500b73c097`); the `RebalanceListener` rename (`6bc6d9b851`); public config
  constants (`b9c4fef95d`).
- **Broker-side**: partition rotation for share fetches (`d193373f24`), share fetch offset
  metadata (`ea6afaa461`), DLQ helper extraction (`a94c289255`).
- **Cosmetic or test-only, nothing to port**: `9740c8e324` (`@Override`), `72bbbc3bbc` (removes
  code Rust never had), `c40d0c9b30`, `c9da36e270`, `62f0fef0e8`, `5ce7e34250`, `934a0c17af`,
  `53abf0e80d` (test reshuffles).
- **Test improvements that could be taken later**: `8de9414907` (tighter
  `testSubscriptionAndPollFollowedByTopicDeletion`), `e28414ee4a` (`testComplexShareConsumer`
  flushes every 100 records).
- **Already in 4.4.0**: `2f3d864e96`, `d37b17e490` (KAFKA-21106), `4d781f506a`, `db6f147797`,
  `1807f7e846`, and on the shared files `7ff5d7b71c` (KAFKA-20854), `e5e26abb27` (KAFKA-20187).
- **Shared files, consumer-only or no share effect**: `cf93eefad7` KAFKA-21293 (Rust's
  `SubscriptionState` has no listener field), `a486c38c53`, `cd69d2ebd0` (Admin javadoc),
  `05f61b9ff5` (KRaft voters).
