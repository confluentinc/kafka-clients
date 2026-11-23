## Summary

Milestone 18: the Java share consumer (KIP-932) and its dependency closure, translated from
**Apache Kafka 4.4.0** to Rust, with the Admin share-group methods. Rust only: the C FFI and the
Python, C and .NET bindings come in a later milestone.

- **Public API** (`confluent_kafka::consumer`): the `ShareConsumer` trait (the methods that block
  in Java return `impl Future + Send`), its dyn-compatible `DynShareConsumer`, `KafkaShareConsumer`,
  `MockShareConsumer`, `ShareConsumerHandle` (Rust-only: `wakeup` from another task while `poll`
  holds the consumer), `AcknowledgeOptions` / `AcknowledgeOptionsBuilder`, `AcknowledgeType` and
  `AcknowledgementCommitCallback`. The KIP-714 methods are left out, as on `Consumer` (decision D2).
- **Admin** (`confluent_kafka::admin`): `describe_share_groups`, `list_share_group_offsets`,
  `alter_share_group_offsets`, `delete_share_group_offsets`, `delete_share_groups`, each with its
  `_with_options` form, their options, results and value types, and the five handlers.
  `MockAdminClient` returns "Not implemented yet" for all five, as Java's mock does.
- **Internals**: the share wire wrappers; acknowledgements; the share session handler; the fetch
  path (`ShareCompletedFetch` reads through the record-batch decoder now shared with
  `CompletedFetch`, zero-copy, allocation-budget tested); `ShareConsumeRequestManager`;
  the share membership and heartbeat managers on the abstract managers; eight share application
  events; `ShareConsumerImpl` on the consumer's single background task. The design rules are in
  `.claude/rules/share-consumer.md`.
- **KIP-1222 renew** (decision D5, option (c), provisional): an in-flight batch keeps one
  refcounted handle on its raw bytes, and a renewed record is decoded and deserialized again when
  it is redelivered. Java hands back the same object. Proposed contract change, see below.
- **Tests**: every Java unit test of the closure is translated or skipped with a reason
  (`ledger.md` §2), and 113 of the 115 methods of the seven `clients-integration-tests` share
  suites. The harness gained per-broker properties, Kafka's published 4.4.0 test jars (downloaded
  and checked by SHA-256, never committed), broker meter reads over JMX, broker-release gates, and
  a CI leg on an apache/kafka 4.4.0 broker (`make -C rust test-integration-broker-4.4`).

## Consumer behaviour changes

These commits change the regular consumer. Their fixup messages are lost on squash, so the
complete list is here (also in `rust/CHANGELOG.md` under `[Unreleased]`).

1. **`AbstractMembershipManager#revokePartitions` translated** (`5b7977401` and fixups). The
   consumer had never translated it (PLAN §8 explains why). What changes:
   - An INFO log "Revoking previously assigned partitions [...]" precedes each revocation, and
     "The pause flag in partitions [...] will be removed due to revocation." names the paused
     partitions whose pause flag the revocation drops.
   - On `unsubscribe` (or subscribing to an empty list) while still in the group (member epoch
     > 0), the released partitions are marked pending revocation before `onPartitionsRevoked`
     runs: fetching for them stops and in-flight fetch responses for them are discarded. Positions
     are unchanged, so a commit inside the callback still works. `close` is unaffected (it calls
     the listener directly, as Java does).
   - As a result, inside that `onPartitionsRevoked`, `position()` on a released partition that
     has no position yet waits until `default.api.timeout.ms` and returns a timeout error instead
     of fetching one, as in Java.
   - A failed `onPartitionsRevoked` logs Java's ERROR line ("onPartitionsRevoked callback
     invocation failed for partitions [...]"), during a rebalance and during `unsubscribe`.
   - Two more steps are translated for parity and change no outcome on any reachable path:
     revoking only the partitions still assigned when the revocation runs, and skipping
     `onPartitionsRevoked` for a member in the fatal state.
2. **Java's assignment order, end to end** (`50e83a71f` and fixups). After a rebalance the
   consumer holds its partitions sorted by topic name, then partition (Java's
   `TopicIdPartitionSet#topicPartitions()`, kept by `SubscriptionState#assignFromSubscribed`'s
   `LinkedHashMap`). That order decides the order fetch requests list partitions in. It used to be
   random and changed from one assignment to the next. A manual `assign()` is unchanged.

Everything else in shared consumer code is behaviour-neutral for the consumer, each in its own
commit: the abstract membership and heartbeat managers reshaped for a second subclass (P4a,
`3923672ad`..`50c4735f6`, and `114534c6e`), the record-batch decoder extracted from
`CompletedFetch` (`7deea5d8a`), a response hook after every network poll that no consumer manager
overrides (`f22581ee2`), `AbstractConfig`'s pre-process hook returning a `Result` (crate-private,
`3a05f902d`), crate-private helpers reused by the share consumer (`41ed87c26`), and a corrected
`run_once` phase-order comment (`c0ddf76bb`). No public consumer signature changes.

## Admin: a breaking change for `Admin` implementers

The five `_with_options` share methods are required methods of the `Admin` trait, as they are
abstract in Java's `Admin`, and as the trait's other `_with_options` methods are. Code that implements
`Admin` itself must add them. See the open decisions.

## Phases

| Phase | Content |
|---|---|
| P0 | Rules (`share-consumer.md`, the R1–R8 pilot checklist), shared-infra audit, coverage ledger, broker-harness spike, R1–R8 backtest |
| P1 | Wire wrappers: ShareFetch, ShareAcknowledge, ShareGroupHeartbeat, ShareGroupDescribe, `ShareRequestMetadata`, Describe/Alter/DeleteShareGroupOffsets, with byte-level vectors |
| P2 | `AcknowledgeType`, `AcknowledgementCommitCallback`, `ShareAcknowledgementMode`, `ShareAcquireMode`, `ShareConsumerConfig`, `Acknowledgements`, `AcknowledgementBatch`, `NodeAcknowledgements`, `AcknowledgementCommitCallbackHandler` |
| P3a | `ShareSessionHandler`, `ShareFetchConfig`, `ShareInFlightBatch` (D5), `ShareCompletedFetch`, the decoder shared with `CompletedFetch` |
| P3b | `ShareFetch`, `ShareFetchBuffer`, `ShareFetchCollector`, the three share fetch metrics classes |
| P4a | Abstract membership and heartbeat managers reshaped for a second subclass (behaviour-neutral) |
| P4b | `ShareMembershipManager`, `ShareHeartbeatRequestManager`, `ShareRebalanceMetricsManager`, `ShareConsumerMetadata`; `revokePartitions` (consumer behaviour change 1) |
| P5 | `ShareConsumeRequestManager` and its 76 tests; Java's assignment order (consumer behaviour change 2) |
| P6 | Share events and processor arms, `RequestManagers` share slots and supplier, the response hook, `WakeupTrigger` |
| P7 | `ShareConsumerImpl`, `KafkaShareConsumerMetrics`, cancellation-safe collect and close |
| P8 | Public API: `ShareConsumer`, `DynShareConsumer`, `KafkaShareConsumer`, `MockShareConsumer`, `ShareConsumerHandle`, `AcknowledgeOptions` |
| P9 | Admin share-group RPCs, `KafkaAdminClientShareGroupTest` |
| P10 | Integration harness and the seven share suites, on 4.2.0 and 4.4.0 brokers |
| P11 | Close-out: coverage and deferral audits, post-4.4.0 Java check, docs, rules-promotion proposal |

Each phase ran an Actor/Critic loop until the Critic had no open comments.

## Where to review

- `design/current/Milestone-18-share-consumer/PLAN.md` **§8.1**: 27 deviations from the consumer
  design, for review at this PR; each is to keep or revert. §8 lists the consumer gaps found
  during the milestone and their dispositions (consumer follow-ups, out of this PR).
- `ledger.md`: the Java→Rust file and test map (every row done or skipped with a reason) and the
  deferral ledger. `method-coverage.md`: every Java method of each translated class mapped to its
  Rust function or a reason.
- `rules-promotion.md`: which of the piloted R1–R8 rules to promote, with proposed text.
- `design/current/doc-contract-changes.md`: two proposed contract changes.

## Tests

Local gate on the branch head (macOS, Docker VM 7.75 GiB), 2026-11-23:

| Suite | Result |
|---|---|
| `make -C rust verify`: format-check, check-generated, lint | pass |
| Library unit tests (`--all-features`) | 6,571 passed, 3 ignored (about 670 test functions added by this milestone) |
| `integration` binary, apache/kafka 4.2.0 (default) | 559 passed, including all 130 share integration tests (the ones gated to 4.3+ / 4.4 skip themselves); 2 producer transaction tests failed in the gate run while the Docker VM's OOM killer killed a broker JVM, and passed on a rerun alone |
| `integration`, apache/kafka 4.4.0 (`make -C rust test-integration-broker-4.4`) | 134 passed: the share suites (every 4.3+ / 4.4-gated test runs here) plus the gated bounce and quota tests. The OOM killer killed one 4.4.0 broker JVM during the run; no test failed |
| `admin`, `consumer`, `producer` test targets, doctests | 5, 35, 8 and 25 passed |
| `xtask` and `generator` | 569 passed |
| `test-integration-perf` | 15 passed |

Coverage: the 95 Java main files of the 4.4.0 closure each map to a Rust file or a skip reason, every
Java unit test of the closure is translated or skipped with a reason, and 113 of the 115 Java share
integration test methods run as 130 Rust tests (`ledger.md` §2, §5).

## Open decisions for the user

1. **PLAN §8.1**: keep or revert each of the 27 deviations. Three carry an explicit question:
   - row 5 / D5: the provisional renew scheme (team review at the D5 artifact) and its proposed contract change;
   - row 17: keep Java's loss of the acknowledgements `pollForFetches` took when the collector fails (and report it upstream), or send them as the drop guard does;
   - row 19: re-enable wakeups when a `close` is dropped. That would change both consumers.
2. **D11, the share fixes Java landed after 4.4.0** (`post-4.4.0-java.md`). Three trunk commits apply and are not ported. KAFKA-21558 is a real correctness fix: acknowledgements piggy-backed for a node that lost its share session never complete, so their callback never runs. The other two are the accumulated renew-acknowledgement check and KAFKA-19804, the initial heartbeat interval, which the consumer shares. Port them now, or after a release carries them?
3. **Two proposed contract changes** in `design/current/doc-contract-changes.md`, both with `Accepted` empty: `ShareConsumer::poll` (a timeout over `i64::MAX` ms waits without limit) and `AcknowledgeType::Renew` (redelivered as a new, re-deserialized record).
4. **Rules** (`rules-promotion.md`, D4): promote R1–R8, as proposed or not. Also four drafted edits: share-consumer.md's settled decisions, documentation.md §8's `errors` line, admin-client.md §9's "cite the Java method", and two stale CLAUDE.md lines.
5. **`Admin` and semver at 1.x**: the five required `_with_options` share methods break external `impl Admin`. That is faithful to Java and to the trait's existing shape. CLAUDE.md §3 asks that what is minor in Java stay minor in Rust; giving them default bodies would avoid the break, as a policy for every future `Admin` method.
6. **Consumer follow-ups** (PLAN §8): owned by "consumer follow-up, out of M18" with no named milestone. Add KAFKA-21455's `notifyAssignmentChange` in `clearAssignmentAndLeaveGroup`, found in the post-4.4.0 check.
7. **Harness memory**: the Docker VM's OOM killer (`global_oom`) killed one broker JVM in the 4.2.0 gate run, where it failed two producer transaction tests that passed on a rerun, and one in the 4.4.0 run, with no failure. The peak was 26 containers / 6.9 GiB of the VM's 7.75 GiB. The decision on the harness was deferred.
8. **Master**: `origin/master` is 5 commits ahead and merges without conflicts (`git merge-tree`). 11 files changed on both sides, among them the test harness (`tests/common`, `consumer_test.rs`, `Makefile`, `.semaphore/semaphore.yml`). Merge it with a merge commit before opening the PR and run the gate again.
9. **Archive**: move `design/current/Milestone-18-share-consumer/` to `design/history/` once §8.1 is reviewed, as Milestone 15 did.
10. **Naming, cosmetic**: `ShareRequestMetadata::do_next_epoch` (Java's static `nextEpoch(int)`, CLAUDE.md §2 would give `next_epoch_with_prev_epoch`) and the crate-private renames that drop `get` in `Acknowledgements`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
