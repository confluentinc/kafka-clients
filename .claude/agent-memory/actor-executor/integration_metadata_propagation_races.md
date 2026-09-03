---
name: integration-metadata-propagation-races
description: Why create_topic's propagation wait cannot cover coordinator-routed RPCs on a multi-broker cluster, which admin RPC families are exposed, and the retry idiom that fixes it
metadata:
  type: project
---

`create_topic` (both the `&dyn Admin` and `AdminBackend` copies) waits via
`describe_topics` reporting the expected partition count. That proves **one
arbitrary broker** has the topic, not all of them: the by-names `describeTopics`
`Call` goes through `NodeProvider::LeastLoaded`, so the answering broker is
whichever is least loaded.

Java's `TestUtils.waitForAllPartitionsMetadata` is
`brokers.forall { _.metadataCache.numPartitions(topic) == n }`
(`core/src/test/scala/unit/kafka/utils/TestUtils.scala:832-853`) — it reads every
broker's cache directly. **A client cannot reproduce that check**, so no pre-call
wait closes the window on a multi-broker cluster.

## Which RPCs are actually exposed

Classify by *who validates the partition*:

  - **Coordinator-routed** (`OffsetCommit` via `alterConsumerGroupOffsets`,
    `OffsetDelete`): exposed. `KafkaApis.handleOffsetCommitRequest` rejects a
    partition with `UNKNOWN_TOPIC_OR_PARTITION` when
    `metadataCache.getLeaderAndIsr` finds it absent **on the receiving broker**
    (`core/src/main/scala/kafka/server/KafkaApis.scala:313-335`). The receiving
    broker is the coordinator, hashed from the group id over
    `__consumer_offsets` — usually *not* the broker `create_topic` watched.
  - **Controller-routed** (`electLeaders`, `alterPartitionReassignments`,
    `createPartitions`): safe. The controller *is* the metadata log's source of
    truth, so it cannot lag it.
  - **Partition-leader-routed** (`listOffsets`, `deleteRecords`): safe in
    practice — the driver re-resolves the leader on
    `UNKNOWN_TOPIC_OR_PARTITION`.
  - Anything preceded by a real consumer `subscribe_and_join` or a producer
    send: safe, those take seconds and retry metadata internally.

So the shape to grep for is **create_topic → admin offset commit with no
consumer/producer in between**, not "create_topic → uses a partition".

## Waiting for a leader does NOT help

`KRaftMetadataCache.getLeaderAndIsr` returns a value whenever the topic +
partition exist in the image, **even with `leader = -1`**
(`core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala:361-367`).
Its absence therefore means "partition missing from that broker's image", not
"election pending". A leader wait narrows nothing here — don't reach for it.

## The fix idiom

Retry the *operation*, per Java's other tool for this,
`TestUtils.retryOnExceptionWithTimeout` (already translated in
`tests/common/test_utils.rs`). Scope the retry to the one propagation error and
let everything else panic straight out of the loop — the helper catches the `Err`
return, **not** panics, which gives Java's `NoRetryException` short-circuit for
free without adding machinery. That keeps a genuine defect failing in seconds
instead of at the 60s bound, and leaves `all_of_exactly`'s completeness check
unweakened.

Retrying an offset commit is sound: when no requested partition validates,
`KafkaApis` builds the whole response itself and never reaches the coordinator
(`KafkaApis.scala:343-346`), so no group is created and no offset stored.

Note the client is *correct* not to retry this itself — Java's
`AlterConsumerGroupOffsetsHandler` deliberately treats
`UNKNOWN_TOPIC_OR_PARTITION` as a terminal per-partition result
(`clients/.../internals/AlterConsumerGroupOffsetsHandler.java:192-198`). The wait
belongs in the test.

## A wrapper's exit status is not evidence the wrapped work happened

**Rule:** before believing a batch/build/verification run, check an artifact the
*work itself* had to produce (a summary file with the expected line count, a
per-run log, a test-result line) — never the wrapper's exit code alone.

**Why:** hit twice in this milestone, from two different directions. (1) A
backgrounded `script.sh > out/driver.log` reported exit 0 while doing *nothing*:
`out/` did not exist, the redirect failed before the script ran, and the 0 came
from the trailing `echo`. (2) An image-build script printed "OK" for a C++
compile failure because the pipeline's status came from `tail`, not the compiler.
In both cases a green wrapper hid zero work.

**How to apply:** `mkdir -p` redirect targets before backgrounding anything; make
the batch script itself write a `TOTAL pass=N fail=M of N` line and verify N
matches what you asked for; prefer `set -o pipefail` or capture `${PIPESTATUS[0]}`
over trusting a pipeline's exit code. When reporting to someone who cannot see
your shell, give the absolute artifact path so they can verify independently.

## Two teeth checks worth repeating for any retry helper

1. Point it at a partition that will never exist → must fail at the bound with
   the last failure, proving the loop is live and cannot pass vacuously.
2. Force a *different* error in the same per-partition switch arm (a >4096-byte
   metadata string gives `OFFSET_METADATA_TOO_LARGE`) → must fail on the first
   attempt in seconds, proving the retry really is scoped.
