// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pluggable verification.
//!
//! Workloads do not know how their output is checked: they emit
//! [`WorkloadEvent`]s into a [`Verifier`], and the runner asks the verifier
//! for a [`ChaosVerdict`] at drain. This mirrors librdkafka's design where the
//! workload prints JSON event lines (`{"e":"consumed",...}`) and `chaos.py`
//! parses them into its own verification map — except here the events are typed
//! and in-process instead of serialized over a pipe.
//!
//! The seam is symmetric to [`super::workload::Workload`]: swap the workload to
//! change what runs, swap the verifier to change what is checked. A future
//! share consumer emits the extra `Acked` / `DeliveryCount` variants and a
//! `ShareAckVerifier` interprets them, with no change to the producer path or
//! the orchestrator.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use confluent_kafka::common::Uuid;

/// One thing a workload observed. New variants may be added without touching
/// existing workloads or verifiers — a verifier ignores variants it does not
/// care about (the open-enum property that makes verification pluggable).
///
/// Two identities travel on each event:
///   - `index`: our monotonic logical id, stamped into the message key by the
///     producer and read back by the consumer. Stable across retries, partition
///     reassignment, and topic recreate — the basis of the conservation check.
///     librdkafka has no equivalent (its perf tool embeds no logical id).
///   - `(topic_id, partition, offset)`: the broker's physical address. Catches
///     anomalies `index` cannot see — a duplicated payload written at a *new*
///     offset, offset gaps, and topic-recreate generation collisions (offset
///     resets to 0 after a recreate, so `topic_id` disambiguates the old and
///     new generations — the reason librdkafka keys on the base64 topic id).
#[derive(Debug, Clone)]
#[allow(dead_code)] // Acked / DeliveryCount land with the share consumer.
pub enum WorkloadEvent {
    /// Producer `producer` is about to hand `index` to the client. This opens
    /// the record's in-flight window; the matching `Delivered` or `SendFailed`
    /// closes it. The verifier uses these events to report each producer's
    /// in-flight peak and to verify that every window is closed by verdict
    /// time.
    Sent {
        index: u64,
        topic: String,
        producer: String,
    },
    /// Producer received a broker ack for `index` at this physical address.
    /// `topic_id` is the generation current when the ack arrived (see
    /// `delivery_callback` in `workload.rs`).
    Delivered {
        index: u64,
        topic: String,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
    },
    /// Producer's send failed: rejected before enqueue or not acknowledged
    /// within the delivery timeout. Any failed send fails the run (see
    /// `ConservationState::failed_sends`).
    SendFailed { index: u64, topic: String, error: String },
    /// A consumer operation returned an error. Recorded for the verdict's error
    /// summary; consumer errors are reported with their text and count, not
    /// scored, because the consumer loop retries them and the conservation
    /// check is what decides whether they had an effect.
    ConsumerError {
        consumer: String,
        op: ConsumerOp,
        error: String,
    },
    /// Consumer `consumer` received `index` at this physical address.
    /// `topic_id` is best-effort: `Uuid::zero()` when the consumer could not
    /// resolve it (single-topic non-recreate runs do not need it).
    Consumed {
        consumer: String,
        index: u64,
        topic: String,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
    },
    /// The consumer's `ConsumerRebalanceListener` callback `callback` fired for
    /// `partitions`, as `(topic, partition)` pairs. Recorded at the start of
    /// the callback, so event order is callback-invocation order. Drives the
    /// rebalance-contract checks (see `ConservationState::owned`).
    Rebalance {
        consumer: String,
        callback: RebalanceCallback,
        partitions: Vec<(String, i32)>,
    },
    /// Consumer `consumer` finished `close()`. A consumer with a listener must
    /// have released every partition (revoked or lost callback) by then — Java's
    /// `runRebalanceCallbacksOnClose` guarantees it.
    ConsumerClosed { consumer: String },
    /// Consumer `consumer` is about to call `close()`. Its close-time callbacks
    /// may legitimately release partitions it was never told it owned (see
    /// `ConservationState::closing`).
    ConsumerClosing { consumer: String },
    /// The broker reports `offset` as the group's committed offset for
    /// `(topic, partition)`, read back by `consumer` via `committed()` right
    /// after one of its own `commit_sync` calls succeeded (with no poll in
    /// between). Compared with that consumer's consumption since it was
    /// assigned the partition — see `ConservationState::record_committed`.
    Committed {
        consumer: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Share consumer acknowledged `index`; `err = None` on success. (Future.)
    Acked {
        index: u64,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        err: Option<i32>,
    },
    /// Broker-reported redelivery count for `index` (KIP-932 `dc`). (Future.)
    DeliveryCount { index: u64, count: u32 },
}

/// The consumer operation that produced a [`WorkloadEvent::ConsumerError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConsumerOp {
    Poll,
    Commit,
    /// The `commit_sync` an application issues inside `on_partitions_revoked`
    /// to flush offsets before the partitions move (the canonical listener
    /// pattern, done through a `ConsumerHandle`).
    RevokeCommit,
    /// The `committed()` read-back the workload issues after a successful sync
    /// commit to feed [`WorkloadEvent::Committed`].
    ReadCommitted,
}

impl std::fmt::Display for ConsumerOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ConsumerOp::Poll => "consumer poll",
            ConsumerOp::Commit => "consumer commit",
            ConsumerOp::RevokeCommit => "consumer commit inside on_partitions_revoked",
            ConsumerOp::ReadCommitted => "consumer committed() read-back",
        })
    }
}

/// Which `ConsumerRebalanceListener` callback fired (see
/// [`WorkloadEvent::Rebalance`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RebalanceCallback {
    /// `on_partitions_revoked`: a clean handoff, the member acknowledged the
    /// revocation and the broker may now reassign the partitions.
    Revoked,
    /// `on_partitions_assigned`: the member now owns the partitions.
    Assigned,
    /// `on_partitions_lost`: the partitions were taken without a clean
    /// handoff (the member was fenced); they may already belong to another
    /// member.
    Lost,
}

impl std::fmt::Display for RebalanceCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RebalanceCallback::Revoked => "on_partitions_revoked",
            RebalanceCallback::Assigned => "on_partitions_assigned",
            RebalanceCallback::Lost => "on_partitions_lost",
        })
    }
}

/// Hint to a verifier that a chaos action legitimately destroyed data, so the
/// affected records must not be scored as loss.
#[derive(Debug, Clone)]
pub enum ExpectedLossHint {
    /// A topic recreate destroyed everything delivered so far, across every
    /// topic. Used by single-topic runs (`--num-topics 1`).
    AllDeliveredSoFar,
    /// A topic recreate destroyed everything delivered so far **for one named
    /// topic only** — the multi-topic case (`--num-topics > 1`), where a
    /// recreate picks a single topic (librdkafka's `rng.choice(topics)`). Loss
    /// on the other topics is still real loss.
    AllDeliveredForTopic(String),
    /// `topic` underwent a recreate — mark it as a **recreate blackout** topic.
    /// Noted once the old topic is gone and before the new one is created, so
    /// no new-generation record can be acknowledged before it. Records
    /// delivered up to that point are the old generation; records delivered
    /// after it went into the new one, whose partitions restart at offset 0
    /// while the consumer can still carry its old-generation position or
    /// committed offset (on a KIP-848 broker the group's committed offsets are
    /// deleted with the topic, and come back only through the client
    /// re-committing its stale position, F2), so it skips the new generation's
    /// low offsets. The verifier excuses exactly that skipped range: a
    /// new-generation record on a partition is expected-lost only if it sits
    /// BELOW the offset at which the consumer first resumed on that partition.
    /// The resume point is frozen at the first post-recreate record observed,
    /// so later loss on the topic (a broker roll two cycles on) stays real
    /// loss, and a partition the consumer never resumes on excuses nothing.
    /// This is the per-topic analog of librdkafka's pre-delete-HWM →
    /// post-recreate expected-loss window, and it holds whether the recreate
    /// reused the topic id (immediate), minted a new one (delayed), or the id
    /// could not be resolved under churn.
    RecreateBlackout(String),
    /// A topic recreate replaced generation `id` with a new one, destroying
    /// every record acked to `id`; the delete began at `deleted_at`. At
    /// verdict, a delivered-but-unobserved record of a destroyed generation is
    /// expected-lost only if it was **unread when the generation was deleted**:
    /// the consumer had not yet reached it on its partition, and it was
    /// acknowledged within the tail window (`DESTROYED_TAIL_WINDOW`) before
    /// the delete. Everything else in a destroyed generation is loss:
    ///
    /// - a record below the highest offset the consumer read on that partition
    ///   is a gap the consumer skipped;
    /// - an older record the consumer never reached is loss. Its partition sat
    ///   unread for longer than any healthy lag: stuck.
    ///
    /// Excusing every record of the generation, as this once did, hid whole
    /// stuck partitions as "expected lost" (Sep 2026 matrix, F9).
    ///
    /// Unlike the point-in-time snapshots (`AllDeliveredForTopic`), this is
    /// evaluated against the `topic_id` recorded WITH each delivered record. The
    /// producer stamps that id when the ack arrives, from a map the harness
    /// switches to the new generation's id (taken from the create-topics
    /// response) before the producer can be acked by the new generation. So
    /// with thousands of records in flight across the recreate, a record
    /// buffered during the delete or retried into the new generation carries
    /// the new id and stays in the loss check, while only records the old
    /// leader actually acked carry `id`. Only emitted when the recreate minted
    /// a genuinely new id; a recreate that reuses the id relies on the
    /// snapshots and `RecreateBlackout`.
    DestroyedGeneration { id: Uuid, deleted_at: Instant },
    /// A topic recreate created generation `id` (a new id). The head of such a
    /// generation that the consumer skipped past is excused like the
    /// `RecreateBlackout` window, but decided by the record's acknowledged
    /// generation instead of an index floor, so records acknowledged by the new
    /// generation before the harness noted the recreate are classified
    /// correctly (the floor race, F9).
    NewGeneration(Uuid),
}

/// How long before a generation's deletion an unread record of it may have
/// been acknowledged and still be excused as destroyed (see
/// [`ExpectedLossHint::DestroyedGeneration`]). Far above any healthy consumer
/// lag, below the lifetime of a generation stuck for a whole cycle.
pub const DESTROYED_TAIL_WINDOW: Duration = Duration::from_secs(60);

/// Records workload events and renders a pass/fail verdict. The harness owns a
/// single `Arc<dyn Verifier>`; every workload writes into it; the runner reads
/// the verdict once at drain.
pub trait Verifier: Send + Sync {
    /// Record one workload event.
    fn record(&self, event: WorkloadEvent);

    /// Note that a chaos action destroyed data (default: no-op, for verifiers
    /// that do not run under topic chaos).
    fn note_expected_loss(&self, _hint: ExpectedLossHint) {}

    /// A monotonically non-decreasing count of consume events seen so far.
    /// Used only for idle-based drain: when this stops growing, the consumers
    /// have caught up. Default 0 (a verifier that does not track consumption
    /// makes the drain fall back to the full fixed window).
    fn consumed_progress(&self) -> u64 {
        0
    }

    /// A monotonically non-decreasing count of consume events seen so far for
    /// ONE topic. The drain uses this to detect that the consumer has resumed
    /// re-reading a just-recreated topic — the GLOBAL [`Self::consumed_progress`]
    /// cannot tell one topic apart (it keeps climbing on the other topics while
    /// the recreated one is still rewinding). Default 0.
    fn consumed_progress_for_topic(&self, _topic: &str) -> u64 {
        0
    }

    /// Delivered records that would currently be scored as loss (`Some(0)` = the
    /// drain may end). `None` = not tracked → drain falls back to idle/fixed.
    fn outstanding(&self) -> Option<usize> {
        None
    }

    /// Render the final verdict.
    fn verdict(&self, min_partitions: usize) -> ChaosVerdict;
}

/// Physical record address: `(topic_id, partition, offset)`.
type PhysKey = (Uuid, i32, i64);

/// Logical record identity: `(topic, index)`.
///
/// The producer restarts `index` at 0 for each topic (one producer per topic),
/// so `index` alone is NOT unique across topics — topic `t0` index 5 and topic
/// `t1` index 5 are two distinct records. Keying the conservation maps on
/// `(topic, index)` keeps them apart. For a single-topic run this collapses to
/// the old `index`-only behaviour (one topic name in every key).
type LogicalKey = (String, u64);

/// The default verifier: conservation + per-record bookkeeping on both the
/// logical `index` and the physical `(topic_id, partition, offset)` key. This
/// carries the checks the harness has always done, now dual-keyed.
#[derive(Default)]
pub struct ConservationVerifier {
    inner: Mutex<ConservationState>,
}

#[derive(Default)]
struct ConservationState {
    /// (topic, index) -> was it broker-acked (must be consumed unless
    /// expected-lost).
    delivered: HashMap<LogicalKey, PhysKey>,
    /// (topic, index) -> times the consumer observed it (>1 = redelivery).
    observed: HashMap<LogicalKey, u32>,
    /// (topic, index) -> the distinct physical addresses at which the consumer
    /// observed it. A single address is the expected case, and a re-read of the
    /// same address does not add one. Two addresses within one `topic_id`
    /// indicate that the producer committed the record twice, which
    /// `enable.idempotence=true` is required to prevent. See
    /// [`ConservationState::double_writes`].
    observed_at: HashMap<LogicalKey, Vec<PhysKey>>,
    /// Physical addresses the consumer has seen — a second sighting of the same
    /// (topic_id, partition, offset) is a physical duplicate.
    phys_seen: HashMap<PhysKey, u32>,
    /// (topic, partition) pairs that carried at least one observed record
    /// (coverage). Scored per topic — see [`ConservationState::partitions_covered`].
    partitions_seen: BTreeSet<(String, i32)>,
    /// (topic, index) pairs legitimately destroyed by a topic recreate (not
    /// loss).
    expected_lost: BTreeSet<LogicalKey>,
    /// topic -> the highest index delivered on it when its recreate completed
    /// (the `RecreateBlackout` hint; `None` if nothing had been delivered yet).
    /// Records at or below the floor predate the recreate: they are covered by
    /// the pre-delete snapshots / destroyed generation, and one that surfaces
    /// from the consumer's fetch buffer *after* the recreate is an
    /// old-generation record, not progress on the new one. Records above the
    /// floor were produced into the new generation.
    /// See [`ExpectedLossHint::RecreateBlackout`].
    blackout_floor: HashMap<String, Option<u64>>,
    /// (topic, partition) -> offset of the FIRST new-generation record (index
    /// above the floor) the consumer observed on that partition after the
    /// recreate: where it resumed. Frozen at first observation, so it cannot
    /// drift upward as the run continues. New-generation records delivered to
    /// that partition at a lower offset were skipped in the blackout and are
    /// excused; anything at or above it, and every record on a partition the
    /// consumer never resumed on, is real loss.
    blackout_resume: HashMap<(String, i32), i64>,
    /// Topic ids of destroyed generations, with when their delete began. See
    /// [`ExpectedLossHint::DestroyedGeneration`] for which of their unobserved
    /// records are excused.
    destroyed_generations: HashMap<Uuid, Instant>,
    /// Topic ids of generations a recreate created. See
    /// [`ExpectedLossHint::NewGeneration`].
    new_generations: BTreeSet<Uuid>,
    /// When each delivered record was acknowledged (when its `Delivered` event
    /// was recorded). Decides the destroyed-generation tail window.
    delivered_at: HashMap<LogicalKey, Instant>,
    /// `(topic_id, partition) -> offset of the first record the consumer
    /// observed there`, for identified (non-zero) generations: where it started
    /// reading that incarnation of the partition. Frozen at first observation.
    first_consumed: HashMap<(Uuid, i32), i64>,
    /// `(topic_id, partition) -> highest offset the consumer observed there`,
    /// for identified (non-zero) generations.
    max_consumed: HashMap<(Uuid, i32), i64>,
    /// Overrides [`DESTROYED_TAIL_WINDOW`] (tests).
    destroyed_tail_window: Option<Duration>,
    /// Producer sends that failed, with the client's error text. Any entry fails
    /// the run: the producer runs with `acks=all`, idempotence, a 60 s metadata
    /// block and a 120 s delivery timeout, and no fault in the matrix keeps a
    /// partition unavailable for anywhere near that long, so a correct client
    /// retries through every fault and fails nothing. A failure therefore
    /// indicates a client defect or an environment problem, and a failure after
    /// the delivery timeout is also ambiguous (the record may or may not have
    /// been written), which the verifier cannot resolve.
    failed_sends: Vec<(LogicalKey, String)>,
    /// Consumer poll and commit errors as (consumer label, operation, error
    /// text). Reported in the verdict's error summary, not scored.
    consumer_errors: Vec<(String, ConsumerOp, String)>,
    /// Total consume events seen (incl. redeliveries) — the drain-progress
    /// signal read by `consumed_progress`.
    consumed_events: u64,
    /// Per-topic consume-event count (incl. redeliveries) — the per-topic
    /// drain-progress signal read by `consumed_progress_for_topic`, used to
    /// detect that a just-recreated topic's consumer has resumed.
    consumed_events_by_topic: HashMap<String, u64>,
    /// (topic, index) currently in flight (`Sent`, but not yet `Delivered` or
    /// `SendFailed`), mapped to the producer that sent it. The settling event
    /// carries no producer identity, so it is attributed through this map. This
    /// relies on the logical key being unique across producers, the same
    /// invariant the conservation maps depend on (see [`LogicalKey`]).
    in_flight: HashMap<LogicalKey, String>,
    /// producer -> records currently in flight.
    in_flight_by_producer: HashMap<String, u32>,
    /// producer -> the maximum number of records it had in flight at any one
    /// time. Reported in the verdict (not scored): the producer workload sends
    /// without awaiting each outcome, so the peak shows how deep the client's
    /// pipeline got during the run.
    max_in_flight_by_producer: HashMap<String, u32>,
    /// (topic, partition, topic id) -> offset of the most recent
    /// acknowledgement on that incarnation of the partition, in event order.
    last_delivered_offset: HashMap<(String, i32, Uuid), i64>,
    /// (topic, partition, topic id) -> highest offset the consumer observed on
    /// that incarnation of the partition. Compared with `last_delivered_offset`
    /// in the per-partition loss report to show where the consumer stopped.
    /// Keyed by incarnation: an offset from another incarnation says nothing
    /// about this one, and mixing them made the known-defect label depend on
    /// which incarnation happened to be longer (F9).
    max_consumed_offset: HashMap<(String, i32, Uuid), i64>,
    /// consumer -> partitions it currently owns according to its own rebalance
    /// callbacks: `on_partitions_assigned` adds, `on_partitions_revoked` /
    /// `on_partitions_lost` remove. Replayed from [`WorkloadEvent::Rebalance`]
    /// to check the listener contract (see [`ConservationState::record_rebalance`]).
    owned: HashMap<String, BTreeSet<(String, i32)>>,
    /// How many times each callback fired, across all consumers.
    rebalance_callbacks: BTreeMap<RebalanceCallback, usize>,
    /// Consumers that fired at least one rebalance callback (i.e. registered a
    /// listener). gRPC-backed consumers register none.
    listener_consumers: BTreeSet<String>,
    /// `(previous owner, partition) -> new owner`: the partition was assigned to
    /// `new owner` while `previous owner` still owned it per its callbacks. The
    /// broker only does that after the previous owner was fenced, so the
    /// previous owner must eventually report the partition as *lost*. Reporting
    /// it *revoked* instead is a contract violation (a clean handoff claimed
    /// after the partition had already moved). Resolved by either callback.
    overlaps: HashMap<(String, (String, i32)), String>,
    /// Rebalance-contract violations, in the order detected. Any fails the run.
    rebalance_violations: Vec<String>,
    /// Consumers that have begun `close()` ([`WorkloadEvent::ConsumerClosing`]).
    /// Java-faithful: when a rebalance hands a closing member new partitions,
    /// reconciliation records them in the assignment before
    /// `on_partitions_assigned` runs, and close then revokes the whole
    /// assignment and skips the pending assigned callback
    /// (`AbstractMembershipManager.java:516-519`,
    /// `AsyncKafkaConsumer.java:1606-1636`). So a closing consumer may release
    /// partitions it was never told it owned. That is counted, not failed.
    closing: BTreeSet<String>,
    /// Release callbacks from a closing consumer for partitions it did not own.
    close_time_unowned_releases: usize,
    /// `(consumer, (topic, partition)) -> offset of the most recent record that
    /// consumer received on the partition since it was last assigned it`
    /// (cleared by `on_partitions_assigned`). What a committed offset read
    /// back by that consumer is compared against.
    consumed_since_assign: HashMap<(String, (String, i32)), i64>,
    /// Committed offsets that were actually compared (the consumer had
    /// consumed from the partition since assignment).
    commit_checks: usize,
    /// Committed-offset violations (ahead of or behind the consumer's own
    /// consumption), in the order detected. Any fails the run.
    commit_violations: Vec<String>,
    /// Committed offsets not compared because another consumer owned the same
    /// partition name at the time, on a recreated topic. The two owners hold
    /// different incarnations. The broker stores one committed offset per name,
    /// so the read-back may be the other owner's (F10b).
    ambiguous_commit_checks: usize,
    /// `(producer, (topic, partition)) -> (index, offset)` of the highest-index
    /// record that producer has had acknowledged on the partition. One
    /// idempotent producer appends to one partition in send order, so a later
    /// index must land at a higher offset and vice versa. See
    /// [`ConservationState::record_ordering`].
    producer_last_ack: HashMap<(String, (String, i32)), (u64, i64)>,
    /// Ordering violations that fail the run: a consumer read an older offset
    /// after a newer one within a single assignment of a partition, or a
    /// producer's acknowledged offsets on a partition disagree with its send
    /// order. Only on topics that were never recreated.
    order_violations: Vec<String>,
    /// The same anomalies observed on a recreated topic, where an offset reset
    /// can legitimately look like a regression. Reported, not scored.
    unscored_order_anomalies: usize,
}

impl ConservationVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    /// A verifier with a different destroyed-generation tail window (tests).
    #[cfg(test)]
    fn with_destroyed_tail_window(window: Duration) -> Self {
        let v = Self::default();
        v.inner.lock().expect("verifier poisoned").destroyed_tail_window = Some(window);
        v
    }
}

impl ConservationState {
    /// Replay one rebalance callback against the ownership model and record
    /// every contract violation it exposes:
    ///
    /// - `on_partitions_assigned` for a partition the consumer already owns:
    ///   a second assigned callback with no revoked/lost in between (the
    ///   client fired assigned twice, or skipped the revocation).
    /// - `on_partitions_revoked` / `on_partitions_lost` for a partition the
    ///   consumer does not own: a release callback for something it was never
    ///   told it had (or told twice).
    /// - `on_partitions_revoked` for a partition another consumer had already
    ///   been assigned while this one still owned it: the broker moves a
    ///   partition without waiting for the owner only when the owner is fenced,
    ///   and a fenced member must be told `on_partitions_lost`. A revoked
    ///   callback here means the client presented a clean handoff that did not
    ///   happen.
    fn record_rebalance(&mut self, consumer: String, callback: RebalanceCallback, partitions: Vec<(String, i32)>) {
        *self.rebalance_callbacks.entry(callback).or_insert(0) += 1;
        self.listener_consumers.insert(consumer.clone());
        match callback {
            RebalanceCallback::Assigned => {
                for partition in partitions {
                    let already = self.owned.get(&consumer).is_some_and(|set| set.contains(&partition));
                    if already {
                        self.rebalance_violations.push(format!(
                            "{consumer}: on_partitions_assigned for {} which it already owned (no revoked/lost in \
                             between)",
                            tp_label(&partition)
                        ));
                    }
                    // Ownership is keyed by topic NAME (that is all a rebalance
                    // callback carries), but the coordinator keys by topic id. A
                    // recreated topic's new-id partitions have no owner, so they
                    // are assigned at once while the old owner's clean revoke of
                    // the old-id partitions of the same name can land later. That
                    // is not a fence, so the overlap check is suspended for
                    // recreated topics.
                    let recreated = self.blackout_floor.contains_key(&partition.0);
                    let previous_owners: Vec<String> = self
                        .owned
                        .iter()
                        .filter(|(other, set)| !recreated && **other != consumer && set.contains(&partition))
                        .map(|(other, _)| other.clone())
                        .collect();
                    for previous in previous_owners {
                        self.overlaps.insert((previous, partition.clone()), consumer.clone());
                    }
                    // A fresh assignment starts consumption on this partition
                    // over: the committed offset the consumer inherits is the
                    // previous owner's, not a statement about its own progress.
                    self.consumed_since_assign.remove(&(consumer.clone(), partition.clone()));
                    self.owned.entry(consumer.clone()).or_default().insert(partition);
                }
            },
            RebalanceCallback::Revoked | RebalanceCallback::Lost => {
                for partition in partitions {
                    let owned = self.owned.get_mut(&consumer).is_some_and(|set| set.remove(&partition));
                    if !owned && self.closing.contains(&consumer) {
                        // Released by close without an assigned callback first;
                        // Java-faithful, see `closing`.
                        self.close_time_unowned_releases += 1;
                    } else if !owned {
                        self.rebalance_violations.push(format!(
                            "{consumer}: {callback} for {} which it did not own",
                            tp_label(&partition)
                        ));
                    }
                    if let Some(new_owner) = self.overlaps.remove(&(consumer.clone(), partition.clone()))
                        && callback == RebalanceCallback::Revoked
                    {
                        self.rebalance_violations.push(format!(
                            "{consumer}: on_partitions_revoked for {} after {new_owner} had already been assigned it \
                             (a clean handoff reported after the partition moved; expected on_partitions_lost)",
                            tp_label(&partition)
                        ));
                    }
                }
            },
        }
    }

    /// Compare a committed offset the broker reported for `consumer` on
    /// `partition` with that consumer's own consumption since it was assigned
    /// the partition. Java's `commitSync()` commits the *position* of every
    /// assigned partition, i.e. last consumed offset + 1, and the workload
    /// reads back only right after a successful sync commit with no poll in
    /// between, so the two must match exactly:
    ///
    /// - committed > consumed + 1: the commit ran ahead of consumption. On a
    ///   crash or handoff the next owner starts past records nobody processed
    ///   (loss).
    /// - committed < consumed + 1: the commit fell behind. The next owner
    ///   re-reads records this consumer already processed (duplication).
    ///
    /// A partition the consumer has not consumed from since assignment is
    /// skipped: its committed offset is the previous owner's, and there is
    /// nothing of this consumer's to compare it with. So is a partition of a
    /// recreated topic that another consumer also owns: see
    /// `ambiguous_commit_checks`.
    fn record_committed(&mut self, consumer: String, partition: (String, i32), committed: i64) {
        let Some(&consumed) = self.consumed_since_assign.get(&(consumer.clone(), partition.clone())) else {
            return;
        };
        if self.blackout_floor.contains_key(&partition.0)
            && self
                .owned
                .iter()
                .any(|(other, set)| *other != consumer && set.contains(&partition))
        {
            self.ambiguous_commit_checks += 1;
            return;
        }
        self.commit_checks += 1;
        let expected = consumed + 1;
        if committed != expected {
            let kind = if committed > expected { "ahead of" } else { "behind" };
            self.commit_violations.push(format!(
                "{consumer}: committed offset {committed} on {} is {kind} its own consumption (last consumed \
                 {consumed}, expected {expected})",
                tp_label(&partition)
            ));
        }
    }

    /// A consumer finished `close()`. If it registered a listener it must have
    /// released everything first: Java's `runRebalanceCallbacksOnClose` fires
    /// `on_partitions_revoked` (or `on_partitions_lost` when fenced) for the
    /// whole assignment before leaving the group.
    fn record_closed(&mut self, consumer: &str) {
        if let Some(set) = self.owned.get(consumer)
            && !set.is_empty()
        {
            let held: Vec<String> = set.iter().map(tp_label).collect();
            self.rebalance_violations.push(format!(
                "{consumer}: closed while still owning {held:?} (no on_partitions_revoked / on_partitions_lost before \
                 close)"
            ));
        }
    }

    /// Close `key`'s in-flight window (its `Delivered` / `SendFailed` arrived).
    /// A key that was never `Sent` is ignored: event streams without `Sent`
    /// (older workloads, the verifier's own unit tests) remain valid.
    fn settle(&mut self, key: &LogicalKey) {
        if let Some(producer) = self.in_flight.remove(key)
            && let Some(n) = self.in_flight_by_producer.get_mut(&producer)
        {
            *n = n.saturating_sub(1);
        }
    }

    /// File an ordering anomaly: scored on a topic that was never recreated,
    /// only counted on one that was (its partitions' offsets restarted, so a
    /// regression there can be the reset rather than a client defect).
    fn record_ordering(&mut self, topic: &str, description: String) {
        if self.blackout_floor.contains_key(topic) {
            self.unscored_order_anomalies += 1;
        } else {
            self.order_violations.push(description);
        }
    }

    /// Producer-side ordering: `producer`'s ack for `index` landed at `offset`
    /// on `(topic, partition)`. Against the highest-index record it already had
    /// acknowledged there, a higher index must have a higher offset and a lower
    /// index a lower one — an idempotent producer appends to a partition in
    /// send order, and `max.in.flight` with idempotence keeps it that way
    /// across retries. Equal offsets for two indexes would be a double write,
    /// caught separately.
    fn record_producer_ack_order(&mut self, producer: String, topic: &str, partition: i32, index: u64, offset: i64) {
        let key = (producer, (topic.to_string(), partition));
        match self.producer_last_ack.get(&key) {
            Some(&(last_index, last_offset)) => {
                let index_forward = index > last_index;
                let offset_forward = offset > last_offset;
                if index_forward != offset_forward {
                    self.record_ordering(
                        topic,
                        format!(
                            "{}: ack for index {index} on {topic}-{partition} at offset {offset}, but index {last_index} \
                             was acknowledged at offset {last_offset} (send order and log order disagree)",
                            key.0
                        ),
                    );
                }
                if index_forward {
                    self.producer_last_ack.insert(key, (index, offset));
                }
            },
            None => {
                self.producer_last_ack.insert(key, (index, offset));
            },
        }
    }

    /// Partition coverage, scored per topic: the number of distinct partitions
    /// the consumer read from on the LEAST-covered topic (0 when nothing was
    /// consumed). Counting `(topic, partition)` pairs across topics would let a
    /// fully-read topic mask another that was never read.
    fn partitions_covered(&self) -> usize {
        let mut per_topic: HashMap<&str, usize> = HashMap::new();
        for (topic, _) in &self.partitions_seen {
            *per_topic.entry(topic.as_str()).or_insert(0) += 1;
        }
        per_topic.values().copied().min().unwrap_or(0)
    }

    /// Whether `(topic, index)` is a record of a recreated topic's NEW
    /// generation (`Some(true)`), of its old one (`Some(false)`), or the topic
    /// was never recreated (`None`). See `blackout_floor`.
    fn recreate_generation(&self, topic: &str, index: u64) -> Option<bool> {
        self.blackout_floor.get(topic).map(|floor| floor.is_none_or(|f| index > f))
    }

    /// Is this unobserved new-generation record excused by the recreate
    /// blackout? Only when the consumer resumed on its partition at a HIGHER
    /// offset: the record sits in the range the stale committed offset made the
    /// consumer skip. A partition the consumer never resumed on excuses nothing.
    fn skipped_in_blackout(&self, topic: &str, partition: i32, offset: i64) -> bool {
        self.blackout_resume
            .get(&(topic.to_string(), partition))
            .is_some_and(|&resume| offset < resume)
    }

    /// Classify logical records that the consumer observed at more than one
    /// physical address.
    ///
    /// Returns `(double_writes, cross_generation)`:
    ///   - `double_writes`: records observed at two or more distinct addresses
    ///     that share a `topic_id`. The producer committed the same record twice
    ///     into one topic generation, which an idempotent producer must never
    ///     do. Each entry carries every address the record was observed at,
    ///     sorted, for the report.
    ///   - `cross_generation`: records whose addresses differ only in
    ///     `topic_id`. A topic recreate destroys the broker-side producer state
    ///     together with the old generation, so a record whose acknowledgement
    ///     was lost in the delete window is legitimately retried into the new
    ///     generation; the client cannot deduplicate across that boundary.
    ///     These are reported but do not fail the run.
    ///
    /// Consumer re-reads (the same address observed more than once) fall into
    /// neither category; they are covered by the `duplicates (by index)` and
    /// `duplicates (by offset)` counters and by the 2× ratio bound.
    fn double_writes(&self) -> (Vec<(LogicalKey, Vec<PhysKey>)>, usize) {
        let mut double_writes = Vec::new();
        let mut cross_generation = 0usize;
        for (key, addrs) in &self.observed_at {
            if addrs.len() < 2 {
                continue;
            }
            let mut per_generation: HashMap<Uuid, usize> = HashMap::new();
            for (topic_id, _, _) in addrs {
                *per_generation.entry(*topic_id).or_insert(0) += 1;
            }
            if per_generation.values().any(|&n| n >= 2) {
                let mut sorted = addrs.clone();
                sorted.sort_unstable();
                double_writes.push((key.clone(), sorted));
            } else {
                cross_generation += 1;
            }
        }
        double_writes.sort_unstable();
        (double_writes, cross_generation)
    }

    /// Whether a record belongs to a generation a recreate has already
    /// replaced: by its topic id when the recreate identified the generations
    /// (always, in practice: KRaft mints a new id per create), by the blackout
    /// floor otherwise.
    fn is_old_generation(&self, topic: &str, index: u64, topic_id: Uuid) -> bool {
        if self.destroyed_generations.contains_key(&topic_id) {
            return true;
        }
        if self.new_generations.contains(&topic_id) {
            return false;
        }
        self.recreate_generation(topic, index) == Some(false)
    }

    /// Whether `topic_id` is a generation a recreate identified (created or
    /// destroyed), so its records are judged by id rather than by the
    /// point-in-time snapshots and the index floor.
    fn is_identified_generation(&self, topic_id: Uuid) -> bool {
        topic_id != Uuid::zero()
            && (self.new_generations.contains(&topic_id) || self.destroyed_generations.contains_key(&topic_id))
    }

    /// Whether an unobserved delivered record is legitimately unconsumable
    /// because of a topic recreate. See [`ExpectedLossHint::DestroyedGeneration`]
    /// and [`ExpectedLossHint::NewGeneration`] for the id-based rules, and
    /// [`ExpectedLossHint::RecreateBlackout`] for the fallback.
    fn excused(&self, key: &LogicalKey, &(topic_id, partition, offset): &PhysKey) -> bool {
        if self.is_identified_generation(topic_id) {
            // The head of a recreated generation the consumer skipped past (it
            // started reading this incarnation of the partition above it).
            if self.new_generations.contains(&topic_id)
                && self
                    .first_consumed
                    .get(&(topic_id, partition))
                    .is_some_and(|&first| offset < first)
            {
                return true;
            }
            return self
                .destroyed_generations
                .get(&topic_id)
                .is_some_and(|&deleted_at| self.unread_at_deletion(key, topic_id, partition, offset, deleted_at));
        }
        if self.expected_lost.contains(key) {
            return true;
        }
        // A new-generation record on a recreated topic that the consumer
        // provably skipped (it resumed on that partition past it); see
        // `blackout_resume`.
        let (topic, index) = key;
        self.recreate_generation(topic, *index) == Some(true) && self.skipped_in_blackout(topic, partition, offset)
    }

    /// A record of a destroyed generation was unread when the generation was
    /// deleted: the consumer had not reached its offset on that partition,
    /// and it was acknowledged within the tail window before the delete.
    fn unread_at_deletion(
        &self,
        key: &LogicalKey,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        deleted_at: Instant,
    ) -> bool {
        let beyond_consumer = self.max_consumed.get(&(topic_id, partition)).is_none_or(|&c| offset > c);
        let window = self.destroyed_tail_window.unwrap_or(DESTROYED_TAIL_WINDOW);
        let recent = self.delivered_at.get(key).is_none_or(|&acked| acked + window > deleted_at);
        beyond_consumer && recent
    }

    /// Delivered records currently scored as loss (unobserved and not
    /// excused by a recreate; see [`Self::excused`]). Shared by `verdict` and
    /// `outstanding` so the drain waits for exactly what the verdict checks.
    fn lost_keys(&self) -> Vec<LogicalKey> {
        self.delivered
            .iter()
            .filter(|(key, addr)| !self.observed.contains_key(*key) && !self.excused(key, addr))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

impl Verifier for ConservationVerifier {
    fn record(&self, event: WorkloadEvent) {
        let mut s = self.inner.lock().expect("verifier poisoned");
        match event {
            WorkloadEvent::Sent { index, topic, producer } => {
                s.in_flight.insert((topic, index), producer.clone());
                let now = {
                    let n = s.in_flight_by_producer.entry(producer.clone()).or_insert(0);
                    *n += 1;
                    *n
                };
                let peak = s.max_in_flight_by_producer.entry(producer).or_insert(0);
                *peak = (*peak).max(now);
            },
            WorkloadEvent::Delivered { index, topic, topic_id, partition, offset } => {
                s.last_delivered_offset.insert((topic.clone(), partition, topic_id), offset);
                let key = (topic, index);
                s.delivered_at.insert(key.clone(), Instant::now());
                // The producer is known from the `Sent` that opened this
                // record's in-flight window (absent for event streams without
                // `Sent`, which skip the producer-order check).
                if let Some(producer) = s.in_flight.get(&key).cloned() {
                    s.record_producer_ack_order(producer, &key.0, partition, index, offset);
                }
                s.settle(&key);
                s.delivered.insert(key, (topic_id, partition, offset));
            },
            WorkloadEvent::SendFailed { index, topic, error } => {
                let key = (topic, index);
                s.settle(&key);
                s.failed_sends.push((key, error));
            },
            WorkloadEvent::Consumed { consumer, index, topic, topic_id, partition, offset } => {
                // The consumer stamps `topic_id` from the live id map at consume
                // time, so a record fetched from the old generation but polled
                // after the recreate switched the map carries the new id. The
                // producer stamps its ack with the generation that acked it, so
                // when the consumer saw the record at the very address the ack
                // named, the ack's id is the truth about that address.
                let topic_id = match s.delivered.get(&(topic.clone(), index)) {
                    Some(&(acked_id, acked_partition, acked_offset))
                        if acked_partition == partition && acked_offset == offset =>
                    {
                        acked_id
                    },
                    _ => topic_id,
                };
                // An old-generation record surfacing after the recreate (still
                // in the consumer's fetch buffer when the topic was replaced) is
                // not progress on the recreated partition, and not where the
                // consumer resumed.
                if !s.is_old_generation(&topic, index, topic_id) {
                    // Consumer-side ordering: within one assignment of a
                    // partition (the entry is cleared by
                    // `on_partitions_assigned` and by a recreate) offsets must
                    // strictly increase. Only for consumers with a listener:
                    // without `Assigned` events a re-read after a reassignment
                    // is indistinguishable from a defect.
                    let key = (consumer, (topic.clone(), partition));
                    if let Some(&previous) = s.consumed_since_assign.get(&key)
                        && offset <= previous
                        && s.listener_consumers.contains(&key.0)
                    {
                        s.record_ordering(
                            &topic,
                            format!(
                                "{}: read {topic}-{partition} offset {offset} after offset {previous} within one \
                                 assignment (fetch returned older data)",
                                key.0
                            ),
                        );
                    }
                    s.consumed_since_assign.insert(key, offset);
                    if s.recreate_generation(&topic, index) == Some(true) {
                        s.blackout_resume.entry((topic.clone(), partition)).or_insert(offset);
                    }
                }
                // Per-incarnation reading progress (every generation, old ones
                // included: a destroyed generation's excusal needs how far the
                // consumer got in it).
                if topic_id != Uuid::zero() {
                    s.first_consumed.entry((topic_id, partition)).or_insert(offset);
                    let max = s.max_consumed.entry((topic_id, partition)).or_insert(offset);
                    *max = (*max).max(offset);
                }
                let max = s
                    .max_consumed_offset
                    .entry((topic.clone(), partition, topic_id))
                    .or_insert(offset);
                *max = (*max).max(offset);
                *s.observed.entry((topic.clone(), index)).or_insert(0) += 1;
                let addr = (topic_id, partition, offset);
                let addrs = s.observed_at.entry((topic.clone(), index)).or_default();
                if !addrs.contains(&addr) {
                    addrs.push(addr);
                }
                *s.phys_seen.entry(addr).or_insert(0) += 1;
                s.partitions_seen.insert((topic.clone(), partition));
                s.consumed_events += 1;
                *s.consumed_events_by_topic.entry(topic).or_insert(0) += 1;
            },
            WorkloadEvent::ConsumerError { consumer, op, error } => {
                s.consumer_errors.push((consumer, op, error));
            },
            WorkloadEvent::Rebalance { consumer, callback, partitions } => {
                s.record_rebalance(consumer, callback, partitions);
            },
            WorkloadEvent::ConsumerClosing { consumer } => {
                s.closing.insert(consumer);
            },
            WorkloadEvent::ConsumerClosed { consumer } => {
                s.record_closed(&consumer);
            },
            WorkloadEvent::Committed { consumer, topic, partition, offset } => {
                s.record_committed(consumer, (topic, partition), offset);
            },
            // The conservation verifier does not interpret share-consumer
            // events; a ShareAckVerifier will.
            WorkloadEvent::Acked { .. } | WorkloadEvent::DeliveryCount { .. } => {},
        }
    }

    fn consumed_progress(&self) -> u64 {
        self.inner.lock().expect("verifier poisoned").consumed_events
    }

    fn consumed_progress_for_topic(&self, topic: &str) -> u64 {
        self.inner
            .lock()
            .expect("verifier poisoned")
            .consumed_events_by_topic
            .get(topic)
            .copied()
            .unwrap_or(0)
    }

    fn outstanding(&self) -> Option<usize> {
        Some(self.inner.lock().expect("verifier poisoned").lost_keys().len())
    }

    fn note_expected_loss(&self, hint: ExpectedLossHint) {
        let mut s = self.inner.lock().expect("verifier poisoned");
        match hint {
            ExpectedLossHint::AllDeliveredSoFar => {
                let all: Vec<LogicalKey> = s.delivered.keys().cloned().collect();
                s.expected_lost.extend(all);
            },
            ExpectedLossHint::AllDeliveredForTopic(topic) => {
                // Only the recreated topic's delivered records are expected-lost;
                // records on the other topics must still be consumed.
                let scoped: Vec<LogicalKey> = s.delivered.keys().filter(|(t, _)| *t == topic).cloned().collect();
                s.expected_lost.extend(scoped);
            },
            ExpectedLossHint::RecreateBlackout(topic) => {
                // Everything delivered so far predates the recreate; the
                // consumer's resume points and its per-partition progress on
                // this topic start over with the new generation (the recreate
                // reset the partitions' offsets, so progress measured against
                // the old generation would make the next committed offset look
                // wrong). A second recreate of the same topic re-arms both.
                let floor = s.delivered.keys().filter(|(t, _)| *t == topic).map(|(_, i)| *i).max();
                s.blackout_floor.insert(topic.clone(), floor);
                s.blackout_resume.retain(|(t, _), _| *t != topic);
                s.consumed_since_assign.retain(|(_, (t, _)), _| *t != topic);
            },
            ExpectedLossHint::DestroyedGeneration { id, deleted_at } => {
                s.destroyed_generations.insert(id, deleted_at);
            },
            ExpectedLossHint::NewGeneration(id) => {
                s.new_generations.insert(id);
            },
        }
    }

    fn verdict(&self, min_partitions: usize) -> ChaosVerdict {
        let s = self.inner.lock().expect("verifier poisoned");

        // Loss: a delivered (topic, index) the consumer never observed, not one a
        // topic recreate legitimately destroyed, and not one skipped below a
        // blackout topic's resume point. Shared with the drain's `outstanding`.
        let lost: Vec<LogicalKey> = s.lost_keys();

        // Logical duplicates: an index observed more than once.
        let logical_duplicates: u64 = s.observed.values().map(|&c| u64::from(c.saturating_sub(1))).sum();
        // Physical duplicates: the same (topic_id, partition, offset) seen more
        // than once — a broker/client double-delivery at a fixed address.
        let physical_duplicates: u64 = s.phys_seen.values().map(|&c| u64::from(c.saturating_sub(1))).sum();
        // Producer double writes: the same logical record committed at two
        // addresses within one topic generation. Unlike the two counters above,
        // which count consumer re-reads, this indicates a producer defect and
        // fails the run.
        let (double_writes, cross_generation_duplicates) = s.double_writes();

        let mut reasons = Vec::new();
        if !double_writes.is_empty() {
            // Render as `topic#index@[p<partition>/<offset>,...]` so the offsets
            // a record was committed at can be located in the broker logs.
            let sample_str: Vec<String> = double_writes
                .iter()
                .take(20)
                .map(|((t, idx), addrs)| {
                    let at: Vec<String> = addrs.iter().map(|(_, p, o)| format!("p{p}/{o}")).collect();
                    format!("{t}#{idx}@[{}]", at.join(","))
                })
                .collect();
            reasons.push(format!(
                "producer duplicates: {} record(s) committed at more than one offset within a topic generation \
                 (idempotence violated; sample: {sample_str:?})",
                double_writes.len()
            ));
        }
        let producer_duplicates: Vec<LogicalKey> = double_writes.into_iter().map(|(key, _)| key).collect();
        // Where the loss sits: per incarnation of each partition (topic,
        // partition, topic id), how many records were lost, the offset range
        // they occupy, and the last offset acknowledged against the highest
        // one consumed on that same incarnation. A partition whose consumer
        // stopped shows as "consumed" far below "last acked" (or absent), which
        // is a different picture from loss scattered across every partition.
        let mut by_partition: BTreeMap<(String, i32, Uuid), (usize, i64, i64)> = BTreeMap::new();
        for key in &lost {
            let (topic_id, partition, offset) = s.delivered[key];
            let entry = by_partition
                .entry((key.0.clone(), partition, topic_id))
                .or_insert((0, offset, offset));
            entry.0 += 1;
            entry.1 = entry.1.min(offset);
            entry.2 = entry.2.max(offset);
        }
        let lost_by_partition: Vec<LostPartition> = by_partition
            .into_iter()
            .map(|((topic, partition, topic_id), (count, first, last))| {
                let incarnation = (topic.clone(), partition, topic_id);
                LostPartition {
                    last_delivered_offset: s.last_delivered_offset[&incarnation],
                    last_consumed_offset: s.max_consumed_offset.get(&incarnation).copied(),
                    topic,
                    partition,
                    topic_id,
                    lost: count,
                    first_lost_offset: first,
                    last_lost_offset: last,
                }
            })
            .collect();
        if !lost.is_empty() {
            let mut sample = lost.clone();
            sample.sort_unstable();
            sample.truncate(20);
            // Render as `topic#index@p<partition>/<offset>` so each lost record
            // can be located in the broker logs and its partition read off.
            let sample_str: Vec<String> = sample
                .iter()
                .map(|key| {
                    let (_, p, o) = s.delivered[key];
                    format!("{}#{}@p{p}/{o}", key.0, key.1)
                })
                .collect();
            reasons.push(format!(
                "data loss: {} acknowledged record(s) never consumed (sample: {sample_str:?})",
                lost.len()
            ));
        }
        let partitions_covered = s.partitions_covered();
        if partitions_covered < min_partitions {
            reasons.push(format!(
                "partition coverage: the least-covered topic had records consumed from only {partitions_covered} \
                 partition(s), expected >= {min_partitions}"
            ));
        }

        // Failed sends: any is a failure (see `ConservationState::failed_sends`).
        // The sample carries the client's error text so the two failure paths
        // (rejected before enqueue vs. timed out after) can be told apart.
        if !s.failed_sends.is_empty() {
            let mut sample: Vec<&(LogicalKey, String)> = s.failed_sends.iter().collect();
            sample.sort_unstable();
            sample.truncate(20);
            let sample_str: Vec<String> = sample.iter().map(|((t, idx), err)| format!("{t}#{idx}: {err}")).collect();
            reasons.push(format!(
                "failed sends: {} record(s) rejected or not acknowledged within the delivery timeout \
                 (sample: {sample_str:?})",
                s.failed_sends.len()
            ));
        }

        // Conservation ratio bound (librdkafka: fail on consumed > 2x
        // delivered). Redeliveries are expected under churn, but a consume
        // *event* count far above the delivered-record count signals runaway
        // duplication. Compare against total delivered (broker-acked): both an
        // old topic generation's records and their consumption sit on the same
        // side of this ratio, so recreate runs are not falsely inflated. Only
        // applied at meaningful volume so a few dups on a tiny run don't trip it.
        let delivered = s.delivered.len() as u64;
        const DUP_RATIO_LIMIT: f64 = 2.0;
        const MIN_VOLUME_FOR_RATIO: u64 = 100;
        if delivered >= MIN_VOLUME_FOR_RATIO && s.consumed_events as f64 > DUP_RATIO_LIMIT * delivered as f64 {
            reasons.push(format!(
                "excessive duplication: {} consume events vs {delivered} delivered record(s) \
                 (ratio {:.2} > {DUP_RATIO_LIMIT})",
                s.consumed_events,
                s.consumed_events as f64 / delivered as f64
            ));
        }

        // In-flight peak: derived from the event stream (`Sent` opens a
        // record's window, `Delivered` / `SendFailed` closes it), not
        // self-reported by the workload. Reported, not scored: the producer
        // sends without awaiting each outcome, so the peak is whatever depth
        // the client's pipeline reached.
        let max_in_flight = s.max_in_flight_by_producer.values().copied().max().unwrap_or(0);

        // Every `Sent` must settle: the producer loop sends without awaiting
        // each outcome, but its `close()` waits for every buffered record's
        // callback before returning, and the drain starts only after every
        // producer has closed. A window still open at verdict therefore means
        // the client never fired a callback for that record (or the run was cut
        // off by the watchdog mid-send).
        let unsettled_sends = s.in_flight.len();
        if unsettled_sends > 0 {
            let mut sample: Vec<&LogicalKey> = s.in_flight.keys().collect();
            sample.sort_unstable();
            sample.truncate(20);
            let sample_str: Vec<String> = sample.iter().map(|(t, idx)| format!("{t}#{idx}")).collect();
            reasons.push(format!(
                "unsettled sends: {unsettled_sends} record(s) sent but never acknowledged or failed \
                 (sample: {sample_str:?})"
            ));
        }

        // Report expected-lost as every delivered record that was legitimately
        // excused, via ANY path (recreate snapshot, blackout skip, or destroyed
        // generation): a delivered record is either observed, scored as loss, or
        // excused, so the excused count is `unobserved delivered − lost`. This
        // includes every excusal mechanism without double-counting and keeps the
        // numbers reconciling: delivered = observed + lost + expected-lost.
        let unobserved_delivered = s.delivered.keys().filter(|key| !s.observed.contains_key(*key)).count();
        let expected_lost = unobserved_delivered.saturating_sub(lost.len());

        // Error summary: every client-reported error, grouped by operation and
        // error text so the report shows what failed and how often, e.g.
        // `7x consumer commit: IllegalStateError: OffsetCommit failed with
        // stale member epoch ...`. Ordered by count, then text.
        let poll_errors = s.consumer_errors.iter().filter(|(_, op, _)| *op == ConsumerOp::Poll).count();
        let commit_errors = s.consumer_errors.iter().filter(|(_, op, _)| *op == ConsumerOp::Commit).count();
        let revoke_commit_errors = s
            .consumer_errors
            .iter()
            .filter(|(_, op, _)| *op == ConsumerOp::RevokeCommit)
            .count();

        // Rebalance listener contract: replayed callback-by-callback in
        // `record_rebalance`; any violation fails the run. The counts are
        // reported so a run with no listener (gRPC consumers) is visibly not
        // exercising the callbacks rather than silently passing them.
        let callbacks = |c: RebalanceCallback| s.rebalance_callbacks.get(&c).copied().unwrap_or(0);
        let revoked_callbacks = callbacks(RebalanceCallback::Revoked);
        let assigned_callbacks = callbacks(RebalanceCallback::Assigned);
        let lost_callbacks = callbacks(RebalanceCallback::Lost);
        let rebalance_violations = s.rebalance_violations.clone();
        if !rebalance_violations.is_empty() {
            let sample: Vec<&String> = rebalance_violations.iter().take(20).collect();
            reasons.push(format!(
                "rebalance listener contract: {} violation(s) (sample: {sample:?})",
                rebalance_violations.len()
            ));
        }

        // Committed offsets vs. the committing consumer's own progress: any
        // mismatch fails the run (see `record_committed`).
        let commit_violations = s.commit_violations.clone();
        if !commit_violations.is_empty() {
            let sample: Vec<&String> = commit_violations.iter().take(20).collect();
            reasons.push(format!(
                "committed offsets: {} violation(s) (sample: {sample:?})",
                commit_violations.len()
            ));
        }

        // Ordering: consumer offset regressions within an assignment and
        // producer send-order vs log-order disagreements (see `record_ordering`).
        let order_violations = s.order_violations.clone();
        if !order_violations.is_empty() {
            let sample: Vec<&String> = order_violations.iter().take(20).collect();
            reasons.push(format!(
                "ordering: {} violation(s) (sample: {sample:?})",
                order_violations.len()
            ));
        }
        let mut grouped: HashMap<String, usize> = HashMap::new();
        for (_, err) in &s.failed_sends {
            *grouped.entry(format!("producer send: {err}")).or_insert(0) += 1;
        }
        for (_, op, err) in &s.consumer_errors {
            *grouped.entry(format!("{op}: {err}")).or_insert(0) += 1;
        }
        let mut error_breakdown: Vec<(String, usize)> = grouped.into_iter().collect();
        error_breakdown.sort_by(|(a_text, a_n), (b_text, b_n)| b_n.cmp(a_n).then_with(|| a_text.cmp(b_text)));

        let topic_count = s.delivered.keys().map(|(t, _)| t).collect::<BTreeSet<_>>().len();
        let recreated: BTreeSet<&String> = s.blackout_floor.keys().collect();
        let known_defect = known_recreate_defect(topic_count, &recreated, &lost_by_partition, &reasons);

        ChaosVerdict {
            known_defect,
            delivered: s.delivered.len(),
            expected_lost,
            failed_sends: s.failed_sends.len(),
            poll_errors,
            commit_errors,
            revoke_commit_errors,
            revoked_callbacks,
            assigned_callbacks,
            lost_callbacks,
            listener_consumers: s.listener_consumers.len(),
            rebalance_violations,
            close_time_unowned_releases: s.close_time_unowned_releases,
            commit_checks: s.commit_checks,
            ambiguous_commit_checks: s.ambiguous_commit_checks,
            commit_violations,
            order_violations,
            unscored_order_anomalies: s.unscored_order_anomalies,
            error_breakdown,
            logical_duplicates,
            physical_duplicates,
            producer_duplicates,
            cross_generation_duplicates,
            partitions_covered,
            max_in_flight,
            unsettled_sends,
            lost,
            lost_by_partition,
            reasons,
        }
    }
}

/// Whether a failed verdict is exactly the known multi-topic recreate defect
/// (see the `--topic-recreate` / `--num-topics` guard in `config.rs`). After a
/// recreate of one of several subscribed topics, the consumer carries its
/// pre-recreate position, committed offset and leader epoch into the new
/// incarnation (Sep 2026 matrix, F2). On an affected partition it either
/// skips the head of the new incarnation (it starts reading above it) or
/// never reads that incarnation at all (stuck behind the stale epoch).
///
/// The signature, all of which must hold:
/// - more than one topic, and every lost partition incarnation is on a topic
///   that was recreated during the run;
/// - the loss on each starts at the head of the incarnation (offset 0);
/// - on each, the consumer read that same incarnation only above the whole
///   lost range (head skipped), or not at all (stuck). "Read" is judged per
///   incarnation: an offset the consumer reached in an earlier incarnation
///   says nothing about this one;
/// - the only failure reasons are that data loss and committed-offset
///   violations (the stale commits under the old topic id time out and knock
///   other commits off). Anything else — duplicates, failed or unsettled sends,
///   ordering, rebalance contract, coverage — is not this defect.
///
/// Returns a one-line description of the match, `None` otherwise. The verdict
/// still FAILS: this only labels the failure, it never excuses it.
fn known_recreate_defect(
    topic_count: usize,
    recreated: &BTreeSet<&String>,
    lost_by_partition: &[LostPartition],
    reasons: &[String],
) -> Option<String> {
    if topic_count < 2 || lost_by_partition.is_empty() {
        return None;
    }
    let skipped = |p: &LostPartition| p.last_consumed_offset.is_some_and(|c| c > p.last_lost_offset);
    let stuck = |p: &LostPartition| p.last_consumed_offset.is_none();
    let signature =
        |p: &LostPartition| recreated.contains(&p.topic) && p.first_lost_offset == 0 && (skipped(p) || stuck(p));
    if !lost_by_partition.iter().all(signature) {
        return None;
    }
    let only_known_reasons = reasons
        .iter()
        .all(|r| r.starts_with("data loss:") || r.starts_with("committed offsets:"));
    if !only_known_reasons || !reasons.iter().any(|r| r.starts_with("data loss:")) {
        return None;
    }
    let lost: usize = lost_by_partition.iter().map(|p| p.lost).sum();
    let topics: BTreeSet<&str> = lost_by_partition.iter().map(|p| p.topic.as_str()).collect();
    let skipped_count = lost_by_partition.iter().filter(|p| skipped(p)).count();
    let stuck_count = lost_by_partition.len() - skipped_count;
    Some(format!(
        "multi-topic recreate stale state — {lost} record(s) lost at the head of {} partition incarnation(s) of \
         recreated topic(s) {}: {skipped_count} head skipped (the consumer started reading above it), \
         {stuck_count} stuck (the consumer never read that incarnation)",
        lost_by_partition.len(),
        topics.into_iter().collect::<Vec<_>>().join(", ")
    ))
}

/// `topic-partition`, Kafka's `TopicPartition` rendering.
fn tp_label((topic, partition): &(String, i32)) -> String {
    format!("{topic}-{partition}")
}

/// Where a run's loss sits on one incarnation of a partition; see
/// [`ChaosVerdict::lost_by_partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostPartition {
    pub topic: String,
    pub partition: i32,
    /// The incarnation (topic id at acknowledgement; zero if unresolved).
    pub topic_id: Uuid,
    /// Lost records on this incarnation of the partition.
    pub lost: usize,
    /// Lowest and highest offset among the lost records.
    pub first_lost_offset: i64,
    pub last_lost_offset: i64,
    /// Offset of the most recent acknowledgement on this incarnation.
    pub last_delivered_offset: i64,
    /// Highest offset the consumer observed on this incarnation; `None` if it
    /// never observed one.
    pub last_consumed_offset: Option<i64>,
}

/// Pass/fail verdict of a chaos run.
#[derive(Debug)]
pub struct ChaosVerdict {
    /// Set when the failure matches the known multi-topic recreate defect's
    /// exact signature (see [`known_recreate_defect`]). The verdict still
    /// fails; the matrix runner uses the label to retry the run.
    pub known_defect: Option<String>,
    pub delivered: usize,
    pub expected_lost: usize,
    /// Producer sends that were rejected or not acknowledged within the
    /// delivery timeout. Non-zero fails the run.
    pub failed_sends: usize,
    /// Consumer `poll` calls that returned an error. Reported, not scored.
    pub poll_errors: usize,
    /// Consumer commit calls that returned an error. Reported, not scored.
    pub commit_errors: usize,
    /// Commits issued inside `on_partitions_revoked` that returned an error.
    /// Reported, not scored (like the other consumer errors).
    pub revoke_commit_errors: usize,
    /// How often each `ConsumerRebalanceListener` callback fired, across all
    /// consumers. Reported: zero everywhere means no consumer registered a
    /// listener (gRPC-backed consumers), so the contract was not exercised.
    pub revoked_callbacks: usize,
    pub assigned_callbacks: usize,
    pub lost_callbacks: usize,
    /// Consumers that fired at least one rebalance callback.
    pub listener_consumers: usize,
    /// Rebalance listener contract violations (double assign, release of an
    /// unowned partition, clean revocation after the partition had moved,
    /// close while still owning partitions). Non-empty fails the run.
    pub rebalance_violations: Vec<String>,
    /// Release callbacks a closing consumer fired for partitions it was never
    /// told it owned (Java-faithful; see `ConservationState::closing`).
    /// Reported, not scored.
    pub close_time_unowned_releases: usize,
    /// Committed offsets read back from the broker and compared with the
    /// committing consumer's own consumption (only partitions it had consumed
    /// from since assignment count). Zero means the check never ran.
    pub commit_checks: usize,
    /// Committed offsets not compared because two consumers owned the same
    /// partition name on a recreated topic (see
    /// `ConservationState::ambiguous_commit_checks`). Reported, not scored.
    pub ambiguous_commit_checks: usize,
    /// Committed offsets that were ahead of or behind the consumer's own
    /// consumption. Non-empty fails the run.
    pub commit_violations: Vec<String>,
    /// Ordering violations on never-recreated topics: a consumer read an older
    /// offset after a newer one within one assignment, or a producer's
    /// acknowledged offsets disagree with its send order. Non-empty fails the
    /// run.
    pub order_violations: Vec<String>,
    /// The same anomalies on recreated topics, where an offset reset can look
    /// like a regression. Reported, not scored.
    pub unscored_order_anomalies: usize,
    /// Every client-reported error (failed sends, poll and commit errors)
    /// grouped by operation and error text, with its count, most frequent
    /// first. Rendered under the verdict as `<n>x <operation>: <error>`.
    pub error_breakdown: Vec<(String, usize)>,
    /// Same logical `index` observed more than once (redelivery).
    pub logical_duplicates: u64,
    /// Same physical `(topic_id, partition, offset)` seen more than once.
    pub physical_duplicates: u64,
    /// Logical records the producer committed at two or more offsets within one
    /// topic generation (double writes; idempotence violated). Non-empty fails
    /// the run. Consumer re-reads are not included.
    pub producer_duplicates: Vec<LogicalKey>,
    /// Logical records observed in more than one topic generation (a retry
    /// committed again after a topic recreate destroyed the broker's producer
    /// state). Excused; reported for context.
    pub cross_generation_duplicates: usize,
    pub partitions_covered: usize,
    /// The maximum number of records any single producer had in flight at one
    /// time. Reported, not scored. 0 when the event stream carried no `Sent`
    /// events.
    pub max_in_flight: u32,
    /// Records `Sent` but neither `Delivered` nor `SendFailed` by verdict time.
    /// Non-zero fails the run.
    pub unsettled_sends: usize,
    /// Delivered `(topic, index)` records the consumer never observed (data
    /// loss).
    pub lost: Vec<LogicalKey>,
    /// `lost` grouped by (topic, partition), ordered by topic then partition,
    /// with the last acknowledged and last consumed offset on each. Empty when
    /// nothing was lost.
    pub lost_by_partition: Vec<LostPartition>,
    /// Failure reasons; empty ⇒ pass.
    pub reasons: Vec<String>,
}

impl ChaosVerdict {
    pub fn is_pass(&self) -> bool {
        self.reasons.is_empty()
    }
}

impl std::fmt::Display for ChaosVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Chaos verdict: {} ===", if self.is_pass() { "PASS" } else { "FAIL" })?;
        writeln!(f, "  delivered (acked) records : {}", self.delivered)?;
        writeln!(f, "  failed sends              : {}", self.failed_sends)?;
        writeln!(f, "  duplicates (by index)     : {}", self.logical_duplicates)?;
        writeln!(f, "  duplicates (by offset)    : {}", self.physical_duplicates)?;
        writeln!(f, "  duplicates (double write) : {}", self.producer_duplicates.len())?;
        if self.cross_generation_duplicates > 0 {
            writeln!(f, "  cross-generation repeats  : {}", self.cross_generation_duplicates)?;
        }
        writeln!(f, "  partitions covered        : {}", self.partitions_covered)?;
        writeln!(f, "  in-flight peak (producer) : {}", self.max_in_flight)?;
        if self.unsettled_sends > 0 {
            writeln!(f, "  unsettled sends           : {}", self.unsettled_sends)?;
        }
        if self.expected_lost > 0 {
            writeln!(f, "  expected-lost (recreate)  : {}", self.expected_lost)?;
        }
        writeln!(f, "  lost (delivered, unseen)  : {}", self.lost.len())?;
        if !self.lost_by_partition.is_empty() {
            writeln!(f, "  lost by partition:")?;
            for p in &self.lost_by_partition {
                let consumed = match p.last_consumed_offset {
                    Some(o) => o.to_string(),
                    None => "none".to_string(),
                };
                let incarnation = if p.topic_id == Uuid::zero() {
                    String::new()
                } else {
                    format!(" [{}]", p.topic_id)
                };
                writeln!(
                    f,
                    "    {} p{}{incarnation}: {} lost at offsets {}..={}; last acked offset {}, highest consumed \
                     offset {consumed}",
                    p.topic, p.partition, p.lost, p.first_lost_offset, p.last_lost_offset, p.last_delivered_offset
                )?;
            }
        }
        writeln!(
            f,
            "  rebalance callbacks       : revoked={} assigned={} lost={} ({} consumer(s) with listener)",
            self.revoked_callbacks, self.assigned_callbacks, self.lost_callbacks, self.listener_consumers
        )?;
        if self.close_time_unowned_releases > 0 {
            writeln!(
                f,
                "  close-time releases of never-assigned partitions (Java-faithful, not scored): {}",
                self.close_time_unowned_releases
            )?;
        }
        writeln!(
            f,
            "  committed offsets checked : {} ({} violation(s))",
            self.commit_checks,
            self.commit_violations.len()
        )?;
        if self.ambiguous_commit_checks > 0 {
            writeln!(
                f,
                "  committed offsets skipped : {} (partition shared by two owners of different incarnations)",
                self.ambiguous_commit_checks
            )?;
        }
        writeln!(
            f,
            "  ordering violations       : {} ({} unscored on recreated topics)",
            self.order_violations.len(),
            self.unscored_order_anomalies
        )?;
        writeln!(f, "  consumer poll errors      : {}", self.poll_errors)?;
        writeln!(f, "  consumer commit errors    : {}", self.commit_errors)?;
        if self.revoke_commit_errors > 0 {
            writeln!(f, "  commit errors in revoked  : {}", self.revoke_commit_errors)?;
        }
        if !self.error_breakdown.is_empty() {
            writeln!(f, "  errors by kind:")?;
            for (text, count) in &self.error_breakdown {
                // One line per distinct error; long messages are cut so the
                // block stays readable, the full text is in the run log.
                const MAX: usize = 160;
                let shown: String = if text.chars().count() > MAX {
                    let mut t: String = text.chars().take(MAX).collect();
                    t.push_str("...");
                    t
                } else {
                    text.clone()
                };
                writeln!(f, "    {count}x {shown}")?;
            }
        }
        for reason in &self.reasons {
            writeln!(f, "  FAIL: {reason}")?;
        }
        if let Some(defect) = &self.known_defect {
            writeln!(f, "  KNOWN DEFECT: {defect}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero() -> Uuid {
        Uuid::zero()
    }

    fn lost_at(topic: &str, partition: i32, lost: (i64, i64), consumed: Option<i64>) -> LostPartition {
        LostPartition {
            topic: topic.to_string(),
            partition,
            topic_id: zero(),
            lost: (lost.1 - lost.0 + 1) as usize,
            first_lost_offset: lost.0,
            last_lost_offset: lost.1,
            last_delivered_offset: lost.1,
            last_consumed_offset: consumed,
        }
    }

    /// The 2026-09-25 reproduction (`--num-topics 2 --topic-recreate --seed 5`):
    /// the new generation's head lost on three partitions of the recreated
    /// topic, the consumer parked at its old-generation offsets, plus a
    /// committed-offset knock-on on the other topic.
    #[test]
    fn the_known_recreate_defect_is_labelled_only_on_its_exact_signature() {
        let t0 = "chaos-run_0".to_string();
        let recreated: BTreeSet<&String> = [&t0].into_iter().collect();
        let repro = vec![
            lost_at("chaos-run_0", 1, (0, 154), Some(1737)),
            lost_at("chaos-run_0", 2, (0, 147), Some(1749)),
            lost_at("chaos-run_0", 3, (0, 157), Some(1803)),
        ];
        let reasons = vec![
            "data loss: 461 acknowledged record(s) never consumed".to_string(),
            "committed offsets: 1 violation(s)".to_string(),
        ];
        assert_eq!(
            known_recreate_defect(2, &recreated, &repro, &reasons).as_deref(),
            Some(
                "multi-topic recreate stale state — 461 record(s) lost at the head of 3 partition incarnation(s) of \
                 recreated topic(s) chaos-run_0: 3 head skipped (the consumer started reading above it), 0 stuck \
                 (the consumer never read that incarnation)"
            )
        );

        // A stuck incarnation (never read at all, loss from its head) is the
        // same defect, alone or mixed with skipped heads.
        let stuck = vec![
            lost_at("chaos-run_0", 1, (0, 154), Some(1737)),
            lost_at("chaos-run_0", 4, (0, 60), None),
        ];
        assert_eq!(
            known_recreate_defect(2, &recreated, &stuck, &reasons).as_deref(),
            Some(
                "multi-topic recreate stale state — 216 record(s) lost at the head of 2 partition incarnation(s) of \
                 recreated topic(s) chaos-run_0: 1 head skipped (the consumer started reading above it), 1 stuck \
                 (the consumer never read that incarnation)"
            )
        );

        // Not the defect: a single topic, loss on a topic never recreated, a
        // consumer that simply stopped (read below the loss), loss that does
        // not start at the head of the incarnation (a gap), or any additional
        // kind of failure.
        assert_eq!(known_recreate_defect(1, &recreated, &repro, &reasons), None);
        let other_topic = vec![lost_at("chaos-run_1", 0, (0, 9), Some(100))];
        assert_eq!(known_recreate_defect(2, &recreated, &other_topic, &reasons), None);
        let stopped = vec![lost_at("chaos-run_0", 1, (0, 900), Some(499))];
        assert_eq!(known_recreate_defect(2, &recreated, &stopped, &reasons), None);
        let gap = vec![lost_at("chaos-run_0", 1, (5, 9), Some(100))];
        assert_eq!(known_recreate_defect(2, &recreated, &gap, &reasons), None);
        let gap_never_read = vec![lost_at("chaos-run_0", 1, (5, 9), None)];
        assert_eq!(known_recreate_defect(2, &recreated, &gap_never_read, &reasons), None);
        let mut extra = reasons.clone();
        extra.push("ordering: 1 violation(s)".to_string());
        assert_eq!(known_recreate_defect(2, &recreated, &repro, &extra), None);
        let no_loss_reason = vec!["committed offsets: 1 violation(s)".to_string()];
        assert_eq!(known_recreate_defect(2, &recreated, &repro, &no_loss_reason), None);
        assert_eq!(known_recreate_defect(2, &recreated, &[], &reasons), None);
    }

    /// A healthy run (each delivered record consumed once) passes and does not
    /// trip the duplication bound.
    #[test]
    fn conservation_passes_when_each_delivered_consumed_once() {
        let v = ConservationVerifier::new();
        for i in 0..200u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
        }
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "healthy run should pass: {verdict}");
        assert_eq!(verdict.lost.len(), 0);
    }

    /// Redelivery below 2x delivered is reported but does NOT fail.
    #[test]
    fn moderate_duplication_passes() {
        let v = ConservationVerifier::new();
        for i in 0..200u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            // consume each once, plus a redelivery for half of them (ratio 1.5x)
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            if i % 2 == 0 {
                v.record(WorkloadEvent::Consumed {
                    consumer: "c".into(),
                    index: i,
                    topic: "t".into(),
                    topic_id: zero(),
                    partition: 0,
                    offset: i as i64,
                });
            }
        }
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "1.5x duplication should still pass: {verdict}");
        assert!(verdict.logical_duplicates > 0);
    }

    /// Consuming far more than 2x delivered trips the conservation ratio bound.
    #[test]
    fn excessive_duplication_fails() {
        let v = ConservationVerifier::new();
        for i in 0..200u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            // consume each THREE times -> 3x delivered, over the 2x bound
            for _ in 0..3 {
                v.record(WorkloadEvent::Consumed {
                    consumer: "c".into(),
                    index: i,
                    topic: "t".into(),
                    topic_id: zero(),
                    partition: 0,
                    offset: i as i64,
                });
            }
        }
        let verdict = v.verdict(1);
        assert!(!verdict.is_pass(), "3x duplication must fail the ratio bound");
        assert!(
            verdict.reasons.iter().any(|r| r.contains("excessive duplication")),
            "expected an excessive-duplication reason, got: {:?}",
            verdict.reasons
        );
    }

    /// A delivered record the consumer never saw (and not expected-lost) is loss.
    #[test]
    fn unconsumed_delivered_is_loss() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Delivered { index: 1, topic: "t".into(), topic_id: zero(), partition: 0, offset: 0 });
        // never consumed
        let verdict = v.verdict(1);
        assert!(!verdict.is_pass());
        assert_eq!(verdict.lost, vec![("t".to_string(), 1)]);
    }

    /// Expected-lost (topic recreate) records are excluded from the loss check.
    #[test]
    fn expected_lost_is_not_counted_as_loss() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Delivered { index: 1, topic: "t".into(), topic_id: zero(), partition: 0, offset: 0 });
        v.note_expected_loss(ExpectedLossHint::AllDeliveredSoFar);
        // index 1 delivered but destroyed by recreate; never consumed, but not loss.
        let verdict = v.verdict(1);
        assert_eq!(verdict.lost.len(), 0, "recreate-destroyed record must not be loss: {verdict}");
        assert_eq!(verdict.expected_lost, 1);
    }

    /// Issue 1 regression: on a recreate-blackout topic, records the producer
    /// keeps delivering into the delete/recreate window — its index does NOT
    /// reset — that the consumer skips (they sit below where the consumer later
    /// resumed) are excused, not scored as loss.
    #[test]
    fn recreate_blackout_skipped_records_are_not_loss() {
        let v = ConservationVerifier::new();
        // Pre-delete: 0..100 delivered and consumed.
        for i in 0..100u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
        }
        // Recreate: harness marks the pre-delete snapshot AND the blackout.
        v.note_expected_loss(ExpectedLossHint::AllDeliveredForTopic("t".to_string()));
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        // Blackout window: producer keeps its climbing index, delivers 100..130
        // into the recreated topic; the consumer skips these (stale offset).
        for i in 100..130u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: (i - 100) as i64,
            });
        }
        // Consumer resumes: 130..160 delivered AND consumed. The max observed
        // index (159) is the resume point; 100..130 sit below it -> excused.
        for i in 130..160u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: (i - 100) as i64,
            });
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: (i - 100) as i64,
            });
        }
        let verdict = v.verdict(1);
        assert_eq!(
            verdict.lost.len(),
            0,
            "blackout-skipped boundary records (below the resume point) must not be loss: {verdict}"
        );
    }

    /// A recreate-blackout topic still fails on records ABOVE the resume point
    /// that the consumer never saw — the excusal is bounded to the skipped
    /// window, it does not blanket-excuse genuine post-resume loss.
    #[test]
    fn recreate_blackout_does_not_excuse_loss_above_resume_point() {
        let v = ConservationVerifier::new();
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        // Skipped in blackout (0..5), never observed -> excused.
        for i in 0..5u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
        }
        // Consumer resumed and observed index 10 (the resume high-water mark).
        v.record(WorkloadEvent::Delivered { index: 10, topic: "t".into(), topic_id: zero(), partition: 0, offset: 10 });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 10,
            topic: "t".into(),
            topic_id: zero(),
            partition: 0,
            offset: 10,
        });
        // Delivered index 11 (above resume) never observed -> REAL loss.
        v.record(WorkloadEvent::Delivered { index: 11, topic: "t".into(), topic_id: zero(), partition: 0, offset: 11 });
        let verdict = v.verdict(1);
        assert_eq!(
            verdict.lost,
            vec![("t".to_string(), 11)],
            "loss above the resume point is real: {verdict}"
        );
        assert!(!verdict.is_pass());
    }

    /// The resume point is where the consumer FIRST resumed, not the highest
    /// index it ever saw: a gap opened later on the recreated topic (a broker
    /// roll two cycles after the recreate) is real loss even though the
    /// consumer went on to read past it.
    #[test]
    fn recreate_blackout_gap_after_resume_is_loss() {
        let v = ConservationVerifier::new();
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        // Skipped in the blackout: offsets 0..5, never observed.
        for i in 0..5u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
        }
        // Consumer resumes at offset 10.
        v.record(WorkloadEvent::Delivered { index: 10, topic: "t".into(), topic_id: zero(), partition: 0, offset: 10 });
        v.record(consumed(10));
        // 11..=20 delivered after the resume and never observed; the consumer
        // then reads 30. Under a "max observed" rule these would be excused.
        for i in 11..=20u64 {
            v.record(WorkloadEvent::Delivered {
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
        }
        v.record(WorkloadEvent::Delivered { index: 30, topic: "t".into(), topic_id: zero(), partition: 0, offset: 30 });
        v.record(consumed(30));
        let verdict = v.verdict(1);
        assert_eq!(verdict.lost.len(), 10, "gap after the resume point is real loss: {verdict}");
        assert_eq!(
            verdict.expected_lost, 5,
            "only the skipped blackout range is excused: {verdict}"
        );
        assert!(!verdict.is_pass());
    }

    /// The resume point is per partition: resuming on p0 says nothing about p1,
    /// where the consumer never resumed, so p1's post-recreate records are loss.
    #[test]
    fn recreate_blackout_resume_is_per_partition() {
        let v = ConservationVerifier::new();
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        v.record(WorkloadEvent::Delivered { index: 0, topic: "t".into(), topic_id: zero(), partition: 0, offset: 0 });
        v.record(WorkloadEvent::Delivered { index: 1, topic: "t".into(), topic_id: zero(), partition: 1, offset: 0 });
        v.record(WorkloadEvent::Delivered { index: 2, topic: "t".into(), topic_id: zero(), partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 2,
            topic: "t".into(),
            topic_id: zero(),
            partition: 0,
            offset: 5,
        });
        let verdict = v.verdict(1);
        assert_eq!(verdict.lost, vec![("t".to_string(), 1)], "{verdict}");
    }

    /// A recreate resets the partitions' offsets, so the committing consumer's
    /// progress on the topic starts over: a commit read back before it consumed
    /// anything from the new generation is not compared, and an old-generation
    /// record surfacing from its fetch buffer after the recreate does not count
    /// as new-generation progress either.
    #[test]
    fn recreate_resets_committed_offset_progress_for_the_topic() {
        let v = ConservationVerifier::new();
        v.record(delivered(5000));
        v.record(consumed(5000));
        v.note_expected_loss(ExpectedLossHint::AllDeliveredSoFar);
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        // Stale buffered record from the old generation (index below the floor).
        v.record(consumed(4999));
        // auto.offset.reset put the position at 0 and the loop committed it.
        v.record(committed("c", 0, 0));
        let verdict = v.verdict(0);
        assert!(verdict.commit_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.commit_checks, 0);
        // New-generation progress is compared again.
        v.record(WorkloadEvent::Delivered {
            index: 5001,
            topic: "t".into(),
            topic_id: zero(),
            partition: 0,
            offset: 0,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5001,
            topic: "t".into(),
            topic_id: zero(),
            partition: 0,
            offset: 0,
        });
        v.record(committed("c", 0, 1));
        v.record(committed("c", 0, 7));
        let verdict = v.verdict(0);
        assert_eq!(verdict.commit_checks, 2);
        assert_eq!(verdict.commit_violations.len(), 1, "{verdict}");
    }

    /// On a recreated topic the coordinator hands the new generation's
    /// partitions out before the old owner's clean revoke of the same-named
    /// old-generation partitions lands. Not a fence: no violation.
    #[test]
    fn recreated_topic_suspends_the_overlap_check() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0, 1, 2]));
        v.record(rebalance("c2", RebalanceCallback::Assigned, &[3, 4, 5]));
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        v.record(rebalance("c2", RebalanceCallback::Assigned, &[1]));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0, 1, 2]));
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0, 2]));
        let verdict = v.verdict(0);
        assert!(verdict.rebalance_violations.is_empty(), "{verdict}");
    }

    /// The consumer stamps a record's topic id from the live map, so an
    /// old-generation record polled after the recreate carries the new id. When
    /// the consumer saw the record at exactly the address the producer's ack
    /// named, the ack's generation wins: the legitimate retry into the new
    /// generation is then a cross-generation repeat, not a double write.
    #[test]
    fn consumed_topic_id_is_corrected_from_the_ack_at_the_same_address() {
        let old = Uuid::with_bytes([1u8; 16]);
        let new = Uuid::with_bytes([2u8; 16]);
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Delivered { index: 7, topic: "t".into(), topic_id: old, partition: 0, offset: 100 });
        // Old-generation copy, polled after the id map switched to `new`.
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 7,
            topic: "t".into(),
            topic_id: new,
            partition: 0,
            offset: 100,
        });
        // The producer's retry into the new generation.
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 7,
            topic: "t".into(),
            topic_id: new,
            partition: 0,
            offset: 3,
        });
        let verdict = v.verdict(0);
        assert!(verdict.producer_duplicates.is_empty(), "not a double write: {verdict}");
        assert_eq!(verdict.cross_generation_duplicates, 1);
        assert!(verdict.is_pass(), "{verdict}");
    }

    /// The per-topic consume-progress signal advances only for the named topic,
    /// so the drain can detect a resumed consumer on a just-recreated topic even
    /// while the other topics' consumption keeps the GLOBAL counter climbing.
    #[test]
    fn per_topic_consume_progress_is_scoped() {
        let v = ConservationVerifier::new();
        assert_eq!(v.consumed_progress_for_topic("t0"), 0);
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 0,
            topic: "t0".into(),
            topic_id: zero(),
            partition: 0,
            offset: 0,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 0,
            topic: "t1".into(),
            topic_id: zero(),
            partition: 0,
            offset: 0,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 1,
            topic: "t0".into(),
            topic_id: zero(),
            partition: 0,
            offset: 1,
        });
        assert_eq!(v.consumed_progress_for_topic("t0"), 2);
        assert_eq!(v.consumed_progress_for_topic("t1"), 1);
        assert_eq!(v.consumed_progress_for_topic("absent"), 0);
        // Global counter is the sum across topics.
        assert_eq!(v.consumed_progress(), 3);
    }

    /// The SAME logical index produced on two different topics is two distinct
    /// records: consuming both must not be scored as a duplicate, and losing one
    /// (but not the other) must be scored as exactly one loss. This is the
    /// multi-topic key-collision guard — with an `index`-only key, topic `t0`
    /// index 5 and topic `t1` index 5 would collapse into one record and corrupt
    /// the accounting.
    #[test]
    fn same_index_on_two_topics_are_distinct_records() {
        let v = ConservationVerifier::new();
        // Both topics deliver index 5 at the same physical partition/offset but
        // under distinct topic ids (post-create ids differ).
        let id0 = Uuid::with_bytes([1u8; 16]);
        let id1 = Uuid::with_bytes([2u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t0".into(), topic_id: id0, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t1".into(), topic_id: id1, partition: 0, offset: 5 });
        // Consume each once.
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t0".into(),
            topic_id: id0,
            partition: 0,
            offset: 5,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t1".into(),
            topic_id: id1,
            partition: 0,
            offset: 5,
        });

        let verdict = v.verdict(1);
        assert!(
            verdict.is_pass(),
            "two distinct records consumed once each should pass: {verdict}"
        );
        assert_eq!(verdict.delivered, 2, "both (topic, index=5) records must be counted");
        assert_eq!(verdict.lost.len(), 0);
        assert_eq!(
            verdict.logical_duplicates, 0,
            "same index on two topics is not a logical duplicate"
        );
    }

    /// Expected-loss scoped to ONE topic must not excuse loss on another topic.
    /// A recreate of `t0` destroys `t0`'s records (not loss), but `t1`'s
    /// unconsumed delivered record is still real loss.
    #[test]
    fn expected_loss_scoped_to_one_topic_does_not_excuse_another() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Delivered { index: 1, topic: "t0".into(), topic_id: zero(), partition: 0, offset: 0 });
        v.record(WorkloadEvent::Delivered { index: 1, topic: "t1".into(), topic_id: zero(), partition: 0, offset: 0 });
        // Recreate hits t0 only: its delivered records are expected-lost.
        v.note_expected_loss(ExpectedLossHint::AllDeliveredForTopic("t0".to_string()));
        // Neither is consumed. t0's loss is excused; t1's is not.
        let verdict = v.verdict(1);
        assert_eq!(verdict.expected_lost, 1, "only t0's record is expected-lost");
        assert_eq!(
            verdict.lost,
            vec![("t1".to_string(), 1)],
            "t1's unconsumed record is still real loss"
        );
        assert!(!verdict.is_pass(), "loss on t1 must fail the run");
    }

    /// The loss sample and the per-partition report name where each lost record
    /// sits. Partition 1 here is a stuck consumer: it acked offsets 0..=3 and
    /// consumed only offset 0, so the report shows the gap between the last
    /// acked and last consumed offsets. Partition 0 is healthy and absent from
    /// the report.
    #[test]
    fn loss_is_reported_per_partition_with_offsets() {
        let v = ConservationVerifier::new();
        let deliver = |index: u64, partition: i32, offset: i64| {
            v.record(WorkloadEvent::Delivered { index, topic: "t".into(), topic_id: zero(), partition, offset });
        };
        let consume = |index: u64, partition: i32, offset: i64| {
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index,
                topic: "t".into(),
                topic_id: zero(),
                partition,
                offset,
            });
        };
        // Partition 0: two records, both consumed.
        deliver(0, 0, 0);
        deliver(2, 0, 1);
        consume(0, 0, 0);
        consume(2, 0, 1);
        // Partition 1: four records, only the first consumed.
        deliver(1, 1, 0);
        deliver(3, 1, 1);
        deliver(5, 1, 2);
        deliver(7, 1, 3);
        consume(1, 1, 0);

        let verdict = v.verdict(1);
        assert_eq!(verdict.lost.len(), 3, "{verdict}");
        assert_eq!(
            verdict.lost_by_partition,
            vec![LostPartition {
                topic: "t".into(),
                partition: 1,
                topic_id: zero(),
                lost: 3,
                first_lost_offset: 1,
                last_lost_offset: 3,
                last_delivered_offset: 3,
                last_consumed_offset: Some(0),
            }],
            "{verdict}"
        );
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("data loss"))
            .unwrap_or_else(|| panic!("expected a data-loss reason: {verdict}"));
        assert!(
            reason.contains("\"t#3@p1/1\"") && reason.contains("\"t#5@p1/2\"") && reason.contains("\"t#7@p1/3\""),
            "the sample must carry partition and offset: {reason}"
        );
        let rendered = verdict.to_string();
        assert!(
            rendered.contains("t p1: 3 lost at offsets 1..=3; last acked offset 3, highest consumed offset 0"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("t p0:"),
            "a partition without loss is not listed: {rendered}"
        );
    }

    /// A partition the consumer never read at all reports `none` for its last
    /// consumed offset, and a passing run renders no per-partition block.
    #[test]
    fn partition_never_consumed_reports_none_and_passing_run_has_no_block() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Delivered { index: 0, topic: "t".into(), topic_id: zero(), partition: 2, offset: 9 });
        let verdict = v.verdict(0);
        assert_eq!(verdict.lost_by_partition.len(), 1, "{verdict}");
        assert_eq!(verdict.lost_by_partition[0].last_consumed_offset, None);
        assert!(
            verdict
                .to_string()
                .contains("t p2: 1 lost at offsets 9..=9; last acked offset 9, highest consumed offset none")
        );

        let ok = ConservationVerifier::new();
        ok.record(delivered(0));
        ok.record(consumed(0));
        let verdict = ok.verdict(1);
        assert!(verdict.is_pass(), "{verdict}");
        assert!(verdict.lost_by_partition.is_empty());
        assert!(!verdict.to_string().contains("lost by partition"), "{verdict}");
    }

    fn sent(index: u64, producer: &str) -> WorkloadEvent {
        WorkloadEvent::Sent { index, topic: "t".into(), producer: producer.into() }
    }

    fn rebalance(consumer: &str, callback: RebalanceCallback, partitions: &[i32]) -> WorkloadEvent {
        WorkloadEvent::Rebalance {
            consumer: consumer.into(),
            callback,
            partitions: partitions.iter().map(|p| ("t".to_string(), *p)).collect(),
        }
    }

    fn closed(consumer: &str) -> WorkloadEvent {
        WorkloadEvent::ConsumerClosed { consumer: consumer.into() }
    }

    fn consumed_by(consumer: &str, partition: i32, offset: i64) -> WorkloadEvent {
        WorkloadEvent::Consumed {
            consumer: consumer.into(),
            index: offset as u64,
            topic: "t".into(),
            topic_id: zero(),
            partition,
            offset,
        }
    }

    fn committed(consumer: &str, partition: i32, offset: i64) -> WorkloadEvent {
        WorkloadEvent::Committed { consumer: consumer.into(), topic: "t".into(), partition, offset }
    }

    fn delivered_at(index: u64, partition: i32, offset: i64) -> WorkloadEvent {
        WorkloadEvent::Delivered { index, topic: "t".into(), topic_id: zero(), partition, offset }
    }

    /// A listener consumer that reads offset 5 after offset 9 on the same
    /// partition without a new assignment in between returned older data.
    #[test]
    fn consumer_offset_regression_within_an_assignment_fails() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(consumed_by("c1", 0, 9));
        v.record(consumed_by("c1", 0, 5));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.order_violations,
            vec!["c1: read t-0 offset 5 after offset 9 within one assignment (fetch returned older data)".to_string()]
        );
        assert!(
            verdict.reasons.iter().any(|r| r.starts_with("ordering: 1 violation(s)")),
            "{verdict}"
        );
    }

    /// A re-read after the partition was assigned again is a legitimate
    /// resume from the committed offset, and a consumer without a listener is
    /// never checked (its reassignments are invisible).
    #[test]
    fn consumer_offset_regression_after_reassignment_or_without_listener_is_fine() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(consumed_by("c1", 0, 9));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0]));
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(consumed_by("c1", 0, 5));
        // gRPC-style consumer: no callbacks at all.
        v.record(consumed_by("grpc", 1, 9));
        v.record(consumed_by("grpc", 1, 5));
        let verdict = v.verdict(0);
        assert!(verdict.order_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.unscored_order_anomalies, 0);
    }

    /// One producer's acks on one partition must keep send order and log order
    /// in step: a later index at a lower offset (or an earlier index at a
    /// higher one) means records were appended out of send order.
    #[test]
    fn producer_send_order_disagreeing_with_log_order_fails() {
        let v = ConservationVerifier::new();
        v.record(sent(1, "p1"));
        v.record(sent(2, "p1"));
        v.record(sent(3, "p1"));
        v.record(delivered_at(1, 0, 10));
        v.record(delivered_at(2, 0, 11));
        v.record(delivered_at(3, 0, 8));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.order_violations,
            vec![
                "p1: ack for index 3 on t-0 at offset 8, but index 2 was acknowledged at offset 11 (send order and \
                 log order disagree)"
                    .to_string()
            ]
        );
    }

    /// Acks arriving out of callback order but consistent with the log (an
    /// earlier index at a lower offset) are not a violation; two producers
    /// interleaving on one partition are tracked separately.
    #[test]
    fn producer_order_check_tolerates_late_acks_and_separates_producers() {
        let v = ConservationVerifier::new();
        for (i, p) in [(1, "p1"), (2, "p1"), (3, "p2"), (4, "p2")] {
            v.record(sent(i, p));
        }
        v.record(delivered_at(2, 0, 11));
        v.record(delivered_at(1, 0, 10)); // late ack, consistent
        v.record(delivered_at(4, 0, 12)); // p2 interleaves at a lower offset than p1's... fine, other producer
        v.record(delivered_at(3, 0, 9));
        let verdict = v.verdict(0);
        assert!(verdict.order_violations.is_empty(), "{verdict}");
    }

    /// On a recreated topic an offset reset can look like a regression, so the
    /// anomaly is reported but does not fail the run.
    #[test]
    fn ordering_anomalies_on_a_recreated_topic_are_reported_not_scored() {
        let v = ConservationVerifier::new();
        v.record(sent(1, "p1"));
        v.record(sent(2, "p1"));
        v.record(delivered_at(1, 0, 10));
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        v.record(delivered_at(2, 0, 0));
        let verdict = v.verdict(0);
        assert!(verdict.order_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.unscored_order_anomalies, 1);
        assert!(
            verdict
                .to_string()
                .contains("ordering violations       : 0 (1 unscored on recreated topics)")
        );
    }

    /// After a sync commit the committed offset is last consumed + 1: counted
    /// as a check, no violation, reported in the verdict.
    #[test]
    fn committed_offset_equal_to_progress_plus_one_passes_and_is_counted() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0, 1]));
        v.record(consumed_by("c1", 0, 41));
        v.record(consumed_by("c1", 1, 7));
        v.record(committed("c1", 0, 42));
        v.record(committed("c1", 1, 8));
        let verdict = v.verdict(0);
        assert!(verdict.commit_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.commit_checks, 2);
        assert!(verdict.to_string().contains("committed offsets checked : 2 (0 violation(s))"));
    }

    /// Committed past what was consumed: the next owner would skip records.
    #[test]
    fn committed_offset_ahead_of_consumption_fails() {
        let v = ConservationVerifier::new();
        v.record(consumed_by("c1", 0, 41));
        v.record(committed("c1", 0, 50));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.commit_violations,
            vec![
                "c1: committed offset 50 on t-0 is ahead of its own consumption (last consumed 41, expected 42)"
                    .to_string()
            ]
        );
        assert!(
            verdict
                .reasons
                .iter()
                .any(|r| r.starts_with("committed offsets: 1 violation(s)")),
            "{verdict}"
        );
    }

    /// Committed short of what was consumed: the next owner would re-read
    /// records this consumer already processed.
    #[test]
    fn committed_offset_behind_consumption_fails() {
        let v = ConservationVerifier::new();
        v.record(consumed_by("c1", 0, 41));
        v.record(committed("c1", 0, 30));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.commit_violations,
            vec![
                "c1: committed offset 30 on t-0 is behind its own consumption (last consumed 41, expected 42)"
                    .to_string()
            ]
        );
    }

    /// A committed offset on a partition the consumer has not consumed from
    /// since it was (re)assigned is the previous owner's: not compared. Progress
    /// is per consumer, so another consumer's reads do not count either.
    #[test]
    fn committed_offset_without_own_consumption_since_assignment_is_not_compared() {
        let v = ConservationVerifier::new();
        // c1 consumed to 10, released the partition, and got it back: the
        // read-back right after reassignment is whatever was committed before.
        v.record(consumed_by("c1", 0, 10));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0]));
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(committed("c1", 0, 3));
        // c2 never consumed p1; c1 did.
        v.record(consumed_by("c1", 1, 99));
        v.record(committed("c2", 1, 5));
        let verdict = v.verdict(0);
        assert!(verdict.commit_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.commit_checks, 0);
    }

    /// A healthy sequence: assigned, a clean revoke handing p1 to a second
    /// consumer, a fence reported as lost, and both consumers closing after
    /// releasing everything. Passes, and the counts are reported.
    #[test]
    fn rebalance_callbacks_are_counted_and_a_clean_sequence_passes() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0, 1]));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[1]));
        v.record(rebalance("c2", RebalanceCallback::Assigned, &[1]));
        v.record(rebalance("c2", RebalanceCallback::Lost, &[1]));
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[1]));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0, 1]));
        v.record(closed("c1"));
        v.record(closed("c2"));
        let verdict = v.verdict(0);
        assert!(verdict.rebalance_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.assigned_callbacks, 3);
        assert_eq!(verdict.revoked_callbacks, 2);
        assert_eq!(verdict.lost_callbacks, 1);
        assert_eq!(verdict.listener_consumers, 2);
        assert!(
            verdict
                .to_string()
                .contains("rebalance callbacks       : revoked=2 assigned=3 lost=1 (2 consumer(s) with listener)")
        );
    }

    /// No listener anywhere (e.g. gRPC consumers): zero counts, no violation.
    #[test]
    fn no_rebalance_callbacks_is_reported_not_failed() {
        let v = ConservationVerifier::new();
        v.record(closed("c1"));
        let verdict = v.verdict(0);
        assert!(verdict.rebalance_violations.is_empty());
        assert_eq!(
            (verdict.revoked_callbacks, verdict.assigned_callbacks, verdict.lost_callbacks),
            (0, 0, 0)
        );
        assert_eq!(verdict.listener_consumers, 0);
        assert!(
            verdict
                .to_string()
                .contains("revoked=0 assigned=0 lost=0 (0 consumer(s) with listener)")
        );
    }

    #[test]
    fn assigned_twice_without_a_release_fails() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        let verdict = v.verdict(0);
        assert_eq!(verdict.rebalance_violations.len(), 1);
        assert_eq!(
            verdict.rebalance_violations[0],
            "c1: on_partitions_assigned for t-0 which it already owned (no revoked/lost in between)"
        );
        assert!(
            verdict
                .reasons
                .iter()
                .any(|r| r.starts_with("rebalance listener contract: 1 violation(s)"))
        );
    }

    #[test]
    fn releasing_an_unowned_partition_fails_for_revoked_and_lost() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0]));
        v.record(rebalance("c1", RebalanceCallback::Lost, &[1]));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.rebalance_violations,
            vec![
                "c1: on_partitions_revoked for t-0 which it did not own".to_string(),
                "c1: on_partitions_lost for t-1 which it did not own".to_string(),
            ]
        );
    }

    /// The broker hands p0 to c2 while c1 still owns it. That only happens when
    /// c1 was fenced, so c1 must report the partition lost. Reporting it
    /// revoked claims a clean handoff that did not happen.
    #[test]
    fn clean_revoke_after_the_partition_already_moved_fails_but_lost_passes() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0, 1]));
        v.record(rebalance("c2", RebalanceCallback::Assigned, &[0, 1]));
        v.record(rebalance("c1", RebalanceCallback::Lost, &[0]));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[1]));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.rebalance_violations,
            vec![
                "c1: on_partitions_revoked for t-1 after c2 had already been assigned it (a clean handoff reported \
                 after the partition moved; expected on_partitions_lost)"
                    .to_string()
            ]
        );
    }

    #[test]
    fn closing_while_still_owning_partitions_fails() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[2, 0]));
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[2]));
        v.record(closed("c1"));
        let verdict = v.verdict(0);
        assert_eq!(
            verdict.rebalance_violations,
            vec![
                "c1: closed while still owning [\"t-0\"] (no on_partitions_revoked / on_partitions_lost before close)"
                    .to_string()
            ]
        );
    }

    /// A commit failing inside `on_partitions_revoked` is summarized under its
    /// own operation, not scored — like every other consumer error.
    #[test]
    fn revoke_commit_errors_are_summarized_separately() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::ConsumerError {
            consumer: "c1".into(),
            op: ConsumerOp::RevokeCommit,
            error: "boom".into(),
        });
        let verdict = v.verdict(0);
        assert!(verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.revoke_commit_errors, 1);
        assert_eq!(verdict.commit_errors, 0);
        let text = verdict.to_string();
        assert!(text.contains("commit errors in revoked  : 1"), "{text}");
        assert!(text.contains("1x consumer commit inside on_partitions_revoked: boom"), "{text}");
    }

    fn delivered(index: u64) -> WorkloadEvent {
        WorkloadEvent::Delivered { index, topic: "t".into(), topic_id: zero(), partition: 0, offset: index as i64 }
    }

    fn consumed(index: u64) -> WorkloadEvent {
        WorkloadEvent::Consumed {
            consumer: "c".into(),
            index,
            topic: "t".into(),
            topic_id: zero(),
            partition: 0,
            offset: index as i64,
        }
    }

    /// Sequential sends, each settled before the next is issued: the peak is 1
    /// and no window is left open.
    #[test]
    fn one_record_in_flight_per_producer_passes() {
        let v = ConservationVerifier::new();
        for i in 0..50u64 {
            v.record(sent(i, "producer-rust-1"));
            v.record(delivered(i));
            v.record(consumed(i));
        }
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "sequential sends must pass: {verdict}");
        assert_eq!(verdict.max_in_flight, 1);
        assert_eq!(verdict.unsettled_sends, 0);
    }

    /// Pipelined sends (a second send issued before the first has settled) are
    /// the normal case: the run passes and the verdict reports the peak.
    #[test]
    fn two_records_in_flight_passes_and_reports_the_peak() {
        let v = ConservationVerifier::new();
        v.record(sent(0, "producer-rust-1"));
        v.record(sent(1, "producer-rust-1"));
        v.record(delivered(0));
        v.record(delivered(1));
        v.record(consumed(0));
        v.record(consumed(1));
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "pipelined sends must pass: {verdict}");
        assert_eq!(verdict.max_in_flight, 2);
        assert_eq!(verdict.unsettled_sends, 0, "both sends settled");
    }

    /// A failed send closes the window in the same way as an acknowledgement:
    /// sending again after a `SendFailed` is still one record in flight. The
    /// run fails for the failed send itself, not for an unsettled window.
    #[test]
    fn failed_send_closes_the_in_flight_window() {
        let v = ConservationVerifier::new();
        v.record(sent(0, "producer-rust-1"));
        v.record(WorkloadEvent::SendFailed { index: 0, topic: "t".into(), error: "timed out".into() });
        v.record(sent(1, "producer-rust-1"));
        v.record(delivered(1));
        v.record(consumed(1));
        let verdict = v.verdict(1);
        assert_eq!(verdict.max_in_flight, 1);
        assert_eq!(verdict.unsettled_sends, 0);
        assert!(!verdict.reasons.iter().any(|r| r.starts_with("unsettled sends")), "{verdict}");
        assert_eq!(verdict.failed_sends, 1);
        assert!(!verdict.is_pass(), "{verdict}");
    }

    /// Any failed send fails the run. The producer's configuration retries
    /// through every fault in the matrix, so a failure indicates a defect, and
    /// a failure after the delivery timeout leaves the record's fate unknown.
    /// The reason carries the record and the client's error text.
    #[test]
    fn any_failed_send_fails_the_run() {
        let v = ConservationVerifier::new();
        for i in 0..3 {
            v.record(delivered(i));
            v.record(consumed(i));
        }
        v.record(WorkloadEvent::SendFailed {
            index: 3,
            topic: "t".into(),
            error: "Timeout: Timeout expired after 120000ms".into(),
        });
        let verdict = v.verdict(1);
        assert!(!verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.failed_sends, 1);
        assert_eq!(verdict.lost.len(), 0, "the failure is the failed send, not loss");
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("failed sends"))
            .unwrap_or_else(|| panic!("expected a failed-sends reason: {verdict}"));
        assert!(
            reason.contains("1 record(s)") && reason.contains("t#3: Timeout: Timeout expired after 120000ms"),
            "reason must name the record and the error: {reason}"
        );
    }

    /// Consumer poll and commit errors are counted per operation and grouped by
    /// error text in the breakdown, most frequent first, together with failed
    /// sends. They do not by themselves fail the run.
    #[test]
    fn consumer_errors_are_summarized_but_not_scored() {
        let v = ConservationVerifier::new();
        for i in 0..3 {
            v.record(delivered(i));
            v.record(consumed(i));
        }
        let stale = "IllegalStateError: OffsetCommit failed with stale member epoch.";
        for consumer in ["consumer-rust-1", "consumer-rust-1", "consumer-rust-2"] {
            v.record(WorkloadEvent::ConsumerError {
                consumer: consumer.into(),
                op: ConsumerOp::Commit,
                error: stale.into(),
            });
        }
        v.record(WorkloadEvent::ConsumerError {
            consumer: "consumer-rust-2".into(),
            op: ConsumerOp::Commit,
            error: "TimeoutError: Timeout of 60000ms expired before successfully committing offsets".into(),
        });
        v.record(WorkloadEvent::ConsumerError {
            consumer: "consumer-rust-1".into(),
            op: ConsumerOp::Poll,
            error: "DisconnectError: broker 2 disconnected".into(),
        });
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.poll_errors, 1);
        assert_eq!(verdict.commit_errors, 4);
        assert_eq!(
            verdict.error_breakdown,
            vec![
                (format!("consumer commit: {stale}"), 3),
                (
                    "consumer commit: TimeoutError: Timeout of 60000ms expired before successfully committing offsets"
                        .into(),
                    1
                ),
                ("consumer poll: DisconnectError: broker 2 disconnected".into(), 1),
            ]
        );
        let rendered = verdict.to_string();
        assert!(rendered.contains("consumer commit errors    : 4"), "{rendered}");
        assert!(rendered.contains(&format!("    3x consumer commit: {stale}")), "{rendered}");
    }

    /// Failed sends appear in the same breakdown under `producer send`.
    #[test]
    fn failed_sends_appear_in_the_error_breakdown() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::SendFailed { index: 0, topic: "t".into(), error: "Timeout: expired".into() });
        v.record(WorkloadEvent::SendFailed { index: 1, topic: "t".into(), error: "Timeout: expired".into() });
        let verdict = v.verdict(1);
        assert_eq!(
            verdict.error_breakdown,
            vec![("producer send: Timeout: expired".to_string(), 2)]
        );
        assert!(
            verdict.to_string().contains("    2x producer send: Timeout: expired"),
            "{verdict}"
        );
    }

    /// A run with no failed sends does not carry a failed-sends reason.
    #[test]
    fn zero_failed_sends_passes() {
        let v = ConservationVerifier::new();
        for i in 0..3 {
            v.record(delivered(i));
            v.record(consumed(i));
        }
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.failed_sends, 0);
    }

    /// The peak is per producer, not global: two producers with one record in
    /// flight each report a peak of 1 (a global count would be 2).
    #[test]
    fn in_flight_peak_is_per_producer_not_global() {
        let v = ConservationVerifier::new();
        v.record(WorkloadEvent::Sent { index: 0, topic: "t0".into(), producer: "producer-rust-100".into() });
        v.record(WorkloadEvent::Sent { index: 0, topic: "t1".into(), producer: "producer-rust-101".into() });
        for topic in ["t0", "t1"] {
            v.record(WorkloadEvent::Delivered {
                index: 0,
                topic: topic.into(),
                topic_id: zero(),
                partition: 0,
                offset: 0,
            });
            v.record(WorkloadEvent::Consumed {
                consumer: "c".into(),
                index: 0,
                topic: topic.into(),
                topic_id: zero(),
                partition: 0,
                offset: 0,
            });
        }
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "{verdict}");
        assert_eq!(
            verdict.max_in_flight, 1,
            "the peak is the maximum over producers, not their sum"
        );
    }

    /// A `Sent` with no `Delivered` or `SendFailed` by verdict time is an
    /// outcome the harness never recorded; the run fails and the reason names
    /// the record.
    #[test]
    fn unsettled_send_at_verdict_fails() {
        let v = ConservationVerifier::new();
        v.record(sent(0, "producer-rust-1"));
        v.record(delivered(0));
        v.record(consumed(0));
        v.record(sent(1, "producer-rust-1"));
        // index 1 never settles.
        let verdict = v.verdict(1);
        assert!(!verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.unsettled_sends, 1);
        assert_eq!(verdict.max_in_flight, 1, "an open window is not a second in flight");
        assert!(
            verdict
                .reasons
                .iter()
                .any(|r| r.starts_with("unsettled sends: 1") && r.contains("t#1")),
            "expected an unsettled-sends reason naming t#1, got {:?}",
            verdict.reasons
        );
    }

    /// The same logical record at two offsets within one topic generation is a
    /// producer double write, which `enable.idempotence=true` is required to
    /// prevent. It fails the run although conservation is intact and the 2×
    /// ratio bound is not approached. The reason names the record and both
    /// offsets.
    #[test]
    fn double_write_within_a_generation_fails() {
        let v = ConservationVerifier::new();
        let id = Uuid::with_bytes([1u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 0,
            offset: 5,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 0,
            offset: 9,
        });
        let verdict = v.verdict(1);
        assert!(!verdict.is_pass(), "a double write must fail: {verdict}");
        assert_eq!(verdict.producer_duplicates, vec![("t".to_string(), 5)]);
        assert_eq!(verdict.cross_generation_duplicates, 0);
        assert_eq!(verdict.lost.len(), 0, "the failure is the double write, not loss");
        assert_eq!(verdict.logical_duplicates, 1, "also counted by the by-index counter");
        assert_eq!(verdict.physical_duplicates, 0, "two distinct addresses, so not a re-read");
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("producer duplicates: 1 record(s)"))
            .unwrap_or_else(|| panic!("expected a producer-duplicates reason, got {:?}", verdict.reasons));
        assert!(reason.contains("t#5@[p0/5,p0/9]"), "reason must list both offsets: {reason}");
    }

    /// A double write across partitions of the same generation is still a
    /// double write: the record exists twice in the topic.
    #[test]
    fn double_write_across_partitions_of_one_generation_fails() {
        let v = ConservationVerifier::new();
        let id = Uuid::with_bytes([1u8; 16]);
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 0,
            offset: 5,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 1,
            offset: 2,
        });
        let verdict = v.verdict(1);
        assert_eq!(verdict.producer_duplicates, vec![("t".to_string(), 5)]);
        assert!(!verdict.is_pass());
    }

    /// Reading the same record twice from the same address is a consumer
    /// re-read (rebalance or failover). It is reported by both duplicate
    /// counters, is not a double write, and the run passes (below the 2× ratio
    /// bound).
    #[test]
    fn re_read_at_the_same_offset_is_not_a_double_write() {
        let v = ConservationVerifier::new();
        let id = Uuid::with_bytes([1u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 0,
            offset: 5,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: id,
            partition: 0,
            offset: 5,
        });
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "a re-read is reported, not failed: {verdict}");
        assert!(verdict.producer_duplicates.is_empty());
        assert_eq!(verdict.logical_duplicates, 1);
        assert_eq!(verdict.physical_duplicates, 1);
    }

    /// The same record in two topic generations is not a client fault: the
    /// recreate destroyed the broker's idempotent-producer state together with
    /// the old generation, so a retry whose acknowledgement was lost in the
    /// delete window is committed again in the new one. Excused and reported;
    /// never a failure.
    #[test]
    fn duplicate_across_generations_is_excused() {
        let v = ConservationVerifier::new();
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: old_id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: old_id,
            partition: 0,
            offset: 5,
        });
        v.record(WorkloadEvent::Consumed {
            consumer: "c".into(),
            index: 5,
            topic: "t".into(),
            topic_id: new_id,
            partition: 0,
            offset: 0,
        });
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "a cross-generation repeat is excused: {verdict}");
        assert!(verdict.producer_duplicates.is_empty());
        assert_eq!(verdict.cross_generation_duplicates, 1);
        assert_eq!(verdict.logical_duplicates, 1, "still visible in the by-index counter");
    }

    /// Event streams without `Sent` (the verifier's other tests, older workloads)
    /// are unaffected: a `Delivered` that was never `Sent` neither fails nor
    /// counts as in flight.
    #[test]
    fn delivered_without_sent_is_tolerated() {
        let v = ConservationVerifier::new();
        v.record(delivered(0));
        v.record(consumed(0));
        let verdict = v.verdict(1);
        assert!(verdict.is_pass(), "{verdict}");
        assert_eq!(verdict.max_in_flight, 0);
        assert_eq!(verdict.unsettled_sends, 0);
    }

    /// A delayed recreate mints a new topic id; records acked to the OLD id are on
    /// a destroyed generation and are expected-lost — even when their `Delivered`
    /// event arrives (as here) AFTER the recreate, the async-ack race the
    /// point-in-time snapshots cannot close. Records on the NEW (live) id that are
    /// never consumed are still real loss.
    #[test]
    fn destroyed_generation_old_id_records_are_not_loss() {
        let v = ConservationVerifier::new();
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        // Recreate happened: harness marks the old generation destroyed.
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        // A late ack to the OLD generation lands only now (after the recreate),
        // unobserved — must be excused, not loss.
        v.record(WorkloadEvent::Delivered { index: 50, topic: "t".into(), topic_id: old_id, partition: 0, offset: 50 });
        // A record on the NEW generation, unobserved -> REAL loss.
        v.record(WorkloadEvent::Delivered { index: 51, topic: "t".into(), topic_id: new_id, partition: 0, offset: 0 });
        let verdict = v.verdict(1);
        assert_eq!(
            verdict.lost,
            vec![("t".to_string(), 51)],
            "old-generation ack is excused; new-generation loss is real: {verdict}"
        );
        assert_eq!(verdict.expected_lost, 1, "the destroyed-generation record is expected-lost");
        assert!(!verdict.is_pass(), "unconsumed live-generation record must fail");
    }

    /// Record `index` of topic `topic` delivered at `(topic_id, partition,
    /// offset)`.
    fn delivered_in(topic: &str, topic_id: Uuid, index: u64, partition: i32, offset: i64) -> WorkloadEvent {
        WorkloadEvent::Delivered { index, topic: topic.into(), topic_id, partition, offset }
    }

    /// Record `index` of topic `topic` consumed at `(topic_id, partition,
    /// offset)`.
    fn consumed_in(topic: &str, topic_id: Uuid, index: u64, partition: i32, offset: i64) -> WorkloadEvent {
        WorkloadEvent::Consumed { consumer: "c".into(), index, topic: topic.into(), topic_id, partition, offset }
    }

    /// A destroyed generation excuses only what was still unread when its
    /// delete began: records the consumer had not reached on their partition,
    /// acknowledged within the tail window before the delete. A gap below the
    /// consumer's position is loss, and so is a partition the consumer never
    /// read if its records are older than the window. Excusing the whole
    /// generation hid exactly those stuck partitions (F9).
    #[test]
    fn destroyed_generation_excuses_only_the_unread_recent_tail() {
        let old_id = Uuid::with_bytes([7u8; 16]);
        let feed = |v: &ConservationVerifier| {
            // p0: offsets 0..=9 acked, the consumer read 0..=5 except 3 (a gap).
            for offset in 0..=9 {
                v.record(delivered_in("t", old_id, offset as u64, 0, offset));
            }
            for offset in [0, 1, 2, 4, 5] {
                v.record(consumed_in("t", old_id, offset as u64, 0, offset));
            }
            // p1: offsets 0..=4 acked, never read (stuck).
            for offset in 0..=4 {
                v.record(delivered_in("t", old_id, 10 + offset as u64, 1, offset));
            }
            std::thread::sleep(Duration::from_millis(5));
            v.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
            std::thread::sleep(Duration::from_millis(5));
            // Acks landing after the delete began (the async-ack race).
            v.record(delivered_in("t", old_id, 15, 0, 10));
            v.record(delivered_in("t", old_id, 16, 1, 5));
        };

        // Everything acked within the window: only the gap is loss.
        let v = ConservationVerifier::new();
        feed(&v);
        let verdict = v.verdict(0);
        assert_eq!(verdict.lost, vec![("t".to_string(), 3)], "the gap is loss: {verdict}");
        assert_eq!(verdict.expected_lost, 11, "the unread tail of p0 and all of p1: {verdict}");

        // A zero window: only acks after the delete began are recent, so the
        // older unread tail of p0 and the stuck p1 are loss.
        let v = ConservationVerifier::with_destroyed_tail_window(Duration::ZERO);
        feed(&v);
        let verdict = v.verdict(0);
        let mut lost = verdict.lost.clone();
        lost.sort_unstable();
        let expected: Vec<LogicalKey> = [3, 6, 7, 8, 9, 10, 11, 12, 13, 14].map(|i| ("t".to_string(), i)).to_vec();
        assert_eq!(lost, expected, "{verdict}");
        assert_eq!(verdict.expected_lost, 2, "only the two post-delete acks: {verdict}");
        let p1 = verdict
            .lost_by_partition
            .iter()
            .find(|p| p.partition == 1)
            .unwrap_or_else(|| panic!("p1 must be reported: {verdict}"));
        assert_eq!((p1.topic_id, p1.first_lost_offset, p1.last_lost_offset), (old_id, 0, 4));
        assert_eq!(p1.last_consumed_offset, None, "p1 was never read: {verdict}");
    }

    /// The head of a new generation the consumer skipped past is excused by
    /// the records' acknowledged generation, even when they were acknowledged
    /// before the blackout floor was taken and so sit at or below it (the floor
    /// race, F9). A partition of the new generation the consumer never read is
    /// still loss.
    #[test]
    fn new_generation_skipped_head_is_excused_by_ack_id_even_below_the_floor() {
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        let v = ConservationVerifier::new();
        for i in 0..5u64 {
            v.record(delivered_in("t", old_id, i, 0, i as i64));
            v.record(consumed_in("t", old_id, i, 0, i as i64));
        }
        v.note_expected_loss(ExpectedLossHint::AllDeliveredSoFar);
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        v.note_expected_loss(ExpectedLossHint::NewGeneration(new_id));
        // Acknowledged by the new generation before the floor is taken.
        for (i, offset) in [(5u64, 0i64), (6, 1), (7, 2)] {
            v.record(delivered_in("t", new_id, i, 0, offset));
        }
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        // The consumer starts reading the new p0 at offset 3.
        for (i, offset) in [(8u64, 3i64), (9, 4)] {
            v.record(delivered_in("t", new_id, i, 0, offset));
            v.record(consumed_in("t", new_id, i, 0, offset));
        }
        // The new p1 is never read.
        v.record(delivered_in("t", new_id, 10, 1, 0));

        let verdict = v.verdict(0);
        assert_eq!(verdict.lost, vec![("t".to_string(), 10)], "{verdict}");
        assert_eq!(verdict.expected_lost, 3, "the skipped head 0..=2 of the new p0: {verdict}");
    }

    /// The per-partition loss report reads "consumed" from the same incarnation
    /// as the lost records. An offset the consumer reached in the destroyed
    /// incarnation says nothing about the new one: here the new incarnation was
    /// never read (stuck), which is the known defect, where borrowing the old
    /// incarnation's offset 17 made it look like a skipped head of 0..=21 —
    /// and not the defect at all, since 17 < 21 (F9).
    #[test]
    fn consumed_offsets_are_per_incarnation_in_the_loss_report_and_the_label() {
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        let other_id = Uuid::with_bytes([9u8; 16]);
        let v = ConservationVerifier::new();
        v.record(delivered_in("t1", other_id, 0, 0, 0));
        v.record(consumed_in("t1", other_id, 0, 0, 0));
        for i in 0..=17u64 {
            v.record(delivered_in("t0", old_id, i, 1, i as i64));
            v.record(consumed_in("t0", old_id, i, 1, i as i64));
        }
        v.note_expected_loss(ExpectedLossHint::AllDeliveredForTopic("t0".to_string()));
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t0".to_string()));
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        v.note_expected_loss(ExpectedLossHint::NewGeneration(new_id));
        for offset in 0..=21i64 {
            v.record(delivered_in("t0", new_id, 18 + offset as u64, 1, offset));
        }

        let verdict = v.verdict(0);
        assert_eq!(verdict.lost.len(), 22, "{verdict}");
        assert_eq!(
            verdict.lost_by_partition,
            vec![LostPartition {
                topic: "t0".into(),
                partition: 1,
                topic_id: new_id,
                lost: 22,
                first_lost_offset: 0,
                last_lost_offset: 21,
                last_delivered_offset: 21,
                last_consumed_offset: None,
            }],
            "{verdict}"
        );
        assert!(
            verdict.to_string().contains(&format!(
                "t0 p1 [{new_id}]: 22 lost at offsets 0..=21; last acked offset 21, highest consumed offset none"
            )),
            "{verdict}"
        );
        let label = verdict
            .known_defect
            .as_deref()
            .unwrap_or_else(|| panic!("expected the label: {verdict}"));
        assert!(label.contains("0 head skipped") && label.contains("1 stuck"), "{label}");
    }

    /// A closing consumer's close-time revoke of partitions it was never told
    /// it owned is Java-faithful (F10a): counted, not a violation. The same
    /// release from a consumer that is not closing still fails (see
    /// `releasing_an_unowned_partition_fails_for_revoked_and_lost`).
    #[test]
    fn a_closing_consumer_may_release_partitions_it_was_never_assigned() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[0]));
        v.record(WorkloadEvent::ConsumerClosing { consumer: "c1".into() });
        v.record(rebalance("c1", RebalanceCallback::Revoked, &[0, 4, 5]));
        v.record(closed("c1"));
        let verdict = v.verdict(0);
        assert!(verdict.rebalance_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.close_time_unowned_releases, 2);
        assert!(verdict.is_pass(), "{verdict}");
        assert!(
            verdict
                .to_string()
                .contains("close-time releases of never-assigned partitions (Java-faithful, not scored): 2"),
            "{verdict}"
        );
    }

    /// On a recreated topic two consumers can own the same partition name for
    /// two incarnations, and the broker keeps one committed offset per name, so
    /// the read-back may be the other owner's (F10b). Such a check is skipped
    /// and counted; once one owner remains it is compared again.
    #[test]
    fn committed_offset_is_not_compared_while_two_owners_share_a_recreated_partition() {
        let v = ConservationVerifier::new();
        v.record(rebalance("c1", RebalanceCallback::Assigned, &[5]));
        v.note_expected_loss(ExpectedLossHint::RecreateBlackout("t".to_string()));
        v.record(rebalance("c2", RebalanceCallback::Assigned, &[5]));
        v.record(consumed_by("c1", 5, 39));
        v.record(committed("c1", 5, 31));
        let verdict = v.verdict(0);
        assert!(verdict.commit_violations.is_empty(), "{verdict}");
        assert_eq!((verdict.commit_checks, verdict.ambiguous_commit_checks), (0, 1));

        v.record(rebalance("c2", RebalanceCallback::Revoked, &[5]));
        v.record(committed("c1", 5, 31));
        let verdict = v.verdict(0);
        assert_eq!(verdict.commit_checks, 1);
        assert_eq!(
            verdict.commit_violations,
            vec![
                "c1: committed offset 31 on t-5 is behind its own consumption (last consumed 39, expected 40)"
                    .to_string()
            ]
        );
    }
}
