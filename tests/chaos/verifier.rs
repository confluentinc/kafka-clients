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
    /// Consumer received `index` at this physical address. `topic_id` is
    /// best-effort: `Uuid::zero()` when the consumer could not resolve it
    /// (single-topic non-recreate runs do not need it).
    Consumed {
        index: u64,
        topic: String,
        topic_id: Uuid,
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
}

impl std::fmt::Display for ConsumerOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ConsumerOp::Poll => "consumer poll",
            ConsumerOp::Commit => "consumer commit",
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
    /// At verdict time, any delivered-but-unobserved record on this topic whose
    /// index is BELOW the highest index the consumer eventually observed on it is
    /// excused (the consumer provably skipped over it). This is the per-topic
    /// analog of librdkafka's pre-delete-HWM → post-recreate expected-loss
    /// window, and — unlike a point-in-time snapshot — it robustly covers the
    /// whole blackout the producer's non-resetting index keeps delivering into,
    /// regardless of whether the recreate reused the topic id (immediate),
    /// minted a new one (delayed), or the id could not be resolved under churn.
    /// Records ABOVE the resume point that are unobserved remain real loss.
    RecreateBlackout(String),
    /// A topic recreate replaced generation `old_id` with a new one, destroying
    /// every record acked to `old_id`. At verdict, any delivered-but-unobserved
    /// record whose physical `topic_id` is a destroyed generation is expected-lost
    /// — the operator deleted the data the client was correctly acked into.
    ///
    /// Unlike the point-in-time snapshots (`AllDeliveredForTopic`), this is
    /// evaluated against the `topic_id` recorded WITH each delivered record. The
    /// producer stamps that id when the ack arrives, from a map the harness
    /// switches to the new generation's id (taken from the create-topics
    /// response) before the producer can be acked by the new generation. So
    /// with thousands of records in flight across the recreate, a record
    /// buffered during the delete or retried into the new generation carries
    /// the new id and stays in the loss check, while only records the old
    /// leader actually acked carry `old_id` and are excused. Only emitted when
    /// the recreate minted a genuinely new id (delayed recreate); an immediate
    /// recreate that reuses the id relies on `RecreateBlackout`.
    DestroyedGeneration(Uuid),
}

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
    /// Partitions that carried at least one observed record (coverage).
    partitions_seen: BTreeSet<i32>,
    /// (topic, index) pairs legitimately destroyed by a topic recreate (not
    /// loss).
    expected_lost: BTreeSet<LogicalKey>,
    /// Topics that underwent a recreate blackout. At verdict, an unobserved
    /// delivered record on one of these whose index is below the max observed
    /// index on that topic is excused (the consumer skipped it during the
    /// blackout). See [`ExpectedLossHint::RecreateBlackout`].
    recreate_blackout: BTreeSet<String>,
    /// Topic-ids of destroyed generations (a delayed recreate minted a new id).
    /// At verdict, an unobserved delivered record whose physical `topic_id` is in
    /// this set is expected-lost. See [`ExpectedLossHint::DestroyedGeneration`].
    destroyed_generations: BTreeSet<Uuid>,
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
    /// (topic, partition) -> offset of the most recent acknowledgement on it,
    /// in event order (not the maximum: a recreate resets offsets to 0, and the
    /// latest ack is what the per-partition loss report needs).
    last_delivered_offset: HashMap<(String, i32), i64>,
    /// (topic, partition) -> offset of the most recent record the consumer
    /// observed on it, in event order. Compared with `last_delivered_offset`
    /// in the per-partition loss report to show where the consumer stopped.
    last_consumed_offset: HashMap<(String, i32), i64>,
}

impl ConservationVerifier {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConservationState {
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

    fn blackout_resume(&self) -> HashMap<&str, u64> {
        self.recreate_blackout
            .iter()
            .map(|topic| {
                let max_observed = self
                    .observed
                    .keys()
                    .filter(|(t, _)| t == topic)
                    .map(|(_, idx)| *idx)
                    .max()
                    .unwrap_or(0);
                (topic.as_str(), max_observed)
            })
            .collect()
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

    /// Delivered records currently scored as loss (unobserved, not expected-lost,
    /// and above any blackout resume point). Shared by `verdict` and `outstanding`
    /// so the drain waits for exactly what the verdict checks.
    fn lost_keys(&self) -> Vec<LogicalKey> {
        let blackout_resume = self.blackout_resume();
        self.delivered
            .iter()
            .filter(|(key, _)| !self.observed.contains_key(*key) && !self.expected_lost.contains(*key))
            // A delivered record whose physical topic_id is a destroyed
            // generation is expected-lost regardless of when its ack landed.
            .filter(|(_, (topic_id, _, _))| !self.destroyed_generations.contains(topic_id))
            .filter(|((topic, idx), _)| match blackout_resume.get(topic.as_str()) {
                Some(&resume) => *idx > resume,
                None => true,
            })
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
                s.last_delivered_offset.insert((topic.clone(), partition), offset);
                let key = (topic, index);
                s.settle(&key);
                s.delivered.insert(key, (topic_id, partition, offset));
            },
            WorkloadEvent::SendFailed { index, topic, error } => {
                let key = (topic, index);
                s.settle(&key);
                s.failed_sends.push((key, error));
            },
            WorkloadEvent::Consumed { index, topic, topic_id, partition, offset } => {
                *s.observed.entry((topic.clone(), index)).or_insert(0) += 1;
                let addr = (topic_id, partition, offset);
                let addrs = s.observed_at.entry((topic.clone(), index)).or_default();
                if !addrs.contains(&addr) {
                    addrs.push(addr);
                }
                *s.phys_seen.entry(addr).or_insert(0) += 1;
                s.partitions_seen.insert(partition);
                s.consumed_events += 1;
                s.last_consumed_offset.insert((topic.clone(), partition), offset);
                *s.consumed_events_by_topic.entry(topic).or_insert(0) += 1;
            },
            WorkloadEvent::ConsumerError { consumer, op, error } => {
                s.consumer_errors.push((consumer, op, error));
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
                s.recreate_blackout.insert(topic);
            },
            ExpectedLossHint::DestroyedGeneration(old_id) => {
                s.destroyed_generations.insert(old_id);
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
        // Where the loss sits: per (topic, partition), how many records were
        // lost, the offset range they occupy, and the last offset acknowledged
        // against the last one consumed on that partition. A partition whose
        // consumer stopped shows as "last consumed" far below "last acked" (or
        // absent), which is a different picture from loss scattered across
        // every partition.
        let mut by_partition: BTreeMap<(String, i32), (usize, i64, i64)> = BTreeMap::new();
        for key in &lost {
            let (_, partition, offset) = s.delivered[key];
            let entry = by_partition.entry((key.0.clone(), partition)).or_insert((0, offset, offset));
            entry.0 += 1;
            entry.1 = entry.1.min(offset);
            entry.2 = entry.2.max(offset);
        }
        let lost_by_partition: Vec<LostPartition> = by_partition
            .into_iter()
            .map(|((topic, partition), (count, first, last))| LostPartition {
                last_delivered_offset: s.last_delivered_offset[&(topic.clone(), partition)],
                last_consumed_offset: s.last_consumed_offset.get(&(topic.clone(), partition)).copied(),
                topic,
                partition,
                lost: count,
                first_lost_offset: first,
                last_lost_offset: last,
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
        if s.partitions_seen.len() < min_partitions {
            reasons.push(format!(
                "partition coverage: only {} partition(s) produced records, expected >= {min_partitions}",
                s.partitions_seen.len()
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
        let mut grouped: HashMap<String, usize> = HashMap::new();
        for (_, err) in &s.failed_sends {
            *grouped.entry(format!("producer send: {err}")).or_insert(0) += 1;
        }
        for (_, op, err) in &s.consumer_errors {
            *grouped.entry(format!("{op}: {err}")).or_insert(0) += 1;
        }
        let mut error_breakdown: Vec<(String, usize)> = grouped.into_iter().collect();
        error_breakdown.sort_by(|(a_text, a_n), (b_text, b_n)| b_n.cmp(a_n).then_with(|| a_text.cmp(b_text)));

        ChaosVerdict {
            delivered: s.delivered.len(),
            expected_lost,
            failed_sends: s.failed_sends.len(),
            poll_errors,
            commit_errors,
            error_breakdown,
            logical_duplicates,
            physical_duplicates,
            producer_duplicates,
            cross_generation_duplicates,
            partitions_covered: s.partitions_seen.len(),
            max_in_flight,
            unsettled_sends,
            lost,
            lost_by_partition,
            reasons,
        }
    }
}

/// Where a run's loss sits on one partition; see [`ChaosVerdict::lost_by_partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostPartition {
    pub topic: String,
    pub partition: i32,
    /// Lost records on this partition.
    pub lost: usize,
    /// Lowest and highest offset among the lost records.
    pub first_lost_offset: i64,
    pub last_lost_offset: i64,
    /// Offset of the most recent acknowledgement on this partition.
    pub last_delivered_offset: i64,
    /// Offset of the most recent record the consumer observed on this
    /// partition; `None` if it never observed one.
    pub last_consumed_offset: Option<i64>,
}

/// Pass/fail verdict of a chaos run.
#[derive(Debug)]
pub struct ChaosVerdict {
    pub delivered: usize,
    pub expected_lost: usize,
    /// Producer sends that were rejected or not acknowledged within the
    /// delivery timeout. Non-zero fails the run.
    pub failed_sends: usize,
    /// Consumer `poll` calls that returned an error. Reported, not scored.
    pub poll_errors: usize,
    /// Consumer commit calls that returned an error. Reported, not scored.
    pub commit_errors: usize,
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
                writeln!(
                    f,
                    "    {} p{}: {} lost at offsets {}..={}; last acked offset {}, last consumed offset {consumed}",
                    p.topic, p.partition, p.lost, p.first_lost_offset, p.last_lost_offset, p.last_delivered_offset
                )?;
            }
        }
        writeln!(f, "  consumer poll errors      : {}", self.poll_errors)?;
        writeln!(f, "  consumer commit errors    : {}", self.commit_errors)?;
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero() -> Uuid {
        Uuid::zero()
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
                index: i,
                topic: "t".into(),
                topic_id: zero(),
                partition: 0,
                offset: i as i64,
            });
            if i % 2 == 0 {
                v.record(WorkloadEvent::Consumed {
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
        v.record(WorkloadEvent::Consumed { index: 10, topic: "t".into(), topic_id: zero(), partition: 0, offset: 10 });
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

    /// The per-topic consume-progress signal advances only for the named topic,
    /// so the drain can detect a resumed consumer on a just-recreated topic even
    /// while the other topics' consumption keeps the GLOBAL counter climbing.
    #[test]
    fn per_topic_consume_progress_is_scoped() {
        let v = ConservationVerifier::new();
        assert_eq!(v.consumed_progress_for_topic("t0"), 0);
        v.record(WorkloadEvent::Consumed { index: 0, topic: "t0".into(), topic_id: zero(), partition: 0, offset: 0 });
        v.record(WorkloadEvent::Consumed { index: 0, topic: "t1".into(), topic_id: zero(), partition: 0, offset: 0 });
        v.record(WorkloadEvent::Consumed { index: 1, topic: "t0".into(), topic_id: zero(), partition: 0, offset: 1 });
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
        let id0 = Uuid::from_bytes([1u8; 16]);
        let id1 = Uuid::from_bytes([2u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t0".into(), topic_id: id0, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t1".into(), topic_id: id1, partition: 0, offset: 5 });
        // Consume each once.
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t0".into(), topic_id: id0, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t1".into(), topic_id: id1, partition: 0, offset: 5 });

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
            v.record(WorkloadEvent::Consumed { index, topic: "t".into(), topic_id: zero(), partition, offset });
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
            rendered.contains("t p1: 3 lost at offsets 1..=3; last acked offset 3, last consumed offset 0"),
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
                .contains("t p2: 1 lost at offsets 9..=9; last acked offset 9, last consumed offset none")
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

    fn delivered(index: u64) -> WorkloadEvent {
        WorkloadEvent::Delivered { index, topic: "t".into(), topic_id: zero(), partition: 0, offset: index as i64 }
    }

    fn consumed(index: u64) -> WorkloadEvent {
        WorkloadEvent::Consumed { index, topic: "t".into(), topic_id: zero(), partition: 0, offset: index as i64 }
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
        let id = Uuid::from_bytes([1u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 9 });
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
        let id = Uuid::from_bytes([1u8; 16]);
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 1, offset: 2 });
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
        let id = Uuid::from_bytes([1u8; 16]);
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: id, partition: 0, offset: 5 });
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
        let old_id = Uuid::from_bytes([7u8; 16]);
        let new_id = Uuid::from_bytes([8u8; 16]);
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration(old_id));
        v.record(WorkloadEvent::Delivered { index: 5, topic: "t".into(), topic_id: old_id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: old_id, partition: 0, offset: 5 });
        v.record(WorkloadEvent::Consumed { index: 5, topic: "t".into(), topic_id: new_id, partition: 0, offset: 0 });
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
        let old_id = Uuid::from_bytes([7u8; 16]);
        let new_id = Uuid::from_bytes([8u8; 16]);
        // Recreate happened: harness marks the old generation destroyed.
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration(old_id));
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
}
