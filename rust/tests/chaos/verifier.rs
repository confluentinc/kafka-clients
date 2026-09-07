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

use std::collections::{BTreeSet, HashMap};
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
    /// Producer received a broker ack for `index` at this physical address.
    Delivered {
        index: u64,
        topic: String,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
    },
    /// Producer's send failed — the record was never committed, so it is not
    /// data loss; recorded only for context.
    SendFailed { index: u64, topic: String, error: String },
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
    /// Count of producer sends that failed (context, not loss).
    failed_sends: usize,
    /// Total consume events seen (incl. redeliveries) — the drain-progress
    /// signal read by `consumed_progress`.
    consumed_events: u64,
    /// Per-topic consume-event count (incl. redeliveries) — the per-topic
    /// drain-progress signal read by `consumed_progress_for_topic`, used to
    /// detect that a just-recreated topic's consumer has resumed.
    consumed_events_by_topic: HashMap<String, u64>,
}

impl ConservationVerifier {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConservationState {
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

    /// Delivered records currently scored as loss (unobserved, not expected-lost,
    /// and above any blackout resume point). Shared by `verdict` and `outstanding`
    /// so the drain waits for exactly what the verdict checks.
    fn lost_keys(&self) -> Vec<LogicalKey> {
        let blackout_resume = self.blackout_resume();
        self.delivered
            .keys()
            .filter(|key| !self.observed.contains_key(*key) && !self.expected_lost.contains(*key))
            .filter(|(topic, idx)| match blackout_resume.get(topic.as_str()) {
                Some(&resume) => *idx > resume,
                None => true,
            })
            .cloned()
            .collect()
    }
}

impl Verifier for ConservationVerifier {
    fn record(&self, event: WorkloadEvent) {
        let mut s = self.inner.lock().expect("verifier poisoned");
        match event {
            WorkloadEvent::Delivered { index, topic, topic_id, partition, offset } => {
                s.delivered.insert((topic, index), (topic_id, partition, offset));
            },
            WorkloadEvent::SendFailed { .. } => {
                s.failed_sends += 1;
            },
            WorkloadEvent::Consumed { index, topic, topic_id, partition, offset } => {
                *s.observed.entry((topic.clone(), index)).or_insert(0) += 1;
                *s.phys_seen.entry((topic_id, partition, offset)).or_insert(0) += 1;
                s.partitions_seen.insert(partition);
                s.consumed_events += 1;
                *s.consumed_events_by_topic.entry(topic).or_insert(0) += 1;
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

        let mut reasons = Vec::new();
        if !lost.is_empty() {
            let mut sample = lost.clone();
            sample.sort_unstable();
            sample.truncate(20);
            // Render as `topic#index` so multi-topic loss is legible.
            let sample_str: Vec<String> = sample.iter().map(|(t, idx)| format!("{t}#{idx}")).collect();
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

        // Report expected-lost as every delivered record that was legitimately
        // excused: the explicit expected_lost snapshots PLUS blackout-skipped
        // records (unobserved, below a blackout topic's resume point) not already
        // counted in expected_lost.
        let blackout_resume = s.blackout_resume();
        let blackout_excused = s
            .delivered
            .keys()
            .filter(|key| !s.observed.contains_key(*key) && !s.expected_lost.contains(*key))
            .filter(|(topic, idx)| match blackout_resume.get(topic.as_str()) {
                Some(&resume) => *idx <= resume,
                None => false,
            })
            .count();

        ChaosVerdict {
            delivered: s.delivered.len(),
            expected_lost: s.expected_lost.len() + blackout_excused,
            failed_sends: s.failed_sends,
            logical_duplicates,
            physical_duplicates,
            partitions_covered: s.partitions_seen.len(),
            lost,
            reasons,
        }
    }
}

/// Pass/fail verdict of a chaos run.
#[derive(Debug)]
pub struct ChaosVerdict {
    pub delivered: usize,
    pub expected_lost: usize,
    pub failed_sends: usize,
    /// Same logical `index` observed more than once (redelivery).
    pub logical_duplicates: u64,
    /// Same physical `(topic_id, partition, offset)` seen more than once.
    pub physical_duplicates: u64,
    pub partitions_covered: usize,
    /// Delivered `(topic, index)` records the consumer never observed (data
    /// loss).
    pub lost: Vec<LogicalKey>,
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
        writeln!(f, "  failed sends (not loss)   : {}", self.failed_sends)?;
        writeln!(f, "  duplicates (by index)     : {}", self.logical_duplicates)?;
        writeln!(f, "  duplicates (by offset)    : {}", self.physical_duplicates)?;
        writeln!(f, "  partitions covered        : {}", self.partitions_covered)?;
        if self.expected_lost > 0 {
            writeln!(f, "  expected-lost (recreate)  : {}", self.expected_lost)?;
        }
        writeln!(f, "  lost (delivered, unseen)  : {}", self.lost.len())?;
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
}
