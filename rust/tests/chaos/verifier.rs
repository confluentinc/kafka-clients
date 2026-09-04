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
#[derive(Debug, Clone, Copy)]
pub enum ExpectedLossHint {
    /// A topic recreate destroyed everything delivered so far.
    AllDeliveredSoFar,
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

    /// Render the final verdict.
    fn verdict(&self, min_partitions: usize) -> ChaosVerdict;
}

/// Physical record address: `(topic_id, partition, offset)`.
type PhysKey = (Uuid, i32, i64);

/// The default verifier: conservation + per-record bookkeeping on both the
/// logical `index` and the physical `(topic_id, partition, offset)` key. This
/// carries the checks the harness has always done, now dual-keyed.
#[derive(Default)]
pub struct ConservationVerifier {
    inner: Mutex<ConservationState>,
}

#[derive(Default)]
struct ConservationState {
    /// index -> was it broker-acked (must be consumed unless expected-lost).
    delivered: HashMap<u64, PhysKey>,
    /// index -> times the consumer observed it (>1 = redelivery).
    observed: HashMap<u64, u32>,
    /// Physical addresses the consumer has seen — a second sighting of the same
    /// (topic_id, partition, offset) is a physical duplicate.
    phys_seen: HashMap<PhysKey, u32>,
    /// Partitions that carried at least one observed record (coverage).
    partitions_seen: BTreeSet<i32>,
    /// indices legitimately destroyed by a topic recreate (not loss).
    expected_lost: BTreeSet<u64>,
    /// Count of producer sends that failed (context, not loss).
    failed_sends: usize,
    /// Total consume events seen (incl. redeliveries) — the drain-progress
    /// signal read by `consumed_progress`.
    consumed_events: u64,
}

impl ConservationVerifier {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Verifier for ConservationVerifier {
    fn record(&self, event: WorkloadEvent) {
        let mut s = self.inner.lock().expect("verifier poisoned");
        match event {
            WorkloadEvent::Delivered { index, topic_id, partition, offset, .. } => {
                s.delivered.insert(index, (topic_id, partition, offset));
            },
            WorkloadEvent::SendFailed { .. } => {
                s.failed_sends += 1;
            },
            WorkloadEvent::Consumed { index, topic_id, partition, offset, .. } => {
                *s.observed.entry(index).or_insert(0) += 1;
                *s.phys_seen.entry((topic_id, partition, offset)).or_insert(0) += 1;
                s.partitions_seen.insert(partition);
                s.consumed_events += 1;
            },
            // The conservation verifier does not interpret share-consumer
            // events; a ShareAckVerifier will.
            WorkloadEvent::Acked { .. } | WorkloadEvent::DeliveryCount { .. } => {},
        }
    }

    fn consumed_progress(&self) -> u64 {
        self.inner.lock().expect("verifier poisoned").consumed_events
    }

    fn note_expected_loss(&self, hint: ExpectedLossHint) {
        let mut s = self.inner.lock().expect("verifier poisoned");
        match hint {
            ExpectedLossHint::AllDeliveredSoFar => {
                let all: Vec<u64> = s.delivered.keys().copied().collect();
                s.expected_lost.extend(all);
            },
        }
    }

    fn verdict(&self, min_partitions: usize) -> ChaosVerdict {
        let s = self.inner.lock().expect("verifier poisoned");

        // Loss: a delivered index the consumer never observed, and not one a
        // topic recreate legitimately destroyed.
        let lost: Vec<u64> = s
            .delivered
            .keys()
            .copied()
            .filter(|idx| !s.observed.contains_key(idx) && !s.expected_lost.contains(idx))
            .collect();

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
            reasons.push(format!(
                "data loss: {} acknowledged record(s) never consumed (sample indices: {sample:?})",
                lost.len()
            ));
        }
        if s.partitions_seen.len() < min_partitions {
            reasons.push(format!(
                "partition coverage: only {} partition(s) produced records, expected >= {min_partitions}",
                s.partitions_seen.len()
            ));
        }

        ChaosVerdict {
            delivered: s.delivered.len(),
            expected_lost: s.expected_lost.len(),
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
    /// Delivered indices the consumer never observed (data loss).
    pub lost: Vec<u64>,
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
