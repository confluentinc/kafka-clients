// Copyright 2026 Confluent Inc.
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

//! The [`Ledger`]: what every writer promised about each record, what the
//! readers saw, and the [`Verdict`] that compares the two.
//!
//! Each send ends in one of three buckets, decided from the error's type (never
//! its message):
//!
//! - **once**: acknowledged (and, in a transaction, committed). It must be read
//!   exactly once by the final read-back, and at least once by the live group.
//! - **at most once**: the outcome is unknown (a retriable error such as a
//!   delivery timeout, or a commit that failed and could not be aborted). It
//!   may be missing, but once a reader finds it (or a copy of it in a mirror)
//!   it was written and is due once like the rest; the records of a transaction
//!   of unknown outcome are all visible or none is.
//! - **never**: rejected, or in an aborted transaction. It must not be read.
//!
//! State is one byte per record (its bucket plus what the readers saw), so a
//! long run at a few thousand records per second stays in the tens of
//! megabytes. Placement (the delivery report's partition and offset against
//! where the record was read) is checked through two maps that only hold the
//! records acknowledged but not yet read, or read but not yet acknowledged.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Mutex;

use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{ConsumerRecord, OffsetAndMetadata};
use confluent_kafka::producer::RecordMetadata;

use super::record::{PayloadProfile, RecordGenerator, RecordId, TxnIntent, verify_record};

/// Bits of a record's state byte.
const EXPECT_MASK: u8 = 0b0000_0011;
const SEEN_LIVE: u8 = 0b0000_0100;
const ACKED: u8 = 0b0000_1000;
const FINAL_SHIFT: u32 = 4;
const FINAL_MASK: u8 = 0b0011_0000;

/// How many examples each failure category keeps.
const SAMPLES_PER_CATEGORY: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expectation {
    /// The writer has not reported the outcome yet.
    Pending = 0,
    Once = 1,
    AtMostOnce = 2,
    Never = 3,
}

fn expectation(flags: u8) -> Expectation {
    match flags & EXPECT_MASK {
        0 => Expectation::Pending,
        1 => Expectation::Once,
        2 => Expectation::AtMostOnce,
        _ => Expectation::Never,
    }
}

fn final_count(flags: u8) -> u8 {
    (flags & FINAL_MASK) >> FINAL_SHIFT
}

/// The bucket of a send whose delivery future failed: a retriable error (the
/// delivery timeout among them) may still have been written, anything else was
/// rejected.
fn failed_delivery(error: &Error) -> Expectation {
    if error.is_retriable_error() {
        Expectation::AtMostOnce
    } else {
        Expectation::Never
    }
}

/// Where a topic's records come from.
#[derive(Clone, Debug)]
pub enum StreamKind {
    /// Writers produce it and report every send.
    Source,
    /// A consume-transform-produce loop copies the named source topic into
    /// it: every record must appear in it as often as in the source.
    Mirror(String),
}

/// How a transaction ended, as the writer saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnOutcome {
    Committed,
    /// A planned abort that reached the broker: its records were flushed and
    /// `abort_transaction` succeeded.
    Aborted,
    /// Aborted or abandoned after something failed (a send, the flush, the
    /// commit or the abort itself): never committed, and not a planned abort.
    Failed,
    /// The commit failed and so did the abort: it may or may not have committed.
    Unknown,
}

/// Something the run did that is reported, not scored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    ConsumerError,
    CommitError,
    ProducerError,
    CleanRestart,
    HardStop,
    ProducerRestart,
}

impl Event {
    fn category(self) -> &'static str {
        match self {
            Event::ConsumerError => "consumer errors",
            Event::CommitError => "commit errors",
            Event::ProducerError => "producer errors",
            Event::CleanRestart => "clean restarts",
            Event::HardStop => "hard stops",
            Event::ProducerRestart => "producer restarts",
        }
    }
}

/// Everything the run counted. The failure counters are listed in
/// [`Counts::failures`].
#[derive(Clone, Debug, Default)]
pub struct Counts {
    pub sent: u64,
    pub acked: u64,
    pub send_rejected: u64,
    pub send_ambiguous: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub txns_failed: u64,
    pub txns_unknown: u64,
    pub producer_restarts: u64,
    pub producer_errors: u64,
    /// Transactions of the consume-transform-produce copiers.
    pub copier_commits: u64,
    pub copier_aborts: u64,
    pub copier_failed: u64,

    pub live_consumed: u64,
    pub redelivered: u64,
    pub clean_restarts: u64,
    pub hard_stops: u64,
    pub commits: u64,
    pub commit_errors: u64,
    pub readbacks_checked: u64,
    pub consumer_errors: u64,

    pub final_consumed: u64,

    pub corrupt: u64,
    pub aborted_visible: u64,
    pub placement_mismatch: u64,
    pub order_violations: u64,
    pub double_writes: u64,
    pub read_below_committed: u64,
    pub readback_violations: u64,
    pub lost_live: u64,
    pub lost_final: u64,
    pub duplicated_final: u64,
    pub unexpected_visible: u64,
    pub unresolved: u64,
    /// Records whose delivery failed in a transaction that then committed: a
    /// commit must fail when any of its sends did.
    pub failed_in_committed_txn: u64,
    /// Transactions of unknown outcome of which some records are visible and
    /// others not.
    pub partial_txns: u64,
}

impl Counts {
    /// Every failure counter with its label; each must be zero for a pass.
    pub fn failures(&self) -> [(&'static str, u64); 14] {
        [
            ("corrupt records", self.corrupt),
            ("aborted records visible", self.aborted_visible),
            ("placement mismatches", self.placement_mismatch),
            ("order violations", self.order_violations),
            ("double writes", self.double_writes),
            ("re-reads below the committed offset", self.read_below_committed),
            ("committed offset read-back violations", self.readback_violations),
            ("lost (live readers)", self.lost_live),
            ("lost (final read-back)", self.lost_final),
            ("duplicated (final read-back)", self.duplicated_final),
            ("rejected or aborted records visible", self.unexpected_visible),
            ("unresolved sends", self.unresolved),
            ("failed records in committed transactions", self.failed_in_committed_txn),
            ("partially visible transactions", self.partial_txns),
        ]
    }

    fn bump(&mut self, event: Event) {
        match event {
            Event::ConsumerError => self.consumer_errors += 1,
            Event::CommitError => self.commit_errors += 1,
            Event::ProducerError => self.producer_errors += 1,
            Event::CleanRestart => self.clean_restarts += 1,
            Event::HardStop => self.hard_stops += 1,
            Event::ProducerRestart => self.producer_restarts += 1,
        }
    }
}

/// What a run must have exercised to count as a pass: an idle run proves
/// nothing.
#[derive(Clone, Debug, Default)]
pub struct Requirements {
    pub min_records_per_stream: u64,
    pub min_txns_committed: u64,
    pub min_txns_aborted: u64,
    pub min_clean_restarts: u64,
    pub min_hard_stops: u64,
    pub min_producer_restarts: u64,
    pub min_copier_aborts: u64,
    pub every_partition_read: bool,
    /// At most this percentage of the sends may fail or be ambiguous, and of
    /// the transactions (the writers' and the copiers') fail or end unknown: a
    /// client that drops most of its records must not pass on the rest.
    pub max_failed_percent: Option<f64>,
}

struct Stream {
    name: String,
    partitions: i32,
    kind: StreamKind,
    /// One state byte per sequence number, per writer.
    writers: BTreeMap<u32, Vec<u8>>,
}

impl Stream {
    fn flags(&self, id: RecordId) -> u8 {
        self.writers
            .get(&id.writer)
            .and_then(|v| v.get(id.seq as usize))
            .copied()
            .unwrap_or(0)
    }

    fn flags_mut(&mut self, id: RecordId) -> &mut u8 {
        let records = self.writers.entry(id.writer).or_default();
        let at = id.seq as usize;
        if records.len() <= at {
            records.resize(at + 1, 0);
        }
        &mut records[at]
    }

    fn set_expectation(&mut self, id: RecordId, expectation: Expectation) {
        let flags = self.flags_mut(id);
        *flags = (*flags & !EXPECT_MASK) | expectation as u8;
    }
}

/// The live group's view of each partition.
#[derive(Default)]
struct LiveState {
    /// Highest offset read per `(stream, partition)`.
    max_offset: HashMap<(usize, i32), i64>,
    /// Last sequence number read per `(stream, partition, writer)`.
    last_seq: HashMap<(usize, i32, u32), u64>,
    /// Last successful commit per `(stream, partition)`.
    committed: HashMap<(usize, i32), i64>,
}

#[derive(Default)]
struct State {
    generators: HashMap<u32, RecordGenerator>,
    topic_index: HashMap<String, usize>,
    streams: Vec<Stream>,
    next_seq: HashMap<(usize, u32), u64>,
    acked_at: HashMap<(usize, RecordId), (i32, i64)>,
    read_before_ack: HashMap<(usize, RecordId), (i32, i64)>,
    live: LiveState,
    final_last_seq: HashMap<(usize, i32, u32), u64>,
    final_read: HashMap<usize, std::collections::BTreeSet<i32>>,
    /// Where the final read-back found an ambiguous record the live group did
    /// not read.
    final_at: HashMap<(usize, RecordId), (i32, i64)>,
    /// The sequence numbers of each transaction of unknown outcome.
    unknown_txns: Vec<(usize, u32, Vec<u64>)>,
    counts: Counts,
    samples: BTreeMap<&'static str, Vec<String>>,
    progress: u64,
}

impl State {
    fn sample(&mut self, category: &'static str, detail: String) {
        let samples = self.samples.entry(category).or_default();
        if samples.len() < SAMPLES_PER_CATEGORY {
            samples.push(detail);
        }
    }

    fn stream(&self, topic: &str) -> usize {
        *self
            .topic_index
            .get(topic)
            .unwrap_or_else(|| panic!("topic {topic} is not registered with the ledger"))
    }

    /// The stream whose writers decide what stream `t` must hold.
    fn source_of(&self, t: usize) -> usize {
        match &self.streams[t].kind {
            StreamKind::Source => t,
            StreamKind::Mirror(source) => self.stream(source),
        }
    }

    /// The delivery report of `id` arrived: compare it with where the record
    /// was read, or keep it until it is.
    fn record_ack(&mut self, t: usize, id: RecordId, metadata: &RecordMetadata) {
        self.counts.acked += 1;
        let flags = self.streams[t].flags_mut(id);
        let seen = *flags & SEEN_LIVE != 0;
        *flags |= ACKED;
        // The broker's duplicate-sequence reply carries no offset.
        if !metadata.has_offset() {
            return;
        }
        let acked = (metadata.partition(), metadata.offset());
        if let Some(read) = self.read_before_ack.remove(&(t, id)) {
            self.check_placement(t, id, acked, read);
        } else if !seen {
            self.acked_at.insert((t, id), acked);
        }
    }

    fn check_placement(&mut self, t: usize, id: RecordId, acked: (i32, i64), read: (i32, i64)) {
        if acked != read {
            self.counts.placement_mismatch += 1;
            let topic = self.streams[t].name.clone();
            self.sample(
                "placement mismatches",
                format!(
                    "{id:?} acknowledged at {topic}-{}@{} but read at {topic}-{}@{}",
                    acked.0, acked.1, read.0, read.1
                ),
            );
        }
    }

    fn resolve(&mut self, t: usize, id: RecordId, result: &Result<RecordMetadata, Error>, ok: Expectation) {
        match result {
            Ok(metadata) => {
                self.record_ack(t, id, metadata);
                self.streams[t].set_expectation(id, ok);
                if ok == Expectation::Never {
                    self.acked_at.remove(&(t, id));
                }
            },
            // A record of an aborted transaction fails when the abort
            // overtakes it; it was never to be read, so that is no rejection.
            Err(_) if ok == Expectation::Never => self.streams[t].set_expectation(id, Expectation::Never),
            Err(error) => {
                let expectation = failed_delivery(error);
                match expectation {
                    Expectation::AtMostOnce => self.counts.send_ambiguous += 1,
                    _ => self.counts.send_rejected += 1,
                }
                self.streams[t].set_expectation(id, expectation);
                let detail = format!("{} writer {} seq {}: {error}", self.streams[t].name, id.writer, id.seq);
                self.sample("failed deliveries (reported)", detail);
            },
        }
    }
}

/// The shared record of a run. Every method takes the lock briefly and never
/// across an `.await`.
pub struct Ledger {
    run_id: u64,
    seed: u64,
    state: Mutex<State>,
}

impl Ledger {
    pub fn new(run_id: u64, seed: u64) -> Self {
        Self { run_id, seed, state: Mutex::new(State::default()) }
    }

    pub fn run_id(&self) -> u64 {
        self.run_id
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("ledger lock")
    }

    /// Registers `writer` and returns the generator its records come from.
    pub fn register_writer(&self, writer: u32, profile: PayloadProfile) -> RecordGenerator {
        let generator = RecordGenerator::new(self.seed, profile);
        self.lock().generators.insert(writer, generator.clone());
        generator
    }

    pub fn register_topic(&self, topic: &str, partitions: i32, kind: StreamKind) {
        let mut st = self.lock();
        let index = st.streams.len();
        st.streams
            .push(Stream { name: topic.to_string(), partitions, kind, writers: BTreeMap::new() });
        st.topic_index.insert(topic.to_string(), index);
    }

    /// The next sequence number of `writer` on `topic`; the record is pending
    /// until its outcome is reported.
    pub fn next_seq(&self, topic: &str, writer: u32) -> u64 {
        let mut st = self.lock();
        let t = st.stream(topic);
        let next = st.next_seq.entry((t, writer)).or_insert(0);
        let seq = *next;
        *next += 1;
        st.streams[t].set_expectation(RecordId { writer, seq }, Expectation::Pending);
        st.counts.sent += 1;
        st.progress += 1;
        seq
    }

    /// `send()` itself failed: the record never reached the producer.
    pub fn send_rejected(&self, topic: &str, writer: u32, seq: u64, error: &Error) {
        let mut st = self.lock();
        let t = st.stream(topic);
        st.counts.send_rejected += 1;
        st.streams[t].set_expectation(RecordId { writer, seq }, Expectation::Never);
        st.sample(
            "rejected sends (reported)",
            format!("{topic} writer {writer} seq {seq}: {error}"),
        );
    }

    /// The delivery result of a record sent outside a transaction.
    pub fn delivered(&self, topic: &str, writer: u32, seq: u64, result: &Result<RecordMetadata, Error>) {
        let mut st = self.lock();
        let t = st.stream(topic);
        st.resolve(t, RecordId { writer, seq }, result, Expectation::Once);
    }

    /// A transaction of `writer` ended: `results` are the delivery results of
    /// every record sent in it.
    pub fn resolve_txn(
        &self,
        topic: &str,
        writer: u32,
        results: &[(u64, Result<RecordMetadata, Error>)],
        outcome: TxnOutcome,
    ) {
        let mut st = self.lock();
        let t = st.stream(topic);
        let ok = match outcome {
            TxnOutcome::Committed => {
                st.counts.txns_committed += 1;
                Expectation::Once
            },
            TxnOutcome::Aborted => {
                st.counts.txns_aborted += 1;
                Expectation::Never
            },
            TxnOutcome::Failed => {
                st.counts.txns_failed += 1;
                Expectation::Never
            },
            TxnOutcome::Unknown => {
                st.counts.txns_unknown += 1;
                st.unknown_txns.push((t, writer, results.iter().map(|(seq, _)| *seq).collect()));
                Expectation::AtMostOnce
            },
        };
        for (seq, result) in results {
            if outcome == TxnOutcome::Committed
                && let Err(error) = result
            {
                st.counts.failed_in_committed_txn += 1;
                st.sample(
                    "failed records in committed transactions",
                    format!("{topic} writer {writer} seq {seq}: {error}"),
                );
            }
            st.resolve(t, RecordId { writer, seq: *seq }, result, ok);
        }
    }

    /// The records of one poll of the live group: within it, each partition's
    /// offsets only increase.
    pub fn live_batch(&self, records: &[ConsumerRecord<Vec<u8>, Vec<u8>>]) {
        let mut last: HashMap<(&str, i32), i64> = HashMap::new();
        for record in records {
            let (topic, p, offset) = (record.topic(), record.partition(), record.offset());
            if let Some(previous) = last.insert((topic, p), offset)
                && offset <= previous
            {
                let mut st = self.lock();
                st.counts.order_violations += 1;
                st.sample(
                    "order violations",
                    format!("live {topic}-{p}: offset {offset} after {previous} in one poll"),
                );
            }
            self.live_record(record);
        }
    }

    /// A record the live group read.
    pub fn live_record(&self, record: &ConsumerRecord<Vec<u8>, Vec<u8>>) {
        let mut guard = self.lock();
        let st = &mut *guard;
        st.progress += 1;
        st.counts.live_consumed += 1;
        let t = st.stream(record.topic());
        let (p, offset) = (record.partition(), record.offset());
        let identity = match verify_record(record, self.run_id, &st.generators) {
            Ok(identity) => identity,
            Err(failure) => {
                st.counts.corrupt += 1;
                st.sample(
                    "corrupt records",
                    format!("live {}-{p}@{offset}: {:?}: {}", record.topic(), failure.defect, failure.detail),
                );
                return;
            },
        };
        let id = identity.id;
        if identity.intent == TxnIntent::Abort {
            st.counts.aborted_visible += 1;
            st.sample(
                "aborted records visible",
                format!("live {}-{p}@{offset}: {id:?}", record.topic()),
            );
        }
        let flags = st.streams[t].flags(id);
        let max = st.live.max_offset.get(&(t, p)).copied().unwrap_or(-1);
        if offset > max {
            st.live.max_offset.insert((t, p), offset);
            let last = st.live.last_seq.get(&(t, p, id.writer)).copied();
            if flags & SEEN_LIVE != 0 {
                st.counts.double_writes += 1;
                st.sample(
                    "double writes",
                    format!("{id:?} read again at a new offset {}-{p}@{offset}", record.topic()),
                );
            } else if let Some(last) = last
                && id.seq <= last
            {
                st.counts.order_violations += 1;
                st.sample(
                    "order violations",
                    format!(
                        "{}-{p}@{offset}: writer {} seq {} after seq {last}",
                        record.topic(),
                        id.writer,
                        id.seq
                    ),
                );
            }
            st.live
                .last_seq
                .insert((t, p, id.writer), last.map_or(id.seq, |last| last.max(id.seq)));
        } else {
            st.counts.redelivered += 1;
            if let Some(&committed) = st.live.committed.get(&(t, p))
                && offset < committed
            {
                st.counts.read_below_committed += 1;
                st.sample(
                    "re-reads below the committed offset",
                    format!("{}-{p}@{offset} re-read, the group had committed {committed}", record.topic()),
                );
            }
        }
        if matches!(st.streams[t].kind, StreamKind::Source) {
            if flags & ACKED != 0 {
                if let Some(acked) = st.acked_at.remove(&(t, id)) {
                    st.check_placement(t, id, acked, (p, offset));
                }
            } else {
                st.read_before_ack.insert((t, id), (p, offset));
            }
        }
        *st.streams[t].flags_mut(id) |= SEEN_LIVE;
    }

    /// The live group committed `offsets`.
    pub fn live_committed(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>) {
        let mut st = self.lock();
        st.counts.commits += 1;
        for (tp, offset) in offsets {
            let t = st.stream(tp.topic());
            st.live.committed.insert((t, tp.partition()), offset.offset());
        }
    }

    /// The committed offsets read back right after a successful commit of
    /// `ours`, with no poll in between. Another member may have committed
    /// further since (a rebalance), never less, and never past what the group
    /// read.
    pub fn readback(
        &self,
        ours: &HashMap<TopicPartition, OffsetAndMetadata>,
        theirs: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) {
        let mut st = self.lock();
        st.counts.readbacks_checked += 1;
        for (tp, offset) in ours {
            let t = st.stream(tp.topic());
            let read_up_to = st.live.max_offset.get(&(t, tp.partition())).map_or(0, |max| max + 1);
            let read = theirs.get(tp).map(OffsetAndMetadata::offset);
            if read.is_none_or(|read| read < offset.offset() || read > read_up_to) {
                st.counts.readback_violations += 1;
                st.sample(
                    "committed offset read-back violations",
                    format!(
                        "{tp}: committed {}, read back {read:?}, read up to {read_up_to}",
                        offset.offset()
                    ),
                );
            }
        }
    }

    /// A copier's committed offsets, read back: the copier is its group's only
    /// member, so they must be exactly what it last committed (`expected`; a
    /// partition it never committed holds its seeded 0).
    pub fn copier_readback(
        &self,
        expected: &HashMap<TopicPartition, i64>,
        read: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) {
        let mut st = self.lock();
        st.counts.readbacks_checked += 1;
        for (tp, offset) in read {
            let expected = expected.get(tp).copied().unwrap_or(0);
            if offset.offset() != expected {
                st.counts.readback_violations += 1;
                st.sample(
                    "committed offset read-back violations",
                    format!("copier {tp}: committed {expected}, read back {}", offset.offset()),
                );
            }
        }
    }

    /// A record the final read-back read (from the start, read_committed).
    pub fn final_record(&self, record: &ConsumerRecord<Vec<u8>, Vec<u8>>) {
        let mut guard = self.lock();
        let st = &mut *guard;
        st.progress += 1;
        st.counts.final_consumed += 1;
        let t = st.stream(record.topic());
        let (p, offset) = (record.partition(), record.offset());
        st.final_read.entry(t).or_default().insert(p);
        let identity = match verify_record(record, self.run_id, &st.generators) {
            Ok(identity) => identity,
            Err(failure) => {
                st.counts.corrupt += 1;
                st.sample(
                    "corrupt records",
                    format!(
                        "final {}-{p}@{offset}: {:?}: {}",
                        record.topic(),
                        failure.defect,
                        failure.detail
                    ),
                );
                return;
            },
        };
        let id = identity.id;
        if identity.intent == TxnIntent::Abort {
            st.counts.aborted_visible += 1;
            st.sample(
                "aborted records visible",
                format!("final {}-{p}@{offset}: {id:?}", record.topic()),
            );
        }
        let flags = st.streams[t].flags(id);
        // A record read before is a duplicate, counted as one in the verdict.
        if final_count(flags) == 0
            && let Some(&last) = st.final_last_seq.get(&(t, p, id.writer))
            && id.seq <= last
        {
            st.counts.order_violations += 1;
            st.sample(
                "order violations",
                format!(
                    "final {}-{p}@{offset}: writer {} seq {} after seq {last}",
                    record.topic(),
                    id.writer,
                    id.seq
                ),
            );
        }
        let last = st.final_last_seq.entry((t, p, id.writer)).or_insert(id.seq);
        *last = (*last).max(id.seq);
        if matches!(st.streams[t].kind, StreamKind::Source)
            && let Some(acked) = st.acked_at.remove(&(t, id))
        {
            st.check_placement(t, id, acked, (p, offset));
        }
        let source = st.source_of(t);
        if flags & SEEN_LIVE == 0 && expectation(st.streams[source].flags(id)) == Expectation::AtMostOnce {
            st.final_at.insert((t, id), (p, offset));
        }
        let flags = st.streams[t].flags_mut(id);
        let count = final_count(*flags);
        if count < 2 {
            *flags = (*flags & !FINAL_MASK) | ((count + 1) << FINAL_SHIFT);
        }
    }

    /// Counts a reported event; `detail` is kept as an example.
    pub fn event(&self, event: Event, detail: impl fmt::Display) {
        let mut st = self.lock();
        st.counts.bump(event);
        if matches!(event, Event::ConsumerError | Event::CommitError | Event::ProducerError) {
            st.sample(event.category(), detail.to_string());
        }
    }

    /// The copy into `topic` of the record at `source` failed in a copier
    /// transaction that then committed.
    pub fn copier_failed_in_committed_txn(&self, topic: &str, source: &str, error: &Error) {
        let mut st = self.lock();
        st.counts.failed_in_committed_txn += 1;
        st.sample(
            "failed records in committed transactions",
            format!("copier to {topic}, copy of {source}: {error}"),
        );
    }

    /// A copier transaction ended.
    pub fn copier_txn(&self, outcome: TxnOutcome) {
        let mut st = self.lock();
        match outcome {
            TxnOutcome::Committed => st.counts.copier_commits += 1,
            TxnOutcome::Aborted => st.counts.copier_aborts += 1,
            TxnOutcome::Failed | TxnOutcome::Unknown => st.counts.copier_failed += 1,
        }
    }

    /// A counter that moves whenever anything is sent or read; the watchdog
    /// fails a run when it stops moving.
    pub fn progress(&self) -> u64 {
        self.lock().progress
    }

    pub fn counts(&self) -> Counts {
        self.lock().counts.clone()
    }

    /// Records the live group must still read: every record due once that it
    /// has not seen, on source and mirror topics alike.
    pub fn live_outstanding(&self) -> u64 {
        let st = self.lock();
        let mut outstanding = 0;
        for (t, stream) in st.streams.iter().enumerate() {
            let source = &st.streams[st.source_of(t)];
            for (writer, records) in &source.writers {
                // Tens of millions of records under the lock: one lookup per
                // writer, not per record.
                let seen = stream.writers.get(writer).map_or(&[][..], Vec::as_slice);
                for (seq, flags) in records.iter().enumerate() {
                    if expectation(*flags) == Expectation::Once && seen.get(seq).is_none_or(|f| f & SEEN_LIVE == 0) {
                        outstanding += 1;
                    }
                }
            }
        }
        outstanding
    }

    /// Scores the run. `live` says whether the live group ran;
    /// `final_complete` whether the final read-back reached the end of every
    /// partition.
    pub fn verdict(&self, live: bool, final_complete: bool, requirements: &Requirements) -> Verdict {
        let mut guard = self.lock();
        let st = &mut *guard;
        let mut counts = st.counts.clone();
        let mut lost: Vec<(&'static str, String)> = Vec::new();
        let mut unmet = Vec::new();
        let mut unreached = 0u64;
        let mut mirrors: HashMap<usize, Vec<usize>> = HashMap::new();
        for t in 0..st.streams.len() {
            let source = st.source_of(t);
            if source != t {
                mirrors.entry(source).or_default().push(t);
            }
        }
        // Whether a reader found a record of source `s`, there or in a mirror.
        let found = |s: usize, id: RecordId| {
            std::iter::once(s)
                .chain(mirrors.get(&s).into_iter().flatten().copied())
                .any(|t| {
                    let flags = st.streams[t].flags(id);
                    flags & SEEN_LIVE != 0 || final_count(flags) > 0
                })
        };
        for (t, stream) in st.streams.iter().enumerate() {
            let s = st.source_of(t);
            let source = &st.streams[s];
            let mut due_once = 0u64;
            for (writer, records) in &source.writers {
                // Consecutive lost sequence numbers are reported as one range.
                let mut live_run: Option<(u64, u64)> = None;
                let mut final_run: Option<(u64, u64)> = None;
                for (seq, source_flags) in records.iter().enumerate() {
                    let seq = seq as u64;
                    let id = RecordId { writer: *writer, seq };
                    let flags = stream.flags(id);
                    let seen_live = flags & SEEN_LIVE != 0;
                    let read = final_count(flags);
                    let mut live_lost = false;
                    let mut final_lost = false;
                    match expectation(*source_flags) {
                        // An ambiguous send that a reader found was written: it
                        // is due once in its source and in every mirror.
                        Expectation::AtMostOnce if final_complete && found(s, id) => {
                            // Only a group that committed past it skipped it; one
                            // that stopped before reaching it did not.
                            live_lost = live
                                && !seen_live
                                && st.final_at.get(&(t, id)).is_some_and(|&(p, offset)| {
                                    st.live.committed.get(&(t, p)).is_some_and(|&c| c > offset)
                                });
                            final_lost = read == 0;
                            if read > 1 {
                                counts.duplicated_final += 1;
                            }
                        },
                        Expectation::Once => {
                            due_once += 1;
                            live_lost = live && !seen_live;
                            // Unread records of an unfinished read-back are not
                            // lost: the unmet requirement below reports them.
                            final_lost = read == 0 && final_complete;
                            unreached += u64::from(read == 0 && !final_complete);
                            if read > 1 {
                                counts.duplicated_final += 1;
                            }
                        },
                        Expectation::Pending | Expectation::AtMostOnce => {
                            if read > 1 {
                                counts.duplicated_final += 1;
                            }
                            if expectation(*source_flags) == Expectation::Pending
                                && matches!(stream.kind, StreamKind::Source)
                            {
                                counts.unresolved += 1;
                            }
                        },
                        Expectation::Never => {
                            if seen_live || read > 0 {
                                counts.unexpected_visible += 1;
                            }
                        },
                    }
                    counts.lost_live += u64::from(live_lost);
                    counts.lost_final += u64::from(final_lost);
                    extend_run(&mut live_run, live_lost, seq, |from, to| {
                        lost.push(("lost (live readers)", describe_lost(&stream.name, *writer, from, to)));
                    });
                    extend_run(&mut final_run, final_lost, seq, |from, to| {
                        lost.push(("lost (final read-back)", describe_lost(&stream.name, *writer, from, to)));
                    });
                }
                if let Some((from, to)) = live_run {
                    lost.push(("lost (live readers)", describe_lost(&stream.name, *writer, from, to)));
                }
                if let Some((from, to)) = final_run {
                    lost.push(("lost (final read-back)", describe_lost(&stream.name, *writer, from, to)));
                }
            }
            // A mirror holds only copies of its source's records.
            if s != t {
                for (writer, records) in &stream.writers {
                    let known = source.writers.get(writer).map_or(0, Vec::len);
                    for (seq, flags) in records.iter().enumerate().skip(known) {
                        if flags & SEEN_LIVE != 0 || final_count(*flags) > 0 {
                            counts.unexpected_visible += 1;
                            lost.push((
                                "rejected or aborted records visible",
                                format!("{}: writer {writer} seq {seq} was never written to its source", stream.name),
                            ));
                        }
                    }
                }
            }
            if due_once < requirements.min_records_per_stream {
                unmet.push(format!(
                    "{}: {due_once} record(s) due once, expected at least {}",
                    stream.name, requirements.min_records_per_stream
                ));
            }
            if requirements.every_partition_read {
                let read: usize = if live {
                    st.live.max_offset.keys().filter(|(stream, _)| *stream == t).count()
                } else {
                    st.final_read.get(&t).map_or(0, |p| p.len())
                };
                if read < stream.partitions as usize {
                    unmet.push(format!("{}: {read} of {} partition(s) read", stream.name, stream.partitions));
                }
            }
        }
        // A transaction commits or aborts as a whole, even when the writer
        // could not tell which.
        if final_complete {
            for (s, writer, seqs) in &st.unknown_txns {
                let visible = seqs.iter().filter(|&&seq| found(*s, RecordId { writer: *writer, seq })).count();
                if visible > 0 && visible < seqs.len() {
                    counts.partial_txns += 1;
                    lost.push((
                        "partially visible transactions",
                        format!(
                            "{} writer {writer}: {visible} of the {} records of seq {}..={} visible",
                            st.streams[*s].name,
                            seqs.len(),
                            seqs.first().copied().unwrap_or(0),
                            seqs.last().copied().unwrap_or(0)
                        ),
                    ));
                }
            }
        }
        for (category, detail) in lost {
            st.sample(category, detail);
        }
        let minimums = [
            ("committed transactions", counts.txns_committed, requirements.min_txns_committed),
            ("aborted transactions", counts.txns_aborted, requirements.min_txns_aborted),
            (
                "clean consumer restarts",
                counts.clean_restarts,
                requirements.min_clean_restarts,
            ),
            ("hard consumer stops", counts.hard_stops, requirements.min_hard_stops),
            (
                "producer restarts",
                counts.producer_restarts,
                requirements.min_producer_restarts,
            ),
            ("copier aborts", counts.copier_aborts, requirements.min_copier_aborts),
        ];
        for (what, got, min) in minimums {
            if got < min {
                unmet.push(format!("{got} {what}, expected at least {min}"));
            }
        }
        if let Some(max) = requirements.max_failed_percent {
            let c = &counts;
            let txns = c.txns_committed
                + c.txns_aborted
                + c.txns_failed
                + c.txns_unknown
                + c.copier_commits
                + c.copier_aborts
                + c.copier_failed;
            let shares = [
                ("sends failed or ambiguous", c.send_rejected + c.send_ambiguous, c.sent),
                (
                    "transactions failed or unknown",
                    c.txns_failed + c.txns_unknown + c.copier_failed,
                    txns,
                ),
            ];
            for (what, part, whole) in shares {
                let percent = if whole == 0 {
                    0.0
                } else {
                    part as f64 * 100.0 / whole as f64
                };
                if percent > max {
                    unmet.push(format!("{part} of {whole} {what} ({percent:.2}%), expected at most {max}%"));
                }
            }
        }
        if !final_complete {
            unmet.push(format!(
                "the final read-back did not reach the end of every partition ({unreached} record(s) due once not read)"
            ));
        }
        Verdict { counts, samples: st.samples.clone(), unmet, aborted: None }
    }
}

/// Extends the run of consecutive lost sequence numbers with `seq`, or closes
/// it (calling `close`) when `seq` is not lost.
fn extend_run(run: &mut Option<(u64, u64)>, lost: bool, seq: u64, close: impl FnOnce(u64, u64)) {
    if lost {
        match run {
            Some((_, to)) => *to = seq,
            None => *run = Some((seq, seq)),
        }
    } else if let Some((from, to)) = run.take() {
        close(from, to);
    }
}

fn describe_lost(topic: &str, writer: u32, from: u64, to: u64) -> String {
    format!("{topic} writer {writer}: seq {from}..={to} ({} record(s))", to - from + 1)
}

/// The outcome of a run.
#[derive(Clone, Debug)]
pub struct Verdict {
    pub counts: Counts,
    pub samples: BTreeMap<&'static str, Vec<String>>,
    /// Requirements the run did not meet (see [`Requirements`]).
    pub unmet: Vec<String>,
    /// Why the run was stopped early (the watchdog), if it was.
    pub aborted: Option<String>,
}

impl Verdict {
    pub fn passed(&self) -> bool {
        self.aborted.is_none() && self.unmet.is_empty() && self.counts.failures().iter().all(|(_, n)| *n == 0)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let c = &self.counts;
        let failures: serde_json::Map<String, serde_json::Value> = c
            .failures()
            .iter()
            .map(|(name, n)| (name.to_string(), serde_json::json!(n)))
            .collect();
        serde_json::json!({
            "verdict": if self.passed() { "PASS" } else { "FAIL" },
            "aborted": self.aborted,
            "unmet_requirements": self.unmet,
            "failures": failures,
            "writers": {
                "sent": c.sent, "acked": c.acked, "rejected": c.send_rejected, "ambiguous": c.send_ambiguous,
                "txns_committed": c.txns_committed, "txns_aborted": c.txns_aborted, "txns_failed": c.txns_failed,
                "txns_unknown": c.txns_unknown,
                "producer_restarts": c.producer_restarts, "producer_errors": c.producer_errors,
                "copier_commits": c.copier_commits, "copier_aborts": c.copier_aborts, "copier_failed": c.copier_failed,
            },
            "live_readers": {
                "consumed": c.live_consumed, "redelivered": c.redelivered, "commits": c.commits,
                "commit_errors": c.commit_errors, "readbacks_checked": c.readbacks_checked,
                "clean_restarts": c.clean_restarts, "hard_stops": c.hard_stops, "consumer_errors": c.consumer_errors,
            },
            "final_read_back": { "consumed": c.final_consumed },
            "samples": self.samples,
        })
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = &self.counts;
        writeln!(f, "=== Integrity verdict: {} ===", if self.passed() { "PASS" } else { "FAIL" })?;
        if let Some(reason) = &self.aborted {
            writeln!(f, "  run stopped early        : {reason}")?;
        }
        writeln!(f, "  sent / acknowledged      : {} / {}", c.sent, c.acked)?;
        writeln!(f, "  rejected / ambiguous     : {} / {}", c.send_rejected, c.send_ambiguous)?;
        writeln!(
            f,
            "  transactions             : {} committed, {} aborted, {} failed, {} unknown",
            c.txns_committed, c.txns_aborted, c.txns_failed, c.txns_unknown
        )?;
        writeln!(
            f,
            "  producer restarts        : {} ({} producer errors)",
            c.producer_restarts, c.producer_errors
        )?;
        writeln!(
            f,
            "  copier transactions      : {} committed, {} aborted, {} failed",
            c.copier_commits, c.copier_aborts, c.copier_failed
        )?;
        writeln!(
            f,
            "  live reads               : {} ({} redelivered)",
            c.live_consumed, c.redelivered
        )?;
        writeln!(
            f,
            "  commits                  : {} ({} errors, {} read back)",
            c.commits, c.commit_errors, c.readbacks_checked
        )?;
        writeln!(
            f,
            "  consumer restarts        : {} clean, {} hard",
            c.clean_restarts, c.hard_stops
        )?;
        writeln!(f, "  consumer errors          : {}", c.consumer_errors)?;
        writeln!(f, "  final read-back          : {}", c.final_consumed)?;
        for (name, n) in c.failures() {
            writeln!(f, "  {name:<38}: {n}{}", if n > 0 { "   <-- FAIL" } else { "" })?;
        }
        for unmet in &self.unmet {
            writeln!(f, "  requirement not met      : {unmet}")?;
        }
        for (category, samples) in &self.samples {
            writeln!(f, "  {category}:")?;
            for sample in samples {
                writeln!(f, "    {sample}")?;
            }
        }
        Ok(())
    }
}
