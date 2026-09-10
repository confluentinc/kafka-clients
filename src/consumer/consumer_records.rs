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

//! A container that holds per-partition lists of [`ConsumerRecord`].
//!
//! Translated from `org.apache.kafka.clients.consumer.ConsumerRecords`.

use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use crate::common::TopicPartition;
use crate::consumer::ConsumerRecord;
use crate::consumer::OffsetAndMetadata;

/// A container that holds the list of [`ConsumerRecord`] per partition for a
/// particular topic.
///
/// There is one [`ConsumerRecord`] list for every topic-partition returned by
/// a `Consumer::poll(...)` operation.
///
/// Corresponds to Java's
/// `org.apache.kafka.clients.consumer.ConsumerRecords<K, V>`.
///
/// Uses an [`IndexMap`] internally so iteration order matches insertion order
/// (Java's `ConsumerRecords` is documented as iterating per the underlying
/// map's iteration order; the test suite assumes ordering matches insertion
/// when a `LinkedHashMap` is supplied).
///
/// # Equality
///
/// `PartialEq` / `Eq` are derived (gated on `K: PartialEq, V: PartialEq` /
/// `K: Eq, V: Eq`) so that tests can `assert_eq!(actual, expected)`
/// against an entire batch. Mirrors Java's `ConsumerRecordsTest` which
/// uses `assertEquals` on whole-batch values. The bounds are gated; users
/// with non-`PartialEq` keys/values are unaffected.
#[derive(Debug, PartialEq, Eq)]
pub struct ConsumerRecords<K, V> {
    records: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    /// Whether the consumed position advanced for at least one partition
    /// during the `collect_fetch` that produced this batch, even if no
    /// records were returned (e.g. all records in a batch were aborted under
    /// READ_COMMITTED).
    ///
    /// This field carries Java's internal `Fetch.positionAdvanced` flag —
    /// the Rust port collapses Java's internal `Fetch<K, V>` into
    /// `ConsumerRecords<K, V>` (no separate `Fetch` type). The public
    /// [`Self::is_empty`] still mirrors Java's *public*
    /// `ConsumerRecords.isEmpty()` (`records.isEmpty()`), but the poll loop
    /// needs Java's internal `Fetch.isEmpty()` (`numRecords == 0 &&
    /// !positionAdvanced`) so it returns promptly when only the position
    /// advanced — see [`Self::is_fetch_empty`]. It does not affect equality
    /// in a user-observable way (it is an internal bookkeeping flag, but is
    /// included in the derived `PartialEq`/`Eq` for completeness).
    position_advanced: bool,
    /// Flag to detect if the legacy [`Self::new`] constructor is used. See
    /// KAFKA-20660 for more details.
    ///
    /// Translates Java's `ConsumerRecords.tainted` (`ConsumerRecords.java:47`).
    /// Like `position_advanced` it is internal bookkeeping, and is included in
    /// the derived `PartialEq`/`Eq` for completeness.
    tainted: bool,
}

/// Java's `ConsumerRecords.TAINT_LOG_INTERVAL_NS` (`ConsumerRecords.java:50`).
const TAINT_LOG_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// The process start instant, so the tainted-log timestamps below can be a
/// plain `AtomicI64` of nanoseconds since that point. Java uses
/// `System.nanoTime()`, whose origin is likewise arbitrary but fixed.
static PROCESS_START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

/// Java's `ConsumerRecords.TAINTED_NEXT_OFFSETS_LAST_LOG_NS`
/// (`ConsumerRecords.java:52`), initialised one interval in the past so the
/// first tainted call logs immediately.
static TAINTED_NEXT_OFFSETS_LAST_LOG_NS: AtomicI64 = AtomicI64::new(-(TAINT_LOG_INTERVAL.as_nanos() as i64));

impl<K, V> ConsumerRecords<K, V> {
    /// Create a new `ConsumerRecords` from per-partition record lists only.
    ///
    /// Corresponds to Java's deprecated `ConsumerRecords(Map)`
    /// (`ConsumerRecords.java:57-60`), whose body is
    /// `this(records, Map.of(), true)` — the next-offsets map is empty and the
    /// instance is marked *tainted*, so [`Self::next_offsets`] logs a
    /// rate-limited error explaining why it returned empty (KAFKA-20660).
    #[deprecated(
        since = "4.0.0",
        note = "mirroring Java's `@Deprecated`; use `new_next_offsets` instead, which supplies next offsets"
    )]
    pub fn new(records: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>) -> Self {
        Self { records, next_offsets: HashMap::new(), position_advanced: false, tainted: true }
    }

    /// Create a new `ConsumerRecords` from per-partition record lists and a
    /// next-offsets map.
    ///
    /// Corresponds to Java's `ConsumerRecords(Map, Map)`
    /// (`ConsumerRecords.java:62`).
    pub fn new_next_offsets(
        records: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
        next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        Self { records, next_offsets, position_advanced: false, tainted: false }
    }

    /// Like [`Self::new`] but also records whether the consumed position
    /// advanced (Java's internal `Fetch.positionAdvanced`). Used by
    /// `FetchCollector::collect_fetch` so the poll loop can mirror Java's
    /// `Fetch.isEmpty()` semantics.
    pub(crate) fn new_with_position_advanced(
        records: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
        next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        position_advanced: bool,
    ) -> Self {
        Self { records, next_offsets, position_advanced, tainted: false }
    }

    /// Returns an empty `ConsumerRecords`.
    ///
    /// Corresponds to Java's static `ConsumerRecords.empty()` /
    /// `ConsumerRecords.EMPTY`.
    pub fn empty() -> Self {
        Self {
            records: IndexMap::new(),
            next_offsets: HashMap::new(),
            position_advanced: false,
            tainted: false,
        }
    }

    /// Java's internal `Fetch.isEmpty()`: `numRecords == 0 &&
    /// !positionAdvanced`. The poll loop uses this (NOT [`Self::is_empty`])
    /// to decide whether to return early — so an all-aborted batch that
    /// advances the position with zero records returns promptly instead of
    /// blocking until the poll timeout, matching `AsyncKafkaConsumer.poll`.
    pub(crate) fn is_fetch_empty(&self) -> bool {
        Self::fetch_is_empty(&self.records, self.position_advanced)
    }

    /// Java's internal `Fetch.isEmpty()` (`Fetch.java:116-118`) evaluated over
    /// the two accumulators that build a `Fetch`, for callers that do not have
    /// a `ConsumerRecords` yet.
    ///
    /// `FetchCollector::collect_fetch` mirrors Java's
    /// `final Fetch<K, V> fetch = Fetch.empty()` with a plain
    /// records-map + `position_advanced` pair (it only materialises the
    /// `ConsumerRecords` on the way out), yet Java tests `fetch.isEmpty()`
    /// three times *during* the loop to decide whether to swallow an error and
    /// whether to leave the offending entry queued. This associated function
    /// exists so both callers answer the question from one place: the
    /// records-only spelling was wrong at all three collector sites, and a
    /// second inline copy of the predicate is exactly how that drifted.
    ///
    /// Note `records.is_empty()` is the faithful reading of Java's
    /// `numRecords == 0`: the collector inserts a partition entry only when it
    /// decoded at least one record for it, so an empty map and a zero record
    /// count are the same condition.
    pub(crate) fn fetch_is_empty(
        records: &IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
        position_advanced: bool,
    ) -> bool {
        records.is_empty() && !position_advanced
    }

    /// Get the records for the given partition.
    ///
    /// Returns an empty slice if no records are present for that partition.
    ///
    /// Corresponds to Java's `ConsumerRecords.records(TopicPartition)`.
    pub fn records_partition(&self, partition: &TopicPartition) -> &[ConsumerRecord<K, V>] {
        self.records.get(partition).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Get the records for the given topic, across all partitions in this
    /// record set.
    ///
    /// Returns an iterator that yields references to records in
    /// partition-insertion order.
    ///
    /// Corresponds to Java's `ConsumerRecords.records(String)`.
    pub fn records_topic<'a>(&'a self, topic: &'a str) -> impl Iterator<Item = &'a ConsumerRecord<K, V>> + 'a {
        self.records
            .iter()
            .filter(move |(tp, _)| tp.topic() == topic)
            .flat_map(|(_, records)| records.iter())
    }

    /// Get the partitions which have records contained in this record set.
    ///
    /// Returns an iterator over partition references in insertion order. The
    /// set may be empty if no data was returned.
    ///
    /// Corresponds to Java's `ConsumerRecords.partitions()`.
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.records.keys()
    }

    /// The number of records across all topics and partitions in this set.
    pub fn count(&self) -> usize {
        self.records.values().map(Vec::len).sum()
    }

    /// `true` if this record set has no partitions.
    ///
    /// Matches Java's `ConsumerRecords.isEmpty()` exactly (Java returns
    /// `records.isEmpty()`, not `count() == 0`).
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the next offsets and metadata for all topic-partitions for which
    /// the position has been advanced in this poll call.
    ///
    /// Corresponds to Java's `ConsumerRecords.nextOffsets()`.
    pub fn next_offsets(&self) -> &HashMap<TopicPartition, OffsetAndMetadata> {
        if self.tainted {
            let now = PROCESS_START.elapsed().as_nanos() as i64;
            let last_log = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed);
            // `next_offsets()` is called on every poll, so a tainted instance
            // would otherwise log the deprecation error on every call. A time
            // based approach is used to avoid this. See KAFKA-20660.
            if now - last_log >= TAINT_LOG_INTERVAL.as_nanos() as i64
                && TAINTED_NEXT_OFFSETS_LAST_LOG_NS
                    .compare_exchange(last_log, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                log::error!(
                    "ConsumerRecords#next_offsets() returned empty because this instance was built with the \
                     deprecated ConsumerRecords(Map) constructor (see KIP-1094), which does not supply next offsets. \
                     Downstream logic that relies on these offsets to advance the consumer's committed position \
                     (for example, Kafka Streams under exactly-once semantics) will be unable to commit, leading to \
                     reprocessing. Update the interceptor or wrapper that constructed it to use the \
                     ConsumerRecords(Map, Map) constructor that supplies next offsets."
                );
            }
        }
        &self.next_offsets
    }
}

impl<K, V> Default for ConsumerRecords<K, V> {
    fn default() -> Self {
        Self::empty()
    }
}

/// Borrowed iteration over every record across all partitions (in
/// partition-insertion order).
///
/// Equivalent to Java's `ConsumerRecords.iterator()` (which iterates the
/// concatenated values of the underlying map).
impl<'a, K, V> IntoIterator for &'a ConsumerRecords<K, V> {
    type Item = &'a ConsumerRecord<K, V>;
    type IntoIter = std::iter::FlatMap<
        indexmap::map::Values<'a, TopicPartition, Vec<ConsumerRecord<K, V>>>,
        std::slice::Iter<'a, ConsumerRecord<K, V>>,
        fn(&'a Vec<ConsumerRecord<K, V>>) -> std::slice::Iter<'a, ConsumerRecord<K, V>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        // The function pointer type must reference `&Vec<T>` exactly to match
        // the `IntoIter` associated type alias above; using `&[T]` here would
        // produce a slice iterator whose lifetime cannot be unified with the
        // map values iterator.
        #[allow(clippy::ptr_arg)]
        fn iter_vec<T>(v: &Vec<T>) -> std::slice::Iter<'_, T> {
            v.iter()
        }
        self.records.values().flat_map(iter_vec)
    }
}

/// Owned iteration that consumes the [`ConsumerRecords`].
impl<K, V> IntoIterator for ConsumerRecords<K, V> {
    type Item = ConsumerRecord<K, V>;
    type IntoIter = std::iter::FlatMap<
        indexmap::map::IntoValues<TopicPartition, Vec<ConsumerRecord<K, V>>>,
        std::vec::IntoIter<ConsumerRecord<K, V>>,
        fn(Vec<ConsumerRecord<K, V>>) -> std::vec::IntoIter<ConsumerRecord<K, V>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn iter_vec<T>(v: Vec<T>) -> std::vec::IntoIter<T> {
            v.into_iter()
        }
        self.records.into_values().flat_map(iter_vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::TimestampType;
    use crate::consumer::ConsumerRecordOptionsBuilder;
    use std::sync::Mutex;

    /// `TAINTED_NEXT_OFFSETS_LAST_LOG_NS` is process-global (as in Java), so the
    /// tests that drive the rate limiter must not run concurrently. Java relies
    /// on JUnit's default single-threaded execution plus a `finally` that
    /// restores the previous value; Rust's test harness is multi-threaded, so
    /// the lock supplies the serialization the restore alone cannot.
    static RATE_LIMIT_LOCK: Mutex<()> = Mutex::new(());

    fn one_record() -> IndexMap<TopicPartition, Vec<ConsumerRecord<i32, String>>> {
        let tp = TopicPartition::new("topic".to_string(), 0);
        let record = ConsumerRecord::new_options(
            ConsumerRecordOptionsBuilder::new()
                .set_topic("topic")
                .set_partition(0)
                .set_offset(0)
                .set_key(Some(0))
                .set_value(Some("value".to_string()))
                .set_timestamp(0)
                .set_timestamp_type(TimestampType::CreateTime)
                .set_serialized_key_size(0)
                .set_serialized_value_size(0)
                .build(),
        );
        let mut records = IndexMap::new();
        records.insert(tp, vec![record]);
        records
    }

    /// Translated from
    /// `ConsumerRecordsTest.testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor`.
    ///
    /// Java asserts on the captured `ERROR` log lines via `LogCaptureAppender`.
    /// This crate has no log-capture facility (the `log` crate's global logger
    /// is process-wide and installable only once), so the assertions here are
    /// made against the rate limiter's own state instead: the timestamp static
    /// advances exactly when Java would emit a line, and stays put when Java
    /// would suppress one. That is the mechanism the Java test is really
    /// exercising — the log text itself is asserted by the `contains(..)` check
    /// Java makes, which is preserved as a comment on the `log::error!` call.
    #[test]
    #[allow(deprecated)]
    fn test_next_offsets_logs_error_periodically_when_constructed_with_deprecated_constructor() {
        let _guard = RATE_LIMIT_LOCK.lock().unwrap();
        // Capture the global throttle state so it can be restored, to avoid
        // leaking into other tests.
        let previous_last_log_ns = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed);

        // Force the rate-limit window to have elapsed so the next tainted call logs.
        let elapsed_window = PROCESS_START.elapsed().as_nanos() as i64 - TAINT_LOG_INTERVAL.as_nanos() as i64 - 1;
        TAINTED_NEXT_OFFSETS_LAST_LOG_NS.store(elapsed_window, Ordering::Relaxed);

        let consumer_records = ConsumerRecords::new(one_record());

        // The deprecated constructor does not supply next offsets, so the map is empty.
        assert!(consumer_records.next_offsets().is_empty());

        // The window had elapsed, so that call logged and advanced the timestamp.
        let after_first = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed);
        assert!(after_first > elapsed_window);

        // Within the rate-limit window, neither repeated calls nor new tainted
        // instances log again.
        assert!(consumer_records.next_offsets().is_empty());
        // `ConsumerRecord` is not `Clone`, so this rebuilds the same map Java
        // reuses; the value is identical and only taintedness matters here.
        assert!(ConsumerRecords::new(one_record()).next_offsets().is_empty());
        assert_eq!(after_first, TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed));

        // Once the window has elapsed, the error is logged again.
        TAINTED_NEXT_OFFSETS_LAST_LOG_NS.store(elapsed_window, Ordering::Relaxed);
        assert!(consumer_records.next_offsets().is_empty());
        assert!(TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed) > elapsed_window);

        TAINTED_NEXT_OFFSETS_LAST_LOG_NS.store(previous_last_log_ns, Ordering::Relaxed);
    }

    /// Translated from
    /// `ConsumerRecordsTest.testNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets`.
    #[test]
    fn test_next_offsets_does_not_log_error_when_constructed_with_next_offsets() {
        let _guard = RATE_LIMIT_LOCK.lock().unwrap();
        let records = one_record();
        let tp = TopicPartition::new("topic".to_string(), 0);
        let mut next_offsets = HashMap::new();
        next_offsets.insert(tp, OffsetAndMetadata::new(1).unwrap());

        let before = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed);
        let consumer_records = ConsumerRecords::new_next_offsets(records, next_offsets.clone());

        assert!(!consumer_records.next_offsets().is_empty());
        assert_eq!(&next_offsets, consumer_records.next_offsets());
        // Not tainted, so the rate limiter was never consulted.
        assert_eq!(before, TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed));
    }

    /// Translated from
    /// `ConsumerRecordsTest.testNextOffsetsDoesNotLogErrorForEmptyRecords`.
    #[test]
    fn test_next_offsets_does_not_log_error_for_empty_records() {
        let _guard = RATE_LIMIT_LOCK.lock().unwrap();
        let before = TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed);
        let empty: ConsumerRecords<i32, String> = ConsumerRecords::empty();
        assert!(empty.next_offsets().is_empty());
        assert_eq!(before, TAINTED_NEXT_OFFSETS_LAST_LOG_NS.load(Ordering::Relaxed));
    }
}
