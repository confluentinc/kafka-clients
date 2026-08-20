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

//! `FetchCollector` — drains [`FetchBuffer`] and produces user-visible
//! [`ConsumerRecords`].
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.FetchCollector`.
//!
//! # `<K, V>` generic surface
//!
//! Java's `FetchCollector<K, V>` is generic over key/value types because
//! it invokes `Deserializers<K, V>`. Rust mirrors this — `FetchCollector`
//! is generic over `K` and `V` and holds an `Arc<Deserializers<K, V>>` to
//! share the deserializer state with other consumer components
//! (`Fetcher` is out of scope per `consumer-threading.md` §20, but the
//! AsyncKafkaConsumer bg task in Phase 10 holds the same Arc).
//!
//! # §27 zero-copy contract
//!
//! The per-record path is entirely zero-copy through `collect_fetch`. The
//! per-record allocation budget enforced by the §27 regression test in
//! Phase 7b is exactly:
//!
//! - 2 × user-supplied `Deserializer::<T>::deserialize` (key + value).
//! - 1 × `RecordHeaders::from_slice` (owned headers per §27's
//!   milestone-8 ruling).
//!
//! Specifically NOT in the per-record budget:
//!
//! - Topic-name `String` allocation — `ConsumerRecord::topic` is
//!   `Arc<str>` (Phase 7a's `CompletedFetch::topic_arc`); cloning into
//!   each record is an atomic pointer bump.
//! - `Vec<u8>::clone` of fetch-buffer bytes — `CompletedFetch::fetch_records`
//!   reads through `peek_current_record` which returns `&DefaultRecord`.
//! - `DefaultRecord` deep clone — same as above.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use log::{debug, info, trace, warn};

use crate::common::TopicPartition;
use crate::common::protocol::Errors;
use crate::common::{Error, requests::fetch_response::records_size};
use crate::consumer::ConsumerOffsetOutOfRangeError;
use crate::consumer::ConsumerRecord;
use crate::consumer::ConsumerRecords;
use crate::consumer::OffsetAndMetadata;
use crate::consumer::internals::completed_fetch::CompletedFetch;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::deserializers::Deserializers;
use crate::consumer::internals::fetch_buffer::FetchBuffer;
use crate::consumer::internals::fetch_config::FetchConfig;
#[cfg(test)]
use crate::consumer::internals::fetch_metrics_aggregator::FetchMetricsAggregator;
use crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager;
use crate::consumer::internals::fetch_utils::request_metadata_update;
use crate::consumer::internals::subscription_state::{FetchPosition, SubscriptionState};
use crate::fetch_response_data::PartitionData;

/// Time source used by `FetchCollector` for the preferred-read-replica
/// lease window. Mirrors Java's `Time` interface.
///
/// Implementations only need to report `milliseconds()`; we don't use
/// `Time::nanoseconds` or `Time::hiResClockMs` on this path.
pub(crate) trait FetchCollectorTime: Send + Sync + 'static {
    fn milliseconds(&self) -> i64;
}

/// Default time source — wraps `std::time::SystemTime::now()`.
#[derive(Debug, Default)]
pub(crate) struct SystemFetchCollectorTime;

impl FetchCollectorTime for SystemFetchCollectorTime {
    fn milliseconds(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Drains the [`FetchBuffer`] and produces user-visible
/// [`ConsumerRecords`].
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.FetchCollector<K, V>`.
pub(crate) struct FetchCollector<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    metadata: Arc<ConsumerMetadata>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    fetch_config: FetchConfig,
    deserializers: Arc<Deserializers<K, V>>,
    time: Arc<dyn FetchCollectorTime>,
    /// Records per-partition lag / lead metrics. Phase M3 re-introduces the
    /// `FetchMetricsManager` parameter Phase 7a dropped.
    ///
    /// The lag/lead sensors are registered at **INFO**, not DEBUG — full Java
    /// parity, since Java's `SensorBuilder` routes every sensor through
    /// `metrics.sensor(name)`, which defaults to `RecordingLevel.INFO`, and
    /// `FetchMetricsManager.java` never sets a level. So a default consumer does
    /// the full per-partition recording on every poll; it is not gated away.
    /// That is the accepted Java-parity cost, and it is why the per-call
    /// allocations on this path matter (see `SensorBuilder::with_tags`, whose
    /// tags closure exists to avoid them).
    metrics_manager: Arc<FetchMetricsManager>,
    /// Test-only injection point that forces [`Self::initialize`] to fail,
    /// translating Java's `FetchCollectorTest.testErrorInInitialize`
    /// anonymous-subclass override of `initialize()`. Rust `FetchCollector`
    /// is a concrete struct with no inheritance, so the test installs a
    /// closure here that returns the error `initialize` should raise. Gated
    /// behind `#[cfg(test)]` so it is compiled out of release builds — zero
    /// production code-size / perf impact (mirrors the project's established
    /// `#[cfg(test)]` injection pattern).
    ///
    /// The closure returns an `Option` so it can *decline* to fail a given
    /// invocation. That is what lets a test drive the "error deferred while
    /// records are already in hand" path, which needs one partition to
    /// initialize successfully before the next one fails.
    #[cfg(test)]
    force_initialize_error: Option<Box<dyn Fn() -> Option<Error> + Send + Sync>>,
}

impl<K, V> FetchCollector<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Constructs a `FetchCollector` from its dependencies.
    ///
    /// Translates Java's
    /// `FetchCollector(LogContext, ConsumerMetadata, SubscriptionState,
    /// FetchConfig, Deserializers, FetchMetricsManager, Time)`. The
    /// `LogContext` is dropped (we use the `log` crate). Phase M3 plumbs the
    /// `FetchMetricsManager` (dropped by Phase 7a) for per-partition lag/lead.
    pub(crate) fn new(
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_config: FetchConfig,
        deserializers: Arc<Deserializers<K, V>>,
        metrics_manager: Arc<FetchMetricsManager>,
        time: Arc<dyn FetchCollectorTime>,
    ) -> Self {
        Self {
            metadata,
            subscriptions,
            fetch_config,
            deserializers,
            time,
            metrics_manager,
            #[cfg(test)]
            force_initialize_error: None,
        }
    }

    /// Test-only: install a closure consulted on every [`Self::initialize`]
    /// call, which fails it by returning `Some(err)` and lets it proceed
    /// normally by returning `None`. Reproduces Java's
    /// `FetchCollectorTest.testErrorInInitialize` anonymous-subclass
    /// override. See the `force_initialize_error` field doc.
    #[cfg(test)]
    fn set_force_initialize_error(&mut self, f: impl Fn() -> Option<Error> + Send + Sync + 'static) {
        self.force_initialize_error = Some(Box::new(f));
    }

    /// Return the fetched [`ConsumerRecord`]s, drain the [`FetchBuffer`]
    /// record buffer, and update the consumed position.
    ///
    /// Returning an empty [`ConsumerRecords`] guarantees the consumed
    /// position is not updated.
    ///
    /// Translates Java's
    /// `Fetch<K, V> collectFetch(FetchBuffer fetchBuffer)`.
    ///
    /// # Errors
    ///
    /// - [`Error::ConsumerOffsetOutOfRange`](crate::common::Error::ConsumerOffsetOutOfRange) when the response has
    ///   `OFFSET_OUT_OF_RANGE` for a partition and no default reset
    ///   strategy is configured.
    /// - [`Error::topic_authorization`] when the response has
    ///   `TOPIC_AUTHORIZATION_FAILED` for a partition.
    /// - Other [`Error`]s for corrupt records, unexpected error
    ///   codes, or deserialization failures (when no records have been
    ///   decoded yet).
    ///
    /// # Take-then-restore invariant on `next_in_line_fetch`
    ///
    /// This function calls `fetch_buffer.take_next_in_line_fetch()` at
    /// the top of each iteration (destructive read). Java's
    /// `FetchCollector.collectFetch` uses `nextInLineFetch()` (non-
    /// destructive). Any future modification that returns `Err`
    /// mid-iteration with an in-progress `next_in_line_fetch` MUST call
    /// `fetch_buffer.set_next_in_line_fetch(Some(fetch))` before
    /// returning, otherwise the buffer slot will be permanently empty.
    /// The current implementation restores on every Err-path that took
    /// ownership; preserve this invariant.
    pub(crate) fn collect_fetch(&self, fetch_buffer: &FetchBuffer) -> Result<ConsumerRecords<K, V>, Error> {
        let mut records_by_partition: IndexMap<TopicPartition, Vec<ConsumerRecord<K, V>>> = IndexMap::new();
        let mut next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        let mut paused_completed_fetches: Vec<CompletedFetch> = Vec::new();
        let mut records_remaining: i32 = self.fetch_config.max_poll_records;
        // Java's `Fetch.positionAdvanced`, accumulated via `Fetch.add` which
        // ORs the per-partition flag.
        let mut position_advanced = false;

        // Track the first error encountered so the finally-style cleanup
        // runs even when the collect loop exits early. Java's behavior
        // matches: a KafkaException is rethrown only if `fetch.isEmpty()`.
        let mut deferred_error: Option<Error> = None;

        while records_remaining > 0 {
            // The Rust port works on a single-owner basis: take the
            // next-in-line out, work with it, put it back. Mirrors Java's
            // mutable-reference semantics.
            let next_in_line = fetch_buffer.take_next_in_line_fetch();
            let next_is_consumed = next_in_line.as_ref().is_some_and(|cf| cf.is_consumed());

            if next_in_line.is_none() || next_is_consumed {
                // The "next in line" slot is empty or finished — pull from
                // the queue (Java's peek + poll).
                if next_in_line.is_some() {
                    // Drop the consumed entry; we don't reuse it.
                    let _ = next_in_line;
                }

                // Java: completedFetch = fetchBuffer.peek(); if null break;
                if fetch_buffer.is_empty() {
                    break;
                }
                // We need to inspect the head: is_initialized + records bytes.
                // Then either initialize-and-poll, or set-as-next-and-poll.
                let head_is_initialized = match fetch_buffer.peek_initialized() {
                    Some(b) => b,
                    None => break,
                };

                if !head_is_initialized {
                    // Pull from the queue. If initialization fails AND the
                    // fetch is empty AND the payload had zero bytes, Java
                    // polls (we discard); otherwise it leaves the entry
                    // behind. We mirror that by taking ownership and
                    // pushing back on the "leave" path.
                    let completed_fetch = fetch_buffer.poll().expect("non-empty checked above");
                    // Snapshot records size BEFORE moving cf into initialize.
                    let records_size_bytes = records_size(&completed_fetch.partition_data);

                    match self.initialize(completed_fetch) {
                        Ok(maybe_initialized) => {
                            fetch_buffer.set_next_in_line_fetch(maybe_initialized);
                        },
                        Err(boxed) => {
                            let (cf, e) = *boxed;
                            // Mirror Java's catch (KafkaException e):
                            //   if (fetch.isEmpty() && records.sizeInBytes() == 0) fetchBuffer.poll();
                            //   throw e;
                            // We've already polled, so the "throw without
                            // polling" case must push_front to restore.
                            let fetch_is_empty = records_by_partition.is_empty();
                            if !(fetch_is_empty && records_size_bytes == 0) {
                                fetch_buffer.push_front(cf);
                            }
                            // Defer the throw so the paused-fetches
                            // restore step still runs (matches Java's
                            // finally block).
                            deferred_error = Some(e);
                            break;
                        },
                    }
                    // Java: fetchBuffer.poll() removes the entry already
                    // moved to next-in-line.  We already polled above, so
                    // no additional action.
                } else {
                    // Initialized — set as next-in-line and drop from queue.
                    let cf = fetch_buffer.poll().expect("non-empty checked above");
                    fetch_buffer.set_next_in_line_fetch(Some(cf));
                }
                // Loop back: read the new next-in-line.
                continue;
            }

            // next_in_line is Some and not consumed.
            let cf = next_in_line.expect("verified Some above");
            // Pause check.
            let is_paused = {
                let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                guard.is_paused(&cf.partition)
            };
            if is_paused {
                debug!(
                    "Skipping fetching records for assigned partition {} because it is paused",
                    cf.partition
                );
                paused_completed_fetches.push(cf);
                fetch_buffer.set_next_in_line_fetch(None);
                continue;
            }

            // Fetch up to `records_remaining` from the cursor.
            match self.fetch_records_from_partition(cf, records_remaining) {
                Ok(FetchPartitionOutcome {
                    partition_records,
                    next_offset,
                    position_advanced: part_position_advanced,
                    cf: cf_back,
                }) => {
                    let num = partition_records.len() as i32;
                    if num > 0 {
                        records_remaining -= num;
                        // Common case (one fetch per partition per poll): the
                        // entry does not yet exist, so MOVE the whole Vec in
                        // wholesale instead of an element-by-element `extend`,
                        // which would memmove every (large) `ConsumerRecord`
                        // struct. Only when a partition already has records
                        // collected in this poll do we fall back to `extend`.
                        match records_by_partition.entry(cf_back.partition.clone()) {
                            indexmap::map::Entry::Vacant(e) => {
                                e.insert(partition_records);
                            },
                            indexmap::map::Entry::Occupied(mut e) => {
                                e.get_mut().extend(partition_records);
                            },
                        }
                    }
                    if let Some(no) = next_offset {
                        next_offsets.insert(cf_back.partition.clone(), no);
                    }
                    // Java's `Fetch.add` ORs `positionAdvanced`.
                    position_advanced |= part_position_advanced;
                    // Put cf back as next-in-line for the next iteration.
                    fetch_buffer.set_next_in_line_fetch(Some(cf_back));
                },
                Err(boxed) => {
                    let (cf_back, e) = *boxed;
                    if !records_by_partition.is_empty() {
                        // Java: if (fetch.isEmpty()) throw e; — non-empty
                        // means we return what we have. Stash the cf back
                        // and stop.
                        fetch_buffer.set_next_in_line_fetch(Some(cf_back));
                        deferred_error = Some(e);
                        break;
                    }
                    // Empty so far: re-stash and propagate. Defer the throw
                    // rather than returning here, so Java's `finally {
                    // fetchBuffer.addAll(pausedCompletedFetches) }` still runs
                    // — a bare `return Err` would drop the paused-partition
                    // completed fetches collected so far, forcing a needless
                    // re-fetch of records already in hand. The tail guard
                    // re-tests the same `fetch.isEmpty()` predicate, so the
                    // propagate/swallow decision is unchanged.
                    fetch_buffer.set_next_in_line_fetch(Some(cf_back));
                    deferred_error = Some(e);
                    break;
                },
            }
        }

        // Java's `finally`: re-enqueue any paused-partition completed
        // fetches so the next collect_fetch can reconsider them.
        if !paused_completed_fetches.is_empty() {
            fetch_buffer.add_all(paused_completed_fetches);
        }

        // Java's outer `catch (KafkaException e)` (`FetchCollector.java:138`)
        // swallows the error when we have records in hand (`!fetch.isEmpty()`).
        // But the generic `java.lang` / `java.util` runtime exceptions are NOT
        // `KafkaException`s — they sit beside it in the hierarchy rather than
        // below it (`common/KafkaException.java:22`), so they escape the catch
        // unconditionally. Mirror that here: a generic error always propagates,
        // even with already-decoded records buffered.
        //
        // Delegate the hierarchy test to `is_kafka_error` rather than matching
        // variants inline — an inline match is a second source of truth that
        // drifts as variants are added. (It had already drifted: it tested
        // only `IllegalState`, silently swallowing `IllegalArgument` and
        // `ConcurrentModification`, both of which Java also lets escape.)
        if let Some(e) = deferred_error
            && (!e.is_kafka_error() || records_by_partition.is_empty())
        {
            return Err(e);
        }

        Ok(ConsumerRecords::new_with_position_advanced(
            records_by_partition,
            next_offsets,
            position_advanced,
        ))
    }

    /// Pulls records from `next_in_line` for its partition, consulting
    /// subscription state for fetchability and position validity.
    ///
    /// On success returns the consumer records, the next offset & metadata
    /// (if the position advanced), and the [`CompletedFetch`] back.
    /// On failure returns the [`CompletedFetch`] back alongside the error
    /// so the caller can decide to restore it as next-in-line.
    ///
    /// Translates Java's `Fetch<K, V> fetchRecords(CompletedFetch, int)`.
    fn fetch_records_from_partition(
        &self,
        mut cf: CompletedFetch,
        max_records: i32,
    ) -> Result<FetchPartitionOutcome<K, V>, FetchFail> {
        let tp = cf.partition.clone();

        // Read snapshot of subscription state under the lock.
        enum FetchabilityCheck {
            NotAssigned,
            NotFetchable,
            MissingPosition,
            Position(FetchPosition),
        }
        let check = {
            let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            if !guard.is_assigned(&tp) {
                FetchabilityCheck::NotAssigned
            } else if !guard.is_fetchable(&tp) {
                FetchabilityCheck::NotFetchable
            } else {
                match guard.position_or_null(&tp) {
                    Some(p) => FetchabilityCheck::Position(p.clone()),
                    None => FetchabilityCheck::MissingPosition,
                }
            }
        };

        match check {
            FetchabilityCheck::NotAssigned => {
                debug!(
                    "Not returning fetched records for partition {} since it is no longer assigned",
                    tp
                );
                cf.drain();
                Ok(FetchPartitionOutcome {
                    partition_records: Vec::new(),
                    next_offset: None,
                    position_advanced: false,
                    cf,
                })
            },
            FetchabilityCheck::NotFetchable => {
                debug!(
                    "Not returning fetched records for assigned partition {} since it is no longer fetchable",
                    tp
                );
                cf.drain();
                Ok(FetchPartitionOutcome {
                    partition_records: Vec::new(),
                    next_offset: None,
                    position_advanced: false,
                    cf,
                })
            },
            FetchabilityCheck::MissingPosition => {
                // Java throws IllegalStateException here
                // (`FetchCollector.java:166-167`), but that path is
                // effectively dead code because Java's `isFetchable(tp)`
                // already requires `hasValidPosition`. In Rust the
                // analogous check inside `is_fetchable` covers the same
                // invariant — but the snapshot is taken inside the same
                // `SubscriptionState` lock guard, so observing
                // `is_fetchable(tp) == true && position_or_null(tp) ==
                // None` would imply a `TopicPartitionState` invariant
                // bug.
                //
                // During KIP-848 rebalance, however, a `CompletedFetch`
                // for a just-revoked partition can land in the buffer
                // while `is_fetchable` briefly flips back true on the
                // subsequent reconciliation (rare but observable). Java
                // would raise too in that case; the Rust translation
                // converges with the practical Java behavior by treating
                // this as a transient skip-this-poll (mirroring Java's
                // adjacent `!isFetchable` / `!isAssigned` arms) instead
                // of surfacing the error to the user. See
                // `COMMENTS.1.md` Issue 7.
                debug!(
                    "Not returning fetched records for assigned partition {} since it has no position yet \
                     (transient rebalance window)",
                    tp
                );
                cf.drain();
                Ok(FetchPartitionOutcome {
                    partition_records: Vec::new(),
                    next_offset: None,
                    position_advanced: false,
                    cf,
                })
            },
            FetchabilityCheck::Position(position) => {
                if cf.next_fetch_offset() != position.offset {
                    // Not next-in-line based on the consumed position;
                    // these must be from an obsolete request.
                    debug!(
                        "Ignoring fetched records for {} at offset {} since the current position is {}",
                        tp,
                        cf.next_fetch_offset(),
                        position
                    );
                    cf.drain();
                    return Ok(FetchPartitionOutcome {
                        partition_records: Vec::new(),
                        next_offset: None,
                        position_advanced: false,
                        cf,
                    });
                }

                // Snapshot the partition record-fetch via cf's iteration.
                let key_de = self.deserializers.key_deserializer();
                let value_de = self.deserializers.value_deserializer();
                let part_records = match cf.fetch_records::<K, V>(&self.fetch_config, key_de, value_de, max_records) {
                    Ok(r) => r,
                    Err(e) => return Err(Box::new((cf, e))),
                };

                trace!(
                    "Returning {} fetched records at offset {} for assigned partition {}",
                    part_records.len(),
                    position,
                    tp
                );

                // Advance the subscription position if the cursor advanced.
                let mut position_advanced = false;
                if cf.next_fetch_offset() > position.offset {
                    let next_position = FetchPosition::with_leader(
                        cf.next_fetch_offset(),
                        cf.last_epoch(),
                        position.current_leader.clone(),
                    );
                    trace!(
                        "Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                        position,
                        next_position,
                        tp,
                        part_records.len()
                    );
                    {
                        let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                        // set_position only fails if the partition is no
                        // longer assigned — in that case drop silently;
                        // Java would also no-op here.
                        let _ = guard.set_position(&tp, next_position);
                    }
                    position_advanced = true;
                }

                // Record per-partition lag / lead, mirroring Java's
                // `FetchCollector` (`subscriptions.partitionLag` /
                // `partitionLead` → `metricsManager.recordPartitionLag/Lead`).
                // The record methods update the client-level INFO
                // `records-lag-max` / `records-lead-min` sensors AND register +
                // record the DETAILED per-partition sensors at INFO — full Java
                // parity, no DEBUG gating (see `FetchMetricsManager`). This is
                // per-partition per-poll, not per-record (Java's accepted
                // per-fetch cost, to be measured in M8).
                let (partition_lag, partition_lead) = {
                    let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                    (
                        guard.partition_lag(&tp, self.fetch_config.isolation_level).ok().flatten(),
                        guard.partition_lead(&tp).ok().flatten(),
                    )
                };
                if let Some(lag) = partition_lag {
                    self.metrics_manager.record_partition_lag(&tp, lag);
                }
                if let Some(lead) = partition_lead {
                    self.metrics_manager.record_partition_lead(&tp, lead);
                }

                let metadata = match OffsetAndMetadata::with_leader_epoch(cf.next_fetch_offset(), cf.last_epoch(), "") {
                    Ok(m) => m,
                    Err(e) => return Err(Box::new((cf, e))),
                };

                Ok(FetchPartitionOutcome {
                    partition_records: part_records,
                    next_offset: Some(metadata),
                    position_advanced,
                    cf,
                })
            },
        }
    }

    /// Initialize a `CompletedFetch` — runs the per-partition error
    /// handling and position-validity check before the records are
    /// iterated.
    ///
    /// Returns the initialized fetch (`Some`) so the caller can install
    /// it as next-in-line, or `None` when the fetch is to be skipped
    /// (e.g. stale offset, invalid position, recoverable error).
    /// Returns `Err((cf, e))` so the caller can decide whether to
    /// push_front the entry on the queue.
    ///
    /// Translates Java's `CompletedFetch initialize(CompletedFetch)`.
    fn initialize(&self, completed_fetch: CompletedFetch) -> Result<Option<CompletedFetch>, FetchFail> {
        // Test-only injection: reproduce Java's testErrorInInitialize, where
        // the anonymous subclass overrides initialize() to throw. Compiled
        // out of release builds.
        #[cfg(test)]
        if let Some(f) = self.force_initialize_error.as_ref()
            && let Some(e) = f()
        {
            return Err(Box::new((completed_fetch, e)));
        }
        // DIAGNOSTIC (fetch_diag): age of this fetch when the app first touches
        // it = the bg-receipt -> app-delivery handoff (FetchBuffer drain depth),
        // the component of e2e latency that is NOT broker-side fetch wait.
        if let Some(created) = completed_fetch.created_at {
            log::info!(
                target: "fetch_diag",
                "app first-touch handoff_ms={} partition={}",
                created.elapsed().as_millis(),
                completed_fetch.partition
            );
        }
        let tp = completed_fetch.partition.clone();
        let error = Errors::for_code(completed_fetch.partition_data.error_code);

        // Compute the result first; `move_partition_to_end` runs in a
        // finally-style cleanup that mirrors Java exactly. The
        // `record_aggregated_metrics(0, 0)` Java call is a no-op here
        // (no metrics framework yet).
        let has_valid_position = {
            let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            guard.has_valid_position(&tp)
        };

        let result: Result<Option<CompletedFetch>, FetchFail> = if !has_valid_position {
            debug!("Ignoring fetched records for partition {tp} since it no longer has valid position");
            Ok(None)
        } else if error == Errors::None {
            self.handle_initialize_success(completed_fetch)
        } else {
            match self.handle_initialize_errors(completed_fetch, error) {
                Ok(_cf) => Ok(None),
                Err(boxed) => Err(boxed),
            }
        };

        // Finally: on any non-None error, move the partition to the end
        // of the subscription state's iteration order. This improves
        // wire-protocol serialization locality for the next fetch round.
        if error != Errors::None {
            let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            guard.move_partition_to_end(&tp);
        }

        result
    }

    /// Translates Java's `CompletedFetch handleInitializeSuccess(CompletedFetch)`.
    fn handle_initialize_success(
        &self,
        mut completed_fetch: CompletedFetch,
    ) -> Result<Option<CompletedFetch>, FetchFail> {
        let tp = completed_fetch.partition.clone();
        let fetch_offset = completed_fetch.next_fetch_offset();

        let position_offset = {
            let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            guard.position_or_null(&tp).map(|p| p.offset)
        };
        if position_offset.is_none_or(|o| o != fetch_offset) {
            debug!(
                "Discarding stale fetch response for partition {tp} since its offset {fetch_offset} does not match \
                 the expected offset {:?} or the partition has been unassigned",
                position_offset
            );
            return Ok(None);
        }

        let partition = &completed_fetch.partition_data;
        trace!(
            "Preparing to read {} bytes of data for partition {tp} with offset {fetch_offset}",
            records_size(partition)
        );

        // Java checks `batches.hasNext()` + `recordsSize > 0`. The Rust
        // CompletedFetch lazily iterates on first fetch_records — the
        // equivalent check fires when the loaded MemoryRecords is empty.
        // We approximate the Java check by treating a non-empty
        // records buffer that yields no batches as the same failure.
        let records_size_bytes = records_size(partition);
        if records_size_bytes > 0 {
            // Construct a temporary cursor view to see if any batch parses.
            // CompletedFetch initializes its cursor on first fetch_records;
            // we can avoid the extra allocation by simply trusting the
            // bytes are well-formed and letting fetch_records report a
            // decode error if not. Java's check exists because brokers
            // before KIP-74 could send a non-empty response with no
            // complete records — we keep the conservative behavior:
            // if the consumer never decodes a record from a non-empty
            // payload AND the cursor stays at fetch_offset, the next
            // iteration's `fetch.numRecords() == 0` path will return
            // and the buffer will not advance.
        }

        if !self.update_partition_state(partition, &tp) {
            return Ok(None);
        }

        completed_fetch.set_initialized();
        Ok(Some(completed_fetch))
    }

    /// Translates Java's
    /// `boolean updatePartitionState(PartitionData, TopicPartition)`.
    fn update_partition_state(&self, partition_data: &PartitionData, tp: &TopicPartition) -> bool {
        let high_watermark = partition_data.high_watermark;
        let log_start_offset = partition_data.log_start_offset;
        let last_stable_offset = partition_data.last_stable_offset;
        let preferred_read_replica = partition_data.preferred_read_replica;

        let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");

        if high_watermark >= 0 {
            trace!("Updating high watermark for partition {tp} to {high_watermark}");
            if !guard.try_updating_high_watermark(tp, high_watermark) {
                return false;
            }
        }
        if log_start_offset >= 0 {
            trace!("Updating log start offset for partition {tp} to {log_start_offset}");
            if !guard.try_updating_log_start_offset(tp, log_start_offset) {
                return false;
            }
        }
        if last_stable_offset >= 0 {
            trace!("Updating last stable offset for partition {tp} to {last_stable_offset}");
            if !guard.try_updating_last_stable_offset(tp, last_stable_offset) {
                return false;
            }
        }

        // Java: FetchResponse.isPreferredReplica(partitionData).
        if preferred_read_replica != crate::common::requests::fetch_response::INVALID_PREFERRED_REPLICA_ID {
            let expire_time_ms = self.time.milliseconds() + self.metadata.metadata_arc().metadata_expire_ms();
            debug!(
                "Updating preferred read replica for partition {tp} to {preferred_read_replica}, set to expire at {expire_time_ms}"
            );
            // tryUpdatingPreferredReadReplica returns false iff the
            // partition is no longer assigned — in that case Java's check
            // returns false.
            return guard.try_updating_preferred_read_replica(tp, preferred_read_replica, expire_time_ms);
        }
        true
    }

    /// Translates Java's
    /// `void handleInitializeErrors(CompletedFetch, Errors)`.
    fn handle_initialize_errors(
        &self,
        completed_fetch: CompletedFetch,
        error: Errors,
    ) -> Result<CompletedFetch, FetchFail> {
        let tp = completed_fetch.partition.clone();
        let fetch_offset = completed_fetch.next_fetch_offset();

        match error {
            Errors::NotLeaderOrFollower
            | Errors::ReplicaNotAvailable
            | Errors::KafkaStorageError
            | Errors::FencedLeaderEpoch
            | Errors::OffsetNotAvailable => {
                debug!("Error in fetch for partition {tp}: {:?}", error);
                request_metadata_update(&self.metadata, &self.subscriptions, &tp);
                Ok(completed_fetch)
            },
            Errors::UnknownTopicOrPartition => {
                warn!("Received unknown topic or partition error in fetch for partition {tp}");
                request_metadata_update(&self.metadata, &self.subscriptions, &tp);
                Ok(completed_fetch)
            },
            Errors::UnknownTopicId => {
                warn!("Received unknown topic ID error in fetch for partition {tp}");
                request_metadata_update(&self.metadata, &self.subscriptions, &tp);
                Ok(completed_fetch)
            },
            Errors::InconsistentTopicId => {
                warn!("Received inconsistent topic ID error in fetch for partition {tp}");
                request_metadata_update(&self.metadata, &self.subscriptions, &tp);
                Ok(completed_fetch)
            },
            Errors::OffsetOutOfRange => self.handle_offset_out_of_range(completed_fetch, fetch_offset),
            Errors::TopicAuthorizationFailed => {
                warn!("Not authorized to read from partition {tp}.");
                let mut set = std::collections::HashSet::new();
                set.insert(tp.topic().to_string());
                Err(Box::new((completed_fetch, Error::topic_authorization(set))))
            },
            Errors::UnknownLeaderEpoch => {
                debug!("Received unknown leader epoch error in fetch for partition {tp}");
                Ok(completed_fetch)
            },
            Errors::UnknownServerError => {
                warn!("Unknown server error while fetching offset {fetch_offset} for topic-partition {tp}");
                Ok(completed_fetch)
            },
            // Java throws a *bare* `KafkaException` here (`FetchCollector.java:371-375`),
            // not a `CorruptRecordException` — deliberately, because
            // `CorruptRecordException extends RetriableException` and a corrupt
            // message must reach the application rather than be retried at the same
            // offset forever. Building it from `Errors::CorruptMessage` would resolve
            // to that retriable class and invert the decision.
            Errors::CorruptMessage => Err(Box::new((
                completed_fetch,
                Error::kafka(format!(
                    "Encountered corrupt message when fetching offset {fetch_offset} for topic-partition {tp}"
                )),
            ))),
            other => Err(Box::new((
                completed_fetch,
                Error::illegal_state(format!(
                    "Unexpected error code {} while fetching at offset {fetch_offset} from topic-partition {tp}",
                    other.code()
                )),
            ))),
        }
    }

    fn handle_offset_out_of_range(
        &self,
        completed_fetch: CompletedFetch,
        fetch_offset: i64,
    ) -> Result<CompletedFetch, FetchFail> {
        let tp = completed_fetch.partition.clone();
        let cleared_replica_id = {
            let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            guard.clear_preferred_read_replica(&tp)
        };
        if cleared_replica_id.is_some() {
            debug!(
                "Unset the preferred read replica {:?} for partition {tp} since we got OFFSET_OUT_OF_RANGE when fetching {fetch_offset}",
                cleared_replica_id
            );
            return Ok(completed_fetch);
        }
        // No preferred replica to clear — we're fetching from the leader,
        // so handle normally.
        let (position_opt, has_default_reset) = {
            let guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            let p = guard.position_or_null(&tp).cloned();
            (p, guard.has_default_offset_reset_policy())
        };
        match position_opt {
            None => {
                debug!(
                    "Discarding stale fetch response for partition {tp} since the fetched offset {fetch_offset} \
                     does not match the current offset {position_opt:?} or the partition has been unassigned"
                );
                Ok(completed_fetch)
            },
            Some(position) if fetch_offset != position.offset => {
                debug!(
                    "Discarding stale fetch response for partition {tp} since the fetched offset {fetch_offset} \
                     does not match the current offset {position} or the partition has been unassigned"
                );
                Ok(completed_fetch)
            },
            Some(position) => {
                let error_message = format!("Fetch position {position} is out of range for partition {tp}");
                if has_default_reset {
                    info!("{error_message}, resetting offset");
                    let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                    guard.request_offset_reset_if_assigned(&tp);
                    Ok(completed_fetch)
                } else {
                    info!("{error_message}, raising error to the application since no reset policy is configured");
                    let mut map = HashMap::new();
                    map.insert(tp.clone(), position.offset);
                    let err = Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::with_message(
                        error_message,
                        map,
                    ));
                    Err(Box::new((completed_fetch, err)))
                }
            },
        }
    }
}

/// Internal helper struct: the per-partition outcome returned from
/// [`FetchCollector::fetch_records_from_partition`].
struct FetchPartitionOutcome<K, V> {
    partition_records: Vec<ConsumerRecord<K, V>>,
    next_offset: Option<OffsetAndMetadata>,
    /// Java's `Fetch.positionAdvanced` for this partition: true iff the
    /// consumed position moved forward (even when zero records are returned,
    /// e.g. an all-aborted batch under READ_COMMITTED).
    position_advanced: bool,
    cf: CompletedFetch,
}

/// Internal `Err` payload for [`FetchCollector::initialize`] and
/// [`FetchCollector::fetch_records_from_partition`] — pairs the rejected
/// `CompletedFetch` with the `Error` so the caller can decide
/// whether to restore the fetch.
///
/// Boxed because `CompletedFetch` is large (~600 bytes); a bare tuple
/// triggers clippy's `result_large_err` warning at the call sites.
type FetchFail = Box<(CompletedFetch, Error)>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::IsolationLevel;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::record::{MemoryRecords, SimpleRecord, TimestampType};
    use crate::common::serialization::Deserializer;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::fetch_response_data::PartitionData;
    use crate::metadata::LeaderAndEpoch;
    use std::collections::HashSet;

    const DEFAULT_RECORD_COUNT: i32 = 10;
    const DEFAULT_MAX_POLL_RECORDS: i32 = 500;

    /// Mock time source — `MockTime` analog.
    struct MockTime {
        now: std::sync::atomic::AtomicI64,
    }
    impl MockTime {
        fn new() -> Self {
            Self { now: std::sync::atomic::AtomicI64::new(0) }
        }
    }
    impl FetchCollectorTime for MockTime {
        fn milliseconds(&self) -> i64 {
            self.now.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))
        }
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Builds a throwaway per-response aggregator tracking only `partition`,
    /// for tests that build a `CompletedFetch` but don't assert metric values.
    fn agg_for(partition: &TopicPartition) -> Arc<FetchMetricsAggregator> {
        let mut partitions = HashSet::new();
        partitions.insert(partition.clone());
        Arc::new(FetchMetricsAggregator::new(FetchMetricsManager::for_test(), partitions))
    }

    fn make_records(starting_offset: i64, count: i32) -> Vec<u8> {
        let records: Vec<SimpleRecord> = (0..count)
            .map(|i| {
                SimpleRecord::new(
                    0,
                    Some("key".as_bytes().to_vec()),
                    Some(format!("value-{i}").into_bytes()),
                    vec![],
                )
            })
            .collect();
        let mr = MemoryRecords::with_records_at_offset(
            2,
            starting_offset,
            Compression::none(),
            TimestampType::CreateTime,
            &records,
        );
        mr.buffer().to_vec()
    }

    /// Test harness mirroring Java's `buildDependencies`.
    struct Harness {
        subs: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        deserializers: Arc<Deserializers<String, String>>,
        fetch_config: FetchConfig,
        fetch_buffer: Arc<FetchBuffer>,
        time: Arc<MockTime>,
        collector: FetchCollector<String, String>,
    }

    fn build_harness(max_poll_records: i32, isolation: IsolationLevel) -> Harness {
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ConsumerMetadata::new(
            0,
            1_000,
            10_000,
            false,
            false,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let fetch_config =
            FetchConfig::new(1, 50 * 1024 * 1024, 500, 1024 * 1024, max_poll_records, true, "", isolation);
        let deserializers = Arc::new(Deserializers::new(Box::new(StringDeserializer), Box::new(StringDeserializer)));
        let fetch_buffer = Arc::new(FetchBuffer::new());
        let time: Arc<MockTime> = Arc::new(MockTime::new());

        let collector = FetchCollector::new(
            metadata.clone(),
            subs.clone(),
            fetch_config.clone(),
            deserializers.clone(),
            FetchMetricsManager::for_test(),
            time.clone(),
        );

        Harness { subs, metadata, deserializers, fetch_config, fetch_buffer, time, collector }
    }

    fn assign_and_seek(h: &Harness, partition: &TopicPartition) {
        let mut guard = h.subs.lock().expect("lock");
        let mut set: HashSet<TopicPartition> = HashSet::new();
        set.insert(partition.clone());
        guard.assign_from_user(set).unwrap();
        guard.seek(partition, 0).unwrap();
    }

    fn build_completed_fetch(
        h: &Harness,
        partition: TopicPartition,
        fetch_offset: i64,
        record_count: i32,
        error: Option<Errors>,
    ) -> CompletedFetch {
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(partition.partition());
        partition_data.set_high_watermark(1000);
        partition_data.set_records(Some(bytes::Bytes::from(make_records(0, record_count))));
        if let Some(e) = error {
            partition_data.set_error_code(e.code());
        }
        let aggregator = agg_for(&partition);
        CompletedFetch::new_full(
            h.subs.clone(),
            Arc::new(crate::common::memory::buffer_supplier::BufferSupplier::create()),
            partition,
            partition_data,
            aggregator,
            fetch_offset,
        )
    }

    /// Translated from `FetchCollectorTest.testFetchNormal`.
    ///
    /// The Java test sets `recordCount = DEFAULT_MAX_POLL_RECORDS` (500) so
    /// the collect_fetch consumes all records in one shot and `records_remaining`
    /// becomes 0, leaving the next-in-line entry partially-consumed. The
    /// Rust port uses `max_poll_records = recordCount` to reproduce the
    /// boundary exactly without generating 500 records.
    #[test]
    fn test_fetch_normal() {
        let record_count = DEFAULT_RECORD_COUNT; // 10
        // Set max_poll_records == record_count so the collect_fetch loop
        // exits as soon as the full batch is returned — matches the Java
        // setup where the test asserts `nextInLineFetch != null` after.
        let h = build_harness(record_count, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, record_count, None);

        assert!(h.fetch_buffer.is_empty());
        h.fetch_buffer.add(cf);
        assert!(!h.fetch_buffer.is_empty());

        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.count());
        assert_eq!(1, fetch.next_offsets().len());
        let expected_meta = OffsetAndMetadata::with_leader_epoch(record_count as i64, None, "").unwrap();
        assert_eq!(&expected_meta, fetch.next_offsets().get(&partition).unwrap());

        // Buffer queue empty, next-in-line still has the cf.
        assert!(h.fetch_buffer.is_empty());
        assert!(h.fetch_buffer.has_next_in_line_fetch());

        // Position updated.
        {
            let guard = h.subs.lock().expect("lock");
            let pos = guard.position(&partition).unwrap().unwrap();
            assert_eq!(record_count as i64, pos.offset);
        }

        // Second poll: empty result, next-in-line gets drained.
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert_eq!(0, fetch.count());
        assert!(fetch.is_empty());
        // After the second poll, the next-in-line is drained.
    }

    /// Translated from `FetchCollectorTest.testNoResultsIfInitializing`.
    #[test]
    fn test_no_results_if_initializing() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        // Assign without seek -> partition initialized but no position.
        {
            let mut guard = h.subs.lock().expect("lock");
            let mut set: HashSet<TopicPartition> = HashSet::new();
            set.insert(partition.clone());
            guard.assign_from_user(set).unwrap();
        }
        // Verify state: no valid position, not fetchable.
        {
            let guard = h.subs.lock().expect("lock");
            assert!(guard.position_or_null(&partition).is_none());
            assert!(!guard.is_fetchable(&partition));
            assert!(!guard.has_valid_position(&partition));
        }

        let cf = build_completed_fetch(&h, partition.clone(), 0, DEFAULT_RECORD_COUNT, None);
        h.fetch_buffer.add(cf);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert_eq!(0, fetch.count());
        assert_eq!(0, fetch.next_offsets().len());
    }

    /// Translated from `FetchCollectorTest.testFetchingPausedPartitionsYieldsNoRecords`.
    #[test]
    fn test_fetching_paused_partitions_yields_no_records() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        {
            let mut guard = h.subs.lock().expect("lock");
            assert!(!guard.is_paused(&partition));
            guard.pause(&partition).unwrap();
            assert!(guard.is_paused(&partition));
        }

        let cf = build_completed_fetch(&h, partition.clone(), 0, DEFAULT_RECORD_COUNT, None);
        h.fetch_buffer.set_next_in_line_fetch(Some(cf));
        assert!(h.fetch_buffer.has_next_in_line_fetch());
        // Queue is empty since cf was placed as next-in-line.
        assert!(h.fetch_buffer.is_empty());

        // Now run collect_fetch and validate that we get an empty fetch back
        // because the partition is paused.
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());

        // The next-in-line was re-enqueued into the buffer.
        assert!(!h.fetch_buffer.is_empty());
        assert!(!h.fetch_buffer.has_next_in_line_fetch());
    }

    /// Translated from `FetchCollectorTest.testFetchWithOffsetOutOfRange` —
    /// no default reset policy: error is raised.
    #[test]
    fn test_fetch_with_offset_out_of_range_no_default_reset() {
        let mut h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        // Re-create with NONE reset policy so OFFSET_OUT_OF_RANGE raises.
        h.subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));
        h.metadata = Arc::new(ConsumerMetadata::new(
            0,
            1_000,
            10_000,
            false,
            false,
            h.subs.clone(),
            ClusterResourceListeners::new(),
        ));
        h.collector = FetchCollector::new(
            h.metadata.clone(),
            h.subs.clone(),
            h.fetch_config.clone(),
            h.deserializers.clone(),
            FetchMetricsManager::for_test(),
            h.time.clone(),
        );
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::OffsetOutOfRange));
        h.fetch_buffer.add(cf);

        let err = h.collector.collect_fetch(&h.fetch_buffer).unwrap_err();
        // Raised as its own class now, not flattened into Error::IllegalState.
        let msg = err.message();
        assert!(msg.contains("out of range"), "unexpected message: {msg}");
    }

    /// Translated from `FetchCollectorTest.testFetchWithOffsetOutOfRange` —
    /// with default reset policy: silent reset.
    #[test]
    fn test_fetch_with_offset_out_of_range_with_default_reset() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::OffsetOutOfRange));
        h.fetch_buffer.add(cf);

        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
    }

    /// Translated from `FetchCollectorTest.testFetchWithTopicAuthorizationFailed`.
    #[test]
    fn test_fetch_with_topic_authorization_failed() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::TopicAuthorizationFailed));
        h.fetch_buffer.add(cf);

        let err = h.collector.collect_fetch(&h.fetch_buffer).unwrap_err();
        match err {
            Error::TopicAuthorization(ref ta) => {
                assert!(ta.unauthorized_topics().contains("topic-a"));
            },
            other => panic!("expected TopicAuthorization, got {other:?}"),
        }
    }

    /// Translated from `FetchCollectorTest.testFetchWithUnknownLeaderEpoch`.
    #[test]
    fn test_fetch_with_unknown_leader_epoch() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::UnknownLeaderEpoch));
        h.fetch_buffer.add(cf);

        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
    }

    /// Translated from `FetchCollectorTest.testFetchWithUnknownServerError`.
    #[test]
    fn test_fetch_with_unknown_server_error() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::UnknownServerError));
        h.fetch_buffer.add(cf);

        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
    }

    /// Translated from `FetchCollectorTest.testFetchWithCorruptMessage`.
    #[test]
    fn test_fetch_with_corrupt_message() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::CorruptMessage));
        h.fetch_buffer.add(cf);

        let err = h.collector.collect_fetch(&h.fetch_buffer).unwrap_err();
        let msg = err.message();
        assert!(msg.contains("corrupt message"), "unexpected message: {msg}");
    }

    /// Translated from `FetchCollectorTest.testFetchWithMetadataRefreshErrors`
    /// (parameterized). Iterates the error set in a loop.
    #[test]
    fn test_fetch_with_metadata_refresh_errors() {
        let errors = vec![
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::KafkaStorageError,
            Errors::FencedLeaderEpoch,
            Errors::OffsetNotAvailable,
            Errors::UnknownTopicOrPartition,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
        ];
        for error in errors {
            let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
            let partition = tp("topic-a", 0);
            assign_and_seek(&h, &partition);
            // Set a preferred read replica so we can verify it's cleared.
            {
                let mut guard = h.subs.lock().expect("lock");
                guard.update_preferred_read_replica(&partition, 5, 1_000).unwrap();
                assert_eq!(Some(5), guard.preferred_read_replica(&partition, 1_000));
            }

            let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(error));
            h.fetch_buffer.add(cf);
            let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
            assert!(fetch.is_empty(), "expected empty for {error:?}");
            assert_eq!(0, fetch.next_offsets().len(), "expected no next_offsets for {error:?}");

            // request_metadata_update should have cleared the preferred replica.
            let mut guard = h.subs.lock().expect("lock");
            assert_eq!(
                None,
                guard.preferred_read_replica(&partition, 1_000),
                "preferred replica should be cleared for {error:?}"
            );
        }
    }

    /// Translated from `FetchCollectorTest.testFetchWithOtherErrors`
    /// (parameterized). Mirrors Java's `Errors.values()` minus the
    /// explicitly-handled error set: every remaining error code must reach
    /// the catch-all arm in `handle_initialize_errors` and surface as an
    /// `IllegalState` (Java's `IllegalStateException`).
    ///
    /// Java builds the source as `Errors.values()` with the handled set
    /// removed. We mirror that exactly: iterate every `Errors` variant and
    /// skip the ones with dedicated handling (the `Errors::None` happy path
    /// plus the metadata-refresh / OOR / auth / leader-epoch / server /
    /// corrupt arms). This is the full set, not a 3-error sample, so adding
    /// a new "other" error to the enum is automatically covered.
    #[test]
    fn test_fetch_with_other_errors() {
        // The errors that have dedicated handling and therefore do NOT take
        // the catch-all arm. Mirrors the `errors.removeAll(...)` list in
        // Java's `testFetchWithOtherErrorsSource`.
        let handled = [
            Errors::None,
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::KafkaStorageError,
            Errors::FencedLeaderEpoch,
            Errors::OffsetNotAvailable,
            Errors::UnknownTopicOrPartition,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
            Errors::OffsetOutOfRange,
            Errors::TopicAuthorizationFailed,
            Errors::UnknownLeaderEpoch,
            Errors::UnknownServerError,
            Errors::CorruptMessage,
        ];

        // Rust's `Errors` has no `values()` array (adding one would be a
        // production change, out of scope for a test-parity phase), so we
        // enumerate the full set by walking every assigned error code via
        // `Errors::for_code` and de-duplicating. Codes 0..=133 cover the
        // current enum; unassigned codes fold into `UnknownServerError`
        // (which is in `handled`, so they are skipped). This mirrors
        // Java's `Errors.values()` minus the removed set.
        let all_errors: std::collections::BTreeSet<i16> = (0i16..=133).collect();
        let mut seen: HashSet<Errors> = HashSet::new();
        let mut checked = 0usize;
        for code in all_errors {
            let error = Errors::for_code(code);
            if !seen.insert(error) {
                continue;
            }
            if handled.contains(&error) {
                continue;
            }
            checked += 1;
            let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
            let partition = tp("topic-a", 0);
            assign_and_seek(&h, &partition);

            let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(error));
            h.fetch_buffer.add(cf);
            let err = h.collector.collect_fetch(&h.fetch_buffer).unwrap_err();
            assert!(
                matches!(err, Error::IllegalState(_)),
                "expected IllegalState for {error:?}, got {err:?}"
            );
            // The catch-all message embeds the offending error code.
            assert!(
                err.message().contains(&error.code().to_string()),
                "expected error code {} in message for {error:?}: {}",
                error.code(),
                err.message()
            );
        }
        // Guard against `Errors::values()` returning an empty / tiny set:
        // there must be many "other" errors.
        assert!(checked > 10, "expected the full Errors set; only checked {checked}");
    }

    // ── testErrorInInitialize (parameterized ×4) ───────────────────────────
    //
    // Translated from `FetchCollectorTest.testErrorInInitialize`. Java
    // overrides `initialize()` in an anonymous subclass to throw; Rust uses
    // the `#[cfg(test)]` `set_force_initialize_error` hook (compiled out of
    // release builds). The contract under test is the `collect_fetch`
    // queue-state bookkeeping: if the CompletedFetch has 0 records the failed
    // initialize causes it to be polled off the queue (queue becomes empty);
    // if it has records the entry is left on the queue. I.e.
    // `recordCount == 0 == fetchBuffer.isEmpty()`.

    /// Builds a CompletedFetch whose payload has `record_count` records, or
    /// zero bytes when `record_count == 0` (so `records_size` is 0 and the
    /// `fetch.isEmpty() && recordsSize == 0` poll path fires, mirroring
    /// Java's empty `createRecords(0)`).
    fn build_completed_fetch_for_init_error(
        h: &Harness,
        partition: TopicPartition,
        record_count: i32,
    ) -> CompletedFetch {
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(partition.partition());
        partition_data.set_high_watermark(1000);
        if record_count == 0 {
            partition_data.set_records(Some(bytes::Bytes::new()));
        } else {
            partition_data.set_records(Some(bytes::Bytes::from(make_records(0, record_count))));
        }
        let aggregator = agg_for(&partition);
        CompletedFetch::new_full(
            h.subs.clone(),
            Arc::new(crate::common::memory::buffer_supplier::BufferSupplier::create()),
            partition,
            partition_data,
            aggregator,
            0,
        )
    }

    /// Drives one `testErrorInInitialize` case: install an initialize-error
    /// closure returning `make_error()`, add a CompletedFetch with
    /// `record_count` records, run `collect_fetch`, assert it errors and
    /// that the queue-empty state matches `record_count == 0`.
    fn run_error_in_initialize_case(record_count: i32, make_error: impl Fn() -> Error + Send + Sync + 'static) {
        let mut h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);
        h.collector.set_force_initialize_error(move || Some(make_error()));

        let cf = build_completed_fetch_for_init_error(&h, partition.clone(), record_count);
        h.fetch_buffer.add(cf);
        assert!(!h.fetch_buffer.is_empty());

        // collect_fetch must surface the injected error (no records decoded).
        let err = h.collector.collect_fetch(&h.fetch_buffer);
        assert!(
            err.is_err(),
            "expected initialize error to propagate for record_count={record_count}"
        );

        // recordCount == 0 ⇒ the empty entry is polled off the queue;
        // recordCount > 0 ⇒ the record-bearing entry is left on the queue.
        assert_eq!(
            record_count == 0,
            h.fetch_buffer.is_empty(),
            "queue-empty state must match (record_count == 0) for record_count={record_count}"
        );
    }

    /// `testErrorInInitialize(10, RuntimeException)` — record-bearing fetch,
    /// generic (non-Kafka) error: entry remains on the queue.
    #[test]
    fn test_error_in_initialize_runtime_with_records() {
        run_error_in_initialize_case(10, || Error::illegal_argument("simulated runtime error in initialize"));
    }

    /// `testErrorInInitialize(0, RuntimeException)` — empty fetch, generic
    /// error: entry is removed from the queue.
    #[test]
    fn test_error_in_initialize_runtime_empty() {
        run_error_in_initialize_case(0, || Error::illegal_argument("simulated runtime error in initialize"));
    }

    /// `testErrorInInitialize(10, KafkaException)` — record-bearing fetch,
    /// KafkaException: entry remains on the queue.
    #[test]
    fn test_error_in_initialize_kafka_with_records() {
        run_error_in_initialize_case(10, || Error::with_message(Errors::UnknownServerError, "simulated kafka error"));
    }

    /// `testErrorInInitialize(0, KafkaException)` — empty fetch,
    /// KafkaException: entry is removed from the queue.
    #[test]
    fn test_error_in_initialize_kafka_empty() {
        run_error_in_initialize_case(0, || Error::with_message(Errors::UnknownServerError, "simulated kafka error"));
    }

    // ── the deferred-error escape clause ──────────────────────────────────
    //
    // Java's `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`
    // (`FetchCollector.java:138`) can only catch `KafkaException`s. A generic
    // `java.lang` / `java.util` runtime exception is a sibling of
    // `KafkaException`, not a subclass (`common/KafkaException.java:22`), so
    // it flies straight past the catch even when records were collected.
    //
    // The cases below pin BOTH halves of that contract, so the escape clause
    // fails the suite if it is either too narrow (a generic error swallowed)
    // or too wide (a Kafka error propagated).

    /// Drives two partitions where the FIRST initializes successfully (its
    /// records land in the fetch) and the SECOND fails with `error`. Returns
    /// the `collect_fetch` outcome, so the caller asserts on swallow vs.
    /// propagate with records already in hand.
    fn run_deferred_error_with_records_in_hand(error: Error) -> Result<ConsumerRecords<String, String>, Error> {
        let mut h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let first = tp("topic-a", 0);
        let second = tp("topic-a", 1);

        // Both partitions assigned and seeked: `assign_and_seek` REPLACES the
        // assignment, so it cannot be used twice here.
        {
            let mut guard = h.subs.lock().expect("lock");
            let set: HashSet<TopicPartition> = [first.clone(), second.clone()].into_iter().collect();
            guard.assign_from_user(set).unwrap();
            guard.seek(&first, 0).unwrap();
            guard.seek(&second, 0).unwrap();
        }

        // Decline the first `initialize`, fail the second. `initialize` runs
        // exactly once per CompletedFetch (an already-initialized one takes
        // the `else` branch in `collect_fetch`), so the call count selects
        // the partition.
        let calls = std::sync::atomic::AtomicUsize::new(0);
        h.collector.set_force_initialize_error(move || {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                None
            } else {
                Some(error.clone())
            }
        });

        h.fetch_buffer.add(build_completed_fetch(&h, first, 0, 5, None));
        h.fetch_buffer.add(build_completed_fetch(&h, second, 0, 5, None));

        h.collector.collect_fetch(&h.fetch_buffer)
    }

    /// A generic error escapes Java's `catch (KafkaException e)` even with
    /// records in hand. Fails if the escape clause narrows back to matching
    /// `IllegalState` alone — `IllegalArgument` would then be swallowed and
    /// `collect_fetch` would return the first partition's records instead.
    #[test]
    fn deferred_generic_error_propagates_even_with_records_collected() {
        let err = run_deferred_error_with_records_in_hand(Error::illegal_argument("simulated generic error"))
            .expect_err("a generic error must escape the catch even with records in hand");
        assert!(
            matches!(err, Error::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert_eq!(err.message(), "simulated generic error");

        // The other two generic variants take the same path.
        for generic in [
            Error::illegal_state("simulated illegal state"),
            Error::concurrent_modification("simulated concurrent modification"),
        ] {
            let expected = generic.message().to_string();
            let err = run_deferred_error_with_records_in_hand(generic)
                .expect_err("every generic error must escape the catch");
            assert_eq!(err.message(), expected);
        }
    }

    /// The complement: a Kafka error IS caught, so the records collected
    /// before it are returned. Fails if the escape clause widens to
    /// propagate everything.
    #[test]
    fn deferred_kafka_error_is_swallowed_when_records_collected() {
        let records = run_deferred_error_with_records_in_hand(Error::with_message(
            Errors::UnknownServerError,
            "simulated kafka error",
        ))
        .expect("a Kafka error must be swallowed when records are in hand");
        assert_eq!(5, records.count(), "the first partition's records must still be returned");
    }

    /// The record-level corruption errors `CompletedFetch` raises must be
    /// `KafkaException`s, so this swallow guard applies to them.
    ///
    /// Java's `maybeEnsureValid` throws a bare `KafkaException`
    /// (`CompletedFetch.java:157-162`) and the batch iterator throws
    /// `InvalidRecordException` (`DefaultRecordBatch.java:307,607`) — both inside
    /// `KafkaException`, so `collectFetch` returns the other partitions' records
    /// and re-reads the bad one next poll.
    ///
    /// These were once built with `Error::illegal_state`, which answers `false` to
    /// `is_kafka_error()` and so took the generic-escape branch Java reserves for
    /// `java.lang` programming errors: the poll returned `Err` and the
    /// already-decoded records were dropped *after* their fetch position had been
    /// advanced — silent data loss. This test fails if any of those sites regresses
    /// to a non-`KafkaException` class.
    #[test]
    fn deferred_record_corruption_errors_are_swallowed_when_records_collected() {
        let corruption_errors = [
            // `maybeEnsureValid(batch)` / decompression — a *bare* KafkaException,
            // which is a sibling of `ApiException`, not a subclass.
            Error::kafka("Record batch for partition topic-a-1 at offset 0 is invalid, cause: crc mismatch"),
            // premature EOF / records remaining / invalid headers — InvalidRecordException.
            Error::InvalidRecord(crate::common::InvalidRecordError::new(
                "Incorrect declared batch size for partition topic-a-1, premature EOF reached",
            )),
        ];

        for error in corruption_errors {
            assert!(
                error.is_kafka_error(),
                "a record-corruption error must be a KafkaException: {error:?}"
            );
            assert!(
                !error.is_api_error() || matches!(error, Error::InvalidRecord(_)),
                "a bare KafkaException is not an ApiException (Java: `ApiException extends KafkaException`): {error:?}"
            );
            let records = run_deferred_error_with_records_in_hand(error.clone())
                .unwrap_or_else(|e| panic!("{error:?} must be swallowed with records in hand, got {e:?}"));
            assert_eq!(
                5,
                records.count(),
                "the healthy partition's records must still be delivered for {error:?}"
            );
        }
    }

    // ── update_partition_state short-circuit branches ──────────────────────
    //
    // Translated from the FetchCollectorTest "OnNotAssignedPartition" family
    // (`testCollectFetchInitializationWithUpdate{HighWatermark,LogStartOffset,
    // LastStableOffset,PreferredReplica}OnNotAssignedPartition`). Java mocks
    // each `tryUpdating*` to return false in isolation. In real Rust
    // `SubscriptionState`, `try_updating_*` returns false IFF the partition is
    // not assigned (`assigned_state_or_null_mut → None`). We therefore drive
    // `update_partition_state` with an UNASSIGNED partition and set ONLY the
    // target field non-negative (others stay negative ⇒ their branch is
    // skipped), so the targeted `try_updating_*` is the one that returns
    // false. Mutation-resistant: deleting the targeted short-circuit branch
    // makes `update_partition_state` fall through to the remaining
    // (skipped) branches and return `true`, failing the assertion.
    //
    // The full-collector wrapper (`collect_fetch`) reaches
    // `update_partition_state` only after `initialize` passes the
    // `has_valid_position` guard; with an unassigned partition that guard is
    // false, so the collector exits empty (the same observable empty-fetch
    // behavior the Java tests assert, covered by
    // `test_collect_fetch_not_assigned_partition_yields_nothing`). The
    // per-branch assertions below pin the individual short-circuits that the
    // Java mocks target.

    /// `...UpdateHighWatermarkOnNotAssignedPartition`: only the
    /// high-watermark branch runs (default `high_watermark = 0 >= 0`; other
    /// fields default negative), and it returns false for the unassigned
    /// partition.
    #[test]
    fn test_update_partition_state_high_watermark_not_assigned() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        // Intentionally NOT assigned.
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(1000);
        // log_start_offset / last_stable_offset / preferred default negative.
        assert!(
            !h.collector.update_partition_state(&partition_data, &partition),
            "high-watermark update must fail for an unassigned partition"
        );
    }

    /// `...UpdateLogStartOffsetOnNotAssignedPartition`: high-watermark branch
    /// skipped (`-1`), only the log-start-offset branch runs and fails.
    #[test]
    fn test_update_partition_state_log_start_offset_not_assigned() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(-1);
        partition_data.set_log_start_offset(10);
        // last_stable_offset / preferred default negative.
        assert!(
            !h.collector.update_partition_state(&partition_data, &partition),
            "log-start-offset update must fail for an unassigned partition"
        );
    }

    /// `...UpdateLastStableOffsetOnNotAssignedPartition`: high-watermark and
    /// log-start-offset branches skipped, only the last-stable-offset branch
    /// runs and fails.
    #[test]
    fn test_update_partition_state_last_stable_offset_not_assigned() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(-1);
        partition_data.set_log_start_offset(-1);
        partition_data.set_last_stable_offset(900);
        assert!(
            !h.collector.update_partition_state(&partition_data, &partition),
            "last-stable-offset update must fail for an unassigned partition"
        );
    }

    /// `...UpdatePreferredReplicaOnNotAssignedPartition`: all watermark
    /// branches skipped, only the preferred-read-replica branch runs and
    /// fails.
    #[test]
    fn test_update_partition_state_preferred_replica_not_assigned() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(-1);
        partition_data.set_log_start_offset(-1);
        partition_data.set_last_stable_offset(-1);
        partition_data.set_preferred_read_replica(21);
        assert!(
            !h.collector.update_partition_state(&partition_data, &partition),
            "preferred-replica update must fail for an unassigned partition"
        );
    }

    /// Collector-level companion to the OnNotAssignedPartition family and
    /// `testCollectFetchInitializationWithNullPosition`: a CompletedFetch for
    /// an unassigned (or unseeked) partition yields an empty fetch and clears
    /// the next-in-line slot. Java forces `hasValidPosition→true` via a mock
    /// to push execution into `updatePartitionState`; in real state the
    /// `has_valid_position` guard short-circuits `initialize` to the same
    /// observable result (empty fetch, next-in-line cleared).
    #[test]
    fn test_collect_fetch_not_assigned_partition_yields_nothing() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        // Assign WITHOUT seek: partition initialized, no valid position
        // (equivalent to Java's `positionOrNull → null` after the
        // has-valid-position guard).
        {
            let mut guard = h.subs.lock().expect("lock");
            let mut set: HashSet<TopicPartition> = HashSet::new();
            set.insert(partition.clone());
            guard.assign_from_user(set).unwrap();
        }

        let cf = build_completed_fetch(&h, partition.clone(), 0, DEFAULT_RECORD_COUNT, None);
        h.fetch_buffer.add(cf);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();

        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
        // The CompletedFetch was consumed off the queue and not re-installed
        // as next-in-line (Java: verify(fetchBuffer).setNextInLineFetch(null)).
        assert!(!h.fetch_buffer.has_next_in_line_fetch());
    }

    /// Translated from
    /// `FetchCollectorTest.testCollectFetchInitializationOffsetOutOfRangeErrorWithNullPosition`.
    ///
    /// Java mocks `hasValidPosition→true, positionOrNull→null` to reach the
    /// OFFSET_OUT_OF_RANGE handler with a null position, which discards the
    /// fetch as stale WITHOUT requesting a reset. In real Rust state the null
    /// position is only reachable inside `handle_offset_out_of_range` (the
    /// `position_or_null → None` arm), so we drive that arm directly: an
    /// unassigned partition has no position, and the OOR handler must return
    /// the fetch back (discarded) without requesting a reset.
    #[test]
    fn test_collect_fetch_oor_null_position_yields_nothing() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic", 0);
        // NOT assigned ⇒ position_or_null returns None inside the handler.
        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::OffsetOutOfRange));

        // The null-position arm discards the fetch (returns it unchanged) and
        // does NOT request a reset.
        let result = h.collector.handle_offset_out_of_range(cf, 0);
        assert!(result.is_ok(), "null-position OOR must discard (not error)");
        // No reset was requested: the partition isn't even assigned.
        let guard = h.subs.lock().expect("lock");
        assert!(
            guard.is_offset_reset_needed(&partition).is_err(),
            "unassigned partition cannot have a reset requested"
        );
    }

    /// Translated from
    /// `FetchCollectorTest.testCollectFetchInitializationOffsetOutOfRangeErrorWithOffsetReset`.
    ///
    /// Asserts that an OFFSET_OUT_OF_RANGE error with a matching position and
    /// a default reset policy causes `request_offset_reset_if_assigned` to be
    /// invoked (Java: `verify(subscriptions).requestOffsetResetIfPartitionAssigned`).
    /// Driven end-to-end through `collect_fetch`.
    #[test]
    fn test_collect_fetch_oor_offset_reset_requests_reset() {
        // build_harness uses AutoOffsetResetStrategy::LATEST ⇒ default reset
        // policy is set (has_default_offset_reset_policy == true).
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        // Assign + seek so the position matches the fetch offset and the OOR
        // handler reaches the reset arm.
        assign_and_seek(&h, &partition);
        {
            // No reset requested yet.
            let guard = h.subs.lock().expect("lock");
            assert!(!guard.is_offset_reset_needed(&partition).unwrap());
        }

        // fetch_offset 0 matches the seeked position 0.
        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::OffsetOutOfRange));
        h.fetch_buffer.add(cf);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();

        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
        // The reset was requested for the partition.
        let guard = h.subs.lock().expect("lock");
        assert!(
            guard.is_offset_reset_needed(&partition).unwrap(),
            "expected an offset reset to be requested after OOR with default reset policy"
        );
    }

    // ── testReadCommittedWithAbortedTransaction ────────────────────────────

    /// Builds a transactional v2 data batch at `base_offset` holding `count`
    /// records, with producer id `producer_id` and `partition_leader_epoch`
    /// (`plep`) set so `last_epoch()` reports it.
    fn txn_data_batch(base_offset: i64, count: i32, producer_id: i64, plep: i32) -> Vec<u8> {
        use crate::common::compress::Compression;
        use crate::common::record::{MemoryRecords, RecordBatch, TimestampType};
        let mut builder = MemoryRecords::builder_full(
            512,
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            base_offset,
            -1,          // log_append_time
            producer_id, // producer_id
            0,           // producer_epoch
            0,           // base_sequence
            true,        // is_transactional
            false,       // is_control_batch
            plep,        // partition_leader_epoch
            512,
        );
        for i in 0..count {
            let offset = base_offset + i as i64;
            let value = format!("value-{offset}");
            builder.append_with_offset_bytes(offset, 0, Some(b"key"), Some(value.as_bytes()));
        }
        builder.build().buffer().to_vec()
    }

    /// Builds a CompletedFetch declaring an aborted transaction for
    /// `producer_id` at `first_offset`, carrying `records_bytes`.
    fn build_completed_fetch_with_aborted_txn(
        h: &Harness,
        partition: TopicPartition,
        fetch_offset: i64,
        records_bytes: Vec<u8>,
        producer_id: i64,
        first_offset: i64,
    ) -> CompletedFetch {
        use crate::fetch_response_data::AbortedTransaction;
        let mut txn = AbortedTransaction::new();
        txn.set_producer_id(producer_id);
        txn.set_first_offset(first_offset);
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(partition.partition());
        partition_data.set_high_watermark(1000);
        partition_data.set_records(Some(bytes::Bytes::from(records_bytes)));
        partition_data.set_aborted_transactions(Some(vec![txn]));
        let aggregator = agg_for(&partition);
        CompletedFetch::new_full(
            h.subs.clone(),
            Arc::new(crate::common::memory::buffer_supplier::BufferSupplier::create()),
            partition,
            partition_data,
            aggregator,
            fetch_offset,
        )
    }

    /// Translated from `FetchCollectorTest.testReadCommittedWithAbortedTransaction`.
    ///
    /// Under READ_COMMITTED a fully-aborted transactional batch yields ZERO
    /// records but the fetch is non-empty: `next_offsets` advances past the
    /// aborted batch (offset-advance-with-zero-records). A subsequent
    /// committed (non-aborted) batch returns all its records and advances the
    /// offset further. The leader epoch on the advanced offset is `Some(0)`
    /// (the batch's partition leader epoch).
    ///
    /// # Deviation from Java's control-marker layout (documented)
    ///
    /// Java's fixture appends an `EndTransactionMarker` control batch after the
    /// aborted data batch, so its `nextOffset` is `recordCount + 1` and Java's
    /// `containsAbortMarker` logic removes the producer from the aborted set on
    /// observing the marker. This port uses PLAIN data batches instead (no control
    /// markers). That was forced until Milestone 11 Phase 8, when
    /// `ControlRecordType` and `CompletedFetch::contains_abort_marker` landed; it is
    /// now a fixture gap rather than a production one (PLAN §9.26). The aborted
    /// batch spans exactly
    /// `recordCount` offsets (so `nextOffset = recordCount`), and the second
    /// (committed) batch uses a DIFFERENT producer id so the
    /// `containsAbortMarker`-gap does not apply. The collector contract under
    /// test — zero records but offset advances past an all-aborted batch, then
    /// records returned + offset advanced for a committed batch — is identical;
    /// only the literal offset values differ because no control marker occupies
    /// an offset slot.
    #[test]
    fn test_read_committed_with_aborted_transaction() {
        const ABORTED_PRODUCER_ID: i64 = 100;
        const COMMITTED_PRODUCER_ID: i64 = 200;
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadCommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let record_count = 20;
        // First CompletedFetch: a fully-aborted transactional data batch,
        // offsets 0..=19 (nextOffset = 20).
        let buf = txn_data_batch(0, record_count, ABORTED_PRODUCER_ID, 0);
        let cf1 = build_completed_fetch_with_aborted_txn(&h, partition.clone(), 0, buf, ABORTED_PRODUCER_ID, 0);
        h.fetch_buffer.add(cf1);

        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();
        // No data records, but the offset moved forward past the aborted batch.
        // The public `is_empty()` (records-only) IS true here (Java's public
        // ConsumerRecords.isEmpty), but the internal `Fetch.isEmpty()`
        // (`is_fetch_empty`, which drives the poll loop) must be FALSE because
        // the position advanced — otherwise poll() would block until timeout.
        assert!(fetch.is_empty(), "no data records ⇒ public is_empty() true");
        assert!(
            !fetch.is_fetch_empty(),
            "Fetch.isEmpty() must be false when the position advanced with zero records"
        );
        assert_eq!(0, fetch.count(), "all records aborted ⇒ zero records");
        assert_eq!(1, fetch.next_offsets().len());
        let expected = OffsetAndMetadata::with_leader_epoch(record_count as i64, Some(0), "").unwrap();
        assert_eq!(&expected, fetch.next_offsets().get(&partition).unwrap());

        // Second CompletedFetch: a committed (non-aborted) transactional data
        // batch from a different producer, offsets 20..=39 (nextOffset = 40).
        // The records ARE returned and the offset advances.
        let start_offset = record_count; // 20 — matches the position after fetch 1
        let buf2 = txn_data_batch(start_offset as i64, record_count, COMMITTED_PRODUCER_ID, 0);
        // No aborted transaction declared for COMMITTED_PRODUCER_ID, so its
        // records are returned. (We still pass an aborted-txn entry for the
        // unrelated ABORTED_PRODUCER_ID, mirroring the response carrying a
        // stale aborted-txn list; it does not match this batch's producer.)
        let cf2 = build_completed_fetch_with_aborted_txn(
            &h,
            partition.clone(),
            start_offset as i64,
            buf2,
            ABORTED_PRODUCER_ID,
            0,
        );
        h.fetch_buffer.add(cf2);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();

        assert!(!fetch.is_empty());
        assert_eq!(record_count as usize, fetch.count(), "committed data records returned");
        assert_eq!(1, fetch.next_offsets().len());
        let expected2 =
            OffsetAndMetadata::with_leader_epoch((start_offset + record_count) as i64, Some(0), "").unwrap();
        assert_eq!(&expected2, fetch.next_offsets().get(&partition).unwrap());
    }

    /// Translated from `FetchCollectorTest.testFetchWithReadReplica`.
    #[test]
    fn test_fetch_with_read_replica() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        // Set the preferred read replica.
        {
            let mut guard = h.subs.lock().expect("lock");
            guard.update_preferred_read_replica(&partition, 67, 1_000).unwrap();
            assert_eq!(Some(67), guard.preferred_read_replica(&partition, 1_000));
        }

        let cf = build_completed_fetch(&h, partition.clone(), 0, DEFAULT_RECORD_COUNT, None);
        h.fetch_buffer.add(cf);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();

        assert_eq!(DEFAULT_RECORD_COUNT as usize, fetch.count());
        let mut guard = h.subs.lock().expect("lock");
        // Preferred replica is still set (no error happened).
        assert_eq!(Some(67), guard.preferred_read_replica(&partition, 1_000));
    }

    /// Translated from `FetchCollectorTest.testFetchWithOffsetOutOfRangeWithPreferredReadReplica`.
    #[test]
    fn test_fetch_with_offset_out_of_range_with_preferred_replica() {
        let h = build_harness(10, IsolationLevel::ReadUncommitted);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        // Set the preferred read replica.
        {
            let mut guard = h.subs.lock().expect("lock");
            guard.update_preferred_read_replica(&partition, 67, 1_000).unwrap();
            assert_eq!(Some(67), guard.preferred_read_replica(&partition, 1_000));
        }

        let cf = build_completed_fetch(&h, partition.clone(), 0, 0, Some(Errors::OffsetOutOfRange));
        h.fetch_buffer.add(cf);
        let fetch = h.collector.collect_fetch(&h.fetch_buffer).unwrap();

        // Fetch is empty, preferred replica is cleared.
        assert!(fetch.is_empty());
        assert_eq!(0, fetch.next_offsets().len());
        let mut guard = h.subs.lock().expect("lock");
        assert_eq!(None, guard.preferred_read_replica(&partition, 1_000));
    }

    /// Smoke-test for the time source plumbing — preferred-read-replica
    /// expiry timestamp is `time.milliseconds() + metadata_expire_ms()`.
    #[test]
    fn test_update_partition_state_uses_time_source() {
        let h = build_harness(DEFAULT_MAX_POLL_RECORDS, IsolationLevel::ReadUncommitted);
        h.time.now.store(42, std::sync::atomic::Ordering::SeqCst);
        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);

        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(0);
        partition_data.set_high_watermark(1000);
        partition_data.set_records(Some(bytes::Bytes::from(make_records(0, 3))));
        partition_data.set_preferred_read_replica(99);

        let updated = h.collector.update_partition_state(&partition_data, &partition);
        assert!(updated);
        let mut guard = h.subs.lock().expect("lock");
        // Replica updated; the lease window expires at the time we
        // configured (42 + metadata_expire_ms).
        assert_eq!(Some(99), guard.preferred_read_replica(&partition, 42));
    }

    /// Use Node here to silence the dead-import warning while preserving
    /// the import for future test additions (Node is needed to exercise
    /// preferred-replica resolution against a real Cluster).
    #[test]
    fn test_node_import_smoke() {
        let _ = Node::new(1, "host".to_string(), 9092);
        let _ = LeaderAndEpoch::no_leader_or_epoch();
    }

    // ── §27 per-record allocation-budget regression test ───────────────────
    //
    // Per `consumer-threading.md` §27, the receive path must stay
    // zero-copy: per-record allocations must match the user-supplied
    // deserializer budget. Specifically, NO topic-name `String`
    // allocations, NO `Vec<u8>` clones of fetch-buffer bytes, NO
    // per-record `DefaultRecord` clones, NO `tokio::spawn`.
    //
    // The test wraps `collect_fetch` in a thread-local
    // [`crate::test_alloc_tracker::AllocTrackingGuard`] and asserts
    // that the per-record allocation count is bounded by the
    // user-deserializer budget. Specifically:
    //
    //   per_record_allocs <= 4
    //
    // Budget breakdown (for `String` key + value deserializer):
    //   - 1 × `Vec<u8>::to_vec` inside `StringDeserializer::deserialize` for key
    //   - 1 × `String::from_utf8` (zero-allocation when valid UTF-8, but
    //     the `Vec<u8>` it owns came from `to_vec` above; counted)
    //   - 1 × `Vec<u8>::to_vec` for value
    //   - 1 × `String::from_utf8` for value
    //   - 1 × `RecordHeaders::from_slice` (Vec backing the headers list,
    //     which may or may not allocate depending on input size)
    //   - 1 × `ConsumerRecord` push to the per-partition `Vec`
    //     (amortized; only counts on grow)
    //
    // The exact count depends on the deserializer impl and how often
    // the per-partition Vec grows. We assert a generous upper bound of
    // 8 per record to leave room for the amortized push without
    // brittleness. The CRUCIAL contract is that there is NO O(N) topic
    // name allocation or fetch-buffer clone — that would push the
    // count to ~3*N higher.

    struct StringDeserializerForBudget;
    impl Deserializer<String> for StringDeserializerForBudget {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
            // Same as `StringDeserializer` above; named differently so
            // the budget test can assert behavior independently from
            // the other tests in this module.
            String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(e.to_string()))
        }
    }

    /// §27 per-record allocation-budget regression test.
    ///
    /// Asserts that `FetchCollector::collect_fetch` per-record
    /// allocations stay within the user-deserializer budget. If a
    /// regression introduces topic-name `String` allocation,
    /// `Vec<u8>::clone` of fetch-buffer bytes, or per-record
    /// `DefaultRecord` deep-clone, this test fails loudly.
    ///
    /// Current measurement (Phase 16, after the §27 zero-copy decode):
    /// ~2.2 allocs/record — exactly the user deserializer's key + value
    /// `String::from_utf8`, with NO copy of the raw key/value bytes out of
    /// the fetch buffer. (Before Phase 16 this was ~4.2 allocs/record,
    /// because `DefaultRecord` owned `Vec<u8>` key/value and was decoded
    /// eagerly per batch — two extra `to_vec()` copies per record.) The
    /// budget is set just above the measured value to lock in the win while
    /// absorbing small std-library / Vec-growth variation, and to flag any
    /// regression that re-introduces a per-record byte copy.
    #[test]
    fn test_collect_fetch_per_record_allocation_budget() {
        const RECORD_COUNT: i32 = 100;
        // Empirical baseline is ~2.2 allocs/record (key + value
        // String::from_utf8 — the user deserializer's `T` output, which is
        // the ONLY allocation §27 permits per record). Budget at 4 catches
        // a regression that re-adds a per-record byte copy (key `to_vec`,
        // value `to_vec`, topic `String::from`, or `DefaultRecord` clone),
        // each of which would push this toward the pre-Phase-16 ~4.2.
        const ALLOC_BUDGET_PER_RECORD: usize = 4;
        // Top-level overhead budget (one-time allocations: IndexMap,
        // HashMap, CompletedFetch::ensure_cursor's one-time MemoryRecords
        // setup, ConsumerRecords construction, etc.). Empirically ~22
        // for this fixture; 100 leaves headroom.
        const OVERHEAD_BUDGET: usize = 100;

        // Build the collector + partition + records OUTSIDE the
        // tracking window so setup allocations don't count.
        let max_poll_records = RECORD_COUNT;
        let h = build_harness(max_poll_records, IsolationLevel::ReadUncommitted);

        // Replace the deserializers with the budget-focused string
        // deserializer (same behavior, different type so the cost is
        // attributed to the regression test).
        let deserializers: Arc<Deserializers<String, String>> = Arc::new(Deserializers::new(
            Box::new(StringDeserializerForBudget),
            Box::new(StringDeserializerForBudget),
        ));
        let collector = FetchCollector::new(
            h.metadata.clone(),
            h.subs.clone(),
            h.fetch_config.clone(),
            deserializers,
            FetchMetricsManager::for_test(),
            h.time.clone(),
        );

        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);
        let cf = build_completed_fetch(&h, partition.clone(), 0, RECORD_COUNT, None);
        h.fetch_buffer.add(cf);

        let alloc_count;
        let fetch_count;
        {
            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            // Reset just before the measured call to drop any setup
            // allocations that happened inside `new()`.
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            let fetch = collector.collect_fetch(&h.fetch_buffer).unwrap();
            alloc_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
            fetch_count = fetch.count();
        }

        assert_eq!(
            RECORD_COUNT as usize, fetch_count,
            "collect_fetch did not return the expected number of records"
        );

        // The budget is `OVERHEAD + per_record * RECORD_COUNT`. If a
        // regression introduces a fetch-buffer clone, topic-name
        // String allocation per record, or per-record DefaultRecord
        // clone, this will exceed the budget.
        let max_allowed = OVERHEAD_BUDGET + ALLOC_BUDGET_PER_RECORD * (RECORD_COUNT as usize);
        assert!(
            alloc_count <= max_allowed,
            "Per-record allocation regression: {alloc_count} allocs for {RECORD_COUNT} records \
             (budget: {max_allowed} = {OVERHEAD_BUDGET} overhead + {ALLOC_BUDGET_PER_RECORD}/record). \
             Likely cause: a new `String::from_utf8`, `Vec<u8>::clone`, or `DefaultRecord::clone` \
             entered the per-record path (consumer-threading.md §27)."
        );

        // Lower bound: there MUST be at least the deserializer cost
        // per record. If this drops to 0, our tracker is misconfigured.
        assert!(
            alloc_count >= RECORD_COUNT as usize,
            "Expected at least 1 allocation per record (key + value deserializer); \
             got {alloc_count} — tracker likely misconfigured"
        );

        // Visible signal for the Critic that the test was actually
        // exercised. If the count is suspiciously low (e.g. the
        // deserializer was elided) the lower bound above catches it.
        eprintln!(
            "§27 allocation budget: {alloc_count} allocs for {RECORD_COUNT} records \
             (avg {avg:.2}/record, max allowed {max_allowed})",
            avg = alloc_count as f64 / RECORD_COUNT as f64,
        );
    }

    /// §27 zero-copy per-record allocation-budget test for the
    /// [`BytesDeserializer`] (Phase 1).
    ///
    /// With `BytesDeserializer`, each record's key and value are handed out as
    /// refcounted `Bytes::slice_ref` slices of the single owning fetch buffer —
    /// NO per-record key/value copy. This locks in that win: the per-record
    /// budget here must be strictly below the `String`/`ByteArray` deserializer
    /// budget (which copies both key and value via `to_vec`), and there must be
    /// no `Vec<u8>::clone` of the fetch buffer.
    ///
    /// The only per-record allocations that remain are the §27-sanctioned ones
    /// that are NOT key/value byte copies: the owned `RecordHeaders` and the
    /// `ConsumerRecord` push onto the per-partition `Vec` (amortized). A
    /// regression that re-introduces a per-record key/value copy (e.g. routing
    /// `BytesDeserializer` through the copying `deserialize` instead of
    /// `deserialize_from_shared`) would push the count up and fail this test.
    #[test]
    fn test_collect_fetch_bytes_deserializer_zero_copy_budget() {
        use crate::common::serialization::BytesDeserializer;

        const RECORD_COUNT: i32 = 100;
        // Each record carries a 1-byte key and a small value (see
        // `make_records`). The copying ByteArray/String path costs ~2.2
        // allocs/record (key copy + value copy). The zero-copy Bytes path
        // must stay at or below 2/record (headers Vec + amortized
        // ConsumerRecord push), with NO key/value byte copy.
        const ALLOC_BUDGET_PER_RECORD: usize = 2;
        const OVERHEAD_BUDGET: usize = 100;

        let max_poll_records = RECORD_COUNT;
        let h = build_harness(max_poll_records, IsolationLevel::ReadUncommitted);

        let deserializers: Arc<Deserializers<bytes::Bytes, bytes::Bytes>> =
            Arc::new(Deserializers::new(Box::new(BytesDeserializer), Box::new(BytesDeserializer)));
        let collector = FetchCollector::new(
            h.metadata.clone(),
            h.subs.clone(),
            h.fetch_config.clone(),
            deserializers,
            FetchMetricsManager::for_test(),
            h.time.clone(),
        );

        let partition = tp("topic-a", 0);
        assign_and_seek(&h, &partition);
        let cf = build_completed_fetch(&h, partition.clone(), 0, RECORD_COUNT, None);
        h.fetch_buffer.add(cf);

        let alloc_count;
        let fetch_count;
        {
            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            let fetch = collector.collect_fetch(&h.fetch_buffer).unwrap();
            alloc_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
            fetch_count = fetch.count();
        }

        assert_eq!(
            RECORD_COUNT as usize, fetch_count,
            "collect_fetch did not return the expected number of records"
        );

        let max_allowed = OVERHEAD_BUDGET + ALLOC_BUDGET_PER_RECORD * (RECORD_COUNT as usize);
        assert!(
            alloc_count <= max_allowed,
            "BytesDeserializer per-record allocation regression: {alloc_count} allocs for \
             {RECORD_COUNT} records (budget: {max_allowed} = {OVERHEAD_BUDGET} overhead + \
             {ALLOC_BUDGET_PER_RECORD}/record). Likely cause: a per-record key/value byte copy \
             re-entered the path — `BytesDeserializer` must slice the shared buffer via \
             `deserialize_from_shared` (consumer-threading.md §27)."
        );

        eprintln!(
            "§27 Bytes zero-copy budget: {alloc_count} allocs for {RECORD_COUNT} records \
             (avg {avg:.2}/record, max allowed {max_allowed})",
            avg = alloc_count as f64 / RECORD_COUNT as f64,
        );
    }
}
