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

//! Translation of `org.apache.kafka.clients.producer.internals.ProducerBatch`.
//!
//! A batch of records that is or will be sent. Wraps a
//! [`MemoryRecordsBuilder`] (Phase 3) plus the per-batch produce-future,
//! callbacks (`Thunk`s), retry counters, and the leader-epoch tracking
//! used by `RecordAccumulator` and `Sender`.
//!
//! ## Thread-safety model
//!
//! Java's contract: "This class is not thread safe and external
//! synchronization must be used when modifying it." The `RecordAccumulator`
//! holds the per-partition deque mutex while calling `try_append`,
//! `is_full`, `close*`, `split` etc.
//!
//! However, the batch's *finalization* (`done`, `abort`,
//! `complete_future_and_fire_callbacks`) is invoked from the sender task
//! and must be visible to a concurrent waiter on `produce_future`. We
//! mirror Java by:
//! - Using `OnceLock<FinalState>` for the once-only state transition
//!   (Java's `AtomicReference<FinalState>` with CAS-once semantics).
//! - Storing the `Thunk` list and the per-batch mutable fields
//!   (`record_count`, `max_record_size`, `last_append_time`, `retry`,
//!   `inflight`, etc.) inside a small `Mutex<MutState>` so that
//!   sender-task `done()` can fire callbacks/futures without taking a
//!   `&mut self` borrow.
//! - The per-`Mutex` critical sections are short and synchronous — they
//!   never cross an `.await` (CLAUDE.md rule 9.6).
//!
//! ## Hot-path constraints (CLAUDE.md rule 12)
//!
//! `try_append` writes serialized bytes directly into the underlying
//! [`MemoryRecordsBuilder`] via `MemoryRecordsBuilder::append`, which
//! streams through the codec (or directly into the buffer for
//! uncompressed batches). No per-record intermediate `Vec<u8>` is
//! allocated. The split path also reuses the original record bytes via
//! `MemoryRecords::records()` iteration (Java: `record.key()/value()`
//! `ByteBuffer` slices; Rust: `&[u8]` borrowed from the `Bytes` payload).
//!
//! ## Plug-in contract for future transactions (Phase 6 NOTES.md)
//!
//! This milestone does NOT carry idempotent-producer state (sequence,
//! base sequence, producer ID, epoch are accessors that read from
//! `MemoryRecordsBuilder`'s defaults — set/reset is reachable from the
//! split path's `assignProducerStateToBatches` but is a no-op when the
//! batch was constructed with the default `NO_*` sentinels). The hooks
//! that would be filled in when transactions land are noted in the
//! relevant comments.

#![allow(dead_code)] // Phase 6d (RecordAccumulator) / 6e (Sender) wire most accessors and `set_inflight`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI32;

use crate::common::record::{MemoryRecordsBuilder, compression_ratio_estimator};
use crate::common::topic_partition::TopicPartition;
use crate::producer::callback::Callback;

use super::future_record_metadata::FutureRecordMetadata;
use super::produce_request_result::ProduceRequestResult;

/// Mirrors Java's private `enum FinalState { ABORTED, FAILED, SUCCEEDED }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalState {
    Aborted,
    Failed,
    Succeeded,
}

/// A callback and the associated FutureRecordMetadata argument to pass
/// to it. Mirrors Java's private static `Thunk`.
struct Thunk {
    callback: Option<Arc<dyn Callback>>,
    future: Arc<FutureRecordMetadata>,
}

/// Mutable per-batch state guarded by a single `Mutex` so that the
/// finalization path (`done`, `abort`) running on the sender task can
/// fire user callbacks without holding `&mut self`. The mutex is never
/// held across an `.await` (CLAUDE.md rule 9.6).
struct MutState {
    /// Java: `private final List<Thunk> thunks = new ArrayList<>();`
    thunks: Vec<Thunk>,
    /// Java: `int recordCount` (package-private).
    record_count: i32,
    /// Java: `int maxRecordSize` (package-private).
    max_record_size: i32,
    /// Java: `private long lastAttemptMs;`
    last_attempt_ms: i64,
    /// Java: `private long lastAppendTime;`
    last_append_time: i64,
    /// Java: `private long drainedMs;`
    drained_ms: i64,
    /// Java: `private boolean retry;`
    retry: bool,
    /// Java: `private boolean reopened;`
    reopened: bool,
    /// Java: `private boolean bufferDeallocated = false;`
    buffer_deallocated: bool,
    /// Java: `private boolean inflight = false;`
    inflight: bool,
    /// Java: `private OptionalInt currentLeaderEpoch;`
    current_leader_epoch: Option<i32>,
    /// Java: `private int attemptsWhenLeaderLastChanged;`
    attempts_when_leader_last_changed: i32,
    /// Java: `private final MemoryRecordsBuilder recordsBuilder;`
    /// Stored under the same mutex so that `try_append`, `close`, and
    /// `split` can mutate the builder without taking `&mut self`. The
    /// `MemoryRecordsBuilder` itself is `!Send`-safe across awaits, but
    /// every access here is sync.
    records_builder: MemoryRecordsBuilder,
}

/// A batch of records that is or will be sent. See module docs.
pub(crate) struct ProducerBatch {
    /// Java: `final long createdMs;`
    created_ms: i64,
    /// Java: `final TopicPartition topicPartition;`
    topic_partition: TopicPartition,
    /// Java: `final ProduceRequestResult produceFuture;`
    produce_future: Arc<ProduceRequestResult>,
    /// Java: `private final boolean isSplitBatch;`
    is_split_batch: bool,
    /// Java: `private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);`
    final_state: OnceLock<FinalState>,
    /// Java: `private final AtomicInteger attempts = new AtomicInteger(0);`
    attempts: AtomicI32,
    /// See [`MutState`].
    mut_state: Mutex<MutState>,
}

// SAFETY: `MutState` contains a `MemoryRecordsBuilder` which is `!Send`
// + `!Sync` due to its self-referential raw pointer (`append_stream`
// borrowing into `buffer_stream`). `ProducerBatch` mediates access to
// the builder through `Mutex<MutState>` — every mutation flows through
// `mut_state.lock()`, so the raw pointer's invariants (exclusive
// `&mut` borrow during Write) are upheld. The producer/sender
// architecture mirrors Java's `synchronized (deque) { batch.append(...) }`
// contract; the `Mutex` is the Rust equivalent of the external
// synchronization Java's class doc requires
// ("This class is not thread safe and external synchronization must be
// used when modifying it").
unsafe impl Send for ProducerBatch {}
// SAFETY: see Send impl above.
unsafe impl Sync for ProducerBatch {}

impl ProducerBatch {
    /// 3-arg constructor (Java's overload). Defaults `is_split_batch` to false.
    pub fn new(tp: TopicPartition, records_builder: MemoryRecordsBuilder, created_ms: i64) -> Self {
        Self::new_with_split(tp, records_builder, created_ms, false)
    }

    /// 4-arg constructor — mirrors Java's primary constructor.
    pub fn new_with_split(
        tp: TopicPartition,
        mut records_builder: MemoryRecordsBuilder,
        created_ms: i64,
        is_split_batch: bool,
    ) -> Self {
        let produce_future = Arc::new(ProduceRequestResult::new(tp.clone()));
        // Java: `CompressionRatioEstimator.estimation(topicPartition.topic(),
        //                                             recordsBuilder.compression().type())`
        let compression_ratio_estimation =
            compression_ratio_estimator::estimation(tp.topic(), records_builder.compression());
        records_builder.set_estimated_compression_ratio(compression_ratio_estimation);

        ProducerBatch {
            created_ms,
            topic_partition: tp,
            produce_future,
            is_split_batch,
            final_state: OnceLock::new(),
            attempts: AtomicI32::new(0),
            mut_state: Mutex::new(MutState {
                thunks: Vec::new(),
                record_count: 0,
                max_record_size: 0,
                last_attempt_ms: created_ms,
                last_append_time: created_ms,
                drained_ms: 0,
                retry: false,
                reopened: false,
                buffer_deallocated: false,
                inflight: false,
                current_leader_epoch: None,
                attempts_when_leader_last_changed: 0,
                records_builder,
            }),
        }
    }

    /// The shared [`ProduceRequestResult`] this batch produces against.
    /// Mirrors Java's package-private `produceFuture` field accessed by
    /// `IncompleteBatches::requestResults`.
    pub fn produce_future(&self) -> &Arc<ProduceRequestResult> {
        &self.produce_future
    }

    /// The topic-partition this batch targets. Mirrors Java's package-private
    /// `topicPartition` field.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// Creation time of the batch. Mirrors Java's package-private
    /// `createdMs`.
    pub fn created_ms(&self) -> i64 {
        self.created_ms
    }

    /// Java: `boolean isSplitBatch()`.
    pub fn is_split_batch(&self) -> bool {
        self.is_split_batch
    }

    // ----- Methods to be filled in subsequent steps -----
    //
    // try_append, done, complete, complete_exceptionally, abort, split,
    // closeForRecordAppends, close, isFull, isClosed, isExpired,
    // recordCount, maxRecordSize, ... — added incrementally.
}
