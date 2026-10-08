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

//! A [`RecordAccumulator`] variant for the incremental buffer.memory allocation strategy.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator`
//! (Apache Kafka 4.4, KIP-1332 / KAFKA-20578).
//!
//! # Composition, not inheritance (PLAN §2.3)
//!
//! Java's `ChunkedRecordAccumulator extends RecordAccumulator` and overrides `append`,
//! `tryAppend` and `createProducerBatch`. Every other accumulator method — `ready`, `drain`,
//! expiry, `reenqueue`, `deallocate`, `abortIncompleteBatches`, ... — is inherited unchanged, and
//! the `Sender` only ever calls those. So the Rust struct holds the base accumulator as
//! `base: Arc<RecordAccumulator>`, which the `Sender` and `KafkaProducer` share exactly as they do
//! with the full strategy, and adds its own `append` / `try_append`. The precedent is
//! `ConsumerHeartbeatRequestManager { inner: AbstractHeartbeatRequestManager }`. Java's two
//! virtual calls inside the base `appendNewBatch` are that method's step parameters.
//!
//! Java's `TODO: support compressed data (with mid-record growth)` is not left open here: the
//! constructor rejects compression, as Java's does, and `KafkaProducer` rejects the combination
//! with a `ConfigException` before an accumulator is built (PLAN §5).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::common::Cluster;
use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::header::RecordHeader;
use crate::common::metrics::Metrics;
use crate::common::record::TimestampType;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::MemoryRecordsBuilder;
use crate::common::record::internal::RecordBatch;
use crate::common::utils::Time;
use crate::common::utils::internals::ByteBufferOutputStream;
use crate::common::utils::internals::LogContext;
use crate::kafka_trace;
use crate::producer::Callback;
use crate::producer::RecordMetadata;
use crate::producer::internals::BufferPool;
use crate::producer::internals::ChunkedByteBufferOutputStream;
use crate::producer::internals::PartitionerConfig;
use crate::producer::internals::ProducerBatch;
use crate::producer::internals::RecordAccumulator;
use crate::producer::internals::TransactionManager;
use crate::producer::internals::buffer_pool::AllocationMode;
use crate::producer::internals::chunked_producer_batch::ChunkedProducerBatch;
use crate::producer::internals::record_accumulator::{AppendFailure, AppendGuard, RecordAppendResult, TopicInfo};

/// A [`RecordAccumulator`] variant that backs each batch with fixed-size chunks drawn from a
/// [`BufferPool`], attaching more chunks on demand as records are appended instead of reserving
/// `batch.size` per batch up front. Buffered memory therefore scales with the data actually
/// written rather than with `active_partition_count × batch.size`.
///
/// See [`append`](Self::append) and [`try_append`](Self::try_append) for how batches are created
/// and grown.
#[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator")]
pub(crate) struct ChunkedRecordAccumulator {
    /// The accumulator this one extends: Java's `super`. Shared with the `Sender`.
    base: Arc<RecordAccumulator>,
    /// The pool, as the one serving [`AllocationMode::Incremental`] (Java's `chunkedFree`; the same
    /// pool the base accumulator holds as `free`).
    chunked_free: Arc<BufferPool>,
    /// Test seam standing in for the anonymous `BufferPool` / `ChunkedRecordAccumulator`
    /// subclasses of Java's `ChunkedRecordAccumulatorTest`.
    #[cfg(test)]
    test_hooks: Option<Arc<dyn ChunkedAccumulatorTestHooks>>,
}

impl ChunkedRecordAccumulator {
    /// Fixed size of every chunk, independent of `batch.size`. The incremental strategy is only
    /// used when `batch.size >= CHUNK_SIZE` (see `KafkaProducer`); below it a batch is smaller
    /// than a single chunk, so the producer uses the full strategy instead.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#CHUNK_SIZE")]
    pub(crate) const CHUNK_SIZE: i32 = 16 * 1024;

    /// Create a chunked record accumulator. The parameters are those of
    /// [`RecordAccumulator::with_log_context`]; `buffer_pool` must serve
    /// [`AllocationMode::Incremental`] allocation.
    ///
    /// Java's second constructor, without `partitionerConfig`, passes `new PartitionerConfig()`:
    /// pass [`PartitionerConfig::default`].
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalArgument`] if `buffer_pool` serves [`AllocationMode::Full`];
    /// - [`Error::UnsupportedVersion`] (Java's `UnsupportedOperationException`, which has no Rust
    ///   variant; the same mapping `MockAdminClient` uses) if `compression` is not `none`.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#ChunkedRecordAccumulator")]
    pub(crate) fn new(
        batch_size: i32,
        compression: crate::common::compress::Compression,
        linger_ms: i32,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        delivery_timeout_ms: i32,
        partitioner_config: PartitionerConfig,
        metrics: Arc<Metrics>,
        metric_grp_name: &str,
        time: Arc<dyn Time>,
        buffer_pool: Arc<BufferPool>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
        log_context: LogContext,
    ) -> Result<Self, Error> {
        if buffer_pool.allocation_mode() != AllocationMode::Incremental {
            return Err(Error::local_illegal_argument(format!(
                "bufferPool must serve {} allocation, but serves {}",
                AllocationMode::Incremental,
                buffer_pool.allocation_mode()
            )));
        }
        // Java checks compression after `super(..)` has run; checking first keeps a refused
        // accumulator from registering the base's pool gauges. Java's `TODO: drop this once the
        // incremental strategy supports compressed data` is KAFKA-20579.
        if compression.compression_type() != CompressionType::None {
            return Err(Error::unsupported_version(
                "Compression is not yet supported with the incremental buffer.memory allocation strategy",
            ));
        }
        let base = Arc::new(RecordAccumulator::with_log_context(
            batch_size,
            compression,
            linger_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            partitioner_config,
            metrics,
            metric_grp_name,
            time,
            Arc::clone(&buffer_pool),
            transaction_manager,
            log_context,
        ));
        Ok(Self {
            base,
            chunked_free: buffer_pool,
            #[cfg(test)]
            test_hooks: None,
        })
    }

    /// The base accumulator, shared with the `Sender` (Java's `this`, seen as a
    /// `RecordAccumulator`).
    pub(crate) fn base(&self) -> &Arc<RecordAccumulator> {
        &self.base
    }

    /// Add a record to the accumulator, return the append result: [`RecordAccumulator::append`]
    /// for the incremental strategy.
    ///
    /// Each pass of the loop first tries the open batch under the deque lock. If the batch needs
    /// more chunk capacity for the record, the gap is allocated off-lock without blocking and
    /// attached under the lock (the extension path); if there is no open batch that can take the
    /// record, a stream sized for this first record is allocated off-lock, blocking for at most
    /// what is left of `max_time_to_block`, and a new batch is created over it (the new-batch
    /// path).
    ///
    /// # Cancellation (Rust-only)
    ///
    /// Java threads cannot be cancelled, but this future can be dropped at an `.await`
    /// (CLAUDE.md §11.6). The [`AppendGuard`] holds the new-batch stream and the extension chunks
    /// exactly as Java's `finally` sees them, so dropping the future returns both to the pool and
    /// counts the append out, like any other exit.
    ///
    /// # Errors
    ///
    /// As [`RecordAccumulator::append`], handing the callback back unfired.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#append")]
    pub(crate) async fn append(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
    ) -> Result<RecordAppendResult, Box<AppendFailure>> {
        let base = &*self.base;
        let (topic_arc, topic_info) = base.topic_info_for(topic);

        // Java's `appendsInProgress.incrementAndGet()` and the `finally` that returns the
        // `newBatch` stream and the `extensionChunks` and decrements it.
        let mut guard = AppendGuard::new(&self.chunked_free, &base.appends_in_progress);
        self.append_inner(
            &topic_arc,
            partition,
            timestamp,
            key,
            value,
            headers,
            callback,
            max_time_to_block,
            now_ms,
            cluster,
            &topic_info,
            &mut guard,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn append_inner(
        &self,
        topic: &Arc<str>,
        partition: i32,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        max_time_to_block: i64,
        now_ms: i64,
        cluster: &Cluster,
        topic_info: &Arc<TopicInfo>,
        guard: &mut AppendGuard<'_>,
    ) -> Result<RecordAppendResult, Box<AppendFailure>> {
        let base = &*self.base;
        let mut callback = callback;
        let mut now_ms = now_ms;
        let unknown_partition = partition == RecordMetadata::UNKNOWN_PARTITION;
        // `guard.new_batch`: the buffer stream allocated to back a new batch (sized for its first
        // record), paired with that size. Set and cleared together across retries; `None` when
        // none is held. `guard.extension_chunks`: the chunks acquired to extend the open batch.

        // Budget shared by every blocking acquisition this append makes, so the total blocking
        // time stays within max_time_to_block. The full strategy holds its one buffer across
        // retries and so blocks at most once; this loop can release the chunks it acquired (when
        // a concurrent appender created a batch to extend instead) and block again on a later
        // iteration.
        let mut remaining_time_to_block = max_time_to_block;
        loop {
            let effective_partition = if unknown_partition {
                let mut partitioner = topic_info.built_in_partitioner.lock().unwrap();
                partitioner.peek_current_partition_info(cluster).partition()
            } else {
                partition
            };
            // Java's `setPartition(callbacks, effectivePartition)`: see `RecordAccumulator::append`.

            topic_info
                .batches
                .entry(effective_partition)
                .or_insert_with(|| Mutex::new(VecDeque::new()));

            let append_result;
            {
                let dq_ref = topic_info.batches.get(&effective_partition).unwrap();
                let mut deque = dq_ref.lock().unwrap();
                if self.partition_changed(topic_info, unknown_partition, &deque, cluster) {
                    continue;
                }

                // The try_append checks the open batch (deque.back()) for chunk capacity:
                // a needs_buffer_extension result means it is within its batch-size limit
                // but its chunks lack capacity for this record, so it will allocate the gap
                // outside the deque lock. A needs_new_batch result means there is no open batch
                // (full or absent), so it will fall through to the first-record (new batch) path.
                let (result, returned_callback) = self.try_append(
                    timestamp,
                    key,
                    value,
                    headers,
                    callback,
                    &mut deque,
                    topic,
                    effective_partition,
                    now_ms,
                )?;
                if result.appended() {
                    return Ok(base.update_partition_info_on_append(
                        result,
                        topic_info,
                        unknown_partition,
                        &deque,
                        cluster,
                    ));
                }
                callback = returned_callback;
                append_result = result;
            }

            if append_result.needs_buffer_extension() {
                let extension_chunks = self
                    .allocate_extension_chunks(
                        append_result.extension_bytes_needed,
                        topic_info,
                        topic,
                        effective_partition,
                    )
                    .await
                    .map_err(|error| AppendFailure::boxed(error, callback.take()))?;
                let Some(extension_chunks) = extension_chunks else {
                    // Pool exhausted, so no writable batch is left to extend: retry, normally
                    // landing on the blocking new-batch path (needs_new_batch).
                    continue;
                };
                guard.extension_chunks = Some(extension_chunks);
                now_ms = base.time.milliseconds();
            } else if append_result.needs_new_batch() && guard.new_batch.is_none() {
                // The open batch is done (e.g., full, closed) so start a new one. Size it for
                // this first record with the same estimator the full strategy uses
                // (RecordAccumulator::append), but reserve only enough for the record rather than
                // a whole batch.size. (Java's `TODO: review when compression is supported`:
                // KAFKA-20579; compression is rejected at construction.)
                let new_batch_size = AbstractRecords::estimate_size_in_bytes_upper_bound(
                    RecordBatch::CURRENT_MAGIC_VALUE,
                    base.compression.compression_type(),
                    key,
                    value,
                    headers,
                );
                kafka_trace!(
                    base.log_context,
                    "Allocating {} byte chunked buffer ({} byte chunks) for topic {} partition {} with remaining timeout {}ms",
                    new_batch_size,
                    self.chunked_free.poolable_size(),
                    topic,
                    effective_partition,
                    remaining_time_to_block
                );
                let allocation_start_ms = base.time.milliseconds();
                let initial_chunks = self.allocate_chunks(new_batch_size, remaining_time_to_block).await;
                // Java's `finally`: update the remaining time to block.
                now_ms = base.time.milliseconds();
                remaining_time_to_block = (remaining_time_to_block - (now_ms - allocation_start_ms).max(0)).max(0);
                let initial_chunks = match initial_chunks {
                    Ok(initial_chunks) => initial_chunks,
                    Err(error) => {
                        if matches!(error, Error::ProducerBufferExhausted(_)) {
                            // The blocking new-batch acquire was not able to get memory within
                            // what is left of max.block.ms. Record it in the buffer-exhausted
                            // metrics.
                            self.chunked_free.record_buffer_exhausted();
                        }
                        return Err(AppendFailure::boxed(error, callback.take()));
                    },
                };
                let stream = ChunkedByteBufferOutputStream::new(
                    initial_chunks,
                    self.chunked_free.poolable_size(),
                    Some(Arc::clone(&self.chunked_free)),
                )
                .map_err(|error| AppendFailure::boxed(error, callback.take()))?;
                guard.new_batch = Some(NewBatchBuffer { stream, first_append_size: new_batch_size });
            }

            {
                let dq_ref = topic_info.batches.get(&effective_partition).unwrap();
                let mut deque = dq_ref.lock().unwrap();
                if self.partition_changed(topic_info, unknown_partition, &deque, cluster) {
                    // The partition switched while we allocated extension chunks off-lock. They
                    // were sized against the previous partition's open batch, so they must not be
                    // attached to a different partition's open batch — refund them and let the
                    // next iteration re-check the new partition from scratch.
                    self.deallocate_extension_chunks(guard.extension_chunks.take());
                    continue;
                }

                if let Some(mut extension_chunks) = guard.extension_chunks.take() {
                    // The batch may have changed while allocate_chunks was off-lock: drained and
                    // replaced, closed for appends, filled to its limit, or already grown by a
                    // concurrent appender. extension_bytes_needed is 0 whenever attaching would be
                    // wrong, so it serves as both tests at once: the batch still needs chunks for
                    // this record, and it is still open (attaching to a stream closed for appends
                    // would fail). The is_chunked covers the one case it cannot: a replacement
                    // that is a plain ProducerBatch (a split batch), which takes no chunks at all.
                    if let Some(last) = deque.back_mut()
                        && last.is_chunked()
                        && last.extension_bytes_needed(timestamp, key, value, headers) > 0
                    {
                        if let Err(error) = last.add_buffers(&mut extension_chunks) {
                            // `add_buffers` drains only on success: hand the chunks back to the
                            // guard so the `finally` returns them, as Java's caller still holds
                            // its list when `addBuffers` throws.
                            guard.extension_chunks = Some(extension_chunks);
                            return Err(AppendFailure::boxed(error, callback.take()));
                        }
                        let (retry_result, returned_callback) = self.try_append(
                            timestamp,
                            key,
                            value,
                            headers,
                            callback,
                            &mut deque,
                            topic,
                            effective_partition,
                            now_ms,
                        )?;
                        if retry_result.appended() {
                            return Ok(base.update_partition_info_on_append(
                                retry_result,
                                topic_info,
                                unknown_partition,
                                &deque,
                                cluster,
                            ));
                        }
                        callback = returned_callback;
                        // Still not appended: concurrent appenders filled the batch,
                        // so the extension we attached is no longer enough.
                        // Loop so the next iteration routes the record
                        // right: needs_buffer_extension with a fresh gap, or needs_new_batch
                        continue;
                    }
                    // The batch no longer needs these chunks, or cannot take them. Return them to
                    // the pool.
                    self.deallocate_extension_chunks(Some(extension_chunks));
                    continue;
                }

                // needs_new_batch path: no extension chunks here implies needs_new_batch,
                // so the buffer stream was allocated (this iteration or carried from a prior one).
                if guard.new_batch.is_none() {
                    return Err(AppendFailure::boxed(
                        Error::local_illegal_state("needsNewBatch path reached without an allocated buffer stream"),
                        callback.take(),
                    ));
                }
                // Reuse the new-batch size estimate as the write-limit basis. (Java's `TODO:
                // review when compression is supported`: KAFKA-20579.)
                let new_batch = &mut guard.new_batch;
                #[cfg(test)]
                let close_calls = self
                    .test_hooks
                    .as_ref()
                    .and_then(|hooks| hooks.close_for_record_appends_calls());
                let (result, returned_callback) = base.append_new_batch(
                    topic,
                    effective_partition,
                    &mut deque,
                    timestamp,
                    key,
                    value,
                    headers,
                    callback,
                    |deque, callback| {
                        self.try_append(
                            timestamp,
                            key,
                            value,
                            headers,
                            callback,
                            deque,
                            topic,
                            effective_partition,
                            now_ms,
                        )
                    },
                    // The step takes the stream off the guard only when it creates the batch:
                    // Java's `if (appendResult.newBatchCreated) newBatch = null;`.
                    || {
                        let pending = new_batch.take().ok_or_else(|| {
                            Error::local_illegal_state("needsNewBatch path reached without an allocated buffer stream")
                        })?;
                        self.chunked_records_builder(pending.stream, pending.first_append_size)
                    },
                    |tp, records_builder, now_ms| {
                        #[cfg_attr(not(test), expect(unused_mut))]
                        let mut batch = self.create_producer_batch(tp, records_builder, now_ms)?;
                        #[cfg(test)]
                        {
                            batch.close_for_record_appends_calls = close_calls;
                        }
                        Ok(batch)
                    },
                    now_ms,
                )?;
                if result.needs_new_batch() {
                    return Err(AppendFailure::boxed(
                        Error::local_illegal_state("appendNewBatch must not return a needsNewBatch result"),
                        returned_callback,
                    ));
                }
                if result.needs_buffer_extension() {
                    // A concurrent appender created an open batch we should extend rather
                    // than start a new one (detected by append_new_batch's in-lock try_append).
                    // Our buffer stream was sized for a fresh batch — release it and loop so
                    // the extension path allocates exactly the gap-sized chunks.
                    if let Some(mut new_batch) = guard.new_batch.take() {
                        new_batch.stream.deallocate();
                    }
                    callback = returned_callback;
                    continue;
                }
                return Ok(base.update_partition_info_on_append(
                    result,
                    topic_info,
                    unknown_partition,
                    &deque,
                    cluster,
                ));
            }
        }
    }

    /// Mid-batch extension: the open batch can still take this record so grow it in place. The
    /// acquire is non-blocking and fails fast when the pool is exhausted, closing the open batch
    /// for appends if it is still writable so the record retries on the new-batch path.
    ///
    /// Returns the chunks, or `None` if the pool was exhausted.
    ///
    /// # Errors
    ///
    /// Any acquire error other than [`Error::ProducerBufferExhausted`] (a closed pool, a request
    /// beyond the pool's total memory), which Java lets propagate out of `append` too.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#allocateExtensionChunks")]
    async fn allocate_extension_chunks(
        &self,
        extension_bytes_needed: i32,
        topic_info: &TopicInfo,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Vec<Vec<u8>>>, Error> {
        match self.allocate_chunks(extension_bytes_needed, 0).await {
            Ok(chunks) => Ok(Some(chunks)),
            Err(Error::ProducerBufferExhausted(_)) => {
                kafka_trace!(
                    self.base.log_context,
                    "Pool exhausted while extending batch for topic {} partition {}; closing existing batch",
                    topic,
                    partition
                );
                if let Some(dq_ref) = topic_info.batches.get(&partition) {
                    let mut deque = dq_ref.lock().unwrap();
                    // No need to check whether it is still open: close_for_record_appends is
                    // idempotent.
                    if let Some(last) = deque.back_mut() {
                        last.close_for_record_appends();
                    }
                }
                Ok(None)
            },
            Err(error) => Err(error),
        }
    }

    /// Return any held extension chunks to the pool. No-op when none are held.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#deallocateExtensionChunks")]
    fn deallocate_extension_chunks(&self, extension_chunks: Option<Vec<Vec<u8>>>) {
        let Some(extension_chunks) = extension_chunks else {
            return;
        };
        for chunk in extension_chunks {
            self.chunked_free.deallocate(chunk);
        }
    }

    /// Try to append to a ProducerBatch, with mid-batch chunk extension support.
    ///
    /// If the open batch is within its batch-size limit but its chunked stream lacks chunk
    /// capacity, returns [`RecordAppendResult::needs_extension`] without attempting the append;
    /// the caller allocates chunks outside the deque lock, attaches them, and retries. Otherwise
    /// defers to the base [`RecordAccumulator::try_append`], which appends or returns
    /// [`RecordAppendResult::NEEDS_NEW_BATCH`].
    ///
    /// The callback comes back exactly when the record was not appended.
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#tryAppend")]
    pub(crate) fn try_append(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        deque: &mut VecDeque<ProducerBatch>,
        topic: &Arc<str>,
        partition: i32,
        now_ms: i64,
    ) -> Result<(RecordAppendResult, Option<Callback>), Box<AppendFailure>> {
        if self.base.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(RecordAccumulator::closed_while_send_in_progress(callback));
        }
        // Split batches in an incremental deque are plain ProducerBatch (heap-backed,
        // grow-on-demand) and never need chunk extension, so the check only applies to chunked
        // batches.
        if let Some(last) = deque.back()
            && last.is_chunked()
        {
            let extension_bytes = last.extension_bytes_needed(timestamp, key, value, headers);
            if extension_bytes > 0 {
                return Ok((RecordAppendResult::needs_extension(extension_bytes), callback));
            }
        }
        self.base
            .try_append(timestamp, key, value, headers, callback, deque, topic, partition, now_ms)
    }

    /// Create the [`ProducerBatch`] for a new batch: a chunked one.
    ///
    /// # Errors
    ///
    /// As [`ChunkedProducerBatch::new_chunked`].
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#createProducerBatch")]
    fn create_producer_batch(
        &self,
        tp: TopicPartition,
        records_builder: MemoryRecordsBuilder,
        now_ms: i64,
    ) -> Result<ProducerBatch, Error> {
        ChunkedProducerBatch::new_chunked(tp, records_builder, now_ms)
    }

    /// Build a [`MemoryRecordsBuilder`] backed by the chunked stream.
    ///
    /// # Arguments
    ///
    /// * `buffer_stream` - the chunked stream backing the batch
    /// * `first_record_size` - the first record's uncompressed size upper bound. Used to set the
    ///   builder's write limit used by `has_room_for` / `is_full`
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator#chunkedRecordsBuilder")]
    fn chunked_records_builder(
        &self,
        buffer_stream: ChunkedByteBufferOutputStream,
        first_record_size: i32,
    ) -> Result<MemoryRecordsBuilder, Error> {
        let write_limit = self.base.batch_size.max(first_record_size);
        MemoryRecordsBuilder::with_buffer_stream(
            ByteBufferOutputStream::Chunked(buffer_stream),
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.base.compression.clone(),
            TimestampType::CreateTime,
            0,
            RecordBatch::NO_TIMESTAMP,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            write_limit.max(0) as usize,
            RecordBatch::NO_TIMESTAMP,
        )
    }

    /// [`RecordAccumulator::partition_changed`], through the test seam that stands in for
    /// Java's tests overriding `partitionChanged`.
    fn partition_changed(
        &self,
        topic_info: &TopicInfo,
        unknown_partition: bool,
        deque: &VecDeque<ProducerBatch>,
        cluster: &Cluster,
    ) -> bool {
        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks
            && let Some(changed) = hooks.partition_changed(self, topic_info, unknown_partition, cluster)
        {
            return changed;
        }
        self.base.partition_changed(topic_info, unknown_partition, deque, cluster)
    }

    /// The pool acquire behind both paths: Java's `chunkedFree.allocateChunks(totalSize,
    /// maxTimeToBlockMs)`, through the test seam that stands in for Java's tests overriding it.
    async fn allocate_chunks(&self, total_size: i32, max_time_to_block_ms: i64) -> Result<Vec<Vec<u8>>, Error> {
        #[cfg(test)]
        if let Some(hooks) = &self.test_hooks {
            return hooks.allocate_chunks(self, total_size, max_time_to_block_ms).await;
        }
        self.pool_allocate_chunks(total_size, max_time_to_block_ms).await
    }

    /// The real pool acquire. A zero budget is the non-blocking form Java's `allocateChunks(..,
    /// 0L)` reduces to (see `BufferPool::try_allocate_chunks`), which never parks; any other
    /// budget may wait for memory.
    async fn pool_allocate_chunks(&self, total_size: i32, max_time_to_block_ms: i64) -> Result<Vec<Vec<u8>>, Error> {
        if max_time_to_block_ms <= 0 {
            self.chunked_free.try_allocate_chunks(total_size)
        } else {
            self.chunked_free.allocate_chunks(total_size, max_time_to_block_ms).await
        }
    }
}

/// A buffer stream allocated to back a new batch, sized to fit the batch's first record, paired
/// with that size. That same size is also used as the batch's write-limit basis (see
/// `ChunkedRecordAccumulator::chunked_records_builder`). The two are always set and cleared
/// together in `ChunkedRecordAccumulator::append`, including across retries.
#[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulator$NewBatchBuffer")]
pub(crate) struct NewBatchBuffer {
    pub(crate) stream: ChunkedByteBufferOutputStream,
    first_append_size: i32,
}

/// The future a [`ChunkedAccumulatorTestHooks::allocate_chunks`] hook returns (test-only).
#[cfg(test)]
pub(crate) type ChunkAllocation<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Vec<u8>>, Error>> + Send + 'a>>;

/// Test seam for `ChunkedRecordAccumulatorTest`. Java's tests subclass `BufferPool` (overriding
/// `allocateChunks`) and `ChunkedRecordAccumulator` (overriding `partitionChanged` and
/// `createProducerBatch`) anonymously; Rust has neither subclassing nor a virtual pool, so the
/// accumulator consults these hooks at exactly those three points instead. Test-only (DoD #7).
#[cfg(test)]
pub(crate) trait ChunkedAccumulatorTestHooks: Send + Sync {
    /// Java's `allocateChunks` override. The default is the real acquire.
    fn allocate_chunks<'a>(
        &'a self,
        accum: &'a ChunkedRecordAccumulator,
        total_size: i32,
        max_time_to_block_ms: i64,
    ) -> ChunkAllocation<'a> {
        Box::pin(accum.pool_allocate_chunks(total_size, max_time_to_block_ms))
    }

    /// Java's `partitionChanged` override: `Some` answers in its place, `None` defers to the
    /// real check (Java's `super.partitionChanged(..)`).
    fn partition_changed(
        &self,
        _accum: &ChunkedRecordAccumulator,
        _topic_info: &TopicInfo,
        _unknown_partition: bool,
        _cluster: &Cluster,
    ) -> Option<bool> {
        None
    }

    /// Java's `createProducerBatch` override returning a `ChunkedProducerBatch` subclass that
    /// counts `closeForRecordAppends` calls: the counter installed on every batch created.
    fn close_for_record_appends_calls(&self) -> Option<Arc<std::sync::atomic::AtomicI32>> {
        None
    }
}

#[cfg(test)]
impl ChunkedRecordAccumulator {
    /// Installs the test hooks.
    pub(crate) fn set_test_hooks(&mut self, hooks: Arc<dyn ChunkedAccumulatorTestHooks>) {
        self.test_hooks = Some(hooks);
    }

    /// The real pool acquire, for a hook that wraps it (Java's `super.allocateChunks(..)`).
    pub(crate) async fn real_allocate_chunks(
        &self,
        total_size: i32,
        max_time_to_block_ms: i64,
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.pool_allocate_chunks(total_size, max_time_to_block_ms).await
    }
}

/// Translated from `ChunkedRecordAccumulatorTest` (Apache Kafka 4.4, KAFKA-20578).
///
/// Java's tests subclass `BufferPool` and `ChunkedRecordAccumulator` anonymously; here each such
/// subclass is a [`ChunkedAccumulatorTestHooks`] implementation, plus the pool's
/// `deallocate_observer` for the `deallocate` overrides. Java's `AtomicReference<..Accumulator>`
/// handles for nested appends are unnecessary: a hook is handed the accumulator it runs in.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetadataSnapshot;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::protocol::Errors;
    use crate::common::record::internal::MemoryRecords;
    use crate::common::record::internal::Record;
    use crate::common::utils::MockTime;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    const TOPIC: &str = "test";
    const PARTITION1: i32 = 0;
    const MAX_BLOCK_TIME_MS: i64 = 1000;
    const KEY: &[u8] = b"k";

    pub(super) type HookFuture<'a> = ChunkAllocation<'a>;

    fn tp1() -> TopicPartition {
        TopicPartition::new(TOPIC.to_string(), PARTITION1)
    }

    /// The fixture fields of Java's test class: `time`, `metrics` and the one-partition `cluster`.
    pub(super) struct Fixture {
        pub(super) time: Arc<MockTime>,
        pub(super) metrics: Arc<Metrics>,
        pub(super) cluster: Cluster,
    }

    pub(super) fn fixture() -> Fixture {
        let time = Arc::new(MockTime::new());
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
        let node1 = Node::new(0, "localhost".to_string(), 1111);
        let part_metadata = vec![crate::common::requests::PartitionMetadata {
            error: Errors::None,
            topic_partition: tp1(),
            leader_id: Some(node1.id()),
            leader_epoch: None,
            replica_ids: vec![],
            in_sync_replica_ids: vec![],
            offline_replica_ids: vec![],
        }];
        let snapshot = MetadataSnapshot::new(
            None,
            HashMap::from([(node1.id(), node1)]),
            part_metadata,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );
        Fixture { time, metrics, cluster: snapshot.cluster().clone() }
    }

    impl Fixture {
        pub(super) fn pool(&self, total_memory: i64, chunk_size: usize) -> Arc<BufferPool> {
            Arc::new(BufferPool::with_allocation_mode(
                total_memory,
                chunk_size,
                Arc::clone(&self.metrics),
                Arc::clone(&self.time) as Arc<dyn Time>,
                "producer-metrics",
                AllocationMode::Incremental,
            ))
        }

        pub(super) fn accumulator_with_pool(
            &self,
            batch_size: i32,
            compression: crate::common::compress::Compression,
            pool: Arc<BufferPool>,
        ) -> Result<ChunkedRecordAccumulator, Error> {
            ChunkedRecordAccumulator::new(
                batch_size,
                compression,
                0,    // lingerMs
                0,    // retryBackoffMs
                0,    // retryBackoffMaxMs
                3200, // deliveryTimeoutMs
                PartitionerConfig::default(),
                Arc::clone(&self.metrics),
                "producer-metrics",
                Arc::clone(&self.time) as Arc<dyn Time>,
                pool,
                None, // transactionManager
                LogContext::empty(),
            )
        }

        pub(super) fn accumulator(
            &self,
            batch_size: i32,
            chunk_size: usize,
            total_memory: i64,
        ) -> ChunkedRecordAccumulator {
            self.accumulator_with_pool(batch_size, Compression::none().build(), self.pool(total_memory, chunk_size))
                .unwrap()
        }

        pub(super) fn hooked(
            &self,
            batch_size: i32,
            pool: Arc<BufferPool>,
            hooks: Arc<dyn ChunkedAccumulatorTestHooks>,
        ) -> ChunkedRecordAccumulator {
            let mut accum = self
                .accumulator_with_pool(batch_size, Compression::none().build(), pool)
                .unwrap();
            accum.set_test_hooks(hooks);
            accum
        }

        /// `accum.append(topic, partition, 0L, key, value, Record.EMPTY_HEADERS, null, maxTimeToBlock,
        /// time.milliseconds(), cluster)`.
        pub(super) async fn append_to(
            &self,
            accum: &ChunkedRecordAccumulator,
            partition: i32,
            value: &[u8],
            max_time_to_block: i64,
        ) -> Result<RecordAppendResult, Error> {
            append_with(accum, &self.time, &self.cluster, partition, value, max_time_to_block).await
        }

        pub(super) async fn append(
            &self,
            accum: &ChunkedRecordAccumulator,
            value: &[u8],
        ) -> Result<RecordAppendResult, Error> {
            self.append_to(accum, PARTITION1, value, MAX_BLOCK_TIME_MS).await
        }

        pub(super) fn buffer_exhausted_total(&self) -> f64 {
            let name = self.metrics.metric_name_description_tags(
                "buffer-exhausted-total",
                "producer-metrics",
                "",
                std::collections::BTreeMap::new(),
            );
            self.metrics.metric(&name).unwrap().measurable_value(self.time.milliseconds())
        }
    }

    /// An append from inside a hook (Java's nested `accum.append(..)` from a pool override).
    pub(super) async fn append_with(
        accum: &ChunkedRecordAccumulator,
        time: &MockTime,
        cluster: &Cluster,
        partition: i32,
        value: &[u8],
        max_time_to_block: i64,
    ) -> Result<RecordAppendResult, Error> {
        accum
            .append(
                TOPIC,
                partition,
                0,
                Some(KEY),
                Some(value),
                &[],
                None,
                max_time_to_block,
                time.milliseconds(),
                cluster,
            )
            .await
            .map_err(|failure| failure.error)
    }

    /// `batchesFor(accum, tp1)`: the record counts of the batches queued for `tp1`, in order.
    pub(super) fn record_counts(accum: &ChunkedRecordAccumulator) -> Vec<i32> {
        accum
            .base()
            .with_deque_for_test(&tp1(), |deque| deque.iter().map(|batch| batch.record_count).collect())
    }

    /// `batchesFor(accum, tp1)` mapped to `isFull()`.
    pub(super) fn fullness(accum: &ChunkedRecordAccumulator) -> Vec<bool> {
        accum
            .base()
            .with_deque_for_test(&tp1(), |deque| deque.iter().map(ProducerBatch::is_full).collect())
    }

    /// Installs a counting `deallocate` observer on `pool` (Java's `deallocate` overrides).
    fn count_deallocations(pool: &BufferPool) -> Arc<AtomicI32> {
        let count = Arc::new(AtomicI32::new(0));
        let observed = Arc::clone(&count);
        *pool.deallocate_observer.lock().unwrap() = Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        count
    }

    /// Translated from `ChunkedRecordAccumulatorTest.testFirstRecordCreatesChunkedBatch`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testFirstRecordCreatesChunkedBatch"
    )]
    async fn test_first_record_creates_chunked_batch() {
        let fx = fixture();
        let chunk_size = 256;
        let accum = fx.accumulator(1024, chunk_size, 16 * chunk_size as i64);

        fx.append(&accum, &[0u8; 64]).await.unwrap();

        assert_eq!(vec![1], record_counts(&accum));
        assert!(accum.base().with_deque_for_test(&tp1(), |deque| deque[0].is_chunked()));
        accum.base().close();
    }

    /// Translated from `ChunkedRecordAccumulatorTest.testSmallFollowupRecordFitsWithoutExtension`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testSmallFollowupRecordFitsWithoutExtension"
    )]
    async fn test_small_followup_record_fits_without_extension() {
        let fx = fixture();
        let chunk_size = 256;
        let accum = fx.accumulator(1024, chunk_size, 16 * chunk_size as i64);

        fx.append(&accum, &[0u8; 16]).await.unwrap();
        fx.append(&accum, &[0u8; 16]).await.unwrap();

        assert_eq!(vec![2], record_counts(&accum), "Both records should land in the same batch");
        accum.base().close();
    }

    /// A follow-up record that would overflow the existing chunks triggers mid-batch extension:
    /// additional chunks are attached and the record lands in the same batch.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testMidBatchExtensionGrowsExistingBatch`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testMidBatchExtensionGrowsExistingBatch"
    )]
    async fn test_mid_batch_extension_grows_existing_batch() {
        let fx = fixture();
        let chunk_size = 128;
        let accum = fx.accumulator(8192, chunk_size, 16 * chunk_size as i64);

        // First record fits in 1 chunk (~64+overhead).
        fx.append(&accum, &[0u8; 32]).await.unwrap();
        assert_eq!(vec![1], record_counts(&accum));

        // Second record big enough to require an extra chunk.
        fx.append(&accum, &[0u8; 200]).await.unwrap();

        // Same batch, now with the second record.
        assert_eq!(
            vec![2],
            record_counts(&accum),
            "Should still be the same batch — extension should not roll, and the second record should land in it"
        );
        accum.base().close();
    }

    /// Java's `poolMockingConcurrentChunkAllocation` (and the inline pool of
    /// `testConcurrentExtensionRaceLoserStartsNewBatch`): adds one append right after a chunk
    /// allocation returns, mocking a concurrent appender racing the same batch.
    struct ConcurrentChunkAllocation {
        inject_append_once: AtomicBool,
        injected_value: Vec<u8>,
        time: Arc<MockTime>,
        cluster: Cluster,
    }

    impl ChunkedAccumulatorTestHooks for ConcurrentChunkAllocation {
        fn allocate_chunks<'a>(
            &'a self,
            accum: &'a ChunkedRecordAccumulator,
            total_size: i32,
            max_time_to_block_ms: i64,
        ) -> HookFuture<'a> {
            Box::pin(async move {
                let chunks = accum.real_allocate_chunks(total_size, max_time_to_block_ms).await?;
                if self.inject_append_once.swap(false, Ordering::SeqCst) {
                    append_with(
                        accum,
                        &self.time,
                        &self.cluster,
                        PARTITION1,
                        &self.injected_value,
                        MAX_BLOCK_TIME_MS,
                    )
                    .await?;
                }
                Ok(chunks)
            })
        }
    }

    fn concurrent_chunk_allocation(fx: &Fixture, injected_value: &[u8]) -> Arc<ConcurrentChunkAllocation> {
        Arc::new(ConcurrentChunkAllocation {
            inject_append_once: AtomicBool::new(false),
            injected_value: injected_value.to_vec(),
            time: Arc::clone(&fx.time),
            cluster: fx.cluster.clone(),
        })
    }

    /// Two concurrent appenders race to extend the same open batch, each sizing its extension
    /// against the same remaining capacity off-lock. Whichever attaches and appends first consumes
    /// that capacity, leaving the other appender's extension too small for its record. Verifies
    /// that the post-attach `try_append` — which attempts the write based on the batch's actual
    /// capacity — makes that appender extend again and land its record in the same batch, rather
    /// than write past the stream's chunk capacity.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testConcurrentExtensionRaceLoserExtendsAgain`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testConcurrentExtensionRaceLoserExtendsAgain"
    )]
    async fn test_concurrent_extension_race_loser_extends_again() {
        let fx = fixture();
        let chunk_size = 256;
        let value = [0u8; 350]; // needs 2 chunks
        let hooks = concurrent_chunk_allocation(&fx, &value);
        let accum = fx.hooked(8192, fx.pool(64 * chunk_size as i64, chunk_size), hooks.clone());

        // Tiny first record opens the batch, both racing records extend.
        fx.append(&accum, &[0u8; 1]).await.unwrap();

        // This append sizes its gap, then the injected append wins the race while the gap chunks
        // are held off-lock, shrinking the remaining capacity the gap was sized against. Even with
        // the over-allocation, this append shouldn't write past the stream's chunk capacity.
        hooks.inject_append_once.store(true, Ordering::SeqCst);
        fx.append(&accum, &value).await.unwrap();

        assert_eq!(
            vec![3],
            record_counts(&accum),
            "Expecting a single batch with 3 records: the initial one plus the 2 racing ones."
        );
        accum.base().close();
    }

    /// Concurrent extensions can over-reserve: each appender sizes its own gap against the same
    /// remaining capacity, so a batch can end up with more chunk capacity than its batch-size limit
    /// admits. The limit must still be enforced on append: a record that no longer fits after the
    /// race winner's append rolls to a new batch even though the over-reserved chunks could
    /// physically hold it, and the unused surplus returns to the pool as soon as the batch is
    /// closed for appends during that roll (before completion).
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testConcurrentExtensionRaceLoserStartsNewBatch`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testConcurrentExtensionRaceLoserStartsNewBatch"
    )]
    async fn test_concurrent_extension_race_loser_starts_new_batch() {
        let fx = fixture();
        let chunk_size = 256;
        let batch_size = 1024;
        // Sized so each record fits the batch-size limit individually, but not both together.
        let value = [0u8; 500];
        let hooks = concurrent_chunk_allocation(&fx, &value);
        let pool = fx.pool(64 * chunk_size as i64, chunk_size);
        let chunk_deallocations = count_deallocations(&pool);
        let accum = fx.hooked(batch_size, pool, hooks.clone());

        // Open the batch with a tiny record; both racing records will extend it.
        fx.append(&accum, &[0u8; 1]).await.unwrap();
        let deallocs_after_open = chunk_deallocations.load(Ordering::SeqCst);

        // This append sizes its gap, then the injected append wins the race while the gap chunks
        // are held off-lock. The retry finds the batch over its batch-size limit, so the record
        // must roll to a new batch despite the attached (over-reserved) capacity.
        hooks.inject_append_once.store(true, Ordering::SeqCst);
        fx.append(&accum, &value).await.unwrap();

        // The losing record must roll to a new batch, not exceed batch.size: the first batch has
        // the initial record plus the race winner, the second the loser record.
        assert_eq!(vec![2, 1], record_counts(&accum));

        // The loser's extension chunks were attached to the first batch but never used (the
        // over-reservation). They are freed as soon as the first batch is closed for appends
        // (during the roll to the new batch) — before completion, not held until deallocate.
        assert!(
            chunk_deallocations.load(Ordering::SeqCst) > deallocs_after_open,
            "the over-reserved unused chunks are freed when the first batch is closed for appends"
        );
        accum.base().close();
    }

    /// Java's anonymous pool and accumulator of `testPartitionSwitchRefundsHeldExtensionChunks`.
    struct SwitchWhileExtensionHeld {
        extension_allocated: AtomicBool,
        switch_fired: AtomicBool,
    }

    impl ChunkedAccumulatorTestHooks for SwitchWhileExtensionHeld {
        fn allocate_chunks<'a>(
            &'a self,
            accum: &'a ChunkedRecordAccumulator,
            total_size: i32,
            max_time_to_block_ms: i64,
        ) -> HookFuture<'a> {
            Box::pin(async move {
                let chunks = accum.real_allocate_chunks(total_size, max_time_to_block_ms).await?;
                // The mid-batch extension path is the only non-blocking caller.
                if max_time_to_block_ms == 0 {
                    self.extension_allocated.store(true, Ordering::SeqCst);
                }
                Ok(chunks)
            })
        }

        fn partition_changed(
            &self,
            _accum: &ChunkedRecordAccumulator,
            _topic_info: &TopicInfo,
            _unknown_partition: bool,
            _cluster: &Cluster,
        ) -> Option<bool> {
            // Inject one spurious switch, but only once an extension allocation has happened —
            // i.e. on the second-sync-block check, while extension chunks are held.
            if self.extension_allocated.load(Ordering::SeqCst)
                && self
                    .switch_fired
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Some(true);
            }
            None
        }
    }

    /// When the sticky partition switches while extension chunks are held off-lock, those chunks
    /// (sized against the previous partition's open batch) must be refunded to the pool on the
    /// retry (not carried over and attached to a different partition's open batch).
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testPartitionSwitchRefundsHeldExtensionChunks`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testPartitionSwitchRefundsHeldExtensionChunks"
    )]
    async fn test_partition_switch_refunds_held_extension_chunks() {
        let fx = fixture();
        let chunk_size = 128;
        let total_memory = 32 * chunk_size as i64;
        let hooks = Arc::new(SwitchWhileExtensionHeld {
            extension_allocated: AtomicBool::new(false),
            switch_fired: AtomicBool::new(false),
        });
        let pool = fx.pool(total_memory, chunk_size);
        let chunk_deallocations = count_deallocations(&pool);
        let accum = fx.hooked(8192, pool, hooks.clone());

        // Warmup: tiny first record establishes an open batch (first-record path, blocking
        // allocate — does not flag extension_allocated).
        fx.append(&accum, &[0u8; 1]).await.unwrap();
        let dealloc_before_extend = chunk_deallocations.load(Ordering::SeqCst);

        // Second record overflows the chunk → extension allocated off-lock → injected switch fires
        // in the second sync block with those chunks held.
        fx.append(&accum, &[0u8; 200]).await.unwrap();

        assert!(
            hooks.switch_fired.load(Ordering::SeqCst),
            "the injected partition switch should have fired"
        );
        assert!(
            chunk_deallocations.load(Ordering::SeqCst) > dealloc_before_extend,
            "extension chunks held at the partition switch must be refunded to the pool; without the fix they are \
             carried and attached instead"
        );

        // The record still appends correctly after the switch-retry.
        assert_eq!(
            vec![2],
            record_counts(&accum),
            "second record should still land in the batch after the switch-retry"
        );
        accum.base().close();
    }

    /// Translated from `ChunkedRecordAccumulatorTest.testInflightExpirationReturnsAllChunksToPool`.
    ///
    /// Java's `deallocate` throws `IllegalStateException` for an inflight batch; the Rust
    /// accumulator panics there (a pre-existing deviation of `RecordAccumulator::deallocate`), so
    /// the panic is caught and its message asserted.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testInflightExpirationReturnsAllChunksToPool"
    )]
    async fn test_inflight_expiration_returns_all_chunks_to_pool() {
        let fx = fixture();
        let chunk_size = 128;
        let total_memory = 32 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let accum = fx
            .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
            .unwrap();

        // A record large enough that allocate_chunks reserves multiple chunks (K > 1).
        fx.append(&accum, &[0u8; 400]).await.unwrap();
        assert_eq!(1, record_counts(&accum).len());

        // Confirm the batch actually consumed multiple chunks. estimate_size_in_bytes_upper_bound
        // for a 400-byte value with v2 framing is well over chunk_size, so K should be >= 2.
        let held_before_deallocate = total_memory - pool.available_memory();
        assert!(
            held_before_deallocate >= 2 * chunk_size as i64,
            "test setup expects K >= 2; pool held {held_before_deallocate} bytes"
        );

        let base = Arc::clone(accum.base());
        let panic = base.with_deque_for_test(&tp1(), |deque| {
            let batch = deque.front_mut().unwrap();
            // Mark the batch as if the Sender had drained it.
            batch.set_inflight(true);
            // The inflight branch fails after deallocating. The chunked override must return all
            // K chunks (not just initial_capacity) before propagating the failure.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| base.deallocate(batch)))
                .expect_err("deallocating an inflight batch must fail")
        });
        let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(
            message.starts_with("Attempting to deallocate a batch that is inflight. Batch is "),
            "{message}"
        );

        assert_eq!(
            total_memory,
            pool.available_memory(),
            "pool should be fully restored after inflight-expiration deallocate; any K-1 unsurrendered chunks \
             indicate the chunked-leak regression"
        );
        accum.base().close();
    }

    /// Java's anonymous pool and accumulator of `testExhaustedExtensionFallsBackToBlockingNewBatchPath`.
    struct ExhaustedExtension {
        alloc_timeouts: Mutex<Vec<i64>>,
        close_for_appends_calls: Arc<AtomicI32>,
        close_calls_at_alloc: Mutex<Vec<i32>>,
    }

    impl ChunkedAccumulatorTestHooks for ExhaustedExtension {
        fn allocate_chunks<'a>(
            &'a self,
            accum: &'a ChunkedRecordAccumulator,
            total_size: i32,
            max_time_to_block_ms: i64,
        ) -> HookFuture<'a> {
            Box::pin(async move {
                self.alloc_timeouts.lock().unwrap().push(max_time_to_block_ms);
                self.close_calls_at_alloc
                    .lock()
                    .unwrap()
                    .push(self.close_for_appends_calls.load(Ordering::SeqCst));
                // Simulate an exhausted pool for the non-blocking extension acquire only.
                if max_time_to_block_ms == 0 {
                    return Err(Error::buffer_exhausted("injected: pool exhausted"));
                }
                accum.real_allocate_chunks(total_size, max_time_to_block_ms).await
            })
        }

        fn close_for_record_appends_calls(&self) -> Option<Arc<AtomicI32>> {
            // Count close_for_record_appends calls on the batches this accumulator creates.
            Some(Arc::clone(&self.close_for_appends_calls))
        }
    }

    /// When the pool is exhausted during a mid-batch extension, the append must not busy-loop
    /// retrying the non-blocking acquire: after a single failed extension acquire
    /// (max_time_to_block_ms = 0), the open batch is closed and the very next pool call is the
    /// blocking new-batch acquire (max_time_to_block_ms > 0), where the record lands in a new
    /// batch.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testExhaustedExtensionFallsBackToBlockingNewBatchPath`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testExhaustedExtensionFallsBackToBlockingNewBatchPath"
    )]
    async fn test_exhausted_extension_falls_back_to_blocking_new_batch_path() {
        let fx = fixture();
        let chunk_size = 256;
        let hooks = Arc::new(ExhaustedExtension {
            alloc_timeouts: Mutex::new(Vec::new()),
            close_for_appends_calls: Arc::new(AtomicI32::new(0)),
            close_calls_at_alloc: Mutex::new(Vec::new()),
        });
        let accum = fx.hooked(8192, fx.pool(16 * chunk_size as i64, chunk_size), hooks.clone());

        // First record establishes an open batch (blocking first-record acquire).
        fx.append(&accum, &[0u8; 100]).await.unwrap();

        // Second record overflows the batch's chunk so it needs extension; the injected exhaustion
        // fails the non-blocking acquire, the batch is closed, and the record must go straight to
        // the blocking new-batch path.
        let result = fx.append(&accum, &[0u8; 100]).await.unwrap();

        // Validate the expected call sequence: blocking (first batch), non-blocking (failed
        // extension), blocking (new batch).
        assert_eq!(
            vec![MAX_BLOCK_TIME_MS, 0, MAX_BLOCK_TIME_MS],
            *hooks.alloc_timeouts.lock().unwrap(),
            "expected a single failed extension acquire followed directly by the blocking new-batch acquire"
        );
        let close_calls_at_alloc = hooks.close_calls_at_alloc.lock().unwrap().clone();
        assert_eq!(0, close_calls_at_alloc[0], "no close before the first-record acquire");
        assert_eq!(0, close_calls_at_alloc[1], "no close before the extension acquire");
        assert!(
            close_calls_at_alloc[2] >= 1,
            "the failed extension must close the batch before the blocking new-batch acquire"
        );
        assert!(result.new_batch_created, "record must land in a new batch");

        // Closed batch + new batch expected.
        assert_eq!(vec![1, 1], record_counts(&accum));
        // The original batch is far below its write limit, so is_full() can only be true via the
        // closed append stream — i.e., it was closed for appends on the failed extension.
        assert!(fullness(&accum)[0], "original batch must be closed for appends");
        accum.base().close();
    }

    /// A single dropped record is counted exactly once even when it first fails the extension
    /// attempt (recovered) and then fails the new-batch acquire. Uses a real (non-overridden) pool
    /// so the actual allocate_chunks path runs on both acquires.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testBufferExhaustedNotDoubleCountedAcrossExtensionAndNewBatch`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testBufferExhaustedNotDoubleCountedAcrossExtensionAndNewBatch"
    )]
    async fn test_buffer_exhausted_not_double_counted_across_extension_and_new_batch() {
        let fx = fixture();
        let chunk_size = 256;
        // Pool holds exactly one chunk: the first record consumes it, leaving the pool empty so
        // both the second record's extension attempt and its new-batch acquire fail.
        let accum = fx.accumulator(8192, chunk_size, chunk_size as i64);

        // First record fills the one available chunk (pool now empty), opening the batch.
        fx.append(&accum, &[0u8; 150]).await.unwrap();
        assert_eq!(0.0, fx.buffer_exhausted_total());

        // Second record overflows the batch's chunk. The extension attempt (always non-blocking)
        // fails first — pool empty, recovered by closing the batch — then the new-batch acquire
        // blocks up to max.block.ms and also fails since the pool stays empty. A small block time
        // keeps the test fast (nothing frees memory during the wait). Both failed acquires must
        // count as a single dropped record.
        let new_batch_block_ms = 50;
        let error = fx
            .append_to(&accum, PARTITION1, &[0u8; 150], new_batch_block_ms)
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::ProducerBufferExhausted(_)), "{error:?}");
        assert_eq!(
            1.0,
            fx.buffer_exhausted_total(),
            "the dropped record must be counted once, not once per failed acquire"
        );
        accum.base().close();
    }

    /// Test that chunks are returned to the pool only at batch completion (deallocate), never at
    /// close.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testBatchCloseDoesNotDeallocateChunksPrematurely`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testBatchCloseDoesNotDeallocateChunksPrematurely"
    )]
    async fn test_batch_close_does_not_deallocate_chunks_prematurely() {
        let fx = fixture();
        let chunk_size = 256;
        let total_memory = 32 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let accum = fx
            .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
            .unwrap();

        // A small record fits in a single chunk, so exactly one (data-bearing) chunk is reserved.
        fx.append(&accum, &[0u8; 64]).await.unwrap();
        assert_eq!(
            total_memory - chunk_size as i64,
            pool.available_memory(),
            "append must reserve exactly one chunk"
        );

        let base = Arc::clone(accum.base());
        base.with_deque_for_test(&tp1(), |deque| {
            let batch = deque.front_mut().unwrap();
            // Matches the call sites in RecordAccumulator::drain and Sender.
            batch.close();
            // close() must not return the chunk to the pool: available memory is unchanged, and
            // the built record set is still readable because its bytes still live in the batch.
            assert_eq!(
                total_memory - chunk_size as i64,
                pool.available_memory(),
                "close() must not return chunks to the pool"
            );
            let records: MemoryRecords = batch.records();
            assert!(
                records.size_in_bytes() > 0,
                "batch must produce a non-empty record set after close; chunks were deallocated prematurely"
            );
            let mut count = 0;
            for record in records.records() {
                count += 1;
                assert!(record.value().is_some());
            }
            assert_eq!(1, count, "expected exactly 1 record after close");

            // Completion (deallocate) is what returns the chunk to the pool.
            base.deallocate(batch);
        });
        assert_eq!(
            total_memory,
            pool.available_memory(),
            "deallocate must return the chunk to the pool"
        );
        accum.base().close();
    }

    /// As small records accumulate in a batch, the attached chunks grow with the batch's
    /// cumulative projected output (not per-record).
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testExtensionTracksCumulativeBatchSize`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testExtensionTracksCumulativeBatchSize"
    )]
    async fn test_extension_tracks_cumulative_batch_size() {
        let fx = fixture();
        let chunk_size = 64;
        let batch_size = 512;
        let accum = fx.accumulator(batch_size, chunk_size, 64 * chunk_size as i64);

        let small_value = [0u8; 24];
        for _ in 0..6 {
            fx.append(&accum, &small_value).await.unwrap();
        }
        assert_eq!(vec![6], record_counts(&accum));

        // Finalize the batch: close() writes the record-batch header and builds the record set.
        // Chunk capacity for every record is ensured at append time (the accumulator attaches
        // extension chunks before appending, and an append without capacity would fail), so the
        // chunks already hold the whole batch by the time it is built.
        let actual_size = accum.base().with_deque_for_test(&tp1(), |deque| {
            let batch = deque.front_mut().unwrap();
            batch.close();
            batch.records().size_in_bytes()
        });
        // Under NONE compression, estimated_bytes_written is exact: physical bytes = header + sum
        // of per-record bytes. The chunks attached must cover that, so actual_size must be >
        // chunk_size for a multi-record batch with non-trivial content.
        assert!(
            actual_size > chunk_size,
            "batch should have grown beyond a single chunk; got {actual_size}"
        );
        accum.base().close();
    }

    /// Translated from `ChunkedRecordAccumulatorTest.testCumulativeAccountsForBatchHeaderOnce`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testCumulativeAccountsForBatchHeaderOnce"
    )]
    async fn test_cumulative_accounts_for_batch_header_once() {
        let fx = fixture();
        let chunk_size = 256;
        let total_memory = 64 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let accum = fx
            .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
            .unwrap();

        let before_alloc = pool.available_memory();
        // First record establishes the batch. Each subsequent small record contributes its
        // uncompressed bytes to the cumulative target; the batch header is NOT re-counted.
        for _ in 0..4 {
            fx.append(&accum, &[0u8; 8]).await.unwrap();
        }

        // Each per-record append adds ~10-30 bytes (key + value + record overhead, V2). Cumulative
        // total for 4 records is well below chunk_size=256, so only 1 chunk should ever be
        // attached. The per-record formula (header counted once per record) would have allocated
        // more.
        let held = before_alloc - pool.available_memory();
        assert_eq!(
            chunk_size as i64, held,
            "cumulative formula should hold exactly one chunk for a small-record batch; header double-counting \
             (per-record formula) would inflate this"
        );
        accum.base().close();
    }

    /// Translated from `ChunkedRecordAccumulatorTest.testChunkedBatchRejectsNonChunkedStream`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testChunkedBatchRejectsNonChunkedStream"
    )]
    fn test_chunked_batch_rejects_non_chunked_stream() {
        let plain_builder = MemoryRecords::builder_with_buffer_magic(
            vec![0u8; 256],
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
        );
        let error = ChunkedProducerBatch::new_chunked(tp1(), plain_builder, 0).unwrap_err();
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            "recordsBuilder must be an instance of ChunkedByteBufferOutputStream, but found \
             org.apache.kafka.common.utils.internals.ByteBufferOutputStream",
            error.message()
        );
    }

    /// Java's anonymous pool of `testBatchClosedForAppendsDuringAllocationIsNotExtended`.
    struct CloseDuringAllocation {
        close_once: AtomicBool,
        /// The addresses (identities) of the chunks handed to the extension path.
        extension_chunks: Mutex<Vec<usize>>,
    }

    impl ChunkedAccumulatorTestHooks for CloseDuringAllocation {
        fn allocate_chunks<'a>(
            &'a self,
            accum: &'a ChunkedRecordAccumulator,
            total_size: i32,
            max_time_to_block_ms: i64,
        ) -> HookFuture<'a> {
            Box::pin(async move {
                let chunks = accum.real_allocate_chunks(total_size, max_time_to_block_ms).await?;
                // The extension acquire is the only non-blocking one (see allocate_extension_chunks).
                if max_time_to_block_ms == 0 {
                    self.extension_chunks
                        .lock()
                        .unwrap()
                        .extend(chunks.iter().map(|chunk| chunk.as_ptr() as usize));
                }
                // Mock a concurrent appender that found the batch full: RecordAccumulator::try_append
                // calls close_for_record_appends() whenever the last batch refuses the record.
                if self.close_once.swap(false, Ordering::SeqCst) {
                    accum.base().with_deque_for_test(&tp1(), |deque| {
                        if let Some(last) = deque.back_mut() {
                            last.close_for_record_appends();
                        }
                    });
                }
                Ok(chunks)
            })
        }
    }

    /// A batch closed for appends while the extension chunks were acquired off-lock must not be
    /// attached to: the chunks go back to the pool and the record goes to a new batch.
    ///
    /// Translated from `ChunkedRecordAccumulatorTest.testBatchClosedForAppendsDuringAllocationIsNotExtended`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.ChunkedRecordAccumulatorTest#testBatchClosedForAppendsDuringAllocationIsNotExtended"
    )]
    async fn test_batch_closed_for_appends_during_allocation_is_not_extended() {
        let fx = fixture();
        let chunk_size = 256;
        // Java sets `dqRef` after the first append so the close only fires from then on; the
        // flag starts clear and is armed at the same point.
        let hooks = Arc::new(CloseDuringAllocation {
            close_once: AtomicBool::new(false),
            extension_chunks: Mutex::new(Vec::new()),
        });
        let pool = fx.pool(64 * chunk_size as i64, chunk_size);
        // Every chunk handed back to the pool, by identity.
        let returned_to_pool = Arc::new(Mutex::new(HashSet::new()));
        let returned = Arc::clone(&returned_to_pool);
        *pool.deallocate_observer.lock().unwrap() = Some(Arc::new(move |address| {
            returned.lock().unwrap().insert(address);
        }));
        let accum = fx.hooked(8192, pool, hooks.clone());

        // First record opens the batch with a single chunk.
        fx.append(&accum, &[0u8; 32]).await.unwrap();
        assert_eq!(1, record_counts(&accum).len());
        hooks.close_once.store(true, Ordering::SeqCst);

        // Second record needs an extension; the batch is closed for appends mid-window.
        let result = fx.append(&accum, &[0u8; 300]).await.unwrap();

        assert!(result.new_batch_created, "record must land in a new batch, not the closed one");
        // Closed batch + new batch expected; no record may have been added to the closed batch.
        assert_eq!(vec![1, 1], record_counts(&accum));
        assert!(fullness(&accum)[0], "the original batch must still be closed for appends");

        // Every chunk the extension path acquired for the now-closed batch must have gone back to
        // the pool, rather than being attached to it or dropped.
        let extension_chunks = hooks.extension_chunks.lock().unwrap().clone();
        assert!(!extension_chunks.is_empty(), "the extension path should have acquired chunks");
        let returned_to_pool = returned_to_pool.lock().unwrap();
        assert!(
            extension_chunks.iter().all(|chunk| returned_to_pool.contains(chunk)),
            "every chunk acquired to extend the closed batch must be returned to the pool"
        );
        accum.base().close();
    }

    // ---- Rust-only tests -------------------------------------------------------------------

    /// Rust-only (CLAUDE.md §11.6): an `append` future dropped while it waits for memory for a new
    /// batch takes nothing with it. The pool's own wait refunds the partial reservation, and the
    /// guard counts the append out, so `abort_incomplete_batches` cannot spin on it.
    #[tokio::test]
    async fn test_cancelled_append_refunds_and_counts_out() {
        let fx = fixture();
        let chunk_size = 256;
        let total_memory = 2 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let accum = fx
            .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
            .unwrap();

        // Hold one of the two chunks, so the next new batch (needing two) has to wait.
        let held = pool.try_allocate_chunks(chunk_size as i32).unwrap();
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            fx.append_to(&accum, PARTITION1, &[0u8; 300], 60_000),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the append must still be waiting for memory when it is dropped"
        );

        assert_eq!(0, pool.queued(), "the dropped append must leave the waiters queue");
        assert_eq!(
            0,
            accum.base().appends_in_progress.load(Ordering::Relaxed),
            "the dropped append must be counted out"
        );
        for chunk in held {
            pool.deallocate(chunk);
        }
        assert_eq!(
            total_memory,
            pool.available_memory(),
            "nothing may stay reserved for the dropped append"
        );
        accum.base().close();
    }

    /// Rust-only (CLAUDE.md §11.6): the guard returns a held new-batch stream and held extension
    /// chunks to the pool however the append exits — including a drop at an `.await`, where no
    /// Java `finally` exists. Drives the guard directly with both slots filled, the state an
    /// append holds between allocating and attaching.
    #[tokio::test]
    async fn test_append_guard_refunds_new_batch_stream_and_extension_chunks() {
        let fx = fixture();
        let chunk_size = 256;
        let total_memory = 8 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let appends_in_progress = std::sync::atomic::AtomicI32::new(0);
        {
            let mut guard = AppendGuard::new(&pool, &appends_in_progress);
            let stream_chunks = pool.try_allocate_chunks(2 * chunk_size as i32).unwrap();
            guard.new_batch = Some(NewBatchBuffer {
                stream: ChunkedByteBufferOutputStream::new(stream_chunks, chunk_size, Some(Arc::clone(&pool))).unwrap(),
                first_append_size: 300,
            });
            guard.extension_chunks = Some(pool.try_allocate_chunks(3 * chunk_size as i32).unwrap());
            assert_eq!(total_memory - 5 * chunk_size as i64, pool.available_memory());
            assert_eq!(1, appends_in_progress.load(Ordering::Relaxed));
        }
        assert_eq!(
            total_memory,
            pool.available_memory(),
            "the guard must return both the stream and the chunks"
        );
        assert_eq!(0, appends_in_progress.load(Ordering::Relaxed));
    }

    /// Rust-only (Critic 97 caution (a)): deallocating a chunked batch returns exactly its chunks.
    /// The single-buffer path (`take_buffer` + `deallocate_with_size(.., initial_capacity)`) would
    /// credit a chunk of non-pooled memory on top of the chunks the stream returns, letting
    /// `buffer.memory` be exceeded — for an inflight batch as much as for a completed one.
    #[tokio::test]
    async fn test_deallocating_a_chunked_batch_does_not_over_credit_the_pool() {
        let chunk_size = 128;
        let total_memory = 32 * chunk_size as i64;
        for inflight in [false, true] {
            let fx = fixture();
            let pool = fx.pool(total_memory, chunk_size);
            let accum = fx
                .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
                .unwrap();
            fx.append(&accum, &[0u8; 400]).await.unwrap();
            let base = Arc::clone(accum.base());
            base.with_deque_for_test(&tp1(), |deque| {
                let batch = deque.front_mut().unwrap();
                batch.set_inflight(inflight);
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| base.deallocate(batch)));
                // A second deallocate is skipped (already deallocated) rather than crediting again.
                batch.set_inflight(false);
                base.deallocate(batch);
            });
            assert_eq!(
                total_memory,
                pool.available_memory(),
                "inflight={inflight}: the pool must get back exactly what the batch held, not more"
            );
            assert_eq!(
                total_memory,
                pool.unallocated_memory() + pool.free_size() as i64 * chunk_size as i64
            );
            accum.base().close();
        }
    }

    /// Rust-only (Critic 97 caution (b)): a chunked batch's first append that its stream was not
    /// pre-sized for is refused before a byte is written — an error, not a panic or an overflow.
    #[tokio::test]
    async fn test_undersized_first_append_errors_without_panicking() {
        let fx = fixture();
        let chunk_size = 128;
        let total_memory = 32 * chunk_size as i64;
        let pool = fx.pool(total_memory, chunk_size);
        let accum = fx
            .accumulator_with_pool(8192, Compression::none().build(), Arc::clone(&pool))
            .unwrap();

        // One chunk, then a first record needing several.
        let stream = ChunkedByteBufferOutputStream::new(
            pool.try_allocate_chunks(chunk_size as i32).unwrap(),
            chunk_size,
            Some(Arc::clone(&pool)),
        )
        .unwrap();
        let records_builder = accum.chunked_records_builder(stream, 1000).unwrap();
        let mut batch = ChunkedProducerBatch::new_chunked(tp1(), records_builder, 0).unwrap();
        assert!(batch.extension_bytes_needed(0, Some(KEY), Some(&[0u8; 400]), &[]) > 0);
        assert!(batch.try_append(0, Some(KEY), Some(&[0u8; 400]), &[], None, 0).is_err());
        assert_eq!(0, batch.record_count, "nothing may have been written");

        // Through the accumulator's new-batch step the refusal is Java's IllegalStateException.
        let topic: Arc<str> = Arc::from(TOPIC);
        let stream = ChunkedByteBufferOutputStream::new(
            pool.try_allocate_chunks(chunk_size as i32).unwrap(),
            chunk_size,
            Some(Arc::clone(&pool)),
        )
        .unwrap();
        let base = Arc::clone(accum.base());
        let mut stream = Some(stream);
        let failure = base
            .with_deque_for_test(&tp1(), |deque| {
                base.append_new_batch(
                    &topic,
                    PARTITION1,
                    deque,
                    0,
                    Some(KEY),
                    Some(&[0u8; 400]),
                    &[],
                    None,
                    |deque, callback| {
                        accum.try_append(0, Some(KEY), Some(&[0u8; 400]), &[], callback, deque, &topic, PARTITION1, 0)
                    },
                    || accum.chunked_records_builder(stream.take().unwrap(), 1000),
                    |tp, records_builder, now_ms| accum.create_producer_batch(tp, records_builder, now_ms),
                    0,
                )
                .map(|_| ())
            })
            .unwrap_err();
        assert!(matches!(failure.error, Error::LocalIllegalState(_)), "{:?}", failure.error);
        assert_eq!(
            "Unexpected append to a chunked batch whose chunks lack capacity for the record; the stream should have \
             been pre-sized for the batch's first record",
            failure.error.message()
        );
        assert_eq!(0, record_counts(&accum).len(), "the refused batch must not be queued");

        drop(batch);
        assert_eq!(
            total_memory,
            pool.available_memory(),
            "the refused batches' chunks must go back to the pool"
        );
        accum.base().close();
    }

    /// Rust-only: the constructor's checks, with Java's messages.
    #[test]
    fn test_constructor_rejects_full_pool_and_compression() {
        let fx = fixture();
        let full_pool = Arc::new(BufferPool::new(
            1024 * 1024,
            16384,
            Arc::clone(&fx.metrics),
            Arc::clone(&fx.time) as Arc<dyn Time>,
            "producer-metrics",
        ));
        let error = fx
            .accumulator_with_pool(16384, Compression::none().build(), full_pool)
            .err()
            .unwrap();
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!("bufferPool must serve INCREMENTAL allocation, but serves FULL", error.message());

        let error = fx
            .accumulator_with_pool(16384, Compression::gzip().build(), fx.pool(1024 * 1024, 16384))
            .err()
            .unwrap();
        assert!(matches!(error, Error::UnsupportedVersion(_)), "{error:?}");
        assert_eq!(
            "Compression is not yet supported with the incremental buffer.memory allocation strategy",
            error.message()
        );
    }
}
