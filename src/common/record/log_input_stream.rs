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

//! Translation of `org.apache.kafka.common.record.LogInputStream`.
//!
//! The trait is wired into [`crate::common::record::byte_buffer_log_input_stream`]
//! and [`crate::common::record::record_batch_iterator`]; the public consumer is
//! `MemoryRecords::batches()` (Phase 3d-3). Until that lands the trait is
//! exercised only from tests, hence the module-level `dead_code` allow on
//! the few crate-private surfaces.

#![allow(dead_code)] // Phase 3d-3 consumer (MemoryRecords::batches) lands next.

use crate::common::errors::KafkaError;
use crate::common::record::RecordBatch;

/// An abstraction between an underlying input stream and record iterators, a
/// `LogInputStream` only returns the batches at one level. For magic values 0
/// and 1, this means that it can either handle iteration at the top level of
/// the log or deep iteration within the payload of a single message, but it
/// does not attempt to handle both. For magic value 2, this is only used for
/// iterating over the top-level record batches (inner records do not follow
/// the [`RecordBatch`] interface).
///
/// The generic typing allows for implementations which present only a view of
/// the log entries.
///
/// Java's `LogInputStream<T>` interface is package-private and uses
/// `IOException` for its single method. The Rust translation returns
/// `Result<Option<T>, KafkaError>`:
///
/// * `Ok(Some(batch))` — a batch was decoded from the stream;
/// * `Ok(None)` — end of stream / not enough data;
/// * `Err(KafkaError)` — corrupt batch or IO error.
pub(crate) trait LogInputStream<T: RecordBatch> {
    /// Get the next record batch from the underlying input stream.
    fn next_batch(&mut self) -> Result<Option<T>, KafkaError>;
}
