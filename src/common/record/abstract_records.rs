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

//! Translation of `org.apache.kafka.common.record.AbstractRecords`.
//!
//! Java's `AbstractRecords` is an abstract class that:
//!
//! 1. Overrides three `Records` interface methods with default bodies. In Rust
//!    those defaults live directly on the [`Records`](crate::common::record::Records)
//!    trait so any implementor inherits them — see
//!    `Records::last_batch`, `Records::has_matching_magic`, `Records::records`.
//! 2. Adds a non-interface helper `firstBatch()` returning the first batch or
//!    `null`. We expose this as a free function [`first_batch`] taking a
//!    `&dyn Records` so any concrete impl can use it.
//! 3. Overrides `toSend()` to return a `DefaultRecordsSend<Records>`. The
//!    `DefaultRecordsSend` type lives in Phase 3d (record-send path); when it
//!    arrives, it will add a default `to_send` method to
//!    [`BaseRecords`](crate::common::record::BaseRecords).
//! 4. Provides three `public static` helpers:
//!    `estimateSizeInBytes(byte, long, CompressionType, Iterable<Record>)`,
//!    `estimateSizeInBytes(byte, CompressionType, Iterable<SimpleRecord>)`,
//!    `estimateSizeInBytesUpperBound(...)` and `recordBatchHeaderSizeInBytes(...)`.
//!    These dispatch to `LegacyRecord` (legacy v0/v1 — out of scope per
//!    PLAN.md) and `DefaultRecordBatch::sizeInBytes` (Phase 3c). They are
//!    therefore deferred to Phase 3c, when `DefaultRecordBatch` lands.

use crate::common::errors::KafkaError;
use crate::common::header::RecordHeader;
use crate::common::record::default_record_batch::estimate_batch_size_upper_bound;
use crate::common::record::{CompressionType, RecordBatch, Records};

/// Return the first batch in `records`, or `None` if empty. Mirrors Java's
/// `AbstractRecords#firstBatch()`.
///
/// Returns `Err(KafkaError::CorruptRecord)` if the first batch fails to
/// parse — matching Java's `CorruptRecordException` propagation through
/// `Iterator.next()`.
pub fn first_batch<'a>(records: &'a dyn Records) -> Result<Option<Box<dyn RecordBatch + 'a>>, KafkaError> {
    records.batches().next().transpose()
}

/// Get an upper bound estimate on the byte size of a batch with only a
/// single record using a given key, value and headers. Mirrors Java's
/// `AbstractRecords#estimateSizeInBytesUpperBound(byte, CompressionType,
/// byte[], byte[], Header[])`.
///
/// Phase 3 only supports magic v2. For v0/v1 callers we conservatively
/// fall through to the v2 upper bound (Java dispatches to
/// `LegacyRecord.recordSize`, which is out of scope per PLAN.md).
pub fn estimate_size_in_bytes_upper_bound(
    _magic: i8,
    _compression: CompressionType,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> i32 {
    estimate_batch_size_upper_bound(key, value, headers)
}
