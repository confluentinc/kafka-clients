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

//! Low-level representation of records and record batches
//! (`org.apache.kafka.common.record.internal`).
//!
//! Provides the low-level representation of records and record batches used by
//! clients and servers. **This package is not a supported Kafka API; the
//! implementation may change without warning between minor or patch releases.**
//!
//! In Kafka 4.3.1 (KAFKA-20128) everything except `TimestampType` (which stays
//! in [`crate::common::record`]) moved from `org.apache.kafka.common.record` to
//! `org.apache.kafka.common.record.internal`. Per CLAUDE.md §2, a package whose
//! name contains `internal` maps to a `pub(crate)` Rust module.

mod abstract_records;
mod compression_ratio_estimator;
mod compression_type;
mod control_record_type;
mod default_record;
mod default_record_batch;
mod memory_records;
pub(crate) mod memory_records_builder;
mod record;
mod record_batch;
mod record_version;
mod simple_record;

pub(crate) use abstract_records::AbstractRecords;
pub(crate) use compression_ratio_estimator::CompressionRatioEstimator;
pub(crate) use compression_type::CompressionType;
pub(crate) use control_record_type::ControlRecordType;
pub(crate) use default_record::{DefaultRecord, DefaultRecordRef};
pub(crate) use default_record_batch::{DefaultRecordBatch, DefaultRecordBatchRef};
pub(crate) use memory_records::BatchIterator;
// Rust-only options types for `MemoryRecords::with_records_with_options` and
// `MemoryRecords::builder_with_options` (CLAUDE.md §2's cap on derived overload
// names). `pub(crate)`, not `pub`, for the same reason as the `SimpleRecord`
// ones below. Re-exported here per §2 so callers reach them through the parent
// module rather than the file module path. Only unit tests build these forms
// today, hence `#[cfg(test)]`; the `*Options` structs themselves are never
// named outside `memory_records.rs`, where they are already in scope.
pub use memory_records::MemoryRecords;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use memory_records::{
    MemoryRecordsBuilderOptions, MemoryRecordsBuilderOptionsBuilder, MemoryRecordsOptions, MemoryRecordsOptionsBuilder,
};
pub(crate) use memory_records_builder::MemoryRecordsBuilder;
pub(crate) use record::Record;
pub(crate) use record_batch::RecordBatch;
pub(crate) use record_version::RecordVersion;
pub use simple_record::SimpleRecord;
// Rust-only options types for `SimpleRecord::with_options` (CLAUDE.md §2's cap
// on derived overload names). `pub(crate)`, not `pub`: the narrow public
// re-export documented in `common::record` covers only the two Java class
// names. Re-exported here per §2 so callers reach them through the parent
// module rather than the file module path. Only the unit tests in
// `default_record_batch` and `fetch_request_manager` build the four-parameter
// form today, hence `#[cfg(test)]`, and `SimpleRecordOptions` itself is never
// named outside `simple_record.rs`, where the type is already in scope.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use simple_record::{SimpleRecordOptions, SimpleRecordOptionsBuilder};
