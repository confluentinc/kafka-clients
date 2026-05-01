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

//! Translation of `org.apache.kafka.common.record`.
//!
//! Phase 3a covers the enum / identifier types and the trait base layer.
//! Phase 3c/3d will fill in `MemoryRecords`, `MemoryRecordsBuilder`,
//! `DefaultRecord`, `DefaultRecordBatch`, and the on-the-wire send path.

pub mod abstract_record_batch;
pub mod abstract_records;
pub mod base_records;
pub(crate) mod byte_buffer_log_input_stream;
pub mod compression_ratio_estimator;
pub mod compression_type;
pub mod control_record_type;
pub mod default_record;
pub mod default_record_batch;
pub mod default_records_send;
pub(crate) mod log_input_stream;
pub mod memory_records;
pub mod mutable_record_batch;
pub mod partial_default_record;
// CLAUDE.md rule 2 mandates each Java class lives in its own file (so the
// `Record` trait lives in `record/record.rs`). Clippy's `module_inception`
// lint would otherwise flag the same-name child module.
#[allow(clippy::module_inception)]
pub mod record;
pub mod record_batch;
pub(crate) mod record_batch_iterator;
pub mod record_validation_stats;
pub mod record_version;
pub mod records;
pub mod records_send;
pub mod simple_record;
pub mod timestamp_type;
pub mod transferable_records;
pub mod unaligned_memory_records;
pub mod unaligned_records;

pub use base_records::BaseRecords;
pub use compression_type::CompressionType;
pub use control_record_type::ControlRecordType;
pub use default_record::DefaultRecord;
pub use default_record_batch::DefaultRecordBatch;
pub use default_records_send::DefaultRecordsSend;
pub use memory_records::MemoryRecords;
pub use mutable_record_batch::MutableRecordBatch;
pub use partial_default_record::PartialDefaultRecord;
pub use record::Record;
pub use record_batch::RecordBatch;
pub use record_validation_stats::RecordValidationStats;
pub use record_version::RecordVersion;
pub use records::Records;
pub use records_send::RecordsSend;
pub use simple_record::SimpleRecord;
pub use timestamp_type::TimestampType;
pub use transferable_records::TransferableRecords;
pub use unaligned_memory_records::UnalignedMemoryRecords;
pub use unaligned_records::UnalignedRecords;
