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
pub mod compression_ratio_estimator;
pub mod compression_type;
pub mod control_record_type;
pub mod default_record;
pub mod mutable_record_batch;
pub mod partial_default_record;
// CLAUDE.md rule 2 mandates each Java class lives in its own file (so the
// `Record` trait lives in `record/record.rs`). Clippy's `module_inception`
// lint would otherwise flag the same-name child module.
#[allow(clippy::module_inception)]
pub mod record;
pub mod record_batch;
pub mod record_version;
pub mod records;
pub mod simple_record;
pub mod timestamp_type;
pub mod transferable_records;

pub use base_records::BaseRecords;
pub use compression_type::CompressionType;
pub use control_record_type::ControlRecordType;
pub use default_record::DefaultRecord;
pub use mutable_record_batch::MutableRecordBatch;
pub use partial_default_record::PartialDefaultRecord;
pub use record::Record;
pub use record_batch::RecordBatch;
pub use record_version::RecordVersion;
pub use records::Records;
pub use simple_record::SimpleRecord;
pub use timestamp_type::TimestampType;
pub use transferable_records::TransferableRecords;
