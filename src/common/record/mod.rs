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

//! Record types for Kafka (org.apache.kafka.common.record).
//!
//! Contains the record batch constants, timestamp types, compression types,
//! record version, and compression ratio estimation.

pub mod compression_ratio_estimator;
pub mod compression_type;
pub mod record_batch;
pub mod record_version;
pub mod timestamp_type;

pub use compression_ratio_estimator::CompressionRatioEstimator;
pub use compression_type::CompressionType;
pub use record_batch::RecordBatch;
pub use record_version::RecordVersion;
pub use timestamp_type::TimestampType;
