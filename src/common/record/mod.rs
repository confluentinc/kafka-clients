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

//! Provides utility components related to Kafka records
//! (`org.apache.kafka.common.record`).
//!
//! In Kafka 4.3.1 (KAFKA-20128) the low-level record and record-batch
//! representation moved to the `internal` submodule
//! (`org.apache.kafka.common.record.internal`). Only [`TimestampType`] stays in
//! this package. [`InvalidRecordError`] mirrors
//! `org.apache.kafka.common.InvalidRecordException` (never a `record`-package
//! type in Java; kept here for its long-standing Rust home).

mod timestamp_type;

pub(crate) mod internal;
pub use timestamp_type::TimestampType;
