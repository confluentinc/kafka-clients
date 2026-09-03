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

pub mod timestamp_type;

pub(crate) mod internal;
pub use timestamp_type::TimestampType;

// DoD #7 visibility deviation: `MemoryRecords` and `SimpleRecord` retain a
// public re-export even though their module is `internal`/`pub(crate)`.
//
// The crate-external message-serde integration test `RecordsSerdeTest`
// (`tests/common/message/records_serde_test.rs`) builds a record set with
// `MemoryRecords::with_records(..)` / `SimpleRecord` to populate a `records`
// field, exactly as Java's `org.apache.kafka.common.message.RecordsSerdeTest`
// does. That test cannot move in-crate: its `SimpleRecordsMessageData` helper is
// generated only into the integration-test crate's `OUT_DIR`, not the library.
// In Java these classes are `public` even inside the `record.internal` package
// (the `internal` package name is a convention, not an access modifier), so a
// public re-export here is Java-faithful. No other moved type is re-exported
// publicly; everything else stays `pub(crate)` behind `internal`.
pub use internal::memory_records::MemoryRecords;
pub use internal::simple_record::SimpleRecord;
