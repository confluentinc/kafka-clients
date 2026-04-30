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

//! Translation of `org.apache.kafka.common.record.AbstractRecordBatch`.
//!
//! Java's `AbstractRecordBatch` is a package-private abstract class providing
//! default implementations for three `RecordBatch` methods:
//!
//! * `hasProducerId()`        ↦ `RecordBatch::has_producer_id` default body
//! * `nextOffset()`           ↦ `RecordBatch::next_offset` default body
//! * `isCompressed()`         ↦ `RecordBatch::is_compressed` default body
//!
//! Rust traits support default method bodies natively, so the abstract class
//! collapses into the parent trait. This file exists so the file-per-class
//! mapping required by CLAUDE.md rule 2 is preserved; downstream Phase 3c
//! impls implement [`crate::common::record::RecordBatch`] directly and inherit
//! the defaults transparently.
