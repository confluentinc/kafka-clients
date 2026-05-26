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

//! Translation of `org.apache.kafka.common.record.BaseRecords`.

/// Base interface for accessing records which could be contained in the log
/// or an in-memory materialization of log records.
///
/// Mirrors Java's `BaseRecords` interface. Java exposes a second method
/// `toSend()` returning a `RecordsSend<? extends BaseRecords>`; the
/// `RecordsSend` type lives in Phase 3d, so this trait restricts itself to
/// `size_in_bytes()` for now. Phase 3d will add `to_send` as a default trait
/// method (or extension trait) once `RecordsSend` exists.
pub trait BaseRecords {
    /// Size in bytes of these records.
    fn size_in_bytes(&self) -> i32;
}
