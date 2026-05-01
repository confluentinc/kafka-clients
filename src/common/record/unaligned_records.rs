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

//! Translation of `org.apache.kafka.common.record.UnalignedRecords`.

use crate::common::record::TransferableRecords;

/// Represents a record set which is not necessarily offset-aligned, and is only
/// used when fetching raft snapshot.
///
/// Mirrors Java's `UnalignedRecords` interface. Java declares a `default`
/// method `toSend()` returning `RecordsSend<? extends BaseRecords>`; the Rust
/// translation places `to_send` on the concrete impls (the `RecordsSend`
/// generic does not survive trait-object boxing because it requires its
/// type parameter `R: TransferableRecords` to be `Sized`).
pub trait UnalignedRecords: TransferableRecords {}
