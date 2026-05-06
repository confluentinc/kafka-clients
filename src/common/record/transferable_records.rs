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

//! Translation of `org.apache.kafka.common.record.TransferableRecords`.

use crate::common::record::BaseRecords;

/// Represents a record set which can be transferred to a channel.
///
/// Mirrors Java's `TransferableRecords` interface. The Java interface adds
/// one method:
///
/// ```text
/// int writeTo(TransferableChannel channel, int position, int length) throws IOException
/// ```
///
/// `TransferableChannel` is the network layer's wrapper over Java's
/// `GatheringByteChannel` and lives in `common/network/*`, which Phase 5
/// translates. Adding `write_to` here in Phase 3a would require either
/// inventing a new `TransferableChannel` Rust type before Phase 5 wires it,
/// or stubbing the type — both forbidden by CLAUDE.md DoD #7. We therefore
/// keep [`TransferableRecords`] empty over [`BaseRecords`] for now; Phase 5
/// will add the `write_to` method to this trait.
pub trait TransferableRecords: BaseRecords {}
