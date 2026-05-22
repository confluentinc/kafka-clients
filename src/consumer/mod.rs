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

//! Consumer types (org.apache.kafka.clients.consumer).
//!
//! Translated from `org.apache.kafka.clients.consumer`. The `clients` Java
//! package segment is intentionally dropped per CLAUDE.md §2.

pub mod close_options;
pub mod consumer_record;
pub mod consumer_records;
pub mod group_protocol;
pub mod offset_and_metadata;
pub mod offset_and_timestamp;
pub mod offset_reset_strategy;
pub mod subscription_pattern;

pub use close_options::{CloseOptions, GroupMembershipOperation};
pub use consumer_record::{ConsumerRecord, NO_TIMESTAMP, NULL_SIZE};
pub use consumer_records::ConsumerRecords;
pub use group_protocol::GroupProtocol;
pub use offset_and_metadata::OffsetAndMetadata;
pub use offset_and_timestamp::OffsetAndTimestamp;
#[allow(deprecated)]
pub use offset_reset_strategy::OffsetResetStrategy;
pub use subscription_pattern::SubscriptionPattern;
