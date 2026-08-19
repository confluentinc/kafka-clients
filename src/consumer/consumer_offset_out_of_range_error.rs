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

//! Translated from `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
//!
//! Named with the `Consumer` prefix because `org.apache.kafka.common.errors`
//! has a *different* class of the same name, already translated as
//! [`OffsetOutOfRangeError`](crate::common::errors::OffsetOutOfRangeError). The
//! two differ in ancestry: that one extends `ApiException`, this one does not.
//! Java tells them apart by package; [`Error`](crate::common::Error) has one
//! flat variant list, so the package moves into the name.

use std::collections::HashMap;
use std::fmt;

use crate::common::TopicPartition;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage};

/// A fetch asked for an offset outside the range the broker retains.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.OffsetOutOfRangeException`.
/// It has no entry in `Errors`, so it carries no protocol code — the wire code
/// `OFFSET_OUT_OF_RANGE` builds the `common.errors` class instead.
///
/// Java `extends` chain:
///    `OffsetOutOfRangeException` -> `InvalidOffsetException` -> `KafkaException`
#[derive(Clone, Debug)]
pub struct ConsumerOffsetOutOfRangeError {
    message: String,
    /// The out-of-range offset per partition.
    pub offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
}

impl ConsumerOffsetOutOfRangeError {
    /// Create the error, mirroring Java's
    /// `OffsetOutOfRangeException(Map<TopicPartition, Long>)`.
    pub fn new(offset_out_of_range_partitions: HashMap<TopicPartition, i64>) -> Self {
        let mut items: Vec<String> = offset_out_of_range_partitions.iter().map(|(k, v)| format!("{k}={v}")).collect();
        items.sort();
        let message = format!(
            "Offsets out of range with no configured reset policy for partitions: {{{}}}",
            items.join(", ")
        );
        Self { message, offset_out_of_range_partitions }
    }

    /// Create the error with a caller-supplied message, mirroring Java's
    /// `OffsetOutOfRangeException(String, Map<TopicPartition, Long>)`.
    pub fn with_message(
        message: impl Into<String>,
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    ) -> Self {
        Self { message: message.into(), offset_out_of_range_partitions }
    }

    /// The out-of-range offset per partition.
    pub fn offset_out_of_range_partitions(&self) -> &HashMap<TopicPartition, i64> {
        &self.offset_out_of_range_partitions
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerOffsetOutOfRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerOffsetOutOfRangeError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerOffsetOutOfRangeError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerOffsetOutOfRangeError {}

impl ErrorHierarchy for ConsumerOffsetOutOfRangeError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_consumer_invalid_offset_error(&self) -> bool {
        true
    }
    fn is_consumer_offset_out_of_range_error(&self) -> bool {
        true
    }
}
