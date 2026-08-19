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

//! Translated from `org.apache.kafka.clients.consumer.LogTruncationException`.

use std::collections::HashMap;
use std::fmt;

use crate::common::TopicPartition;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage};
use crate::consumer::OffsetAndMetadata;

/// Log truncation was detected: the broker's log diverges from the offsets the
/// consumer had fetched.
///
/// Corresponds to Java's `LogTruncationException`. It has no entry in `Errors`,
/// so it carries no protocol code.
///
/// Java `extends` chain:
///    `LogTruncationException` -> `OffsetOutOfRangeException` ->
///   `InvalidOffsetException` -> `KafkaException`
///
/// The `OffsetOutOfRangeException` in that chain is the consumer package's, so
/// this is a
/// [`ConsumerOffsetOutOfRangeError`](super::ConsumerOffsetOutOfRangeError) too —
/// which is why it carries the same `offset_out_of_range_partitions` map.
#[derive(Clone, Debug)]
pub struct ConsumerLogTruncationError {
    message: String,
    /// The out-of-range offset per partition, inherited from the parent class.
    pub offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
    /// The divergent offset per partition.
    pub divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
}

impl ConsumerLogTruncationError {
    /// Mirrors Java's
    /// `LogTruncationException(Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndMetadata>)`,
    /// which composes the message from the divergent offsets:
    /// `"Truncated partitions detected with divergent offsets " + divergentOffsets`.
    pub fn new(
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        let mut items: Vec<String> = divergent_offsets.iter().map(|(k, v)| format!("{k}={v}")).collect();
        items.sort();
        let message = format!("Truncated partitions detected with divergent offsets {{{}}}", items.join(", "));
        Self { message, offset_out_of_range_partitions, divergent_offsets }
    }

    /// Mirrors Java's three-argument
    /// `LogTruncationException(String, Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndMetadata>)`.
    pub fn with_message(
        message: impl Into<String>,
        offset_out_of_range_partitions: HashMap<TopicPartition, i64>,
        divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Self {
        Self { message: message.into(), offset_out_of_range_partitions, divergent_offsets }
    }

    /// The out-of-range offset per partition.
    pub fn offset_out_of_range_partitions(&self) -> &HashMap<TopicPartition, i64> {
        &self.offset_out_of_range_partitions
    }

    /// The divergent offset per partition.
    pub fn divergent_offsets(&self) -> &HashMap<TopicPartition, OffsetAndMetadata> {
        &self.divergent_offsets
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerLogTruncationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerLogTruncationError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerLogTruncationError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerLogTruncationError {}

impl ErrorHierarchy for ConsumerLogTruncationError {
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
