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

//! Translated from `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.

use std::collections::HashSet;
use std::fmt;

use crate::common::TopicPartition;
use crate::common::kafka_error::{ErrorCode, ErrorHierarchy, ErrorMessage};

/// No offset is defined for one or more partitions and no reset policy is set.
///
/// Corresponds to Java's `NoOffsetForPartitionException`. It has no entry in
/// `Errors`, so it carries no protocol code.
///
/// Java `extends` chain:
///    `NoOffsetForPartitionException` -> `InvalidOffsetException` ->
///   `KafkaException`
///
/// The `InvalidOffsetException` here is the **consumer package's** abstract
/// class, which extends `KafkaException` — not the concrete
/// [`InvalidOffsetError`](crate::common::errors::InvalidOffsetError) of
/// `common.errors`, which extends `ApiException`. Hence
/// [`is_consumer_invalid_offset_error`](ErrorHierarchy::is_consumer_invalid_offset_error)
/// rather than `is_invalid_offset_error`.
#[derive(Clone, Debug)]
pub struct ConsumerNoOffsetForPartitionError {
    message: String,
    /// The partitions with no defined offset.
    pub partitions: HashSet<TopicPartition>,
}

impl ConsumerNoOffsetForPartitionError {
    /// Create the error for a single partition, mirroring Java's
    /// `NoOffsetForPartitionException(TopicPartition)`.
    ///
    /// Java's single-partition constructor renders the **singular** "partition:"
    /// while the collection constructor renders "partitions:"; both forms are
    /// asserted by tests, so they are built separately rather than routed
    /// through one another.
    pub fn new(partition: TopicPartition) -> Self {
        let message = format!("Undefined offset with no reset policy for partition: {partition}");
        Self { message, partitions: HashSet::from([partition]) }
    }

    /// Create the error for a set of partitions, mirroring Java's
    /// `NoOffsetForPartitionException(Collection<TopicPartition>)`.
    pub fn for_partitions(partitions: impl IntoIterator<Item = TopicPartition>) -> Self {
        let partitions: HashSet<TopicPartition> = partitions.into_iter().collect();
        let mut items: Vec<String> = partitions.iter().map(ToString::to_string).collect();
        items.sort();
        let message = format!("Undefined offset with no reset policy for partitions: [{}]", items.join(", "));
        Self { message, partitions }
    }

    /// The partitions with no defined offset.
    pub fn partitions(&self) -> &HashSet<TopicPartition> {
        &self.partitions
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConsumerNoOffsetForPartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerNoOffsetForPartitionError: {}", self.message)
    }
}

impl ErrorMessage for ConsumerNoOffsetForPartitionError {
    fn message(&self) -> &str {
        &self.message
    }
}

impl ErrorCode for ConsumerNoOffsetForPartitionError {}

impl ErrorHierarchy for ConsumerNoOffsetForPartitionError {
    fn is_kafka_error(&self) -> bool {
        true
    }
    fn is_consumer_invalid_offset_error(&self) -> bool {
        true
    }
}
