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

//! Data holders for the consumer partition assignor protocol.
//!
//! Translated from the `Subscription` and `Assignment` nested data classes of
//! `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor`.
//!
//! Only the two data holders are in scope here: they are consumed by
//! [`ConsumerProtocol`](crate::consumer::internals::consumer_protocol::ConsumerProtocol)
//! for (de)serializing member subscriptions/assignments, which the admin
//! group-describe path relies on. The `ConsumerPartitionAssignor` trait itself
//! and the client-side assignors (`RangeAssignor`, `StickyAssignor`, ...) are
//! out of scope for Milestone 8/11 (KIP-848 uses server-side assignment) — see
//! `consumer-threading.md` §20.

use crate::common::TopicPartition;

/// The default generation id used when a subscription carries no generation.
///
/// Corresponds to `AbstractStickyAssignor.DEFAULT_GENERATION`.
pub const DEFAULT_GENERATION: i32 = -1;

/// A consumer member's subscription.
///
/// Corresponds to `ConsumerPartitionAssignor.Subscription`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    topics: Vec<String>,
    user_data: Option<Vec<u8>>,
    owned_partitions: Vec<TopicPartition>,
    group_instance_id: Option<String>,
    generation_id: Option<i32>,
    rack_id: Option<String>,
}

impl Subscription {
    /// Creates a subscription with all fields.
    ///
    /// A `generation_id` less than zero is mapped to `None`, matching Java's
    /// `generationId < 0 ? Optional.empty() : Optional.of(generationId)`.
    pub fn new(
        topics: Vec<String>,
        user_data: Option<Vec<u8>>,
        owned_partitions: Vec<TopicPartition>,
        generation_id: i32,
        rack_id: Option<String>,
    ) -> Self {
        Self {
            topics,
            user_data,
            owned_partitions,
            group_instance_id: None,
            generation_id: if generation_id < 0 { None } else { Some(generation_id) },
            rack_id,
        }
    }

    /// Creates a subscription from topics, optional user data and owned
    /// partitions (default generation, no rack).
    ///
    /// Mirrors `Subscription(List, ByteBuffer, List)`.
    pub fn with_owned_partitions(
        topics: Vec<String>,
        user_data: Option<Vec<u8>>,
        owned_partitions: Vec<TopicPartition>,
    ) -> Self {
        Self::new(topics, user_data, owned_partitions, DEFAULT_GENERATION, None)
    }

    /// Creates a subscription from topics and optional user data.
    ///
    /// Mirrors `Subscription(List, ByteBuffer)`.
    pub fn with_user_data(topics: Vec<String>, user_data: Option<Vec<u8>>) -> Self {
        Self::new(topics, user_data, Vec::new(), DEFAULT_GENERATION, None)
    }

    /// Creates a subscription from topics only.
    ///
    /// Mirrors `Subscription(List)`.
    pub fn with_topics(topics: Vec<String>) -> Self {
        Self::new(topics, None, Vec::new(), DEFAULT_GENERATION, None)
    }

    /// The subscribed topics.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// The opaque user data attached to the subscription.
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }

    /// The partitions currently owned by the member.
    pub fn owned_partitions(&self) -> &[TopicPartition] {
        &self.owned_partitions
    }

    /// The rack id of the member, if any.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Sets the group instance id.
    pub fn set_group_instance_id(&mut self, group_instance_id: Option<String>) {
        self.group_instance_id = group_instance_id;
    }

    /// The group instance id of the member, if any.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// The generation id of the subscription, if any.
    pub fn generation_id(&self) -> Option<i32> {
        self.generation_id
    }
}

/// A consumer member's assignment.
///
/// Corresponds to `ConsumerPartitionAssignor.Assignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    partitions: Vec<TopicPartition>,
    user_data: Option<Vec<u8>>,
}

impl Assignment {
    /// Creates an assignment with partitions and optional user data.
    pub fn new(partitions: Vec<TopicPartition>, user_data: Option<Vec<u8>>) -> Self {
        Self { partitions, user_data }
    }

    /// Creates an assignment from partitions only.
    ///
    /// Mirrors `Assignment(List)`.
    pub fn with_partitions(partitions: Vec<TopicPartition>) -> Self {
        Self::new(partitions, None)
    }

    /// The assigned partitions.
    pub fn partitions(&self) -> &[TopicPartition] {
        &self.partitions
    }

    /// The opaque user data attached to the assignment.
    pub fn user_data(&self) -> Option<&[u8]> {
        self.user_data.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_negative_generation_is_none() {
        let s = Subscription::new(vec!["t".to_string()], None, Vec::new(), -1, None);
        assert_eq!(s.generation_id(), None);
    }

    #[test]
    fn subscription_non_negative_generation_is_some() {
        let s = Subscription::new(vec!["t".to_string()], None, Vec::new(), 5, Some("r".to_string()));
        assert_eq!(s.generation_id(), Some(5));
        assert_eq!(s.rack_id(), Some("r"));
    }

    #[test]
    fn subscription_defaults() {
        let s = Subscription::with_topics(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(s.topics(), &["a".to_string(), "b".to_string()]);
        assert!(s.user_data().is_none());
        assert!(s.owned_partitions().is_empty());
        assert_eq!(s.generation_id(), None);
    }

    #[test]
    fn assignment_accessors() {
        let a = Assignment::with_partitions(vec![TopicPartition::new("t", 0)]);
        assert_eq!(a.partitions().len(), 1);
        assert!(a.user_data().is_none());
    }
}
