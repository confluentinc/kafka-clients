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

//! A listing of a topic in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TopicListing`.

use crate::common::Uuid;

/// A listing of a topic in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.TopicListing`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicListing {
    name: String,
    topic_id: Uuid,
    internal: bool,
}

impl TopicListing {
    /// Create an instance with the specified parameters.
    ///
    /// * `name` - the topic name
    /// * `topic_id` - the topic id
    /// * `internal` - whether the topic is internal to Kafka
    pub fn new(name: impl Into<String>, topic_id: Uuid, internal: bool) -> Self {
        Self { name: name.into(), topic_id, internal }
    }

    /// The id of the topic.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// The name of the topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the topic is internal to Kafka. An example of an internal topic
    /// is the offsets and group management topic: `__consumer_offsets`.
    pub fn is_internal(&self) -> bool {
        self.internal
    }
}

impl std::fmt::Display for TopicListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(name={}, topicId={}, internal={})", self.name, self.topic_id, self.internal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors() {
        let id = Uuid::new(1, 2);
        let listing = TopicListing::new("t", id, true);
        assert_eq!(listing.name(), "t");
        assert_eq!(listing.topic_id(), id);
        assert!(listing.is_internal());
    }
}
