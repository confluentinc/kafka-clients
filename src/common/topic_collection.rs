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

//! A collection of topics defined by name or ID.
//!
//! Corresponds to `org.apache.kafka.common.TopicCollection`.

use crate::common::Uuid;

/// A collection of topics. This collection may define topics by name or ID.
///
/// Corresponds to `org.apache.kafka.common.TopicCollection`. Java models this
/// as an abstract class with `TopicIdCollection` / `TopicNameCollection`
/// subclasses; in Rust the closed set of subclasses is an enum, so callers
/// `match` on the variant instead of using `instanceof`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicCollection {
    /// A collection of topics defined by their topic ID
    /// (`TopicCollection.TopicIdCollection`).
    TopicIds(Vec<Uuid>),
    /// A collection of topics defined by their topic name
    /// (`TopicCollection.TopicNameCollection`).
    TopicNames(Vec<String>),
}

impl TopicCollection {
    /// Returns a collection of topics defined by topic ID.
    ///
    /// Translated from `TopicCollection.ofTopicIds`.
    pub fn of_topic_ids(topics: Vec<Uuid>) -> TopicCollection {
        TopicCollection::TopicIds(topics)
    }

    /// Returns a collection of topics defined by topic name.
    ///
    /// Translated from `TopicCollection.ofTopicNames`.
    pub fn of_topic_names(topics: Vec<String>) -> TopicCollection {
        TopicCollection::TopicNames(topics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_topic_ids() {
        let ids = vec![Uuid::new(1, 2), Uuid::new(3, 4)];
        let collection = TopicCollection::of_topic_ids(ids.clone());
        assert_eq!(collection, TopicCollection::TopicIds(ids));
    }

    #[test]
    fn of_topic_names() {
        let names = vec!["a".to_string(), "b".to_string()];
        let collection = TopicCollection::of_topic_names(names.clone());
        assert_eq!(collection, TopicCollection::TopicNames(names));
    }
}
