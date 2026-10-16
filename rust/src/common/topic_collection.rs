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
//! Corresponds to `org.apache.kafka.common.TopicCollection`. Its nested
//! subclasses, [`TopicIdCollection`] and [`TopicNameCollection`], are reached
//! through this module, as CLAUDE.md §2 provides for nested classes.

use crate::common::Uuid;

/// A collection of topics. This collection may define topics by name or ID.
///
/// Corresponds to `org.apache.kafka.common.TopicCollection`. Java models this
/// as an abstract class with `TopicIdCollection` / `TopicNameCollection`
/// subclasses; in Rust the closed set of subclasses is an enum whose variants
/// wrap the subclasses, so callers `match` on the variant instead of using
/// `instanceof` and read the topics through the subclass's getter. Keeping the
/// subclass behind the variant, rather than its `Vec`, lets a field Java adds
/// to the subclass arrive without changing the variant's shape.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.TopicCollection")]
pub enum TopicCollection {
    /// A collection of topics defined by their topic ID
    /// (`TopicCollection.TopicIdCollection`).
    TopicIds(TopicIdCollection),
    /// A collection of topics defined by their topic name
    /// (`TopicCollection.TopicNameCollection`).
    TopicNames(TopicNameCollection),
}

impl TopicCollection {
    /// Returns a collection of topics defined by topic ID.
    ///
    /// Translated from `TopicCollection.ofTopicIds`.
    #[doc(alias = "org.apache.kafka.common.TopicCollection#ofTopicIds")]
    pub fn of_topic_ids(topics: Vec<Uuid>) -> TopicCollection {
        TopicCollection::TopicIds(TopicIdCollection::new(topics))
    }

    /// Returns a collection of topics defined by topic name.
    ///
    /// Translated from `TopicCollection.ofTopicNames`.
    #[doc(alias = "org.apache.kafka.common.TopicCollection#ofTopicNames")]
    pub fn of_topic_names(topics: Vec<String>) -> TopicCollection {
        TopicCollection::TopicNames(TopicNameCollection::new(topics))
    }
}

/// A class used to represent a collection of topics defined by their topic ID.
/// Subclassing this class beyond the classes within this file is not supported.
///
/// Corresponds to `org.apache.kafka.common.TopicCollection.TopicIdCollection`.
/// Built only by [`TopicCollection::of_topic_ids`], as Java's constructor is
/// private.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.TopicCollection$TopicIdCollection")]
pub struct TopicIdCollection {
    topic_ids: Vec<Uuid>,
}

impl TopicIdCollection {
    /// Translated from the private constructor `TopicIdCollection(Collection<Uuid>)`.
    #[doc(alias = "org.apache.kafka.common.TopicCollection$TopicIdCollection#TopicIdCollection")]
    fn new(topic_ids: Vec<Uuid>) -> Self {
        Self { topic_ids }
    }

    /// Returns the topic IDs of the collection, in the order they were given.
    ///
    /// Translated from `TopicIdCollection.topicIds()`, which returns an
    /// unmodifiable view; here a shared slice.
    #[doc(alias = "org.apache.kafka.common.TopicCollection$TopicIdCollection#topicIds")]
    pub fn topic_ids(&self) -> &[Uuid] {
        &self.topic_ids
    }
}

/// A class used to represent a collection of topics defined by their topic name.
/// Subclassing this class beyond the classes within this file is not supported.
///
/// Corresponds to `org.apache.kafka.common.TopicCollection.TopicNameCollection`.
/// Built only by [`TopicCollection::of_topic_names`], as Java's constructor is
/// private.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.TopicCollection$TopicNameCollection")]
pub struct TopicNameCollection {
    topic_names: Vec<String>,
}

impl TopicNameCollection {
    /// Translated from the private constructor `TopicNameCollection(Collection<String>)`.
    #[doc(alias = "org.apache.kafka.common.TopicCollection$TopicNameCollection#TopicNameCollection")]
    fn new(topic_names: Vec<String>) -> Self {
        Self { topic_names }
    }

    /// Returns the topic names of the collection, in the order they were given.
    ///
    /// Translated from `TopicNameCollection.topicNames()`, which returns an
    /// unmodifiable view; here a shared slice.
    #[doc(alias = "org.apache.kafka.common.TopicCollection$TopicNameCollection#topicNames")]
    pub fn topic_names(&self) -> &[String] {
        &self.topic_names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_topic_ids() {
        let ids = vec![Uuid::new(1, 2), Uuid::new(3, 4)];
        let collection = TopicCollection::of_topic_ids(ids.clone());
        let TopicCollection::TopicIds(by_id) = &collection else {
            panic!("ofTopicIds builds a TopicIdCollection, got {collection:?}");
        };
        assert_eq!(by_id.topic_ids(), ids.as_slice());
        assert_eq!(collection, TopicCollection::of_topic_ids(ids));
    }

    #[test]
    fn of_topic_names() {
        let names = vec!["a".to_string(), "b".to_string()];
        let collection = TopicCollection::of_topic_names(names.clone());
        let TopicCollection::TopicNames(by_name) = &collection else {
            panic!("ofTopicNames builds a TopicNameCollection, got {collection:?}");
        };
        assert_eq!(by_name.topic_names(), names.as_slice());
        assert_eq!(collection, TopicCollection::of_topic_names(names));
    }

    /// Translated from `TopicCollectionTest.testTopicCollection`
    /// (`clients/admin/TopicCollectionTest.java`): the collections hand back
    /// the topics they were built from.
    #[test]
    fn test_topic_collection() {
        let topic_ids = vec![Uuid::random_uuid(), Uuid::random_uuid(), Uuid::random_uuid()];
        let topic_names = vec!["foo".to_string(), "bar".to_string()];

        let TopicCollection::TopicIds(id_collection) = TopicCollection::of_topic_ids(topic_ids.clone()) else {
            panic!("ofTopicIds builds a TopicIdCollection");
        };
        let TopicCollection::TopicNames(name_collection) = TopicCollection::of_topic_names(topic_names.clone())
        else {
            panic!("ofTopicNames builds a TopicNameCollection");
        };

        assert!(topic_ids.iter().all(|id| id_collection.topic_ids().contains(id)));
        assert!(topic_names.iter().all(|name| name_collection.topic_names().contains(name)));
    }

    #[test]
    fn collections_differ_by_kind_and_content() {
        assert_ne!(
            TopicCollection::of_topic_names(vec!["a".to_string()]),
            TopicCollection::of_topic_names(vec!["b".to_string()])
        );
        assert_ne!(
            TopicCollection::of_topic_ids(vec![Uuid::new(1, 2)]),
            TopicCollection::of_topic_names(Vec::new())
        );
    }
}
