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

//! Translation of `org.apache.kafka.common.TopicCollection`.

use crate::common::Uuid;

/// A collection of topics, defined either by name or by topic id.
///
/// Mirrors Java's sealed `TopicCollection` abstract class with two static
/// factory methods producing the `TopicNameCollection` and
/// `TopicIdCollection` subclasses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicCollection {
    /// Collection defined by topic ids.
    OfTopicIds(TopicIdCollection),
    /// Collection defined by topic names.
    OfTopicNames(TopicNameCollection),
}

impl TopicCollection {
    /// Return a collection of topics defined by topic id.
    pub fn of_topic_ids<I: IntoIterator<Item = Uuid>>(topics: I) -> TopicIdCollection {
        TopicIdCollection::new(topics)
    }

    /// Return a collection of topics defined by topic name.
    pub fn of_topic_names<I, S>(topics: I) -> TopicNameCollection
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        TopicNameCollection::new(topics.into_iter().map(Into::into))
    }
}

/// A collection of topics defined by their topic id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicIdCollection {
    topic_ids: Vec<Uuid>,
}

impl TopicIdCollection {
    fn new<I: IntoIterator<Item = Uuid>>(topic_ids: I) -> Self {
        Self { topic_ids: topic_ids.into_iter().collect() }
    }

    /// A collection of topic ids. The slice is read-only — Rust's
    /// borrow checker provides the unmodifiability that Java enforced
    /// with `Collections.unmodifiableCollection`.
    pub fn topic_ids(&self) -> &[Uuid] {
        &self.topic_ids
    }
}

impl From<TopicIdCollection> for TopicCollection {
    fn from(c: TopicIdCollection) -> Self {
        TopicCollection::OfTopicIds(c)
    }
}

/// A collection of topics defined by their topic name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicNameCollection {
    topic_names: Vec<String>,
}

impl TopicNameCollection {
    fn new<I: IntoIterator<Item = String>>(topic_names: I) -> Self {
        Self { topic_names: topic_names.into_iter().collect() }
    }

    /// A collection of topic names.
    pub fn topic_names(&self) -> &[String] {
        &self.topic_names
    }
}

impl From<TopicNameCollection> for TopicCollection {
    fn from(c: TopicNameCollection) -> Self {
        TopicCollection::OfTopicNames(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_collection_via_factories() {
        // Translation of clients/admin/TopicCollectionTest.testTopicCollection.
        let topic_ids = vec![Uuid::random(), Uuid::random(), Uuid::random()];
        let topic_names: Vec<&str> = vec!["foo", "bar"];

        let id_collection = TopicCollection::of_topic_ids(topic_ids.iter().copied());
        let name_collection = TopicCollection::of_topic_names(topic_names.iter().copied());

        for id in &topic_ids {
            assert!(id_collection.topic_ids().contains(id));
        }
        for n in &topic_names {
            assert!(name_collection.topic_names().iter().any(|x| x == *n));
        }
    }

    #[test]
    fn enum_variants_round_trip() {
        let id_collection = TopicCollection::of_topic_ids([Uuid::random()]);
        let wrapped: TopicCollection = id_collection.clone().into();
        assert!(matches!(wrapped, TopicCollection::OfTopicIds(_)));

        let name_collection = TopicCollection::of_topic_names(["abc".to_string()]);
        let wrapped: TopicCollection = name_collection.clone().into();
        assert!(matches!(wrapped, TopicCollection::OfTopicNames(_)));
    }
}
