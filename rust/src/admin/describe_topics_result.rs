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

//! The result of `Admin::describe_topics_with_topics`.

use std::collections::HashMap;
use std::hash::Hash;

use crate::admin::TopicDescription;
use crate::common::{KafkaFuture, Uuid};

/// The result of `Admin::describe_topics_with_topics`.
// Exactly one of the two maps is present, the one matching the kind of
// `TopicCollection` the request used.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult")]
pub struct DescribeTopicsResult {
    topic_id_futures: Option<HashMap<Uuid, KafkaFuture<TopicDescription>>>,
    name_futures: Option<HashMap<String, KafkaFuture<TopicDescription>>>,
}

impl DescribeTopicsResult {
    /// Creates a result keyed by topic id or by topic name.
    ///
    /// # Panics
    ///
    /// If both maps or neither are given, as Java throws
    /// `IllegalArgumentException`.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#DescribeTopicsResult")]
    pub(crate) fn with_topic_id_futures_name_futures(
        topic_id_futures: Option<HashMap<Uuid, KafkaFuture<TopicDescription>>>,
        name_futures: Option<HashMap<String, KafkaFuture<TopicDescription>>>,
    ) -> Self {
        assert!(
            topic_id_futures.is_none() || name_futures.is_none(),
            "topicIdFutures and nameFutures cannot both be specified."
        );
        assert!(
            topic_id_futures.is_some() || name_futures.is_some(),
            "topicIdFutures and nameFutures cannot both be null."
        );
        DescribeTopicsResult { topic_id_futures, name_futures }
    }

    /// Creates a result keyed by topic id.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#ofTopicIds")]
    pub(crate) fn of_topic_ids(topic_id_futures: HashMap<Uuid, KafkaFuture<TopicDescription>>) -> Self {
        Self::with_topic_id_futures_name_futures(Some(topic_id_futures), None)
    }

    /// Creates a result keyed by topic name.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#ofTopicNames")]
    pub(crate) fn of_topic_names(name_futures: HashMap<String, KafkaFuture<TopicDescription>>) -> Self {
        Self::with_topic_id_futures_name_futures(None, Some(name_futures))
    }

    /// Use when `Admin::describe_topics_with_topics_options` used a
    /// `TopicCollection` of ids.
    ///
    /// # Returns
    ///
    /// a map from topic IDs to futures which can be used to check the status of individual topics
    /// if the request used topic IDs, otherwise return `None`.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#topicIdValues")]
    pub fn topic_id_values(&self) -> Option<&HashMap<Uuid, KafkaFuture<TopicDescription>>> {
        self.topic_id_futures.as_ref()
    }

    /// Use when `Admin::describe_topics_with_topics_options` used a
    /// `TopicCollection` of names.
    ///
    /// # Returns
    ///
    /// a map from topic names to futures which can be used to check the status of individual topics
    /// if the request used topic names, otherwise return `None`.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#topicNameValues")]
    pub fn topic_name_values(&self) -> Option<&HashMap<String, KafkaFuture<TopicDescription>>> {
        self.name_futures.as_ref()
    }

    /// Returns a future map from topic names to descriptions which can be used to check the
    /// status of individual description if the describe topic request used topic names,
    /// otherwise `None`. This request succeeds only if all the topic descriptions succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#allTopicNames")]
    pub fn all_topic_names(&self) -> Option<KafkaFuture<HashMap<String, TopicDescription>>> {
        Self::all(self.name_futures.as_ref())
    }

    /// Returns a future map from topic ids to descriptions which can be used to check the
    /// status of individual description if the describe topic request used topic ids,
    /// otherwise `None`. This request succeeds only if all the topic descriptions succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#allTopicIds")]
    pub fn all_topic_ids(&self) -> Option<KafkaFuture<HashMap<Uuid, TopicDescription>>> {
        Self::all(self.topic_id_futures.as_ref())
    }

    /// Return a future which succeeds only if all the topic descriptions
    /// succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsResult#all")]
    fn all<T: Clone + Eq + Hash + Send + Sync + 'static>(
        futures: Option<&HashMap<T, KafkaFuture<TopicDescription>>>,
    ) -> Option<KafkaFuture<HashMap<T, TopicDescription>>> {
        let futures = futures?;
        Some(KafkaFuture::join_map(
            futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;

    fn description(name: &str) -> TopicDescription {
        TopicDescription::new(name, false, vec![])
    }

    #[tokio::test]
    async fn by_names_all_collects_map() {
        let h1: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("a".to_string(), h1.future());
        map.insert("b".to_string(), h2.future());
        let result = DescribeTopicsResult::of_topic_names(map);
        assert!(result.topic_id_values().is_none());
        assert!(result.all_topic_ids().is_none());

        h1.complete(description("a"));
        h2.complete(description("b"));
        let all = result.all_topic_names().unwrap().get().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all["a"].name(), "a");
    }

    #[tokio::test]
    async fn by_ids_all_fails_if_one_fails() {
        let h1: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(Uuid::new(1, 1), h1.future());
        let result = DescribeTopicsResult::of_topic_ids(map);
        assert!(result.all_topic_names().is_none());
        h1.complete_with_error(Error::local_illegal_state("x".to_string()));
        assert!(result.all_topic_ids().unwrap().get().await.is_err());
    }

    /// `DescribeTopicsResultTest.testNullAllTopicNames`.
    #[test]
    fn test_null_all_topic_names() {
        let result = DescribeTopicsResult::of_topic_ids(HashMap::from([(
            Uuid::random_uuid(),
            KafkaFuture::completed_future(Ok(description("foo"))),
        )]));
        assert!(result.all_topic_names().is_none());
    }

    /// `DescribeTopicsResultTest.testNullAllTopicIds`.
    #[test]
    fn test_null_all_topic_ids() {
        let result = DescribeTopicsResult::of_topic_names(HashMap::from([(
            "foo".to_string(),
            KafkaFuture::completed_future(Ok(description("foo"))),
        )]));
        assert!(result.all_topic_ids().is_none());
    }

    /// The constructor checks Java's invariant as `DeleteTopicsResult`'s does
    /// (`DeleteTopicsResultTest.testInvalidConfigurations`).
    #[test]
    fn test_invalid_configurations() {
        let both_null = std::panic::catch_unwind(|| {
            DescribeTopicsResult::with_topic_id_futures_name_futures(None, None);
        })
        .expect_err("expected a panic");
        assert_eq!(
            both_null.downcast_ref::<&str>(),
            Some(&"topicIdFutures and nameFutures cannot both be null.")
        );
        let both_set = std::panic::catch_unwind(|| {
            DescribeTopicsResult::with_topic_id_futures_name_futures(Some(HashMap::new()), Some(HashMap::new()));
        })
        .expect_err("expected a panic");
        assert_eq!(
            both_set.downcast_ref::<&str>(),
            Some(&"topicIdFutures and nameFutures cannot both be specified.")
        );
    }
}
