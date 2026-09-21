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
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeTopicsResult`.

use std::collections::HashMap;

use crate::admin::TopicDescription;
use crate::common::{KafkaFuture, Uuid};

/// The result of `Admin::describe_topics_with_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeTopicsResult`. As
/// with `DeleteTopicsResult`, the "keyed by id XOR name" invariant is a closed
/// enum instead of Java's two nullable maps.
#[derive(Clone, Debug)]
pub enum DescribeTopicsResult {
    /// Result keyed by topic id (the request used a `TopicIdCollection`).
    ByTopicId(HashMap<Uuid, KafkaFuture<TopicDescription>>),
    /// Result keyed by topic name (the request used a `TopicNameCollection`).
    ByTopicName(HashMap<String, KafkaFuture<TopicDescription>>),
}

impl DescribeTopicsResult {
    /// Creates a result keyed by topic id.
    pub(crate) fn of_topic_ids(topic_id_futures: HashMap<Uuid, KafkaFuture<TopicDescription>>) -> Self {
        DescribeTopicsResult::ByTopicId(topic_id_futures)
    }

    /// Creates a result keyed by topic name.
    pub(crate) fn of_topic_names(name_futures: HashMap<String, KafkaFuture<TopicDescription>>) -> Self {
        DescribeTopicsResult::ByTopicName(name_futures)
    }

    /// A map from topic IDs to futures if the request used topic IDs, otherwise
    /// `None`.
    pub fn topic_id_values(&self) -> Option<&HashMap<Uuid, KafkaFuture<TopicDescription>>> {
        match self {
            DescribeTopicsResult::ByTopicId(futures) => Some(futures),
            DescribeTopicsResult::ByTopicName(_) => None,
        }
    }

    /// A map from topic names to futures if the request used topic names,
    /// otherwise `None`.
    pub fn topic_name_values(&self) -> Option<&HashMap<String, KafkaFuture<TopicDescription>>> {
        match self {
            DescribeTopicsResult::ByTopicName(futures) => Some(futures),
            DescribeTopicsResult::ByTopicId(_) => None,
        }
    }

    /// A future map from topic names to descriptions if the request used topic
    /// names, otherwise `None`. Succeeds only if all descriptions succeed.
    pub fn all_topic_names(&self) -> Option<KafkaFuture<HashMap<String, TopicDescription>>> {
        match self {
            DescribeTopicsResult::ByTopicName(futures) => Some(KafkaFuture::join_map(
                futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            )),
            DescribeTopicsResult::ByTopicId(_) => None,
        }
    }

    /// A future map from topic ids to descriptions if the request used topic
    /// ids, otherwise `None`. Succeeds only if all descriptions succeed.
    pub fn all_topic_ids(&self) -> Option<KafkaFuture<HashMap<Uuid, TopicDescription>>> {
        match self {
            DescribeTopicsResult::ByTopicId(futures) => {
                Some(KafkaFuture::join_map(futures.iter().map(|(k, v)| (*k, v.clone())).collect()))
            },
            DescribeTopicsResult::ByTopicName(_) => None,
        }
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
}
