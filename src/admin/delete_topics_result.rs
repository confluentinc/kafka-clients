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

//! The result of `Admin::delete_topics`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteTopicsResult`.

use std::collections::HashMap;

use crate::common::{KafkaFuture, Uuid};

/// The result of `Admin::delete_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteTopicsResult`. Java
/// carries two nullable maps and rejects both-null / both-set in the
/// constructor; the Rust port makes the "exactly one keying" invariant a closed
/// enum so it is unrepresentable to have both or neither.
#[derive(Clone, Debug)]
pub enum DeleteTopicsResult {
    /// Result keyed by topic id (the request used a `TopicIdCollection`).
    ByTopicId(HashMap<Uuid, KafkaFuture<()>>),
    /// Result keyed by topic name (the request used a `TopicNameCollection`).
    ByTopicName(HashMap<String, KafkaFuture<()>>),
}

impl DeleteTopicsResult {
    /// Creates a result keyed by topic id.
    pub(crate) fn of_topic_ids(topic_id_futures: HashMap<Uuid, KafkaFuture<()>>) -> Self {
        DeleteTopicsResult::ByTopicId(topic_id_futures)
    }

    /// Creates a result keyed by topic name.
    pub(crate) fn of_topic_names(name_futures: HashMap<String, KafkaFuture<()>>) -> Self {
        DeleteTopicsResult::ByTopicName(name_futures)
    }

    /// A map from topic IDs to futures if the deletion used topic IDs, otherwise
    /// `None`.
    pub fn topic_id_values(&self) -> Option<&HashMap<Uuid, KafkaFuture<()>>> {
        match self {
            DeleteTopicsResult::ByTopicId(futures) => Some(futures),
            DeleteTopicsResult::ByTopicName(_) => None,
        }
    }

    /// A map from topic names to futures if the deletion used topic names,
    /// otherwise `None`.
    pub fn topic_name_values(&self) -> Option<&HashMap<String, KafkaFuture<()>>> {
        match self {
            DeleteTopicsResult::ByTopicName(futures) => Some(futures),
            DeleteTopicsResult::ByTopicId(_) => None,
        }
    }

    /// A future which succeeds only if all the topic deletions succeed.
    pub fn all(&self) -> KafkaFuture<()> {
        match self {
            DeleteTopicsResult::ByTopicId(futures) => KafkaFuture::all_of(futures.values().cloned().collect()),
            DeleteTopicsResult::ByTopicName(futures) => KafkaFuture::all_of(futures.values().cloned().collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::kafka_future::KafkaFutureImpl;

    #[tokio::test]
    async fn by_names_all_succeeds() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("a".to_string(), h1.future());
        map.insert("b".to_string(), h2.future());
        let result = DeleteTopicsResult::of_topic_names(map);
        assert!(result.topic_id_values().is_none());
        assert!(result.topic_name_values().is_some());
        h1.complete(());
        h2.complete(());
        assert_eq!(result.all().get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn by_ids_all_fails_if_one_fails() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(Uuid::new(1, 1), h1.future());
        let result = DeleteTopicsResult::of_topic_ids(map);
        assert!(result.topic_name_values().is_none());
        h1.complete_exceptionally(Error::IllegalState("gone".to_string()));
        assert!(result.all().get().await.is_err());
    }
}
