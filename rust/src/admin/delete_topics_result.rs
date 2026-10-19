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

/// The result of the `Admin::delete_topics` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteTopicsResult`.
/// Exactly one of the two maps is present, the one matching the kind of
/// `TopicCollection` the request used.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult")]
pub struct DeleteTopicsResult {
    topic_id_futures: Option<HashMap<Uuid, KafkaFuture<()>>>,
    name_futures: Option<HashMap<String, KafkaFuture<()>>>,
}

impl DeleteTopicsResult {
    /// Creates a result keyed by topic id or by topic name.
    ///
    /// # Panics
    ///
    /// If both maps or neither are given, as Java throws
    /// `IllegalArgumentException`.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#DeleteTopicsResult")]
    pub(crate) fn with_topic_id_futures_name_futures(
        topic_id_futures: Option<HashMap<Uuid, KafkaFuture<()>>>,
        name_futures: Option<HashMap<String, KafkaFuture<()>>>,
    ) -> Self {
        assert!(
            topic_id_futures.is_none() || name_futures.is_none(),
            "topicIdFutures and nameFutures cannot both be specified."
        );
        assert!(
            topic_id_futures.is_some() || name_futures.is_some(),
            "topicIdFutures and nameFutures cannot both be null."
        );
        DeleteTopicsResult { topic_id_futures, name_futures }
    }

    /// Creates a result keyed by topic id.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#ofTopicIds")]
    pub(crate) fn of_topic_ids(topic_id_futures: HashMap<Uuid, KafkaFuture<()>>) -> Self {
        Self::with_topic_id_futures_name_futures(Some(topic_id_futures), None)
    }

    /// Creates a result keyed by topic name.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#ofTopicNames")]
    pub(crate) fn of_topic_names(name_futures: HashMap<String, KafkaFuture<()>>) -> Self {
        Self::with_topic_id_futures_name_futures(None, Some(name_futures))
    }

    /// Use when `Admin::delete_topics_with_options` used a `TopicCollection`
    /// of ids.
    ///
    /// Returns a map from topic IDs to futures which can be used to check the
    /// status of individual deletions if the delete topics request used topic
    /// IDs, otherwise `None`.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#topicIdValues")]
    pub fn topic_id_values(&self) -> Option<&HashMap<Uuid, KafkaFuture<()>>> {
        self.topic_id_futures.as_ref()
    }

    /// Use when `Admin::delete_topics_with_options` used a `TopicCollection`
    /// of names.
    ///
    /// Returns a map from topic names to futures which can be used to check
    /// the status of individual deletions if the delete topics request used
    /// topic names, otherwise `None`.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#topicNameValues")]
    pub fn topic_name_values(&self) -> Option<&HashMap<String, KafkaFuture<()>>> {
        self.name_futures.as_ref()
    }

    /// A future which succeeds only if all the topic deletions succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteTopicsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        match &self.topic_id_futures {
            None => KafkaFuture::all_of(self.name_futures.iter().flat_map(HashMap::values).cloned().collect()),
            Some(topic_id_futures) => KafkaFuture::all_of(topic_id_futures.values().cloned().collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;

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
        h1.complete_with_error(Error::local_illegal_state("gone".to_string()));
        assert!(result.all().get().await.is_err());
    }

    /// The message of the panic `f` raises.
    fn panic_message(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let payload = std::panic::catch_unwind(f).expect_err("expected a panic");
        payload
            .downcast_ref::<&str>()
            .map(ToString::to_string)
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap()
    }

    fn completed() -> KafkaFuture<()> {
        let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        future.complete(());
        future.future()
    }

    /// `DeleteTopicsResultTest.testDeleteTopicsResultWithNames`.
    #[test]
    fn test_delete_topics_result_with_names() {
        let topic_names = HashMap::from([("foo".to_string(), completed())]);
        let topic_name_futures = DeleteTopicsResult::of_topic_names(topic_names);
        let values = topic_name_futures.topic_name_values().unwrap();
        assert_eq!(values.keys().collect::<Vec<_>>(), ["foo"]);
        assert!(values["foo"].is_done());
        assert!(topic_name_futures.topic_id_values().is_none());
        assert!(topic_name_futures.all().is_done());
    }

    /// `DeleteTopicsResultTest.testDeleteTopicsResultWithIds`.
    #[test]
    fn test_delete_topics_result_with_ids() {
        let id = Uuid::random_uuid();
        let topic_ids = HashMap::from([(id, completed())]);
        let topic_id_futures = DeleteTopicsResult::of_topic_ids(topic_ids);
        let values = topic_id_futures.topic_id_values().unwrap();
        assert_eq!(values.keys().collect::<Vec<_>>(), [&id]);
        assert!(values[&id].is_done());
        assert!(topic_id_futures.topic_name_values().is_none());
        assert!(topic_id_futures.all().is_done());
    }

    /// `DeleteTopicsResultTest.testInvalidConfigurations`.
    #[test]
    fn test_invalid_configurations() {
        assert_eq!(
            panic_message(|| {
                DeleteTopicsResult::with_topic_id_futures_name_futures(None, None);
            }),
            "topicIdFutures and nameFutures cannot both be null."
        );
        assert_eq!(
            panic_message(|| {
                DeleteTopicsResult::with_topic_id_futures_name_futures(Some(HashMap::new()), Some(HashMap::new()));
            }),
            "topicIdFutures and nameFutures cannot both be specified."
        );
    }
}
