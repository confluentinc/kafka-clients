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

//! The result of `Admin::list_topics`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListTopicsResult`.

use std::collections::{HashMap, HashSet};

use crate::admin::TopicListing;
use crate::common::KafkaFuture;

/// The result of `Admin::list_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListTopicsResult`.
#[derive(Clone, Debug)]
pub struct ListTopicsResult {
    future: KafkaFuture<HashMap<String, TopicListing>>,
}

impl ListTopicsResult {
    /// Creates a result wrapping the topic-listing future.
    pub(crate) fn new(future: KafkaFuture<HashMap<String, TopicListing>>) -> Self {
        Self { future }
    }

    /// Return a future which yields a map of topic names to `TopicListing`
    /// objects.
    pub fn names_to_listings(&self) -> KafkaFuture<HashMap<String, TopicListing>> {
        self.future.clone()
    }

    /// Return a future which yields a collection of `TopicListing` objects.
    pub fn listings(&self) -> KafkaFuture<Vec<TopicListing>> {
        self.future.then_apply(|map| map.into_values().collect())
    }

    /// Return a future which yields a set of topic names.
    pub fn names(&self) -> KafkaFuture<HashSet<String>> {
        self.future.then_apply(|map| map.into_keys().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn names_listings_and_map() {
        let handle: KafkaFutureImpl<HashMap<String, TopicListing>> = KafkaFutureImpl::new();
        let result = ListTopicsResult::new(handle.future());

        let mut map = HashMap::new();
        map.insert("t1".to_string(), TopicListing::new("t1", Uuid::new(1, 1), false));
        map.insert("t2".to_string(), TopicListing::new("t2", Uuid::new(2, 2), true));
        handle.complete(map);

        let names = result.names().get().await.unwrap();
        assert_eq!(names, HashSet::from(["t1".to_string(), "t2".to_string()]));
        assert_eq!(result.listings().get().await.unwrap().len(), 2);
        assert_eq!(result.names_to_listings().get().await.unwrap().len(), 2);
    }
}
