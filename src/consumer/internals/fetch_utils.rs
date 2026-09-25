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

//! Disparate utilities shared by the consumer fetch path.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.FetchUtils`.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use crate::common::TopicPartition;
use crate::consumer::internals::ConsumerMetadata;
use crate::consumer::internals::SubscriptionState;

/// Translates the Java static-utility class `org.apache.kafka.clients.consumer.internals.FetchUtils`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub(crate) struct FetchUtils;

impl FetchUtils {
    /// Performs two combined actions based on the state related to the
    /// given `topic_partition`:
    ///
    /// 1. Invokes `Metadata::request_update(false)` to signal that the
    ///    metadata is incorrect and needs to be updated.
    /// 2. Invokes `SubscriptionState::clear_preferred_read_replica` to clear
    ///    out any read replica information that may be present.
    ///
    /// This utility should be invoked if the client detects (or is told by a
    /// node in the broker) that an attempt was made to fetch from a node that
    /// isn't the leader or preferred replica.
    ///
    /// Translates `FetchUtils.requestMetadataUpdate(Metadata, SubscriptionState,
    /// TopicPartition)`.
    pub(crate) fn request_metadata_update(
        metadata: &ConsumerMetadata,
        subscriptions: &Arc<Mutex<SubscriptionState>>,
        topic_partition: &TopicPartition,
    ) {
        metadata.metadata_arc().request_update(false);
        let mut guard = subscriptions.lock().expect("SubscriptionState mutex poisoned");
        guard.clear_preferred_read_replica(topic_partition);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::AutoOffsetResetStrategy;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)))
    }

    fn make_consumer_metadata(subs: Arc<Mutex<SubscriptionState>>) -> ConsumerMetadata {
        ConsumerMetadata::new(50, 50, 50_000, false, false, subs, ClusterResourceListeners::new())
    }

    /// `request_metadata_update` requests a metadata refresh and clears the
    /// preferred read replica for the given partition. Mirrors Java's
    /// `FetchUtilsTest.testRequestMetadataUpdate`.
    #[test]
    fn test_request_metadata_update_clears_preferred_replica_and_requests_update() {
        let subs = make_subscriptions();
        let cm = make_consumer_metadata(subs.clone());
        let tp0 = tp("test", 0);

        // Assign and set a preferred read replica.
        {
            let mut guard = subs.lock().expect("lock");
            let mut set = std::collections::HashSet::new();
            set.insert(tp0.clone());
            guard.assign_from_user(set).unwrap();
            guard.update_preferred_read_replica(&tp0, 7, 1_000).unwrap();
            assert_eq!(Some(7), guard.preferred_read_replica(&tp0, 1_000));
        }

        FetchUtils::request_metadata_update(&cm, &subs, &tp0);

        // Preferred replica was cleared (Java's `subscriptions.preferredReadReplica`
        // returns `Optional.empty()`).
        let mut guard = subs.lock().expect("lock");
        assert_eq!(None, guard.preferred_read_replica(&tp0, 1_000));
    }
}
