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

//! Share-consumer-specific metadata management (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata`. In
//! Java this is a subclass of `Metadata`. In Rust we use composition over
//! `Arc<Metadata>` + `MetadataOverrides`, exactly like
//! [`super::consumer_metadata::ConsumerMetadata`] and `ProducerMetadata`.
//!
//! `ShareConsumerMetadata` is much thinner than
//! [`super::consumer_metadata::ConsumerMetadata`]: share groups only support
//! explicit topic-name subscription (`subscribe(Set<String>)`), so there is
//! no client-side regex / broker-side RE2J / transient-topic handling. The
//! metadata request is always scoped to the subscription's metadata topics,
//! and a topic is retained iff `subscription.needsMetadata(topic)`.

#![allow(dead_code)] // Phase 4: lands before any Rust caller (Phases 5-6).

use std::ops::Deref;
use std::sync::{Arc, Mutex};

use crate::common::internals::ClusterResourceListeners;
use crate::common::requests::MetadataRequestBuilder;
use crate::common::utils::LogContext;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::metadata::{Metadata, MetadataOverrides};

/// Share-consumer metadata — extends [`Metadata`] with share-consumer-specific
/// behavior:
///
/// - Scope metadata requests to the share consumer's subscribed topic names
///   (`subscription.metadataTopics()`).
/// - Retain a topic iff its name is needed by the subscription
///   (`subscription.needsMetadata(topic)`).
/// - Honour `allow.auto.create.topics`.
///
/// Translated from
/// `org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata`.
pub(crate) struct ShareConsumerMetadata {
    /// Underlying [`Metadata`], wrapped in `Arc` so it can be shared with
    /// `NetworkClient` (and other downstream consumers) just like
    /// [`super::consumer_metadata::ConsumerMetadata`].
    metadata: Arc<Metadata>,
    /// Shared subscription state. Cloned into the override closures so they
    /// can read it on each `Metadata::update` / `new_metadata_request_builder`
    /// call.
    subscription: Arc<Mutex<SubscriptionState>>,
    allow_auto_topic_creation: bool,
}

impl ShareConsumerMetadata {
    /// Full constructor mirroring Java's primary `ShareConsumerMetadata(...)`
    /// constructor.
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        allow_auto_topic_creation: bool,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        // ── retain_topic_fn: retain iff the subscription needs the topic.
        //    Java `ShareConsumerMetadata.retainTopic(topic, isInternal, nowMs)`
        //    ignores `isInternal` / `nowMs` and returns
        //    `subscription.needsMetadata(topic)`. ─────────────────────────
        let retain_subscription = Arc::clone(&subscription);
        let retain_topic_fn = Box::new(move |topic: &str, _is_internal: bool, _now_ms: i64| -> bool {
            let sub_guard = retain_subscription.lock().unwrap();
            sub_guard.needs_metadata(topic)
        });

        // ── request_builder_fn: scope the next metadata request to the
        //    subscription's metadata topics. Java's
        //    `newMetadataRequestBuilder()` always uses
        //    `MetadataRequest.Builder.forTopicNames(subscription.metadataTopics(),
        //    allowAutoTopicCreation)`. ─────────────────────────────────────
        let builder_subscription = Arc::clone(&subscription);
        let request_builder_fn: Box<dyn Fn() -> MetadataRequestBuilder + Send + Sync> = Box::new(move || {
            let sub_guard = builder_subscription.lock().unwrap();
            let topics: Vec<String> = sub_guard.metadata_topics().into_iter().collect();
            let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
            MetadataRequestBuilder::for_topic_names(&topic_refs, allow_auto_topic_creation)
        });

        let metadata = Arc::new(Metadata::with_overrides(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            cluster_resource_listeners,
            MetadataOverrides {
                retain_topic_fn: Some(retain_topic_fn),
                retain_topic_with_id_fn: None,
                enable_partial_updates: false,
                request_builder_fn: Some(request_builder_fn),
                new_topics_request_builder_fn: None,
                post_update_fn: None,
            },
            LogContext::empty(),
        ));

        Self { metadata, subscription, allow_auto_topic_creation }
    }

    /// `ShareConsumerMetadata(ConsumerConfig, SubscriptionState, LogContext,
    /// ClusterResourceListeners)` — convenience constructor that reads the
    /// relevant config values.
    ///
    /// Reads `retry_backoff_ms`, `retry_backoff_max_ms`,
    /// `metadata_max_age_ms`, and `allow_auto_create_topics` via the
    /// `pub(crate)` fields on [`ConsumerConfig`].
    pub(crate) fn from_config(
        config: &ConsumerConfig,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        Self::new(
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            config.metadata_max_age_ms,
            config.allow_auto_create_topics,
            subscription,
            cluster_resource_listeners,
        )
    }

    /// Translates Java's `allowAutoTopicCreation()`.
    pub(crate) fn allow_auto_topic_creation(&self) -> bool {
        self.allow_auto_topic_creation
    }

    /// Returns a shared reference to the underlying [`Metadata`] for sharing
    /// with `NetworkClient`. Mirrors
    /// [`super::consumer_metadata::ConsumerMetadata::metadata_arc`].
    pub(crate) fn metadata_arc(&self) -> Arc<Metadata> {
        Arc::clone(&self.metadata)
    }
}

impl Deref for ShareConsumerMetadata {
    type Target = Metadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    //! Java has no `ShareConsumerMetadataTest`. These tests cover the
    //! share-consumer-specific override behavior (`newMetadataRequestBuilder`
    //! scopes to `metadataTopics()`; `retainTopic` retains only subscribed
    //! topics), mirroring the corresponding `ConsumerMetadataTest` cases that
    //! ARE relevant to the share metadata's simpler surface.

    use super::*;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::MetadataResponse;
    use crate::common::{Node, Uuid};
    use crate::consumer::AutoOffsetResetStrategy;
    use crate::metadata_response_data::{
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
    };
    use std::collections::HashSet;

    const REFRESH_BACKOFF_MS: i64 = 50;
    const REFRESH_BACKOFF_MAX_MS: i64 = 50;
    const METADATA_EXPIRE_MS: i64 = 50_000;

    fn new_subscription() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn new_share_metadata(sub: Arc<Mutex<SubscriptionState>>) -> ShareConsumerMetadata {
        ShareConsumerMetadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            false,
            sub,
            ClusterResourceListeners::new(),
        )
    }

    fn node() -> Node {
        Node::new(1, "localhost".to_string(), 9092)
    }

    fn build_response(topics: &[(&str, bool, Uuid)]) -> MetadataResponse {
        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("test-cluster-id".to_string()));
        data.set_controller_id(node().id());

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(node().id());
        broker.set_host(node().host().to_string());
        broker.set_port(node().port());
        data.set_brokers(vec![broker]);

        let response_topics: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|(name, is_internal, topic_id)| {
                let mut t = MetadataResponseTopic::new();
                t.set_name(Some((*name).to_string()));
                t.set_topic_id(*topic_id);
                t.set_error_code(Errors::None.code());
                t.set_is_internal(*is_internal);
                let mut partition = MetadataResponsePartition::new();
                partition.set_partition_index(0);
                partition.set_error_code(Errors::None.code());
                partition.set_leader_id(node().id());
                partition.set_leader_epoch(5);
                partition.set_replica_nodes(vec![node().id()]);
                partition.set_isr_nodes(vec![node().id()]);
                partition.set_offline_replicas(Vec::new());
                t.set_partitions(vec![partition]);
                t
            })
            .collect();
        data.set_topics(response_topics);

        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    fn topics_set(metadata: &ShareConsumerMetadata) -> HashSet<String> {
        metadata.fetch().topics().map(|s| s.to_string()).collect()
    }

    /// The metadata request is scoped to the subscription's metadata topics
    /// (never "all topics" — share groups have no client-side regex).
    #[test]
    fn test_request_builder_scopes_to_metadata_topics() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_to_share_group(HashSet::from(["foo".to_string(), "bar".to_string()]))
            .unwrap();
        let metadata = new_share_metadata(Arc::clone(&sub));

        let builder = metadata.new_metadata_request_builder();
        assert!(!builder.is_all_topics());
        let builder_topics: HashSet<String> = builder.topics().iter().map(|s| s.to_string()).collect();
        assert_eq!(builder_topics, HashSet::from(["foo".to_string(), "bar".to_string()]));
    }

    /// Only topics the subscription needs are retained after a metadata
    /// update; a non-subscribed topic in the response is dropped.
    #[test]
    fn test_retain_only_subscribed_topics() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_to_share_group(HashSet::from(["foo".to_string()]))
            .unwrap();
        let metadata = new_share_metadata(Arc::clone(&sub));

        let response = build_response(&[("foo", false, Uuid::zero()), ("other", false, Uuid::zero())]);
        metadata.update_with_current_request_version(&response, false, 1000);

        assert_eq!(topics_set(&metadata), HashSet::from(["foo".to_string()]));
    }

    /// `allow_auto_topic_creation` reflects the constructor value and is
    /// threaded into the metadata request builder.
    #[test]
    fn test_allow_auto_topic_creation_accessor() {
        let sub = new_subscription();
        let metadata = ShareConsumerMetadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            true,
            sub,
            ClusterResourceListeners::new(),
        );
        assert!(metadata.allow_auto_topic_creation());
    }
}
