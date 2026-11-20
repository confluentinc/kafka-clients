// Copyright 2026 Confluent Inc.
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

//! Share-consumer metadata management.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata`. Java
//! extends `Metadata`; Rust composes an `Arc<Metadata>` built with
//! [`MetadataOverrides`], as [`super::ConsumerMetadata`] does, and derefs to
//! it.

use std::ops::Deref;
use std::sync::{Arc, Mutex};

use crate::common::internals::ClusterResourceListeners;
use crate::common::requests::metadata_request;
use crate::common::utils::internals::LogContext;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::SubscriptionState;
use crate::metadata::{Metadata, MetadataOverrides};

/// Share-consumer metadata: a [`Metadata`] whose requests and retained topics
/// are scoped to the share consumer's subscription.
///
/// - [`Metadata::new_metadata_request_builder`] asks for the subscription's
///   metadata topics by name (Java's `newMetadataRequestBuilder` override).
/// - A topic is retained only if the subscription needs its metadata (Java's
///   `retainTopic` override). Unlike the consumer, internal topics and
///   patterns play no part.
///
/// Java: `public class ShareConsumerMetadata extends Metadata`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata")]
pub(crate) struct ShareConsumerMetadata {
    /// The underlying [`Metadata`], in an `Arc` so the network client and the
    /// membership manager share it.
    metadata: Arc<Metadata>,
    /// Java: `private final boolean allowAutoTopicCreation`.
    allow_auto_topic_creation: bool,
}

impl ShareConsumerMetadata {
    /// Java: `ShareConsumerMetadata(long refreshBackoffMs, long refreshBackoffMaxMs,
    /// long metadataExpireMs, boolean allowAutoTopicCreation, SubscriptionState
    /// subscription, LogContext logContext, ClusterResourceListeners
    /// clusterResourceListeners)`. The `LogContext` is dropped, as for
    /// [`super::ConsumerMetadata`] (the `log` crate has no per-client context).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata#ShareConsumerMetadata")]
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        allow_auto_topic_creation: bool,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        // Lock order: `Metadata::update` runs the retain callback while
        // holding `Metadata`'s inner mutex, so it nests `Metadata` ->
        // `SubscriptionState`, as Java's `synchronized retainTopic` runs inside
        // the `Metadata.update` monitor. Never call into `Metadata` while
        // holding the `SubscriptionState` guard.
        let retain_subscription = Arc::clone(&subscription);
        let retain_topic_fn = Box::new(move |topic: &str, _is_internal: bool, _now_ms: i64| -> bool {
            let sub_guard = match retain_subscription.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            sub_guard.needs_metadata(topic)
        });

        let builder_subscription = subscription;
        let request_builder_fn: Box<dyn Fn() -> metadata_request::Builder + Send + Sync> = Box::new(move || {
            let topics: Vec<String> = {
                let sub_guard = match builder_subscription.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                sub_guard.metadata_topics().into_iter().collect()
            };
            let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
            metadata_request::Builder::for_topic_names(&topic_refs, allow_auto_topic_creation)
        });

        let metadata = Arc::new(Metadata::with_overrides(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            cluster_resource_listeners,
            MetadataOverrides {
                retain_topic_fn: Some(retain_topic_fn),
                // Java's two-argument `retainTopic(String, Uuid, boolean, long)`
                // is not overridden; the base delegates to the name-only one.
                retain_topic_with_id_fn: None,
                // `newMetadataRequestBuilderForNewTopics` is not overridden:
                // the base returns `null`, so no partial updates.
                enable_partial_updates: false,
                request_builder_fn: Some(request_builder_fn),
                new_topics_request_builder_fn: None,
                post_update_fn: None,
            },
            LogContext::empty(),
        ));

        Self { metadata, allow_auto_topic_creation }
    }

    /// Java: `ShareConsumerMetadata(ConsumerConfig config, SubscriptionState
    /// subscriptions, LogContext logContext, ClusterResourceListeners
    /// clusterResourceListeners)`: reads `retry.backoff.ms`,
    /// `retry.backoff.max.ms`, `metadata.max.age.ms` and
    /// `allow.auto.create.topics`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata#ShareConsumerMetadata")]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "ShareConsumerImpl uses it from Milestone 18 P7 on")
    )]
    pub(crate) fn with_config(
        config: &ConsumerConfig,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        Self::new(
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            config.metadata_max_age_ms,
            config.allow_auto_create_topics,
            subscriptions,
            cluster_resource_listeners,
        )
    }

    /// Java: `allowAutoTopicCreation()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerMetadata#allowAutoTopicCreation")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn allow_auto_topic_creation(&self) -> bool {
        self.allow_auto_topic_creation
    }

    /// The shared [`Metadata`] handle, for the membership manager and the
    /// network client (Java passes the `ShareConsumerMetadata` itself, a
    /// `Metadata`).
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

/// Java has no `ShareConsumerMetadataTest`; these tests pin the two overrides
/// and the config constructor.
#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::MetadataResponseData;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::{AbstractRequest, MetadataResponse, RequestBuilder};
    use crate::common::{Node, Uuid};
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};

    fn new_subscription() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn new_metadata(sub: Arc<Mutex<SubscriptionState>>, allow_auto_topic_creation: bool) -> ShareConsumerMetadata {
        ShareConsumerMetadata::new(50, 50, 50_000, allow_auto_topic_creation, sub, ClusterResourceListeners::new())
    }

    /// A one-partition-per-topic `MetadataResponse` for `(name, is_internal)`
    /// topics (the `consumer_metadata.rs` test helper).
    fn build_response(topics: &[(&str, bool)]) -> MetadataResponse {
        let node = Node::new(1, "localhost".to_string(), 9092);
        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("test-cluster-id".to_string()));
        data.set_controller_id(node.id());
        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(node.id());
        broker.set_host(node.host().to_string());
        broker.set_port(node.port());
        data.set_brokers(vec![broker]);
        let response_topics: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|(name, is_internal)| {
                let mut t = MetadataResponseTopic::new();
                t.set_name(Some((*name).to_string()));
                t.set_topic_id(Uuid::random_uuid());
                t.set_error_code(Errors::None.code());
                t.set_is_internal(*is_internal);
                let mut partition = MetadataResponsePartition::new();
                partition.set_partition_index(0);
                partition.set_error_code(Errors::None.code());
                partition.set_leader_id(node.id());
                partition.set_leader_epoch(5);
                partition.set_replica_nodes(vec![node.id()]);
                partition.set_isr_nodes(vec![node.id()]);
                partition.set_offline_replicas(Vec::new());
                t.set_partitions(vec![partition]);
                t
            })
            .collect();
        data.set_topics(response_topics);
        MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version())
    }

    /// `newMetadataRequestBuilder` asks for the subscription's metadata
    /// topics by name, carrying `allow.auto.create.topics`.
    #[test]
    fn test_metadata_request_scoped_to_subscription() {
        for allow in [true, false] {
            let sub = new_subscription();
            sub.lock()
                .unwrap()
                .subscribe_to_share_group(HashSet::from(["foo".to_string(), "bar".to_string()]))
                .unwrap();
            let metadata = new_metadata(Arc::clone(&sub), allow);
            assert_eq!(metadata.allow_auto_topic_creation(), allow);

            let mut builder = metadata.new_metadata_request_builder();
            assert!(!builder.is_all_topics());
            let topics: HashSet<String> = builder.topics().iter().map(|t| t.to_string()).collect();
            assert_eq!(topics, HashSet::from(["foo".to_string(), "bar".to_string()]));
            let version = builder.latest_allowed_version();
            match builder.build_version(version).unwrap() {
                AbstractRequest::Metadata(request) => assert_eq!(request.allow_auto_topic_creation(), allow),
                _ => panic!("expected a Metadata request"),
            }
        }
    }

    /// `retainTopic` keeps only the topics the subscription needs; whether a
    /// topic is internal plays no part.
    #[test]
    fn test_retain_topic_follows_subscription() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_to_share_group(HashSet::from(["foo".to_string(), "__consumer_offsets".to_string()]))
            .unwrap();
        let metadata = new_metadata(Arc::clone(&sub), true);
        let response = build_response(&[("foo", false), ("bar", false), ("__consumer_offsets", true)]);
        metadata.update_with_current_request_version(&response, false, 1000);
        let topics: HashSet<String> = metadata.fetch().topics().map(|t| t.to_string()).collect();
        assert_eq!(topics, HashSet::from(["foo".to_string(), "__consumer_offsets".to_string()]));
    }

    /// `metadata_arc` hands out the one `Metadata` the wrapper derefs to.
    #[test]
    fn test_metadata_arc_shares_the_metadata() {
        let metadata = new_metadata(new_subscription(), true);
        assert!(std::ptr::eq(
            Arc::as_ptr(&metadata.metadata_arc()),
            &*metadata as *const Metadata
        ));
    }

    /// The config constructor reads `allow.auto.create.topics` and
    /// `metadata.max.age.ms` from `ConsumerConfig`.
    #[test]
    fn test_with_config() {
        let props = HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
            ("allow.auto.create.topics".to_string(), "false".to_string()),
            ("metadata.max.age.ms".to_string(), "12345".to_string()),
        ]);
        let config = ConsumerConfig::new(&props).unwrap();
        let metadata = ShareConsumerMetadata::with_config(&config, new_subscription(), ClusterResourceListeners::new());
        assert!(!metadata.allow_auto_topic_creation());
        assert_eq!(metadata.metadata_expire_ms(), 12345);
    }
}
