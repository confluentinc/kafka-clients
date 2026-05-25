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

//! Consumer-specific metadata management.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerMetadata`. In Java
//! this is a subclass of `Metadata`. In Rust we use composition over
//! `Arc<Metadata>` + `MetadataOverrides`, exactly like `ProducerMetadata`.
//!
//! The `retain_topic_with_id_fn` override (added to `MetadataOverrides` in
//! the Phase-4 prep commit) is used to retain topics received as topic IDs
//! in a broker-side regex assignment — `subscription.is_assigned_from_re2j(topic_id)`.
//! The single-arg name-only retain handles the client-side regex /
//! transient-topic / explicit subscription paths.

#![allow(dead_code)] // Phase 4: lands before any Rust caller (Phases 5-11).

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use crate::common::internals::ClusterResourceListeners;
use crate::common::requests::MetadataRequestBuilder;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::metadata::{Metadata, MetadataOverrides};

/// Consumer-specific inner state. Holds the transient-topics set used to
/// scope metadata requests for offset-related APIs (e.g. `endOffsets`,
/// `offsetsForTimes`).
struct ConsumerMetadataInner {
    /// Topics added temporarily for offset-related APIs. They survive only
    /// for the lifetime of the API call.
    transient_topics: HashSet<String>,
}

/// Consumer metadata — extends [`Metadata`] with consumer-specific
/// behavior:
///
/// - Scope metadata requests to the consumer's subscribed topics
///   (client-side regex / explicit names / broker-side RE2J / transient).
/// - Retain a topic if its **name** is in the subscription (`needs_metadata`),
///   matches the client-side regex (`matches_subscribed_pattern`), or is a
///   transient topic; **or** if its **id** was assigned by the coordinator
///   after a broker-side RE2J subscription (`is_assigned_from_re2j`).
/// - Honour `include.internal.topics` and `allow.auto.create.topics`.
///
/// Translated from `org.apache.kafka.clients.consumer.internals.ConsumerMetadata`.
pub(crate) struct ConsumerMetadata {
    /// Underlying [`Metadata`], wrapped in `Arc` so it can be shared with
    /// `NetworkClient` (and other downstream consumers) just like
    /// `ProducerMetadata` does.
    metadata: Arc<Metadata>,
    /// Consumer-specific mutable state — transient topics set.
    inner: Arc<Mutex<ConsumerMetadataInner>>,
    /// Shared subscription state. Cloned into the override closures so they
    /// can read it on each `Metadata::update` / `new_metadata_request_builder`
    /// call.
    subscription: Arc<Mutex<SubscriptionState>>,
    allow_auto_topic_creation: bool,
}

impl ConsumerMetadata {
    /// Full constructor mirroring Java's primary `ConsumerMetadata(...)`
    /// constructor.
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        include_internal_topics: bool,
        allow_auto_topic_creation: bool,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ConsumerMetadataInner { transient_topics: HashSet::new() }));

        // ── retain_topic_fn: name-only retain (client-side regex, explicit names,
        //    transient topics) ────────────────────────────────────────────────
        let retain_subscription = Arc::clone(&subscription);
        let retain_inner = Arc::clone(&inner);
        let retain_topic_fn = Box::new(move |topic: &str, is_internal: bool, _now_ms: i64| -> bool {
            let inner_guard = retain_inner.lock().unwrap();
            let sub_guard = retain_subscription.lock().unwrap();
            if inner_guard.transient_topics.contains(topic) || sub_guard.needs_metadata(topic) {
                return true;
            }
            if is_internal && !include_internal_topics {
                return false;
            }
            sub_guard.matches_subscribed_pattern(topic)
        });

        // ── retain_topic_with_id_fn: name-or-id retain (Java's two-arg
        //    `retainTopic(topicName, topicId, isInternal, nowMs)`) ──────────
        let retain_id_subscription = Arc::clone(&subscription);
        let retain_id_inner = Arc::clone(&inner);
        let retain_topic_with_id_fn = Box::new(
            move |topic: &str, topic_id: Option<crate::common::Uuid>, is_internal: bool, _now_ms: i64| -> bool {
                let inner_guard = retain_id_inner.lock().unwrap();
                let sub_guard = retain_id_subscription.lock().unwrap();
                // First branch matches the single-arg retain.
                if inner_guard.transient_topics.contains(topic) || sub_guard.needs_metadata(topic) {
                    return true;
                }
                let name_retained = if is_internal && !include_internal_topics {
                    false
                } else {
                    sub_guard.matches_subscribed_pattern(topic)
                };
                if name_retained {
                    return true;
                }
                // Second branch: id-based retain for broker-side RE2J.
                topic_id.is_some_and(|id| sub_guard.is_assigned_from_re2j(id))
            },
        );

        // ── request_builder_fn: scope the next metadata request ────────────
        // Mirrors Java's `newMetadataRequestBuilder()`:
        // 1. Client-side regex (`hasPatternSubscription`) -> all topics
        //    (so we can compute the regex client-side).
        // 2. Broker-side RE2J with no transient topics -> request by topic IDs.
        // 3. Otherwise -> request by metadata-topics + transient topic names.
        let builder_subscription = Arc::clone(&subscription);
        let builder_inner = Arc::clone(&inner);
        let request_builder_fn: Box<dyn Fn() -> MetadataRequestBuilder + Send + Sync> = Box::new(move || {
            let sub_guard = builder_subscription.lock().unwrap();
            if sub_guard.has_pattern_subscription() {
                return MetadataRequestBuilder::all_topics();
            }
            let inner_guard = builder_inner.lock().unwrap();
            if sub_guard.has_re2j_pattern_subscription() && inner_guard.transient_topics.is_empty() {
                let assigned_ids: HashSet<crate::common::Uuid> =
                    sub_guard.assigned_topic_ids().iter().copied().collect();
                return MetadataRequestBuilder::for_topic_ids(&assigned_ids);
            }
            // Explicit topic names + transient topics.
            let mut topics: HashSet<String> = sub_guard.metadata_topics();
            topics.extend(inner_guard.transient_topics.iter().cloned());
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
                retain_topic_with_id_fn: Some(retain_topic_with_id_fn),
                enable_partial_updates: false,
                request_builder_fn: Some(request_builder_fn),
                new_topics_request_builder_fn: None,
                post_update_fn: None,
            },
        ));

        Self { metadata, inner, subscription, allow_auto_topic_creation }
    }

    /// `ConsumerMetadata(ConsumerConfig, SubscriptionState, ClusterResourceListeners)`
    /// — convenience constructor that reads the relevant config values.
    ///
    /// Reads `retry_backoff_ms`, `retry_backoff_max_ms`,
    /// `metadata_max_age_ms`, `exclude_internal_topics`, and
    /// `allow_auto_create_topics` via the `pub(crate)` fields on
    /// `ConsumerConfig` (no public getters currently exist for these — the
    /// fields are accessed directly within the crate).
    pub(crate) fn from_config(
        config: &ConsumerConfig,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        Self::new(
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            config.metadata_max_age_ms,
            !config.exclude_internal_topics,
            config.allow_auto_create_topics,
            subscription,
            cluster_resource_listeners,
        )
    }

    /// Translates Java's `allowAutoTopicCreation()`.
    pub(crate) fn allow_auto_topic_creation(&self) -> bool {
        self.allow_auto_topic_creation
    }

    /// Translates Java's `addTransientTopics(Set<String>)`.
    ///
    /// Adds topics to the transient set. If the resulting set introduces
    /// topics not yet in the metadata cache, schedule a partial update so
    /// the next refresh covers them.
    pub(crate) fn add_transient_topics(&self, topics: HashSet<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.transient_topics.extend(topics);
        // Snapshot the transient set for the contains-all check that
        // mirrors Java's `!fetch().topics().containsAll(topics)`.
        let transient_snapshot: Vec<String> = inner.transient_topics.iter().cloned().collect();
        drop(inner);

        let cluster = self.metadata.fetch();
        let known_topics: HashSet<&str> = cluster.topics().collect();
        if !transient_snapshot.iter().all(|t| known_topics.contains(t.as_str())) {
            self.metadata.request_update_for_new_topics();
        }
    }

    /// Translates Java's `clearTransientTopics()`.
    pub(crate) fn clear_transient_topics(&self) {
        self.inner.lock().unwrap().transient_topics.clear();
    }

    /// Returns a shared reference to the underlying [`Metadata`] for sharing
    /// with `NetworkClient`. Mirrors `ProducerMetadata::metadata_arc`.
    pub(crate) fn metadata_arc(&self) -> Arc<Metadata> {
        Arc::clone(&self.metadata)
    }
}

impl Deref for ConsumerMetadata {
    type Target = Metadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ConsumerMetadataTest`. The two
    //! `testValidPartitionLeadershipUpdate` / `testInvalidPartitionLeadershipUpdates`
    //! tests use Mockito + the full `RequestTestUtils.metadataUpdateWith` /
    //! `metadataResponse` helpers; they exercise only the inherited
    //! `Metadata.updatePartitionLeadership` path, which is already covered
    //! by `metadata::tests::test_topic_metadata_on_update_partition_leadership`
    //! and friends — so they're not translated here. The tests below cover
    //! the consumer-specific override behavior that *is* unique to
    //! `ConsumerMetadata`.

    use super::*;
    use crate::common::internals::topic::GROUP_METADATA_TOPIC_NAME;
    use crate::common::protocol::{ApiKeys, Errors};
    use crate::common::requests::MetadataResponse;
    use crate::common::{Node, TopicPartition, Uuid};
    use crate::consumer::{AutoOffsetResetStrategy, SubscriptionPattern};
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

    fn topics_set(metadata: &ConsumerMetadata) -> HashSet<String> {
        metadata.fetch().topics().map(|s| s.to_string()).collect()
    }

    const REFRESH_BACKOFF_MS: i64 = 50;
    const REFRESH_BACKOFF_MAX_MS: i64 = 50;
    const METADATA_EXPIRE_MS: i64 = 50_000;

    fn new_subscription() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)))
    }

    fn new_consumer_metadata(sub: Arc<Mutex<SubscriptionState>>, include_internal_topics: bool) -> ConsumerMetadata {
        ConsumerMetadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            include_internal_topics,
            false,
            sub,
            ClusterResourceListeners::new(),
        )
    }

    fn node() -> Node {
        Node::new(1, "localhost".to_string(), 9092)
    }

    /// Build a complete `MetadataResponse` from a list of `(topic_name, is_internal, topic_id)`.
    ///
    /// Each topic contains a single partition (mirroring the Java
    /// `topicMetadata(...)` helper); empty-partition topics aren't reflected
    /// in `Cluster::topics()` since that view is derived from
    /// `partitions_by_topic`.
    fn build_response(topics: &[(&str, bool, Uuid)]) -> MetadataResponse {
        use crate::metadata_response_data::MetadataResponsePartition;

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

    /// Translated from `ConsumerMetadataTest.testPatternSubscriptionNoInternalTopics` and
    /// `testPatternSubscriptionIncludeInternalTopics`.
    #[test]
    fn test_pattern_subscription() {
        // Compile the regex once outside the loop to satisfy
        // `regex_creation_in_loops`. `Regex::clone` is cheap (Arc'd).
        let pattern = regex::Regex::new("__.*").unwrap();
        for include_internal in [false, true] {
            let sub = new_subscription();
            sub.lock().unwrap().subscribe_pattern(pattern.clone(), None).unwrap();
            let metadata = new_consumer_metadata(Arc::clone(&sub), include_internal);

            // Client-side regex -> request all topics.
            let builder = metadata.new_metadata_request_builder();
            assert!(builder.is_all_topics());

            // Apply a response that contains an internal and a matching/
            // non-matching topic.
            let response = build_response(&[
                ("__consumer_offsets", true, Uuid::zero()),
                ("__matching_topic", false, Uuid::zero()),
                ("non_matching_topic", false, Uuid::zero()),
            ]);
            metadata.update_with_current_request_version(&response, false, 1000);

            let cached = topics_set(&metadata);
            if include_internal {
                assert_eq!(
                    cached,
                    HashSet::from(["__matching_topic".to_string(), "__consumer_offsets".to_string()])
                );
            } else {
                assert_eq!(cached, HashSet::from(["__matching_topic".to_string()]));
            }
        }
    }

    /// Translated from `ConsumerMetadataTest.testSubscriptionToBrokerRegexDoesNotRequestAllTopicsMetadata`.
    #[test]
    fn test_subscription_to_broker_regex_does_not_request_all_topics_metadata() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_re2j_pattern(SubscriptionPattern::new("__.*"), None)
            .unwrap();

        let assigned_topic_id = Uuid::random_uuid();
        sub.lock().unwrap().set_assigned_topic_ids(HashSet::from([assigned_topic_id]));

        let metadata = new_consumer_metadata(Arc::clone(&sub), false);
        let builder = metadata.new_metadata_request_builder();
        assert!(!builder.is_all_topics());
        assert_eq!(builder.topic_ids(), vec![assigned_topic_id]);
    }

    /// Translated from `ConsumerMetadataTest.testSubscriptionToBrokerRegexRetainsAssignedTopics`.
    ///
    /// This is the *behavioral gate* the plan flagged for verifying the
    /// `retain_topic_with_id_fn` plumbing.
    #[test]
    fn test_subscription_to_broker_regex_retains_assigned_topics() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_re2j_pattern(SubscriptionPattern::new("__.*"), None)
            .unwrap();

        let assigned_topic_id = Uuid::random_uuid();
        sub.lock().unwrap().set_assigned_topic_ids(HashSet::from([assigned_topic_id]));

        let metadata = new_consumer_metadata(Arc::clone(&sub), false);

        // The metadata request scopes to the topic IDs.
        let builder = metadata.new_metadata_request_builder();
        assert_eq!(builder.topic_ids(), vec![assigned_topic_id]);

        // Broker responds with the topic name AND id. The name does NOT
        // match the client-side `needs_metadata` check (no entry in
        // `SubscriptionState.subscription` — only the topic id was set).
        // The two-arg retain should still keep the topic because its id is
        // in `assigned_topic_ids` / `is_assigned_from_re2j`.
        let response = build_response(&[("__matching_topic", false, assigned_topic_id)]);
        metadata.update_with_current_request_version(&response, false, 1000);

        assert_eq!(topics_set(&metadata), HashSet::from(["__matching_topic".to_string()]));
    }

    /// Translated from `ConsumerMetadataTest.testSubscriptionToBrokerRegexAllowsTransientTopics`.
    #[test]
    fn test_subscription_to_broker_regex_allows_transient_topics() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_re2j_pattern(SubscriptionPattern::new("__.*"), None)
            .unwrap();
        let assigned_topic_id = Uuid::random_uuid();
        sub.lock().unwrap().set_assigned_topic_ids(HashSet::from([assigned_topic_id]));

        let metadata = new_consumer_metadata(Arc::clone(&sub), false);

        let builder = metadata.new_metadata_request_builder();
        assert!(!builder.is_all_topics());
        assert_eq!(builder.topic_ids(), vec![assigned_topic_id]);

        // Adding a transient topic temporarily switches to topic-name request.
        let transient_topic = "__transient_topic";
        metadata.add_transient_topics(HashSet::from([transient_topic.to_string()]));
        let builder = metadata.new_metadata_request_builder();
        assert!(!builder.is_all_topics());
        assert_eq!(builder.topics(), vec![transient_topic]);

        // Clearing transient topics restores the topic-id request.
        metadata.clear_transient_topics();
        let builder = metadata.new_metadata_request_builder();
        assert!(!builder.is_all_topics());
        assert_eq!(builder.topic_ids(), vec![assigned_topic_id]);
    }

    /// Translated from `ConsumerMetadataTest.testUserAssignment`.
    #[test]
    fn test_user_assignment() {
        let sub = new_subscription();
        let tp_foo_0 = TopicPartition::new("foo".to_string(), 0);
        let tp_bar_0 = TopicPartition::new("bar".to_string(), 0);
        let tp_consumer_offsets_0 = TopicPartition::new("__consumer_offsets".to_string(), 0);
        sub.lock()
            .unwrap()
            .assign_from_user(HashSet::from([
                tp_foo_0.clone(),
                tp_bar_0.clone(),
                tp_consumer_offsets_0.clone(),
            ]))
            .unwrap();
        assert_basic_subscription(
            Arc::clone(&sub),
            HashSet::from(["foo".to_string(), "bar".to_string()]),
            HashSet::from(["__consumer_offsets".to_string()]),
        );

        let tp_baz_0 = TopicPartition::new("baz".to_string(), 0);
        sub.lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp_baz_0, tp_consumer_offsets_0]))
            .unwrap();
        assert_basic_subscription(
            sub,
            HashSet::from(["baz".to_string()]),
            HashSet::from(["__consumer_offsets".to_string()]),
        );
    }

    /// Translated from `ConsumerMetadataTest.testNormalSubscription`.
    #[test]
    fn test_normal_subscription() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_topics(
                HashSet::from(["foo".to_string(), "bar".to_string(), "__consumer_offsets".to_string()]),
                None,
            )
            .unwrap();
        sub.lock()
            .unwrap()
            .group_subscribe(&[
                "baz".to_string(),
                "foo".to_string(),
                "bar".to_string(),
                "__consumer_offsets".to_string(),
            ])
            .unwrap();
        assert_basic_subscription(
            Arc::clone(&sub),
            HashSet::from(["foo".to_string(), "bar".to_string(), "baz".to_string()]),
            HashSet::from(["__consumer_offsets".to_string()]),
        );

        sub.lock().unwrap().reset_group_subscription();
        assert_basic_subscription(
            sub,
            HashSet::from(["foo".to_string(), "bar".to_string()]),
            HashSet::from(["__consumer_offsets".to_string()]),
        );
    }

    /// Translated from `ConsumerMetadataTest.testTransientTopics`.
    #[test]
    fn test_transient_topics() {
        let sub = new_subscription();
        sub.lock()
            .unwrap()
            .subscribe_topics(HashSet::from(["foo".to_string()]), None)
            .unwrap();
        let metadata = new_consumer_metadata(Arc::clone(&sub), false);

        let foo_id = Uuid::random_uuid();
        metadata.update_with_current_request_version(&build_response(&[("foo", false, foo_id)]), false, 100);
        assert_eq!(metadata.topic_ids().get("foo"), Some(&foo_id));
        assert!(!metadata.update_requested());

        metadata.add_transient_topics(HashSet::from(["foo".to_string()]));
        assert!(!metadata.update_requested());

        metadata.add_transient_topics(HashSet::from(["bar".to_string()]));
        assert!(metadata.update_requested());

        let bar_id = Uuid::random_uuid();
        metadata.update_with_current_request_version(
            &build_response(&[("foo", false, foo_id), ("bar", false, bar_id)]),
            false,
            200,
        );
        assert_eq!(metadata.topic_ids().get("foo"), Some(&foo_id));
        assert_eq!(metadata.topic_ids().get("bar"), Some(&bar_id));
        assert!(!metadata.update_requested());

        assert_eq!(topics_set(&metadata), HashSet::from(["foo".to_string(), "bar".to_string()]));

        metadata.clear_transient_topics();
        // After clearing, a refresh that doesn't include bar drops it.
        metadata.update_with_current_request_version(&build_response(&[("foo", false, foo_id)]), false, 300);
        assert_eq!(topics_set(&metadata), HashSet::from(["foo".to_string()]));
        assert_eq!(metadata.topic_ids().get("foo"), Some(&foo_id));
        assert!(!metadata.topic_ids().contains_key("bar"));
    }

    /// Translated from `ConsumerMetadataTest.testBasicSubscription` helper.
    /// `expected_topics` are non-internal; `expected_internal_topics` are
    /// internal (always allowed because `topic_metadata` calls
    /// `Topic::is_internal` — we don't use it here, the helper just labels
    /// them is_internal=true).
    fn assert_basic_subscription(
        sub: Arc<Mutex<SubscriptionState>>,
        expected_topics: HashSet<String>,
        expected_internal_topics: HashSet<String>,
    ) {
        let mut all_topics = expected_topics.clone();
        all_topics.extend(expected_internal_topics.iter().cloned());

        let metadata = new_consumer_metadata(sub, false);

        // newMetadataRequestBuilder topics should cover the union.
        let builder = metadata.new_metadata_request_builder();
        let builder_topics: HashSet<String> = builder.topics().iter().map(|s| s.to_string()).collect();
        assert_eq!(builder_topics, all_topics);

        // After updating with all topics in the response, fetch.topics()
        // should include the union (internal topics retained because
        // `needs_metadata` returns true for them).
        let mut response_topics: Vec<(&str, bool, Uuid)> = Vec::new();
        let owned_topics: Vec<String> = expected_topics.iter().cloned().collect();
        let owned_internal: Vec<String> = expected_internal_topics.iter().cloned().collect();
        for t in &owned_topics {
            response_topics.push((t.as_str(), false, Uuid::zero()));
        }
        for t in &owned_internal {
            response_topics.push((t.as_str(), true, Uuid::zero()));
        }
        let response = build_response(&response_topics);
        metadata.update_with_current_request_version(&response, false, 1000);
        // Internal topic `__consumer_offsets` is in `subscription.needs_metadata`
        // because it's part of the user's subscription/assignment so it's
        // retained even with include_internal_topics=false.
        assert_eq!(topics_set(&metadata), all_topics);
    }

    /// Sanity: the `Topic` const re-export is reachable. Mirrors the
    /// Java test's `Topic.GROUP_METADATA_TOPIC_NAME` usage even though we
    /// don't exercise that path here.
    #[test]
    fn test_topic_constant_is_accessible() {
        assert_eq!(GROUP_METADATA_TOPIC_NAME, "__consumer_offsets");
    }
}
