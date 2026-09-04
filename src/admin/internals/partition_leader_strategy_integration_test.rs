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

//! Integration-level tests that drive a real [`AdminApiDriver`] through the real
//! [`PartitionLeaderStrategy`] / [`PartitionLeaderCache`] /
//! [`PartitionLeaderFuture`] closure, exercising the lookup→fulfillment caching
//! behavior end-to-end.
//!
//! Translated from
//! `org.apache.kafka.clients.admin.internals.PartitionLeaderStrategyIntegrationTest`.
//! Despite the "Integration" name this is a client-side unit test: it feeds
//! constructed responses directly into the driver and never touches the network.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::admin::internals::admin_api_driver::{AdminApiDriver, RequestSpec};
use crate::admin::internals::admin_api_handler::{AdminApiHandler, ApiResult, RequestAndKeys};
use crate::admin::internals::admin_api_lookup_strategy::AdminApiLookupStrategy;
use crate::admin::internals::partition_leader_cache::PartitionLeaderCache;
use crate::admin::internals::partition_leader_strategy::{PartitionLeaderFuture, PartitionLeaderStrategy};
use crate::common::protocol::{ApiKeys, Errors};
use crate::common::requests::{
    ConcreteResponse, ListOffsetsResponse, MetadataRequestBuilder, MetadataResponse, RequestBuilder,
};
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::common::{Error, KafkaFuture, Node, TopicPartition};
use crate::list_offsets_response_data::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
};
use crate::metadata_response_data::{MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic};

const TIMEOUT_MS: i64 = 5000;
const RETRY_BACKOFF_MS: i64 = 100;
const NOW: i64 = 1_000;

fn node1() -> Node {
    Node::new(1, "host1".to_string(), 9092)
}

fn node2() -> Node {
    Node::new(2, "host2".to_string(), 9092)
}

fn tp(topic: &str, partition: i32) -> TopicPartition {
    TopicPartition::new(topic, partition)
}

/// Mirrors the anonymous `MockApiHandler` in the Java test: a `Batched` handler
/// whose fulfillment request is a dummy `Metadata` request and whose response
/// handling parses a `ListOffsets` response, routing `NOT_LEADER_OR_FOLLOWER` /
/// `LEADER_NOT_AVAILABLE` back to the lookup stage.
struct MockApiHandler {
    strategy: PartitionLeaderStrategy,
}

impl MockApiHandler {
    fn new() -> Self {
        Self { strategy: PartitionLeaderStrategy::new(LogContext::new("[test] ")) }
    }
}

impl AdminApiHandler<TopicPartition, ()> for MockApiHandler {
    fn api_name(&self) -> &str {
        "mock-api"
    }

    fn build_request(&self, _broker_id: i32, keys: &HashSet<TopicPartition>) -> Vec<RequestAndKeys<TopicPartition>> {
        vec![RequestAndKeys {
            request: Box::new(MetadataRequestBuilder::new(None, false)) as Box<dyn RequestBuilder>,
            keys: keys.clone(),
        }]
    }

    fn handle_response(
        &self,
        _broker: &Node,
        _keys: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> ApiResult<TopicPartition, ()> {
        let ConcreteResponse::ListOffsets(response) = response else {
            panic!("MockApiHandler expects a ListOffsets response");
        };

        let mut completed: HashMap<TopicPartition, ()> = HashMap::new();
        let mut failed: HashMap<TopicPartition, Error> = HashMap::new();
        let mut unmapped: Vec<TopicPartition> = Vec::new();

        for topic in &response.data().topics {
            for partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.name.clone(), partition.partition_index);
                let error = Errors::for_code(partition.error_code);
                if error != Errors::None {
                    if matches!(error, Errors::NotLeaderOrFollower | Errors::LeaderNotAvailable) {
                        unmapped.push(topic_partition);
                    } else if !Error::new(error).is_retriable_error() {
                        failed.insert(topic_partition, Error::new(error));
                    }
                } else {
                    completed.insert(topic_partition, ());
                }
            }
        }

        ApiResult { completed_keys: completed, failed_keys: failed, unmapped_keys: unmapped }
    }

    fn lookup_strategy(&self) -> &dyn AdminApiLookupStrategy<TopicPartition> {
        &self.strategy
    }
}

fn build_driver(
    request_keys: HashSet<TopicPartition>,
    cache: &Arc<PartitionLeaderCache>,
) -> (AdminApiDriver<TopicPartition, ()>, HashMap<TopicPartition, KafkaFuture<()>>) {
    let future = PartitionLeaderFuture::<()>::new(request_keys, Arc::clone(cache));
    // Capture the per-partition public futures before the future is moved into
    // the driver; they share completion state with the boxed future via `Arc`,
    // so `is_done()` observes completions the driver drives. This is how the
    // Rust test keeps the `result.all().get(tp)` handles the Java test holds.
    let futures = future.all();
    let backoff = ExponentialBackoff::new(RETRY_BACKOFF_MS, 2, RETRY_BACKOFF_MS, 0.0).unwrap();
    let driver = AdminApiDriver::new(
        Box::new(MockApiHandler::new()),
        Box::new(future),
        NOW + TIMEOUT_MS,
        backoff,
        LogContext::new("[test] "),
    );
    (driver, futures)
}

fn metadata_response_with_partition_leaders(mapping: &[(TopicPartition, i32)]) -> ConcreteResponse {
    let mut data = MetadataResponseData::new();
    let mut topics: Vec<MetadataResponseTopic> = Vec::new();
    for (topic_partition, broker_id) in mapping {
        let mut p = MetadataResponsePartition::new();
        p.set_partition_index(topic_partition.partition());
        p.set_leader_id(*broker_id);
        if let Some(existing) = topics.iter_mut().find(|t| t.name.as_deref() == Some(topic_partition.topic())) {
            existing.partitions.push(p);
        } else {
            let mut t = MetadataResponseTopic::new();
            t.set_name(Some(topic_partition.topic().to_string()));
            t.set_partitions(vec![p]);
            topics.push(t);
        }
    }
    data.set_topics(topics);
    ConcreteResponse::Metadata(MetadataResponse::new_version(data, ApiKeys::METADATA.latest_version()))
}

fn list_offsets_response(keys: &HashSet<TopicPartition>, error: Errors) -> ConcreteResponse {
    let mut data = ListOffsetsResponseData::new();
    let mut topics: Vec<ListOffsetsTopicResponse> = Vec::new();
    for tp in keys {
        let mut part = ListOffsetsPartitionResponse::new();
        part.set_partition_index(tp.partition());
        part.set_error_code(error.code());
        if let Some(existing) = topics.iter_mut().find(|t| t.name == tp.topic()) {
            existing.partitions.push(part);
        } else {
            let mut t = ListOffsetsTopicResponse::new();
            t.set_name(tp.topic().to_string());
            t.set_partitions(vec![part]);
            topics.push(t);
        }
    }
    data.set_topics(topics);
    ConcreteResponse::ListOffsets(ListOffsetsResponse::new(data))
}

fn list_offsets_response_success(keys: &HashSet<TopicPartition>) -> ConcreteResponse {
    list_offsets_response(keys, Errors::None)
}

fn list_offsets_response_failure(keys: &HashSet<TopicPartition>, error: Errors) -> ConcreteResponse {
    list_offsets_response(keys, error)
}

/// Sort request specs by their scope's destination broker id (lookup scopes,
/// which have none, sort first) so the assertions can index positionally the
/// way the Java test does. Java's `RequestSpec` list order is deterministic
/// there; the Rust driver iterates `HashMap`s, so we impose the order here.
fn sorted_by_broker(mut specs: Vec<RequestSpec<TopicPartition>>) -> Vec<RequestSpec<TopicPartition>> {
    specs.sort_by_key(|s| s.scope.destination_broker_id());
    specs
}

#[test]
fn test_caching_repeated_request() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);
    let tp1 = tp("T", 1);
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp0.clone(), tp1.clone()]);

    // First, the lookup stage needs to obtain leadership data because the cache is empty.
    let (mut driver, futures) = build_driver(request_keys.clone(), &cache);
    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, request_keys);

    // The cache will be populated using the leader information from this metadata response.
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp0.clone(), 1), (tp1.clone(), 2)]),
        Node::no_node(),
    );
    assert!(!futures[&tp0].is_done());
    assert!(!futures[&tp1].is_done());

    let cached = cache.get([&tp0, &tp1]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&2));

    // Second, the fulfillment stage makes the actual requests.
    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());

    // On the second request, the partition leader cache already contains all the leadership data,
    // so the request goes straight to the fulfillment stage.
    let (mut driver, futures) = build_driver(request_keys, &cache);
    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());
}

#[test]
fn test_caching_overlapping_requests() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);
    let tp1 = tp("T", 1);
    let tp2 = tp("T", 2);
    let tp3 = tp("T", 3);

    // Request 1 - T-0 and T-1 (cache empty).
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp0.clone(), tp1.clone()]);
    let (mut driver, futures) = build_driver(request_keys.clone(), &cache);
    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, request_keys);

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp0.clone(), 1), (tp1.clone(), 2)]),
        Node::no_node(),
    );
    assert!(!futures[&tp0].is_done());
    assert!(!futures[&tp1].is_done());
    let cached = cache.get([&tp0, &tp1]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&2));

    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());

    // Request 2 - T-1 and T-2 (T-1 cached; lookup and fulfillment overlap).
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp1.clone(), tp2.clone()]);
    let (mut driver, futures) = build_driver(request_keys, &cache);
    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    // The lookup spec (no broker id) sorts first; it covers only T-2.
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, HashSet::from([tp2.clone()]));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp2.clone(), 1)]),
        Node::no_node(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node2(),
    );
    assert!(futures[&tp1].is_done()); // Already fulfilled.
    assert!(!futures[&tp2].is_done());

    let cached = cache.get([&tp0, &tp1, &tp2]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&2));
    assert_eq!(cached.get(&tp2), Some(&1));

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    assert!(futures[&tp1].is_done());
    assert!(futures[&tp2].is_done());

    // Request 3 - T-0, T-1 and T-2 (all cached).
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp0.clone(), tp1.clone(), tp2.clone()]);
    let (mut driver, futures) = build_driver(request_keys, &cache);
    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());
    assert!(futures[&tp2].is_done());

    // Request 4 - T-0, T-1, T-2 and T-3 (only T-3 needs lookup).
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp0.clone(), tp1.clone(), tp2.clone(), tp3.clone()]);
    let (mut driver, futures) = build_driver(request_keys, &cache);
    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 3);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, HashSet::from([tp3.clone()]));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[2].scope.destination_broker_id(), Some(2));

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp3.clone(), 2)]),
        Node::no_node(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_success(&specs[1].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[2].scope,
        &specs[2].keys,
        &list_offsets_response_success(&specs[2].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());
    assert!(futures[&tp2].is_done());
    assert!(!futures[&tp3].is_done());

    let cached = cache.get([&tp0, &tp1, &tp2, &tp3]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&2));
    assert_eq!(cached.get(&tp2), Some(&1));
    assert_eq!(cached.get(&tp3), Some(&2));

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(2));
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());
    assert!(futures[&tp2].is_done());
    assert!(futures[&tp3].is_done());
}

#[test]
fn test_not_leader_fulfillment_error() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);
    let tp1 = tp("T", 1);
    let request_keys: HashSet<TopicPartition> = HashSet::from([tp0.clone(), tp1.clone()]);

    let (mut driver, futures) = build_driver(request_keys.clone(), &cache);
    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, request_keys);

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp0.clone(), 1), (tp1.clone(), 2)]),
        Node::no_node(),
    );
    let cached = cache.get([&tp0, &tp1]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&2));

    let specs = sorted_by_broker(driver.poll());
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[1].scope.destination_broker_id(), Some(2));

    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    driver.on_response(
        NOW,
        &specs[1].scope,
        &specs[1].keys,
        &list_offsets_response_failure(&specs[1].keys, Errors::NotLeaderOrFollower),
        &node2(),
    );
    assert!(futures[&tp0].is_done());
    assert!(!futures[&tp1].is_done());

    // Now the lookup occurs again - change leadership to node 1.
    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &metadata_response_with_partition_leaders(&[(tp1.clone(), 1)]),
        Node::no_node(),
    );
    assert!(futures[&tp0].is_done());
    assert!(!futures[&tp1].is_done());
    let cached = cache.get([&tp0, &tp1]);
    assert_eq!(cached.get(&tp0), Some(&1));
    assert_eq!(cached.get(&tp1), Some(&1));

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    driver.on_response(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &list_offsets_response_success(&specs[0].keys),
        &node1(),
    );
    assert!(futures[&tp0].is_done());
    assert!(futures[&tp1].is_done());
}

/// KAFKA-20673. A leader cached from an earlier request can point at a broker
/// that has since left the cluster; such a request skips the lookup stage and
/// goes straight to fulfillment. The driver must be able to send the request
/// back to the lookup stage so the leader can be re-resolved.
#[test]
fn test_retry_lookup_for_stale_cached_leader() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);

    // Seed the cache so the request goes straight to the fulfillment stage on broker 1.
    cache.put(&HashMap::from([(tp0.clone(), 1)]));

    let (mut driver, futures) = build_driver(HashSet::from([tp0.clone()]), &cache);
    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), Some(1));
    assert_eq!(specs[0].keys, HashSet::from([tp0.clone()]));

    // Send the request back to the lookup stage. The leader can be re-resolved, so this reports
    // that it moved the key back to lookup.
    assert!(driver.maybe_retry_lookup(NOW, &specs[0].scope, &specs[0].keys));
    assert!(!futures[&tp0].is_done());

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].scope.destination_broker_id(), None);
    assert_eq!(specs[0].keys, HashSet::from([tp0.clone()]));
}

#[tokio::test]
async fn test_fatal_lookup_error() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);
    let (mut driver, futures) = build_driver(HashSet::from([tp0.clone()]), &cache);

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].keys, HashSet::from([tp0.clone()]));

    driver.on_failure(NOW, &specs[0].scope, &specs[0].keys, &Error::new(Errors::UnknownServerError));
    assert!(futures[&tp0].is_done());
    assert_eq!(futures[&tp0].get().await.unwrap_err().error(), Errors::UnknownServerError);
    assert!(driver.poll().is_empty());
}

#[test]
fn test_retry_lookup_after_disconnect() {
    let cache = Arc::new(PartitionLeaderCache::new());
    let tp0 = tp("T", 0);
    let (mut driver, _futures) = build_driver(HashSet::from([tp0.clone()]), &cache);

    let specs = driver.poll();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].keys, HashSet::from([tp0.clone()]));

    // Java's `AdminApiDriver.onFailure` tests `instanceof DisconnectException`,
    // which is its own class here — not the `NETWORK_EXCEPTION` wire code.
    driver.on_failure(
        NOW,
        &specs[0].scope,
        &specs[0].keys,
        &Error::Disconnect(crate::common::errors::DisconnectError::new("disconnected")),
    );
    let retry_specs = driver.poll();
    assert_eq!(retry_specs.len(), 1);
    assert_eq!(retry_specs[0].keys, HashSet::from([tp0.clone()]));
    assert_eq!(retry_specs[0].next_allowed_try_ms, NOW);
    assert!(driver.poll().is_empty());
}
