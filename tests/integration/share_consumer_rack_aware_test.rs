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

//! Integration test translated from
//! `org.apache.kafka.clients.consumer.ShareConsumerRackAwareTest` (Apache Kafka
//! 4.2, `clients-integration-tests`).
//!
//! # Docker / multi-broker / Admin gating (test is `#[ignore]`d)
//!
//! The single Java test, `testShareConsumerWithRackAwareAssignor`, requires
//! infrastructure the Rust testcontainers harness does not provide:
//!
//! * **A 3-broker KRaft cluster** with distinct `broker.rack` (rack0/1/2). The
//!   Rust harness provisions a single-broker cluster.
//! * **A server-side `RackAwareAssignor`** configured via
//!   `group.coordinator.share-group-assignors` — a broker-side class with no
//!   client-side surface.
//! * **An `AdminClient`** for `createTopics` with an explicit replica
//!   assignment, `alterPartitionReassignments`, and `describeShareGroups`
//!   (`ShareGroupDescription`). The Rust client has no `AdminClient`.
//!
//! The only client-observable behavior is that each `ShareConsumer` sends its
//! `client.rack` in the `ShareGroupHeartbeat` request; the broker's rack-aware
//! assignor then assigns rack-local partitions. The `client.rack` wiring is
//! already covered at the unit level (the share heartbeat's `rackId` field is
//! sent from `ConsumerConfig.client_rack`, see
//! `share_heartbeat_request_manager.rs`), and `new_share_consumer` threads
//! `client.rack` into the `ShareMembershipManager`. This test is therefore
//! translated for fidelity but `#[ignore]`d; run it manually only against a
//! multi-broker, Admin-capable, share-group-enabled cluster.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::consumer::ShareConsumerConfig;
use confluent_kafka::consumer::new_share_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::test_context::TestContext;

struct ByteArrayDeserializer;
impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

/// Build a rack-pinned share consumer (`client.rack` set).
fn create_rack_consumer(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    rack: &str,
) -> Box<dyn ShareConsumer<Vec<u8>, Vec<u8>>> {
    let mut props = HashMap::new();
    props.insert("bootstrap.servers".to_string(), bootstrap.to_string());
    props.insert("group.id".to_string(), group_id.to_string());
    props.insert("client.id".to_string(), client_id.to_string());
    props.insert("client.rack".to_string(), rack.to_string());
    let config = ShareConsumerConfig::from_properties(&props).expect("valid share consumer config");
    new_share_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .expect("share consumer construction")
}

async fn produce_record(bootstrap: &str, topic: &str, value: &str) {
    let props: HashMap<String, String> = [
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "rack-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
    ]
    .into_iter()
    .collect();
    let producer: KafkaProducer<String, String> = KafkaProducer::from_config(
        ProducerConfig::from_properties(&props).unwrap(),
        Box::new(StringSerializer),
        Box::new(StringSerializer),
    )
    .expect("build producer");
    let record = ProducerRecord::with_key(topic.to_string(), None, Some(value.to_string()));
    let future = producer.send(record).await.expect("send");
    future.get_timeout(Duration::from_secs(30)).await.expect("ack");
    producer.close().await.expect("producer close");
}

/// Java `ShareConsumerRackAwareTest.testShareConsumerWithRackAwareAssignor`.
/// See the module docs: gated on a 3-broker rack cluster + AdminClient +
/// server-side `RackAwareAssignor`. Translated for fidelity; `#[ignore]`d.
#[tokio::test]
#[ignore = "needs a 3-broker rack cluster + AdminClient + server-side RackAwareAssignor (not in the Rust harness)"]
async fn test_share_consumer_with_rack_aware_assignor() {
    let mut ctx = TestContext::new(crate::common::cluster_config::ClusterConfig::default()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let group_id = "group0";
    let topic = "test-topic";

    // Java creates the topic on broker 0 explicitly (Admin) and alters
    // share.auto.offset.reset — neither is available here.
    produce_record(&bootstrap, topic, "value").await;

    let mut consumer0 = create_rack_consumer(&bootstrap, group_id, "client0", "rack0");
    let mut consumer1 = create_rack_consumer(&bootstrap, group_id, "client1", "rack1");
    let mut consumer2 = create_rack_consumer(&bootstrap, group_id, "client2", "rack2");

    for c in [consumer0.as_mut(), consumer1.as_mut(), consumer2.as_mut()] {
        c.subscribe(vec![topic.to_string()]).await.expect("subscribe");
    }

    // With a rack-aware server-side assignor the single partition (on rack0)
    // is assigned to consumer0. Poll each until one delivers the record.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut total = 0usize;
    while Instant::now() < deadline && total == 0 {
        for c in [consumer0.as_mut(), consumer1.as_mut(), consumer2.as_mut()] {
            total += c.poll(Duration::from_millis(500)).await.expect("poll").count();
        }
    }
    assert_eq!(total, 1, "the rack-local consumer should receive the record");

    for mut c in [consumer0, consumer1, consumer2] {
        c.close().await.expect("close");
    }
    ctx.cleanup().await;
}
