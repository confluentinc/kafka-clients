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

//! DescribeTopicPartitions response handling (KIP-966).
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeTopicPartitionsResponse`.

use std::collections::HashMap;
use std::io;

use crate::DescribeTopicPartitionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{Node, TopicPartitionInfo};
use crate::describe_topic_partitions_response_data::DescribeTopicPartitionsResponsePartition;

use super::AbstractResponse;

/// A DescribeTopicPartitions response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeTopicPartitionsResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse")]
pub struct DescribeTopicPartitionsResponse {
    data: DescribeTopicPartitionsResponseData,
}

impl DescribeTopicPartitionsResponse {
    /// Creates a new `DescribeTopicPartitionsResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#DescribeTopicPartitionsResponse")]
    pub fn new(data: DescribeTopicPartitionsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_TOPIC_PARTITIONS
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#data")]
    pub fn data(&self) -> &DescribeTopicPartitionsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeTopicPartitionsResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Whether the client should throttle on this response (always).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#shouldClientThrottle")]
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }

    /// Returns the error counts for this response: every partition error of
    /// every topic, then the topic's own error.
    ///
    /// Mirrors `DescribeTopicPartitionsResponse.errorCounts`.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut error_counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut error_counts, Errors::for_code(partition.error_code));
            }
            AbstractResponse::update_error_counts(&mut error_counts, Errors::for_code(topic.error_code));
        }
        error_counts
    }

    /// Parses a `DescribeTopicPartitionsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeTopicPartitionsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Converts a wire partition into a [`TopicPartitionInfo`], resolving broker
    /// ids through `nodes`.
    ///
    /// Mirrors `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`:
    /// the leader is `nodes.get(leaderId)` (`None` when the id is unknown), and
    /// every replica, ISR, ELR and last-known-ELR id missing from `nodes` becomes
    /// `new Node(id, "", -1)`.
    ///
    /// Java dereferences `eligibleLeaderReplicas()` and `lastKnownElr()`
    /// unconditionally, so a null list (both fields are `nullableVersions: 0+`)
    /// would throw a `NullPointerException` there. A 4.3.1 broker always sends
    /// a list (`KRaftMetadataCache.java:221-222`). Rust keeps a null list as
    /// `None`, the same "unavailable" value [`TopicPartitionInfo::elr`] reports
    /// for Java's four-argument constructor.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeTopicPartitionsResponse#partitionToTopicPartitionInfo")]
    pub fn partition_to_topic_partition_info(
        partition: &DescribeTopicPartitionsResponsePartition,
        nodes: &HashMap<i32, Node>,
    ) -> TopicPartitionInfo {
        let node_or_default = |id: &i32| nodes.get(id).cloned().unwrap_or_else(|| Node::new(*id, String::new(), -1));
        let to_nodes = |ids: &[i32]| ids.iter().map(node_or_default).collect::<Vec<_>>();
        TopicPartitionInfo::with_nullable_elr_last_known_elr(
            partition.partition_index,
            nodes.get(&partition.leader_id).cloned(),
            to_nodes(&partition.replica_nodes),
            to_nodes(&partition.isr_nodes),
            partition.eligible_leader_replicas.as_deref().map(to_nodes),
            partition.last_known_elr.as_deref().map(to_nodes),
        )
    }
}

impl std::fmt::Display for DescribeTopicPartitionsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeTopicPartitionsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;
    use crate::describe_topic_partitions_response_data::{Cursor, DescribeTopicPartitionsResponseTopic};

    /// `RequestResponseTest.createDescribeTopicPartitionsResponse`: topic `foo`
    /// (id `sKhZV8LnTA275KvByB9bVg`) with partition 1 led by broker 1, and a
    /// next cursor `(foo, 2)`. The ELR lists are left at their `null` default.
    fn java_response_data() -> DescribeTopicPartitionsResponseData {
        let mut partition = DescribeTopicPartitionsResponsePartition::new();
        partition
            .set_error_code(0)
            .set_isr_nodes(vec![1])
            .set_partition_index(1)
            .set_leader_id(1)
            .set_replica_nodes(vec![1])
            .set_leader_epoch(0);
        let mut topic = DescribeTopicPartitionsResponseTopic::new();
        topic
            .set_topic_id(Uuid::from_string("sKhZV8LnTA275KvByB9bVg").unwrap())
            .set_error_code(0)
            .set_is_internal(false)
            .set_name(Some("foo".to_string()))
            .set_topic_authorized_operations(0)
            .set_partitions(vec![partition]);
        let mut cursor = Cursor::new();
        cursor.set_topic_name("foo".to_string()).set_partition_index(2);
        let mut data = DescribeTopicPartitionsResponseData::new();
        data.set_topics(vec![topic]).set_next_cursor(Some(cursor));
        data
    }

    /// The v0 encoding of [`java_response_data`].
    const JAVA_RESPONSE_V0: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
        0x02, // topics: compact array, 1 element
        0x00, 0x00, // error_code = 0
        0x04, b'f', b'o', b'o', // name "foo" (compact nullable string)
        0xb0, 0xa8, 0x59, 0x57, 0xc2, 0xe7, 0x4c, 0x0d, // topic_id, high 8 bytes
        0xbb, 0xe4, 0xab, 0xc1, 0xc8, 0x1f, 0x5b, 0x56, // topic_id, low 8 bytes
        0x00, // is_internal = false
        0x02, // partitions: compact array, 1 element
        0x00, 0x00, // error_code = 0
        0x00, 0x00, 0x00, 0x01, // partition_index = 1
        0x00, 0x00, 0x00, 0x01, // leader_id = 1
        0x00, 0x00, 0x00, 0x00, // leader_epoch = 0
        0x02, 0x00, 0x00, 0x00, 0x01, // replica_nodes [1]
        0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes [1]
        0x00, // eligible_leader_replicas = null
        0x00, // last_known_elr = null
        0x01, // offline_replicas []
        0x00, // partition tagged fields
        0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
        0x00, // topic tagged fields
        0x01, // next_cursor present
        0x04, b'f', b'o', b'o', // next_cursor.topic_name "foo"
        0x00, 0x00, 0x00, 0x02, // next_cursor.partition_index = 2
        0x00, // Cursor tagged fields
        0x00, // response tagged fields
    ];

    fn serialize(data: DescribeTopicPartitionsResponseData) -> Vec<u8> {
        let mut response = ConcreteResponse::DescribeTopicPartitions(DescribeTopicPartitionsResponse::new(data));
        response.serialize(0).unwrap().into_buffer().as_slice().to_vec()
    }

    /// Byte-level encoding against a known vector (v0, flexible). A null
    /// nullable compact array is the varint `0`; an empty one is `1`.
    #[test]
    fn serialize_known_byte_vector_v0() {
        assert_eq!(serialize(java_response_data()), JAVA_RESPONSE_V0);
    }

    /// Decoding the known vector yields the Java fixture back, with the ELR
    /// lists null.
    #[test]
    fn parse_known_byte_vector_v0() {
        let mut readable = ByteBufferAccessor::new(JAVA_RESPONSE_V0.to_vec());
        let parsed = DescribeTopicPartitionsResponse::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data(), &java_response_data());
        let partition = &parsed.data().topics[0].partitions[0];
        assert_eq!(partition.eligible_leader_replicas, None);
        assert_eq!(partition.last_known_elr, None);
        assert_eq!(parsed.data().next_cursor.as_ref().unwrap().partition_index, 2);
    }

    /// The `ConcreteResponse` arms (`admin-client.md` §7): parsing by API key
    /// reaches this type, and the throttle / error-count accessors dispatch to it.
    #[test]
    fn concrete_response_arms_dispatch_to_describe_topic_partitions() {
        let mut readable = ByteBufferAccessor::new(JAVA_RESPONSE_V0.to_vec());
        let mut response = ConcreteResponse::parse(&ApiKeys::DESCRIBE_TOPIC_PARTITIONS, &mut readable, 0).unwrap();
        assert_eq!(response.api_key(), &ApiKeys::DESCRIBE_TOPIC_PARTITIONS);
        assert!(response.should_client_throttle(0));
        response.maybe_set_throttle_time_ms(5);
        assert_eq!(response.throttle_time_ms(), 5);
        assert_eq!(response.error_counts().get(&Errors::None), Some(&2));
        let ConcreteResponse::DescribeTopicPartitions(inner) = &response else {
            panic!("expected a DescribeTopicPartitions response, got {response}");
        };
        assert_eq!(inner.data().topics[0].name.as_deref(), Some("foo"));
    }

    /// Empty ELR lists — what a healthy 4.x broker sends — encode as the
    /// compact-array length `1` and decode back as `Some([])`, not `None`.
    #[test]
    fn empty_elr_lists_are_distinct_from_null_on_the_wire() {
        let mut data = java_response_data();
        data.set_next_cursor(None);
        let partition = &mut data.topics[0].partitions[0];
        partition
            .set_eligible_leader_replicas(Some(Vec::new()))
            .set_last_known_elr(Some(vec![2]));
        let bytes = serialize(data.clone());
        // Everything before `isr_nodes` is the known vector's prefix.
        assert_eq!(
            &bytes[..JAVA_RESPONSE_V0.len() - 25],
            &JAVA_RESPONSE_V0[..JAVA_RESPONSE_V0.len() - 25]
        );
        let expected_tail: &[u8] = &[
            0x02, 0x00, 0x00, 0x00, 0x01, // isr_nodes [1]
            0x01, // eligible_leader_replicas = []
            0x02, 0x00, 0x00, 0x00, 0x02, // last_known_elr = [2]
            0x01, // offline_replicas []
            0x00, // partition tagged fields
            0x00, 0x00, 0x00, 0x00, // topic_authorized_operations = 0
            0x00, // topic tagged fields
            0xff, // next_cursor = null
            0x00, // response tagged fields
        ];
        assert!(bytes.ends_with(expected_tail), "got {bytes:02x?}");
        let mut readable = ByteBufferAccessor::new(bytes);
        let parsed = DescribeTopicPartitionsResponse::parse(&mut readable, 0).unwrap();
        let partition = &parsed.data().topics[0].partitions[0];
        assert_eq!(partition.eligible_leader_replicas, Some(Vec::new()));
        assert_eq!(partition.last_known_elr, Some(vec![2]));
        assert_eq!(parsed.data(), &data);
    }

    /// `errorCounts` counts every partition error and every topic error.
    #[test]
    fn error_counts_include_partition_and_topic_errors() {
        let mut data = java_response_data();
        data.topics[0].partitions[0].set_error_code(Errors::LeaderNotAvailable.code());
        let mut failed = DescribeTopicPartitionsResponseTopic::new();
        failed
            .set_name(Some("bar".to_string()))
            .set_error_code(Errors::TopicAuthorizationFailed.code());
        data.topics.push(failed);
        let counts = DescribeTopicPartitionsResponse::new(data).error_counts();
        assert_eq!(counts.get(&Errors::LeaderNotAvailable), Some(&1));
        assert_eq!(counts.get(&Errors::TopicAuthorizationFailed), Some(&1));
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.len(), 3);
    }

    #[test]
    fn throttle_time_accessors() {
        let mut response = DescribeTopicPartitionsResponse::new(java_response_data());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(42);
        assert_eq!(response.throttle_time_ms(), 42);
        assert!(response.should_client_throttle(0));
    }

    /// `partitionToTopicPartitionInfo` resolves every id through the node map,
    /// substitutes `Node(id, "", -1)` for an unknown replica / ISR / ELR id, and
    /// leaves an unknown leader `None`.
    #[test]
    fn partition_to_topic_partition_info_resolves_nodes() {
        let nodes: HashMap<i32, Node> = (0..2).map(|id| (id, Node::new(id, format!("host{id}"), 9092))).collect();
        let mut partition = DescribeTopicPartitionsResponsePartition::new();
        partition
            .set_partition_index(3)
            .set_leader_id(0)
            .set_replica_nodes(vec![0, 1, 5])
            .set_isr_nodes(vec![0, 5])
            .set_eligible_leader_replicas(Some(vec![1]))
            .set_last_known_elr(Some(vec![7]));
        let info = DescribeTopicPartitionsResponse::partition_to_topic_partition_info(&partition, &nodes);
        assert_eq!(info.partition(), 3);
        assert_eq!(info.leader(), Some(&nodes[&0]));
        let unknown = |id: i32| Node::new(id, String::new(), -1);
        assert_eq!(info.replicas(), &[nodes[&0].clone(), nodes[&1].clone(), unknown(5)]);
        assert_eq!(info.isr(), &[nodes[&0].clone(), unknown(5)]);
        assert_eq!(info.elr(), Some(&[nodes[&1].clone()][..]));
        assert_eq!(info.last_known_elr(), Some(&[unknown(7)][..]));

        // An unknown leader id is `nodes.get(..)` → null; empty ELR lists stay
        // empty lists; null ELR lists stay unavailable.
        partition
            .set_leader_id(9)
            .set_eligible_leader_replicas(Some(Vec::new()))
            .set_last_known_elr(None);
        let info = DescribeTopicPartitionsResponse::partition_to_topic_partition_info(&partition, &nodes);
        assert_eq!(info.leader(), None);
        assert_eq!(info.elr(), Some(&[][..]));
        assert_eq!(info.last_known_elr(), None);
    }
}
