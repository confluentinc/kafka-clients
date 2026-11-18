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

//! `ShareAcknowledge` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeResponse`.
//!
//! Possible error codes:
//!  - `GroupAuthorizationFailed`
//!  - `TopicAuthorizationFailed`
//!  - `UnknownTopicOrPartition`
//!  - `NotLeaderOrFollower`
//!  - `UnknownTopicId`
//!  - `InvalidRecordState`
//!  - `KafkaStorageError`
//!  - `InvalidRequest`
//!  - `UnknownServerError`
//!
//! Java's private `matchingTopic(ShareAcknowledgeTopicResponse, TopicIdPartition)`
//! has no caller in Java 4.4.0, so it is not translated.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::ShareAcknowledgeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{Node, TopicIdPartition};
use crate::share_acknowledge_response_data::{NodeEndpoint, PartitionData, ShareAcknowledgeTopicResponse};

use super::AbstractResponse;

/// A `ShareAcknowledge` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse")]
pub struct ShareAcknowledgeResponse {
    data: ShareAcknowledgeResponseData,
}

impl ShareAcknowledgeResponse {
    /// Creates a new `ShareAcknowledgeResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#ShareAcknowledgeResponse")]
    pub fn new(data: ShareAcknowledgeResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    /// Returns the top-level error.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#error")]
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#data")]
    pub fn data(&self) -> &ShareAcknowledgeResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareAcknowledgeResponseData {
        &mut self.data
    }

    /// Returns the error counts: the top-level error once, plus one per
    /// partition.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for topic in &self.data.responses {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `ShareAcknowledgeResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareAcknowledgeResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Returns a partition response carrying `error` for the partition of
    /// `topic_id_partition`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#partitionResponse")]
    pub fn partition_response_with_topic_id_partition(
        topic_id_partition: &TopicIdPartition,
        error: Errors,
    ) -> PartitionData {
        Self::partition_response_with_partition(topic_id_partition.partition(), error)
    }

    /// Returns a partition response carrying `error` for `partition`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#partitionResponse")]
    pub fn partition_response_with_partition(partition: i32, error: Errors) -> PartitionData {
        let mut partition_data = PartitionData::new();
        partition_data.set_partition_index(partition).set_error_code(error.code());
        partition_data
    }

    /// Builds a response from per-partition data, in iteration order.
    ///
    /// Translates `ShareAcknowledgeResponse.of(Errors, int, LinkedHashMap, List<Node>, int)`;
    /// the iterator stands in for the `LinkedHashMap` and moves each partition in.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#of")]
    pub fn of(
        error: Errors,
        throttle_time_ms: i32,
        response_data: impl IntoIterator<Item = (TopicIdPartition, PartitionData)>,
        node_endpoints: &[Node],
        acquisition_lock_timeout: i32,
    ) -> Self {
        Self::new(Self::to_message(
            error,
            throttle_time_ms,
            response_data,
            node_endpoints,
            acquisition_lock_timeout,
        ))
    }

    /// Builds the response data: partitions grouped by topic id in first-seen
    /// order, each partition index set from its key, plus the node endpoints.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeResponse#toMessage")]
    pub fn to_message(
        error: Errors,
        throttle_time_ms: i32,
        partitions: impl IntoIterator<Item = (TopicIdPartition, PartitionData)>,
        node_endpoints: &[Node],
        acquisition_lock_timeout: i32,
    ) -> ShareAcknowledgeResponseData {
        let mut topic_responses: Vec<ShareAcknowledgeTopicResponse> = Vec::new();
        for (topic_id_partition, mut partition_data) in partitions {
            // Since PartitionData alone doesn't know the partition ID, we set it here.
            partition_data.set_partition_index(topic_id_partition.partition());
            // Checking if the topic is already present in the map.
            let topic_id = topic_id_partition.topic_id();
            let index = match topic_responses.iter().position(|topic| topic.topic_id == topic_id) {
                Some(index) => index,
                None => {
                    let mut topic_response = ShareAcknowledgeTopicResponse::new();
                    topic_response.set_topic_id(topic_id);
                    topic_responses.push(topic_response);
                    topic_responses.len() - 1
                },
            };
            topic_responses[index].partitions.push(partition_data);
        }
        let mut data = ShareAcknowledgeResponseData::new();
        // KafkaApis should only pass in node endpoints on error, otherwise this should
        // be an empty list.
        for endpoint in node_endpoints {
            let mut node_endpoint = NodeEndpoint::new();
            node_endpoint
                .set_node_id(endpoint.id())
                .set_host(endpoint.host().to_string())
                .set_port(endpoint.port())
                .set_rack(endpoint.rack().map(str::to_string));
            data.node_endpoints.push(node_endpoint);
        }
        data.set_throttle_time_ms(throttle_time_ms)
            .set_error_code(error.code())
            .set_acquisition_lock_timeout_ms(acquisition_lock_timeout)
            .set_responses(topic_responses);
        data
    }
}

impl fmt::Display for ShareAcknowledgeResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;

    /// `RequestResponseTest.createShareAcknowledgeResponse()`.
    fn create_share_acknowledge_response() -> ShareAcknowledgeResponse {
        let mut partition = PartitionData::new();
        partition.set_partition_index(0).set_error_code(Errors::None.code());
        let mut topic = ShareAcknowledgeTopicResponse::new();
        topic.set_topic_id(Uuid::random_uuid()).set_partitions(vec![partition]);
        let mut data = ShareAcknowledgeResponseData::new();
        data.set_responses(vec![topic]);
        data.set_throttle_time_ms(345);
        data.set_error_code(Errors::None.code());
        ShareAcknowledgeResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `SHARE_ACKNOWLEDGE`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::ShareAcknowledge(create_share_acknowledge_response());
        for version in ApiKeys::SHARE_ACKNOWLEDGE.all_versions() {
            check_response(&response, version);
        }
    }

    /// `RequestResponseTest#testErrorCountsIncludesNone`:
    /// `assertEquals(2, createShareAcknowledgeResponse().errorCounts().get(Errors.NONE))`.
    #[test]
    fn test_error_counts_includes_none() {
        assert_eq!(Some(&2), create_share_acknowledge_response().error_counts().get(&Errors::None));
    }

    #[test]
    fn test_partition_response() {
        let tip = TopicIdPartition::with_partition_topic(Uuid::new(0, 1), 5, "t");
        let by_tip =
            ShareAcknowledgeResponse::partition_response_with_topic_id_partition(&tip, Errors::InvalidRecordState);
        assert_eq!(5, by_tip.partition_index());
        assert_eq!(Errors::InvalidRecordState.code(), by_tip.error_code());
        let by_index = ShareAcknowledgeResponse::partition_response_with_partition(2, Errors::None);
        assert_eq!(2, by_index.partition_index());
        assert_eq!(0, by_index.error_code());
        assert_eq!(&None, by_index.error_message());
    }

    /// `of` groups partitions by topic id in first-seen order, sets each
    /// partition index from its key and copies the node endpoints.
    #[test]
    fn test_of_groups_by_topic() {
        let topic_b = Uuid::new(0, 2);
        let topic_a = Uuid::new(0, 1);
        let entries = vec![
            (TopicIdPartition::with_partition_topic(topic_b, 1, "b"), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(topic_a, 0, "a"), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(topic_b, 9, "b"), PartitionData::new()),
        ];
        let nodes = [Node::with_rack(2, "h".to_string(), 1, Some("rack".to_string()))];
        let response = ShareAcknowledgeResponse::of(Errors::NotLeaderOrFollower, 3, entries, &nodes, 4);
        let data = response.data();
        assert_eq!(3, data.throttle_time_ms());
        assert_eq!(4, data.acquisition_lock_timeout_ms());
        assert_eq!(Errors::NotLeaderOrFollower, response.error());
        let layout: Vec<(Uuid, Vec<i32>)> = data
            .responses()
            .iter()
            .map(|t| (t.topic_id(), t.partitions().iter().map(|p| p.partition_index()).collect()))
            .collect();
        assert_eq!(vec![(topic_b, vec![1, 9]), (topic_a, vec![0])], layout);
        let endpoint = &data.node_endpoints()[0];
        assert_eq!(
            (2, "h", 1, Some("rack")),
            (
                endpoint.node_id(),
                endpoint.host().as_str(),
                endpoint.port(),
                endpoint.rack().as_deref()
            )
        );
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_share_acknowledge_response();
        assert_eq!(345, response.throttle_time_ms());
        response.maybe_set_throttle_time_ms(1);
        assert_eq!(1, response.throttle_time_ms());
        assert_eq!(&ApiKeys::SHARE_ACKNOWLEDGE, response.api_key());
    }

    /// Byte-level v1 and v2 encodings, derived by hand from
    /// `ShareAcknowledgeResponse.json` in field order (all versions flexible).
    /// They differ only in `AcquisitionLockTimeoutMs`, a v2+ ignorable field
    /// that v1 drops.
    #[test]
    fn test_v1_and_v2_encoding() {
        let mut partition = PartitionData::new();
        partition
            .set_error_code(Errors::InvalidRecordState.code())
            .set_error_message(Some("x".to_string()));
        partition.current_leader_mut().set_leader_id(-1).set_leader_epoch(-1);
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let response = ShareAcknowledgeResponse::of(
            Errors::None,
            2,
            vec![(TopicIdPartition::with_partition_topic(topic_id, 4, "t"), partition)],
            &[],
            100,
        );

        let encode = |version: i16| {
            let mut expected = Vec::new();
            expected.extend_from_slice(&2i32.to_be_bytes()); // ThrottleTimeMs
            expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
            expected.push(0x00); // ErrorMessage: null
            if version >= 2 {
                expected.extend_from_slice(&100i32.to_be_bytes()); // AcquisitionLockTimeoutMs
            }
            expected.push(0x02); // Responses: compact array of 1
            expected.extend_from_slice(&[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            ]); // TopicId
            expected.push(0x02); // Partitions: compact array of 1
            expected.extend_from_slice(&4i32.to_be_bytes()); // PartitionIndex
            expected.extend_from_slice(&Errors::InvalidRecordState.code().to_be_bytes()); // ErrorCode
            expected.extend_from_slice(&[0x02, b'x']); // ErrorMessage
            expected.extend_from_slice(&(-1i32).to_be_bytes()); // CurrentLeader.LeaderId
            expected.extend_from_slice(&(-1i32).to_be_bytes()); // CurrentLeader.LeaderEpoch
            expected.push(0x00); // CurrentLeader tagged fields
            expected.push(0x00); // partition tagged fields
            expected.push(0x00); // topic tagged fields
            expected.push(0x01); // NodeEndpoints: compact array of 0
            expected.push(0x00); // response tagged fields
            expected
        };

        for version in ApiKeys::SHARE_ACKNOWLEDGE.all_versions() {
            let mut concrete = ConcreteResponse::ShareAcknowledge(response.clone());
            assert_eq!(
                encode(version),
                concrete.serialize(version).unwrap().into_buffer(),
                "v{version}"
            );
        }
        let expected = encode(1);
        let parsed = ShareAcknowledgeResponse::parse(&mut ByteBufferAccessor::new(expected), 1).unwrap();
        assert_eq!(
            HashMap::from([(Errors::None, 1), (Errors::InvalidRecordState, 1)]),
            parsed.error_counts()
        );
    }
}
