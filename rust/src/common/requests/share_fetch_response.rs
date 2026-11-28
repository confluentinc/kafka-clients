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

//! `ShareFetch` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareFetchResponse`.
//!
//! Possible error codes:
//!  - `GroupAuthorizationFailed`
//!  - `TopicAuthorizationFailed`
//!  - `UnknownTopicOrPartition`
//!  - `NotLeaderOrFollower`
//!  - `UnknownTopicId`
//!  - `InvalidRecordState`
//!  - `KafkaStorageError`
//!  - `CorruptMessage`
//!  - `InvalidRequest`
//!  - `UnknownServerError`

use std::collections::HashMap;
use std::fmt;
use std::io;

use bytes::Bytes;
use indexmap::IndexMap;

use crate::ShareFetchResponseData;
use crate::common::protocol::{ApiKeys, Errors, Message, ObjectSerializationCache, Readable};
use crate::common::{Node, TopicIdPartition, TopicPartition, Uuid};
use crate::share_fetch_response_data::{NodeEndpoint, PartitionData, ShareFetchableTopicResponse};

use super::AbstractResponse;

/// A `ShareFetch` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareFetchResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse")]
pub struct ShareFetchResponse {
    data: ShareFetchResponseData,
}

impl ShareFetchResponse {
    /// Wraps the given data. Private in Java: responses come from
    /// [`parse`](Self::parse) or [`of`](Self::of).
    fn new(data: ShareFetchResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_FETCH
    }

    /// Returns the top-level error.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#error")]
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#data")]
    pub fn data(&self) -> &ShareFetchResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareFetchResponseData {
        &mut self.data
    }

    /// Returns the error counts: the top-level error once, plus one per
    /// partition.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#errorCounts")]
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

    /// Returns the per-partition data in response order, keyed by
    /// [`TopicIdPartition`] with the topic name resolved through `topic_names`.
    /// A topic whose id is not in `topic_names` is skipped, as in Java.
    ///
    /// The partition data is cloned; its record bytes are a reference-counted
    /// [`Bytes`] slice of the response buffer, so no record byte is copied.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#responseData")]
    pub fn response_data(&self, topic_names: &HashMap<Uuid, String>) -> IndexMap<TopicIdPartition, PartitionData> {
        let mut response_data = IndexMap::new();
        for topic_response in &self.data.responses {
            if let Some(name) = topic_names.get(&topic_response.topic_id) {
                for partition_data in &topic_response.partitions {
                    response_data.insert(
                        TopicIdPartition::new(
                            topic_response.topic_id,
                            TopicPartition::new(name.clone(), partition_data.partition_index),
                        ),
                        partition_data.clone(),
                    );
                }
            }
        }
        response_data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `ShareFetchResponse` from a readable buffer at the given
    /// version. Unlike [`of`](Self::of), this does not convert absent records
    /// to empty records.
    ///
    /// **This method should only be used in client-side.**
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareFetchResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Returns the partition's record bytes, or an empty slice when the
    /// partition has none (Java: `MemoryRecords.EMPTY`).
    ///
    /// Java's `ClassCastException` branch (records that are not a `Records`)
    /// has no analogue: the generated data always holds raw bytes. As with
    /// `FetchResponse::records_or_fail`, the zero-copy route for the receive
    /// path is the `records` field itself, a [`Bytes`] slice of the response
    /// buffer.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#recordsOrFail")]
    pub fn records_or_fail(partition: &PartitionData) -> &[u8] {
        partition.records.as_deref().unwrap_or(&[])
    }

    /// Returns the size in bytes of a response holding the given partitions:
    /// `4` (the size prefix) plus the serialized size of the data at `version`.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be sized at `version`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#sizeOf")]
    pub fn size_of(
        version: i16,
        partitions: impl IntoIterator<Item = (TopicIdPartition, PartitionData)>,
    ) -> io::Result<i32> {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we
        // can use arbitrary values here without affecting the result.
        let data = Self::to_message(Errors::None, 0, partitions, &[], 0);
        let mut cache = ObjectSerializationCache::new();
        Ok(4 + data.size(&mut cache, version)?)
    }

    /// Returns the size in bytes of the partition's records; `0` when it has
    /// none.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#recordsSize")]
    pub fn records_size(partition: &PartitionData) -> i32 {
        partition.records.as_ref().map_or(0, |records| records.len() as i32)
    }

    /// Builds a response from per-partition data, in iteration order. Absent
    /// records become empty records.
    ///
    /// **This method should only be used in server-side.** It also builds the
    /// client's error response (`ShareFetchRequest::get_error_response`).
    ///
    /// Translates `ShareFetchResponse.of(Errors, int, LinkedHashMap, List<Node>, int)`;
    /// the iterator stands in for the `LinkedHashMap` and moves each partition in.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#of")]
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

    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#toMessage")]
    fn to_message(
        error: Errors,
        throttle_time_ms: i32,
        partitions: impl IntoIterator<Item = (TopicIdPartition, PartitionData)>,
        node_endpoints: &[Node],
        acquisition_lock_timeout: i32,
    ) -> ShareFetchResponseData {
        let mut topic_responses: Vec<ShareFetchableTopicResponse> = Vec::new();
        for (topic_id_partition, mut partition_data) in partitions {
            // Since PartitionData alone doesn't know the partition ID, we set it here.
            partition_data.set_partition_index(topic_id_partition.partition());
            // To protect the clients from failing due to null records, we always
            // convert null records to empty records.
            if partition_data.records.is_none() {
                partition_data.set_records(Some(Bytes::new()));
            }
            // Checking if the topic is already present in the map.
            let topic_id = topic_id_partition.topic_id();
            let index = match topic_responses.iter().position(|topic| topic.topic_id == topic_id) {
                Some(index) => index,
                None => {
                    let mut topic_response = ShareFetchableTopicResponse::new();
                    topic_response.set_topic_id(topic_id);
                    topic_responses.push(topic_response);
                    topic_responses.len() - 1
                },
            };
            topic_responses[index].partitions.push(partition_data);
        }
        let mut data = ShareFetchResponseData::new();
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

    /// Returns an empty partition response carrying `error`, with empty records.
    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#partitionResponse")]
    pub fn partition_response_with_topic_id_partition(
        topic_id_partition: &TopicIdPartition,
        error: Errors,
    ) -> PartitionData {
        Self::partition_response_with_partition(topic_id_partition.partition(), error)
    }

    #[doc(alias = "org.apache.kafka.common.requests.ShareFetchResponse#partitionResponse")]
    fn partition_response_with_partition(partition: i32, error: Errors) -> PartitionData {
        let mut partition_data = PartitionData::new();
        partition_data
            .set_partition_index(partition)
            .set_error_code(error.code())
            .set_records(Some(Bytes::new()));
        partition_data
    }
}

impl fmt::Display for ShareFetchResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::record::internal::{MemoryRecords, SimpleRecord};
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;
    use crate::share_fetch_response_data::AcquiredRecords;

    /// `RequestResponseTest.createShareFetchResponse()`.
    fn create_share_fetch_response() -> ShareFetchResponse {
        let records = MemoryRecords::with_records(
            Compression::none().build(),
            &[SimpleRecord::with_value(Some(b"blah".to_vec()))],
        );
        let mut acquired = AcquiredRecords::new();
        acquired.set_first_offset(0).set_last_offset(0).set_delivery_count(1);
        let mut partition = PartitionData::new();
        partition
            .set_partition_index(0)
            .set_error_code(Errors::None.code())
            .set_records(Some(records.into_buffer()))
            .set_acquired_records(vec![acquired]);
        let topic_id_partition = TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(String::new(), 0));
        ShareFetchResponse::of(Errors::None, 345, vec![(topic_id_partition, partition)], &[], 0)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `SHARE_FETCH`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::ShareFetch(create_share_fetch_response());
        for version in ApiKeys::SHARE_FETCH.all_versions() {
            check_response(&response, version);
        }
    }

    /// `RequestResponseTest#testErrorCountsIncludesNone`:
    /// `assertEquals(2, createShareFetchResponse().errorCounts().get(Errors.NONE))`.
    #[test]
    fn test_error_counts_includes_none() {
        assert_eq!(Some(&2), create_share_fetch_response().error_counts().get(&Errors::None));
    }

    /// `RequestResponseTest#testShareFetchResponseShouldNotHaveNullRecords`:
    /// `of` turns absent records into empty records (`MemoryRecords.EMPTY`).
    #[test]
    fn test_share_fetch_response_should_not_have_null_records() {
        let id = Uuid::random_uuid();
        let mut partition_data = PartitionData::new();
        partition_data
            .set_partition_index(0)
            .set_acquired_records(Vec::new())
            .set_records(None);
        let topic_id_partition = TopicIdPartition::new(id, TopicPartition::new("test".to_string(), 0));
        let share_fetch_response =
            ShareFetchResponse::of(Errors::None, 0, vec![(topic_id_partition, partition_data)], &[], 0);
        // validateNoNullRecords
        for response in share_fetch_response.data().responses() {
            for partition in response.partitions() {
                assert_eq!(&Some(Bytes::new()), partition.records());
            }
        }
    }

    /// `of` groups partitions by topic id in first-seen order, sets each
    /// partition index from its key, and copies the node endpoints.
    #[test]
    fn test_of_groups_by_topic_and_sets_partition_index() {
        let topic_b = Uuid::new(0, 2);
        let topic_a = Uuid::new(0, 1);
        let entries = vec![
            (TopicIdPartition::with_partition_topic(topic_b, 4, "b"), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(topic_a, 1, "a"), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(topic_b, 0, "b"), PartitionData::new()),
        ];
        let nodes = [
            Node::with_rack(3, "h3".to_string(), 9093, Some("r".to_string())),
            Node::new(1, "h1".to_string(), 9091),
        ];
        let response = ShareFetchResponse::of(Errors::NotLeaderOrFollower, 7, entries, &nodes, 30_000);
        let data = response.data();
        assert_eq!(7, data.throttle_time_ms());
        assert_eq!(Errors::NotLeaderOrFollower, response.error());
        assert_eq!(30_000, data.acquisition_lock_timeout_ms());
        let layout: Vec<(Uuid, Vec<i32>)> = data
            .responses()
            .iter()
            .map(|t| (t.topic_id(), t.partitions().iter().map(|p| p.partition_index()).collect()))
            .collect();
        assert_eq!(vec![(topic_b, vec![4, 0]), (topic_a, vec![1])], layout);
        let endpoints: Vec<(i32, &str, i32, Option<&str>)> = data
            .node_endpoints()
            .iter()
            .map(|e| (e.node_id(), e.host().as_str(), e.port(), e.rack().as_deref()))
            .collect();
        assert_eq!(vec![(3, "h3", 9093, Some("r")), (1, "h1", 9091, None)], endpoints);
    }

    /// `responseData` keys by resolved name and skips unknown topic ids.
    #[test]
    fn test_response_data_skips_unresolved_topics() {
        let known = Uuid::new(0, 1);
        let unknown = Uuid::new(0, 2);
        let entries = vec![
            (TopicIdPartition::with_partition_topic(unknown, 0, ""), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(known, 2, ""), PartitionData::new()),
            (TopicIdPartition::with_partition_topic(known, 1, ""), PartitionData::new()),
        ];
        let response = ShareFetchResponse::of(Errors::None, 0, entries, &[], 0);
        let names = HashMap::from([(known, "k".to_string())]);
        let keys: Vec<TopicIdPartition> = response.response_data(&names).into_keys().collect();
        assert_eq!(
            vec![
                TopicIdPartition::with_partition_topic(known, 2, "k"),
                TopicIdPartition::with_partition_topic(known, 1, "k"),
            ],
            keys
        );
    }

    #[test]
    fn test_partition_response_and_records_helpers() {
        let tip = TopicIdPartition::with_partition_topic(Uuid::new(0, 1), 6, "t");
        let partition = ShareFetchResponse::partition_response_with_topic_id_partition(&tip, Errors::UnknownTopicId);
        assert_eq!(6, partition.partition_index());
        assert_eq!(Errors::UnknownTopicId.code(), partition.error_code());
        assert_eq!(&Some(Bytes::new()), partition.records());
        assert_eq!(0, ShareFetchResponse::records_size(&partition));
        assert!(ShareFetchResponse::records_or_fail(&partition).is_empty());

        let mut with_records = PartitionData::new();
        with_records.set_records(Some(Bytes::from_static(&[1, 2, 3])));
        assert_eq!(3, ShareFetchResponse::records_size(&with_records));
        assert_eq!(&[1, 2, 3], ShareFetchResponse::records_or_fail(&with_records));

        let mut without_records = PartitionData::new();
        without_records.set_records(None);
        assert_eq!(0, ShareFetchResponse::records_size(&without_records));
        assert!(ShareFetchResponse::records_or_fail(&without_records).is_empty());
    }

    /// `sizeOf` is the size prefix plus the serialized size of `toMessage`.
    #[test]
    fn test_size_of() {
        let entries = || {
            vec![(
                TopicIdPartition::with_partition_topic(Uuid::new(0, 1), 0, "t"),
                PartitionData::new(),
            )]
        };
        for version in ApiKeys::SHARE_FETCH.all_versions() {
            let mut response = ConcreteResponse::ShareFetch(ShareFetchResponse::of(Errors::None, 0, entries(), &[], 0));
            let serialized = response.serialize(version).unwrap().into_buffer();
            assert_eq!(
                4 + serialized.len() as i32,
                ShareFetchResponse::size_of(version, entries()).unwrap()
            );
        }
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_share_fetch_response();
        assert_eq!(345, response.throttle_time_ms());
        response.maybe_set_throttle_time_ms(1);
        assert_eq!(1, response.throttle_time_ms());
        assert_eq!(&ApiKeys::SHARE_FETCH, response.api_key());
    }

    /// Byte-level v1 encoding, derived by hand from `ShareFetchResponse.json`
    /// in field order (all versions flexible; v1 and v2 share one schema).
    /// `Records` is non-nullable from v1, so it is a compact length (`len + 1`)
    /// and the bytes.
    #[test]
    fn test_v1_encoding() {
        let mut acquired = AcquiredRecords::new();
        acquired.set_first_offset(3).set_last_offset(4).set_delivery_count(2);
        let mut partition = PartitionData::new();
        partition
            .set_error_code(0)
            .set_error_message(None)
            .set_acknowledge_error_code(0)
            .set_acknowledge_error_message(None)
            .set_records(Some(Bytes::from_static(&[0xaa, 0xbb])))
            .set_acquired_records(vec![acquired]);
        partition.current_leader_mut().set_leader_id(1).set_leader_epoch(5);
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let nodes = [Node::new(1, "h".to_string(), 9092)];
        let response = ShareFetchResponse::of(
            Errors::None,
            8,
            vec![(TopicIdPartition::with_partition_topic(topic_id, 2, "t"), partition)],
            &nodes,
            15_000,
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(&8i32.to_be_bytes()); // ThrottleTimeMs
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.extend_from_slice(&15_000i32.to_be_bytes()); // AcquisitionLockTimeoutMs (v1+)
        expected.push(0x02); // Responses: compact array of 1
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        expected.push(0x02); // Partitions: compact array of 1
        expected.extend_from_slice(&2i32.to_be_bytes()); // PartitionIndex (set by `of`)
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.extend_from_slice(&0i16.to_be_bytes()); // AcknowledgeErrorCode
        expected.push(0x00); // AcknowledgeErrorMessage: null
        expected.extend_from_slice(&1i32.to_be_bytes()); // CurrentLeader.LeaderId
        expected.extend_from_slice(&5i32.to_be_bytes()); // CurrentLeader.LeaderEpoch
        expected.push(0x00); // CurrentLeader tagged fields
        expected.extend_from_slice(&[0x03, 0xaa, 0xbb]); // Records: 2 bytes
        expected.push(0x02); // AcquiredRecords: compact array of 1
        expected.extend_from_slice(&3i64.to_be_bytes()); // FirstOffset
        expected.extend_from_slice(&4i64.to_be_bytes()); // LastOffset
        expected.extend_from_slice(&2i16.to_be_bytes()); // DeliveryCount
        expected.push(0x00); // AcquiredRecords tagged fields
        expected.push(0x00); // partition tagged fields
        expected.push(0x00); // topic tagged fields
        expected.push(0x02); // NodeEndpoints: compact array of 1
        expected.extend_from_slice(&1i32.to_be_bytes()); // NodeId
        expected.extend_from_slice(&[0x02, b'h']); // Host
        expected.extend_from_slice(&9092i32.to_be_bytes()); // Port
        expected.push(0x00); // Rack: null
        expected.push(0x00); // endpoint tagged fields
        expected.push(0x00); // response tagged fields

        for version in ApiKeys::SHARE_FETCH.all_versions() {
            let mut concrete = ConcreteResponse::ShareFetch(response.clone());
            assert_eq!(expected, concrete.serialize(version).unwrap().into_buffer(), "v{version}");
        }
        let parsed = ShareFetchResponse::parse(&mut ByteBufferAccessor::new(expected), 1).unwrap();
        assert_eq!(
            &[0xaa, 0xbb],
            ShareFetchResponse::records_or_fail(&parsed.data().responses()[0].partitions()[0])
        );
    }
}
