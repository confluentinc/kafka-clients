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
use std::io;

use indexmap::IndexMap;

use crate::common::Node;
use crate::common::TopicIdPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Message, ObjectSerializationCache, Readable};
use crate::share_fetch_response_data::{
    NodeEndpoint, PartitionData, ShareFetchResponseData, ShareFetchableTopicResponse,
};

/// A `ShareFetch` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareFetchResponse`.
#[derive(Debug, Clone)]
pub struct ShareFetchResponse {
    data: ShareFetchResponseData,
}

impl ShareFetchResponse {
    /// Creates a new `ShareFetchResponse` from the underlying data.
    ///
    /// Mirrors Java's private constructor (used by `parse` and `of`).
    pub fn new(data: ShareFetchResponseData) -> Self {
        Self { data }
    }

    /// Returns the top-level error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareFetchResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareFetchResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_FETCH
    }

    /// Aggregates the response error counts: top-level plus per-partition.
    ///
    /// Translates `ShareFetchResponse.errorCounts()`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts: HashMap<Errors, i32> = HashMap::new();
        *counts.entry(Errors::for_code(self.data.error_code)).or_insert(0) += 1;
        for topic in &self.data.responses {
            for partition in &topic.partitions {
                *counts.entry(Errors::for_code(partition.error_code)).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Returns the per-partition response data keyed by [`TopicIdPartition`] in
    /// the order they appear in the response (Java returns `LinkedHashMap`).
    /// Topic responses whose id cannot be resolved via `topic_names` are
    /// skipped, matching Java's `responseData`.
    ///
    /// Translates `ShareFetchResponse.responseData(Map<Uuid, String>)`.
    pub fn response_data(&self, topic_names: &HashMap<Uuid, String>) -> IndexMap<TopicIdPartition, PartitionData> {
        let mut response_data: IndexMap<TopicIdPartition, PartitionData> = IndexMap::new();
        for topic_response in &self.data.responses {
            if let Some(name) = topic_names.get(&topic_response.topic_id) {
                for partition_data in &topic_response.partitions {
                    response_data.insert(
                        TopicIdPartition::from_parts(
                            topic_response.topic_id,
                            partition_data.partition_index,
                            name.clone(),
                        ),
                        partition_data.clone(),
                    );
                }
            }
        }
        response_data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle upon receiving this response.
    /// Java's `ShareFetchResponse` does NOT override `shouldClientThrottle`; it
    /// inherits the `AbstractResponse` default which returns `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Creates a `ShareFetchResponse` from the given byte buffer.
    ///
    /// Unlike [`Self::of`], this method doesn't convert null records to empty.
    /// This method should only be used on the client side.
    ///
    /// Translates `ShareFetchResponse.parse(Readable, short)`.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareFetchResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Creates a `ShareFetchResponse` from the given data, converting null
    /// records to empty records for consistent representation. This method
    /// should only be used on the server side.
    ///
    /// Translates `ShareFetchResponse.of(Errors, int, LinkedHashMap, List, int)`.
    /// The `LinkedHashMap` is represented as an ordered slice of pairs.
    pub fn of(
        error: Errors,
        throttle_time_ms: i32,
        response_data: Vec<(TopicIdPartition, PartitionData)>,
        node_endpoints: &[Node],
        acquisition_lock_timeout: i32,
    ) -> Self {
        Self::new(to_message(
            error,
            throttle_time_ms,
            response_data,
            node_endpoints,
            acquisition_lock_timeout,
        ))
    }
}

impl std::fmt::Display for ShareFetchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShareFetchResponse(error={:?}, throttleTimeMs={})",
            self.error(),
            self.throttle_time_ms()
        )
    }
}

/// Builds the wire-level [`ShareFetchResponseData`] from the given entries.
///
/// Translates the private `ShareFetchResponse.toMessage(...)`.
fn to_message(
    error: Errors,
    throttle_time_ms: i32,
    response_data: Vec<(TopicIdPartition, PartitionData)>,
    node_endpoints: &[Node],
    acquisition_lock_timeout: i32,
) -> ShareFetchResponseData {
    let mut topic_responses: Vec<ShareFetchableTopicResponse> = Vec::new();
    for (tip, mut partition_data) in response_data {
        // Since PartitionData alone doesn't know the partition ID, we set it here.
        partition_data.set_partition_index(tip.partition());
        // To protect the clients from failing due to null records, we always
        // convert null records to empty records.
        if partition_data.records.is_none() {
            partition_data.set_records(Some(Vec::new()));
        }
        // Check if the topic is already present in the list.
        let topic_idx = match topic_responses.iter().position(|t| t.topic_id == tip.topic_id()) {
            Some(idx) => idx,
            None => {
                let mut topic_response = ShareFetchableTopicResponse::new();
                topic_response.set_topic_id(tip.topic_id());
                topic_response.set_partitions(Vec::new());
                topic_responses.push(topic_response);
                topic_responses.len() - 1
            },
        };
        topic_responses[topic_idx].partitions.push(partition_data);
    }

    let mut data = ShareFetchResponseData::new();
    // KafkaApis should only pass in node endpoints on error, otherwise this
    // should be an empty list.
    for endpoint in node_endpoints {
        let mut node_endpoint = NodeEndpoint::new();
        node_endpoint
            .set_node_id(endpoint.id())
            .set_host(endpoint.host().to_string())
            .set_port(endpoint.port())
            .set_rack(endpoint.rack().map(|s| s.to_string()));
        data.node_endpoints.push(node_endpoint);
    }
    data.set_throttle_time_ms(throttle_time_ms)
        .set_error_code(error.code())
        .set_acquisition_lock_timeout_ms(acquisition_lock_timeout)
        .set_responses(topic_responses);
    data
}

/// Returns the records buffer from a [`PartitionData`], borrowing the
/// underlying bytes — no copy. An absent buffer yields an empty slice.
///
/// Corresponds to Java's `ShareFetchResponse.recordsOrFail(PartitionData)`. The
/// Java `ClassCastException` branch (records not a `Records` instance) has no
/// analogue in Rust — the field is already an owned byte buffer.
pub fn records_or_fail(partition: &PartitionData) -> &[u8] {
    partition.records.as_deref().unwrap_or(&[])
}

/// Returns the size in bytes of the partition's records, or 0 if absent.
///
/// Translates `ShareFetchResponse.recordsSize(PartitionData)`.
pub fn records_size(partition: &PartitionData) -> i32 {
    partition.records.as_ref().map(|r| r.len() as i32).unwrap_or(0)
}

/// Builds an empty partition response carrying the given error code.
///
/// Translates `ShareFetchResponse.partitionResponse(int, Errors)`.
pub fn partition_response(partition: i32, error: Errors) -> PartitionData {
    let mut pd = PartitionData::new();
    pd.set_partition_index(partition)
        .set_error_code(error.code())
        .set_records(Some(Vec::new()));
    pd
}

/// Builds an empty partition response for a [`TopicIdPartition`].
///
/// Translates `ShareFetchResponse.partitionResponse(TopicIdPartition, Errors)`.
pub fn partition_response_for(topic_id_partition: &TopicIdPartition, error: Errors) -> PartitionData {
    partition_response(topic_id_partition.partition(), error)
}

/// Convenience method to find the size of a response.
///
/// Translates `ShareFetchResponse.sizeOf(short, Iterator)`.
///
/// # Errors
///
/// Returns an error if serialization sizing fails.
pub fn size_of(version: i16, part_iterator: Vec<(TopicIdPartition, PartitionData)>) -> io::Result<i32> {
    // Since the throttleTimeMs and metadata field sizes are constant and fixed,
    // we can use arbitrary values here without affecting the result.
    let data = to_message(Errors::None, 0, part_iterator, &[], 0);
    let mut cache = ObjectSerializationCache::new();
    Ok(4 + Message::size(&data, &mut cache, version)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_and_throttle() {
        let mut data = ShareFetchResponseData::new();
        data.set_error_code(Errors::ShareSessionNotFound.code());
        data.set_throttle_time_ms(7);
        let mut resp = ShareFetchResponse::new(data);
        assert_eq!(resp.error(), Errors::ShareSessionNotFound);
        assert_eq!(resp.throttle_time_ms(), 7);
        resp.maybe_set_throttle_time_ms(11);
        assert_eq!(resp.throttle_time_ms(), 11);
        assert!(!resp.should_client_throttle(2));
    }

    #[test]
    fn test_of_groups_by_topic_and_fills_empty_records() {
        let foo = Uuid::new(1, 1);
        let mut p0 = PartitionData::new();
        p0.set_records(None);
        let mut p1 = PartitionData::new();
        p1.set_records(None);
        let resp = ShareFetchResponse::of(
            Errors::None,
            0,
            vec![
                (TopicIdPartition::from_parts(foo, 0, "foo"), p0),
                (TopicIdPartition::from_parts(foo, 1, "foo"), p1),
            ],
            &[],
            0,
        );
        assert_eq!(resp.data().responses.len(), 1);
        assert_eq!(resp.data().responses[0].partitions.len(), 2);
        // Null records converted to empty (non-null) records.
        assert_eq!(resp.data().responses[0].partitions[0].records, Some(Vec::new()));
        // Partition index is set from the topic-id-partition key.
        assert_eq!(resp.data().responses[0].partitions[0].partition_index, 0);
        assert_eq!(resp.data().responses[0].partitions[1].partition_index, 1);
    }

    #[test]
    fn test_error_counts_aggregates() {
        let foo = Uuid::new(1, 1);
        let mut p_err = PartitionData::new();
        p_err.set_error_code(Errors::NotLeaderOrFollower.code());
        let resp = ShareFetchResponse::of(
            Errors::None,
            0,
            vec![(TopicIdPartition::from_parts(foo, 0, "foo"), p_err)],
            &[],
            0,
        );
        let counts = resp.error_counts();
        assert_eq!(*counts.get(&Errors::None).unwrap(), 1);
        assert_eq!(*counts.get(&Errors::NotLeaderOrFollower).unwrap(), 1);
    }

    #[test]
    fn test_response_data_resolves_names_and_skips_unresolved() {
        let foo = Uuid::new(1, 1);
        let bar = Uuid::new(2, 2);
        let resp = ShareFetchResponse::of(
            Errors::None,
            0,
            vec![
                (TopicIdPartition::from_parts(foo, 0, "foo"), PartitionData::new()),
                (TopicIdPartition::from_parts(bar, 0, "bar"), PartitionData::new()),
            ],
            &[],
            0,
        );
        let mut names = HashMap::new();
        names.insert(foo, "foo".to_string());
        let data = resp.response_data(&names);
        assert_eq!(data.len(), 1, "unresolved topic id must be skipped");
        assert!(data.keys().any(|tip| tip.topic() == "foo"));
    }

    #[test]
    fn test_records_helpers() {
        let mut p = PartitionData::new();
        p.set_records(Some(vec![1, 2, 3]));
        assert_eq!(records_or_fail(&p), &[1, 2, 3]);
        assert_eq!(records_size(&p), 3);
        p.set_records(None);
        assert_eq!(records_or_fail(&p).len(), 0);
        assert_eq!(records_size(&p), 0);
    }

    #[test]
    fn test_size_of_is_positive() {
        let foo = Uuid::new(1, 1);
        let size = size_of(2, vec![(TopicIdPartition::from_parts(foo, 0, "foo"), PartitionData::new())]).unwrap();
        assert!(size > 4, "size {size} must exceed the 4-byte length prefix");
    }
}
