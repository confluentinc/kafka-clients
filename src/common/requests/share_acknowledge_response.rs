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

use std::collections::HashMap;
use std::io;

use crate::common::Node;
use crate::common::TopicIdPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_acknowledge_response_data::{
    NodeEndpoint, PartitionData, ShareAcknowledgeResponseData, ShareAcknowledgeTopicResponse,
};

/// A `ShareAcknowledge` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeResponse`.
#[derive(Debug, Clone)]
pub struct ShareAcknowledgeResponse {
    data: ShareAcknowledgeResponseData,
}

impl ShareAcknowledgeResponse {
    /// Creates a new `ShareAcknowledgeResponse` from the underlying data.
    pub fn new(data: ShareAcknowledgeResponseData) -> Self {
        Self { data }
    }

    /// Returns the top-level error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareAcknowledgeResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareAcknowledgeResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    /// Aggregates the response error counts: top-level plus per-partition.
    ///
    /// Translates `ShareAcknowledgeResponse.errorCounts()`.
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

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle upon receiving this response.
    /// Java's `ShareAcknowledgeResponse` does NOT override
    /// `shouldClientThrottle`; it inherits the `AbstractResponse` default which
    /// returns `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Parses a `ShareAcknowledgeResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareAcknowledgeResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Creates a `ShareAcknowledgeResponse` from the given data.
    ///
    /// Translates `ShareAcknowledgeResponse.of(Errors, int, LinkedHashMap, List, int)`.
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

impl std::fmt::Display for ShareAcknowledgeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShareAcknowledgeResponse(error={:?}, throttleTimeMs={})",
            self.error(),
            self.throttle_time_ms()
        )
    }
}

/// Builds the wire-level [`ShareAcknowledgeResponseData`] from the given entries.
///
/// Translates the public `ShareAcknowledgeResponse.toMessage(...)`.
pub fn to_message(
    error: Errors,
    throttle_time_ms: i32,
    response_data: Vec<(TopicIdPartition, PartitionData)>,
    node_endpoints: &[Node],
    acquisition_lock_timeout: i32,
) -> ShareAcknowledgeResponseData {
    let mut topic_responses: Vec<ShareAcknowledgeTopicResponse> = Vec::new();
    for (tip, mut partition_data) in response_data {
        // Since PartitionData alone doesn't know the partition ID, we set it here.
        partition_data.set_partition_index(tip.partition());
        let topic_idx = match topic_responses.iter().position(|t| t.topic_id == tip.topic_id()) {
            Some(idx) => idx,
            None => {
                let mut topic_response = ShareAcknowledgeTopicResponse::new();
                topic_response.set_topic_id(tip.topic_id());
                topic_response.set_partitions(Vec::new());
                topic_responses.push(topic_response);
                topic_responses.len() - 1
            },
        };
        topic_responses[topic_idx].partitions.push(partition_data);
    }

    let mut data = ShareAcknowledgeResponseData::new();
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

/// Builds an empty partition response carrying the given error code.
///
/// Translates `ShareAcknowledgeResponse.partitionResponse(int, Errors)`.
pub fn partition_response(partition: i32, error: Errors) -> PartitionData {
    let mut pd = PartitionData::new();
    pd.set_partition_index(partition).set_error_code(error.code());
    pd
}

/// Builds an empty partition response for a [`TopicIdPartition`].
///
/// Translates `ShareAcknowledgeResponse.partitionResponse(TopicIdPartition, Errors)`.
pub fn partition_response_for(topic_id_partition: &TopicIdPartition, error: Errors) -> PartitionData {
    partition_response(topic_id_partition.partition(), error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;

    #[test]
    fn test_error_and_throttle() {
        let mut data = ShareAcknowledgeResponseData::new();
        data.set_error_code(Errors::InvalidShareSessionEpoch.code());
        data.set_throttle_time_ms(3);
        let mut resp = ShareAcknowledgeResponse::new(data);
        assert_eq!(resp.error(), Errors::InvalidShareSessionEpoch);
        assert_eq!(resp.throttle_time_ms(), 3);
        resp.maybe_set_throttle_time_ms(9);
        assert_eq!(resp.throttle_time_ms(), 9);
        assert!(!resp.should_client_throttle(2));
    }

    #[test]
    fn test_of_groups_by_topic_and_sets_partition_index() {
        let foo = Uuid::new(1, 1);
        let resp = ShareAcknowledgeResponse::of(
            Errors::None,
            0,
            vec![
                (TopicIdPartition::from_parts(foo, 0, "foo"), PartitionData::new()),
                (TopicIdPartition::from_parts(foo, 2, "foo"), PartitionData::new()),
            ],
            &[],
            0,
        );
        assert_eq!(resp.data().responses.len(), 1);
        assert_eq!(resp.data().responses[0].partitions.len(), 2);
        assert_eq!(resp.data().responses[0].partitions[0].partition_index, 0);
        assert_eq!(resp.data().responses[0].partitions[1].partition_index, 2);
    }

    #[test]
    fn test_error_counts_aggregates() {
        let foo = Uuid::new(1, 1);
        let mut p_err = PartitionData::new();
        p_err.set_error_code(Errors::InvalidRecordState.code());
        let resp = ShareAcknowledgeResponse::of(
            Errors::None,
            0,
            vec![(TopicIdPartition::from_parts(foo, 0, "foo"), p_err)],
            &[],
            0,
        );
        let counts = resp.error_counts();
        assert_eq!(*counts.get(&Errors::None).unwrap(), 1);
        assert_eq!(*counts.get(&Errors::InvalidRecordState).unwrap(), 1);
    }

    #[test]
    fn test_partition_response_factory() {
        let foo = Uuid::new(1, 1);
        let pd = partition_response_for(&TopicIdPartition::from_parts(foo, 5, "foo"), Errors::NotLeaderOrFollower);
        assert_eq!(pd.partition_index, 5);
        assert_eq!(pd.error_code, Errors::NotLeaderOrFollower.code());
    }
}
