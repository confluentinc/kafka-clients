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

//! ElectLeaders request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ElectLeadersRequest`.

use std::collections::HashSet;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::{ElectionType, KafkaError, TopicPartition};
use crate::elect_leaders_request_data::{ElectLeadersRequestData, TopicPartitions};
use crate::elect_leaders_response_data::{PartitionResult, ReplicaElectionResult};

use super::{ConcreteRequest, ConcreteResponse, ElectLeadersResponse, RequestBuilder};

/// An ElectLeaders request.
///
/// Corresponds to `org.apache.kafka.common.requests.ElectLeadersRequest`.
#[derive(Debug, Clone)]
pub struct ElectLeadersRequest {
    data: ElectLeadersRequestData,
    version: i16,
}

impl ElectLeadersRequest {
    /// Creates a new `ElectLeadersRequest` from data and version.
    pub fn new(data: ElectLeadersRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ElectLeadersRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ElectLeadersRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ELECT_LEADERS
    }

    /// Returns the set of topic partitions in this request.
    ///
    /// Mirrors `ElectLeadersRequest.topicPartitions()`.
    pub fn topic_partitions(&self) -> HashSet<TopicPartition> {
        match &self.data.topic_partitions {
            None => HashSet::new(),
            Some(topic_partitions) => topic_partitions
                .iter()
                .flat_map(|tp| tp.partitions.iter().map(|p| TopicPartition::new(tp.topic.clone(), *p)))
                .collect(),
        }
    }

    /// Creates an error response for this request, failing every requested
    /// partition with the given error.
    ///
    /// Mirrors `ElectLeadersRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut election_results = Vec::new();
        if let Some(topic_partitions) = &self.data.topic_partitions {
            for topic in topic_partitions {
                let mut election_result = ReplicaElectionResult::new();
                election_result.set_topic(topic.topic.clone());
                for partition_id in &topic.partitions {
                    let mut partition_result = PartitionResult::new();
                    partition_result.set_partition_id(*partition_id);
                    partition_result.set_error_code(error.code());
                    partition_result.set_error_message(None);
                    election_result.partition_result.push(partition_result);
                }
                election_results.push(election_result);
            }
        }
        ConcreteResponse::ElectLeaders(ElectLeadersResponse::from_results(
            throttle_time_ms,
            error.code(),
            election_results,
            self.version,
        ))
    }

    /// Parses an `ElectLeadersRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ElectLeadersRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ElectLeadersRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ElectLeadersRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`ElectLeadersRequest`].
///
/// Corresponds to `ElectLeadersRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ElectLeadersRequestBuilder {
    election_type: ElectionType,
    topic_partitions: Option<Vec<TopicPartition>>,
    timeout_ms: i32,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ElectLeadersRequestBuilder {
    /// Creates a builder for the given election type, partitions and timeout.
    ///
    /// A `None` `topic_partitions` requests election for all partitions.
    ///
    /// Mirrors `ElectLeadersRequest.Builder(ElectionType, Collection, int)`.
    pub fn new(election_type: ElectionType, topic_partitions: Option<Vec<TopicPartition>>, timeout_ms: i32) -> Self {
        Self {
            election_type,
            topic_partitions,
            timeout_ms,
            oldest_allowed_version: ApiKeys::ELECT_LEADERS.oldest_version(),
            latest_allowed_version: ApiKeys::ELECT_LEADERS.latest_version(),
        }
    }

    /// Builds the request data for a given version.
    ///
    /// Mirrors `Builder.toRequestData`.
    ///
    /// # Errors
    ///
    /// Returns an error if a non-`PREFERRED` election type is requested at
    /// version 0, mirroring Java's `UnsupportedVersionException`.
    fn to_request_data(&self, version: i16) -> Result<ElectLeadersRequestData, KafkaError> {
        if self.election_type != ElectionType::Preferred && version == 0 {
            return Err(KafkaError::unsupported_version(
                "API Version 0 only supports PREFERRED election type",
            ));
        }

        let mut data = ElectLeadersRequestData::new();
        data.set_timeout_ms(self.timeout_ms);

        match &self.topic_partitions {
            Some(topic_partitions) => {
                let mut topics: Vec<TopicPartitions> = Vec::new();
                for tp in topic_partitions {
                    match topics.iter_mut().find(|t| t.topic == tp.topic()) {
                        Some(existing) => existing.partitions.push(tp.partition()),
                        None => {
                            let mut tps = TopicPartitions::new();
                            tps.set_topic(tp.topic().to_string());
                            tps.partitions.push(tp.partition());
                            topics.push(tps);
                        },
                    }
                }
                data.set_topic_partitions(Some(topics));
            },
            None => {
                data.set_topic_partitions(None);
            },
        }

        data.set_election_type(self.election_type.value());
        Ok(data)
    }
}

impl RequestBuilder for ElectLeadersRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ELECT_LEADERS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        let data = self
            .to_request_data(version)
            .map_err(|e| io::Error::new(io::ErrorKind::Unsupported, e.message().to_string()))?;
        Ok(ConcreteRequest::ElectLeaders(ElectLeadersRequest::new(data, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_partitions_flattens_by_topic() {
        let mut data = ElectLeadersRequestData::new();
        let mut tps = TopicPartitions::new();
        tps.set_topic("t".to_string());
        tps.set_partitions(vec![0, 1]);
        data.set_topic_partitions(Some(vec![tps]));
        let request = ElectLeadersRequest::new(data, 2);
        let partitions = request.topic_partitions();
        assert_eq!(partitions.len(), 2);
        assert!(partitions.contains(&TopicPartition::new("t", 0)));
        assert!(partitions.contains(&TopicPartition::new("t", 1)));
    }

    #[test]
    fn topic_partitions_null_is_empty() {
        let mut data = ElectLeadersRequestData::new();
        data.set_topic_partitions(None);
        let request = ElectLeadersRequest::new(data, 2);
        assert!(request.topic_partitions().is_empty());
    }

    #[test]
    fn builder_v0_rejects_non_preferred() {
        let mut builder =
            ElectLeadersRequestBuilder::new(ElectionType::Unclean, Some(vec![TopicPartition::new("t", 0)]), 100);
        let err = builder.build_version(0).unwrap_err();
        assert!(err.to_string().contains("API Version 0 only supports PREFERRED election type"));
    }

    #[test]
    fn get_error_response_fails_every_partition() {
        let mut data = ElectLeadersRequestData::new();
        let mut tps = TopicPartitions::new();
        tps.set_topic("t".to_string());
        tps.set_partitions(vec![0, 1]);
        data.set_topic_partitions(Some(vec![tps]));
        let request = ElectLeadersRequest::new(data, 2);
        let response = request.get_error_response(100, &Errors::ClusterAuthorizationFailed);
        if let ConcreteResponse::ElectLeaders(r) = response {
            assert_eq!(r.data().throttle_time_ms, 100);
            assert_eq!(r.data().error_code, Errors::ClusterAuthorizationFailed.code());
            assert_eq!(r.data().replica_election_results.len(), 1);
            assert_eq!(r.data().replica_election_results[0].partition_result.len(), 2);
        } else {
            panic!("expected ElectLeaders response");
        }
    }

    /// Round-trips a request through the shared `ConcreteRequest` serialize /
    /// parse path, exercising the enum wiring end-to-end.
    #[test]
    fn serialize_parse_round_trip() {
        let mut builder =
            ElectLeadersRequestBuilder::new(ElectionType::Preferred, Some(vec![TopicPartition::new("t", 3)]), 30000);
        let mut request = builder.build_version(2).unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = ElectLeadersRequest::parse(&mut readable, 2).unwrap();
        assert_eq!(parsed.data().election_type, 0);
        assert_eq!(parsed.data().timeout_ms, 30000);
        let topic_partitions = parsed.data().topic_partitions.as_ref().unwrap();
        assert_eq!(topic_partitions.len(), 1);
        assert_eq!(topic_partitions[0].topic, "t");
        assert_eq!(topic_partitions[0].partitions, vec![3]);
    }

    /// Byte-level encoding test against a known vector. ElectLeaders v2 is a
    /// flexible version, so the body is:
    ///   election_type: int8 = 0 (00)
    ///   topic_partitions: compact array (len+1 = 0x02)
    ///     topic: compact string "t" (0x02, 0x74)
    ///     partitions: compact int32 array (len+1 = 0x02), partition 0 (00 00 00 00)
    ///     _tagged_fields: 0x00
    ///   timeout_ms: int32 = 100 (00 00 00 64)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v2() {
        let mut builder =
            ElectLeadersRequestBuilder::new(ElectionType::Preferred, Some(vec![TopicPartition::new("t", 0)]), 100);
        let mut request = builder.build_version(2).unwrap();
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, // election_type = 0 (PREFERRED)
            0x02, // topic_partitions array length + 1
            0x02, 0x74, // topic "t"
            0x02, // partitions array length + 1
            0x00, 0x00, 0x00, 0x00, // partition 0
            0x00, // topic_partitions element tagged fields
            0x00, 0x00, 0x00, 0x64, // timeout_ms = 100
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
