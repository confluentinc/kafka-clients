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

//! Translation of `org.apache.kafka.common.requests.MetadataResponse`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::message::metadata_response_data::MetadataResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;
use crate::common::uuid::{Uuid, ZERO_UUID};

/// Translation of `org.apache.kafka.common.requests.MetadataResponse`.
///
/// Note: this is the producer-relevant subset of the Java class. The
/// `Holder` cache (broker map / topic metadata view) and `buildCluster`
/// are deferred to Phase 4 (`common::Cluster` / `common::Node` /
/// `common::PartitionInfo` are not yet translated).
pub struct MetadataResponse {
    data: MetadataResponseData,
    has_reliable_leader_epochs: bool,
}

impl MetadataResponse {
    /// Mirrors `MetadataResponse.NO_CONTROLLER_ID = -1`.
    pub const NO_CONTROLLER_ID: i32 = -1;
    /// Mirrors `MetadataResponse.NO_LEADER_ID = -1`.
    pub const NO_LEADER_ID: i32 = -1;
    /// Mirrors `MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED = Integer.MIN_VALUE`.
    pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

    /// Mirrors `new MetadataResponse(MetadataResponseData, boolean)`.
    pub fn new(data: MetadataResponseData, has_reliable_leader_epochs: bool) -> Self {
        MetadataResponse { data, has_reliable_leader_epochs }
    }

    /// Mirrors `new MetadataResponse(MetadataResponseData, short version)`.
    pub fn new_for_version(data: MetadataResponseData, version: i16) -> Self {
        MetadataResponse::new(data, Self::has_reliable_leader_epochs_for(version))
    }

    /// Mirrors the package-private `hasReliableLeaderEpochs(short)`.
    pub fn has_reliable_leader_epochs_for(version: i16) -> bool {
        version >= 9
    }

    /// Mirrors `MetadataResponse.data()`.
    pub fn response_data(&self) -> &MetadataResponseData {
        &self.data
    }

    /// Mirrors `MetadataResponse.hasReliableLeaderEpochs()`.
    pub fn has_reliable_leader_epochs(&self) -> bool {
        self.has_reliable_leader_epochs
    }

    /// Mirrors `MetadataResponse.errors()`. Returns a map of
    /// topic-name → error for every topic with a non-NONE error.
    pub fn errors(&self) -> Result<HashMap<String, Errors>, KafkaError> {
        let mut out = HashMap::new();
        for metadata in &self.data.topics {
            if metadata.name.is_none() {
                return Err(KafkaError::IllegalArgument(
                    "Use errorsByTopicId() when managing topic using topic id".to_owned(),
                ));
            }
            let err = Errors::for_code(metadata.error_code);
            if err != Errors::None {
                out.insert(metadata.name.clone().unwrap_or_default(), err);
            }
        }
        Ok(out)
    }

    /// Mirrors `MetadataResponse.errorsByTopicId()`.
    pub fn errors_by_topic_id(&self) -> Result<HashMap<Uuid, Errors>, KafkaError> {
        let mut out = HashMap::new();
        for metadata in &self.data.topics {
            if metadata.topic_id == ZERO_UUID {
                return Err(KafkaError::IllegalArgument(
                    "Use errors() when managing topic using topic name".to_owned(),
                ));
            }
            let err = Errors::for_code(metadata.error_code);
            if err != Errors::None {
                out.insert(metadata.topic_id, err);
            }
        }
        Ok(out)
    }

    /// Mirrors `MetadataResponse.topLevelError()`.
    pub fn top_level_error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Mirrors `MetadataResponse.clusterId()`.
    pub fn cluster_id(&self) -> Option<&str> {
        self.data.cluster_id.as_deref()
    }

    /// Mirrors `MetadataResponse.clusterAuthorizedOperations()`.
    pub fn cluster_authorized_operations(&self) -> i32 {
        self.data.cluster_authorized_operations
    }

    /// Mirrors `MetadataResponse.topicAuthorizedOperations(String)`.
    pub fn topic_authorized_operations(&self, topic_name: &str) -> Option<i32> {
        self.data
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic_name))
            .map(|t| t.topic_authorized_operations)
    }

    /// Mirrors `MetadataResponse.topicsByError(Errors)`.
    pub fn topics_by_error(&self, error: Errors) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for metadata in &self.data.topics {
            if metadata.error_code == error.code()
                && let Some(ref name) = metadata.name
            {
                out.insert(name.clone());
            }
        }
        out
    }

    /// Mirrors `MetadataResponse.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = MetadataResponseData::read(accessor, version)?;
        Ok(MetadataResponse::new(data, Self::has_reliable_leader_epochs_for(version)))
    }
}

impl AbstractRequestResponse for MetadataResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for MetadataResponse {
    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(3).expect("METADATA")
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut out: HashMap<Errors, i32> = HashMap::new();
        for metadata in &self.data.topics {
            for partition in &metadata.partitions {
                abstract_response::update_error_counts(&mut out, Errors::for_code(partition.error_code));
            }
            abstract_response::update_error_counts(&mut out, Errors::for_code(metadata.error_code));
        }
        out
    }

    fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.throttle_time_ms = throttle_time_ms;
    }

    fn should_client_throttle(&self, version: i16) -> bool {
        version >= 6
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::metadata_response_data::MetadataResponseTopic;

    #[test]
    fn parse_round_trip_v12() {
        let topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("topic1".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp_data = MetadataResponseData {
            throttle_time_ms: 5,
            brokers: Vec::new(),
            cluster_id: Some("cluster".to_owned()),
            controller_id: 1,
            topics: vec![topic],
            cluster_authorized_operations: 0,
            error_code: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new_for_version(resp_data, 12);
        let mut serialized = AbstractResponse::serialize(&resp, 12).expect("serialize");
        let parsed = MetadataResponse::parse(&mut serialized, 12).expect("parse");
        assert_eq!(parsed.cluster_id(), Some("cluster"));
        assert_eq!(parsed.response_data().topics.len(), 1);
        assert!(parsed.has_reliable_leader_epochs());
    }

    #[test]
    fn errors_returns_only_non_none() {
        let topic_ok = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("ok".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let topic_err = MetadataResponseTopic {
            error_code: Errors::UnknownTopicOrPartition.code(),
            name: Some("missing".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![topic_ok, topic_err], ..MetadataResponseData::new() },
            true,
        );
        let map = resp.errors().expect("errors");
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("missing"), Some(&Errors::UnknownTopicOrPartition));
    }

    #[test]
    fn errors_by_topic_id_when_named_only_returns_error() {
        let zero_id_topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("ok".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![zero_id_topic], ..MetadataResponseData::new() },
            true,
        );
        let result = resp.errors_by_topic_id();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Use errors()"), "expected guidance message, got {msg}");
    }

    #[test]
    fn topic_authorized_operations_returns_value_or_none() {
        let topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("t".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0xabc,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![topic], ..MetadataResponseData::new() },
            true,
        );
        assert_eq!(resp.topic_authorized_operations("t"), Some(0xabc));
        assert_eq!(resp.topic_authorized_operations("missing"), None);
    }
}
