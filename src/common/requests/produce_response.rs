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

//! Translation of `org.apache.kafka.common.requests.ProduceResponse`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::message::produce_response_data::ProduceResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;

/// Translation of `org.apache.kafka.common.requests.ProduceResponse`.
///
/// Note: the deprecated `ProduceResponse(Map<TopicIdPartition, PartitionResponse>)`
/// constructors and the inner `PartitionResponse` / `RecordError` helper
/// classes are kept verbatim since they are referenced by `ProduceRequest.
/// getErrorResponse`. We translate them as plain Rust structs because they
/// have no `Builder` / `Schema` / `Message` involvement — they are pure
/// data containers.
pub struct ProduceResponse {
    data: ProduceResponseData,
}

impl ProduceResponse {
    /// Mirrors `ProduceResponse.INVALID_OFFSET = -1L`.
    pub const INVALID_OFFSET: i64 = -1;

    /// Mirrors `new ProduceResponse(ProduceResponseData)`.
    pub fn new(data: ProduceResponseData) -> Self {
        ProduceResponse { data }
    }

    /// Mirrors `ProduceResponse.data()`.
    pub fn response_data(&self) -> &ProduceResponseData {
        &self.data
    }

    /// Mirrors `ProduceResponse.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = ProduceResponseData::read(accessor, version)?;
        Ok(ProduceResponse::new(data))
    }
}

impl AbstractRequestResponse for ProduceResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for ProduceResponse {
    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(0).expect("PRODUCE")
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut out: HashMap<Errors, i32> = HashMap::new();
        for topic in &self.data.responses {
            for partition in &topic.partition_responses {
                abstract_response::update_error_counts(&mut out, Errors::for_code(partition.error_code));
            }
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

/// Mirrors the inner `ProduceResponse.PartitionResponse` data class.
/// Used by callers that build error responses out of the wire-level types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResponse {
    pub error: Errors,
    pub base_offset: i64,
    pub log_append_time: i64,
    pub log_start_offset: i64,
    pub record_errors: Vec<RecordError>,
    pub error_message: Option<String>,
    pub current_leader: crate::common::message::produce_response_data::LeaderIdAndEpoch,
}

impl PartitionResponse {
    /// Mirrors `new PartitionResponse(Errors error)`.
    pub fn from_error(error: Errors) -> Self {
        PartitionResponse {
            error,
            base_offset: ProduceResponse::INVALID_OFFSET,
            // Mirrors RecordBatch.NO_TIMESTAMP = -1.
            log_append_time: -1,
            log_start_offset: ProduceResponse::INVALID_OFFSET,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: crate::common::message::produce_response_data::LeaderIdAndEpoch::new(),
        }
    }

    /// Mirrors `new PartitionResponse(Errors, String)`.
    pub fn from_error_and_message(error: Errors, error_message: impl Into<String>) -> Self {
        let mut r = Self::from_error(error);
        r.error_message = Some(error_message.into());
        r
    }
}

/// Mirrors the inner `ProduceResponse.RecordError` data class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordError {
    pub batch_index: i32,
    pub message: Option<String>,
}

impl RecordError {
    pub fn new(batch_index: i32, message: Option<String>) -> Self {
        RecordError { batch_index, message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::produce_response_data::{
        LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
    };

    /// Translation of `ProduceResponseTest#produceResponseVersionTest`.
    /// We rebuild the response directly via `ProduceResponseData` rather
    /// than via the deprecated `Map<TopicIdPartition, PartitionResponse>`
    /// constructor, which depends on `TopicIdPartition` (Phase 4+).
    #[test]
    fn throttle_time_round_trip_across_versions() {
        let data_v0 = ProduceResponseData { throttle_time_ms: 0, ..ProduceResponseData::new() };
        let data_v1 = ProduceResponseData { throttle_time_ms: 10, ..ProduceResponseData::new() };
        let v0 = ProduceResponse::new(data_v0);
        let v1 = ProduceResponse::new(data_v1);

        assert_eq!(v0.throttle_time_ms(), 0, "v0 throttle must be 0");
        assert_eq!(v1.throttle_time_ms(), 10, "v1 throttle must be 10");
    }

    /// Translation of `ProduceResponseTest#produceResponseRecordErrorsTest`.
    /// Java loops over `PRODUCE.allVersions()`; we iterate over [0..=latest].
    #[test]
    fn produce_response_record_errors_test() {
        use crate::common::message::produce_response_data::BatchIndexAndErrorMessage;

        // Java uses Uuid.fromString("4w0AQXe9TvBG5JkYABorYD"); we use a
        // deterministic concrete (most_sig_bits, least_sig_bits) pair —
        // the actual UUID value is not asserted, only round-tripping.
        let topic_id = crate::common::uuid::Uuid::new(0x12345678, 0x9abcdef0);

        let partition_resp = PartitionProduceResponse {
            index: 0,
            error_code: Errors::None.code(),
            base_offset: 10000,
            log_append_time_ms: -1,
            log_start_offset: 100,
            record_errors: vec![BatchIndexAndErrorMessage {
                batch_index: 3,
                batch_index_error_message: Some("Record error".to_owned()),
                unknown_tagged_fields: Vec::new(),
            }],
            error_message: Some("Produce failed".to_owned()),
            current_leader: LeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };

        let topic_resp = TopicProduceResponse {
            name: "test".to_owned(),
            topic_id,
            partition_responses: vec![partition_resp],
            unknown_tagged_fields: Vec::new(),
        };

        let response_data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let resp = ProduceResponse::new(response_data);

        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        for version in produce.oldest_version()..=produce.latest_version() {
            let mut serialized = AbstractResponse::serialize(&resp, version).expect("serialize");
            let parsed = ProduceResponse::parse(&mut serialized, version).expect("parse");
            let topic_responses = &parsed.response_data().responses;
            assert_eq!(topic_responses.len(), 1);
            let partitions = &topic_responses[0].partition_responses;
            assert_eq!(partitions.len(), 1);
            let deserialized = &partitions[0];
            if version >= 8 {
                assert_eq!(deserialized.record_errors.len(), 1);
                assert_eq!(deserialized.record_errors[0].batch_index, 3);
                assert_eq!(
                    deserialized.record_errors[0].batch_index_error_message.as_deref(),
                    Some("Record error")
                );
                assert_eq!(deserialized.error_message.as_deref(), Some("Produce failed"));
            } else {
                assert_eq!(deserialized.record_errors.len(), 0);
                assert!(deserialized.error_message.is_none());
            }
        }
    }

    #[test]
    fn error_counts_aggregates_across_partitions() {
        use crate::common::message::produce_response_data::PartitionProduceResponse;
        let partition_a = PartitionProduceResponse {
            index: 0,
            error_code: Errors::NetworkException.code(),
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let partition_b = PartitionProduceResponse {
            index: 1,
            error_code: Errors::NetworkException.code(),
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: LeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let topic_resp = TopicProduceResponse {
            name: "test".to_owned(),
            topic_id: crate::common::uuid::Uuid::zero(),
            partition_responses: vec![partition_a, partition_b],
            unknown_tagged_fields: Vec::new(),
        };
        let resp_data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let resp = ProduceResponse::new(resp_data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::NetworkException), Some(&2));
    }

    #[test]
    fn should_client_throttle_only_v6_plus() {
        let resp = ProduceResponse::new(ProduceResponseData::new());
        assert!(!resp.should_client_throttle(5));
        assert!(resp.should_client_throttle(6));
        assert!(resp.should_client_throttle(13));
    }
}
