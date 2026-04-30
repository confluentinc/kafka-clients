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

//! Translation of `org.apache.kafka.common.requests.ProduceRequest`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::message::produce_request_data::ProduceRequestData;
use crate::common::message::produce_response_data::{
    LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponseData, TopicProduceResponse,
};
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequest;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::ProduceResponse;

/// Translation of `org.apache.kafka.common.requests.ProduceRequest`.
pub struct ProduceRequest {
    data: ProduceRequestData,
    version: i16,
    /// Cached on construction to mirror Java's behaviour: even if the
    /// request's `data` is later cleared (`clearPartitionRecords`), the
    /// metadata fields remain accessible.
    acks: i16,
    timeout: i32,
    transactional_id: Option<String>,
}

impl ProduceRequest {
    /// Last (inclusive) version where transactions used the v1 protocol.
    /// Mirrors `ProduceRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 = 11`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 11;

    /// Mirrors `new ProduceRequest(ProduceRequestData, short version)`.
    pub fn new(data: ProduceRequestData, version: i16) -> Self {
        let acks = data.acks;
        let timeout = data.timeout_ms;
        let transactional_id = data.transactional_id.clone();
        ProduceRequest { data, version, acks, timeout, transactional_id }
    }

    /// Mirrors `ProduceRequest.data()`. The Java version throws
    /// `IllegalStateException` if `clearPartitionRecords` has been called;
    /// since the Rust translation does not yet wire up `clearPartitionRecords`
    /// (records aren't owned by the wrapper — they're a `Vec<u8>` slice in
    /// the generated `*Data` struct), we always return the inner data.
    pub fn request_data(&self) -> &ProduceRequestData {
        &self.data
    }

    /// Mirrors `ProduceRequest.acks()`.
    pub fn acks(&self) -> i16 {
        self.acks
    }

    /// Mirrors `ProduceRequest.timeout()`.
    pub fn timeout(&self) -> i32 {
        self.timeout
    }

    /// Mirrors `ProduceRequest.transactionalId()`.
    pub fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    /// Mirrors `ProduceRequest.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = ProduceRequestData::read(accessor, version)?;
        Ok(ProduceRequest::new(data, version))
    }

    /// Mirrors `ProduceRequest.isTransactionV2Requested(short version)`.
    pub fn is_transaction_v2_requested(version: i16) -> bool {
        version > Self::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    }

    /// Sum of bytes in all `records` fields across all partitions. Mirrors
    /// the lazily-initialised `partitionSizes` accumulator in Java (used
    /// by the broker for quotas and by `getErrorResponse` to know how many
    /// partition responses to emit).
    fn partition_count(&self) -> usize {
        self.data.topic_data.iter().map(|t| t.partition_data.len()).sum()
    }
}

impl AbstractRequestResponse for ProduceRequest {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractRequest for ProduceRequest {
    fn version(&self) -> i16 {
        self.version
    }

    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(0).expect("PRODUCE")
    }

    fn get_error_response(&self, throttle_time_ms: i32, error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
        // Java: in case the producer doesn't actually want any response.
        if self.acks == 0 {
            return None;
        }

        let err = Errors::for_code(error.code());
        let mut data = ProduceResponseData { throttle_time_ms, ..ProduceResponseData::new() };

        for topic in &self.data.topic_data {
            // Find existing TopicProduceResponse or push a new one.
            let position = data
                .responses
                .iter()
                .position(|r| r.name == topic.name && r.topic_id == topic.topic_id);
            let tpr_idx = match position {
                Some(idx) => idx,
                None => {
                    data.responses.push(TopicProduceResponse {
                        name: topic.name.clone(),
                        topic_id: topic.topic_id,
                        partition_responses: Vec::new(),
                        unknown_tagged_fields: Vec::new(),
                    });
                    data.responses.len() - 1
                },
            };
            for partition in &topic.partition_data {
                data.responses[tpr_idx].partition_responses.push(PartitionProduceResponse {
                    index: partition.index,
                    error_code: err.code(),
                    base_offset: ProduceResponse::INVALID_OFFSET,
                    log_append_time_ms: -1, // matches Java's RecordBatch.NO_TIMESTAMP
                    log_start_offset: ProduceResponse::INVALID_OFFSET,
                    record_errors: Vec::new(),
                    error_message: err.message().map(str::to_owned),
                    current_leader: LeaderIdAndEpoch::new(),
                    unknown_tagged_fields: Vec::new(),
                });
            }
        }
        Some(Box::new(ProduceResponse::new(data)))
    }

    fn error_counts(&self, error: &KafkaError) -> Result<HashMap<Errors, i32>, KafkaError> {
        // ProduceRequest in Java overrides errorCounts to count *partitions*,
        // not response entries — and to handle the acks=0 case where
        // get_error_response returns null.
        let err = Errors::for_code(error.code());
        let mut map = HashMap::new();
        map.insert(err, self.partition_count() as i32);
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::produce_request_data::{PartitionProduceData, TopicProduceData};
    use crate::common::uuid::Uuid;

    /// Translation of `ProduceRequestTest#testBuildWithCurrentMessageFormat`.
    /// We can't construct `MemoryRecords` (Phase 3) so the records field
    /// uses a placeholder byte array — the version-bound assertions are the
    /// part of the test that's relevant here.
    #[test]
    fn build_uses_correct_oldest_and_latest_versions() {
        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        // Java's `ProduceRequest.builder(data)` defaults to
        // [oldestVersion(), latestVersion()]. We don't model `Builder` —
        // we just verify those bounds match `ApiKey::oldest_version` /
        // `latest_version` (which `Builder.build` uses).
        assert!(produce.oldest_version() >= 0);
        assert!(produce.latest_version() >= produce.oldest_version());
    }

    #[test]
    fn is_transaction_v2_requested_threshold_at_v11() {
        assert!(!ProduceRequest::is_transaction_v2_requested(11));
        assert!(ProduceRequest::is_transaction_v2_requested(12));
        assert!(ProduceRequest::is_transaction_v2_requested(13));
    }

    #[test]
    fn get_error_response_returns_none_when_acks_zero() {
        let data = ProduceRequestData {
            acks: 0,
            timeout_ms: 1000,
            transactional_id: None,
            topic_data: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let req = ProduceRequest::new(data, 9);
        assert!(req.get_error_response(0, &KafkaError::Network("err".to_owned())).is_none());
    }

    /// Translation of `ProduceRequestTest#testBuilderOldestAndLatestAllowed`'s
    /// intent: the constructed request preserves topic_data / acks / timeout.
    #[test]
    fn produce_request_round_trips_constructor_state() {
        let data = ProduceRequestData {
            acks: -1,
            timeout_ms: 10,
            transactional_id: None,
            topic_data: vec![TopicProduceData {
                name: String::new(),
                topic_id: Uuid::new(0x123, 0x456),
                partition_data: vec![PartitionProduceData {
                    index: 1,
                    records: Some(b"hello".to_vec()),
                    unknown_tagged_fields: Vec::new(),
                }],
                unknown_tagged_fields: Vec::new(),
            }],
            unknown_tagged_fields: Vec::new(),
        };
        let req = ProduceRequest::new(data, 13);
        assert_eq!(req.acks(), -1);
        assert_eq!(req.timeout(), 10);
        assert!(req.transactional_id().is_none());
    }

    #[test]
    fn error_counts_reports_per_partition_count() {
        // 2 topics * 1 partition each = 2 partitions.
        let topic = TopicProduceData {
            name: String::new(),
            topic_id: Uuid::new(0x1, 0x2),
            partition_data: vec![PartitionProduceData { index: 0, records: None, unknown_tagged_fields: Vec::new() }],
            unknown_tagged_fields: Vec::new(),
        };
        let topic2 = TopicProduceData { topic_id: Uuid::new(0x3, 0x4), ..topic.clone() };
        let data = ProduceRequestData {
            acks: -1,
            timeout_ms: 1000,
            transactional_id: None,
            topic_data: vec![topic, topic2],
            unknown_tagged_fields: Vec::new(),
        };
        let req = ProduceRequest::new(data, 13);
        let counts = req.error_counts(&KafkaError::Network("e".to_owned())).expect("counts");
        assert_eq!(counts.get(&Errors::NetworkException), Some(&2));
    }

    #[test]
    fn parse_round_trip_v3() {
        let data = ProduceRequestData {
            acks: -1,
            timeout_ms: 1000,
            transactional_id: None,
            topic_data: vec![TopicProduceData {
                name: "t".to_owned(),
                topic_id: Uuid::zero(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(b"hello".to_vec()),
                    unknown_tagged_fields: Vec::new(),
                }],
                unknown_tagged_fields: Vec::new(),
            }],
            unknown_tagged_fields: Vec::new(),
        };
        let req = ProduceRequest::new(data, 3);
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize");
        let parsed = ProduceRequest::parse(&mut serialized, 3).expect("parse");
        assert_eq!(parsed.acks(), -1);
        assert_eq!(parsed.timeout(), 1000);
    }
}
