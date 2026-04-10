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

//! Produce response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ProduceResponse`.
//!
//! This wrapper supports both v0 and v8 of ProduceResponse.
//!
//! Possible error codes:
//! - [`Errors::CorruptMessage`]
//! - [`Errors::UnknownTopicOrPartition`]
//! - [`Errors::NotLeaderOrFollower`]
//! - [`Errors::MessageTooLarge`]
//! - [`Errors::InvalidTopicException`]
//! - [`Errors::RecordListTooLarge`]
//! - [`Errors::NotEnoughReplicas`]
//! - [`Errors::NotEnoughReplicasAfterAppend`]
//! - [`Errors::InvalidRequiredAcks`]
//! - [`Errors::TopicAuthorizationFailed`]
//! - [`Errors::UnsupportedForMessageFormat`]
//! - [`Errors::InvalidProducerEpoch`]
//! - [`Errors::ClusterAuthorizationFailed`]
//! - [`Errors::TransactionalIdAuthorizationFailed`]
//! - [`Errors::InvalidRecord`]
//! - [`Errors::InvalidTxnState`]
//! - [`Errors::InvalidProducerIdMapping`]
//! - [`Errors::ConcurrentTransactions`]
//! - [`Errors::UnknownTopicId`]

use std::collections::HashMap;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::produce_response_data::ProduceResponseData;

use super::abstract_response::update_error_counts;

/// Sentinel value for an invalid offset in produce responses.
///
/// Corresponds to `ProduceResponse.INVALID_OFFSET` in Java.
pub const INVALID_OFFSET: i64 = -1;

/// A Produce response.
///
/// Corresponds to `org.apache.kafka.common.requests.ProduceResponse`.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    data: ProduceResponseData,
}

impl ProduceResponse {
    /// Creates a new `ProduceResponse` from data.
    pub fn new(data: ProduceResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ProduceResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub fn data_mut(&mut self) -> &mut ProduceResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::PRODUCE
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the error counts for this response, aggregated across all partitions.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut error_counts = HashMap::new();
        for topic_response in &self.data.responses {
            for partition_response in &topic_response.partition_responses {
                update_error_counts(&mut error_counts, Errors::for_code(partition_response.error_code));
            }
        }
        error_counts
    }

    /// Returns whether the client should throttle upon receiving this response.
    ///
    /// Client-side throttling is enabled starting from version 6.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 6
    }

    /// Parses a `ProduceResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> std::io::Result<Self> {
        let data = ProduceResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for ProduceResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProduceResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::message::Message;
    use crate::common::protocol::{ByteBufferAccessor, Errors};
    use crate::common::record::NO_TIMESTAMP;
    use crate::common::uuid::Uuid;
    use crate::produce_response_data::{
        BatchIndexAndErrorMessage, LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
    };

    /// Info for a record error used in test helpers.
    #[derive(Debug, Clone)]
    struct RecordErrorInfo {
        batch_index: i32,
        message: Option<String>,
    }

    /// Test helper describing a single partition's produce response.
    struct PartitionResponseEntry<'a> {
        topic: &'a str,
        topic_id: Uuid,
        partition: i32,
        error: Errors,
        base_offset: i64,
        log_append_time: i64,
        log_start_offset: i64,
        record_errors: Vec<RecordErrorInfo>,
        error_message: Option<&'a str>,
    }

    /// Helper: build a ProduceResponseData from partition response entries.
    ///
    /// This replaces the deprecated Java constructors that take `Map<TopicIdPartition, PartitionResponse>`.
    fn build_produce_response_data(
        responses: &[PartitionResponseEntry<'_>],
        throttle_time_ms: i32,
    ) -> ProduceResponseData {
        let mut data = ProduceResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);

        for entry in responses {
            // Find or create the topic response
            let tpr = data
                .responses
                .iter_mut()
                .find(|t| t.name == entry.topic && t.topic_id == entry.topic_id);

            let tpr = if let Some(existing) = tpr {
                existing
            } else {
                let mut new_tpr = TopicProduceResponse::new();
                new_tpr.set_name(entry.topic.to_string());
                new_tpr.set_topic_id(entry.topic_id);
                data.responses.push(new_tpr);
                data.responses.last_mut().unwrap()
            };

            let mut ppr = PartitionProduceResponse::new();
            ppr.set_index(entry.partition);
            ppr.set_error_code(entry.error.code());
            ppr.set_base_offset(entry.base_offset);
            ppr.set_log_append_time_ms(entry.log_append_time);
            ppr.set_log_start_offset(entry.log_start_offset);
            ppr.set_error_message(entry.error_message.map(|s| s.to_string()));

            let batch_errors: Vec<BatchIndexAndErrorMessage> = entry
                .record_errors
                .iter()
                .map(|re| {
                    let mut bie = BatchIndexAndErrorMessage::new();
                    bie.set_batch_index(re.batch_index);
                    bie.set_batch_index_error_message(re.message.clone());
                    bie
                })
                .collect();
            ppr.set_record_errors(batch_errors);
            ppr.set_current_leader(LeaderIdAndEpoch::new());

            tpr.partition_responses.push(ppr);
        }

        data
    }

    /// Translated from `ProduceResponseTest.produceResponseVersionTest`.
    ///
    /// Tests that throttle time and partition response data are correctly preserved
    /// across different response versions.
    #[test]
    fn test_produce_response_version() {
        let topic_id = Uuid::from_string("5JkYABorYD4w0AQXe9TvBG").unwrap();

        let make_entry = |throttle| {
            (
                vec![PartitionResponseEntry {
                    topic: "test",
                    topic_id,
                    partition: 0,
                    error: Errors::None,
                    base_offset: 10000,
                    log_append_time: NO_TIMESTAMP,
                    log_start_offset: 100,
                    record_errors: vec![],
                    error_message: None,
                }],
                throttle,
            )
        };

        let (entries, throttle) = make_entry(0);
        let v0_response = ProduceResponse::new(build_produce_response_data(&entries, throttle));

        let (entries, throttle) = make_entry(10);
        let v1_response = ProduceResponse::new(build_produce_response_data(&entries, throttle));

        let (entries, throttle) = make_entry(10);
        let v2_response = ProduceResponse::new(build_produce_response_data(&entries, throttle));

        assert_eq!(0, v0_response.throttle_time_ms(), "Throttle time must be zero");
        assert_eq!(10, v1_response.throttle_time_ms(), "Throttle time must be 10");
        assert_eq!(10, v2_response.throttle_time_ms(), "Throttle time must be 10");

        let all_responses = [v0_response, v1_response, v2_response];
        for produce_response in &all_responses {
            assert_eq!(1, produce_response.data().responses.len());
            let topic_produce_response = &produce_response.data().responses[0];
            assert_eq!(1, topic_produce_response.partition_responses.len());
            let partition_produce_response = &topic_produce_response.partition_responses[0];
            assert_eq!(100, partition_produce_response.log_start_offset);
            assert_eq!(10000, partition_produce_response.base_offset);
            assert_eq!(NO_TIMESTAMP, partition_produce_response.log_append_time_ms);
            assert_eq!(Errors::None, Errors::for_code(partition_produce_response.error_code));
            assert!(partition_produce_response.error_message.is_none());
            assert!(partition_produce_response.record_errors.is_empty());
            assert_eq!(topic_id, topic_produce_response.topic_id);
        }
    }

    /// Translated from `ProduceResponseTest.produceResponseRecordErrorsTest`.
    ///
    /// Tests that record-level errors and error messages are correctly serialized and
    /// deserialized across all supported versions. Fields `recordErrors` and `errorMessage`
    /// are only present in version >= 8.
    #[test]
    fn test_produce_response_record_errors() {
        let topic_id = Uuid::from_string("4w0AQXe9TvBG5JkYABorYD").unwrap();

        let record_errors = vec![RecordErrorInfo { batch_index: 3, message: Some("Record error".to_string()) }];

        let entries = vec![PartitionResponseEntry {
            topic: "test",
            topic_id,
            partition: 0,
            error: Errors::None,
            base_offset: 10000,
            log_append_time: NO_TIMESTAMP,
            log_start_offset: 100,
            record_errors,
            error_message: Some("Produce failed"),
        }];
        let data = build_produce_response_data(&entries, 0);
        let response = ProduceResponse::new(data);

        for version in ProduceResponseData::LOWEST_SUPPORTED_VERSION..=ProduceResponseData::HIGHEST_SUPPORTED_VERSION {
            // Serialize
            let mut cache = crate::common::protocol::object_serialization_cache::ObjectSerializationCache::new();
            let size = Message::size(response.data(), &mut cache, version).unwrap();
            let mut buf = ByteBufferAccessor::new(size as usize);
            Message::write(response.data(), &mut buf, &cache, version).unwrap();
            buf.flip();

            // Deserialize
            let produce_response = ProduceResponse::parse(&mut buf, version).unwrap();
            let topic_produce_response = &produce_response.data().responses[0];
            let deserialized = &topic_produce_response.partition_responses[0];

            if version >= 8 {
                assert_eq!(1, deserialized.record_errors.len());
                assert_eq!(3, deserialized.record_errors[0].batch_index);
                assert_eq!(
                    Some("Record error".to_string()),
                    deserialized.record_errors[0].batch_index_error_message
                );
                assert_eq!(Some("Produce failed".to_string()), deserialized.error_message);
            } else {
                assert_eq!(0, deserialized.record_errors.len());
                assert!(deserialized.error_message.is_none());
            }
        }
    }
}
