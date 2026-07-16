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

//! Produce request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ProduceRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::BatchIterator;
use crate::common::record::CompressionType;
use crate::common::record::RecordBatch;
use crate::produce_request_data::ProduceRequestData;
use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::ProduceResponse;
use super::RequestBuilder;

/// Sentinel value: last stable version before Transaction V2 protocol.
///
/// When using transaction V1 protocol, the request version upper limit is set to
/// this value so that the broker knows the client is using transaction protocol V1.
pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 11;

/// Invalid offset sentinel for produce responses.
pub const INVALID_OFFSET: i64 = -1;

/// A Produce request.
///
/// Corresponds to `org.apache.kafka.common.requests.ProduceRequest`.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    data: ProduceRequestData,
    version: i16,
    /// Cached acks value (copied from data since data may be cleared).
    acks: i16,
    /// Cached timeout value (copied from data since data may be cleared).
    timeout: i32,
    /// Cached transactional ID (copied from data since data may be cleared).
    transactional_id: Option<String>,
}

impl ProduceRequest {
    /// Creates a new `ProduceRequest` from data and version.
    pub fn new(data: ProduceRequestData, version: i16) -> Self {
        let acks = data.acks;
        let timeout = data.timeout_ms;
        let transactional_id = data.transactional_id.clone();
        Self { data, version, acks, timeout, transactional_id }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ProduceRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ProduceRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::PRODUCE
    }

    /// The number of acknowledgments the producer requires.
    pub fn acks(&self) -> i16 {
        self.acks
    }

    /// The timeout to await a response in milliseconds.
    pub fn timeout(&self) -> i32 {
        self.timeout
    }

    /// The transactional ID, or `None` if not transactional.
    pub fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    /// Whether the Transaction V2 protocol is being requested.
    pub fn is_transaction_v2_requested(version: i16) -> bool {
        version > LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    }

    /// Creates an error response for this request.
    ///
    /// Returns `None` when acks is 0 because the producer does not expect any
    /// response in that case. In Java, `getErrorResponse()` returns `null` for
    /// acks=0.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> Option<ConcreteResponse> {
        // In case the producer doesn't actually want any response
        if self.acks == 0 {
            return None;
        }

        let mut response_data = ProduceResponseData::new();
        response_data.set_throttle_time_ms(throttle_time_ms);

        for topic_data in &self.data.topic_data {
            let mut tpr = TopicProduceResponse::new();
            tpr.set_name(topic_data.name.clone());
            tpr.set_topic_id(topic_data.topic_id);

            let mut partition_responses = Vec::new();
            for partition_data in &topic_data.partition_data {
                let mut ppr = PartitionProduceResponse::new();
                ppr.set_index(partition_data.index);
                ppr.set_base_offset(INVALID_OFFSET);
                ppr.set_log_append_time_ms(RecordBatch::NO_TIMESTAMP);
                ppr.set_log_start_offset(INVALID_OFFSET);
                ppr.set_error_code(error.code());
                ppr.set_error_message(Some(error.message().to_string()));
                partition_responses.push(ppr);
            }
            tpr.set_partition_responses(partition_responses);
            response_data.responses.push(tpr);
        }

        Some(ConcreteResponse::Produce(ProduceResponse::new(response_data)))
    }

    /// Validates the record batches for a partition before building a produce request.
    ///
    /// Checks that:
    /// 1. At least one record batch exists per partition
    /// 2. Record batch magic is V2
    /// 3. ZStandard compression is not used before version 7
    /// 4. Exactly one record batch per partition
    ///
    /// Corresponds to Java's `ProduceRequest.validateRecords`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn validate_records(version: i16, records_bytes: &Option<bytes::Bytes>) -> io::Result<()> {
        let bytes: &[u8] = match records_bytes {
            Some(b) if !b.is_empty() => b,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Produce requests with version {} must have at least one record batch per partition",
                        version
                    ),
                ));
            },
        };

        let mut batches = BatchIterator::new(bytes);

        let first_batch = match batches.next() {
            Some(batch) => batch,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Produce requests with version {} must have at least one record batch per partition",
                        version
                    ),
                ));
            },
        };

        if first_batch.magic() != RecordBatch::MAGIC_VALUE_V2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are only allowed to contain record batches with magic version 2",
                    version
                ),
            ));
        }

        if version < 7 && first_batch.compression_type() == CompressionType::Zstd {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are not allowed to use ZStandard compression",
                    version
                ),
            ));
        }

        if batches.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are only allowed to contain exactly one record batch per partition",
                    version
                ),
            ));
        }

        Ok(())
    }

    /// Parses a `ProduceRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ProduceRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ProduceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProduceRequest(version={}, acks={}, timeout={}, transactionalId={:?})",
            self.version, self.acks, self.timeout, self.transactional_id
        )
    }
}

/// Builder for [`ProduceRequest`].
///
/// Corresponds to `ProduceRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ProduceRequestBuilder {
    data: ProduceRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ProduceRequestBuilder {
    /// Creates a builder with default version range.
    pub fn new(data: ProduceRequestData) -> Self {
        Self::builder(data, false)
    }

    /// Creates a builder, optionally limiting the version to Transaction V1.
    ///
    /// When `use_transaction_v1_version` is true, the maximum version is capped at
    /// [`LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`] so that the broker knows the
    /// client is using transaction protocol V1.
    pub fn builder(data: ProduceRequestData, use_transaction_v1_version: bool) -> Self {
        let max_version = if use_transaction_v1_version {
            LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        } else {
            ApiKeys::PRODUCE.latest_version()
        };
        Self {
            data,
            oldest_allowed_version: ApiKeys::PRODUCE.oldest_version(),
            latest_allowed_version: max_version,
        }
    }

    /// Creates a builder with explicit version range.
    pub fn from_data(min_version: i16, max_version: i16, data: ProduceRequestData) -> Self {
        Self { data, oldest_allowed_version: min_version, latest_allowed_version: max_version }
    }
}

impl RequestBuilder for ProduceRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::PRODUCE
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Validate the given records first, matching Java's Builder.build(short version)
        for topic_data in &self.data.topic_data {
            for partition_data in &topic_data.partition_data {
                ProduceRequest::validate_records(version, &partition_data.records)?;
            }
        }
        Ok(ConcreteRequest::Produce(ProduceRequest::new(
            std::mem::replace(&mut self.data, ProduceRequestData::new()),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_request_basic() {
        let mut data = ProduceRequestData::new();
        data.set_acks(-1);
        data.set_timeout_ms(30000);
        let request = ProduceRequest::new(data, 9);
        assert_eq!(request.acks(), -1);
        assert_eq!(request.timeout(), 30000);
        assert_eq!(request.version(), 9);
        assert_eq!(*request.api_key(), ApiKeys::PRODUCE);
        assert!(request.transactional_id().is_none());
    }

    #[test]
    fn test_produce_request_with_transactional_id() {
        let mut data = ProduceRequestData::new();
        data.set_transactional_id(Some("my-txn".to_string()));
        data.set_acks(-1);
        data.set_timeout_ms(1000);
        let request = ProduceRequest::new(data, 9);
        assert_eq!(request.transactional_id(), Some("my-txn"));
    }

    #[test]
    fn test_builder_default_version_range() {
        let data = ProduceRequestData::new();
        let builder = ProduceRequestBuilder::new(data);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::PRODUCE.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::PRODUCE.latest_version());
    }

    #[test]
    fn test_builder_transaction_v1_version() {
        let data = ProduceRequestData::new();
        let builder = ProduceRequestBuilder::builder(data, true);
        assert_eq!(builder.latest_allowed_version(), LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2);
    }

    #[test]
    fn test_is_transaction_v2_requested() {
        assert!(!ProduceRequest::is_transaction_v2_requested(11));
        assert!(ProduceRequest::is_transaction_v2_requested(12));
        assert!(ProduceRequest::is_transaction_v2_requested(13));
    }

    #[test]
    fn test_get_error_response_acks_zero() {
        let mut data = ProduceRequestData::new();
        data.set_acks(0);
        let request = ProduceRequest::new(data, 9);
        let response = request.get_error_response(100, &Errors::UnknownTopicOrPartition);
        // With acks=0, the response should be None (Java returns null)
        assert!(response.is_none());
    }

    #[test]
    fn test_get_error_response_acks_all() {
        use crate::produce_request_data::{PartitionProduceData, TopicProduceData};

        let mut partition = PartitionProduceData::new();
        partition.set_index(0);

        let mut topic = TopicProduceData::new();
        topic.set_name("test-topic".to_string());
        topic.set_partition_data(vec![partition]);

        let mut data = ProduceRequestData::new();
        data.set_acks(-1);
        data.set_topic_data(vec![topic]);

        let request = ProduceRequest::new(data, 9);
        let response = request.get_error_response(0, &Errors::UnknownTopicOrPartition);
        let Some(ConcreteResponse::Produce(r)) = &response else {
            panic!("Expected Some(Produce response)");
        };
        assert_eq!(r.data().responses.len(), 1);
        assert_eq!(r.data().responses[0].name, "test-topic");
        assert_eq!(r.data().responses[0].partition_responses.len(), 1);
        assert_eq!(
            r.data().responses[0].partition_responses[0].error_code,
            Errors::UnknownTopicOrPartition.code()
        );
        // Verify error message is also set (Issue 3)
        assert_eq!(
            r.data().responses[0].partition_responses[0].error_message,
            Some(Errors::UnknownTopicOrPartition.message().to_string())
        );
    }
}
