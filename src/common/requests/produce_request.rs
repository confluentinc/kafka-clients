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

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::compression_type::CompressionType;
use crate::common::record::{MAGIC_VALUE_V2, MemoryRecords};
use crate::common::uuid::Uuid;
use crate::produce_request_data::ProduceRequestData;
use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};

use super::ConcreteRequest;
use super::abstract_request::RequestBuilder;
use super::abstract_response::ConcreteResponse;
use super::produce_response::{INVALID_OFFSET, ProduceResponse};

/// The last stable version before Transaction V2 protocol.
///
/// When using transaction V1 protocol in a transaction, the request version upper
/// limit is set to this value so that the broker knows the producer is using
/// transaction protocol V1.
pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 11;

/// A Produce request.
///
/// Corresponds to `org.apache.kafka.common.requests.ProduceRequest`.
///
/// The `acks`, `timeout`, and `transactional_id` fields are copied from `data` at
/// construction time. In Java they are cached so that `clearPartitionRecords()` can
/// null out the data field while preserving those metadata values. We do not
/// implement `clearPartitionRecords` yet (server-side optimization), but we keep
/// the same field structure for compatibility.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    data: ProduceRequestData,
    version: i16,
    acks: i16,
    timeout: i32,
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

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::PRODUCE
    }

    /// Returns the number of acknowledgments the producer requires.
    pub fn acks(&self) -> i16 {
        self.acks
    }

    /// Returns the timeout to await a response in milliseconds.
    pub fn timeout(&self) -> i32 {
        self.timeout
    }

    /// Returns the transactional ID, or `None` if the producer is not transactional.
    pub fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    /// Computes the partition sizes lazily.
    ///
    /// Returns a map from `(topic_name, topic_id, partition)` to the total size
    /// of the records in bytes for that partition.
    fn partition_sizes(&self) -> HashMap<(String, Uuid, i32), i32> {
        let mut sizes = HashMap::new();
        for topic_data in &self.data.topic_data {
            for partition_data in &topic_data.partition_data {
                let size_in_bytes = partition_data.records.as_ref().map(|r| r.len() as i32).unwrap_or(0);
                let key = (topic_data.name.clone(), topic_data.topic_id, partition_data.index);
                *sizes.entry(key).or_insert(0) += size_in_bytes;
            }
        }
        sizes
    }

    /// Creates an error response for this request.
    ///
    /// Returns `None` if `acks == 0` (fire-and-forget mode).
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> Option<ConcreteResponse> {
        // In case the producer doesn't actually want any response
        if self.acks == 0 {
            return None;
        }

        let mut response_data = ProduceResponseData::new();
        response_data.set_throttle_time_ms(throttle_time_ms);

        for (topic_name, topic_id, partition) in self.partition_sizes().keys() {
            // Find or create the topic response
            let tpr_idx = response_data
                .responses
                .iter()
                .position(|t| &t.name == topic_name && t.topic_id == *topic_id);

            let tpr_idx = match tpr_idx {
                Some(idx) => idx,
                None => {
                    let mut tpr = TopicProduceResponse::new();
                    tpr.set_name(topic_name.clone());
                    tpr.set_topic_id(*topic_id);
                    response_data.responses.push(tpr);
                    response_data.responses.len() - 1
                },
            };

            let mut ppr = PartitionProduceResponse::new();
            ppr.set_index(*partition);
            ppr.set_record_errors(Vec::new());
            ppr.set_base_offset(INVALID_OFFSET);
            ppr.set_log_append_time_ms(crate::common::record::NO_TIMESTAMP);
            ppr.set_log_start_offset(INVALID_OFFSET);
            ppr.set_error_message(None);
            ppr.set_error_code(error.code());

            response_data.responses[tpr_idx].partition_responses.push(ppr);
        }

        Some(ConcreteResponse::Produce(ProduceResponse::new(response_data)))
    }

    /// Validates the records in a partition's produce data.
    ///
    /// Ensures:
    /// - There is exactly one record batch per partition
    /// - The record batch uses magic version 2
    /// - ZStd compression is not used before version 7
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn validate_records(version: i16, records: &[u8]) -> io::Result<()> {
        let mem_records = MemoryRecords::from_buffer(records.to_vec());
        let batches = mem_records.batches();

        if batches.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} must have at least one record batch per partition",
                    version
                ),
            ));
        }

        let entry = &batches[0];
        if entry.magic() != MAGIC_VALUE_V2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are only allowed to contain record batches with magic version 2",
                    version
                ),
            ));
        }

        if version < 7 && entry.compression_type() == CompressionType::Zstd {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Produce requests with version {} are not allowed to use ZStandard compression",
                    version
                ),
            ));
        }

        if batches.len() > 1 {
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

    /// Returns whether transaction V2 is requested for the given version.
    pub fn is_transaction_v2_requested(version: i16) -> bool {
        version > LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    }
}

impl std::fmt::Display for ProduceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProduceRequest(acks={}, timeout={}, version={}, numPartitions={})",
            self.acks,
            self.timeout,
            self.version,
            self.partition_sizes().len()
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
    /// Creates a builder with the given data, not using transaction V1 version limiting.
    pub fn new(data: ProduceRequestData) -> Self {
        Self::new_with_transaction_flag(data, false)
    }

    /// Creates a builder with the given data and transaction V1 flag.
    ///
    /// When `use_transaction_v1_version` is true, the maximum version is limited to
    /// [`LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`] so the broker knows the producer
    /// is using transaction protocol V1.
    pub fn new_with_transaction_flag(data: ProduceRequestData, use_transaction_v1_version: bool) -> Self {
        let max_version = if use_transaction_v1_version {
            LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        } else {
            ApiKeys::PRODUCE.latest_version()
        };
        Self::from_data(ApiKeys::PRODUCE.oldest_version(), max_version, data)
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

    fn build_version(&self, version: i16) -> io::Result<ConcreteRequest> {
        // Validate the given records first
        for topic_data in &self.data.topic_data {
            for partition_data in &topic_data.partition_data {
                if let Some(ref records) = partition_data.records {
                    ProduceRequest::validate_records(version, records)?;
                }
            }
        }
        Ok(ConcreteRequest::Produce(ProduceRequest::new(self.data.clone(), version)))
    }
}

impl std::fmt::Display for ProduceRequestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(type=ProduceRequest, acks={}, timeout={}, transactionalId='{}')",
            self.data.acks,
            self.data.timeout_ms,
            self.data.transactional_id.as_deref().unwrap_or("")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::compression_type::CompressionType;
    use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
    use crate::common::record::timestamp_type::TimestampType;
    use crate::common::record::{
        CURRENT_MAGIC_VALUE, MAGIC_VALUE_V0, MAGIC_VALUE_V1, MemoryRecords, NO_PARTITION_LEADER_EPOCH,
        NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, NO_TIMESTAMP, SimpleRecord,
    };
    use crate::produce_request_data::{PartitionProduceData, TopicProduceData};

    /// Helper to get current time in millis.
    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Create a simple record with key and value for tests.
    fn simple_record() -> SimpleRecord {
        SimpleRecord::with_timestamp(now_ms(), Some(b"key".to_vec()), Some(b"value".to_vec()))
    }

    /// Helper to build a ProduceRequestData with a single partition's records.
    fn build_produce_data_single(
        topic_name: Option<&str>,
        topic_id: Uuid,
        partition: i32,
        records: MemoryRecords,
        acks: i16,
        timeout_ms: i32,
    ) -> ProduceRequestData {
        let mut partition_data = PartitionProduceData::new();
        partition_data.set_index(partition);
        partition_data.set_records(Some(records.into_buffer()));

        let mut topic_data = TopicProduceData::new();
        if let Some(name) = topic_name {
            topic_data.set_name(name.to_string());
        }
        topic_data.set_topic_id(topic_id);
        topic_data.set_partition_data(vec![partition_data]);

        let mut data = ProduceRequestData::new();
        data.set_topic_data(vec![topic_data]);
        data.set_acks(acks);
        data.set_timeout_ms(timeout_ms);
        data
    }

    /// Helper: assert that building at every allowed version throws the expected error.
    fn assert_throws_for_all_versions(builder: &ProduceRequestBuilder) {
        for version in builder.oldest_allowed_version()..=builder.latest_allowed_version() {
            let result = builder.build_version(version);
            assert!(result.is_err(), "Expected error at version {}, but build succeeded", version);
        }
    }

    fn create_non_idempotent_non_transactional_records() -> ProduceRequest {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let records = MemoryRecords::with_records(0, &[simple_record()]);
        let data = build_produce_data_single(None, topic_id, 1, records, -1, 10);
        ProduceRequestBuilder::new(data).build().unwrap().into_produce().unwrap()
    }

    /// Translated from `ProduceRequestTest.shouldBeFlaggedAsTransactionalWhenTransactionalRecords`.
    ///
    /// Tests that a produce request containing transactional records is flagged as transactional.
    #[test]
    fn test_should_be_flagged_as_transactional_when_transactional_records() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            1,     // producer_id
            1,     // producer_epoch
            1,     // base_sequence
            true,  // is_transactional
            false, // is_control_batch
            NO_PARTITION_LEADER_EPOCH,
            &[simple_record()],
        );

        let data = build_produce_data_single(None, topic_id, 1, records, -1, 10);
        let request = ProduceRequestBuilder::new(data).build().unwrap();
        let produce = request.as_produce().unwrap();
        assert!(has_transactional_records(produce));
    }

    /// Translated from `ProduceRequestTest.shouldNotBeFlaggedAsTransactionalWhenNoRecords`.
    #[test]
    fn test_should_not_be_flagged_as_transactional_when_no_records() {
        let request = create_non_idempotent_non_transactional_records();
        assert!(!has_transactional_records(&request));
    }

    /// Translated from `ProduceRequestTest.shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent`.
    #[test]
    fn test_should_not_be_flagged_as_idempotent_when_records_not_idempotent() {
        let request = create_non_idempotent_non_transactional_records();
        assert!(!has_idempotent_records(&request));
    }

    /// Translated from `ProduceRequestTest.shouldBeFlaggedAsIdempotentWhenIdempotentRecords`.
    #[test]
    fn test_should_be_flagged_as_idempotent_when_idempotent_records() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            1,
            NO_TIMESTAMP,
            1,     // producer_id
            1,     // producer_epoch
            1,     // base_sequence
            false, // is_transactional
            false, // is_control_batch
            NO_PARTITION_LEADER_EPOCH,
            &[simple_record()],
        );

        let data = build_produce_data_single(None, topic_id, 1, records, -1, 10);
        let request = ProduceRequestBuilder::new(data).build().unwrap();
        let produce = request.as_produce().unwrap();
        assert!(has_idempotent_records(produce));
    }

    /// Translated from `ProduceRequestTest.testBuildWithCurrentMessageFormat`.
    #[test]
    fn test_build_with_current_message_format() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let mut builder = MemoryRecordsBuilder::new(
            256,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            256,
        )
        .unwrap();
        builder.append(10, None, Some(b"a"), &[]).unwrap();
        let records = builder.build().unwrap();

        let data = build_produce_data_single(None, topic_id, 9, records, 1, 5000);
        let request_builder = ProduceRequestBuilder::new_with_transaction_flag(data, false);
        assert_eq!(ApiKeys::PRODUCE.oldest_version(), request_builder.oldest_allowed_version());
        assert_eq!(ApiKeys::PRODUCE.latest_version(), request_builder.latest_allowed_version());
    }

    /// Translated from `ProduceRequestTest.testBuildWithCurrentMessageFormatWithoutTopicId`.
    #[test]
    fn test_build_with_current_message_format_without_topic_id() {
        let mut builder = MemoryRecordsBuilder::new(
            256,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            256,
        )
        .unwrap();
        builder.append(10, None, Some(b"a"), &[]).unwrap();
        let records = builder.build().unwrap();

        // TopicId will default to Uuid::zero() and client will get UNKNOWN_TOPIC_ID error.
        let data = build_produce_data_single(Some("topic"), Uuid::zero(), 9, records, 1, 5000);
        let request_builder = ProduceRequestBuilder::new_with_transaction_flag(data, false);
        assert_eq!(ApiKeys::PRODUCE.oldest_version(), request_builder.oldest_allowed_version());
        assert_eq!(ApiKeys::PRODUCE.latest_version(), request_builder.latest_allowed_version());
    }

    /// Translated from `ProduceRequestTest.testV3AndAboveShouldContainOnlyOneRecordBatch`.
    #[test]
    fn test_v3_and_above_should_contain_only_one_record_batch() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();

        // Build two batches into one buffer
        let mut builder1 = MemoryRecordsBuilder::new(
            256,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            256,
        )
        .unwrap();
        builder1.append(10, None, Some(b"a"), &[]).unwrap();
        let records1 = builder1.build().unwrap();

        let mut builder2 = MemoryRecordsBuilder::new(
            256,
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            1,
            NO_TIMESTAMP,
            NO_PRODUCER_ID,
            NO_PRODUCER_EPOCH,
            NO_SEQUENCE,
            false,
            false,
            NO_PARTITION_LEADER_EPOCH,
            256,
        )
        .unwrap();
        builder2.append(11, Some(b"1"), Some(b"b"), &[]).unwrap();
        builder2.append(12, None, Some(b"c"), &[]).unwrap();
        let records2 = builder2.build().unwrap();

        // Concatenate the two batch buffers
        let mut combined = records1.into_buffer();
        combined.extend_from_slice(records2.buffer());
        let combined_records = MemoryRecords::from_buffer(combined);

        let data = build_produce_data_single(None, topic_id, 0, combined_records, 1, 5000);
        let request_builder = ProduceRequestBuilder::new(data);
        assert_throws_for_all_versions(&request_builder);
    }

    /// Translated from `ProduceRequestTest.testV3AndAboveCannotHaveNoRecordBatches`.
    #[test]
    fn test_v3_and_above_cannot_have_no_record_batches() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let empty_records = MemoryRecords::from_buffer(Vec::new());

        let data = build_produce_data_single(None, topic_id, 0, empty_records, 1, 5000);
        let request_builder = ProduceRequestBuilder::new(data);
        assert_throws_for_all_versions(&request_builder);
    }

    /// Translated from `ProduceRequestTest.testV3AndAboveCannotUseMagicV0`.
    ///
    /// Skipped: Our MemoryRecordsBuilder only supports magic V2 (the current version).
    /// The Java test verifies that magic V0 batches are rejected, but since our builder
    /// cannot produce magic V0 batches, we cannot directly translate this test.
    /// The validation logic in `validate_records` does correctly reject non-V2 magic values,
    /// which we verify in `test_validate_records_rejects_wrong_magic`.
    #[test]
    fn test_v3_and_above_cannot_use_magic_v0() {
        // Our MemoryRecordsBuilder only supports magic V2 so we cannot construct a V0 batch.
        // Instead, we verify the validation logic directly.
        // A V0 batch would have magic byte 0 at offset MAGIC_OFFSET in the batch header.
        // We create a minimal valid V2 batch and corrupt the magic byte.
        let records = MemoryRecords::with_records(0, &[SimpleRecord::with_timestamp(10, None, Some(b"a".to_vec()))]);
        let mut buf = records.into_buffer();
        // Magic byte is at offset MAGIC_OFFSET (17) from the start of the batch
        buf[crate::common::record::MAGIC_OFFSET] = MAGIC_VALUE_V0 as u8;
        let result = ProduceRequest::validate_records(3, &buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic version 2"));
    }

    /// Translated from `ProduceRequestTest.testV3AndAboveCannotUseMagicV1`.
    ///
    /// Same approach as above: corrupt the magic byte to V1 and verify rejection.
    #[test]
    fn test_v3_and_above_cannot_use_magic_v1() {
        let records = MemoryRecords::with_records(0, &[SimpleRecord::with_timestamp(10, None, Some(b"a".to_vec()))]);
        let mut buf = records.into_buffer();
        buf[crate::common::record::MAGIC_OFFSET] = MAGIC_VALUE_V1 as u8;
        let result = ProduceRequest::validate_records(3, &buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic version 2"));
    }

    /// Translated from `ProduceRequestTest.testV6AndBelowCannotUseZStdCompression`.
    ///
    /// Skipped: Our MemoryRecordsBuilder does not support Zstd compression yet.
    /// We verify the validation logic directly by checking that `validate_records`
    /// correctly rejects ZStd-compressed batches for versions < 7.
    /// When we have Zstd compression support, this test should be updated to use
    /// MemoryRecordsBuilder with Zstd compression.
    #[test]
    fn test_v6_and_below_cannot_use_zstd_compression() {
        // Create a valid v2 batch and manually set the compression type to Zstd
        // in the batch attributes.
        let records = MemoryRecords::with_records(0, &[SimpleRecord::with_timestamp(10, None, Some(b"a".to_vec()))]);
        let mut buf = records.into_buffer();

        // The attributes field is at offset LOG_OVERHEAD + 4 + 1 + 4 = LOG_OVERHEAD + 9
        // (CRC offset + CRC length + magic + padding? No, let's calculate properly)
        // From DefaultRecordBatch:
        //   ATTRIBUTES_OFFSET = LOG_OVERHEAD + 4 (CRC) + 1 (magic) + 4 (more fields? No)
        // Actually from the Java source:
        //   ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH + CRC_LENGTH = 17 + 1 + 4 = 22
        // But our code has:
        //   MAGIC_OFFSET = LOG_OVERHEAD + 4 = 16
        //   LOG_OVERHEAD = 12
        // Let's use the DefaultRecordBatch constants
        use crate::common::record::default_record_batch;
        let attrs_offset = default_record_batch::ATTRIBUTES_OFFSET;
        // Read the current attributes (2 bytes, big-endian)
        let current_attrs = i16::from_be_bytes([buf[attrs_offset], buf[attrs_offset + 1]]);
        // Clear compression bits (bits 0-2) and set to Zstd (4)
        let new_attrs = (current_attrs & !0x07) | (CompressionType::Zstd as i16);
        let new_attrs_bytes = new_attrs.to_be_bytes();
        buf[attrs_offset] = new_attrs_bytes[0];
        buf[attrs_offset + 1] = new_attrs_bytes[1];

        // For versions < 7, should fail
        for version in ApiKeys::PRODUCE.oldest_version()..7 {
            let result = ProduceRequest::validate_records(version, &buf);
            assert!(result.is_err(), "Expected error for version {} with ZStd compression", version);
        }

        // For versions >= 7, the compression validation passes (magic is still wrong
        // due to CRC mismatch but validate_records only checks magic and compression type,
        // not CRC).
        let result = ProduceRequest::validate_records(7, &buf);
        assert!(result.is_ok(), "Version 7 should allow ZStd compression");
    }

    /// Translated from `ProduceRequestTest.testMixedTransactionalData`.
    #[test]
    fn test_mixed_transactional_data() {
        let producer_id: i64 = 15;
        let producer_epoch: i16 = 5;
        let sequence: i32 = 10;

        let non_txn_records = MemoryRecords::with_records(0, &[SimpleRecord::with_value(Some(b"foo".to_vec()))]);
        let txn_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            producer_id,
            producer_epoch,
            sequence,
            true, // transactional
            false,
            NO_PARTITION_LEADER_EPOCH,
            &[SimpleRecord::with_value(Some(b"bar".to_vec()))],
        );

        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let mut txn_partition = PartitionProduceData::new();
        txn_partition.set_index(0);
        txn_partition.set_records(Some(txn_records.into_buffer()));

        let mut non_txn_partition = PartitionProduceData::new();
        non_txn_partition.set_index(1);
        non_txn_partition.set_records(Some(non_txn_records.into_buffer()));

        let mut topic_data1 = TopicProduceData::new();
        topic_data1.set_topic_id(topic_id);
        topic_data1.set_partition_data(vec![txn_partition]);

        let mut topic_data2 = TopicProduceData::new();
        topic_data2.set_topic_id(topic_id);
        topic_data2.set_partition_data(vec![non_txn_partition]);

        let mut data = ProduceRequestData::new();
        data.set_topic_data(vec![topic_data1, topic_data2]);
        data.set_acks(-1);
        data.set_timeout_ms(5000);

        let request = ProduceRequestBuilder::new_with_transaction_flag(data, true).build().unwrap();
        let produce = request.as_produce().unwrap();
        assert!(has_transactional_records(produce));
        assert!(has_idempotent_records(produce));
    }

    /// Translated from `ProduceRequestTest.testMixedIdempotentData`.
    #[test]
    fn test_mixed_idempotent_data() {
        let producer_id: i64 = 15;
        let producer_epoch: i16 = 5;
        let sequence: i32 = 10;

        let non_idempotent_records = MemoryRecords::with_records(0, &[SimpleRecord::with_value(Some(b"foo".to_vec()))]);
        let idempotent_records = MemoryRecords::build_with_records(
            CURRENT_MAGIC_VALUE,
            CompressionType::None,
            TimestampType::CreateTime,
            0,
            NO_TIMESTAMP,
            producer_id,
            producer_epoch,
            sequence,
            false, // not transactional
            false,
            NO_PARTITION_LEADER_EPOCH,
            &[SimpleRecord::with_value(Some(b"bar".to_vec()))],
        );

        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let mut idempotent_partition = PartitionProduceData::new();
        idempotent_partition.set_index(0);
        idempotent_partition.set_records(Some(idempotent_records.into_buffer()));

        let mut non_idempotent_partition = PartitionProduceData::new();
        non_idempotent_partition.set_index(1);
        non_idempotent_partition.set_records(Some(non_idempotent_records.into_buffer()));

        let mut topic_data1 = TopicProduceData::new();
        topic_data1.set_topic_id(topic_id);
        topic_data1.set_partition_data(vec![idempotent_partition]);

        let mut topic_data2 = TopicProduceData::new();
        topic_data2.set_topic_id(topic_id);
        topic_data2.set_partition_data(vec![non_idempotent_partition]);

        let mut data = ProduceRequestData::new();
        data.set_topic_data(vec![topic_data1, topic_data2]);
        data.set_acks(-1);
        data.set_timeout_ms(5000);

        let request = ProduceRequestBuilder::new_with_transaction_flag(data, true).build().unwrap();
        let produce = request.as_produce().unwrap();
        assert!(!has_transactional_records(produce));
        assert!(has_idempotent_records(produce));
    }

    /// Translated from `ProduceRequestTest.testBuilderOldestAndLatestAllowed`.
    #[test]
    fn test_builder_oldest_and_latest_allowed() {
        let topic_id = Uuid::from_string("H3Emm3vW7AKKO4NTRPaCWt").unwrap();
        let records = MemoryRecords::with_records(0, &[simple_record()]);
        let data = build_produce_data_single(None, topic_id, 1, records, -1, 10);
        let builder = ProduceRequestBuilder::new(data);
        assert_eq!(ApiKeys::PRODUCE.oldest_version(), builder.oldest_allowed_version());
        assert_eq!(ApiKeys::PRODUCE.latest_version(), builder.latest_allowed_version());
    }

    /// Helper: check if any record batch in the request is transactional.
    ///
    /// Corresponds to `RequestUtils.hasTransactionalRecords` in Java.
    fn has_transactional_records(request: &ProduceRequest) -> bool {
        for topic_data in &request.data().topic_data {
            for partition_data in &topic_data.partition_data {
                if let Some(ref records_bytes) = partition_data.records {
                    let mem_records = MemoryRecords::from_buffer(records_bytes.clone());
                    for batch in mem_records.batches() {
                        if batch.is_transactional() {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// Helper: check if any record batch in the request has a valid producer ID (idempotent).
    ///
    /// Corresponds to `RequestTestUtils.hasIdempotentRecords` in Java.
    fn has_idempotent_records(request: &ProduceRequest) -> bool {
        for topic_data in &request.data().topic_data {
            for partition_data in &topic_data.partition_data {
                if let Some(ref records_bytes) = partition_data.records {
                    let mem_records = MemoryRecords::from_buffer(records_bytes.clone());
                    for batch in mem_records.batches() {
                        if batch.producer_id() >= 0 {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}
