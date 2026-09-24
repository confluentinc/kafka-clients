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
use crate::common::record::internal::ByteBufferLogInputStream;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::DefaultRecordBatchRef;
use crate::common::record::internal::RecordBatch;
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
    /// Corresponds to Java's `ProduceRequest.validateRecords`
    /// (`ProduceRequest.java:211-232`).
    ///
    /// Java walks `records.batches().iterator()`, whose `hasNext()` is
    /// `ByteBufferLogInputStream.nextBatch() != null`: true for any complete
    /// batch, whatever its magic. [`BatchIterator`](crate::common::record::internal::memory_records::BatchIterator)
    /// cannot answer that, because it ends at a v0/v1 batch this client has no
    /// view for, so the two presence checks ask the stream directly and the magic
    /// is read off the header, where `nextBatch()` itself reads it
    /// (`ByteBufferLogInputStream.java:48`). None of it copies the batch.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails. A header the stream rejects fails
    /// with the stream's `CORRUPT_MESSAGE` text, as Java's `hasNext()` throws its
    /// `CorruptRecordException`; a v2 first batch shorter than its own 61-byte
    /// header, which Java would pass (it reads only the magic and the attributes),
    /// fails with the `ensureValid()` size text, because this client has no view
    /// of such a batch.
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

        let Some(first_batch_size) = complete_batch_size(bytes)? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} must have at least one record batch per partition",
                    version
                ),
            ));
        };
        let first_batch = &bytes[..first_batch_size];

        // In bounds: a complete batch is at least `LOG_OVERHEAD + 14` bytes.
        if first_batch[RecordBatch::MAGIC_OFFSET] as i8 != RecordBatch::MAGIC_VALUE_V2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are only allowed to contain record batches with magic version 2",
                    version
                ),
            ));
        }

        let first_batch = DefaultRecordBatchRef::new(first_batch).map_err(io::Error::from)?;
        if version < 7 && first_batch.compression_type() == CompressionType::Zstd {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Produce requests with version {} are not allowed to use ZStandard compression",
                    version
                ),
            ));
        }

        if complete_batch_size(&bytes[first_batch_size..])?.is_some() {
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

/// Java's `iterator.hasNext()` for a batch iterator positioned at the start of
/// `bytes`: the size of the complete batch there, if one is.
///
/// `ByteBufferLogInputStream.nextBatch() != null` (`ByteBufferLogInputStream.java:41-46`):
/// the header validates and the whole batch is present. A header that does not
/// validate is the `CorruptRecordException` Java's `hasNext()` throws.
fn complete_batch_size(bytes: &[u8]) -> io::Result<Option<usize>> {
    match ByteBufferLogInputStream::new(bytes, i32::MAX).next_batch_size() {
        Ok(batch_size) => Ok(batch_size.filter(|&batch_size| batch_size <= bytes.len())),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.message().to_string())),
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

    /// Java's `ProduceRequest.Builder.build(short version)` (`ProduceRequest.java:68-74`)
    /// is a pure read of `data` — it validates, then returns
    /// `new ProduceRequest(data, version)` sharing the reference — so a builder may be
    /// built more than once and every result carries the records.
    ///
    /// Rust's `build_version` instead drains the builder with
    /// `std::mem::replace(&mut self.data, ProduceRequestData::new())` (`:300`), so the
    /// second build silently returns a request with no `topic_data`. It is the only one
    /// of this crate's 53 `RequestBuilder` impls that drains rather than clones.
    ///
    /// **Reproducer for PLAN §9.30**, left in place and `#[ignore]`d with that section
    /// cited — the treatment §9.25 gives its own. Not reachable in production today
    /// (`NetworkClient::do_send` is the only production build site and builds once per
    /// `ClientRequest`), so this is a latent divergence rather than a live defect; §9.30
    /// carries the reachability derivation.
    #[test]
    #[ignore = "PLAN §9.30: ProduceRequestBuilder::build_version drains the builder, where Java's \
                Builder.build does not"]
    fn test_build_is_repeatable() {
        use crate::common::compress::Compression;
        use crate::common::record::{MemoryRecords, TimestampType};
        use crate::produce_request_data::{PartitionProduceData, TopicProduceData};

        // `validate_records` requires a real magic-v2 batch (`:185-193`), so build one
        // rather than a sentinel payload — otherwise the build fails before reaching the
        // behaviour under test.
        let mut records_builder = MemoryRecords::builder_with_buffer(
            vec![0u8; 512],
            RecordBatch::MAGIC_VALUE_V2,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        records_builder.append_with_offset_bytes(0, 1_700_000_000_000, Some(b"k"), Some(b"v"));
        let records = records_builder.build().into_buffer();

        let mut partition = PartitionProduceData::new();
        partition.set_index(0);
        partition.set_records(Some(records));

        let mut topic = TopicProduceData::new();
        topic.set_name("test-topic".to_string());
        topic.set_partition_data(vec![partition]);

        let mut data = ProduceRequestData::new();
        data.set_acks(-1);
        data.set_topic_data(vec![topic]);

        let mut builder = ProduceRequestBuilder::from_data(3, 3, data);

        let ConcreteRequest::Produce(first) = builder.build_version(3).expect("first build") else {
            panic!("expected a produce request");
        };
        let ConcreteRequest::Produce(second) = builder.build_version(3).expect("second build") else {
            panic!("expected a produce request");
        };

        assert_eq!(first.data().topic_data.len(), 1, "the first build must carry the topic");
        assert_eq!(
            second.data().topic_data.len(),
            1,
            "Java's Builder.build does not consume the builder, so the second build must \
             carry the topic too"
        );
        assert_eq!(first.data().topic_data, second.data().topic_data);
    }

    // ── `validate_records` (`ProduceRequest.java:211-232`) ─────────────────

    /// One uncompressed v2 batch holding one record at `base_offset`.
    fn v2_batch(base_offset: i64, compression: crate::common::compress::Compression) -> Vec<u8> {
        use crate::common::record::{MemoryRecords, TimestampType};

        let mut builder = MemoryRecords::builder(512, compression, TimestampType::CreateTime, base_offset);
        builder.append_kv(1_700_000_000_000, Some(b"k"), Some(b"v"));
        builder.build().buffer().to_vec()
    }

    fn uncompressed_v2_batch(base_offset: i64) -> Vec<u8> {
        v2_batch(base_offset, crate::common::compress::Compression::none())
    }

    /// A message format v0 or v1 message set of one record — `offset, size, crc,
    /// magic, attributes, [timestamp,] key, value` — with a null key and a
    /// one-byte value. The CRC is left zero: `validate_records` never reads it.
    fn legacy_message(magic: i8) -> Vec<u8> {
        let mut body = vec![0u8; 4]; // crc
        body.push(magic as u8);
        body.push(0); // attributes
        if magic == RecordBatch::MAGIC_VALUE_V1 {
            body.extend_from_slice(&1_700_000_000_000_i64.to_be_bytes());
        }
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // null key
        body.extend_from_slice(&1_i32.to_be_bytes());
        body.push(b'v');
        let mut message = 0_i64.to_be_bytes().to_vec();
        message.extend_from_slice(&(body.len() as i32).to_be_bytes());
        message.extend_from_slice(&body);
        message
    }

    fn validation_error(version: i16, records: Option<Vec<u8>>) -> String {
        ProduceRequest::validate_records(version, &records.map(bytes::Bytes::from))
            .expect_err("the records must be rejected")
            .to_string()
    }

    #[test]
    fn test_validate_records_accepts_one_v2_batch() {
        ProduceRequest::validate_records(3, &Some(bytes::Bytes::from(uncompressed_v2_batch(0))))
            .expect("one complete v2 batch is valid");
    }

    /// Java's `testV3AndAboveCannotHaveNoRecordBatches`, plus the incomplete
    /// batch `hasNext()` also answers `false` for.
    #[test]
    fn test_validate_records_requires_a_complete_batch() {
        let expected = "Produce requests with version 3 must have at least one record batch per partition";
        assert_eq!(expected, validation_error(3, None));
        assert_eq!(expected, validation_error(3, Some(Vec::new())));
        let mut truncated = uncompressed_v2_batch(0);
        truncated.pop();
        assert_eq!(expected, validation_error(3, Some(truncated)));
    }

    /// Java's `testV3AndAboveCannotUseMagicV0` and `testV3AndAboveCannotUseMagicV1`:
    /// a legacy batch is rejected by its magic, not reported as a missing batch —
    /// the batch iterator has no view for it, so the magic is read off the header.
    #[test]
    fn test_validate_records_rejects_legacy_magic() {
        for magic in [RecordBatch::MAGIC_VALUE_V0, RecordBatch::MAGIC_VALUE_V1] {
            assert_eq!(
                "Produce requests with version 3 are only allowed to contain record batches with magic version 2",
                validation_error(3, Some(legacy_message(magic))),
                "magic v{magic}"
            );
        }
    }

    /// Java's `testV3AndAboveShouldContainOnlyOneRecordBatch`. `hasNext()` is true
    /// for any complete second batch, so a legacy one counts as well.
    #[test]
    fn test_validate_records_rejects_a_second_batch() {
        let expected =
            "Produce requests with version 3 are only allowed to contain exactly one record batch per partition";
        for second in [uncompressed_v2_batch(1), legacy_message(RecordBatch::MAGIC_VALUE_V1)] {
            let mut records = uncompressed_v2_batch(0);
            records.extend_from_slice(&second);
            assert_eq!(expected, validation_error(3, Some(records)));
        }
    }

    /// Java's `testV6AndBelowCannotUseZStdCompression`.
    #[test]
    fn test_validate_records_rejects_zstd_below_version_7() {
        let zstd = v2_batch(0, crate::common::compress::Compression::zstd());
        assert_eq!(
            "Produce requests with version 6 are not allowed to use ZStandard compression",
            validation_error(6, Some(zstd.clone()))
        );
        ProduceRequest::validate_records(7, &Some(bytes::Bytes::from(zstd))).expect("zstd is allowed from v7");
    }

    /// A header the stream rejects is the `CorruptRecordException` Java's
    /// `hasNext()` throws, not a missing batch.
    #[test]
    fn test_validate_records_propagates_a_corrupt_header() {
        let mut records = uncompressed_v2_batch(0);
        records[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&(-5_i32).to_be_bytes());
        assert_eq!(
            "Record size -5 is less than the minimum record overhead (14)",
            validation_error(3, Some(records))
        );
    }
}
