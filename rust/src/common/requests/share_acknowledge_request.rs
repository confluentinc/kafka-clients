// Copyright 2026 Confluent Inc.
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

//! `ShareAcknowledge` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeRequest`.

use std::fmt;
use std::io;

use crate::ShareAcknowledgeRequestData;
use crate::ShareAcknowledgeResponseData;
use crate::common::TopicIdPartition;
use crate::common::errors::UnsupportedVersionError;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_acknowledge_request_data::{AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch};

use super::AbstractRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareAcknowledgeResponse;
use super::ShareRequestMetadata;

/// A `ShareAcknowledge` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareAcknowledgeRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest")]
pub struct ShareAcknowledgeRequest {
    data: ShareAcknowledgeRequestData,
    version: i16,
}

impl ShareAcknowledgeRequest {
    /// Creates a new `ShareAcknowledgeRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest#ShareAcknowledgeRequest")]
    pub fn new(data: ShareAcknowledgeRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest#data")]
    pub fn data(&self) -> &ShareAcknowledgeRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareAcknowledgeRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    /// Builds the error response for this request: the throttle time and the
    /// top-level error code.
    ///
    /// Translates `ShareAcknowledgeRequest.getErrorResponse(int, Throwable)`;
    /// the caller supplies `Errors.forException(e)`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ShareAcknowledgeResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_error_code(error.code());
        ConcreteResponse::ShareAcknowledge(ShareAcknowledgeResponse::new(data))
    }

    /// Parses a `ShareAcknowledgeRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareAcknowledgeRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl fmt::Display for ShareAcknowledgeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`ShareAcknowledgeRequest`].
///
/// Corresponds to `ShareAcknowledgeRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest$Builder")]
pub struct Builder {
    data: ShareAcknowledgeRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest$Builder#Builder")]
    pub fn new(data: ShareAcknowledgeRequestData) -> Self {
        Self { data }
    }

    /// Builds the request a share consumer sends to acknowledge records on one
    /// broker, one partition per `acknowledgements_map` entry, in its order.
    ///
    /// With `metadata`, the request carries its member id and epoch.
    ///
    /// Translates `ShareAcknowledgeRequest.Builder.forConsumer`. Java takes a
    /// `Map` (a `HashMap` from `ShareSessionHandler`); the iterator keeps
    /// whatever order the caller's map has, and moves the batches in instead of
    /// sharing the lists.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest$Builder#forConsumer")]
    pub fn for_consumer(
        group_id: &str,
        metadata: Option<&ShareRequestMetadata>,
        is_renew_ack: bool,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, Vec<AcknowledgementBatch>)>,
    ) -> Self {
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_group_id(Some(group_id.to_string()));
        if let Some(metadata) = metadata {
            data.set_member_id(Some(metadata.member_id().to_string()))
                .set_share_session_epoch(metadata.epoch());
        }
        data.set_is_renew_ack(is_renew_ack);

        // The `find`-then-`add` of Java's `AcknowledgeTopicCollection` (keyed by
        // topic id) and `AcknowledgePartitionCollection` (keyed by partition index).
        let mut ack_topics: Vec<AcknowledgeTopic> = Vec::new();
        for (tip, batches) in acknowledgements_map {
            let topic_index = match ack_topics.iter().position(|topic| topic.topic_id == tip.topic_id()) {
                Some(index) => index,
                None => {
                    let mut ack_topic = AcknowledgeTopic::new();
                    ack_topic.set_topic_id(tip.topic_id());
                    ack_topics.push(ack_topic);
                    ack_topics.len() - 1
                },
            };
            let ack_topic = &mut ack_topics[topic_index];
            let partition_index = match ack_topic
                .partitions
                .iter()
                .position(|partition| partition.partition_index == tip.partition())
            {
                Some(index) => index,
                None => {
                    let mut ack_partition = AcknowledgePartition::new();
                    ack_partition.set_partition_index(tip.partition());
                    ack_topic.partitions.push(ack_partition);
                    ack_topic.partitions.len() - 1
                },
            };
            ack_topic.partitions[partition_index].set_acknowledgement_batches(batches);
        }
        data.set_topics(ack_topics);
        Self::new(data)
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareAcknowledgeRequest$Builder#data")]
    pub fn data(&self) -> &ShareAcknowledgeRequestData {
        &self.data
    }
}

impl fmt::Display for Builder {
    /// Matches Java's `Builder.toString()`: the data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_ACKNOWLEDGE
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::SHARE_ACKNOWLEDGE.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::SHARE_ACKNOWLEDGE.latest_version_enable_unstable_last_version(false)
    }

    /// Translates `ShareAcknowledgeRequest.Builder.build(short)`: v1 cannot
    /// carry a RENEW acknowledgement.
    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        if version < 2 {
            // The v1 does not support AcknowledgeType RENEW.
            if self.data.is_renew_ack {
                return Err(UnsupportedVersionError::new(
                    "The v1 ShareAcknowledge does not support AcknowledgeType.RENEW",
                )
                .into_io_error());
            }
        }
        Ok(AbstractRequest::ShareAcknowledge(ShareAcknowledgeRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::request_response_test::{check_error_response, check_request};
    use crate::common::{Error, Uuid};

    fn unwrap(request: AbstractRequest) -> ShareAcknowledgeRequest {
        match request {
            AbstractRequest::ShareAcknowledge(request) => request,
            other => panic!("expected ShareAcknowledge, got {other:?}"),
        }
    }

    fn batch(first: i64, last: i64, types: Vec<i8>) -> AcknowledgementBatch {
        let mut batch = AcknowledgementBatch::new();
        batch.set_first_offset(first).set_last_offset(last).set_acknowledge_types(types);
        batch
    }

    /// `RequestResponseTest.createShareAcknowledgeRequest(short)`.
    fn create_share_acknowledge_request(version: i16) -> ShareAcknowledgeRequest {
        let mut partition = AcknowledgePartition::new();
        partition
            .set_partition_index(0)
            .set_acknowledgement_batches(vec![batch(0, 0, vec![0])]);
        let mut topic = AcknowledgeTopic::new();
        topic.set_topic_id(Uuid::random_uuid()).set_partitions(vec![partition]);
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_member_id(Some(Uuid::random_uuid().to_string()))
            .set_topics(vec![topic]);
        unwrap(Builder::new(data).build_version(version).expect("build"))
    }

    /// `RequestResponseTest.testSerialization`'s `SHARE_ACKNOWLEDGE` arm, at
    /// every version: `checkRequest` and `checkErrorResponse` (the
    /// `checkResponse` half lives in `share_acknowledge_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::SHARE_ACKNOWLEDGE.all_versions() {
            let request = AbstractRequest::ShareAcknowledge(create_share_acknowledge_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(ShareAcknowledgeRequestData::new());
        assert_eq!(&ApiKeys::SHARE_ACKNOWLEDGE, builder.api_key());
        assert_eq!(1, builder.oldest_allowed_version());
        assert_eq!(2, builder.latest_allowed_version());
    }

    /// `Builder.build(short)` rejects a RENEW acknowledgement below v2 with
    /// Java's `UnsupportedVersionException` message, and accepts it at v2.
    #[test]
    fn test_build_rejects_renew_ack_below_v2() {
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_is_renew_ack(true);
        let mut builder = Builder::new(data);
        let error = builder.build_version(1).unwrap_err();
        assert_eq!(io::ErrorKind::Unsupported, error.kind());
        let error = Error::UnsupportedVersion(UnsupportedVersionError::from_io_error(&error));
        assert_eq!(
            "The v1 ShareAcknowledge does not support AcknowledgeType.RENEW",
            error.message()
        );
        assert!(unwrap(builder.build_version(2).unwrap()).data().is_renew_ack());
    }

    /// `forConsumer` copies the metadata and nests the batches by topic and
    /// partition in map order.
    #[test]
    fn test_for_consumer() {
        let topic_b = Uuid::new(0, 2);
        let topic_a = Uuid::new(0, 1);
        let member_id = Uuid::new(3, 3);
        let metadata = ShareRequestMetadata::new(member_id, 7);
        let acks = vec![
            (
                TopicIdPartition::with_partition_topic(topic_b, 1, "b"),
                vec![batch(0, 1, vec![1])],
            ),
            (
                TopicIdPartition::with_partition_topic(topic_a, 0, "a"),
                vec![batch(2, 2, vec![2])],
            ),
            (
                TopicIdPartition::with_partition_topic(topic_b, 0, "b"),
                vec![batch(4, 4, vec![3]), batch(5, 5, vec![1])],
            ),
        ];
        let builder = Builder::for_consumer("grp", Some(&metadata), true, acks);
        let data = builder.data();
        assert_eq!(Some("grp"), data.group_id().as_deref());
        assert_eq!(Some(member_id.to_string()), data.member_id().clone());
        assert_eq!(7, data.share_session_epoch());
        assert!(data.is_renew_ack());
        // (topic id, [(partition index, [first offset of each batch])])
        type Layout = Vec<(Uuid, Vec<(i32, Vec<i64>)>)>;
        let layout: Layout = data
            .topics()
            .iter()
            .map(|t| {
                (
                    t.topic_id(),
                    t.partitions()
                        .iter()
                        .map(|p| {
                            (
                                p.partition_index(),
                                p.acknowledgement_batches().iter().map(|b| b.first_offset()).collect(),
                            )
                        })
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            vec![
                (topic_b, vec![(1, vec![0]), (0, vec![4, 5])]),
                (topic_a, vec![(0, vec![2])])
            ],
            layout
        );
    }

    /// Without metadata, the member id keeps its default and the epoch is 0.
    #[test]
    fn test_for_consumer_without_metadata() {
        let builder = Builder::for_consumer("g", None, false, Vec::new());
        assert_eq!(Some(""), builder.data().member_id().as_deref());
        assert_eq!(0, builder.data().share_session_epoch());
        assert!(builder.data().topics().is_empty());
    }

    #[test]
    fn test_get_error_response() {
        let request = create_share_acknowledge_request(2);
        match request.get_error_response(12, &Errors::InvalidRecordState) {
            ConcreteResponse::ShareAcknowledge(response) => {
                assert_eq!(12, response.throttle_time_ms());
                assert_eq!(Errors::InvalidRecordState, response.error());
                assert!(response.data().responses().is_empty());
            },
            other => panic!("expected ShareAcknowledge, got {other:?}"),
        }
    }

    fn encoding_data(renew: bool) -> ShareAcknowledgeRequestData {
        let mut partition = AcknowledgePartition::new();
        partition
            .set_partition_index(3)
            .set_acknowledgement_batches(vec![batch(10, 12, vec![1, 2, 3])]);
        let mut topic = AcknowledgeTopic::new();
        topic
            .set_topic_id(Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10))
            .set_partitions(vec![partition]);
        let mut data = ShareAcknowledgeRequestData::new();
        data.set_group_id(None)
            .set_member_id(Some("m".to_string()))
            .set_share_session_epoch(9)
            .set_is_renew_ack(renew)
            .set_topics(vec![topic]);
        data
    }

    fn topics() -> Vec<u8> {
        let mut bytes = vec![0x02]; // Topics: compact array of 1
        bytes.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        bytes.push(0x02); // Partitions: compact array of 1
        bytes.extend_from_slice(&3i32.to_be_bytes()); // PartitionIndex
        bytes.push(0x02); // AcknowledgementBatches: compact array of 1
        bytes.extend_from_slice(&10i64.to_be_bytes()); // FirstOffset
        bytes.extend_from_slice(&12i64.to_be_bytes()); // LastOffset
        bytes.extend_from_slice(&[0x04, 0x01, 0x02, 0x03]); // AcknowledgeTypes: [1, 2, 3]
        bytes.push(0x00); // batch tagged fields
        bytes.push(0x00); // partition tagged fields
        bytes.push(0x00); // topic tagged fields
        bytes.push(0x00); // request tagged fields
        bytes
    }

    /// Byte-level v2 encoding, derived by hand from `ShareAcknowledgeRequest.json`
    /// in field order: the null group id is a lone `0`, and v2 adds
    /// `IsRenewAck` after `ShareSessionEpoch`.
    #[test]
    fn test_v2_encoding() {
        let mut expected = vec![0x00, 0x02, b'm']; // GroupId: null, MemberId
        expected.extend_from_slice(&9i32.to_be_bytes()); // ShareSessionEpoch
        expected.push(0x01); // IsRenewAck: true (v2+)
        expected.extend(topics());
        let mut request = Builder::new(encoding_data(true)).build_version(2).unwrap();
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
        let parsed = ShareAcknowledgeRequest::parse(&mut ByteBufferAccessor::new(expected), 2).unwrap();
        assert_eq!(&encoding_data(true), parsed.data());
    }

    /// The same request at v1 has no `IsRenewAck`.
    #[test]
    fn test_v1_encoding() {
        let mut expected = vec![0x00, 0x02, b'm'];
        expected.extend_from_slice(&9i32.to_be_bytes());
        expected.extend(topics());
        let mut request = Builder::new(encoding_data(false)).build_version(1).unwrap();
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }
}
