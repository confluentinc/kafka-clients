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

//! `AddPartitionsToTxn` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.AddPartitionsToTxnRequest`.
//!
//! # Two wire shapes
//!
//! This RPC carries two different layouts. Up to and including v3 it is a
//! *client* request: a single transactional id with its topic list in the
//! `v3_and_below_*` fields. From v4 it is a *broker* request, carrying a
//! collection of transactions so a broker can batch-verify several at once
//! (KIP-890). A producer only ever sends the v3-and-below shape — see
//! [`AddPartitionsToTxnRequestBuilder::for_client`].
//!
//! # Scope: broker-side request inspection is not translated
//!
//! Java's class also carries `normalizeRequest`, `allVerifyOnlyRequest`,
//! `partitionsByTransaction`, and `errorResponseForTransaction`. All four
//! inspect a *received* v4+ request, which only a broker does. Verified callers,
//! none under `clients/src/main`:
//!
//!   - `Builder.forBroker` → `server/.../AddPartitionsToTxnManager.java:343`
//!   - `normalizeRequest` → `core/.../KafkaApis.scala:1852`
//!   - `partitionsByTransaction` → `KafkaApis.scala:1857`, `:1895`
//!   - `errorResponseForTransaction` → `KafkaApis.scala:1899`, `:1938`
//!   - `allVerifyOnlyRequest` → `core/.../network/RequestChannel.scala:228`
//!
//! They are omitted for the same reason `WriteTxnMarkers` and
//! `EndTransactionMarker` are out of scope for this client-only port (see
//! `design/history/Milestone-11/PLAN.md` §1.1). Translating them would add
//! permanently unreachable code that `#![deny(warnings)]` would force us to
//! mask with `#[allow(dead_code)]`.

use std::collections::HashMap;
use std::io;

use crate::AddPartitionsToTxnRequestData;
use crate::AddPartitionsToTxnResponseData;
use crate::add_partitions_to_txn_request_data::AddPartitionsToTxnTopic;
use crate::add_partitions_to_txn_response_data::{AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult};
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AddPartitionsToTxnResponse;
use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;

/// An `AddPartitionsToTxn` request.
///
/// Corresponds to `org.apache.kafka.common.requests.AddPartitionsToTxnRequest`.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequest {
    data: AddPartitionsToTxnRequestData,
    version: i16,
}

impl AddPartitionsToTxnRequest {
    /// Highest version a client may send.
    ///
    /// Corresponds to `AddPartitionsToTxnRequest.LAST_CLIENT_VERSION`.
    pub const LAST_CLIENT_VERSION: i16 = 3;

    /// Lowest version carrying the broker (batched-transactions) shape.
    ///
    /// Also the first version to support verification requests. Corresponds to
    /// `AddPartitionsToTxnRequest.EARLIEST_BROKER_VERSION`.
    pub const EARLIEST_BROKER_VERSION: i16 = 4;

    /// Creates a new `AddPartitionsToTxnRequest` from data and version.
    pub fn new(data: AddPartitionsToTxnRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddPartitionsToTxnRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AddPartitionsToTxnRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_PARTITIONS_TO_TXN
    }

    /// Flattens a topic collection into its topic-partitions.
    ///
    /// Corresponds to Java's static
    /// `getPartitions(AddPartitionsToTxnTopicCollection)`.
    pub fn get_partitions(topics: &[AddPartitionsToTxnTopic]) -> Vec<TopicPartition> {
        topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| TopicPartition::new(topic.name.clone(), *partition))
            })
            .collect()
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `AddPartitionsToTxnRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    ///
    /// Below [`Self::EARLIEST_BROKER_VERSION`] the error is reported per partition, in
    /// the `results_by_topic_v3_and_below` field; from v4 it is a single
    /// top-level `error_code`. The throttle time is set either way.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = AddPartitionsToTxnResponseData::new();
        if self.version < AddPartitionsToTxnRequest::EARLIEST_BROKER_VERSION {
            response.set_results_by_topic_v3_and_below(Self::error_response_for_topics(
                &self.data.v3_and_below_topics,
                error,
            ));
        } else {
            response.set_error_code(error.code());
        }
        response.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::AddPartitionsToTxn(AddPartitionsToTxnResponse::new(response))
    }

    /// Builds a per-partition error result for every partition in `topics`.
    ///
    /// Corresponds to Java's private `errorResponseForTopics`.
    fn error_response_for_topics(
        topics: &[AddPartitionsToTxnTopic],
        error: &Errors,
    ) -> Vec<AddPartitionsToTxnTopicResult> {
        topics
            .iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        let mut result = AddPartitionsToTxnPartitionResult::new();
                        result.set_partition_index(*partition).set_partition_error_code(error.code());
                        result
                    })
                    .collect();
                let mut topic_result = AddPartitionsToTxnTopicResult::new();
                topic_result.set_name(topic.name.clone()).set_results_by_partition(partitions);
                topic_result
            })
            .collect()
    }

    /// Parses an `AddPartitionsToTxnRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AddPartitionsToTxnRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for AddPartitionsToTxnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AddPartitionsToTxnRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`AddPartitionsToTxnRequest`].
///
/// Corresponds to `AddPartitionsToTxnRequest.Builder` in Java. Java exposes two
/// named constructors; only the client one is translated (see the module docs).
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequestBuilder {
    data: AddPartitionsToTxnRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl AddPartitionsToTxnRequestBuilder {
    /// Creates a builder for a producer's own transaction, capped at
    /// [`AddPartitionsToTxnRequest::LAST_CLIENT_VERSION`].
    ///
    /// Corresponds to `AddPartitionsToTxnRequest.Builder.forClient`.
    pub fn for_client(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        partitions: &[TopicPartition],
    ) -> Self {
        let mut data = AddPartitionsToTxnRequestData::new();
        data.set_v3_and_below_transactional_id(transactional_id.into())
            .set_v3_and_below_producer_id(producer_id)
            .set_v3_and_below_producer_epoch(producer_epoch)
            .set_v3_and_below_topics(Self::build_txn_topic_collection(partitions));

        Self {
            data,
            oldest_allowed_version: ApiKeys::ADD_PARTITIONS_TO_TXN.oldest_version(),
            latest_allowed_version: AddPartitionsToTxnRequest::LAST_CLIENT_VERSION,
        }
    }

    /// Groups `partitions` by topic name.
    ///
    /// Corresponds to Java's private static `buildTxnTopicCollection`. Java uses
    /// a `HashMap`, so its output order is unspecified; this sorts by topic name
    /// so the encoding is deterministic. Determinism is a *precondition* for the
    /// byte-level wire tests `definition-of-done.md` §3 requires — those do not
    /// exist yet for this type, tracked as PLAN §9.14. The sort is still correct
    /// and load-bearing: without it the encoding varies run to run, so the tests
    /// could not be written at all.
    fn build_txn_topic_collection(partitions: &[TopicPartition]) -> Vec<AddPartitionsToTxnTopic> {
        let mut partition_map: HashMap<&str, Vec<i32>> = HashMap::new();
        for topic_partition in partitions {
            partition_map
                .entry(topic_partition.topic())
                .or_default()
                .push(topic_partition.partition());
        }

        let mut names: Vec<&str> = partition_map.keys().copied().collect();
        names.sort_unstable();

        names
            .into_iter()
            .map(|name| {
                let mut topic = AddPartitionsToTxnTopic::new();
                topic.set_name(name.to_string()).set_partitions(partition_map[name].clone());
                topic
            })
            .collect()
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &AddPartitionsToTxnRequestData {
        &self.data
    }
}

impl RequestBuilder for AddPartitionsToTxnRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ADD_PARTITIONS_TO_TXN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(ConcreteRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    #[test]
    fn test_for_client_sets_v3_and_below_fields() {
        let builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 42, 7, &[tp("topic-a", 0)]);
        let data = builder.data();

        assert_eq!(data.v3_and_below_transactional_id, "txn-1");
        assert_eq!(data.v3_and_below_producer_id, 42);
        assert_eq!(data.v3_and_below_producer_epoch, 7);
        // The v4+ field stays empty — a client never sends the broker shape.
        assert!(data.transactions.is_empty());
    }

    /// A client builder must never offer a version above `LAST_CLIENT_VERSION`,
    /// even though the API itself supports higher ones.
    #[test]
    fn test_for_client_caps_the_version_at_last_client_version() {
        let builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 1, 0, &[tp("t", 0)]);
        assert_eq!(builder.latest_allowed_version(), AddPartitionsToTxnRequest::LAST_CLIENT_VERSION);
        assert_eq!(
            builder.oldest_allowed_version(),
            ApiKeys::ADD_PARTITIONS_TO_TXN.oldest_version()
        );
        assert!(
            AddPartitionsToTxnRequest::LAST_CLIENT_VERSION < ApiKeys::ADD_PARTITIONS_TO_TXN.latest_version(),
            "the cap is only meaningful if the API supports higher versions"
        );
    }

    #[test]
    fn test_build_txn_topic_collection_groups_by_topic() {
        let builder = AddPartitionsToTxnRequestBuilder::for_client(
            "txn-1",
            1,
            0,
            &[tp("topic-b", 1), tp("topic-a", 0), tp("topic-b", 3), tp("topic-a", 2)],
        );
        let topics = &builder.data().v3_and_below_topics;

        assert_eq!(topics.len(), 2, "two distinct topics");
        // Sorted by name for deterministic encoding (Java's HashMap is unordered).
        assert_eq!(topics[0].name, "topic-a");
        assert_eq!(topics[0].partitions, vec![0, 2]);
        assert_eq!(topics[1].name, "topic-b");
        assert_eq!(topics[1].partitions, vec![1, 3]);
    }

    #[test]
    fn test_get_partitions_round_trips_the_collection() {
        let partitions = vec![tp("topic-a", 0), tp("topic-a", 2), tp("topic-b", 1)];
        let builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 1, 0, &partitions);

        let flattened = AddPartitionsToTxnRequest::get_partitions(&builder.data().v3_and_below_topics);
        assert_eq!(flattened, partitions);
    }

    #[test]
    fn test_get_partitions_on_empty_collection() {
        assert!(AddPartitionsToTxnRequest::get_partitions(&[]).is_empty());
    }

    /// Below v4 the error is reported per partition, not top-level.
    #[test]
    fn test_get_error_response_v3_and_below_is_per_partition() {
        let mut builder = AddPartitionsToTxnRequestBuilder::for_client(
            "txn-1",
            1,
            0,
            &[tp("topic-a", 0), tp("topic-a", 5), tp("topic-b", 1)],
        );
        let request = match builder
            .build_version(AddPartitionsToTxnRequest::LAST_CLIENT_VERSION)
            .expect("build")
        {
            ConcreteRequest::AddPartitionsToTxn(request) => request,
            other => panic!("expected AddPartitionsToTxn, got {other:?}"),
        };

        match request.get_error_response(99, &Errors::NotCoordinator) {
            ConcreteResponse::AddPartitionsToTxn(response) => {
                let data = response.data();
                assert_eq!(data.throttle_time_ms, 99);
                // Top-level error stays unset below v4.
                assert_eq!(data.error_code, Errors::None.code());
                assert!(data.results_by_transaction.is_empty());

                let topics = &data.results_by_topic_v3_and_below;
                assert_eq!(topics.len(), 2);
                assert_eq!(topics[0].name, "topic-a");
                assert_eq!(topics[0].results_by_partition.len(), 2);
                for partition in &topics[0].results_by_partition {
                    assert_eq!(partition.partition_error_code, Errors::NotCoordinator.code());
                }
                assert_eq!(topics[1].name, "topic-b");
                assert_eq!(
                    topics[1].results_by_partition[0].partition_index, 1,
                    "partition index preserved"
                );
            },
            other => panic!("expected AddPartitionsToTxn response, got {other:?}"),
        }
    }

    /// From v4 the error is a single top-level code with no per-partition
    /// results. A client never sends v4+, but `get_error_response` is reached
    /// through the generic dispatch, so both branches are translated.
    #[test]
    fn test_get_error_response_v4_and_above_is_top_level() {
        let mut data = AddPartitionsToTxnRequestData::new();
        data.set_v3_and_below_topics(vec![]);
        let request = AddPartitionsToTxnRequest::new(data, AddPartitionsToTxnRequest::EARLIEST_BROKER_VERSION);

        match request.get_error_response(5, &Errors::InvalidTxnState) {
            ConcreteResponse::AddPartitionsToTxn(response) => {
                assert_eq!(response.data().error_code, Errors::InvalidTxnState.code());
                assert_eq!(response.data().throttle_time_ms, 5);
                assert!(response.data().results_by_topic_v3_and_below.is_empty());
            },
            other => panic!("expected AddPartitionsToTxn response, got {other:?}"),
        }
    }

    #[test]
    fn test_serialization_round_trip_all_client_versions() {
        for version in ApiKeys::ADD_PARTITIONS_TO_TXN.oldest_version()..=AddPartitionsToTxnRequest::LAST_CLIENT_VERSION
        {
            let mut builder =
                AddPartitionsToTxnRequestBuilder::for_client("txn-1", 42, 7, &[tp("topic-a", 0), tp("topic-b", 1)]);
            let mut built = builder.build_version(version).expect("build");
            let mut buffer = built.serialize().expect("serialize");
            buffer.flip();
            let parsed = AddPartitionsToTxnRequest::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data().v3_and_below_transactional_id, "txn-1", "v{version}");
            assert_eq!(parsed.data().v3_and_below_producer_id, 42);
            assert_eq!(parsed.data().v3_and_below_producer_epoch, 7);
            assert_eq!(
                AddPartitionsToTxnRequest::get_partitions(&parsed.data().v3_and_below_topics),
                vec![tp("topic-a", 0), tp("topic-b", 1)],
                "v{version}"
            );
        }
    }

    #[test]
    fn test_api_key_and_version() {
        let builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 1, 0, &[]);
        assert_eq!(builder.api_key(), &ApiKeys::ADD_PARTITIONS_TO_TXN);
        let request = AddPartitionsToTxnRequest::new(AddPartitionsToTxnRequestData::new(), 2);
        assert_eq!(request.api_key(), &ApiKeys::ADD_PARTITIONS_TO_TXN);
        assert_eq!(request.version(), 2);
    }

    /// Translated from `AddPartitionsToTxnRequestTest.testConstructor`, the
    /// `version < 4` branch — the client shape.
    ///
    /// The `version >= 4` branch is **not** translated: it builds via
    /// `Builder.forBroker` and asserts on `data().transactions()`, both of which
    /// are broker-side and out of scope (see the module docs).
    ///
    /// Java parameterises over every API version via `@ApiKeyVersionsSource`;
    /// per `definition-of-done.md` §3 that becomes a loop.
    #[test]
    fn test_constructor() {
        const PRODUCER_ID: i64 = 10;
        const PRODUCER_EPOCH: i16 = 1;
        const THROTTLE_TIME_MS: i32 = 10;

        for version in ApiKeys::ADD_PARTITIONS_TO_TXN.oldest_version()..=AddPartitionsToTxnRequest::LAST_CLIENT_VERSION
        {
            let partitions = vec![tp("topic", 0), tp("topic", 1)];
            let mut builder =
                AddPartitionsToTxnRequestBuilder::for_client("transaction1", PRODUCER_ID, PRODUCER_EPOCH, &partitions);
            let request = match builder.build_version(version).expect("build") {
                ConcreteRequest::AddPartitionsToTxn(request) => request,
                other => panic!("expected AddPartitionsToTxn, got {other:?}"),
            };

            assert_eq!(request.data().v3_and_below_transactional_id, "transaction1");
            assert_eq!(request.data().v3_and_below_producer_id, PRODUCER_ID);
            assert_eq!(request.data().v3_and_below_producer_epoch, PRODUCER_EPOCH);
            assert_eq!(
                AddPartitionsToTxnRequest::get_partitions(&request.data().v3_and_below_topics),
                partitions,
                "v{version}"
            );

            match request.get_error_response(THROTTLE_TIME_MS, &Errors::UnknownTopicOrPartition) {
                ConcreteResponse::AddPartitionsToTxn(response) => {
                    assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS);
                    // Below v4 the error is per partition, so two partitions
                    // yield a count of 2 — not one top-level error.
                    let counts = response.error_counts();
                    assert_eq!(counts.len(), 1, "v{version}");
                    assert_eq!(counts.get(&Errors::UnknownTopicOrPartition), Some(&2), "v{version}");
                },
                other => panic!("expected AddPartitionsToTxn response, got {other:?}"),
            }
        }
    }

    /// Byte-level wire-encoding test for the v3-and-below (client) shape.
    ///
    /// The round-trip test above (`serialize()` then `parse()`) proves the
    /// writer and reader *agree*, but a systematic encoding fault shared by both
    /// — a wrong varint width, a missing compact-string bias, a dropped
    /// tagged-fields trailer — round-trips cleanly while being wire-incompatible
    /// with the Java client. `definition-of-done.md` §3 therefore requires a
    /// byte-level test against known vectors.
    ///
    /// The expected bytes below are derived **independently** from the Kafka
    /// wire-protocol spec (field order per `AddPartitionsToTxnRequest.json`,
    /// big-endian fixed-width ints, and the flexible-vs-non-flexible framing
    /// rules), NOT captured from `serialize()`'s output — capturing the output
    /// would merely freeze current behaviour and catch nothing.
    ///
    /// Input is fixed and minimal: `for_client("txn-1", 42, 7, [topic-a/0])` —
    /// one topic, one partition — so each byte is reviewable by hand.
    ///
    /// `serialize()` emits the request **body only** (no request header, no size
    /// prefix — see `AbstractRequest::serialize`), so the expected vectors are
    /// exactly the message body.
    #[test]
    fn test_serialize_wire_bytes_v2_non_flexible() {
        // v0/v1/v2 encode identically: the write path branches only on
        // `version >= 3` (flexible) and `version >= 4` (broker shape), so any of
        // 0/1/2 exercises the same non-flexible layout. v2 is the last
        // non-flexible version.
        let version: i16 = 2;
        let mut builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 42, 7, &[tp("topic-a", 0)]);
        let mut built = builder.build_version(version).expect("build");
        let actual = built.serialize().expect("serialize");

        // Derived from the spec for a NON-flexible version (v2):
        //   - strings: int16 length prefix, then raw UTF-8 bytes
        //   - arrays:  int32 count prefix, then elements
        //   - no per-struct tagged-fields trailer
        #[rustfmt::skip]
        let expected: &[u8] = &[
            // V3AndBelowTransactionalId (string) = "txn-1"
            0x00, 0x05,                                     // int16 len = 5
            0x74, 0x78, 0x6e, 0x2d, 0x31,                   // "txn-1"
            // V3AndBelowProducerId (int64) = 42
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a,
            // V3AndBelowProducerEpoch (int16) = 7
            0x00, 0x07,
            // V3AndBelowTopics ([]AddPartitionsToTxnTopic), int32 count = 1
            0x00, 0x00, 0x00, 0x01,
                // Topic[0].Name (string) = "topic-a"
                0x00, 0x07,                                 // int16 len = 7
                0x74, 0x6f, 0x70, 0x69, 0x63, 0x2d, 0x61,   // "topic-a"
                // Topic[0].Partitions ([]int32), int32 count = 1
                0x00, 0x00, 0x00, 0x01,
                    0x00, 0x00, 0x00, 0x00,                 // partition index = 0
        ];

        assert_eq!(
            actual.buffer(),
            expected,
            "v2 body encoding diverges from the hand-derived spec vector"
        );

        // Belt-and-suspenders: parse the hand-derived bytes and confirm the
        // fields come back as expected — so the test proves both that the bytes
        // match the spec AND that the reader agrees with them.
        let mut reader = ByteBufferAccessor::new(expected.to_vec());
        let parsed = AddPartitionsToTxnRequest::parse(&mut reader, version).expect("parse");
        assert_eq!(parsed.data().v3_and_below_transactional_id, "txn-1");
        assert_eq!(parsed.data().v3_and_below_producer_id, 42);
        assert_eq!(parsed.data().v3_and_below_producer_epoch, 7);
        assert_eq!(
            AddPartitionsToTxnRequest::get_partitions(&parsed.data().v3_and_below_topics),
            vec![tp("topic-a", 0)]
        );
    }

    /// Byte-level wire-encoding test for the flexible version (v3).
    ///
    /// v3 is the essential case: it exercises the flexible-version framing that a
    /// round-trip test cannot distinguish from a broken variant —
    ///   - compact strings: unsigned-varint length = `len + 1`
    ///   - compact arrays:  unsigned-varint count  = `count + 1`
    ///   - a per-struct tagged-fields trailer (`0x00` unsigned varint when
    ///     empty) on BOTH each topic struct AND the top-level message.
    ///
    /// As above, the expected bytes are derived independently from the spec, not
    /// captured from `serialize()`.
    #[test]
    fn test_serialize_wire_bytes_v3_flexible() {
        let version: i16 = 3;
        let mut builder = AddPartitionsToTxnRequestBuilder::for_client("txn-1", 42, 7, &[tp("topic-a", 0)]);
        let mut built = builder.build_version(version).expect("build");
        let actual = built.serialize().expect("serialize");

        // Derived from the spec for a FLEXIBLE version (v3, flexibleVersions "3+").
        // Small lengths/counts (< 128) each encode as a single-byte varint.
        #[rustfmt::skip]
        let expected: &[u8] = &[
            // V3AndBelowTransactionalId (compact string) = "txn-1"
            0x06,                                           // uvarint len+1 = 6 (len 5)
            0x74, 0x78, 0x6e, 0x2d, 0x31,                   // "txn-1"
            // V3AndBelowProducerId (int64) = 42
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a,
            // V3AndBelowProducerEpoch (int16) = 7
            0x00, 0x07,
            // V3AndBelowTopics (compact array) count+1 = 2 (1 topic)
            0x02,
                // Topic[0].Name (compact string) = "topic-a"
                0x08,                                       // uvarint len+1 = 8 (len 7)
                0x74, 0x6f, 0x70, 0x69, 0x63, 0x2d, 0x61,   // "topic-a"
                // Topic[0].Partitions (compact array) count+1 = 2 (1 partition)
                0x02,
                    0x00, 0x00, 0x00, 0x00,                 // partition index = 0 (int32, not compact)
                // Topic[0] tagged fields: uvarint count = 0 (empty)
                0x00,
            // Top-level tagged fields: uvarint count = 0 (empty)
            0x00,
        ];

        assert_eq!(
            actual.buffer(),
            expected,
            "v3 flexible body encoding diverges from the hand-derived spec vector"
        );

        // Belt-and-suspenders: parse the hand-derived bytes back.
        let mut reader = ByteBufferAccessor::new(expected.to_vec());
        let parsed = AddPartitionsToTxnRequest::parse(&mut reader, version).expect("parse");
        assert_eq!(parsed.data().v3_and_below_transactional_id, "txn-1");
        assert_eq!(parsed.data().v3_and_below_producer_id, 42);
        assert_eq!(parsed.data().v3_and_below_producer_epoch, 7);
        assert_eq!(
            AddPartitionsToTxnRequest::get_partitions(&parsed.data().v3_and_below_topics),
            vec![tp("topic-a", 0)]
        );
    }

    // -- Java tests deliberately not translated (DoD §3) ---------------------
    //
    // `AddPartitionsToTxnRequestTest.testBatchedRequests` and
    // `.testNormalizeRequest` exercise `Builder.forBroker`,
    // `partitionsByTransaction`, `errorResponseForTransaction`, and
    // `normalizeRequest`. All four inspect or construct the v4+ broker shape,
    // which this client-only port does not translate (see the module docs for
    // the caller analysis). There is nothing here for them to test.
    //
    // The `version >= 4` half of `testConstructor` is skipped for the same
    // reason; the `version < 4` half is translated above.
}
