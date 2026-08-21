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

//! Serialization/deserialization for consumer subscriptions and assignments.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerProtocol`.
//!
//! `ConsumerProtocol` contains the schemas for consumer subscriptions and
//! assignments for use with Kafka's generalized (classic) group management
//! protocol. The current implementation assumes that future versions will not
//! break compatibility: when it encounters a newer version, it parses it using
//! the current (highest) format.
//!
//! Java's `SchemaException` is mapped to
//! [`Error::serialization`](crate::common::Error::serialization) — a
//! non-retriable parse error, matching `SchemaException`'s nature.

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::errors::SerializationError;
use crate::common::protocol::message_util::to_version_prefixed_byte_buffer;
use crate::common::protocol::{ByteBufferAccessor, Readable};
use crate::consumer::consumer_partition_assignor::{Assignment, Subscription};
use crate::consumer_protocol_assignment_data::{
    ConsumerProtocolAssignmentData, TopicPartition as AssignmentTopicPartition,
};
use crate::consumer_protocol_subscription_data::{
    ConsumerProtocolSubscriptionData, TopicPartition as SubscriptionTopicPartition,
};

/// The consumer protocol type name.
///
/// Corresponds to `ConsumerProtocol.PROTOCOL_TYPE`. Consumed by the admin
/// group-describe handlers landing later in this phase.
#[allow(dead_code)]
pub(crate) const PROTOCOL_TYPE: &str = "consumer";

/// Safety check translating Java's `static { }` initializer
/// (`ConsumerProtocol.java:47-58`), which refuses to load the class when the
/// two halves of the protocol have drifted apart:
///
/// ```java
/// if (ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION
///         != ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
///     throw new IllegalStateException("Subscription and Assignment schemas must have the same lowest version");
/// if (ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION
///         != ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
///     throw new IllegalStateException("Subscription and Assignment schemas must have the same highest version");
/// ```
///
/// A `const` block is the closest Rust analogue of a static initializer: it
/// fails the build rather than the first call, so a spec bump that raised one
/// schema's version bound without the other could not compile — where today
/// [`ConsumerProtocol::check_subscription_version`] and
/// [`ConsumerProtocol::check_assignment_version`] would silently clamp
/// `serialize_subscription` and `deserialize_assignment` to *different* wire
/// versions. The message text is asserted by
/// `test_subscription_and_assignment_schema_versions_match`, which a `const`
/// panic message cannot be.
const _: () = {
    assert!(
        ConsumerProtocolSubscriptionData::LOWEST_SUPPORTED_VERSION
            == ConsumerProtocolAssignmentData::LOWEST_SUPPORTED_VERSION,
        "Subscription and Assignment schemas must have the same lowest version"
    );
    assert!(
        ConsumerProtocolSubscriptionData::HIGHEST_SUPPORTED_VERSION
            == ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION,
        "Subscription and Assignment schemas must have the same highest version"
    );
};

/// Serialization/deserialization helpers for the classic consumer protocol.
///
/// Corresponds to `ConsumerProtocol`. Package `internal` → `pub(crate)`.
pub(crate) struct ConsumerProtocol;

// The full `ConsumerProtocol` class is translated per DoD #2, but the admin
// group-describe path (the only in-scope caller for Milestone 11 Tier 2) wires
// just `deserialize_assignment` / `PROTOCOL_TYPE`. The serialize/subscription
// helpers are exercised by the round-trip unit tests below and become live once
// the classic-assignor/consumer-join paths are translated in a later milestone.
#[allow(dead_code)]
impl ConsumerProtocol {
    /// Translate a decode failure the way each Java `deserialize*` method's
    /// `catch (BufferUnderflowException e)` does.
    ///
    /// Java's clause is NARROW: only a genuine end-of-buffer is relabelled as
    /// `new SchemaException("Buffer underflow while parsing consumer protocol's
    /// <part>", e)`. Every other decode fault — a negative array/string length,
    /// invalid UTF-8 — is not a `BufferUnderflowException`, escapes the clause,
    /// and surfaces with its own message. Relabelling those too would report
    /// "Buffer underflow" for a fault that is nothing of the kind.
    ///
    /// [`std::io::ErrorKind::UnexpectedEof`] is the crate's
    /// `BufferUnderflowException`: `ByteBufferAccessor::check_remaining` and
    /// `BytesReader` raise it on a short buffer, while the generated readers use
    /// `InvalidData` / `InvalidInput` for the structural faults.
    ///
    /// Java passes `e` as a separate **cause**, so the relabelled message is
    /// exactly the text above with no `": ..."` suffix; the original is reachable
    /// through [`Error::source`].
    fn map_decode_error(e: std::io::Error, part: &str) -> Error {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Serialization(SerializationError::with_source(
                format!("Buffer underflow while parsing consumer protocol's {part}"),
                Error::serialization(e.to_string()),
            ))
        } else {
            // Not a BufferUnderflowException in Java — propagate unrelabelled.
            Error::serialization(e.to_string())
        }
    }

    /// Reads the 2-byte version header from the buffer.
    ///
    /// Mirrors `ConsumerProtocol.deserializeVersion`.
    pub(crate) fn deserialize_version(buffer: &mut dyn Readable) -> Result<i16, Error> {
        buffer.read_short().map_err(|e| Self::map_decode_error(e, "header"))
    }

    /// Serializes a subscription at the highest supported version.
    ///
    /// Mirrors `ConsumerProtocol.serializeSubscription(Subscription)`.
    pub(crate) fn serialize_subscription(subscription: &Subscription) -> Result<Vec<u8>, Error> {
        Self::serialize_subscription_versioned(
            subscription,
            ConsumerProtocolSubscriptionData::HIGHEST_SUPPORTED_VERSION,
        )
    }

    /// Serializes a subscription at the given version.
    ///
    /// Mirrors `ConsumerProtocol.serializeSubscription(Subscription, short)`.
    pub(crate) fn serialize_subscription_versioned(
        subscription: &Subscription,
        version: i16,
    ) -> Result<Vec<u8>, Error> {
        let version = Self::check_subscription_version(version)?;

        let mut data = ConsumerProtocolSubscriptionData::new();

        let mut topics: Vec<String> = subscription.topics().to_vec();
        topics.sort();
        data.set_topics(topics);

        data.set_user_data(subscription.user_data().map(<[u8]>::to_vec));

        let mut owned_partitions: Vec<TopicPartition> = subscription.owned_partitions().to_vec();
        owned_partitions.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
        let mut wire_owned: Vec<SubscriptionTopicPartition> = Vec::new();
        for tp in &owned_partitions {
            if wire_owned.last().is_none_or(|last| last.topic != tp.topic()) {
                let mut partition = SubscriptionTopicPartition::new();
                partition.set_topic(tp.topic().to_string());
                wire_owned.push(partition);
            }
            wire_owned.last_mut().expect("just pushed").partitions.push(tp.partition());
        }
        data.set_owned_partitions(wire_owned);

        if let Some(rack_id) = subscription.rack_id() {
            data.set_rack_id(Some(rack_id.to_string()));
        }

        data.set_generation_id(subscription.generation_id().unwrap_or(-1));

        Ok(to_version_prefixed_byte_buffer(version, &mut data)
            .map_err(|e| Error::serialization(format!("Failed to serialize consumer protocol's subscription: {e}")))?
            .into_buffer())
    }

    /// Deserializes a subscription at the given version (body only, version
    /// already consumed).
    ///
    /// Mirrors `ConsumerProtocol.deserializeSubscription(ByteBuffer, short)`.
    pub(crate) fn deserialize_subscription_versioned(
        buffer: &mut dyn Readable,
        version: i16,
    ) -> Result<Subscription, Error> {
        let version = Self::check_subscription_version(version)?;

        let data = ConsumerProtocolSubscriptionData::read(buffer, version)
            .map_err(|e| Self::map_decode_error(e, "subscription"))?;

        let mut owned_partitions = Vec::new();
        for tp in &data.owned_partitions {
            for partition in &tp.partitions {
                owned_partitions.push(TopicPartition::new(tp.topic.clone(), *partition));
            }
        }

        let rack_id = match &data.rack_id {
            Some(rack) if !rack.is_empty() => Some(rack.clone()),
            _ => None,
        };

        Ok(Subscription::new(
            data.topics.clone(),
            data.user_data.clone(),
            owned_partitions,
            data.generation_id,
            rack_id,
        ))
    }

    /// Deserializes a subscription, reading the version header from the buffer.
    ///
    /// Mirrors `ConsumerProtocol.deserializeSubscription(ByteBuffer)`.
    pub(crate) fn deserialize_subscription(bytes: &[u8]) -> Result<Subscription, Error> {
        let mut buffer = ByteBufferAccessor::from_bytes(bytes.to_vec());
        let version = Self::deserialize_version(&mut buffer)?;
        Self::deserialize_subscription_versioned(&mut buffer, version)
    }

    /// Deserializes the raw generated subscription struct at the given version
    /// (body only, version already consumed).
    ///
    /// Mirrors
    /// `ConsumerProtocol.deserializeConsumerProtocolSubscription(ByteBuffer, short)`.
    pub(crate) fn deserialize_consumer_protocol_subscription_versioned(
        buffer: &mut dyn Readable,
        version: i16,
    ) -> Result<ConsumerProtocolSubscriptionData, Error> {
        let version = Self::check_subscription_version(version)?;
        ConsumerProtocolSubscriptionData::read(buffer, version).map_err(|e| Self::map_decode_error(e, "subscription"))
    }

    /// Deserializes the raw generated subscription struct, reading the version
    /// header from the buffer.
    ///
    /// Mirrors
    /// `ConsumerProtocol.deserializeConsumerProtocolSubscription(ByteBuffer)`.
    pub(crate) fn deserialize_consumer_protocol_subscription(
        bytes: &[u8],
    ) -> Result<ConsumerProtocolSubscriptionData, Error> {
        let mut buffer = ByteBufferAccessor::from_bytes(bytes.to_vec());
        let version = Self::deserialize_version(&mut buffer)?;
        Self::deserialize_consumer_protocol_subscription_versioned(&mut buffer, version)
    }

    /// Serializes an assignment at the highest supported version.
    ///
    /// Mirrors `ConsumerProtocol.serializeAssignment(Assignment)`.
    pub(crate) fn serialize_assignment(assignment: &Assignment) -> Result<Vec<u8>, Error> {
        Self::serialize_assignment_versioned(assignment, ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION)
    }

    /// Serializes an assignment at the given version.
    ///
    /// Mirrors `ConsumerProtocol.serializeAssignment(Assignment, short)`.
    pub(crate) fn serialize_assignment_versioned(assignment: &Assignment, version: i16) -> Result<Vec<u8>, Error> {
        let version = Self::check_assignment_version(version)?;

        let mut data = ConsumerProtocolAssignmentData::new();
        data.set_user_data(assignment.user_data().map(<[u8]>::to_vec));

        let mut assigned: Vec<AssignmentTopicPartition> = Vec::new();
        for tp in assignment.partitions() {
            // Mirrors `data.assignedPartitions().find(tp.topic())`.
            if let Some(existing) = assigned.iter_mut().find(|p| p.topic == tp.topic()) {
                existing.partitions.push(tp.partition());
            } else {
                let mut partition = AssignmentTopicPartition::new();
                partition.set_topic(tp.topic().to_string());
                partition.partitions.push(tp.partition());
                assigned.push(partition);
            }
        }
        data.set_assigned_partitions(assigned);

        Ok(to_version_prefixed_byte_buffer(version, &mut data)
            .map_err(|e| Error::serialization(format!("Failed to serialize consumer protocol's assignment: {e}")))?
            .into_buffer())
    }

    /// Serializes the raw generated assignment struct at the given version.
    ///
    /// Mirrors `ConsumerProtocol.serializeAssignment(ConsumerProtocolAssignment, short)`.
    /// Rust cannot overload `serialize_assignment`, so this data-struct variant
    /// carries the `_data` suffix.
    pub(crate) fn serialize_assignment_data(
        mut data: ConsumerProtocolAssignmentData,
        version: i16,
    ) -> Result<Vec<u8>, Error> {
        let version = Self::check_assignment_version(version)?;
        Ok(to_version_prefixed_byte_buffer(version, &mut data)
            .map_err(|e| Error::serialization(format!("Failed to serialize consumer protocol's assignment: {e}")))?
            .into_buffer())
    }

    /// Deserializes an assignment at the given version (body only, version
    /// already consumed).
    ///
    /// Mirrors `ConsumerProtocol.deserializeAssignment(ByteBuffer, short)`.
    pub(crate) fn deserialize_assignment_versioned(
        buffer: &mut dyn Readable,
        version: i16,
    ) -> Result<Assignment, Error> {
        let version = Self::check_assignment_version(version)?;

        let data = ConsumerProtocolAssignmentData::read(buffer, version)
            .map_err(|e| Self::map_decode_error(e, "assignment"))?;

        let mut assigned_partitions = Vec::new();
        for tp in &data.assigned_partitions {
            for partition in &tp.partitions {
                assigned_partitions.push(TopicPartition::new(tp.topic.clone(), *partition));
            }
        }

        Ok(Assignment::new(assigned_partitions, data.user_data.clone()))
    }

    /// Deserializes an assignment, reading the version header from the buffer.
    ///
    /// Mirrors `ConsumerProtocol.deserializeAssignment(ByteBuffer)`. This is the
    /// entry point used by the admin group-describe handlers to decode a
    /// classic member's raw assignment bytes.
    pub(crate) fn deserialize_assignment(bytes: &[u8]) -> Result<Assignment, Error> {
        let mut buffer = ByteBufferAccessor::from_bytes(bytes.to_vec());
        let version = Self::deserialize_version(&mut buffer)?;
        Self::deserialize_assignment_versioned(&mut buffer, version)
    }

    /// Deserializes the raw generated assignment struct at the given version
    /// (body only, version already consumed).
    ///
    /// Mirrors
    /// `ConsumerProtocol.deserializeConsumerProtocolAssignment(ByteBuffer, short)`.
    pub(crate) fn deserialize_consumer_protocol_assignment_versioned(
        buffer: &mut dyn Readable,
        version: i16,
    ) -> Result<ConsumerProtocolAssignmentData, Error> {
        let version = Self::check_assignment_version(version)?;
        ConsumerProtocolAssignmentData::read(buffer, version).map_err(|e| Self::map_decode_error(e, "assignment"))
    }

    /// Deserializes the raw generated assignment struct, reading the version
    /// header from the buffer.
    ///
    /// Mirrors
    /// `ConsumerProtocol.deserializeConsumerProtocolAssignment(ByteBuffer)`.
    pub(crate) fn deserialize_consumer_protocol_assignment(
        bytes: &[u8],
    ) -> Result<ConsumerProtocolAssignmentData, Error> {
        let mut buffer = ByteBufferAccessor::from_bytes(bytes.to_vec());
        let version = Self::deserialize_version(&mut buffer)?;
        Self::deserialize_consumer_protocol_assignment_versioned(&mut buffer, version)
    }

    /// Validates a subscription version, clamping to the highest supported
    /// version if newer.
    ///
    /// Mirrors `ConsumerProtocol.checkSubscriptionVersion`.
    fn check_subscription_version(version: i16) -> Result<i16, Error> {
        if version < ConsumerProtocolSubscriptionData::LOWEST_SUPPORTED_VERSION {
            Err(Error::serialization(format!("Unsupported subscription version: {version}")))
        } else if version > ConsumerProtocolSubscriptionData::HIGHEST_SUPPORTED_VERSION {
            Ok(ConsumerProtocolSubscriptionData::HIGHEST_SUPPORTED_VERSION)
        } else {
            Ok(version)
        }
    }

    /// Validates an assignment version, clamping to the highest supported
    /// version if newer.
    ///
    /// Mirrors `ConsumerProtocol.checkAssignmentVersion`.
    fn check_assignment_version(version: i16) -> Result<i16, Error> {
        if version < ConsumerProtocolAssignmentData::LOWEST_SUPPORTED_VERSION {
            Err(Error::serialization(format!("Unsupported assignment version: {version}")))
        } else if version > ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION {
            Ok(ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION)
        } else {
            Ok(version)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    /// Java's `catch (BufferUnderflowException e)` in every `deserialize*`
    /// method is a NARROW type filter: only a genuine end-of-buffer is
    /// relabelled "Buffer underflow while parsing consumer protocol's <part>".
    ///
    /// A truncated header IS an underflow, so it gets the relabel — with the
    /// original as a separate cause, matching Java's
    /// `new SchemaException(message, e)` (so `message()` carries no `": ..."`
    /// suffix).
    #[test]
    fn genuine_underflow_is_relabelled_with_the_cause_attached() {
        // One byte where a 2-byte version header is required.
        let mut accessor = ByteBufferAccessor::from_bytes(vec![0u8]);
        let err = ConsumerProtocol::deserialize_version(&mut accessor)
            .expect_err("a 1-byte buffer cannot yield a 2-byte version");

        assert_eq!(
            "Buffer underflow while parsing consumer protocol's header",
            err.message(),
            "Java's message is exact, with the cause carried separately"
        );
        // Java raises `SchemaException` here.
        assert!(
            matches!(err, Error::Serialization(_)),
            "a serialization error maps here: {err:?}"
        );
        assert!(err.is_kafka_error(), "a serialization error is a Kafka error: {err:?}");
        // Java passes `e` as the cause rather than interpolating it.
        assert!(
            std::error::Error::source(&err).is_some(),
            "the underlying io error must be the cause: {err:?}"
        );
    }

    /// The other half of the narrow filter: a decode fault that is NOT a
    /// `BufferUnderflowException` escapes Java's clause and surfaces with its
    /// own message. Relabelling it "Buffer underflow" would be an incorrect
    /// diagnosis — the concrete case the audit flagged is an admin
    /// `DescribeConsumerGroups` decoding a classic member whose assignment
    /// carries a negative array length.
    #[test]
    fn non_underflow_decode_fault_keeps_its_own_message() {
        // Version 0 header, then a negative topic-array length (-1 as i32).
        // A negative length is `InvalidData`, not `UnexpectedEof`.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0i16.to_be_bytes()); // version 0
        bytes.extend_from_slice(&(-1i32).to_be_bytes()); // negative array length

        let err =
            ConsumerProtocol::deserialize_assignment(&bytes).expect_err("a negative array length must fail to decode");

        assert!(
            !err.message().contains("Buffer underflow"),
            "a negative array length is not an underflow — it must not be relabelled as one, got: {}",
            err.message()
        );
        assert!(
            err.message().contains("Negative array length"),
            "the real fault must be reported on its own terms, got: {}",
            err.message()
        );
    }

    /// Round-trips an assignment through serialize/deserialize at the highest
    /// version — this is the exact path the admin classic-group describe handler
    /// exercises.
    #[test]
    fn assignment_round_trip() {
        let partitions = vec![tp("foo", 0), tp("foo", 1), tp("bar", 2)];
        let assignment = Assignment::with_partitions(partitions.clone());
        let bytes = ConsumerProtocol::serialize_assignment(&assignment).unwrap();

        let decoded = ConsumerProtocol::deserialize_assignment(&bytes).unwrap();
        let mut decoded_partitions = decoded.partitions().to_vec();
        decoded_partitions.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
        let mut expected = partitions;
        expected.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
        assert_eq!(decoded_partitions, expected);
        assert!(decoded.user_data().is_none());
    }

    /// Round-trips an assignment carrying user data.
    #[test]
    fn assignment_round_trip_with_user_data() {
        let assignment = Assignment::new(vec![tp("t", 3)], Some(vec![1, 2, 3, 4]));
        let bytes = ConsumerProtocol::serialize_assignment(&assignment).unwrap();
        let decoded = ConsumerProtocol::deserialize_assignment(&bytes).unwrap();
        assert_eq!(decoded.partitions(), &[tp("t", 3)]);
        assert_eq!(decoded.user_data(), Some(&[1, 2, 3, 4][..]));
    }

    /// An empty assignment decodes to no partitions.
    #[test]
    fn empty_assignment_round_trip() {
        let assignment = Assignment::with_partitions(Vec::new());
        let bytes = ConsumerProtocol::serialize_assignment(&assignment).unwrap();
        let decoded = ConsumerProtocol::deserialize_assignment(&bytes).unwrap();
        assert!(decoded.partitions().is_empty());
    }

    /// Round-trips a subscription including owned partitions and generation.
    #[test]
    fn subscription_round_trip() {
        let subscription = Subscription::new(
            vec!["b".to_string(), "a".to_string()],
            None,
            vec![tp("a", 0), tp("a", 1)],
            7,
            Some("rack-1".to_string()),
        );
        let bytes = ConsumerProtocol::serialize_subscription(&subscription).unwrap();
        let decoded = ConsumerProtocol::deserialize_subscription(&bytes).unwrap();
        // Topics are sorted on serialization.
        assert_eq!(decoded.topics(), &["a".to_string(), "b".to_string()]);
        assert_eq!(decoded.owned_partitions(), &[tp("a", 0), tp("a", 1)]);
        assert_eq!(decoded.generation_id(), Some(7));
        assert_eq!(decoded.rack_id(), Some("rack-1"));
    }

    /// A version below the lowest supported version is rejected.
    #[test]
    fn deserialize_assignment_rejects_low_version() {
        let err =
            ConsumerProtocol::deserialize_assignment_versioned(&mut ByteBufferAccessor::from_bytes(Vec::new()), -1)
                .expect_err("negative version must be rejected");
        assert!(
            err.message().contains("Unsupported assignment version: -1"),
            "got: {}",
            err.message()
        );
    }

    /// A version above the highest supported version is clamped and parsed with
    /// the current format (mirrors Java's forward-compat behavior).
    #[test]
    fn serialize_assignment_clamps_high_version() {
        let assignment = Assignment::with_partitions(vec![tp("t", 0)]);
        // Version 99 is clamped to HIGHEST_SUPPORTED_VERSION on both ends.
        let bytes = ConsumerProtocol::serialize_assignment_versioned(&assignment, 99).unwrap();
        let decoded = ConsumerProtocol::deserialize_assignment(&bytes).unwrap();
        assert_eq!(decoded.partitions(), &[tp("t", 0)]);
    }

    /// Round-trips the raw generated assignment struct through
    /// `serialize_assignment_data` / `deserialize_consumer_protocol_assignment`.
    #[test]
    fn consumer_protocol_assignment_data_round_trip() {
        // Serialize a normal assignment, then decode it as the raw data struct.
        let assignment = Assignment::with_partitions(vec![tp("foo", 0), tp("foo", 1)]);
        let bytes = ConsumerProtocol::serialize_assignment(&assignment).unwrap();

        let data = ConsumerProtocol::deserialize_consumer_protocol_assignment(&bytes).unwrap();
        assert_eq!(data.assigned_partitions.len(), 1);
        assert_eq!(data.assigned_partitions[0].topic, "foo");
        assert_eq!(data.assigned_partitions[0].partitions, vec![0, 1]);

        // Re-serialize the raw data struct and confirm it decodes back to the
        // same partitions via the high-level entry point.
        let reserialized = ConsumerProtocol::serialize_assignment_data(
            data,
            ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION,
        )
        .unwrap();
        let decoded = ConsumerProtocol::deserialize_assignment(&reserialized).unwrap();
        let mut partitions = decoded.partitions().to_vec();
        partitions.sort_by(|a, b| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
        assert_eq!(partitions, vec![tp("foo", 0), tp("foo", 1)]);
    }

    /// Round-trips the raw generated subscription struct through
    /// `serialize_subscription` / `deserialize_consumer_protocol_subscription`.
    #[test]
    fn consumer_protocol_subscription_data_round_trip() {
        let subscription = Subscription::new(vec!["b".to_string(), "a".to_string()], None, vec![tp("a", 0)], 3, None);
        let bytes = ConsumerProtocol::serialize_subscription(&subscription).unwrap();

        let data = ConsumerProtocol::deserialize_consumer_protocol_subscription(&bytes).unwrap();
        // Topics are sorted on serialization.
        assert_eq!(data.topics, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(data.generation_id, 3);
    }

    /// Java's `static { }` initializer refuses to load `ConsumerProtocol` when
    /// the subscription and assignment schemas disagree on either version bound
    /// (`ConsumerProtocol.java:47-58`). The `const` block above this module is
    /// the build-time half of that; this test pins the two messages, which a
    /// `const` panic cannot express, and states why the check matters:
    /// `check_subscription_version` and `check_assignment_version` clamp to
    /// their OWN schema's highest version, so divergent bounds would silently
    /// serialise a subscription and deserialise an assignment at different wire
    /// versions.
    #[test]
    fn test_subscription_and_assignment_schema_versions_match() {
        assert_eq!(
            ConsumerProtocolSubscriptionData::LOWEST_SUPPORTED_VERSION,
            ConsumerProtocolAssignmentData::LOWEST_SUPPORTED_VERSION,
            "Subscription and Assignment schemas must have the same lowest version"
        );
        assert_eq!(
            ConsumerProtocolSubscriptionData::HIGHEST_SUPPORTED_VERSION,
            ConsumerProtocolAssignmentData::HIGHEST_SUPPORTED_VERSION,
            "Subscription and Assignment schemas must have the same highest version"
        );
    }
}
