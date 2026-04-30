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

//! Unified `KafkaError` translation of `org.apache.kafka.common.errors.*`
//! and `org.apache.kafka.common.protocol.Errors`.
//!
//! In Java the error hierarchy is split across many `*Exception` classes that
//! all extend `KafkaException`. In Rust we collapse them into a single enum
//! whose variants mirror the producer-relevant Java types. Three classification
//! helpers preserve the contract every caller relies on:
//!
//! * [`KafkaError::is_retriable`] — true iff the Java type extends
//!   `RetriableException` (or one of its subclasses, e.g. `InvalidMetadataException`).
//! * [`KafkaError::is_fatal`] — true iff the producer must shut down rather than
//!   retry. This includes authentication / authorization failures and the
//!   `ApplicationRecoverableException` family.
//! * [`KafkaError::txn_requires_abort`] — true iff the in-flight transaction
//!   must be aborted (we keep the helper for API parity but never set it on
//!   producer-only errors at this milestone since transactions are out of scope).
//!
//! Each variant also carries an [`KafkaError::code`] matching the on-wire
//! Kafka protocol error codes from `org.apache.kafka.common.protocol.Errors`.
//! Client-side / non-protocol errors use negative codes following librdkafka
//! conventions (`RD_KAFKA_RESP_ERR__*`).

// Code-mapping decisions:
//
// * Server-side (protocol) errors keep the wire `code()` value defined in
//   `org.apache.kafka.common.protocol.Errors` (range `-1..=133`). The values
//   are stable as part of Kafka's wire contract.
// * Client-side errors (no Java protocol code) use librdkafka-style negative
//   codes (see the `ERR_CODE_*` constants below).
//
// We deliberately do not paper over the Java/librdkafka naming differences —
// every variant has both a `code()` for protocol parity and a Rust name.

use std::fmt;

/// Generic client-side timeout (librdkafka `_TIMED_OUT`).
pub const ERR_CODE_TIMED_OUT: i16 = -185;
/// Network transport failure (librdkafka `_TRANSPORT`).
pub const ERR_CODE_TRANSPORT: i16 = -195;
/// Serialization failure (librdkafka `_VALUE_SERIALIZATION` / `_KEY_SERIALIZATION`).
pub const ERR_CODE_SERIALIZATION: i16 = -159;
/// Configuration / invalid argument (librdkafka `_INVALID_ARG`).
pub const ERR_CODE_CONFIG: i16 = -186;
/// Interrupted (librdkafka `_INTR`).
pub const ERR_CODE_INTERRUPTED: i16 = -188;
/// Record too large from client-side check (librdkafka `_MSG_SIZE_TOO_LARGE`).
pub const ERR_CODE_RECORD_TOO_LARGE_CLIENT: i16 = -190;
/// Buffer pool exhausted (librdkafka `_QUEUE_FULL`).
pub const ERR_CODE_BUFFER_EXHAUSTED: i16 = -191;
/// Unknown / catch-all error.
pub const ERR_CODE_UNKNOWN: i16 = -1;

/// Translation of `org.apache.kafka.common.errors.*` and the producer-relevant
/// subset of `org.apache.kafka.common.protocol.Errors`.
///
/// Every variant captures its Java `getMessage()` text in the inner `String`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaError {
    // ----- Catch-all / wrapper (`KafkaException`) -----
    /// Generic `KafkaException` — used when the Java code throws a bare
    /// `KafkaException` not bound to a specific protocol code.
    Generic(String),

    // ----- Generic `ApiException` -----
    /// `org.apache.kafka.common.errors.ApiException` (default catch-all on the
    /// public protocol path).
    Api(String),

    /// `UnknownServerException` — wire code `-1`. Returned by the broker for
    /// unrecognized errors.
    UnknownServer(String),

    // ----- Retriable (extends `RetriableException`) -----
    /// `org.apache.kafka.common.errors.TimeoutException` — wire code `7`.
    Timeout(String),
    /// `org.apache.kafka.common.errors.DisconnectException` — client-side, no
    /// wire code; we map to librdkafka `_TRANSPORT`.
    Disconnect(String),
    /// `org.apache.kafka.common.errors.CorruptRecordException` — wire code `2`.
    CorruptRecord(String),

    // Subclasses of `InvalidMetadataException` (which is a
    // `RefreshRetriableException` -> retriable).
    /// `org.apache.kafka.common.errors.NetworkException` — wire code `13`.
    /// In Java extends `InvalidMetadataException`.
    Network(String),
    /// `org.apache.kafka.common.errors.LeaderNotAvailableException` — wire code `5`.
    LeaderNotAvailable(String),
    /// `org.apache.kafka.common.errors.NotLeaderOrFollowerException` — wire code `6`.
    NotLeaderOrFollower(String),
    /// `org.apache.kafka.common.errors.UnknownTopicOrPartitionException` —
    /// wire code `3`.
    UnknownTopicOrPartition(String),

    // ----- Non-retriable `ApiException` subclasses -----
    /// `org.apache.kafka.common.errors.RecordTooLargeException` — wire code `10`.
    RecordTooLarge(String),
    /// Client-side record-too-large check (no wire code; librdkafka
    /// `_MSG_SIZE_TOO_LARGE`).
    RecordTooLargeClient(String),
    /// `org.apache.kafka.common.errors.InvalidTopicException` — wire code `17`.
    InvalidTopic(String),
    /// `org.apache.kafka.common.errors.InvalidRecordException` (also exposed as
    /// `org.apache.kafka.common.InvalidRecordException`) — wire code `87`.
    InvalidRecord(String),
    /// `org.apache.kafka.common.errors.SerializationException` — client-side.
    Serialization(String),
    /// `org.apache.kafka.common.errors.OutOfOrderSequenceException` — wire code `45`.
    OutOfOrderSequence(String),
    /// `org.apache.kafka.common.errors.UnknownProducerIdException` — wire code `59`.
    /// Subclass of `OutOfOrderSequenceException`.
    UnknownProducerId(String),
    /// `org.apache.kafka.common.errors.UnsupportedVersionException` — wire code `35`.
    UnsupportedVersion(String),
    /// `org.apache.kafka.common.errors.InvalidRequestException` — wire code `42`.
    /// Returned when a request cannot be parsed or fails validation prior to
    /// reaching the API handler.
    InvalidRequest(String),

    // ----- Authentication / Authorization (treated as fatal). -----
    /// `org.apache.kafka.common.errors.AuthenticationException`. No wire code
    /// (client-side); `SASL_AUTHENTICATION_FAILED` wire code `58` is its
    /// closest server-side counterpart.
    Authentication(String),
    /// `org.apache.kafka.common.errors.AuthorizationException`. No wire code
    /// directly; `TOPIC_AUTHORIZATION_FAILED` (29), `GROUP_AUTHORIZATION_FAILED`
    /// (30), and `CLUSTER_AUTHORIZATION_FAILED` (31) are subclasses.
    Authorization(String),
    /// `org.apache.kafka.common.errors.TopicAuthorizationException` — wire code `29`.
    TopicAuthorization(String),
    /// `org.apache.kafka.common.errors.ClusterAuthorizationException` — wire code `31`.
    ClusterAuthorization(String),

    // ----- Idempotence / transactions (out of scope this milestone, but the
    // ----- variants are present so producer config validation can reject them).
    /// `org.apache.kafka.common.errors.ProducerFencedException` — wire code `90`.
    ProducerFenced(String),
    /// `org.apache.kafka.common.errors.InvalidProducerEpochException` — wire code `47`.
    InvalidProducerEpoch(String),
    /// `org.apache.kafka.common.errors.TransactionAbortedException` — client-side.
    TransactionAborted(String),

    // ----- Client-side resource / control flow -----
    /// `org.apache.kafka.common.errors.InterruptException`.
    Interrupt(String),
    /// `org.apache.kafka.clients.producer.BufferExhaustedException`.
    /// Translated here for convenience (lives in the producer package in Java).
    BufferExhausted(String),
    /// `org.apache.kafka.common.config.ConfigException`.
    Config(String),
    /// Argument validation failure (Java `IllegalArgumentException`). Carried
    /// here so we have a single `Result` type without a stdlib panic.
    IllegalArgument(String),
}

impl KafkaError {
    /// True iff this error type is retriable. Mirrors the
    /// `RetriableException` family in Java.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            KafkaError::Timeout(_)
                | KafkaError::Disconnect(_)
                | KafkaError::CorruptRecord(_)
                | KafkaError::Network(_)
                | KafkaError::LeaderNotAvailable(_)
                | KafkaError::NotLeaderOrFollower(_)
                | KafkaError::UnknownTopicOrPartition(_)
        )
    }

    /// True iff this error is fatal in the **non-idempotent producer** — the
    /// producer cannot recover and the client should be closed.
    ///
    /// This corresponds to:
    /// * `AuthenticationException`, `AuthorizationException` and subclasses
    /// * `ApplicationRecoverableException` family (`ProducerFencedException`,
    ///   `InvalidProducerEpochException`)
    /// * `UnsupportedVersionException` (the broker speaks a newer protocol)
    ///
    /// Note: Java's `Sender.completeBatch` additionally treats
    /// `OutOfOrderSequenceException` and `UnknownProducerIdException` as fatal
    /// when idempotence is enabled (the `failBatch(... isFatalIdempotentException ...)`
    /// path). When the idempotent / transactional producer paths are wired up
    /// (Milestone 6+), this method (or a separate `is_fatal_idempotent`) must
    /// be extended to include those two variants.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            KafkaError::Authentication(_)
                | KafkaError::Authorization(_)
                | KafkaError::TopicAuthorization(_)
                | KafkaError::ClusterAuthorization(_)
                | KafkaError::ProducerFenced(_)
                | KafkaError::InvalidProducerEpoch(_)
                | KafkaError::UnsupportedVersion(_)
        )
    }

    /// True iff an in-flight transaction must be aborted on receipt of this
    /// error. Always `false` at this milestone since transactions are out of
    /// scope; kept for API parity with librdkafka's `txn_requires_abort()`.
    pub fn txn_requires_abort(&self) -> bool {
        matches!(self, KafkaError::TransactionAborted(_))
    }

    /// On-wire / client error code. Server-side codes match Kafka's
    /// `Errors.code()`; client-side codes follow librdkafka conventions.
    pub fn code(&self) -> i16 {
        match self {
            KafkaError::Generic(_) => ERR_CODE_UNKNOWN,
            KafkaError::Api(_) => ERR_CODE_UNKNOWN,
            KafkaError::UnknownServer(_) => -1,

            KafkaError::Timeout(_) => 7,
            KafkaError::Disconnect(_) => ERR_CODE_TRANSPORT,
            KafkaError::CorruptRecord(_) => 2,
            KafkaError::Network(_) => 13,
            KafkaError::LeaderNotAvailable(_) => 5,
            KafkaError::NotLeaderOrFollower(_) => 6,
            KafkaError::UnknownTopicOrPartition(_) => 3,

            KafkaError::RecordTooLarge(_) => 10,
            KafkaError::RecordTooLargeClient(_) => ERR_CODE_RECORD_TOO_LARGE_CLIENT,
            KafkaError::InvalidTopic(_) => 17,
            KafkaError::InvalidRecord(_) => 87,
            KafkaError::Serialization(_) => ERR_CODE_SERIALIZATION,
            KafkaError::OutOfOrderSequence(_) => 45,
            KafkaError::UnknownProducerId(_) => 59,
            KafkaError::UnsupportedVersion(_) => 35,
            KafkaError::InvalidRequest(_) => 42,

            KafkaError::Authentication(_) => 58,
            KafkaError::Authorization(_) => 29,
            KafkaError::TopicAuthorization(_) => 29,
            KafkaError::ClusterAuthorization(_) => 31,

            KafkaError::ProducerFenced(_) => 90,
            KafkaError::InvalidProducerEpoch(_) => 47,
            KafkaError::TransactionAborted(_) => ERR_CODE_UNKNOWN,

            KafkaError::Interrupt(_) => ERR_CODE_INTERRUPTED,
            KafkaError::BufferExhausted(_) => ERR_CODE_BUFFER_EXHAUSTED,
            KafkaError::Config(_) => ERR_CODE_CONFIG,
            KafkaError::IllegalArgument(_) => ERR_CODE_CONFIG,
        }
    }

    /// The Java exception class name for this error. Used for diagnostic
    /// messages and log parity with the Java client.
    pub fn java_class_name(&self) -> &'static str {
        match self {
            KafkaError::Generic(_) => "KafkaException",
            KafkaError::Api(_) => "ApiException",
            KafkaError::UnknownServer(_) => "UnknownServerException",
            KafkaError::Timeout(_) => "TimeoutException",
            KafkaError::Disconnect(_) => "DisconnectException",
            KafkaError::CorruptRecord(_) => "CorruptRecordException",
            KafkaError::Network(_) => "NetworkException",
            KafkaError::LeaderNotAvailable(_) => "LeaderNotAvailableException",
            KafkaError::NotLeaderOrFollower(_) => "NotLeaderOrFollowerException",
            KafkaError::UnknownTopicOrPartition(_) => "UnknownTopicOrPartitionException",
            KafkaError::RecordTooLarge(_) => "RecordTooLargeException",
            KafkaError::RecordTooLargeClient(_) => "RecordTooLargeException",
            KafkaError::InvalidTopic(_) => "InvalidTopicException",
            KafkaError::InvalidRecord(_) => "InvalidRecordException",
            KafkaError::Serialization(_) => "SerializationException",
            KafkaError::OutOfOrderSequence(_) => "OutOfOrderSequenceException",
            KafkaError::UnknownProducerId(_) => "UnknownProducerIdException",
            KafkaError::UnsupportedVersion(_) => "UnsupportedVersionException",
            KafkaError::InvalidRequest(_) => "InvalidRequestException",
            KafkaError::Authentication(_) => "AuthenticationException",
            KafkaError::Authorization(_) => "AuthorizationException",
            KafkaError::TopicAuthorization(_) => "TopicAuthorizationException",
            KafkaError::ClusterAuthorization(_) => "ClusterAuthorizationException",
            KafkaError::ProducerFenced(_) => "ProducerFencedException",
            KafkaError::InvalidProducerEpoch(_) => "InvalidProducerEpochException",
            KafkaError::TransactionAborted(_) => "TransactionAbortedException",
            KafkaError::Interrupt(_) => "InterruptException",
            KafkaError::BufferExhausted(_) => "BufferExhaustedException",
            KafkaError::Config(_) => "ConfigException",
            KafkaError::IllegalArgument(_) => "IllegalArgumentException",
        }
    }

    /// Returns the message stored on this error, equivalent to Java's
    /// `Throwable#getMessage()`.
    pub fn message(&self) -> &str {
        match self {
            KafkaError::Generic(m)
            | KafkaError::Api(m)
            | KafkaError::UnknownServer(m)
            | KafkaError::Timeout(m)
            | KafkaError::Disconnect(m)
            | KafkaError::CorruptRecord(m)
            | KafkaError::Network(m)
            | KafkaError::LeaderNotAvailable(m)
            | KafkaError::NotLeaderOrFollower(m)
            | KafkaError::UnknownTopicOrPartition(m)
            | KafkaError::RecordTooLarge(m)
            | KafkaError::RecordTooLargeClient(m)
            | KafkaError::InvalidTopic(m)
            | KafkaError::InvalidRecord(m)
            | KafkaError::Serialization(m)
            | KafkaError::OutOfOrderSequence(m)
            | KafkaError::UnknownProducerId(m)
            | KafkaError::UnsupportedVersion(m)
            | KafkaError::InvalidRequest(m)
            | KafkaError::Authentication(m)
            | KafkaError::Authorization(m)
            | KafkaError::TopicAuthorization(m)
            | KafkaError::ClusterAuthorization(m)
            | KafkaError::ProducerFenced(m)
            | KafkaError::InvalidProducerEpoch(m)
            | KafkaError::TransactionAborted(m)
            | KafkaError::Interrupt(m)
            | KafkaError::BufferExhausted(m)
            | KafkaError::Config(m)
            | KafkaError::IllegalArgument(m) => m.as_str(),
        }
    }

    /// Construct an error from an on-wire error code and an optional message.
    /// Mirrors `Errors.forCode(short)` followed by `error.exception(message)`.
    /// Unknown codes map to [`KafkaError::UnknownServer`].
    pub fn from_code(code: i16, message: Option<&str>) -> Option<Self> {
        let m = || message.unwrap_or("").to_owned();
        Some(match code {
            0 => return None, // NONE — no error.
            -1 => KafkaError::UnknownServer(m()),
            2 => KafkaError::CorruptRecord(m()),
            3 => KafkaError::UnknownTopicOrPartition(m()),
            5 => KafkaError::LeaderNotAvailable(m()),
            6 => KafkaError::NotLeaderOrFollower(m()),
            7 => KafkaError::Timeout(m()),
            10 => KafkaError::RecordTooLarge(m()),
            13 => KafkaError::Network(m()),
            17 => KafkaError::InvalidTopic(m()),
            29 => KafkaError::TopicAuthorization(m()),
            31 => KafkaError::ClusterAuthorization(m()),
            35 => KafkaError::UnsupportedVersion(m()),
            42 => KafkaError::InvalidRequest(m()),
            45 => KafkaError::OutOfOrderSequence(m()),
            47 => KafkaError::InvalidProducerEpoch(m()),
            58 => KafkaError::Authentication(m()),
            59 => KafkaError::UnknownProducerId(m()),
            87 => KafkaError::InvalidRecord(m()),
            90 => KafkaError::ProducerFenced(m()),
            // Codes outside the producer-relevant set (or unknown) collapse
            // to `UnknownServerException`, matching Java's `Errors.forCode`.
            _ => KafkaError::UnknownServer(m()),
        })
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = self.message();
        if msg.is_empty() {
            write!(f, "{}", self.java_class_name())
        } else {
            write!(f, "{}: {}", self.java_class_name(), msg)
        }
    }
}

impl std::error::Error for KafkaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retriable_set_matches_java_hierarchy() {
        // Direct RetriableException subclasses
        assert!(KafkaError::Timeout("t".into()).is_retriable());
        assert!(KafkaError::Disconnect("t".into()).is_retriable());
        assert!(KafkaError::CorruptRecord("t".into()).is_retriable());
        // RefreshRetriableException -> InvalidMetadataException subclasses
        assert!(KafkaError::Network("t".into()).is_retriable());
        assert!(KafkaError::LeaderNotAvailable("t".into()).is_retriable());
        assert!(KafkaError::NotLeaderOrFollower("t".into()).is_retriable());
        assert!(KafkaError::UnknownTopicOrPartition("t".into()).is_retriable());

        // Non-retriable
        assert!(!KafkaError::RecordTooLarge("t".into()).is_retriable());
        assert!(!KafkaError::InvalidTopic("t".into()).is_retriable());
        assert!(!KafkaError::Serialization("t".into()).is_retriable());
        assert!(!KafkaError::Authentication("t".into()).is_retriable());
        assert!(!KafkaError::Config("t".into()).is_retriable());
    }

    #[test]
    fn fatal_set_matches_producer_contract() {
        assert!(KafkaError::Authentication("t".into()).is_fatal());
        assert!(KafkaError::Authorization("t".into()).is_fatal());
        assert!(KafkaError::TopicAuthorization("t".into()).is_fatal());
        assert!(KafkaError::ClusterAuthorization("t".into()).is_fatal());
        assert!(KafkaError::ProducerFenced("t".into()).is_fatal());
        assert!(KafkaError::InvalidProducerEpoch("t".into()).is_fatal());
        assert!(KafkaError::UnsupportedVersion("t".into()).is_fatal());

        assert!(!KafkaError::Timeout("t".into()).is_fatal());
        assert!(!KafkaError::Network("t".into()).is_fatal());
        assert!(!KafkaError::Config("t".into()).is_fatal());
    }

    #[test]
    fn protocol_codes_match_java_errors_enum() {
        assert_eq!(KafkaError::CorruptRecord("".into()).code(), 2);
        assert_eq!(KafkaError::UnknownTopicOrPartition("".into()).code(), 3);
        assert_eq!(KafkaError::LeaderNotAvailable("".into()).code(), 5);
        assert_eq!(KafkaError::NotLeaderOrFollower("".into()).code(), 6);
        assert_eq!(KafkaError::Timeout("".into()).code(), 7);
        assert_eq!(KafkaError::RecordTooLarge("".into()).code(), 10);
        assert_eq!(KafkaError::Network("".into()).code(), 13);
        assert_eq!(KafkaError::InvalidTopic("".into()).code(), 17);
        assert_eq!(KafkaError::TopicAuthorization("".into()).code(), 29);
        assert_eq!(KafkaError::ClusterAuthorization("".into()).code(), 31);
        assert_eq!(KafkaError::UnsupportedVersion("".into()).code(), 35);
        assert_eq!(KafkaError::InvalidRequest("".into()).code(), 42);
        assert_eq!(KafkaError::OutOfOrderSequence("".into()).code(), 45);
        assert_eq!(KafkaError::InvalidProducerEpoch("".into()).code(), 47);
        assert_eq!(KafkaError::Authentication("".into()).code(), 58);
        assert_eq!(KafkaError::UnknownProducerId("".into()).code(), 59);
        assert_eq!(KafkaError::InvalidRecord("".into()).code(), 87);
        assert_eq!(KafkaError::ProducerFenced("".into()).code(), 90);
        assert_eq!(KafkaError::UnknownServer("".into()).code(), -1);
    }

    #[test]
    fn from_code_round_trip() {
        // NONE
        assert!(KafkaError::from_code(0, None).is_none());
        // Producer-relevant codes
        let cases = [
            (-1, "UnknownServerException"),
            (2, "CorruptRecordException"),
            (3, "UnknownTopicOrPartitionException"),
            (5, "LeaderNotAvailableException"),
            (6, "NotLeaderOrFollowerException"),
            (7, "TimeoutException"),
            (10, "RecordTooLargeException"),
            (13, "NetworkException"),
            (17, "InvalidTopicException"),
            (29, "TopicAuthorizationException"),
            (31, "ClusterAuthorizationException"),
            (35, "UnsupportedVersionException"),
            (42, "InvalidRequestException"),
            (45, "OutOfOrderSequenceException"),
            (47, "InvalidProducerEpochException"),
            (58, "AuthenticationException"),
            (59, "UnknownProducerIdException"),
            (87, "InvalidRecordException"),
            (90, "ProducerFencedException"),
        ];
        for (code, name) in cases {
            let err = KafkaError::from_code(code, Some("msg")).expect("non-zero code");
            assert_eq!(err.code(), code);
            assert_eq!(err.java_class_name(), name);
            assert_eq!(err.message(), "msg");
        }
        // Unknown code -> UnknownServerException, per `Errors.forCode`.
        let unknown = KafkaError::from_code(9999, Some("oh")).unwrap();
        assert!(matches!(unknown, KafkaError::UnknownServer(_)));
    }

    #[test]
    fn display_includes_class_name_and_message() {
        let err = KafkaError::Timeout("expired".into());
        assert_eq!(err.to_string(), "TimeoutException: expired");

        let err = KafkaError::Timeout(String::new());
        assert_eq!(err.to_string(), "TimeoutException");
    }
}
