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

//! Error types for the Kafka client.
//!
//! Corresponds to the exception hierarchy in org.apache.kafka.common.errors.

use std::fmt;

/// Error codes matching the Java Kafka exception hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Corresponds to TimeoutException.
    TimedOut,
    /// Corresponds to RecordTooLargeException.
    RecordTooLarge,
    /// Corresponds to SerializationException.
    Serialization,
    /// Corresponds to BufferExhaustedException.
    BufferExhausted,
    /// Corresponds to InterruptException.
    Interrupted,
    /// Corresponds to InvalidTopicException.
    InvalidTopic,
    /// Corresponds to UnknownTopicOrPartitionException.
    UnknownTopicOrPartition,
    /// Corresponds to NotLeaderOrFollowerException.
    NotLeaderOrFollower,
    /// Corresponds to CorruptRecordException.
    CorruptRecord,
    /// Corresponds to NetworkException.
    Network,
    /// Corresponds to TopicAuthorizationException.
    TopicAuthorization,
    /// Corresponds to ProducerFencedException.
    ProducerFenced,
    /// Corresponds to InvalidProducerEpochException.
    InvalidProducerEpoch,
    /// Corresponds to TransactionalIdAuthorizationException.
    TransactionalIdAuthorization,
    /// Corresponds to OutOfOrderSequenceException.
    OutOfOrderSequence,
    /// Corresponds to InvalidTxnStateException.
    InvalidTxnState,
    /// Corresponds to KafkaException (generic).
    Unexpected,
    /// IO error.
    Io,
}

/// The primary error type for the Kafka client library.
///
/// Modeled after librdkafka's error type with `is_retriable`, `is_fatal`,
/// and `txn_requires_abort` classification methods.
#[derive(Debug)]
pub struct KafkaError {
    code: ErrorCode,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl KafkaError {
    /// Create a new error with the given code and message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        KafkaError {
            code,
            message: message.into(),
            source: None,
        }
    }

    /// Create a new error with a source cause.
    pub fn with_source(
        code: ErrorCode,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        KafkaError {
            code,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Returns the error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns true if this error is retriable (transient failure).
    pub fn is_retriable(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::TimedOut
                | ErrorCode::NotLeaderOrFollower
                | ErrorCode::Network
                | ErrorCode::UnknownTopicOrPartition
        )
    }

    /// Returns true if this error is fatal (producer must be closed).
    pub fn is_fatal(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::ProducerFenced
                | ErrorCode::InvalidProducerEpoch
                | ErrorCode::TransactionalIdAuthorization
        )
    }

    /// Returns true if the current transaction must be aborted.
    pub fn txn_requires_abort(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::OutOfOrderSequence
                | ErrorCode::InvalidTxnState
                | ErrorCode::ProducerFenced
        )
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for KafkaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|s| s.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl From<std::io::Error> for KafkaError {
    fn from(err: std::io::Error) -> Self {
        KafkaError::with_source(ErrorCode::Io, err.to_string(), err)
    }
}

/// Library-wide Result alias.
pub type Result<T> = std::result::Result<T, KafkaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retriable_errors() {
        assert!(KafkaError::new(ErrorCode::TimedOut, "timeout").is_retriable());
        assert!(KafkaError::new(ErrorCode::Network, "disconnect").is_retriable());
        assert!(!KafkaError::new(ErrorCode::RecordTooLarge, "too big").is_retriable());
    }

    #[test]
    fn test_fatal_errors() {
        assert!(KafkaError::new(ErrorCode::ProducerFenced, "fenced").is_fatal());
        assert!(!KafkaError::new(ErrorCode::TimedOut, "timeout").is_fatal());
    }

    #[test]
    fn test_txn_requires_abort() {
        assert!(KafkaError::new(ErrorCode::OutOfOrderSequence, "ooo").txn_requires_abort());
        assert!(KafkaError::new(ErrorCode::ProducerFenced, "fenced").txn_requires_abort());
        assert!(!KafkaError::new(ErrorCode::TimedOut, "timeout").txn_requires_abort());
    }

    #[test]
    fn test_display() {
        let err = KafkaError::new(ErrorCode::TimedOut, "request timed out");
        assert_eq!(format!("{err}"), "TimedOut: request timed out");
    }

    #[test]
    fn test_from_io_error() {
        use std::error::Error;
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken pipe");
        let kafka_err: KafkaError = io_err.into();
        assert_eq!(kafka_err.code(), ErrorCode::Io);
        assert!(kafka_err.source().is_some());
    }
}
