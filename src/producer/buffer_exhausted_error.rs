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

//! Translation of `org.apache.kafka.clients.producer.BufferExhaustedException`.
//!
//! In Java this is a `TimeoutException` subclass thrown by `BufferPool.allocate`
//! when no memory becomes available within `max.block.ms`. The Rust client
//! uses the unified [`KafkaError`] enum as its single signalling channel —
//! [`KafkaError::BufferExhausted`] is the canonical variant. This module
//! provides a constructor shim that mirrors the Java class-name in logs and
//! diagnostics.
//!
//! [`KafkaError::BufferExhausted`] inherits the retriable property from
//! `TimeoutException` in Java (verified in `is_retriable`).

use crate::common::errors::KafkaError;

/// Constructor shim for [`KafkaError::BufferExhausted`].
///
/// Mirrors `new BufferExhaustedException(String message)`.
pub struct BufferExhaustedError;

impl BufferExhaustedError {
    /// Equivalent to Java's `new BufferExhaustedException(String message)`.
    ///
    /// Returns [`KafkaError`] rather than `Self` because the unified error
    /// type is the canonical signalling channel; this shim only preserves
    /// the Java class-name for log / diagnostic parity.
    pub fn with_message(message: impl Into<String>) -> KafkaError {
        KafkaError::BufferExhausted(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_message_preserves_text() {
        let err = BufferExhaustedError::with_message("buffer too full");
        assert!(matches!(err, KafkaError::BufferExhausted(_)));
        assert_eq!(err.message(), "buffer too full");
        assert_eq!(err.to_string(), "BufferExhaustedException: buffer too full");
    }

    #[test]
    fn java_class_name_matches_source() {
        let err = BufferExhaustedError::with_message("");
        assert_eq!(err.java_class_name(), "BufferExhaustedException");
    }

    #[test]
    fn is_retriable_like_timeout_subclass() {
        // Java: `BufferExhaustedException extends TimeoutException`, which
        // extends `RetriableException`.
        assert!(BufferExhaustedError::with_message("x").is_retriable());
    }
}
