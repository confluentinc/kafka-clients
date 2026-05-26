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

//! Translation of `org.apache.kafka.clients.StaleMetadataException`.
//!
//! In Java this is an internal `Exception` subclass — `extends
//! InvalidMetadataException` — used as a way to trigger a metadata update
//! before retrying another operation. We expose it as a tiny constructor
//! shim that produces [`KafkaError::StaleMetadata`], so the unified error
//! type stays the canonical signalling channel while preserving the Java
//! class-name for log / diagnostic parity (`StaleMetadataException`).
//!
//! Note: this is not a public API. Mirror of the same comment on the Java
//! source.

use crate::common::errors::KafkaError;

/// Convenience constructor for the [`KafkaError::StaleMetadata`] variant.
/// Mirrors the two Java constructors (`StaleMetadataException()` and
/// `StaleMetadataException(String)`).
pub struct StaleMetadataError;

impl StaleMetadataError {
    /// Equivalent to Java's no-arg `new StaleMetadataException()`.
    ///
    /// Returns a [`KafkaError`] rather than `Self` because the unified
    /// error type is the canonical signalling channel; this is a
    /// constructor shim that preserves the Java class-name.
    pub fn empty() -> KafkaError {
        KafkaError::StaleMetadata(String::new())
    }

    /// Equivalent to Java's `new StaleMetadataException(String message)`.
    pub fn with_message(message: impl Into<String>) -> KafkaError {
        KafkaError::StaleMetadata(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_constructor() {
        let err = StaleMetadataError::empty();
        assert!(matches!(err, KafkaError::StaleMetadata(_)));
        // Empty message — Display falls back to the class name.
        assert_eq!(err.to_string(), "StaleMetadataException");
    }

    #[test]
    fn with_message_preserves_text() {
        let err = StaleMetadataError::with_message("missing broker list");
        assert_eq!(err.message(), "missing broker list");
        assert_eq!(err.to_string(), "StaleMetadataException: missing broker list");
    }

    #[test]
    fn is_retriable_like_invalid_metadata_subclass() {
        // Java: `InvalidMetadataException` extends `RefreshRetriableException`,
        // so `StaleMetadataException` is retriable.
        assert!(StaleMetadataError::empty().is_retriable());
    }

    #[test]
    fn java_class_name_matches_source() {
        assert_eq!(StaleMetadataError::empty().java_class_name(), "StaleMetadataException");
    }
}
