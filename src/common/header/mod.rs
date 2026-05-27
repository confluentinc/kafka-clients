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

//! Kafka record headers (org.apache.kafka.common.header).
//!
//! A header is a key-value pair attached to a Kafka record. The [`Header`] trait
//! defines the contract, while [`Headers`] is a mutable ordered collection of
//! headers.

pub(crate) mod internals;

// Re-export the canonical `RecordHeader` and `RecordHeaders` implementations
// for external users. In Java these live in
// `org.apache.kafka.common.header.internals` as `public class` declarations
// — they are part of the public API surface despite being in an `internals`
// package. CLAUDE.md §2 keeps the `internals` Rust module `pub(crate)`, so
// we re-export the public types at the `common::header` level to make them
// reachable from external code (matching Java's effective visibility).
pub use internals::{RecordHeader, RecordHeaders};

/// A header is a key-value pair.
///
/// Corresponds to Java's `org.apache.kafka.common.header.Header`.
pub trait Header {
    /// Returns the key of the header.
    ///
    /// The key must not be null (always returns a valid string reference).
    fn key(&self) -> &str;

    /// Returns the value of the header.
    ///
    /// The value may be `None` (corresponding to Java's null).
    fn value(&self) -> Option<&[u8]>;
}

/// A mutable ordered collection of [`Header`] objects.
///
/// Note that multiple headers may have the same key. The order of headers
/// is preserved in the order they were added.
///
/// Corresponds to Java's `org.apache.kafka.common.header.Headers`.
pub trait Headers {
    /// Adds a header (key inside), to the end, returning if the operation succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if headers are in a read-only state.
    fn add(&mut self, header: RecordHeader) -> Result<(), IllegalStateError>;

    /// Creates and adds a header, to the end, returning if the operation succeeded.
    ///
    /// The key and value are borrowed; the allocation is performed internally.
    ///
    /// # Errors
    ///
    /// Returns an error if headers are in a read-only state.
    fn add_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), IllegalStateError>;

    /// Removes all headers for the given key returning if the operation succeeded,
    /// while preserving the insertion order of the remaining headers.
    ///
    /// # Errors
    ///
    /// Returns an error if headers are in a read-only state.
    fn remove(&mut self, key: &str) -> Result<(), IllegalStateError>;

    /// Returns just one (the very last) header for the given key, if present.
    fn last_header(&self, key: &str) -> Option<&RecordHeader>;

    /// Returns all headers for the given key, in the order they were added in.
    fn headers_for_key(&self, key: &str) -> Vec<&RecordHeader>;

    /// Returns all headers as a slice.
    ///
    /// If no headers are present an empty slice is returned.
    fn to_array(&self) -> &[RecordHeader];

    /// Returns an iterator over the headers.
    fn iter(&self) -> std::slice::Iter<'_, RecordHeader>;
}

/// Error returned when a mutating operation is attempted on read-only headers.
///
/// Corresponds to Java's `IllegalStateException` thrown by `RecordHeaders`
/// when the collection has been set to read-only.
#[derive(Clone, Debug)]
pub struct IllegalStateError {
    message: String,
}

impl IllegalStateError {
    /// Create a new `IllegalStateError` with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for IllegalStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for IllegalStateError {}
