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
// package. The translation rules keep the `internals` Rust module `pub(crate)`, so
// we re-export the public types at the `common::header` level to make them
// reachable from external code (matching Java's effective visibility).
use crate::common::LocalIllegalStateError;

pub use internals::{RecordHeader, RecordHeaders};

/// A header is a key-value pair.
#[doc(alias = "org.apache.kafka.common.header.Header")]
pub trait Header {
    /// Returns the key of the header.
    ///
    /// # Returns
    ///
    /// the header's key; must not be `None`.
    #[doc(alias = "org.apache.kafka.common.header.Header#key")]
    fn key(&self) -> &str;

    /// Returns the value of the header.
    ///
    /// # Returns
    ///
    /// the header's value; may be `None`.
    #[doc(alias = "org.apache.kafka.common.header.Header#value")]
    fn value(&self) -> Option<&[u8]>;
}

/// A mutable ordered collection of [`Header`] objects. Note that multiple headers may have the same
/// key.
///
/// The order of headers is preserved in the order they were added.
#[doc(alias = "org.apache.kafka.common.header.Headers")]
pub trait Headers {
    /// Adds a header (key inside), to the end, returning if the operation succeeded.
    ///
    /// # Arguments
    ///
    /// - `header` - the Header to be added.
    ///
    /// # Errors
    ///
    /// - [`Error::is_local_illegal_state_error`](crate::common::Error::is_local_illegal_state_error)
    ///   if headers are in a read-only state.
    #[doc(alias = "org.apache.kafka.common.header.Headers#add")]
    fn add_with_header(&mut self, header: RecordHeader) -> Result<(), LocalIllegalStateError>;

    /// Creates and adds a header, to the end, returning if the operation succeeded.
    ///
    /// # Arguments
    ///
    /// - `key` - of the header to be added.
    /// - `value` - of the header to be added; may be `None`.
    ///
    /// # Errors
    ///
    /// - [`Error::is_local_illegal_state_error`](crate::common::Error::is_local_illegal_state_error)
    ///   if headers are in a read-only state.
    ///
    /// # Translation notes
    ///
    /// The key and value are borrowed and copied into the new header.
    #[doc(alias = "org.apache.kafka.common.header.Headers#add")]
    fn add_with_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), LocalIllegalStateError>;

    /// Removes all headers for the given key returning if the operation succeeded,
    /// while preserving the insertion order of the remaining headers.
    ///
    /// # Arguments
    ///
    /// - `key` - to remove all headers for.
    ///
    /// # Errors
    ///
    /// - [`Error::is_local_illegal_state_error`](crate::common::Error::is_local_illegal_state_error)
    ///   if headers are in a read-only state.
    #[doc(alias = "org.apache.kafka.common.header.Headers#remove")]
    fn remove(&mut self, key: &str) -> Result<(), LocalIllegalStateError>;

    /// Returns just one (the very last) header for the given key, if present.
    ///
    /// # Arguments
    ///
    /// - `key` - to get the last header for.
    ///
    /// # Returns
    ///
    /// this last header matching the given key, returns `None` if not present.
    #[doc(alias = "org.apache.kafka.common.header.Headers#lastHeader")]
    fn last_header(&self, key: &str) -> Option<&RecordHeader>;

    /// Returns all headers for the given key, in the order they were added in, if present.
    ///
    /// # Arguments
    ///
    /// - `key` - to return the headers for.
    ///
    /// # Returns
    ///
    /// all headers for the given key, in the order they were added in, if NO headers are present an
    /// empty list is returned.
    #[doc(alias = "org.apache.kafka.common.header.Headers#headers")]
    fn headers(&self, key: &str) -> Vec<&RecordHeader>;

    /// Returns all headers as a slice, in the order they were added in.
    ///
    /// # Returns
    ///
    /// the headers, if NO headers are present an empty slice is returned.
    #[doc(alias = "org.apache.kafka.common.header.Headers#toArray")]
    fn to_array(&self) -> &[RecordHeader];

    /// Returns an iterator over the headers.
    // Java's Iterable.iterator, a JDK interface Headers extends
    #[doc(alias = "rust-only")]
    fn iter(&self) -> std::slice::Iter<'_, RecordHeader>;
}
