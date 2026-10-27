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

//! A mutable ordered collection of record headers.
//!
//! Corresponds to Java's `org.apache.kafka.common.header.internals.RecordHeaders`.

use crate::common::LocalIllegalStateError;
use crate::common::header::RecordHeader;
use crate::common::header::{Header, Headers};
use std::sync::Arc;

/// A mutable ordered collection of [`RecordHeader`] objects.
///
/// Note that multiple headers may have the same key. The order of headers
/// is preserved in the order they were added.
///
/// Once [`set_read_only()`](RecordHeaders::set_read_only) is called, all
/// mutating operations will return an error.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders")]
// the only `Headers` implementation, returned by ConsumerRecord / ProducerRecord `headers()`
#[doc(alias = "public-in-rust")]
pub struct RecordHeaders {
    /// Java's `List<Header> headers`. `None` is the empty list and allocates
    /// nothing, which keeps a record without headers allocation-free. The list
    /// is shared: `Clone` bumps a reference count instead of copying the
    /// headers, so a record's headers can be lent to a C serde that answers
    /// later without a copy, and a write copies only while another clone holds
    /// the list (`Arc::make_mut`).
    headers: Option<Arc<Vec<RecordHeader>>>,
    is_read_only: bool,
}

impl RecordHeaders {
    /// Create an empty `RecordHeaders`.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#RecordHeaders")]
    pub fn new() -> Self {
        Self { headers: None, is_read_only: false }
    }

    /// Create `RecordHeaders` holding a copy of the given headers, in order.
    ///
    /// # Arguments
    ///
    /// - `headers` - the headers to copy
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#RecordHeaders")]
    pub fn with_header_slice(headers: &[RecordHeader]) -> Self {
        Self::with_header_iter(headers.iter().cloned())
    }

    /// Create `RecordHeaders` holding the given headers, in order.
    ///
    /// # Arguments
    ///
    /// - `headers` - the headers to hold
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#RecordHeaders")]
    pub fn with_header_iter(headers: impl IntoIterator<Item = RecordHeader>) -> Self {
        let headers: Vec<RecordHeader> = headers.into_iter().collect();
        Self { headers: (!headers.is_empty()).then(|| Arc::new(headers)), is_read_only: false }
    }

    /// Create `RecordHeaders` by copying from another `RecordHeaders`, as Java's
    /// `RecordHeaders(Iterable<Header>)` does given a `RecordHeaders`.
    ///
    /// The new instance is writable regardless of the source's read-only state.
    /// It shares the source's list until either of them is written.
    ///
    /// # Arguments
    ///
    /// - `other` - the headers to copy
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#RecordHeaders")]
    pub fn with_record_headers(other: &RecordHeaders) -> Self {
        Self { headers: other.headers.clone(), is_read_only: false }
    }

    /// The headers, in insertion order.
    fn as_slice(&self) -> &[RecordHeader] {
        self.headers.as_deref().map_or(&[], Vec::as_slice)
    }

    /// The list to write to, created on the first write and copied first when
    /// a clone still shares it.
    fn as_vec_mut(&mut self) -> &mut Vec<RecordHeader> {
        Arc::make_mut(self.headers.get_or_insert_with(Default::default))
    }

    /// Set the headers to read-only mode.
    ///
    /// After calling this, all mutating operations will return an error.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#setReadOnly")]
    pub fn set_read_only(&mut self) {
        self.is_read_only = true;
    }

    /// Returns whether the headers are in read-only mode.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#isReadOnly")]
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Check whether writing is allowed.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#canWrite")]
    fn can_write(&self) -> Result<(), LocalIllegalStateError> {
        if self.is_read_only {
            Err(LocalIllegalStateError::new("RecordHeaders has been closed."))
        } else {
            Ok(())
        }
    }
}

impl Default for RecordHeaders {
    fn default() -> Self {
        Self::new()
    }
}

impl Headers for RecordHeaders {
    fn add_with_header(&mut self, header: RecordHeader) -> Result<(), LocalIllegalStateError> {
        self.can_write()?;
        self.as_vec_mut().push(header);
        Ok(())
    }

    fn add_with_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), LocalIllegalStateError> {
        self.add_with_header(RecordHeader::new(key.to_owned(), value.map(|v| v.to_vec())))
    }

    fn remove(&mut self, key: &str) -> Result<(), LocalIllegalStateError> {
        self.can_write()?;
        if self.as_slice().iter().any(|h| h.key() == key) {
            self.as_vec_mut().retain(|h| h.key() != key);
        }
        Ok(())
    }

    fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        self.as_slice().iter().rev().find(|h| h.key() == key)
    }

    fn headers(&self, key: &str) -> Vec<&RecordHeader> {
        self.as_slice().iter().filter(|h| h.key() == key).collect()
    }

    fn to_array(&self) -> &[RecordHeader] {
        self.as_slice()
    }

    fn iter(&self) -> std::slice::Iter<'_, RecordHeader> {
        self.as_slice().iter()
    }
}

impl PartialEq for RecordHeaders {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RecordHeaders {}

impl std::hash::Hash for RecordHeaders {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl std::fmt::Display for RecordHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordHeaders(headers = {:?}, isReadOnly = {})",
            self.as_slice(),
            self.is_read_only
        )
    }
}

impl<'a> IntoIterator for &'a RecordHeaders {
    type Item = &'a RecordHeader;
    type IntoIter = std::slice::Iter<'a, RecordHeader>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl std::iter::FromIterator<RecordHeader> for RecordHeaders {
    fn from_iter<I: IntoIterator<Item = RecordHeader>>(iter: I) -> Self {
        Self::with_header_iter(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::header::Header;

    fn assert_header(key: &str, value: &str, actual: &RecordHeader) {
        assert_eq!(key, actual.key());
        assert_eq!(
            Some(value.as_bytes()),
            actual.value(),
            "Header value mismatch for key '{}'",
            key
        );
    }

    fn get_count(headers: &RecordHeaders) -> usize {
        headers.to_array().len()
    }

    /// Corresponds to Java's testAdd.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testAdd")]
    fn test_add() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        let header = headers.iter().next().unwrap();
        assert_header("key", "value", header);

        headers
            .add_with_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();

        assert_header("key2", "value2", headers.last_header("key2").unwrap());
        assert_eq!(2, get_count(&headers));
    }

    /// Corresponds to Java's testAddHeadersPreserveOrder.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testAddHeadersPreserveOrder")]
    fn test_add_headers_preserve_order() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        let headers_arr = headers.to_array();
        assert_header("key", "value", &headers_arr[0]);
        assert_header("key2", "value2", &headers_arr[1]);
        assert_header("key3", "value3", &headers_arr[2]);

        assert_eq!(3, get_count(&headers));
    }

    /// Corresponds to Java's testRemove.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testRemove")]
    fn test_remove() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        assert!(headers.iter().next().is_some());

        headers.remove("key").unwrap();

        assert!(headers.iter().next().is_none());
    }

    /// Corresponds to Java's testPreserveOrderAfterRemove.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testPreserveOrderAfterRemove")]
    fn test_preserve_order_after_remove() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        headers.remove("key").unwrap();
        let headers_arr = headers.to_array();
        assert_header("key2", "value2", &headers_arr[0]);
        assert_header("key3", "value3", &headers_arr[1]);
        assert_eq!(2, get_count(&headers));

        headers
            .add_with_header(RecordHeader::new("key4".to_string(), Some(b"value4".to_vec())))
            .unwrap();
        headers.remove("key3").unwrap();
        let headers_arr = headers.to_array();
        assert_header("key2", "value2", &headers_arr[0]);
        assert_header("key4", "value4", &headers_arr[1]);
        assert_eq!(2, get_count(&headers));
    }

    /// Corresponds to Java's testAddRemoveInterleaved.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testAddRemoveInterleaved")]
    fn test_add_remove_interleaved() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();

        assert!(headers.iter().next().is_some());

        headers.remove("key").unwrap();

        assert_eq!(1, get_count(&headers));

        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        assert!(headers.last_header("key").is_none());

        assert_header("key2", "value2", headers.last_header("key2").unwrap());

        assert_header("key3", "value3", headers.last_header("key3").unwrap());

        assert_eq!(2, get_count(&headers));

        headers.remove("key2").unwrap();

        assert!(headers.last_header("key").is_none());

        assert!(headers.last_header("key2").is_none());

        assert_header("key3", "value3", headers.last_header("key3").unwrap());

        assert_eq!(1, get_count(&headers));

        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value4".to_vec())))
            .unwrap();

        assert_header("key3", "value4", headers.last_header("key3").unwrap());

        assert_eq!(2, get_count(&headers));

        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"valueNew".to_vec())))
            .unwrap();

        assert_eq!(3, get_count(&headers));

        assert_header("key", "valueNew", headers.last_header("key").unwrap());

        headers.remove("key3").unwrap();

        assert_eq!(1, get_count(&headers));

        assert!(headers.last_header("key2").is_none());

        headers.remove("key").unwrap();

        assert!(headers.iter().next().is_none());
    }

    /// Corresponds to Java's testLastHeader.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testLastHeader")]
    fn test_last_header() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        assert_header("key", "value3", headers.last_header("key").unwrap());
        assert_eq!(3, get_count(&headers));
    }

    /// Corresponds to Java's testReadOnly.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testReadOnly")]
    fn test_read_only() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers.set_read_only();

        // Adding should fail
        let result = headers.add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())));
        assert!(result.is_err(), "Should fail as headers are closed.");

        // Removing should fail
        let result = headers.remove("key");
        assert!(result.is_err(), "Should fail as headers are closed.");
    }

    /// Corresponds to Java's testHeaders.
    #[test]
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeadersTest#testHeaders")]
    fn test_headers() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key1".to_string(), Some(b"key1value".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_with_header(RecordHeader::new("key2".to_string(), Some(b"key2value".to_vec())))
            .unwrap();

        let key_headers = headers.headers("key");
        assert_eq!(2, key_headers.len());
        assert_header("key", "value", key_headers[0]);
        assert_header("key", "value2", key_headers[1]);

        let key1_headers = headers.headers("key1");
        assert_eq!(1, key1_headers.len());
        assert_header("key1", "key1value", key1_headers[0]);

        let key2_headers = headers.headers("key2");
        assert_eq!(1, key2_headers.len());
        assert_header("key2", "key2value", key2_headers[0]);
    }

    /// Corresponds to Java's testNew.
    #[test]
    fn test_new_from_existing() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers.set_read_only();

        let mut new_headers = RecordHeaders::with_record_headers(&headers);
        new_headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
            .unwrap();

        // Ensure existing headers are not modified
        assert_header("key", "value", headers.last_header("key").unwrap());
        assert_eq!(1, get_count(&headers));

        // Ensure new headers are modified
        assert_header("key", "value2", new_headers.last_header("key").unwrap());
        assert_eq!(2, get_count(&new_headers));
    }

    /// Corresponds to Java's testHeadersIteratorRemove.
    /// In Rust, iterators over slices do not support removal,
    /// so this is inherently safe by the type system.
    #[test]
    fn test_headers_iterator_is_read_only() {
        let mut headers = RecordHeaders::new();
        headers
            .add_with_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        // headers returns Vec<&RecordHeader> which is inherently read-only
        let key_headers = headers.headers("key");
        assert_eq!(1, key_headers.len());
    }
}
