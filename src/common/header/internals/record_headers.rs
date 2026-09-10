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
use crate::common::header::internals::RecordHeader;
use crate::common::header::{Header, Headers};

/// A mutable ordered collection of [`RecordHeader`] objects.
///
/// Note that multiple headers may have the same key. The order of headers
/// is preserved in the order they were added.
///
/// Once [`set_read_only()`](RecordHeaders::set_read_only) is called, all
/// mutating operations will return an error.
///
/// Corresponds to Java's `org.apache.kafka.common.header.internals.RecordHeaders`.
#[derive(Clone, Debug)]
pub struct RecordHeaders {
    headers: Vec<RecordHeader>,
    is_read_only: bool,
}

impl RecordHeaders {
    /// Create an empty `RecordHeaders`.
    ///
    /// Translates Java's no-arg `RecordHeaders()` (`RecordHeaders.java:35`).
    /// The three Java constructors have no parameter in common, and the no-arg
    /// one matches that empty intersection, so it keeps the plain name under
    /// CLAUDE.md §2 — its two siblings are suffixed below.
    pub fn new() -> Self {
        Self { headers: Vec::new(), is_read_only: false }
    }

    /// Create `RecordHeaders` from a slice of headers.
    ///
    /// Translates Java's `RecordHeaders(Header[] headers)`
    /// (`RecordHeaders.java:39`). Java's other parameterised constructor names
    /// its parameter `headers` too, so the parameter name cannot tell them
    /// apart; per CLAUDE.md §2 the Rust parameter *type* discriminates, spelled
    /// as the caller writes it (`&[RecordHeader]` → `_header_slice`).
    ///
    /// # Panics
    ///
    /// This corresponds to the Java behavior where null entries in the array
    /// cause a NullPointerException. In Rust, the Option type prevents null
    /// headers so this is safe by construction.
    pub fn new_header_slice(headers: &[RecordHeader]) -> Self {
        Self { headers: headers.to_vec(), is_read_only: false }
    }

    /// Create `RecordHeaders` from headers.
    ///
    /// Translates Java's `RecordHeaders(Iterable<Header> headers)`
    /// (`RecordHeaders.java:43`) — see
    /// [`new_header_slice`](RecordHeaders::new_header_slice) for why the type,
    /// not the parameter name, supplies the suffix.
    pub fn new_header_iter(headers: impl IntoIterator<Item = RecordHeader>) -> Self {
        Self { headers: headers.into_iter().collect(), is_read_only: false }
    }

    /// Create `RecordHeaders` by copying from another `RecordHeaders`.
    ///
    /// The new instance is writable regardless of the source's read-only state.
    pub fn from_record_headers(other: &RecordHeaders) -> Self {
        Self { headers: other.headers.clone(), is_read_only: false }
    }

    /// Set the headers to read-only mode.
    ///
    /// After calling this, all mutating operations will return an error.
    pub fn set_read_only(&mut self) {
        self.is_read_only = true;
    }

    /// Returns whether the headers are in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Check whether writing is allowed.
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
    fn add_header(&mut self, header: RecordHeader) -> Result<(), LocalIllegalStateError> {
        self.can_write()?;
        self.headers.push(header);
        Ok(())
    }

    fn add_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), LocalIllegalStateError> {
        self.add_header(RecordHeader::new(key.to_owned(), value.map(|v| v.to_vec())))
    }

    fn remove(&mut self, key: &str) -> Result<(), LocalIllegalStateError> {
        self.can_write()?;
        self.headers.retain(|h| h.key() != key);
        Ok(())
    }

    fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        self.headers.iter().rev().find(|h| h.key() == key)
    }

    fn headers(&self, key: &str) -> Vec<&RecordHeader> {
        self.headers.iter().filter(|h| h.key() == key).collect()
    }

    fn to_array(&self) -> &[RecordHeader] {
        &self.headers
    }

    fn iter(&self) -> std::slice::Iter<'_, RecordHeader> {
        self.headers.iter()
    }
}

impl PartialEq for RecordHeaders {
    fn eq(&self, other: &Self) -> bool {
        self.headers == other.headers
    }
}

impl Eq for RecordHeaders {}

impl std::hash::Hash for RecordHeaders {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.headers.hash(state);
    }
}

impl std::fmt::Display for RecordHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordHeaders(headers = {:?}, isReadOnly = {})",
            self.headers, self.is_read_only
        )
    }
}

impl<'a> IntoIterator for &'a RecordHeaders {
    type Item = &'a RecordHeader;
    type IntoIter = std::slice::Iter<'a, RecordHeader>;

    fn into_iter(self) -> Self::IntoIter {
        self.headers.iter()
    }
}

impl std::iter::FromIterator<RecordHeader> for RecordHeaders {
    fn from_iter<I: IntoIterator<Item = RecordHeader>>(iter: I) -> Self {
        Self { headers: iter.into_iter().collect(), is_read_only: false }
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
    fn test_add() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        let header = headers.iter().next().unwrap();
        assert_header("key", "value", header);

        headers
            .add_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();

        assert_header("key2", "value2", headers.last_header("key2").unwrap());
        assert_eq!(2, get_count(&headers));
    }

    /// Corresponds to Java's testAddHeadersPreserveOrder.
    #[test]
    fn test_add_headers_preserve_order() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        let headers_arr = headers.to_array();
        assert_header("key", "value", &headers_arr[0]);
        assert_header("key2", "value2", &headers_arr[1]);
        assert_header("key3", "value3", &headers_arr[2]);

        assert_eq!(3, get_count(&headers));
    }

    /// Corresponds to Java's testRemove.
    #[test]
    fn test_remove() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        assert!(headers.iter().next().is_some());

        headers.remove("key").unwrap();

        assert!(headers.iter().next().is_none());
    }

    /// Corresponds to Java's testPreserveOrderAfterRemove.
    #[test]
    fn test_preserve_order_after_remove() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        headers.remove("key").unwrap();
        let headers_arr = headers.to_array();
        assert_header("key2", "value2", &headers_arr[0]);
        assert_header("key3", "value3", &headers_arr[1]);
        assert_eq!(2, get_count(&headers));

        headers
            .add_header(RecordHeader::new("key4".to_string(), Some(b"value4".to_vec())))
            .unwrap();
        headers.remove("key3").unwrap();
        let headers_arr = headers.to_array();
        assert_header("key2", "value2", &headers_arr[0]);
        assert_header("key4", "value4", &headers_arr[1]);
        assert_eq!(2, get_count(&headers));
    }

    /// Corresponds to Java's testAddRemoveInterleaved.
    #[test]
    fn test_add_remove_interleaved() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key2".to_string(), Some(b"value2".to_vec())))
            .unwrap();

        assert!(headers.iter().next().is_some());

        headers.remove("key").unwrap();

        assert_eq!(1, get_count(&headers));

        headers
            .add_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
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
            .add_header(RecordHeader::new("key3".to_string(), Some(b"value4".to_vec())))
            .unwrap();

        assert_header("key3", "value4", headers.last_header("key3").unwrap());

        assert_eq!(2, get_count(&headers));

        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"valueNew".to_vec())))
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
    fn test_last_header() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value3".to_vec())))
            .unwrap();

        assert_header("key", "value3", headers.last_header("key").unwrap());
        assert_eq!(3, get_count(&headers));
    }

    /// Corresponds to Java's testReadOnly.
    #[test]
    fn test_read_only() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers.set_read_only();

        // Adding should fail
        let result = headers.add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())));
        assert!(result.is_err(), "Should fail as headers are closed.");

        // Removing should fail
        let result = headers.remove("key");
        assert!(result.is_err(), "Should fail as headers are closed.");
    }

    /// Corresponds to Java's testHeaders.
    #[test]
    fn test_headers() {
        let mut headers = RecordHeaders::new();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key1".to_string(), Some(b"key1value".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
            .unwrap();
        headers
            .add_header(RecordHeader::new("key2".to_string(), Some(b"key2value".to_vec())))
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
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();
        headers.set_read_only();

        let mut new_headers = RecordHeaders::from_record_headers(&headers);
        new_headers
            .add_header(RecordHeader::new("key".to_string(), Some(b"value2".to_vec())))
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
            .add_header(RecordHeader::new("key".to_string(), Some(b"value".to_vec())))
            .unwrap();

        // headers returns Vec<&RecordHeader> which is inherently read-only
        let key_headers = headers.headers("key");
        assert_eq!(1, key_headers.len());
    }
}
