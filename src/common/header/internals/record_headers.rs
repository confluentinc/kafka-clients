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

//! Translation of `org.apache.kafka.common.header.internals.RecordHeaders`.

use std::fmt;

use crate::common::header::Header;
use crate::common::header::Headers;
use crate::common::header::internals::record_header::RecordHeader;

/// Errors returned by mutating methods on [`RecordHeaders`]. Mirrors Java's
/// `IllegalStateException` for read-only headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordHeadersError {
    /// Mutating a `RecordHeaders` after `set_read_only()` has been called.
    /// Mirrors `IllegalStateException("RecordHeaders has been closed.")`.
    ReadOnly,
}

impl fmt::Display for RecordHeadersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordHeadersError::ReadOnly => f.write_str("RecordHeaders has been closed."),
        }
    }
}

impl std::error::Error for RecordHeadersError {}

/// Mutable, ordered, multi-map of record headers. Mirrors Java's
/// `RecordHeaders`.
///
/// The underlying storage is a `Vec<RecordHeader>` because:
///
/// 1. Header lists are typically small (under a dozen entries) so a linear
///    scan is faster than a hash lookup.
/// 2. Insertion order is part of the public contract (see Java's
///    `Headers#add` rustdoc).
/// 3. We need to support multiple entries per key (`headers(String)` returns
///    every match in order).
#[derive(Default, Clone)]
pub struct RecordHeaders {
    headers: Vec<RecordHeader>,
    is_read_only: bool,
}

impl RecordHeaders {
    /// Construct an empty `RecordHeaders`. Mirrors `new RecordHeaders()`.
    pub fn new() -> Self {
        RecordHeaders::default()
    }

    /// Construct from an existing iterable of headers. Mirrors
    /// `new RecordHeaders(Iterable<Header>)` and `new RecordHeaders(Header[])`.
    /// Also available via the standard [`FromIterator`] trait.
    pub fn from_headers<I: IntoIterator<Item = RecordHeader>>(iter: I) -> Self {
        RecordHeaders { headers: iter.into_iter().collect(), is_read_only: false }
    }

    /// Mark the headers as read-only. After this any mutation returns
    /// [`RecordHeadersError::ReadOnly`]. Mirrors `setReadOnly()`.
    pub fn set_read_only(&mut self) {
        self.is_read_only = true;
    }

    /// True iff [`RecordHeaders::set_read_only`] has been called.
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    /// Iterate every header in insertion order. Mirrors `iterator()`.
    pub fn iter(&self) -> std::slice::Iter<'_, RecordHeader> {
        self.headers.iter()
    }

    /// Number of headers in the collection. Convenience getter (Java callers
    /// use `toArray().length`).
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// True iff there are no headers.
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    fn check_writable(&self) -> Result<(), RecordHeadersError> {
        if self.is_read_only {
            Err(RecordHeadersError::ReadOnly)
        } else {
            Ok(())
        }
    }
}

impl FromIterator<RecordHeader> for RecordHeaders {
    fn from_iter<I: IntoIterator<Item = RecordHeader>>(iter: I) -> Self {
        RecordHeaders::from_headers(iter)
    }
}

impl Headers for RecordHeaders {
    fn add(&mut self, header: RecordHeader) -> Result<&mut Self, RecordHeadersError> {
        self.check_writable()?;
        self.headers.push(header);
        Ok(self)
    }

    fn add_kv(&mut self, key: &str, value: Option<&[u8]>) -> Result<&mut Self, RecordHeadersError> {
        self.add(RecordHeader::new(key, value))
    }

    fn remove(&mut self, key: &str) -> Result<&mut Self, RecordHeadersError> {
        self.check_writable()?;
        self.headers.retain(|h| h.key() != key);
        Ok(self)
    }

    fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        self.headers.iter().rev().find(|h| h.key() == key)
    }

    fn headers<'a>(&'a self, key: &'a str) -> Box<dyn Iterator<Item = &'a RecordHeader> + 'a> {
        Box::new(self.headers.iter().filter(move |h| h.key() == key))
    }

    fn to_vec(&self) -> Vec<&RecordHeader> {
        self.headers.iter().collect()
    }
}

impl<'a> IntoIterator for &'a RecordHeaders {
    type Item = &'a RecordHeader;
    type IntoIter = std::slice::Iter<'a, RecordHeader>;

    fn into_iter(self) -> Self::IntoIter {
        self.headers.iter()
    }
}

impl PartialEq for RecordHeaders {
    fn eq(&self, other: &Self) -> bool {
        self.headers == other.headers
    }
}

impl Eq for RecordHeaders {}

impl fmt::Debug for RecordHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RecordHeaders(headers = {:?}, isReadOnly = {})",
            self.headers, self.is_read_only
        )
    }
}

#[cfg(test)]
mod tests {
    // Translation of `RecordHeadersTest` (the parts that exercise
    // `RecordHeaders` rather than `RecordHeader`). RecordHeader-specific
    // tests live in the `record_header.rs` module.
    //
    // SKIPPED tests:
    // * `testHeadersIteratorRemove` — Java exposes a mutable iterator that
    //   throws `UnsupportedOperationException` on `remove()`. Rust's
    //   `slice::Iter` is immutable, so this contract is enforced statically;
    //   no runtime test is needed.
    // * `shouldThrowNpeWhenAddingNullHeader` /
    //   `shouldThrowNpeWhenAddingCollectionWithNullHeader` — Rust types
    //   prevent `None`/null from being passed in the first place (the
    //   signatures take `RecordHeader` and `IntoIterator<Item=RecordHeader>`,
    //   not optional values).

    use super::*;

    fn h(key: &str, value: &[u8]) -> RecordHeader {
        RecordHeader::new(key, Some(value))
    }

    /// Java: `testAdd`.
    #[test]
    fn add_basic() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        let first = headers.iter().next().unwrap();
        assert_eq!(first.key(), "key");
        assert_eq!(first.value(), Some(b"value".as_slice()));

        headers.add(h("key2", b"value2")).unwrap();
        let last = headers.last_header("key2").unwrap();
        assert_eq!(last.key(), "key2");
        assert_eq!(last.value(), Some(b"value2".as_slice()));
        assert_eq!(headers.len(), 2);
    }

    /// Java: `testAddHeadersPreserveOrder`.
    #[test]
    fn add_preserves_order() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.add(h("key2", b"value2")).unwrap();
        headers.add(h("key3", b"value3")).unwrap();

        let arr = headers.to_vec();
        assert_eq!(arr[0].key(), "key");
        assert_eq!(arr[0].value(), Some(b"value".as_slice()));
        assert_eq!(arr[1].key(), "key2");
        assert_eq!(arr[1].value(), Some(b"value2".as_slice()));
        assert_eq!(arr[2].key(), "key3");
        assert_eq!(arr[2].value(), Some(b"value3".as_slice()));
        assert_eq!(headers.len(), 3);
    }

    /// Java: `testRemove`.
    #[test]
    fn remove_basic() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        assert!(headers.iter().next().is_some());
        headers.remove("key").unwrap();
        assert!(headers.iter().next().is_none());
    }

    /// Java: `testPreserveOrderAfterRemove`.
    #[test]
    fn remove_preserves_order() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.add(h("key2", b"value2")).unwrap();
        headers.add(h("key3", b"value3")).unwrap();

        headers.remove("key").unwrap();
        let arr = headers.to_vec();
        assert_eq!(arr[0].key(), "key2");
        assert_eq!(arr[0].value(), Some(b"value2".as_slice()));
        assert_eq!(arr[1].key(), "key3");
        assert_eq!(arr[1].value(), Some(b"value3".as_slice()));
        assert_eq!(headers.len(), 2);

        headers.add(h("key4", b"value4")).unwrap();
        headers.remove("key3").unwrap();
        let arr = headers.to_vec();
        assert_eq!(arr[0].key(), "key2");
        assert_eq!(arr[0].value(), Some(b"value2".as_slice()));
        assert_eq!(arr[1].key(), "key4");
        assert_eq!(arr[1].value(), Some(b"value4".as_slice()));
        assert_eq!(headers.len(), 2);
    }

    /// Java: `testAddRemoveInterleaved`.
    #[test]
    fn add_remove_interleaved() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.add(h("key2", b"value2")).unwrap();

        assert!(headers.iter().next().is_some());
        headers.remove("key").unwrap();
        assert_eq!(headers.len(), 1);

        headers.add(h("key3", b"value3")).unwrap();
        assert!(headers.last_header("key").is_none());
        assert_eq!(headers.last_header("key2").unwrap().value(), Some(b"value2".as_slice()));
        assert_eq!(headers.last_header("key3").unwrap().value(), Some(b"value3".as_slice()));
        assert_eq!(headers.len(), 2);

        headers.remove("key2").unwrap();
        assert!(headers.last_header("key").is_none());
        assert!(headers.last_header("key2").is_none());
        assert_eq!(headers.last_header("key3").unwrap().value(), Some(b"value3".as_slice()));
        assert_eq!(headers.len(), 1);

        headers.add(h("key3", b"value4")).unwrap();
        assert_eq!(headers.last_header("key3").unwrap().value(), Some(b"value4".as_slice()));
        assert_eq!(headers.len(), 2);

        headers.add(h("key", b"valueNew")).unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(headers.last_header("key").unwrap().value(), Some(b"valueNew".as_slice()));

        headers.remove("key3").unwrap();
        assert_eq!(headers.len(), 1);
        assert!(headers.last_header("key2").is_none());

        headers.remove("key").unwrap();
        assert!(headers.iter().next().is_none());
    }

    /// Java: `testLastHeader`.
    #[test]
    fn last_header_returns_most_recent() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.add(h("key", b"value2")).unwrap();
        headers.add(h("key", b"value3")).unwrap();

        assert_eq!(headers.last_header("key").unwrap().value(), Some(b"value3".as_slice()));
        assert_eq!(headers.len(), 3);
    }

    /// Java: `testReadOnly`.
    #[test]
    fn read_only_blocks_mutations() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.set_read_only();

        let err = headers.add(h("key", b"value")).unwrap_err();
        assert_eq!(err, RecordHeadersError::ReadOnly);

        let err = headers.remove("key").unwrap_err();
        assert_eq!(err, RecordHeadersError::ReadOnly);
    }

    /// Java: `testHeaders`.
    #[test]
    fn headers_iterator_filters_by_key() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.add(h("key1", b"key1value")).unwrap();
        headers.add(h("key", b"value2")).unwrap();
        headers.add(h("key2", b"key2value")).unwrap();

        let key_headers: Vec<_> = headers.headers("key").collect();
        assert_eq!(key_headers.len(), 2);
        assert_eq!(key_headers[0].value(), Some(b"value".as_slice()));
        assert_eq!(key_headers[1].value(), Some(b"value2".as_slice()));

        let key1_headers: Vec<_> = headers.headers("key1").collect();
        assert_eq!(key1_headers.len(), 1);
        assert_eq!(key1_headers[0].value(), Some(b"key1value".as_slice()));

        let key2_headers: Vec<_> = headers.headers("key2").collect();
        assert_eq!(key2_headers.len(), 1);
        assert_eq!(key2_headers[0].value(), Some(b"key2value".as_slice()));
    }

    /// Java: `testNew`.
    #[test]
    fn copy_construction_does_not_share_state() {
        let mut headers = RecordHeaders::new();
        headers.add(h("key", b"value")).unwrap();
        headers.set_read_only();

        let mut new_headers: RecordHeaders = headers.iter().cloned().collect();
        new_headers.add(h("key", b"value2")).unwrap();

        assert_eq!(headers.last_header("key").unwrap().value(), Some(b"value".as_slice()));
        assert_eq!(headers.len(), 1);

        assert_eq!(new_headers.last_header("key").unwrap().value(), Some(b"value2".as_slice()));
        assert_eq!(new_headers.len(), 2);
    }
}
