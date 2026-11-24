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
use crate::common::record::internal::DefaultRecord;
use bytes::Bytes;
use std::sync::{Arc, OnceLock};

/// A mutable ordered collection of [`RecordHeader`] objects.
///
/// Note that multiple headers may have the same key. The order of headers
/// is preserved in the order they were added.
///
/// Once [`set_read_only()`](RecordHeaders::set_read_only) is called, all
/// mutating operations will return an error.
#[derive(Clone)]
#[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders")]
// the only `Headers` implementation, returned by ConsumerRecord / ProducerRecord `headers()`
#[doc(alias = "public-in-rust")]
pub struct RecordHeaders {
    /// Java's `List<Header> headers`. Unset is the empty list, or the list of
    /// `wire` not read yet, and allocates nothing, which keeps a record
    /// without headers, or whose headers nobody reads, allocation-free. The
    /// list is shared: `Clone` bumps a reference count instead of copying the
    /// headers, so a record's headers can be lent to a C serde that answers
    /// later without a copy, and a write copies only while another clone holds
    /// the list (`Arc::make_mut`).
    headers: OnceLock<Arc<Vec<RecordHeader>>>,
    /// The headers of a fetched record as the record holds them, read into
    /// `headers` the first time they are read or written. Java reads them
    /// into its list when it reads the record (`DefaultRecord.readHeaders`);
    /// reading them on demand spares a record whose headers nobody reads the
    /// list.
    wire: Option<WireHeaders>,
    is_read_only: bool,
}

/// The header section of a record, still encoded
/// (`[HeaderKeyLength HeaderKey HeaderValueLength HeaderValue]*`): a slice of
/// the buffer the record was read from.
#[derive(Clone)]
struct WireHeaders {
    bytes: Bytes,
    num_headers: i32,
}

impl WireHeaders {
    /// The headers of the section, each a pair of slices of it.
    fn read(&self) -> Vec<RecordHeader> {
        // `RecordHeaders::from_wire` requires a section the record parser accepted, which it
        // does only after reading every header of it, so reading it again cannot fail.
        DefaultRecord::read_headers(&self.bytes, self.num_headers)
            .expect("header section validated when the record was parsed")
    }
}

impl RecordHeaders {
    /// Create an empty `RecordHeaders`.
    #[doc(alias = "org.apache.kafka.common.header.internals.RecordHeaders#RecordHeaders")]
    pub fn new() -> Self {
        Self { headers: OnceLock::new(), wire: None, is_read_only: false }
    }

    /// Create `RecordHeaders` over the header section of a record: a slice of
    /// the buffer the record was read from. Nothing is read or copied until
    /// the headers are first read or written, and each header then holds
    /// slices of `headers_bytes`, which it keeps alive.
    ///
    /// `headers_bytes` must be a section of `num_headers` headers that the
    /// record parser accepted (`DefaultRecordRef::headers_bytes`).
    pub(crate) fn from_wire(headers_bytes: Bytes, num_headers: i32) -> Self {
        let wire = (num_headers > 0).then_some(WireHeaders { bytes: headers_bytes, num_headers });
        Self { headers: OnceLock::new(), wire, is_read_only: false }
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
        let list = OnceLock::new();
        if !headers.is_empty() {
            // The lock was created above, so nothing else has set it.
            let _ = list.set(Arc::new(headers));
        }
        Self { headers: list, wire: None, is_read_only: false }
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
        Self { headers: other.headers.clone(), wire: other.wire.clone(), is_read_only: false }
    }

    /// The headers, in insertion order.
    fn as_slice(&self) -> &[RecordHeader] {
        match &self.wire {
            Some(wire) => self.headers.get_or_init(|| Arc::new(wire.read())),
            None => self.headers.get().map_or(&[], |headers| headers.as_slice()),
        }
    }

    /// The list to write to, created on the first write and copied first when
    /// a clone still shares it.
    fn as_vec_mut(&mut self) -> &mut Vec<RecordHeader> {
        // From here on the list is the only copy of the headers: the record's own are read
        // into it first, unless a read already did.
        let wire = self.wire.take();
        self.headers
            .get_or_init(|| Arc::new(wire.map_or_else(Vec::new, |wire| wire.read())));
        Arc::make_mut(self.headers.get_mut().expect("the list was set just above"))
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

impl std::fmt::Debug for RecordHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordHeaders")
            .field("headers", &self.as_slice())
            .field("is_read_only", &self.is_read_only)
            .finish()
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

    // ── Headers of a fetched record, read on demand ─────────────────────────

    /// The header section of a record with the headers `"key"` = `"value"`,
    /// `"key2"` with no value and `"key"` = `"value2"`, and their count.
    fn wire_section() -> (Bytes, i32) {
        let headers = [
            RecordHeader::new("key".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("key2".to_string(), None),
            RecordHeader::new("key".to_string(), Some(b"value2".to_vec())),
        ];
        let mut record = Vec::new();
        DefaultRecord::write_to(&mut record, 0, 0, None, None, &headers).unwrap();
        let record = Bytes::from(record);
        let (record_ref, _) = DefaultRecord::read_ref_from_buffer(&record, 0, 0, 0, None).unwrap();
        (record.slice_ref(record_ref.headers_bytes()), record_ref.num_headers())
    }

    /// The same headers as [`wire_section`], built from owned parts.
    fn eager_headers() -> RecordHeaders {
        RecordHeaders::with_header_iter([
            RecordHeader::new("key".to_string(), Some(b"value".to_vec())),
            RecordHeader::new("key2".to_string(), None),
            RecordHeader::new("key".to_string(), Some(b"value2".to_vec())),
        ])
    }

    /// Headers over a record's header section are the headers of that
    /// section, in order, whichever method reads them.
    #[test]
    fn test_from_wire_reads_the_headers_of_the_section() {
        let (section, num_headers) = wire_section();
        let headers = RecordHeaders::from_wire(section, num_headers);

        assert_eq!(3, get_count(&headers));
        assert_header("key", "value", &headers.to_array()[0]);
        assert_eq!("key2", headers.to_array()[1].key());
        assert_eq!(None, headers.to_array()[1].value());
        assert_header("key", "value2", &headers.to_array()[2]);
        assert_header("key", "value2", headers.last_header("key").unwrap());
        assert_eq!(2, headers.headers("key").len());
        assert_eq!(3, headers.iter().count());

        let eager = eager_headers();
        assert_eq!(eager, headers);
        assert_eq!(eager.to_string(), headers.to_string());
        assert_eq!(format!("{eager:?}"), format!("{headers:?}"));
    }

    /// Nothing is read when the headers are created, nor when they are
    /// cloned: the list is built by the first read, and the headers it holds
    /// are slices of the section.
    #[test]
    fn test_from_wire_reads_nothing_until_the_first_access() {
        let (section, num_headers) = wire_section();
        let headers = RecordHeaders::from_wire(section.clone(), num_headers);
        assert!(headers.headers.get().is_none());

        let clone = headers.clone();
        let copy = RecordHeaders::with_record_headers(&headers);
        assert!(headers.headers.get().is_none());
        assert!(clone.headers.get().is_none());
        assert!(copy.headers.get().is_none());

        let section_range = section.as_ptr_range();
        for header in headers.to_array() {
            let key = header.key().as_bytes().as_ptr_range();
            assert!(section_range.start <= key.start && key.end <= section_range.end);
            if let Some(value) = header.value() {
                let value = value.as_ptr_range();
                assert!(section_range.start <= value.start && value.end <= section_range.end);
            }
        }
        assert!(headers.headers.get().is_some());
        // Each clone reads the section for itself.
        assert!(clone.headers.get().is_none());
        assert_eq!(headers, clone);
        assert_eq!(headers, copy);
    }

    /// A clone made after the first read shares the list that read built.
    #[test]
    fn test_from_wire_clone_after_the_first_read_shares_the_list() {
        let (section, num_headers) = wire_section();
        let headers = RecordHeaders::from_wire(section, num_headers);
        let first = headers.to_array().as_ptr();

        let clone = headers.clone();

        assert_eq!(first, clone.to_array().as_ptr());
    }

    /// A write reads the section first, so the record's headers come before
    /// the added one, whether or not they were read before; a clone is not
    /// affected by it.
    #[test]
    fn test_from_wire_add_and_remove() {
        let (section, num_headers) = wire_section();

        // Written before any read.
        let mut headers = RecordHeaders::from_wire(section.clone(), num_headers);
        let untouched = headers.clone();
        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();
        assert_eq!(4, get_count(&headers));
        assert_header("key", "value", &headers.to_array()[0]);
        assert_header("key3", "value3", &headers.to_array()[3]);
        assert_eq!(3, get_count(&untouched));
        assert_eq!(eager_headers(), untouched);

        // Written after a read.
        let mut headers = RecordHeaders::from_wire(section.clone(), num_headers);
        assert_eq!(3, get_count(&headers));
        let untouched = headers.clone();
        headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap();
        assert_eq!(4, get_count(&headers));
        assert_header("key3", "value3", &headers.to_array()[3]);
        assert_eq!(3, get_count(&untouched));

        // Removed before any read.
        let mut headers = RecordHeaders::from_wire(section, num_headers);
        headers.remove("key").unwrap();
        assert_eq!(1, get_count(&headers));
        assert_eq!("key2", headers.to_array()[0].key());
        assert!(headers.last_header("key").is_none());
    }

    /// Read-only headers over a section refuse a write and can still be read.
    #[test]
    fn test_from_wire_read_only() {
        let (section, num_headers) = wire_section();
        let mut headers = RecordHeaders::from_wire(section, num_headers);
        headers.set_read_only();

        let error = headers
            .add_with_header(RecordHeader::new("key3".to_string(), Some(b"value3".to_vec())))
            .unwrap_err();
        assert_eq!("LocalIllegalStateError: RecordHeaders has been closed.", error.to_string());
        let error = headers.remove("key").unwrap_err();
        assert_eq!("LocalIllegalStateError: RecordHeaders has been closed.", error.to_string());

        assert_eq!(3, get_count(&headers));
        assert_eq!(eager_headers(), headers);
    }

    /// A record without headers has an empty list and no section to keep.
    #[test]
    fn test_from_wire_without_headers() {
        let headers = RecordHeaders::from_wire(Bytes::new(), 0);

        assert!(headers.wire.is_none());
        assert_eq!(0, get_count(&headers));
        assert_eq!(RecordHeaders::new(), headers);
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
