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

//! A response header in the Kafka protocol.
//!
//! Corresponds to `org.apache.kafka.common.requests.ResponseHeader`.

use std::fmt;
use std::io;

use crate::common::protocol::ByteBufferAccessor;
use crate::common::protocol::Message;
use crate::common::protocol::ObjectSerializationCache;
use crate::common::protocol::Readable;
use crate::response_header_data::ResponseHeaderData;

/// Sentinel value indicating that the cached size has not been computed yet.
const SIZE_NOT_INITIALIZED: i32 = -1;

/// A response header in the Kafka protocol.
///
/// Wraps the generated [`ResponseHeaderData`] and provides convenience methods
/// for serialization, size computation, and parsing.
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    data: ResponseHeaderData,
    header_version: i16,
    size: i32,
}

impl ResponseHeader {
    /// Creates a new `ResponseHeader` with the given correlation id and header version.
    pub fn new_correlation_id(correlation_id: i32, header_version: i16) -> Self {
        let mut data = ResponseHeaderData::new();
        data.set_correlation_id(correlation_id);
        Self { data, header_version, size: SIZE_NOT_INITIALIZED }
    }

    /// Creates a new `ResponseHeader` from existing data and a header version.
    pub fn new_data(data: ResponseHeaderData, header_version: i16) -> Self {
        Self { data, header_version, size: SIZE_NOT_INITIALIZED }
    }

    /// Returns the correlation id of this response.
    pub fn correlation_id(&self) -> i32 {
        self.data.correlation_id
    }

    /// Returns the header version.
    pub fn header_version(&self) -> i16 {
        self.header_version
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ResponseHeaderData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub fn data_mut(&mut self) -> &mut ResponseHeaderData {
        &mut self.data
    }

    /// Calculates the size of this header in bytes using the given serialization cache.
    ///
    /// This method recalculates the size on each invocation. Prefer [`size`](Self::size)
    /// unless you need to pair this call with a subsequent [`write`](Self::write) using
    /// the same cache.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation fails.
    pub fn size_with_cache(&mut self, cache: &mut ObjectSerializationCache) -> io::Result<i32> {
        let s = Message::size(&self.data, cache, self.header_version)?;
        self.size = s;
        Ok(s)
    }

    /// Returns the size of this header in bytes.
    ///
    /// The result is cached after the first invocation.
    ///
    /// # Errors
    ///
    /// Returns an error if size calculation fails.
    pub fn size(&mut self) -> io::Result<i32> {
        if self.size == SIZE_NOT_INITIALIZED {
            let mut cache = ObjectSerializationCache::new();
            self.size_with_cache(&mut cache)?;
        }
        Ok(self.size)
    }

    /// Writes this header to the given buffer using the provided serialization cache.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    pub fn write(&mut self, buffer: &mut ByteBufferAccessor, cache: &ObjectSerializationCache) -> io::Result<()> {
        Message::write(&mut self.data, buffer, cache, self.header_version)
    }

    /// Parses a `ResponseHeader` from the given buffer.
    ///
    /// The header size is computed from the number of bytes consumed during parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(buffer: &mut dyn Readable, header_version: i16) -> io::Result<Self> {
        // Track consumed bytes via `remaining()` (on the `Readable` trait)
        // rather than `position()` (a concrete accessor method), so the header
        // can be parsed from any reader — notably the zero-copy `BytesReader`
        // used on the receive path (§27).
        let start_remaining = buffer.remaining();
        let data = ResponseHeaderData::read(buffer, header_version)?;
        let consumed = start_remaining - buffer.remaining();
        Ok(Self { data, header_version, size: consumed as i32 })
    }
}

impl PartialEq for ResponseHeader {
    fn eq(&self, other: &Self) -> bool {
        self.header_version == other.header_version && self.data == other.data
    }
}

impl Eq for ResponseHeader {}

impl std::hash::Hash for ResponseHeader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data.hash(state);
        self.header_version.hash(state);
    }
}

impl fmt::Display for ResponseHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResponseHeader(correlationId={}, headerVersion={})",
            self.data.correlation_id, self.header_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Basic roundtrip test for ResponseHeader: create, serialize, parse, compare.
    #[test]
    fn test_response_header_roundtrip_v0() {
        let mut header = ResponseHeader::new_correlation_id(42, 0);
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache).unwrap();
        assert_eq!(size, 4); // correlation_id is 4 bytes, v0 has no tagged fields

        let mut buf = ByteBufferAccessor::new(size as usize);
        header.write(&mut buf, &cache).unwrap();
        buf.flip();

        let parsed = ResponseHeader::parse(&mut buf, 0).unwrap();
        assert_eq!(header, parsed);
        assert_eq!(parsed.correlation_id(), 42);
    }

    /// Roundtrip test for flexible header version (v1, which includes tagged fields).
    #[test]
    fn test_response_header_roundtrip_v1() {
        let mut header = ResponseHeader::new_correlation_id(123, 1);
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache).unwrap();
        // correlation_id (4 bytes) + tagged fields count varint (1 byte for 0)
        assert_eq!(size, 5);

        let mut buf = ByteBufferAccessor::new(size as usize);
        header.write(&mut buf, &cache).unwrap();
        buf.flip();

        let parsed = ResponseHeader::parse(&mut buf, 1).unwrap();
        assert_eq!(header, parsed);
        assert_eq!(parsed.correlation_id(), 123);
        assert_eq!(parsed.header_version(), 1);
    }

    #[test]
    fn test_response_header_display() {
        let header = ResponseHeader::new_correlation_id(99, 1);
        let display = format!("{}", header);
        assert!(display.contains("correlationId=99"));
        assert!(display.contains("headerVersion=1"));
    }

    /// Tests that the cached size method returns the same value as the computed size.
    #[test]
    fn test_response_header_size_caching() {
        let mut header = ResponseHeader::new_correlation_id(42, 1);
        let size1 = header.size().unwrap();
        let size2 = header.size().unwrap();
        assert_eq!(size1, size2);
    }
}
