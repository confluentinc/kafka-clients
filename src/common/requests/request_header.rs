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

//! The header for a request in the Kafka protocol.
//!
//! Corresponds to `org.apache.kafka.common.requests.RequestHeader`.

use std::fmt;
use std::io;

use crate::common::protocol::Message;
use crate::common::protocol::ObjectSerializationCache;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Readable};
use crate::request_header_data::RequestHeaderData;

use super::ResponseHeader;

/// Sentinel value indicating that the cached size has not been computed yet.
const SIZE_NOT_INITIALIZED: i32 = -1;

/// The header for a request in the Kafka protocol.
///
/// Wraps the generated [`RequestHeaderData`] and provides convenience methods
/// for serialization, size computation, and parsing.
#[derive(Debug, Clone)]
pub struct RequestHeader {
    data: RequestHeaderData,
    header_version: i16,
    size: i32,
}

/// The parameters of Java's
/// `RequestHeader(ApiKeys, short, String, int)` (`RequestHeader.java:38`) that
/// do not fit in the derived method name.
///
/// Java's two `RequestHeader` constructors (`:38`, `:47`) share no parameter
/// name, so all four of `:38`'s reach its derived name. CLAUDE.md §2 caps that
/// at three and moves the remainder here. This struct has no Java counterpart:
/// it exists solely to satisfy that naming rule (DoD #7).
///
/// It deliberately has **no** `Default`. Java declares no `RequestHeader`
/// overload that omits `correlationId`, so there is no Java-derived default to
/// carry across, and a synthesised `0` would silently produce a header that
/// cannot be matched to its response. Construct it with [`RequestHeaderOptions::new`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RequestHeaderOptions {
    /// Java's `correlationId`.
    pub correlation_id: i32,
}

impl RequestHeaderOptions {
    /// Creates the options carrying the given correlation id.
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

impl RequestHeader {
    /// Creates a new `RequestHeader` with the given API key, version, client id, and correlation id.
    ///
    /// Corresponds to Java's `RequestHeader(ApiKeys, short, String, int)`
    /// (`RequestHeader.java:38`).
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not recognized.
    pub fn new_request_api_key_request_version_client_id_options(
        request_api_key: &ApiKeys,
        request_version: i16,
        client_id: &str,
        options: RequestHeaderOptions,
    ) -> io::Result<Self> {
        let correlation_id = options.correlation_id;
        let mut data = RequestHeaderData::new();
        data.set_request_api_key(request_api_key.id());
        data.set_request_api_version(request_version);
        data.set_client_id(Some(client_id.to_string()));
        data.set_correlation_id(correlation_id);
        let header_version = request_api_key.request_header_version(request_version);
        Ok(Self { data, header_version, size: SIZE_NOT_INITIALIZED })
    }

    /// Creates a new `RequestHeader` from existing data and a header version.
    ///
    /// Corresponds to Java's `RequestHeader(RequestHeaderData, short)`
    /// (`RequestHeader.java:47`).
    pub fn new_data_header_version(data: RequestHeaderData, header_version: i16) -> Self {
        Self { data, header_version, size: SIZE_NOT_INITIALIZED }
    }

    /// Returns the API key of this request.
    ///
    /// # Panics
    ///
    /// Panics if the stored API key id does not correspond to a known API key.
    /// This should never happen for a properly constructed `RequestHeader`.
    pub fn api_key(&self) -> &'static ApiKeys {
        ApiKeys::for_id(self.data.request_api_key).expect("RequestHeader contains an unknown API key id")
    }

    /// Returns the API version of this request.
    pub fn api_version(&self) -> i16 {
        self.data.request_api_version
    }

    /// Returns the header version.
    pub fn header_version(&self) -> i16 {
        self.header_version
    }

    /// Returns the client id string.
    ///
    /// Returns an empty string if the client id is `None`.
    pub fn client_id(&self) -> &str {
        self.data.client_id.as_deref().unwrap_or("")
    }

    /// Returns the correlation id of this request.
    pub fn correlation_id(&self) -> i32 {
        self.data.correlation_id
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &RequestHeaderData {
        &self.data
    }

    /// Returns whether the API version is within the supported range.
    pub fn is_api_version_supported(&self) -> bool {
        self.api_key().is_version_supported(self.api_version())
    }

    /// Returns whether the API version is deprecated.
    pub fn is_api_version_deprecated(&self) -> bool {
        self.api_key().is_version_deprecated(self.api_version())
    }

    /// Creates a corresponding response header with the same correlation id
    /// and the appropriate response header version.
    pub fn to_response_header(&self) -> ResponseHeader {
        ResponseHeader::new_correlation_id(
            self.data.correlation_id,
            self.api_key().response_header_version(self.api_version()),
        )
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

    /// Parses a `RequestHeader` from the given buffer.
    ///
    /// The header version is derived from the API key and API version found in the buffer.
    /// The buffer position is advanced past the header after parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The API key id is not recognized
    /// - The API key has no valid versions
    /// - The header data cannot be parsed
    pub fn parse(buffer: &mut ByteBufferAccessor) -> io::Result<Self> {
        let start_position = buffer.position();

        // Read API key and version to determine header version, then reset position.
        let api_key_id = buffer.read_short()?;
        let api_version = buffer.read_short()?;

        let api_key = ApiKeys::for_id(api_key_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("Unknown API key {api_key_id}")))?;

        // Check that the API key has valid versions before trying to get the header version
        if !api_key.has_valid_version() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Unsupported api with key {} ({}) and version {}",
                    api_key_id,
                    api_key.name(),
                    api_version
                ),
            ));
        }

        let header_version = api_key.request_header_version(api_version);

        // Reset to start position and parse the full header data
        buffer.set_position(start_position)?;
        let mut header_data = RequestHeaderData::read(buffer, header_version)?;

        // Due to a quirk in the protocol, client ID is marked as nullable.
        // However, we treat a null client ID as equivalent to an empty client ID.
        if header_data.client_id.is_none() {
            header_data.set_client_id(Some(String::new()));
        }

        // Size of header is calculated by the shift in the buffer position during parsing.
        let consumed = buffer.position().saturating_sub(start_position);

        Ok(Self { data: header_data, header_version, size: consumed as i32 })
    }
}

impl PartialEq for RequestHeader {
    fn eq(&self, other: &Self) -> bool {
        self.header_version == other.header_version && self.data == other.data
    }
}

impl Eq for RequestHeader {}

impl std::hash::Hash for RequestHeader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data.hash(state);
        self.header_version.hash(state);
    }
}

impl fmt::Display for RequestHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RequestHeader(apiKey={}, apiVersion={}, clientId={}, correlationId={}, headerVersion={})",
            self.api_key(),
            self.api_version(),
            self.client_id(),
            self.correlation_id(),
            self.header_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Writable;

    /// Helper: serializes a RequestHeader into a ByteBufferAccessor for parsing tests.
    /// Equivalent to Java's `RequestTestUtils.serializeRequestHeader`.
    fn serialize_request_header(header: &mut RequestHeader) -> io::Result<ByteBufferAccessor> {
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache)?;
        let mut buffer = ByteBufferAccessor::new(size as usize);
        header.write(&mut buffer, &cache)?;
        buffer.flip();
        Ok(buffer)
    }

    /// Translated from Java `RequestHeaderTest.testRequestHeaderV1`.
    #[test]
    fn test_request_header_v1() {
        let mut header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::FIND_COORDINATOR,
            1,
            "",
            RequestHeaderOptions::new(10),
        )
        .unwrap();
        assert_eq!(header.header_version(), 1);

        let mut buffer = serialize_request_header(&mut header).unwrap();
        assert_eq!(buffer.remaining(), 10);
        let deserialized = RequestHeader::parse(&mut buffer).unwrap();
        assert_eq!(header, deserialized);
    }

    /// Translated from Java `RequestHeaderTest.testRequestHeaderV2`.
    #[test]
    fn test_request_header_v2() {
        let mut header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::CREATE_DELEGATION_TOKEN,
            2,
            "",
            RequestHeaderOptions::new(10),
        )
        .unwrap();
        assert_eq!(header.header_version(), 2);

        let mut buffer = serialize_request_header(&mut header).unwrap();
        assert_eq!(buffer.remaining(), 11);
        let deserialized = RequestHeader::parse(&mut buffer).unwrap();
        assert_eq!(header, deserialized);
    }

    /// Translated from Java `RequestHeaderTest.parseHeaderFromBufferWithNonZeroPosition`.
    #[test]
    fn test_parse_header_from_buffer_with_non_zero_position() {
        // Create a buffer with some leading bytes to simulate a non-zero start position
        let mut full_buf = ByteBufferAccessor::new(64);
        // Write 10 bytes of padding
        for _ in 0..10 {
            full_buf.write_byte(0).unwrap();
        }

        let mut header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::FIND_COORDINATOR,
            1,
            "",
            RequestHeaderOptions::new(10),
        )
        .unwrap();
        let mut cache = ObjectSerializationCache::new();
        header.size_with_cache(&mut cache).unwrap();
        header.write(&mut full_buf, &cache).unwrap();

        let limit = full_buf.len();

        // Create a sub-buffer from position 10 to limit (simulates a Java buffer
        // with position=10 and limit=end after header bytes)
        let sub_bytes = full_buf.buffer()[10..limit].to_vec();
        let mut parse_buf = ByteBufferAccessor::from_bytes(sub_bytes);

        let parsed = RequestHeader::parse(&mut parse_buf).unwrap();
        assert_eq!(header, parsed);
    }

    /// Translated from Java `RequestHeaderTest.parseHeaderWithNullClientId`.
    #[test]
    fn test_parse_header_with_null_client_id() {
        let mut header_data = RequestHeaderData::new();
        header_data.set_client_id(None);
        header_data.set_correlation_id(123);
        header_data.set_request_api_key(ApiKeys::FIND_COORDINATOR.id());
        header_data.set_request_api_version(10);

        let mut cache = ObjectSerializationCache::new();
        let size = Message::size(&header_data, &mut cache, 2).unwrap();
        let mut buffer = ByteBufferAccessor::new(size as usize);
        Message::write(&mut header_data, &mut buffer, &cache, 2).unwrap();
        buffer.flip();

        let parsed = RequestHeader::parse(&mut buffer).unwrap();
        assert_eq!(parsed.client_id(), "");
        assert_eq!(parsed.correlation_id(), 123);
        assert_eq!(*parsed.api_key(), ApiKeys::FIND_COORDINATOR);
        assert_eq!(parsed.api_version(), 10);
    }

    /// Translated from Java `RequestHeaderTest.verifySizeMethodsReturnSameValue`.
    ///
    /// The Java test uses Mockito to verify that `size(ObjectSerializationCache)` is
    /// only called once (to verify caching). In Rust, we verify the same semantic:
    /// that the cached `size()` returns the same value as a fresh computation.
    #[test]
    fn test_verify_size_methods_return_same_value() {
        let mut header_data = RequestHeaderData::new();
        header_data.set_client_id(Some("hakuna-matata".to_string()));
        header_data.set_correlation_id(123);
        header_data.set_request_api_key(ApiKeys::FIND_COORDINATOR.id());
        header_data.set_request_api_version(10);

        // Serialize to buffer and parse back
        let mut cache = ObjectSerializationCache::new();
        let size = Message::size(&header_data, &mut cache, 2).unwrap();
        let mut buffer = ByteBufferAccessor::new(size as usize);
        Message::write(&mut header_data, &mut buffer, &cache, 2).unwrap();
        buffer.flip();

        let mut parsed = RequestHeader::parse(&mut buffer).unwrap();

        // Verify that the fresh size calculation matches the cached value
        let size_calculated = parsed.size_with_cache(&mut ObjectSerializationCache::new()).unwrap();
        let size_from_cache = parsed.size().unwrap();
        assert_eq!(size_calculated, size_from_cache);
    }

    /// Java's `RequestHeader.toString()` interpolates `apiKey()` — an `ApiKeys`
    /// enum value that overrides no `toString()` — so `apiKey=` carries the enum
    /// **constant**, not the specification spelling held by the public `name`
    /// field (`RequestHeader.java:163-170`). Pin the whole string, since the
    /// header's rendering is the text every `NetworkClient` send log embeds.
    #[test]
    fn test_request_header_display() {
        let header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::METADATA,
            1,
            "test-client",
            RequestHeaderOptions::new(42),
        )
        .unwrap();
        assert_eq!(
            format!("{header}"),
            "RequestHeader(apiKey=METADATA, apiVersion=1, clientId=test-client, correlationId=42, headerVersion=1)"
        );
        // Not the `name` field's spelling, which the previous rendering used.
        assert!(!format!("{header}").contains("apiKey=Metadata"));
    }

    #[test]
    fn test_request_header_to_response_header() {
        let header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::METADATA,
            12,
            "client",
            RequestHeaderOptions::new(99),
        )
        .unwrap();
        let response_header = header.to_response_header();
        assert_eq!(response_header.correlation_id(), 99);
    }

    #[test]
    fn test_request_header_is_api_version_supported() {
        let header = RequestHeader::new_request_api_key_request_version_client_id_options(
            &ApiKeys::METADATA,
            ApiKeys::METADATA.oldest_version(),
            "c",
            RequestHeaderOptions::new(1),
        )
        .unwrap();
        assert!(header.is_api_version_supported());
    }
}
