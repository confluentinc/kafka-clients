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

//! Translation of `org.apache.kafka.common.requests.ResponseHeader`.

use std::cell::Cell;

use crate::common::errors::KafkaError;
use crate::common::message::response_header_data::ResponseHeaderData;
use crate::common::protocol::Message;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::requests::AbstractRequestResponse;

/// A response header in the Kafka protocol. Wraps the generated
/// [`ResponseHeaderData`].
pub struct ResponseHeader {
    data: ResponseHeaderData,
    header_version: i16,
    size_cache: Cell<Option<i32>>,
}

impl ResponseHeader {
    /// Mirrors `new ResponseHeader(int correlationId, short headerVersion)`.
    pub fn new(correlation_id: i32, header_version: i16) -> Self {
        let data = ResponseHeaderData { correlation_id, unknown_tagged_fields: Vec::new() };
        Self::from_data(data, header_version)
    }

    /// Mirrors `new ResponseHeader(ResponseHeaderData data, short headerVersion)`.
    pub fn from_data(data: ResponseHeaderData, header_version: i16) -> Self {
        ResponseHeader { data, header_version, size_cache: Cell::new(None) }
    }

    /// Calculate the size of the header. Mirrors the test-visible
    /// `ResponseHeader.size(ObjectSerializationCache)`.
    pub fn size_with_cache(&self, serialization_cache: &mut ObjectSerializationCache) -> i32 {
        let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
        Message::add_size(&self.data, &mut sizer, serialization_cache, self.header_version);
        sizer.total_size()
    }

    /// Returns the size of the header in bytes. Mirrors `ResponseHeader.size()`.
    pub fn size(&self) -> i32 {
        if let Some(s) = self.size_cache.get() {
            return s;
        }
        let mut cache = ObjectSerializationCache::new();
        let s = self.size_with_cache(&mut cache);
        self.size_cache.set(Some(s));
        s
    }

    /// Mirrors `ResponseHeader.correlationId()`.
    pub fn correlation_id(&self) -> i32 {
        self.data.correlation_id
    }

    /// Mirrors `ResponseHeader.headerVersion()`.
    pub fn header_version(&self) -> i16 {
        self.header_version
    }

    /// Mirrors `ResponseHeader.data()`.
    pub fn header_data(&self) -> &ResponseHeaderData {
        &self.data
    }

    /// Test-only: write into a pre-allocated `ByteBufferAccessor`. Mirrors
    /// `ResponseHeader.write(ByteBuffer, ObjectSerializationCache)`.
    pub fn write(&self, accessor: &mut ByteBufferAccessor, cache: &ObjectSerializationCache) -> Result<(), KafkaError> {
        Message::write(&self.data, accessor, cache, self.header_version)
    }

    /// Parse a response header. Mirrors
    /// `ResponseHeader.parse(ByteBuffer, short headerVersion)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, header_version: i16) -> Result<Self, KafkaError> {
        let start_position = accessor.position();
        let data = ResponseHeaderData::read(accessor, header_version)?;
        let header = ResponseHeader::from_data(data, header_version);
        let parsed_size = (accessor.position() as i32 - start_position as i32).max(0);
        header.size_cache.set(Some(parsed_size));
        Ok(header)
    }
}

impl AbstractRequestResponse for ResponseHeader {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl std::fmt::Debug for ResponseHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseHeader")
            .field("data", &self.data)
            .field("header_version", &self.header_version)
            .finish()
    }
}

impl std::fmt::Display for ResponseHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResponseHeader(correlationId={}, headerVersion={})",
            self.correlation_id(),
            self.header_version
        )
    }
}

impl Clone for ResponseHeader {
    fn clone(&self) -> Self {
        ResponseHeader {
            data: self.data.clone(),
            header_version: self.header_version,
            size_cache: Cell::new(self.size_cache.get()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a v0 (non-flexible) `ResponseHeader`.
    #[test]
    fn response_header_v0_round_trip() {
        let header = ResponseHeader::new(42, 0);
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache) as usize;
        // v0: just a 4-byte correlation id
        assert_eq!(size, 4);

        let mut accessor = ByteBufferAccessor::allocate(size);
        header.write(&mut accessor, &cache).expect("write");
        accessor.flip();

        let parsed = ResponseHeader::parse(&mut accessor, 0).expect("parse");
        assert_eq!(parsed.correlation_id(), 42);
        assert_eq!(parsed.header_version(), 0);
        assert_eq!(parsed.size(), 4);
    }

    /// Round-trip a v1 (flexible) `ResponseHeader`.
    #[test]
    fn response_header_v1_round_trip() {
        let header = ResponseHeader::new(123, 1);
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache) as usize;
        // v1 flexible: 4-byte correlation id + 1-byte tagged-field count (0)
        assert_eq!(size, 5);

        let mut accessor = ByteBufferAccessor::allocate(size);
        header.write(&mut accessor, &cache).expect("write");
        accessor.flip();

        let parsed = ResponseHeader::parse(&mut accessor, 1).expect("parse");
        assert_eq!(parsed.correlation_id(), 123);
        assert_eq!(parsed.header_version(), 1);
    }

    /// `to_response_header()` round-trips correlation id and uses the right
    /// header version derived from the api key.
    #[test]
    fn to_response_header_uses_correct_header_version_for_api() {
        use crate::common::protocol::ApiKeys;
        let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
        let req_header = crate::common::requests::RequestHeader::new(api_versions, 3, "id", 99);
        let resp_header = req_header.to_response_header().expect("to_response_header");
        assert_eq!(resp_header.correlation_id(), 99);
        // ApiVersions response v3 uses header v0 (the broker keeps the response header non-flexible
        // even when the request header is flexible — see KIP-511 in apache/kafka).
        assert_eq!(resp_header.header_version(), 0);
    }
}
