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

//! Translation of `org.apache.kafka.common.requests.RequestHeader`.

use std::cell::Cell;

use crate::common::errors::KafkaError;
use crate::common::message::request_header_data::RequestHeaderData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::protocol::{ApiKey, ApiKeys, Message, Readable};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::ResponseHeader;

/// The header for a request in the Kafka protocol.
///
/// Wraps the generated [`RequestHeaderData`] with the convenience surface
/// the Java `RequestHeader` exposes (`new RequestHeader(apiKey, apiVersion,
/// clientId, correlationId)`, `RequestHeader.parse(buf, version)`,
/// `RequestHeader.size()`, `RequestHeader.toResponseHeader()`).
pub struct RequestHeader {
    data: RequestHeaderData,
    header_version: i16,
    /// Cached size in bytes — populated lazily on first call to
    /// [`Self::size`]. Mirrors Java's `private int size = SIZE_NOT_INITIALIZED`.
    /// `Cell` is sufficient because the cache is only mutated through
    /// `&self`, never across threads (`RequestHeader` is `!Sync` like the
    /// Java class effectively is — Java's mutation is also racy).
    size_cache: Cell<Option<i32>>,
}

impl RequestHeader {
    /// Construct a new `RequestHeader` from its constituent fields. Mirrors
    /// `new RequestHeader(ApiKeys requestApiKey, short requestVersion,
    /// String clientId, int correlationId)`.
    pub fn new(request_api_key: &ApiKey, request_version: i16, client_id: &str, correlation_id: i32) -> Self {
        let data = RequestHeaderData {
            request_api_key: request_api_key.id,
            request_api_version: request_version,
            correlation_id,
            client_id: Some(client_id.to_owned()),
            unknown_tagged_fields: Vec::new(),
        };
        let header_version = request_api_key.request_header_version(request_version);
        Self::from_data(data, header_version)
    }

    /// Construct from an already-built `RequestHeaderData`. Mirrors
    /// `new RequestHeader(RequestHeaderData data, short headerVersion)`.
    pub fn from_data(data: RequestHeaderData, header_version: i16) -> Self {
        RequestHeader { data, header_version, size_cache: Cell::new(None) }
    }

    /// Mirrors `RequestHeader.apiKey()`.
    pub fn api_key(&self) -> Result<&'static ApiKey, KafkaError> {
        ApiKeys::for_id(self.data.request_api_key as i32)
    }

    /// Mirrors `RequestHeader.apiVersion()`.
    pub fn api_version(&self) -> i16 {
        self.data.request_api_version
    }

    /// Mirrors `RequestHeader.headerVersion()`.
    pub fn header_version(&self) -> i16 {
        self.header_version
    }

    /// Mirrors `RequestHeader.clientId()`. Returns the empty string when the
    /// underlying field is `None`, mirroring Java's null-as-empty handling.
    pub fn client_id(&self) -> &str {
        self.data.client_id.as_deref().unwrap_or("")
    }

    /// Mirrors `RequestHeader.correlationId()`.
    pub fn correlation_id(&self) -> i32 {
        self.data.correlation_id
    }

    /// Mirrors `RequestHeader.data()`.
    pub fn header_data(&self) -> &RequestHeaderData {
        &self.data
    }

    /// Calculate the size of the header. Mirrors the test-visible
    /// `RequestHeader.size(ObjectSerializationCache)`. Updates the cache.
    pub fn size_with_cache(&self, serialization_cache: &mut ObjectSerializationCache) -> i32 {
        let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
        Message::add_size(&self.data, &mut sizer, serialization_cache, self.header_version);
        let s = sizer.total_size();
        self.size_cache.set(Some(s));
        s
    }

    /// Returns the size of the header in bytes. Mirrors `RequestHeader.size()`.
    /// Idempotent and inexpensive after the first call.
    pub fn size(&self) -> i32 {
        if let Some(s) = self.size_cache.get() {
            return s;
        }
        let mut cache = ObjectSerializationCache::new();
        self.size_with_cache(&mut cache)
    }

    /// Test-only: write into a pre-allocated `ByteBufferAccessor`. Mirrors
    /// `RequestHeader.write(ByteBuffer, ObjectSerializationCache)`.
    pub fn write(&self, accessor: &mut ByteBufferAccessor, cache: &ObjectSerializationCache) -> Result<(), KafkaError> {
        Message::write(&self.data, accessor, cache, self.header_version)
    }

    /// Mirrors `RequestHeader.isApiVersionSupported()`.
    pub fn is_api_version_supported(&self) -> bool {
        match self.api_key() {
            Ok(k) => k.is_version_supported(self.api_version()),
            Err(_) => false,
        }
    }

    /// Mirrors `RequestHeader.isApiVersionDeprecated()`.
    pub fn is_api_version_deprecated(&self) -> bool {
        match self.api_key() {
            Ok(k) => k.is_version_deprecated(self.api_version()),
            Err(_) => false,
        }
    }

    /// Mirrors `RequestHeader.toResponseHeader()`.
    pub fn to_response_header(&self) -> Result<ResponseHeader, KafkaError> {
        let header_version = self.api_key()?.response_header_version(self.api_version());
        Ok(ResponseHeader::new(self.correlation_id(), header_version))
    }

    /// Parse a request header from `accessor`. Mirrors
    /// `RequestHeader.parse(ByteBuffer)`.
    ///
    /// Java first peeks the first 4 bytes (api key + api version), derives
    /// the header version from the api key, then rewinds and re-parses
    /// against the proper `RequestHeaderData` schema. Our Rust translation
    /// mirrors this exactly.
    pub fn parse(accessor: &mut ByteBufferAccessor) -> Result<Self, KafkaError> {
        let start_position = accessor.position();
        let api_key_id_result = accessor.read_short();
        let api_version_result = accessor.read_short();

        // Re-position to the start so we can re-parse the full header.
        accessor.set_position(start_position);

        let api_key_id = match api_key_id_result {
            Ok(v) => v,
            Err(_) => {
                return Err(KafkaError::InvalidRequest(
                    "Error parsing request header. Our best guess of the apiKeyId is: -1".to_owned(),
                ));
            },
        };
        let api_version = match api_version_result {
            Ok(v) => v,
            Err(_) => {
                return Err(KafkaError::InvalidRequest(format!(
                    "Error parsing request header. Our best guess of the apiKeyId is: {api_key_id}"
                )));
            },
        };

        let api_key = match ApiKeys::for_id(api_key_id as i32) {
            Ok(k) => k,
            Err(_) => {
                return Err(KafkaError::InvalidRequest(format!("Unknown API key {api_key_id}")));
            },
        };

        if !api_key.has_valid_version() {
            return Err(KafkaError::InvalidRequest(format!(
                "Unsupported api with key {} ({}) and version {}",
                api_key_id, api_key.name, api_version
            )));
        }

        let header_version = api_key.request_header_version(api_version);
        let mut header_data = match RequestHeaderData::read(accessor, header_version) {
            Ok(d) => d,
            Err(_) => {
                return Err(KafkaError::InvalidRequest(format!(
                    "Error parsing request header. Our best guess of the apiKeyId is: {api_key_id}"
                )));
            },
        };
        // Java treats a null clientId as equivalent to "" for downstream code.
        if header_data.client_id.is_none() {
            header_data.client_id = Some(String::new());
        }
        let header = RequestHeader::from_data(header_data, header_version);
        // Cache the size from the position delta.
        let parsed_size = (accessor.position() as i32 - start_position as i32).max(0);
        header.size_cache.set(Some(parsed_size));
        Ok(header)
    }
}

impl AbstractRequestResponse for RequestHeader {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl std::fmt::Debug for RequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestHeader")
            .field("data", &self.data)
            .field("header_version", &self.header_version)
            .finish()
    }
}

impl std::fmt::Display for RequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let api_key_name = self.api_key().map(|k| k.name).unwrap_or("<unknown>");
        write!(
            f,
            "RequestHeader(apiKey={api_key_name}, apiVersion={}, clientId={}, correlationId={}, headerVersion={})",
            self.api_version(),
            self.client_id(),
            self.correlation_id(),
            self.header_version
        )
    }
}

impl Clone for RequestHeader {
    fn clone(&self) -> Self {
        RequestHeader {
            data: self.data.clone(),
            header_version: self.header_version,
            size_cache: Cell::new(self.size_cache.get()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `RequestHeaderTest#testRequestHeaderV1`.
    #[test]
    fn request_header_v1() {
        let api_version: i16 = 1;
        let find_coordinator = ApiKeys::for_id(10).expect("FIND_COORDINATOR");
        let header = RequestHeader::new(find_coordinator, api_version, "", 10);
        assert_eq!(header.header_version(), 1);

        let buf = serialize_header(&header);
        assert_eq!(buf.len(), 10);

        let mut accessor = ByteBufferAccessor::wrap(buf);
        let parsed = RequestHeader::parse(&mut accessor).expect("parse");
        assert_eq!(parsed, header);
    }

    /// Translation of `RequestHeaderTest#testRequestHeaderV2`.
    #[test]
    fn request_header_v2() {
        let api_version: i16 = 2;
        let create_delegation_token = ApiKeys::for_id(38).expect("CREATE_DELEGATION_TOKEN");
        let header = RequestHeader::new(create_delegation_token, api_version, "", 10);
        assert_eq!(header.header_version(), 2);

        let buf = serialize_header(&header);
        assert_eq!(buf.len(), 11);

        let mut accessor = ByteBufferAccessor::wrap(buf);
        let parsed = RequestHeader::parse(&mut accessor).expect("parse");
        assert_eq!(parsed, header);
    }

    /// Translation of `RequestHeaderTest#parseHeaderFromBufferWithNonZeroPosition`.
    #[test]
    fn parse_header_from_buffer_with_nonzero_position() {
        let find_coordinator = ApiKeys::for_id(10).expect("FIND_COORDINATOR");
        let header = RequestHeader::new(find_coordinator, 1, "", 10);
        let mut serialization_cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut serialization_cache) as usize;

        let mut accessor = ByteBufferAccessor::allocate(10 + size);
        accessor.set_position(10);
        Message::write(&header.data, &mut accessor, &serialization_cache, header.header_version).expect("write");
        accessor.flip();
        accessor.set_position(10);

        let parsed = RequestHeader::parse(&mut accessor).expect("parse");
        assert_eq!(parsed, header);
    }

    /// Translation of `RequestHeaderTest#parseHeaderWithNullClientId`.
    #[test]
    fn parse_header_with_null_client_id() {
        let header_data = RequestHeaderData {
            request_api_key: 10, // FIND_COORDINATOR
            request_api_version: 10,
            correlation_id: 123,
            client_id: None,
            unknown_tagged_fields: Vec::new(),
        };
        let mut serialization_cache = ObjectSerializationCache::new();
        let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
        Message::add_size(&header_data, &mut sizer, &mut serialization_cache, 2);
        let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
        Message::write(&header_data, &mut accessor, &serialization_cache, 2).expect("write");
        accessor.flip();

        let parsed = RequestHeader::parse(&mut accessor).expect("parse");
        assert_eq!(parsed.client_id(), "");
        assert_eq!(parsed.correlation_id(), 123);
        assert_eq!(parsed.api_key().expect("known").id, 10);
        assert_eq!(parsed.api_version(), 10);
    }

    /// Translation of `RequestHeaderTest#verifySizeMethodsReturnSameValue`.
    /// Java uses Mockito spy verification; we directly assert the cached
    /// value matches a fresh recomputation.
    #[test]
    fn verify_size_methods_return_same_value() {
        let header_data = RequestHeaderData {
            request_api_key: 10,
            request_api_version: 10,
            correlation_id: 123,
            client_id: Some("hakuna-matata".to_owned()),
            unknown_tagged_fields: Vec::new(),
        };
        let mut serialization_cache = ObjectSerializationCache::new();
        let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
        Message::add_size(&header_data, &mut sizer, &mut serialization_cache, 2);
        let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
        Message::write(&header_data, &mut accessor, &serialization_cache, 2).expect("write");
        accessor.flip();

        let parsed = RequestHeader::parse(&mut accessor).expect("parse");
        let mut fresh_cache = ObjectSerializationCache::new();
        let size_calculated = parsed.size_with_cache(&mut fresh_cache);
        let size_from_cache = parsed.size();
        assert_eq!(size_calculated, size_from_cache);
    }

    /// Helper: serialize a header into a `Vec<u8>` (read-mode accessor's
    /// payload). Mirrors `RequestTestUtils.serializeRequestHeader`.
    fn serialize_header(header: &RequestHeader) -> Vec<u8> {
        let mut cache = ObjectSerializationCache::new();
        let size = header.size_with_cache(&mut cache) as usize;
        let mut accessor = ByteBufferAccessor::allocate(size);
        Message::write(&header.data, &mut accessor, &cache, header.header_version).expect("write");
        accessor.flip();
        accessor.buffer().to_vec()
    }
}
