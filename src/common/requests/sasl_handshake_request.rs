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

//! Translation of `org.apache.kafka.common.requests.SaslHandshakeRequest`.

use std::sync::OnceLock;

use crate::common::errors::KafkaError;
use crate::common::message::sasl_handshake_request_data::SaslHandshakeRequestData;
use crate::common::message::sasl_handshake_response_data::SaslHandshakeResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequest;
use crate::common::requests::AbstractRequestBuilder;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::SaslHandshakeResponse;

/// Translation of `org.apache.kafka.common.requests.SaslHandshakeRequest.Builder`.
///
/// Java declares this as a `public static class Builder extends
/// AbstractRequest.Builder<SaslHandshakeRequest>`. Rust models it as a
/// concrete struct implementing the type-erased
/// [`AbstractRequestBuilder`] trait.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequestBuilder {
    data: SaslHandshakeRequestData,
}

impl SaslHandshakeRequestBuilder {
    /// Mirrors `new Builder(SaslHandshakeRequestData data)`.
    pub fn new(data: SaslHandshakeRequestData) -> Self {
        SaslHandshakeRequestBuilder { data }
    }
}

impl AbstractRequestBuilder for SaslHandshakeRequestBuilder {
    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(17).expect("SASL_HANDSHAKE api_key always present")
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.api_key().oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        self.api_key().latest_version()
    }

    fn build(&self, version: i16) -> Result<Box<dyn AbstractRequest>, KafkaError> {
        Ok(Box::new(SaslHandshakeRequest::new(self.data.clone(), version)))
    }
}

/// Translation of `org.apache.kafka.common.requests.SaslHandshakeRequest`.
///
/// Request from SASL client containing client SASL mechanism.
///
/// For interoperability with Kafka 0.9.0.x, the mechanism flow may be
/// omitted when using GSSAPI. Hence this request should not conflict with
/// the first GSSAPI client packet. For GSSAPI, the first context
/// establishment packet starts with byte `0x60` (APPLICATION-0 tag)
/// followed by a variable-length encoded size. This handshake request
/// starts with a request header two-byte API key set to 17, followed by a
/// mechanism name, making it easy to distinguish from a GSSAPI packet.
pub struct SaslHandshakeRequest {
    data: SaslHandshakeRequestData,
    version: i16,
}

impl SaslHandshakeRequest {
    /// Mirrors `new SaslHandshakeRequest(SaslHandshakeRequestData data, short version)`.
    pub fn new(data: SaslHandshakeRequestData, version: i16) -> Self {
        SaslHandshakeRequest { data, version }
    }

    /// Mirrors `SaslHandshakeRequest.data()`.
    pub fn request_data(&self) -> &SaslHandshakeRequestData {
        &self.data
    }

    /// Convenience accessor for the chosen SASL mechanism. Mirrors
    /// the `data().mechanism()` accessor pattern used by callers.
    pub fn mechanism(&self) -> &str {
        &self.data.mechanism
    }

    /// Mirrors `SaslHandshakeRequest.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = SaslHandshakeRequestData::read(accessor, version)?;
        Ok(SaslHandshakeRequest::new(data, version))
    }
}

impl AbstractRequestResponse for SaslHandshakeRequest {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractRequest for SaslHandshakeRequest {
    fn version(&self) -> i16 {
        self.version
    }

    fn api_key(&self) -> &'static ApiKey {
        static SASL_HANDSHAKE: OnceLock<&'static ApiKey> = OnceLock::new();
        SASL_HANDSHAKE
            .get_or_init(|| ApiKeys::for_id(17).expect("SASL_HANDSHAKE api_key always present in ALL_API_KEYS"))
    }

    fn get_error_response(&self, _throttle_time_ms: i32, error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
        let response = SaslHandshakeResponseData {
            error_code: Errors::for_code(error.code()).code(),
            ..SaslHandshakeResponseData::new()
        };
        Some(Box::new(SaslHandshakeResponse::new(response)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip at v0: serialize → parse → confirm mechanism survives.
    #[test]
    fn round_trip_v0_plain() {
        let req = SaslHandshakeRequest::new(
            SaslHandshakeRequestData { mechanism: "PLAIN".to_owned(), unknown_tagged_fields: Vec::new() },
            0,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize v0");
        let parsed = SaslHandshakeRequest::parse(&mut serialized, 0).expect("parse v0");
        assert_eq!(parsed.version(), 0);
        assert_eq!(parsed.mechanism(), "PLAIN");
    }

    /// Round-trip at v1: same wire shape as v0 — Java JSON spec annotates
    /// "Version 1 is the same as version 0" / `flexibleVersions: "none"`,
    /// so the bytes should be identical to v0 for the same mechanism.
    #[test]
    fn round_trip_v1_plain() {
        let req = SaslHandshakeRequest::new(
            SaslHandshakeRequestData { mechanism: "PLAIN".to_owned(), unknown_tagged_fields: Vec::new() },
            1,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize v1");
        let parsed = SaslHandshakeRequest::parse(&mut serialized, 1).expect("parse v1");
        assert_eq!(parsed.version(), 1);
        assert_eq!(parsed.mechanism(), "PLAIN");
    }

    /// `flexibleVersions: "none"` means *neither* version uses
    /// compact-strings or tagged-field trailers. Re-encode the parsed v1
    /// request and confirm we end up at byte-identical bytes (no extra
    /// trailer) as the original.
    #[test]
    fn flexible_versions_none_no_tagged_trailer_v1() {
        let req = SaslHandshakeRequest::new(
            SaslHandshakeRequestData { mechanism: "PLAIN".to_owned(), unknown_tagged_fields: Vec::new() },
            1,
        );
        let bytes_first = AbstractRequest::serialize(&req).expect("serialize first").buffer().to_vec();
        let mut accessor = ByteBufferAccessor::wrap(bytes_first.clone());
        let parsed = SaslHandshakeRequest::parse(&mut accessor, 1).expect("parse");
        let bytes_second = AbstractRequest::serialize(&parsed).expect("serialize second").buffer().to_vec();
        assert_eq!(bytes_first, bytes_second, "v1 round-trip must be byte-stable");
    }

    /// Empty mechanism string serializes as `00 00` (i16 length 0) + no
    /// body. Java's spec treats `string` as non-null with empty default,
    /// so this is a legal-but-unusual case.
    #[test]
    fn round_trip_empty_mechanism() {
        let req = SaslHandshakeRequest::new(
            SaslHandshakeRequestData { mechanism: String::new(), unknown_tagged_fields: Vec::new() },
            0,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize");
        let parsed = SaslHandshakeRequest::parse(&mut serialized, 0).expect("parse");
        assert!(parsed.mechanism().is_empty());
    }

    /// Java `createSaslHandshakeRequest(version)` in
    /// `RequestResponseTest.java` constructs with `"PLAIN"`. Mirror the
    /// fixture across both versions: error_counts surfaces NONE.
    #[test]
    fn error_counts_via_get_error_response_v1() {
        let req = SaslHandshakeRequest::new(
            SaslHandshakeRequestData { mechanism: "PLAIN".to_owned(), unknown_tagged_fields: Vec::new() },
            1,
        );
        let err_resp = req
            .get_error_response(0, &KafkaError::Authentication("bad mechanism".to_owned()))
            .expect("response");
        let counts = err_resp.error_counts();
        let total: i32 = counts.values().sum();
        assert!(total >= 1);
    }

    /// Builder convenience: oldest/latest allowed version come from the
    /// `ApiKey` registry (here `0..=1`).
    #[test]
    fn builder_oldest_latest_match_registry() {
        let builder = SaslHandshakeRequestBuilder::new(SaslHandshakeRequestData {
            mechanism: "PLAIN".to_owned(),
            unknown_tagged_fields: Vec::new(),
        });
        assert_eq!(builder.oldest_allowed_version(), 0);
        assert_eq!(builder.latest_allowed_version(), 1);
        let built = builder.build(1).expect("build");
        assert_eq!(built.version(), 1);
    }
}
