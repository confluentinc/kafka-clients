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

//! Translation of `org.apache.kafka.common.requests.SaslAuthenticateRequest`.

use std::fmt;
use std::sync::OnceLock;

use crate::common::errors::KafkaError;
use crate::common::message::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::common::message::sasl_authenticate_response_data::SaslAuthenticateResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequest;
use crate::common::requests::AbstractRequestBuilder;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::SaslAuthenticateResponse;

/// Translation of
/// `org.apache.kafka.common.requests.SaslAuthenticateRequest.Builder`.
///
/// Java declares this as a `public static class Builder extends
/// AbstractRequest.Builder<SaslAuthenticateRequest>`.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequestBuilder {
    data: SaslAuthenticateRequestData,
}

impl SaslAuthenticateRequestBuilder {
    /// Mirrors `new Builder(SaslAuthenticateRequestData data)`.
    pub fn new(data: SaslAuthenticateRequestData) -> Self {
        SaslAuthenticateRequestBuilder { data }
    }
}

impl AbstractRequestBuilder for SaslAuthenticateRequestBuilder {
    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(36).expect("SASL_AUTHENTICATE api_key always present")
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.api_key().oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        self.api_key().latest_version()
    }

    fn build(&self, version: i16) -> Result<Box<dyn AbstractRequest>, KafkaError> {
        Ok(Box::new(SaslAuthenticateRequest::new(self.data.clone(), version)))
    }
}

/// Translation of `org.apache.kafka.common.requests.SaslAuthenticateRequest`.
///
/// Request from SASL client containing client SASL authentication token as
/// defined by the SASL protocol for the configured SASL mechanism.
///
/// For interoperability with versions prior to Kafka 1.0.0, this request is
/// used only with broker version 1.0.0 and higher that support
/// `SaslHandshakeRequest` v1. Clients connecting to older brokers will send
/// `SaslHandshakeRequest` v0 followed by SASL tokens without the Kafka
/// request headers.
pub struct SaslAuthenticateRequest {
    data: SaslAuthenticateRequestData,
    version: i16,
}

impl SaslAuthenticateRequest {
    /// Mirrors `new SaslAuthenticateRequest(SaslAuthenticateRequestData data,
    /// short version)`.
    pub fn new(data: SaslAuthenticateRequestData, version: i16) -> Self {
        SaslAuthenticateRequest { data, version }
    }

    /// Mirrors `SaslAuthenticateRequest.data()`.
    pub fn request_data(&self) -> &SaslAuthenticateRequestData {
        &self.data
    }

    /// Convenience accessor for the SASL authentication token. Mirrors the
    /// `data().authBytes()` accessor pattern.
    pub fn auth_bytes(&self) -> &[u8] {
        &self.data.auth_bytes
    }

    /// Mirrors `SaslAuthenticateRequest.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = SaslAuthenticateRequestData::read(accessor, version)?;
        Ok(SaslAuthenticateRequest::new(data, version))
    }
}

impl AbstractRequestResponse for SaslAuthenticateRequest {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractRequest for SaslAuthenticateRequest {
    fn version(&self) -> i16 {
        self.version
    }

    fn api_key(&self) -> &'static ApiKey {
        static SASL_AUTHENTICATE: OnceLock<&'static ApiKey> = OnceLock::new();
        SASL_AUTHENTICATE
            .get_or_init(|| ApiKeys::for_id(36).expect("SASL_AUTHENTICATE api_key always present in ALL_API_KEYS"))
    }

    fn get_error_response(&self, _throttle_time_ms: i32, error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
        let err = Errors::for_code(error.code());
        let response = SaslAuthenticateResponseData {
            error_code: err.code(),
            error_message: Some(error.message().to_owned()),
            ..SaslAuthenticateResponseData::new()
        };
        Some(Box::new(SaslAuthenticateResponse::new(response)))
    }
}

impl fmt::Debug for SaslAuthenticateRequest {
    /// Mirrors Java's `toString()` override which masks `authBytes` because
    /// they may contain credentials.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslAuthenticateRequest")
            .field("version", &self.version)
            .field("auth_bytes", &"<redacted>")
            .field("unknown_tagged_fields", &self.data.unknown_tagged_fields)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::RawTaggedField;

    /// Round-trip at v0 (flex boundary -1): non-flexible encoding —
    /// auth_bytes uses an `int32` length prefix, no tagged-field trailer.
    #[test]
    fn round_trip_v0_pre_flexible() {
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"\x00user\x00pass".to_vec(), unknown_tagged_fields: Vec::new() },
            0,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize v0");
        let parsed = SaslAuthenticateRequest::parse(&mut serialized, 0).expect("parse v0");
        assert_eq!(parsed.version(), 0);
        assert_eq!(parsed.auth_bytes(), b"\x00user\x00pass");
    }

    /// Round-trip at v1: same wire shape as v0 (spec: "Version 1 is the
    /// same as version 0").
    #[test]
    fn round_trip_v1_pre_flexible() {
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData {
                auth_bytes: b"\x00admin\x00admin-secret".to_vec(),
                unknown_tagged_fields: Vec::new(),
            },
            1,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize v1");
        let parsed = SaslAuthenticateRequest::parse(&mut serialized, 1).expect("parse v1");
        assert_eq!(parsed.version(), 1);
        assert_eq!(parsed.auth_bytes(), b"\x00admin\x00admin-secret");
    }

    /// Round-trip at v2 (flex boundary +1): flexible encoding — auth_bytes
    /// uses compact-bytes (varint length + 1), plus a varint tagged-field
    /// trailer at the end of the body.
    #[test]
    fn round_trip_v2_flexible() {
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"\x00user\x00pass".to_vec(), unknown_tagged_fields: Vec::new() },
            2,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize v2");
        let parsed = SaslAuthenticateRequest::parse(&mut serialized, 2).expect("parse v2");
        assert_eq!(parsed.version(), 2);
        assert_eq!(parsed.auth_bytes(), b"\x00user\x00pass");
    }

    /// Empty auth_bytes — mirrors Java's `createSaslAuthenticateRequest`
    /// fixture which uses `new byte[0]`.
    #[test]
    fn round_trip_empty_auth_bytes_all_versions() {
        for v in 0..=2 {
            let req = SaslAuthenticateRequest::new(SaslAuthenticateRequestData::new(), v);
            let mut serialized = AbstractRequest::serialize(&req).expect("serialize");
            let parsed = SaslAuthenticateRequest::parse(&mut serialized, v).expect("parse");
            assert!(parsed.auth_bytes().is_empty(), "v{v} empty auth_bytes");
        }
    }

    /// Translation of Java
    /// `testValidTaggedFieldsWithSaslAuthenticateRequest`. Tagged fields
    /// are emitted at v2+ in the trailer. Set one and confirm it round-trips.
    #[test]
    fn tagged_fields_round_trip_v2() {
        let tag = RawTaggedField::new(1, vec![0x1, 0x2, 0x3]);
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"test".to_vec(), unknown_tagged_fields: vec![tag.clone()] },
            2,
        );
        let mut serialized = AbstractRequest::serialize(&req).expect("serialize");
        let parsed = SaslAuthenticateRequest::parse(&mut serialized, 2).expect("parse");
        assert_eq!(parsed.auth_bytes(), b"test");
        assert_eq!(parsed.request_data().unknown_tagged_fields.len(), 1);
        assert_eq!(parsed.request_data().unknown_tagged_fields[0], tag);
    }

    /// Tagged fields rejected on non-flexible versions. Java returns an
    /// error at write time; we mirror that behaviour.
    #[test]
    fn tagged_fields_rejected_pre_flexible() {
        let tag = RawTaggedField::new(1, vec![0x1, 0x2, 0x3]);
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"test".to_vec(), unknown_tagged_fields: vec![tag] },
            1,
        );
        let result = AbstractRequest::serialize(&req);
        let err = match result {
            Ok(_) => panic!("v1 should reject tagged fields"),
            Err(e) => e,
        };
        let msg = err.message();
        assert!(
            msg.contains("Tagged fields were set"),
            "expected the tagged-fields error, got: {msg}"
        );
    }

    /// Builder convenience: oldest/latest allowed version come from the
    /// `ApiKey` registry (here `0..=2`).
    #[test]
    fn builder_oldest_latest_match_registry() {
        let builder = SaslAuthenticateRequestBuilder::new(SaslAuthenticateRequestData::new());
        assert_eq!(builder.oldest_allowed_version(), 0);
        assert_eq!(builder.latest_allowed_version(), 2);
        let built = builder.build(2).expect("build");
        assert_eq!(built.version(), 2);
    }

    /// Debug output redacts auth_bytes — translation of Java
    /// `testSaslAuthenticateRequestResponseToStringMasksSensitiveData`.
    ///
    /// **Test-only leak hygiene** (Critic 9 Suggestion 2): on assertion
    /// failure, do NOT echo the full Debug string (which by definition
    /// contains the secret we're guarding against). Reference a sentinel
    /// substring only.
    #[test]
    fn debug_masks_auth_bytes() {
        const SENTINEL: &str = "sensitive-auth-token-123";
        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: SENTINEL.as_bytes().to_vec(), unknown_tagged_fields: Vec::new() },
            2,
        );
        let dbg = format!("{req:?}");
        assert!(
            !dbg.contains(SENTINEL),
            "Debug output contained the sentinel — credentials leaked! dbg.len()={}",
            dbg.len()
        );
        assert!(
            dbg.contains("<redacted>"),
            "expected redaction marker not present (dbg.len()={})",
            dbg.len()
        );
    }

    /// Generator-level redaction (Critic 9 Suggestion 1): direct Debug/
    /// Display on the data class must NOT leak `auth_bytes` either.
    /// Before Phase 9a, the wrapper-level Debug masked `auth_bytes` but
    /// the generated `*Data` struct's derived Debug + Display delegate
    /// would still print the raw bytes — defeating the redaction at the
    /// first `tracing::debug!("data = {}", req.request_data())` callsite.
    #[test]
    fn data_class_debug_and_display_redact_auth_bytes() {
        const SENTINEL: &str = "sensitive-auth-token-123";
        let data =
            SaslAuthenticateRequestData { auth_bytes: SENTINEL.as_bytes().to_vec(), unknown_tagged_fields: Vec::new() };
        // {:?} delegates to the hand-emitted Debug impl which redacts.
        let dbg = format!("{data:?}");
        assert!(
            !dbg.contains(SENTINEL),
            "data class Debug leaked sentinel! dbg.len()={}",
            dbg.len()
        );
        assert!(
            dbg.contains("<redacted>"),
            "expected redaction marker in data class Debug (dbg.len()={})",
            dbg.len()
        );
        // {} goes through Display which delegates to Debug.
        let disp = format!("{data}");
        assert!(
            !disp.contains(SENTINEL),
            "data class Display leaked sentinel! disp.len()={}",
            disp.len()
        );
        assert!(
            disp.contains("<redacted>"),
            "expected redaction marker in data class Display (disp.len()={})",
            disp.len()
        );
    }

    /// Hex fixture: SaslAuthenticateRequest v0 body — non-flexible
    /// encoding for an RFC 4616 PLAIN authentication token
    /// `\0username\0password` (here using "user"/"pass" for brevity,
    /// 10 bytes total).
    ///
    /// **Fixture provenance**: hand-derived from
    /// `SaslAuthenticateRequest.json` (apiKey 36, `flexibleVersions:
    /// "2+"`) — at v0 the body uses `int32` length-prefixed bytes per
    /// the non-flexible encoding rules. Awaiting Java-runtime byte
    /// capture from the Apache Kafka 4.2 client. Independently
    /// cross-checked against the `testInvalidSaslAuthenticateRequest`
    /// Java test which asserts an `int32` length lives at the start of
    /// the body for the chosen v1 (same shape as v0).
    ///
    /// Wire layout:
    /// - `00 00 00 0A` — i32 byte-array length = 10
    /// - 10 bytes — `\0user\0pass`
    ///
    /// Total: 14 bytes.
    #[test]
    fn hex_fixture_v0_plain_token() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, 0x00, 0x0A, // i32 length = 10
            0x00, b'u', b's', b'e', b'r', // \0user
            0x00, b'p', b'a', b's', b's', // \0pass
        ];

        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"\x00user\x00pass".to_vec(), unknown_tagged_fields: Vec::new() },
            0,
        );
        let serialized = AbstractRequest::serialize(&req).expect("serialize v0");
        assert_eq!(
            serialized.buffer(),
            EXPECTED,
            "SaslAuthenticateRequest v0 (PLAIN token) bytes diverged from the hex fixture"
        );
    }

    /// Hex fixture: SaslAuthenticateRequest v1 body — same shape as v0
    /// (spec: "Version 1 is the same as version 0").
    ///
    /// **Fixture provenance**: hand-derived.
    #[test]
    fn hex_fixture_v1_plain_token() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, 0x00, 0x0A, // i32 length = 10
            0x00, b'u', b's', b'e', b'r', 0x00, b'p', b'a', b's', b's',
        ];

        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"\x00user\x00pass".to_vec(), unknown_tagged_fields: Vec::new() },
            1,
        );
        let serialized = AbstractRequest::serialize(&req).expect("serialize v1");
        assert_eq!(serialized.buffer(), EXPECTED);
    }

    /// Hex fixture: SaslAuthenticateRequest v2 body — flexible
    /// encoding. The flex boundary (v1→v2) is the highest-priority byte
    /// fidelity test per CLAUDE.md "wire-protocol byte-vector
    /// divergence" risk #1.
    ///
    /// **Fixture provenance**: hand-derived from
    /// `SaslAuthenticateRequest.json` (`flexibleVersions: "2+"`) —
    /// at v2 the body uses compact-bytes (`unsignedVarint(len+1)`) and
    /// a zero-length tagged-field trailer.
    ///
    /// Wire layout (12 bytes total):
    /// - `0B` — unsigned-varint(11) = compact-bytes length+1 for a
    ///   10-byte payload
    /// - 10 bytes — `\0user\0pass`
    /// - `00` — unsigned-varint(0) = zero tagged fields
    #[test]
    fn hex_fixture_v2_plain_token() {
        const EXPECTED: &[u8] = &[
            0x0B, // varint(11) = compact-bytes length+1
            0x00, b'u', b's', b'e', b'r', 0x00, b'p', b'a', b's', b's', // payload (10 bytes)
            0x00, // varint(0) = zero tagged fields
        ];

        let req = SaslAuthenticateRequest::new(
            SaslAuthenticateRequestData { auth_bytes: b"\x00user\x00pass".to_vec(), unknown_tagged_fields: Vec::new() },
            2,
        );
        let serialized = AbstractRequest::serialize(&req).expect("serialize v2");
        assert_eq!(
            serialized.buffer(),
            EXPECTED,
            "SaslAuthenticateRequest v2 (flex boundary) bytes diverged from the hex fixture"
        );
    }

    /// Hex fixture: SaslAuthenticateRequest v2 with empty auth_bytes —
    /// pins the `varint(1) + varint(0)` encoding for an empty payload
    /// at the flex boundary.
    ///
    /// **Fixture provenance**: hand-derived.
    #[test]
    fn hex_fixture_v2_empty_auth_bytes() {
        const EXPECTED: &[u8] = &[
            0x01, // varint(1) = compact-bytes length+1 for 0-byte payload
            0x00, // varint(0) = zero tagged fields
        ];

        let req = SaslAuthenticateRequest::new(SaslAuthenticateRequestData::new(), 2);
        let serialized = AbstractRequest::serialize(&req).expect("serialize v2 empty");
        assert_eq!(serialized.buffer(), EXPECTED);
    }
}
