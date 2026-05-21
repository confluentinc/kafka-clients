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

//! Translation of `org.apache.kafka.common.requests.SaslAuthenticateResponse`.

use std::collections::HashMap;
use std::fmt;
use std::sync::OnceLock;

use crate::common::errors::KafkaError;
use crate::common::message::sasl_authenticate_response_data::SaslAuthenticateResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;

/// Translation of `org.apache.kafka.common.requests.SaslAuthenticateResponse`.
///
/// Response from SASL server which for a SASL challenge as defined by the
/// SASL protocol for the mechanism configured for the client.
pub struct SaslAuthenticateResponse {
    data: SaslAuthenticateResponseData,
}

impl SaslAuthenticateResponse {
    /// Mirrors `new SaslAuthenticateResponse(SaslAuthenticateResponseData)`.
    pub fn new(data: SaslAuthenticateResponseData) -> Self {
        SaslAuthenticateResponse { data }
    }

    /// Mirrors `SaslAuthenticateResponse.data()`.
    pub fn response_data(&self) -> &SaslAuthenticateResponseData {
        &self.data
    }

    /// Mirrors `SaslAuthenticateResponse.error()`.
    ///
    /// Possible error codes:
    /// - `SASL_AUTHENTICATION_FAILED(57)` *(misnumbered in Java's javadoc;
    ///   the actual wire code is 58)*: Authentication failed
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Mirrors `SaslAuthenticateResponse.errorMessage()`. May be `None` when
    /// the response has no error (or the broker omitted the message).
    pub fn error_message(&self) -> Option<&str> {
        self.data.error_message.as_deref()
    }

    /// Mirrors `SaslAuthenticateResponse.sessionLifetimeMs()`. Only populated
    /// from v1 onwards; defaults to 0 at v0.
    pub fn session_lifetime_ms(&self) -> i64 {
        self.data.session_lifetime_ms
    }

    /// Mirrors `SaslAuthenticateResponse.saslAuthBytes()`.
    pub fn sasl_auth_bytes(&self) -> &[u8] {
        &self.data.auth_bytes
    }

    /// Mirrors `SaslAuthenticateResponse.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = SaslAuthenticateResponseData::read(accessor, version)?;
        Ok(SaslAuthenticateResponse::new(data))
    }
}

impl AbstractRequestResponse for SaslAuthenticateResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for SaslAuthenticateResponse {
    fn api_key(&self) -> &'static ApiKey {
        static SASL_AUTHENTICATE: OnceLock<&'static ApiKey> = OnceLock::new();
        SASL_AUTHENTICATE
            .get_or_init(|| ApiKeys::for_id(36).expect("SASL_AUTHENTICATE api_key always present in ALL_API_KEYS"))
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        abstract_response::error_counts_one(Errors::for_code(self.data.error_code))
    }

    fn throttle_time_ms(&self) -> i32 {
        // Java: `return DEFAULT_THROTTLE_TIME;` — `SaslAuthenticateResponse`
        // schema has no `throttle_time_ms` field.
        abstract_response::DEFAULT_THROTTLE_TIME
    }

    fn maybe_set_throttle_time_ms(&mut self, _throttle_time_ms: i32) {
        // Not supported by the response schema (matches Java's no-op).
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl fmt::Debug for SaslAuthenticateResponse {
    /// Mirrors Java's `toString()` override which masks `authBytes` because
    /// they may contain credentials.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslAuthenticateResponse")
            .field("error_code", &self.data.error_code)
            .field("error_message", &self.data.error_message)
            .field("auth_bytes", &"<redacted>")
            .field("session_lifetime_ms", &self.data.session_lifetime_ms)
            .field("unknown_tagged_fields", &self.data.unknown_tagged_fields)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip at v0 (pre-flexible): error_message uses `int16` length
    /// prefix, auth_bytes uses `int32` length prefix; no session_lifetime_ms
    /// (v1+), no tagged-field trailer.
    #[test]
    fn round_trip_v0_pre_flexible() {
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            // Java's RequestResponseTest fixture uses `null` for the
            // error_message on a non-error response.
            error_message: None,
            auth_bytes: Vec::new(),
            // v0 has no session_lifetime_ms field — default 0 should not
            // be written nor re-read.
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 0).expect("serialize");
        let parsed = SaslAuthenticateResponse::parse(&mut serialized, 0).expect("parse");
        assert_eq!(parsed.error(), Errors::None);
        assert_eq!(parsed.error_message(), None);
        assert!(parsed.sasl_auth_bytes().is_empty());
        // v0 has no session_lifetime_ms field — should remain at default.
        assert_eq!(parsed.session_lifetime_ms(), 0);
    }

    /// Round-trip at v1: adds `session_lifetime_ms` (i64). Mirrors Java's
    /// `createSaslAuthenticateResponse()` fixture (uses `Long.MAX_VALUE`).
    #[test]
    fn round_trip_v1_with_session_lifetime() {
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: Vec::new(),
            session_lifetime_ms: i64::MAX,
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 1).expect("serialize");
        let parsed = SaslAuthenticateResponse::parse(&mut serialized, 1).expect("parse");
        assert_eq!(parsed.error(), Errors::None);
        assert_eq!(parsed.session_lifetime_ms(), i64::MAX);
    }

    /// Round-trip at v2 (flex boundary +1): error_message uses compact-
    /// nullable-string, auth_bytes uses compact-bytes, plus a varint
    /// tagged-field trailer.
    #[test]
    fn round_trip_v2_flexible_with_error_message() {
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::SaslAuthenticationFailed.code(),
            error_message: Some("Authentication failed: Invalid username or password".to_owned()),
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 2).expect("serialize");
        let parsed = SaslAuthenticateResponse::parse(&mut serialized, 2).expect("parse");
        assert_eq!(parsed.error(), Errors::SaslAuthenticationFailed);
        assert_eq!(
            parsed.error_message(),
            Some("Authentication failed: Invalid username or password")
        );
    }

    /// Round-trip at v2 with auth_bytes (server challenge from a real
    /// SASL exchange). Verifies compact-bytes encoding survives the
    /// trip.
    #[test]
    fn round_trip_v2_with_server_challenge() {
        let challenge = b"server-challenge-bytes-12345";
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: challenge.to_vec(),
            session_lifetime_ms: 3_600_000, // 1 hour
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 2).expect("serialize");
        let parsed = SaslAuthenticateResponse::parse(&mut serialized, 2).expect("parse");
        assert_eq!(parsed.sasl_auth_bytes(), challenge);
        assert_eq!(parsed.session_lifetime_ms(), 3_600_000);
    }

    /// Throttle-time getter is the constant `DEFAULT_THROTTLE_TIME` (0)
    /// because the response schema has no throttle_time field.
    #[test]
    fn throttle_time_is_default() {
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData::new());
        assert_eq!(resp.throttle_time_ms(), 0);
    }

    /// `maybeSetThrottleTimeMs` is a no-op (Java parity).
    #[test]
    fn maybe_set_throttle_time_ms_no_op() {
        let mut resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData::new());
        resp.maybe_set_throttle_time_ms(42);
        assert_eq!(resp.throttle_time_ms(), 0);
    }

    /// Translation of Java
    /// `testSaslAuthenticateRequestResponseToStringMasksSensitiveData` for
    /// the response side. Debug output must not leak `authBytes`.
    ///
    /// **Test-only leak hygiene** (Critic 9 Suggestion 2): on assertion
    /// failure, do NOT echo the full Debug string. Reference a sentinel
    /// substring only.
    #[test]
    fn debug_masks_auth_bytes() {
        const SENTINEL: &str = "sensitive-auth-token-123";
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: 0,
            error_message: None,
            auth_bytes: SENTINEL.as_bytes().to_vec(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        let dbg = format!("{resp:?}");
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
    /// Display on the data class must NOT leak `auth_bytes` either. See
    /// the analogous test in `sasl_authenticate_request.rs` for the full
    /// rationale.
    #[test]
    fn data_class_debug_and_display_redact_auth_bytes() {
        const SENTINEL: &str = "sensitive-auth-token-123";
        let data = SaslAuthenticateResponseData {
            error_code: 0,
            error_message: None,
            auth_bytes: SENTINEL.as_bytes().to_vec(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        };
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

    /// Error code mapping: SASL_AUTHENTICATION_FAILED (58) is the
    /// canonical "wrong credentials" error.
    #[test]
    fn sasl_authentication_failed_error_counts() {
        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::SaslAuthenticationFailed.code(),
            error_message: Some("Authentication failed".to_owned()),
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        assert_eq!(resp.error_counts().get(&Errors::SaslAuthenticationFailed), Some(&1));
    }

    /// Hex fixture: SaslAuthenticateResponse v0 body — non-flexible
    /// encoding, no session_lifetime_ms field.
    ///
    /// **Fixture provenance**: hand-derived from
    /// `SaslAuthenticateResponse.json` (apiKey 36, `flexibleVersions:
    /// "2+"`, `session_lifetime_ms` field starts at v1+) — at v0 the
    /// body has only error_code + nullable error_message + auth_bytes.
    /// Awaiting Java-runtime byte capture from the Apache Kafka 4.2
    /// broker.
    ///
    /// Wire layout (8 bytes total):
    /// - `00 00` — i16 error_code = 0
    /// - `FF FF` — i16 error_message length = -1 (null)
    /// - `00 00 00 00` — i32 auth_bytes length = 0
    #[test]
    fn hex_fixture_v0_success_null_message() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, // error_code = 0
            0xFF, 0xFF, // error_message length = -1 (null)
            0x00, 0x00, 0x00, 0x00, // auth_bytes length = 0
        ];

        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0, // Should NOT be written at v0
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 0).expect("serialize v0");
        assert_eq!(
            serialized.buffer(),
            EXPECTED,
            "SaslAuthenticateResponse v0 (success, null msg) bytes diverged from the hex fixture"
        );
    }

    /// Hex fixture: SaslAuthenticateResponse v1 body — adds an i64
    /// session_lifetime_ms tail. Mirrors Java's
    /// `createSaslAuthenticateResponse()` fixture (`Long.MAX_VALUE`).
    ///
    /// **Fixture provenance**: hand-derived.
    ///
    /// Wire layout (16 bytes total):
    /// - `00 00` — error_code = 0
    /// - `FF FF` — error_message = null
    /// - `00 00 00 00` — auth_bytes length = 0
    /// - `7F FF FF FF FF FF FF FF` — i64 session_lifetime_ms =
    ///   i64::MAX (Java `Long.MAX_VALUE`)
    #[test]
    fn hex_fixture_v1_success_long_max_session() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, // error_code = 0
            0xFF, 0xFF, // error_message = null
            0x00, 0x00, 0x00, 0x00, // auth_bytes length = 0
            0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // session_lifetime_ms = i64::MAX
        ];

        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: Vec::new(),
            session_lifetime_ms: i64::MAX,
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 1).expect("serialize v1");
        assert_eq!(serialized.buffer(), EXPECTED);
    }

    /// Hex fixture: SaslAuthenticateResponse v2 body — flexible
    /// encoding. The flex boundary (v1→v2) is the highest-priority byte
    /// fidelity test per CLAUDE.md "wire-protocol byte-vector
    /// divergence" risk #1.
    ///
    /// **Fixture provenance**: hand-derived from
    /// `SaslAuthenticateResponse.json` (`flexibleVersions: "2+"`) —
    /// at v2 error_message uses compact-nullable-string (uvarint(0) for
    /// null), auth_bytes uses compact-bytes, and a zero-length
    /// tagged-field trailer follows.
    ///
    /// Wire layout (13 bytes total):
    /// - `00 00` — error_code = 0
    /// - `00` — varint(0) = error_message is null
    /// - `01` — varint(1) = auth_bytes length+1 for empty payload
    /// - `00 00 00 00 00 00 00 00` — session_lifetime_ms = 0
    /// - `00` — varint(0) = zero tagged fields
    #[test]
    fn hex_fixture_v2_success_null_message_empty_auth() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, // error_code = 0
            0x00, // error_message: varint(0) = null
            0x01, // auth_bytes: varint(1) = empty bytes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
            0x00, // tagged-field count = 0
        ];

        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::None.code(),
            error_message: None,
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 2).expect("serialize v2");
        assert_eq!(
            serialized.buffer(),
            EXPECTED,
            "SaslAuthenticateResponse v2 (flex boundary, success) bytes diverged from the hex fixture"
        );
    }

    /// Hex fixture: SaslAuthenticateResponse v2 body for an authentication
    /// failure — error_code 58 plus a non-empty `errorMessage`. Exercises
    /// the compact-nullable-string non-null branch.
    ///
    /// **Fixture provenance**: hand-derived.
    ///
    /// Wire layout:
    /// - `00 3A` — error_code = 58 (SASL_AUTHENTICATION_FAILED)
    /// - `05` — varint(5) = error_message length+1 for 4-byte payload
    /// - 4 bytes — "fail"
    /// - `01` — auth_bytes length+1 = 1 → empty
    /// - 8 bytes — session_lifetime_ms = 0
    /// - `00` — tagged-field count = 0
    #[test]
    fn hex_fixture_v2_auth_failed_with_message() {
        const EXPECTED: &[u8] = &[
            0x00, 0x3A, // error_code = 58
            0x05, // varint(5) = error_message length+1
            b'f', b'a', b'i', b'l', // "fail"
            0x01, // varint(1) = empty auth_bytes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // session_lifetime_ms = 0
            0x00, // tagged-field count = 0
        ];

        let resp = SaslAuthenticateResponse::new(SaslAuthenticateResponseData {
            error_code: Errors::SaslAuthenticationFailed.code(),
            error_message: Some("fail".to_owned()),
            auth_bytes: Vec::new(),
            session_lifetime_ms: 0,
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 2).expect("serialize v2");
        assert_eq!(serialized.buffer(), EXPECTED);
    }
}
