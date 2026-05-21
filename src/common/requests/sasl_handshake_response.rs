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

//! Translation of `org.apache.kafka.common.requests.SaslHandshakeResponse`.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::common::errors::KafkaError;
use crate::common::message::sasl_handshake_response_data::SaslHandshakeResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;

/// Translation of `org.apache.kafka.common.requests.SaslHandshakeResponse`.
///
/// Response from SASL server which indicates if the client-chosen mechanism
/// is enabled in the server. For error responses, the list of enabled
/// mechanisms is included in the response.
pub struct SaslHandshakeResponse {
    data: SaslHandshakeResponseData,
}

impl SaslHandshakeResponse {
    /// Mirrors `new SaslHandshakeResponse(SaslHandshakeResponseData)`.
    pub fn new(data: SaslHandshakeResponseData) -> Self {
        SaslHandshakeResponse { data }
    }

    /// Mirrors `SaslHandshakeResponse.data()`.
    pub fn response_data(&self) -> &SaslHandshakeResponseData {
        &self.data
    }

    /// Mirrors `SaslHandshakeResponse.error()`.
    ///
    /// Possible error codes:
    /// - `UNSUPPORTED_SASL_MECHANISM(33)`: Client mechanism not enabled in server
    /// - `ILLEGAL_SASL_STATE(34)`: Invalid request during SASL handshake
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Mirrors `SaslHandshakeResponse.enabledMechanisms()`.
    pub fn enabled_mechanisms(&self) -> &[String] {
        &self.data.mechanisms
    }

    /// Mirrors `SaslHandshakeResponse.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = SaslHandshakeResponseData::read(accessor, version)?;
        Ok(SaslHandshakeResponse::new(data))
    }
}

impl AbstractRequestResponse for SaslHandshakeResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for SaslHandshakeResponse {
    fn api_key(&self) -> &'static ApiKey {
        static SASL_HANDSHAKE: OnceLock<&'static ApiKey> = OnceLock::new();
        SASL_HANDSHAKE
            .get_or_init(|| ApiKeys::for_id(17).expect("SASL_HANDSHAKE api_key always present in ALL_API_KEYS"))
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        abstract_response::error_counts_one(Errors::for_code(self.data.error_code))
    }

    fn throttle_time_ms(&self) -> i32 {
        // Java: `return DEFAULT_THROTTLE_TIME;` — `SaslHandshakeResponse`
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip at v0: matches Java `createSaslHandshakeResponse()` from
    /// `RequestResponseTest.java` (errorCode = NONE, mechanisms = ["GSSAPI"]).
    #[test]
    fn round_trip_v0_kafka_request_response_test_fixture() {
        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::None.code(),
            mechanisms: vec!["GSSAPI".to_owned()],
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 0).expect("serialize");
        let parsed = SaslHandshakeResponse::parse(&mut serialized, 0).expect("parse");
        assert_eq!(parsed.error(), Errors::None);
        assert_eq!(parsed.enabled_mechanisms(), &["GSSAPI".to_owned()]);
    }

    /// Round-trip at v1: spec annotates "Version 1 is the same as version 0".
    #[test]
    fn round_trip_v1_plain_only() {
        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::None.code(),
            mechanisms: vec!["PLAIN".to_owned()],
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 1).expect("serialize");
        let parsed = SaslHandshakeResponse::parse(&mut serialized, 1).expect("parse");
        assert_eq!(parsed.error(), Errors::None);
        assert_eq!(parsed.enabled_mechanisms(), &["PLAIN".to_owned()]);
    }

    /// Multiple-mechanism array shape.
    #[test]
    fn round_trip_multiple_mechanisms() {
        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::None.code(),
            mechanisms: vec![
                "PLAIN".to_owned(),
                "SCRAM-SHA-256".to_owned(),
                "SCRAM-SHA-512".to_owned(),
                "OAUTHBEARER".to_owned(),
            ],
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 1).expect("serialize");
        let parsed = SaslHandshakeResponse::parse(&mut serialized, 1).expect("parse");
        assert_eq!(parsed.enabled_mechanisms().len(), 4);
        assert_eq!(parsed.enabled_mechanisms()[0], "PLAIN");
        assert_eq!(parsed.enabled_mechanisms()[3], "OAUTHBEARER");
    }

    /// Error-path: broker rejected the chosen mechanism. Error code 33 is
    /// `UNSUPPORTED_SASL_MECHANISM`. The response should still parse.
    #[test]
    fn round_trip_unsupported_sasl_mechanism_error() {
        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::UnsupportedSaslMechanism.code(),
            mechanisms: vec!["PLAIN".to_owned()],
            unknown_tagged_fields: Vec::new(),
        });
        let mut serialized = AbstractResponse::serialize(&resp, 1).expect("serialize");
        let parsed = SaslHandshakeResponse::parse(&mut serialized, 1).expect("parse");
        assert_eq!(parsed.error(), Errors::UnsupportedSaslMechanism);
        assert_eq!(parsed.error_counts().get(&Errors::UnsupportedSaslMechanism), Some(&1));
    }

    /// Throttle-time getter is the constant `DEFAULT_THROTTLE_TIME` (0)
    /// because the response schema has no throttle_time field.
    #[test]
    fn throttle_time_is_default() {
        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData::new());
        assert_eq!(resp.throttle_time_ms(), 0);
    }

    /// `maybeSetThrottleTimeMs` is a no-op (Java parity).
    #[test]
    fn maybe_set_throttle_time_ms_no_op() {
        let mut resp = SaslHandshakeResponse::new(SaslHandshakeResponseData::new());
        resp.maybe_set_throttle_time_ms(42);
        assert_eq!(resp.throttle_time_ms(), 0);
    }

    /// Hex fixture: SaslHandshakeResponse v0 body — `errorCode=NONE`,
    /// `mechanisms=["GSSAPI"]`. Mirrors the
    /// `createSaslHandshakeResponse()` fixture in Java's
    /// `RequestResponseTest.java`.
    ///
    /// **Fixture provenance**: hand-derived from `SaslHandshakeResponse.json`
    /// (apiKey 17, `flexibleVersions: "none"`) — awaiting Java-runtime
    /// byte capture from the Apache Kafka 4.2 broker/test fixture.
    /// Verified by inspection against the non-flexible encoding rules
    /// for `int16` and `[]string`.
    ///
    /// Wire layout:
    /// - `00 00` — error_code (i16) = 0
    /// - `00 00 00 01` — array length (i32) = 1
    /// - `00 06` — mechanism string length (i16) = 6
    /// - 6 bytes — "GSSAPI"
    ///
    /// Total: 14 bytes.
    #[test]
    fn hex_fixture_v0_gssapi_only() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, // error_code = 0
            0x00, 0x00, 0x00, 0x01, // array length = 1
            0x00, 0x06, // mechanism length = 6
            b'G', b'S', b'S', b'A', b'P', b'I', // "GSSAPI"
        ];

        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::None.code(),
            mechanisms: vec!["GSSAPI".to_owned()],
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 0).expect("serialize v0");
        assert_eq!(
            serialized.buffer(),
            EXPECTED,
            "SaslHandshakeResponse v0 (mechanisms=[GSSAPI]) bytes diverged from the hex fixture"
        );
    }

    /// Hex fixture: SaslHandshakeResponse v1 body — same shape as v0
    /// (spec annotates v1 as identical to v0). Mechanism list is
    /// `["PLAIN"]` here for variety.
    ///
    /// **Fixture provenance**: hand-derived.
    ///
    /// Wire layout:
    /// - `00 00` — error_code = 0
    /// - `00 00 00 01` — array length = 1
    /// - `00 05` — mechanism length = 5
    /// - 5 bytes — "PLAIN"
    #[test]
    fn hex_fixture_v1_plain_only() {
        const EXPECTED: &[u8] = &[
            0x00, 0x00, // error_code = 0
            0x00, 0x00, 0x00, 0x01, // array length = 1
            0x00, 0x05, // mechanism length = 5
            b'P', b'L', b'A', b'I', b'N',
        ];

        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::None.code(),
            mechanisms: vec!["PLAIN".to_owned()],
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 1).expect("serialize v1");
        assert_eq!(serialized.buffer(), EXPECTED);
    }

    /// Hex fixture: SaslHandshakeResponse v1 body for an error case —
    /// `errorCode=UNSUPPORTED_SASL_MECHANISM (33)`, `mechanisms=[]`
    /// (broker rejected, no mechanisms enabled — atypical, but used to
    /// pin the i32(0) empty-array encoding).
    ///
    /// **Fixture provenance**: hand-derived.
    ///
    /// Wire layout:
    /// - `00 21` — error_code = 33 (UNSUPPORTED_SASL_MECHANISM)
    /// - `00 00 00 00` — array length = 0
    #[test]
    fn hex_fixture_v1_error_empty_mechanisms() {
        const EXPECTED: &[u8] = &[
            0x00, 0x21, // error_code = 33
            0x00, 0x00, 0x00, 0x00, // array length = 0
        ];

        let resp = SaslHandshakeResponse::new(SaslHandshakeResponseData {
            error_code: Errors::UnsupportedSaslMechanism.code(),
            mechanisms: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        });
        let serialized = AbstractResponse::serialize(&resp, 1).expect("serialize v1");
        assert_eq!(serialized.buffer(), EXPECTED);
    }
}
