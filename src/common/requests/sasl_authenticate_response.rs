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
