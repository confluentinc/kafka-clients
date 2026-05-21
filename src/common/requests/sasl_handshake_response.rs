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
