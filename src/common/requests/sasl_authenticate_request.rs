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
