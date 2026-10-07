// Copyright 2026 Confluent Inc.
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

//! UnregisterController request handling (KAFKA-20395).
//!
//! Corresponds to `org.apache.kafka.common.requests.UnregisterControllerRequest`.

use std::io;

use crate::UnregisterControllerRequestData;
use crate::UnregisterControllerResponseData;
use crate::common::Error;
use crate::common::protocol::{ApiKeys, Readable};

use super::{AbstractRequest, ConcreteResponse, RequestBuilder, UnregisterControllerResponse};

/// An UnregisterController request.
///
/// Corresponds to `org.apache.kafka.common.requests.UnregisterControllerRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest")]
pub struct UnregisterControllerRequest {
    data: UnregisterControllerRequestData,
    version: i16,
}

impl UnregisterControllerRequest {
    /// Creates a new `UnregisterControllerRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest#UnregisterControllerRequest")]
    pub fn new(data: UnregisterControllerRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest#data")]
    pub fn data(&self) -> &UnregisterControllerRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut UnregisterControllerRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UNREGISTER_CONTROLLER
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `UnregisterControllerRequest.getErrorResponse(int, Throwable)`:
    /// the code is `Errors.forException(e)` ([`Error::error`]) and the message is
    /// `e.getMessage()` ([`Error::message`]). Java's `Throwable` is the crate's
    /// [`Error`] here, rather than the bare `Errors` code most wrappers take,
    /// because this response carries the throwable's own message and the Java
    /// test (`RequestResponseTest.testUnregisterControllerResponseWithUnknownServerError`)
    /// asserts that a custom message survives.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Error) -> ConcreteResponse {
        let mut data = UnregisterControllerResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(error.error().code());
        data.set_error_message(Some(error.message().to_string()));
        ConcreteResponse::UnregisterController(UnregisterControllerResponse::new(data))
    }

    /// Parses an `UnregisterControllerRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = UnregisterControllerRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for UnregisterControllerRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnregisterControllerRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`UnregisterControllerRequest`].
///
/// Corresponds to `UnregisterControllerRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest$Builder")]
pub struct Builder {
    data: UnregisterControllerRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl Builder {
    /// Creates a builder from the given request data.
    ///
    /// Java calls `super(ApiKeys.UNREGISTER_CONTROLLER)`, i.e. `latestVersion(false)`
    /// (producer-transactions.md §12).
    #[doc(alias = "org.apache.kafka.common.requests.UnregisterControllerRequest$Builder#Builder")]
    pub fn new(data: UnregisterControllerRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::UNREGISTER_CONTROLLER.oldest_version(),
            latest_allowed_version: ApiKeys::UNREGISTER_CONTROLLER.latest_version_enable_unstable_last_version(false),
        }
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::UNREGISTER_CONTROLLER
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        Ok(AbstractRequest::UnregisterController(UnregisterControllerRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{ByteBufferAccessor, Errors};

    /// Mirrors `RequestResponseTest.createUnregisterControllerRequest`.
    fn create_unregister_controller_request(version: i16) -> AbstractRequest {
        let mut data = UnregisterControllerRequestData::new();
        data.set_controller_id(1);
        Builder::new(data).build_version(version).unwrap()
    }

    /// Translated from
    /// `RequestResponseTest.testUnregisterControllerResponseWithUnknownServerError`:
    /// a generic runtime error maps to `UNKNOWN_SERVER_ERROR` and its own message
    /// is carried in the response. Java throws a `RuntimeException`; the crate's
    /// nearest generic runtime error is `LocalIllegalState`, which
    /// `Errors.forException` also maps to `UNKNOWN_SERVER_ERROR`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.RequestResponseTest#testUnregisterControllerResponseWithUnknownServerError"
    )]
    fn test_unregister_controller_response_with_unknown_server_error() {
        let mut builder = Builder::new(UnregisterControllerRequestData::new());
        let AbstractRequest::UnregisterController(request) = builder.build_version(0).unwrap() else {
            panic!("expected an UnregisterController request");
        };
        let custom_error_message = "custom error message";
        let response = request.get_error_response(0, &Error::local_illegal_state(custom_error_message));
        let ConcreteResponse::UnregisterController(response) = response else {
            panic!("expected an UnregisterController response");
        };
        assert_eq!(response.throttle_time_ms(), 0);
        assert_eq!(response.data().error_code(), Errors::UnknownServerError.code());
        assert_eq!(response.data().error_message().as_deref(), Some(custom_error_message));
    }

    /// The dispatcher's `get_error_response` (which only carries an `Errors`
    /// code) reaches this wrapper with the code's default message.
    #[test]
    fn dispatcher_error_response_uses_the_code_default_message() {
        let request = create_unregister_controller_request(0);
        let response = request.get_error_response(7, &Errors::NotController).unwrap();
        let ConcreteResponse::UnregisterController(response) = response else {
            panic!("expected an UnregisterController response");
        };
        assert_eq!(response.throttle_time_ms(), 7);
        assert_eq!(response.data().error_code(), Errors::NotController.code());
        assert_eq!(
            response.data().error_message().as_deref(),
            Some(Errors::NotController.message())
        );
    }

    /// Java's builder caps at the latest *released* version; v0 is the only one.
    #[test]
    fn builder_version_range_is_v0() {
        let builder = Builder::new(UnregisterControllerRequestData::new());
        assert_eq!(builder.oldest_allowed_version(), 0);
        assert_eq!(builder.latest_allowed_version(), 0);
        assert_eq!(builder.api_key().id(), 94);
    }

    /// Byte-level encoding test against a known vector. UnregisterController v0
    /// is flexible (`"flexibleVersions": "0+"`), so the body is:
    ///   controller_id: int32 = 1 (00 00 00 01)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut request = create_unregister_controller_request(0);
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, // controller_id = 1
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }

    /// The request header is v2 (flexible) for a flexible request: api_key 94,
    /// api_version 0, correlation id, compact-less nullable client id (`ClientId`
    /// overrides flexibleVersions to "none"), header tagged fields, then the body.
    #[test]
    fn serialize_with_header_known_byte_vector_v0() {
        use crate::common::requests::{RequestHeader, RequestHeaderOptionsBuilder};
        let mut request = create_unregister_controller_request(0);
        let header = RequestHeader::with_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::UNREGISTER_CONTROLLER)
                .set_request_version(0)
                .set_client_id("c")
                .set_correlation_id(5)
                .build()
                .unwrap(),
        )
        .unwrap();
        let bytes = request.serialize_with_header(&header).unwrap();
        let expected: &[u8] = &[
            0x00, 0x5e, // api_key = 94
            0x00, 0x00, // api_version = 0
            0x00, 0x00, 0x00, 0x05, // correlation_id = 5
            0x00, 0x01, 0x63, // client_id "c" (int16 length prefix)
            0x00, // header tagged fields
            0x00, 0x00, 0x00, 0x01, // controller_id = 1
            0x00, // body tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }

    /// Parses Java's known v0 vector back and through the shared
    /// `parse_request` dispatcher (the `AbstractRequest.parseRequest` arm).
    #[test]
    fn parse_known_byte_vector_v0() {
        let bytes = vec![0x00, 0x00, 0x00, 0x2a, 0x00];
        let mut readable = ByteBufferAccessor::new(bytes.clone());
        let parsed = UnregisterControllerRequest::parse(&mut readable, 0).unwrap();
        assert_eq!(parsed.data().controller_id(), 42);

        let mut readable = ByteBufferAccessor::new(bytes);
        let parsed = AbstractRequest::parse_request(&ApiKeys::UNREGISTER_CONTROLLER, 0, &mut readable).unwrap();
        let AbstractRequest::UnregisterController(parsed) = parsed.request() else {
            panic!("expected an UnregisterController request");
        };
        assert_eq!(parsed.data().controller_id(), 42);
        assert_eq!(parsed.version(), 0);
    }
}
