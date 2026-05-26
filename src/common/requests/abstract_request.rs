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

//! Translation of `org.apache.kafka.common.requests.AbstractRequest`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::protocol::ApiKey;
use crate::common::protocol::Errors;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::message_util::to_byte_buffer_accessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::RequestHeader;
use crate::common::requests::request_utils;

/// Abstract base trait for request wrappers — translation of
/// `org.apache.kafka.common.requests.AbstractRequest`.
///
/// The Java class is an abstract class with concrete state (`version`,
/// `apiKey`) and abstract methods (`data()`, `getErrorResponse(...)`).
/// In Rust we model it as a trait whose state-bearing methods (`version`,
/// `api_key`, `data`) are required and the helpers built on top
/// (`serialize`, `serialize_with_header`, `size_in_bytes`,
/// `error_counts`) are provided default methods.
pub trait AbstractRequest: AbstractRequestResponse + std::marker::Send + std::marker::Sync {
    /// The API version this request will be encoded at. Mirrors
    /// `AbstractRequest.version()`.
    fn version(&self) -> i16;

    /// The API key. Mirrors `AbstractRequest.apiKey()`.
    fn api_key(&self) -> &'static ApiKey;

    /// Build an error response for this request. Mirrors
    /// `getErrorResponse(int throttleTimeMs, Throwable e)`. Returns `None`
    /// when the request type does not produce a response (e.g. produce with
    /// acks=0).
    fn get_error_response(&self, throttle_time_ms: i32, error: &KafkaError) -> Option<Box<dyn AbstractResponse>>;

    /// Convenience overload of [`Self::get_error_response`] that uses
    /// [`crate::common::requests::abstract_response::DEFAULT_THROTTLE_TIME`].
    fn get_error_response_default_throttle(&self, error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
        self.get_error_response(crate::common::requests::abstract_response::DEFAULT_THROTTLE_TIME, error)
    }

    /// Test-visible: serialize the request's body into a fresh
    /// `ByteBufferAccessor`. Mirrors `AbstractRequest.serialize()`.
    fn serialize(&self) -> Result<ByteBufferAccessor, KafkaError> {
        to_byte_buffer_accessor(self.data(), self.version())
    }

    /// Test-visible: total size of the encoded request body. Mirrors
    /// `AbstractRequest.sizeInBytes()`.
    fn size_in_bytes(&self) -> i32 {
        let mut cache = ObjectSerializationCache::new();
        let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
        self.data().add_size(&mut sizer, &mut cache, self.version());
        sizer.total_size()
    }

    /// Mirrors `AbstractRequest.serializeWithHeader(RequestHeader header)`.
    /// Validates that the header's `apiKey`/`apiVersion` agree with `self`,
    /// then concatenates header+body bytes (no length prefix).
    fn serialize_with_header(&self, header: &RequestHeader) -> Result<Vec<u8>, KafkaError> {
        let header_api_key = header.api_key()?;
        if header_api_key.id != self.api_key().id {
            return Err(KafkaError::IllegalArgument(format!(
                "Could not build request {} with header api key {}",
                self.api_key().name,
                header_api_key.name
            )));
        }
        if header.api_version() != self.version() {
            return Err(KafkaError::IllegalArgument(format!(
                "Could not build request version {} with header version {}",
                self.version(),
                header.api_version()
            )));
        }
        request_utils::serialize(header.header_data(), header.header_version(), self.data(), self.version())
    }

    /// Mirrors `AbstractRequest.errorCounts(Throwable e)`.
    fn error_counts(&self, error: &KafkaError) -> Result<HashMap<Errors, i32>, KafkaError> {
        match self.get_error_response(0, error) {
            None => Err(KafkaError::IllegalArgument(format!(
                "Error counts could not be obtained for request {}",
                self.api_key().name
            ))),
            Some(resp) => Ok(resp.error_counts()),
        }
    }
}
