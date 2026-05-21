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

//! Translation of `org.apache.kafka.common.requests`.
//!
//! Wraps the generated `*Data` structs (see `crate::common::message`) with
//! the higher-level Java surface: `RequestHeader`, `ResponseHeader`, the
//! `AbstractRequest`/`AbstractResponse` traits, the per-API wrapper types,
//! and the helpers in `RequestUtils`.

pub mod abstract_request;
pub mod abstract_request_builder;
pub mod abstract_request_response;
pub mod abstract_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod correlation_id_mismatch_exception;
pub mod metadata_request;
pub mod metadata_response;
pub mod produce_request;
pub mod produce_response;
pub mod request_header;
pub mod request_utils;
pub mod response_header;
pub mod sasl_authenticate_request;
pub mod sasl_authenticate_response;
pub mod sasl_handshake_request;
pub mod sasl_handshake_response;

pub use abstract_request::AbstractRequest;
pub use abstract_request_builder::AbstractRequestBuilder;
pub use abstract_request_response::AbstractRequestResponse;
pub use abstract_response::AbstractResponse;
pub use abstract_response::parse_response;
pub use api_versions_request::ApiVersionsRequest;
pub use api_versions_request::ApiVersionsRequestBuilder;
pub use api_versions_response::ApiVersionsResponse;
pub use correlation_id_mismatch_exception::CorrelationIdMismatchError;
pub use metadata_request::MetadataRequest;
pub use metadata_request::MetadataRequestBuilder;
pub use metadata_response::MetadataResponse;
pub use produce_request::ProduceRequest;
pub use produce_response::ProduceResponse;
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::SaslAuthenticateRequest;
pub use sasl_authenticate_request::SaslAuthenticateRequestBuilder;
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::SaslHandshakeRequest;
pub use sasl_handshake_request::SaslHandshakeRequestBuilder;
pub use sasl_handshake_response::SaslHandshakeResponse;

#[cfg(test)]
mod tests;
