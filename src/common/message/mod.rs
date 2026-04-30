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

//! Translation of `org.apache.kafka.common.message`.
//!
//! This is the runtime side of the generator-emitted code. The contents of
//! the various `*_data.rs` files are emitted by `generator/` from the JSON
//! specs in `generator/messages/` at build time and end up in
//! `$OUT_DIR/generated/`. We pull each one in selectively via the
//! `include!` macro so we can incrementally widen the surface as the
//! generator emit code is brought in line with the runtime traits.
//!
//! Phase 2d-1 wires up `api_message_type` (used by the `ApiKey` ↔
//! `ApiMessageType` drift test) and `request_header_data` (the
//! proof-of-concept generated message). Phase 2d-2 additively wires
//! `response_header_data`, `api_versions_request_data`, and
//! `api_versions_response_data`. Phase 2d-3/4 will continue to include
//! the remaining generated message data structs as their wire-protocol
//! round-trip tests are added.

/// `ApiMessageType` enum, generated from the message JSON specs. Mirrors
/// Java's generated `org.apache.kafka.common.message.ApiMessageType`.
pub mod api_message_type {
    include!(concat!(env!("OUT_DIR"), "/generated/api_message_type.rs"));
}

/// `RequestHeaderData`, generated from `RequestHeader.json`. Mirrors Java's
/// generated `org.apache.kafka.common.message.RequestHeaderData`.
pub mod request_header_data {
    include!(concat!(env!("OUT_DIR"), "/generated/request_header_data.rs"));
}

/// `ResponseHeaderData`, generated from `ResponseHeader.json`. Mirrors Java's
/// generated `org.apache.kafka.common.message.ResponseHeaderData`.
pub mod response_header_data {
    include!(concat!(env!("OUT_DIR"), "/generated/response_header_data.rs"));
}

/// `ApiVersionsRequestData`, generated from `ApiVersionsRequest.json`.
/// Mirrors Java's generated
/// `org.apache.kafka.common.message.ApiVersionsRequestData`.
pub mod api_versions_request_data {
    include!(concat!(env!("OUT_DIR"), "/generated/api_versions_request_data.rs"));
}

/// `ApiVersionsResponseData`, generated from `ApiVersionsResponse.json`.
/// Mirrors Java's generated
/// `org.apache.kafka.common.message.ApiVersionsResponseData`.
pub mod api_versions_response_data {
    include!(concat!(env!("OUT_DIR"), "/generated/api_versions_response_data.rs"));
}

/// `MetadataRequestData`, generated from `MetadataRequest.json`. Mirrors
/// Java's generated `org.apache.kafka.common.message.MetadataRequestData`.
pub mod metadata_request_data {
    include!(concat!(env!("OUT_DIR"), "/generated/metadata_request_data.rs"));
}

/// `MetadataResponseData`, generated from `MetadataResponse.json`. Mirrors
/// Java's generated `org.apache.kafka.common.message.MetadataResponseData`.
pub mod metadata_response_data {
    include!(concat!(env!("OUT_DIR"), "/generated/metadata_response_data.rs"));
}

/// `ProduceRequestData`, generated from `ProduceRequest.json`. Mirrors
/// Java's generated `org.apache.kafka.common.message.ProduceRequestData`.
pub mod produce_request_data {
    include!(concat!(env!("OUT_DIR"), "/generated/produce_request_data.rs"));
}

/// `ProduceResponseData`, generated from `ProduceResponse.json`. Mirrors
/// Java's generated `org.apache.kafka.common.message.ProduceResponseData`.
pub mod produce_response_data {
    include!(concat!(env!("OUT_DIR"), "/generated/produce_response_data.rs"));
}

#[cfg(test)]
mod tests;
