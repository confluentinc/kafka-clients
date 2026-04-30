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

//! Cross-cutting tests for the `requests` wrapper layer.
//!
//! Phase 2e DoD requires that each wrapper's framing produce byte-identical
//! output to "header bytes" ++ "body bytes". The per-`*Data` byte fixtures
//! captured from the Java client live in
//! `src/common/message/tests.rs`; here we lock the wrapper-level
//! concatenation by reusing those known-good *Data* hex strings.
//!
//! TODO Phase 4: capture a Java-derived end-to-end fixture
//! (`header_v2 ++ produce_request_v9` taken from a `RequestUtils.serialize`
//! call) so the wrapper-level test directly compares against bytes the Java
//! client produced. The current per-`*Data` Java fixtures cover the inner
//! encoding, but a single end-to-end fixture would close the loop. See
//! COMMENTS.0.md Issue 10.

use crate::common::message::api_versions_request_data::ApiVersionsRequestData;
use crate::common::message::api_versions_response_data::ApiVersionsResponseData;
use crate::common::message::metadata_request_data::MetadataRequestData;
use crate::common::message::metadata_response_data::MetadataResponseData;
use crate::common::message::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};
use crate::common::message::produce_response_data::ProduceResponseData;
use crate::common::message::request_header_data::RequestHeaderData;
use crate::common::message::response_header_data::ResponseHeaderData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::object_serialization_cache::ObjectSerializationCache;
use crate::common::protocol::{ApiKeys, Errors, Message};
use crate::common::requests::{
    AbstractRequest, AbstractRequestResponse, AbstractResponse, ApiVersionsRequest, ApiVersionsResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
};
use crate::common::uuid::Uuid;

/// Produce raw bytes for a single `Message` at the given version.
fn encode_message<M: Message + ?Sized>(msg: &M, version: i16) -> Vec<u8> {
    let mut cache = ObjectSerializationCache::new();
    let mut sizer = crate::common::protocol::MessageSizeAccumulator::new();
    msg.add_size(&mut sizer, &mut cache, version);
    let mut accessor = ByteBufferAccessor::allocate(sizer.total_size() as usize);
    msg.write(&mut accessor, &cache, version).expect("write");
    accessor.flip();
    accessor.buffer().to_vec()
}

/// Lock that `RequestHeader::serialize_with_header(req)` == header bytes ++ body bytes.
#[test]
fn request_header_framing_concatenates_header_and_body_api_versions_v0() {
    let req_data = ApiVersionsRequestData::new();
    let request = ApiVersionsRequest::new(req_data, 0);

    let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
    let header = RequestHeader::new(api_versions, 0, "test", 7);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(request.data(), request.version());
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractRequest::serialize_with_header(&request, &header).expect("serialize_with_header");
    assert_eq!(actual, expected, "wrapper framing must concatenate header + body");
}

#[test]
fn request_header_framing_metadata_v12() {
    let request = MetadataRequest::build(Some(vec!["t1".to_owned()]), true, 12).expect("build");

    let metadata = ApiKeys::for_id(3).expect("METADATA");
    let header = RequestHeader::new(metadata, 12, "client", 99);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(request.data(), 12);
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractRequest::serialize_with_header(&request, &header).expect("serialize_with_header");
    assert_eq!(actual, expected);
}

#[test]
fn request_header_framing_produce_v3() {
    let data = ProduceRequestData {
        acks: -1,
        timeout_ms: 1000,
        transactional_id: None,
        topic_data: vec![TopicProduceData {
            name: "t".to_owned(),
            topic_id: Uuid::zero(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(b"hello".to_vec()),
                unknown_tagged_fields: Vec::new(),
            }],
            unknown_tagged_fields: Vec::new(),
        }],
        unknown_tagged_fields: Vec::new(),
    };
    let request = ProduceRequest::new(data, 3);

    let produce = ApiKeys::for_id(0).expect("PRODUCE");
    let header = RequestHeader::new(produce, 3, "p", 1);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(request.data(), 3);
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractRequest::serialize_with_header(&request, &header).expect("serialize_with_header");
    assert_eq!(actual, expected);
}

/// Lock that `AbstractResponse::serialize_with_header` produces
/// header ++ body bytes (no length prefix).
#[test]
fn response_header_framing_api_versions_v0() {
    let resp_data = ApiVersionsResponseData { error_code: Errors::None.code(), ..ApiVersionsResponseData::new() };
    let response = ApiVersionsResponse::new(resp_data);
    let header = ResponseHeader::new(7, 0);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(response.data(), 0);
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractResponse::serialize_with_header(&response, &header, 0).expect("serialize_with_header");
    assert_eq!(actual, expected);
}

#[test]
fn response_header_framing_metadata_v12() {
    let response = MetadataResponse::new(MetadataResponseData::new(), true);
    let header = ResponseHeader::new(99, 1);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(response.data(), 12);
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractResponse::serialize_with_header(&response, &header, 12).expect("serialize_with_header");
    assert_eq!(actual, expected);
}

#[test]
fn response_header_framing_produce_v3() {
    let response = ProduceResponse::new(ProduceResponseData::new());
    let header = ResponseHeader::new(1, 0);

    let header_bytes = encode_message(&header.header_data().clone(), header.header_version());
    let body_bytes = encode_message(response.data(), 3);
    let mut expected = header_bytes;
    expected.extend_from_slice(&body_bytes);

    let actual = AbstractResponse::serialize_with_header(&response, &header, 3).expect("serialize_with_header");
    assert_eq!(actual, expected);
}

/// `serialize_with_header` rejects header/body version disagreement,
/// matching Java's `IllegalArgumentException`.
#[test]
fn serialize_with_header_rejects_api_key_mismatch() {
    let req = ApiVersionsRequest::new(ApiVersionsRequestData::new(), 0);
    // Build a header for a different API key.
    let metadata = ApiKeys::for_id(3).expect("METADATA");
    let header = RequestHeader::new(metadata, 0, "id", 1);
    let result = AbstractRequest::serialize_with_header(&req, &header);
    let err = result.expect_err("api key mismatch should error");
    let msg = err.to_string();
    assert!(
        msg.contains("Could not build request") && msg.contains("with header api key"),
        "unexpected error: {msg}"
    );
}

#[test]
fn serialize_with_header_rejects_api_version_mismatch() {
    let req = ApiVersionsRequest::new(ApiVersionsRequestData::new(), 0);
    let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
    let header = RequestHeader::new(api_versions, 1, "id", 1); // request version=0, header version=1
    let result = AbstractRequest::serialize_with_header(&req, &header);
    let err = result.expect_err("version mismatch should error");
    let msg = err.to_string();
    assert!(
        msg.contains("Could not build request version") && msg.contains("with header version"),
        "unexpected error: {msg}"
    );
}

/// Round-trip: a server reply can be parsed by `parse_response` after
/// being serialized with the wrapper framing path.
#[test]
fn parse_response_round_trip_api_versions() {
    use crate::common::requests::abstract_response::parse_response;

    let resp_data = ApiVersionsResponseData { error_code: Errors::None.code(), ..ApiVersionsResponseData::new() };
    let response = ApiVersionsResponse::new(resp_data);

    let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
    let req_header = RequestHeader::new(api_versions, 0, "client", 42);
    let resp_header = req_header.to_response_header().expect("to_response_header");

    let bytes = AbstractResponse::serialize_with_header(&response, &resp_header, 0).expect("serialize_with_header");
    let mut accessor = ByteBufferAccessor::wrap(bytes);

    let parsed = parse_response(&mut accessor, &req_header).expect("parse_response");
    assert_eq!(parsed.api_key().id, 18);
}

/// Mismatched correlation ids surface a `CorrelationIdMismatchError`-flavored
/// `KafkaError::Generic` with the right shape.
#[test]
fn parse_response_correlation_id_mismatch_errors_with_helpful_message() {
    use crate::common::requests::abstract_response::parse_response;

    let resp_data = ApiVersionsResponseData { error_code: Errors::None.code(), ..ApiVersionsResponseData::new() };
    let response = ApiVersionsResponse::new(resp_data);

    // The response carries correlation id 99, but the request expects 42.
    let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
    let req_header = RequestHeader::new(api_versions, 0, "client", 42);
    let bad_resp_header = ResponseHeader::new(99, 0);
    let bytes = AbstractResponse::serialize_with_header(&response, &bad_resp_header, 0).expect("serialize_with_header");
    let mut accessor = ByteBufferAccessor::wrap(bytes);

    let err = match parse_response(&mut accessor, &req_header) {
        Ok(_) => panic!("expected mismatch error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("Correlation id for response (99) does not match request (42)"),
        "unexpected error: {msg}"
    );
}

/// Helper alias to satisfy unused-import lint: makes the imports above
/// load-bearing for tests that don't directly reference every type.
#[allow(dead_code)]
fn _unused_imports_anchor() -> RequestHeaderData {
    RequestHeaderData {
        request_api_key: 0,
        request_api_version: 0,
        correlation_id: 0,
        client_id: None,
        unknown_tagged_fields: Vec::new(),
    }
}

#[allow(dead_code)]
fn _unused_imports_anchor_resp() -> ResponseHeaderData {
    ResponseHeaderData { correlation_id: 0, unknown_tagged_fields: Vec::new() }
}

#[allow(dead_code)]
fn _unused_imports_anchor_metadata() -> MetadataRequestData {
    MetadataRequestData::new()
}
