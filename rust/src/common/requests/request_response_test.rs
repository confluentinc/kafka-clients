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

//! The private round-trip helpers of `RequestResponseTest`, shared by the
//! per-wrapper test modules.
//!
//! Rust distributes `org.apache.kafka.common.requests.RequestResponseTest` over
//! the wrapper files (each wrapper carries its own `testSerialization` arm and
//! `create*` helpers), so the three Java `private` helpers those arms call live
//! here once instead of being repeated in every file. Test-only module.

use std::collections::HashSet;

use crate::common::protocol::{ByteBufferAccessor, Errors};

use super::{AbstractRequest, ConcreteResponse};

/// Translates `RequestResponseTest#checkRequest`: the request serializes, parses
/// back, and serializes again to the same bytes.
pub(crate) fn check_request(request: &AbstractRequest) {
    let mut original = request.clone();
    let bytes = original.serialize().expect("serialize request").into_buffer();
    let mut parsed = AbstractRequest::parse_request(
        request.api_key(),
        request.version(),
        &mut ByteBufferAccessor::new(bytes.clone()),
    )
    .unwrap_or_else(|e| panic!("Failed to deserialize request {request}: {e}"))
    .request;
    let reserialized = parsed.serialize().expect("re-serialize request").into_buffer();
    assert_eq!(bytes, reserialized, "Request {request}failed equality test");
}

/// Translates `RequestResponseTest#checkResponse`: the response serializes at
/// `version`, parses back, and serializes again to the same bytes.
pub(crate) fn check_response(response: &ConcreteResponse, version: i16) {
    let mut original = response.clone();
    let bytes = original.serialize(version).expect("serialize response").into_buffer();
    let mut parsed = ConcreteResponse::parse(response.api_key(), &mut ByteBufferAccessor::new(bytes.clone()), version)
        .unwrap_or_else(|e| panic!("Failed to deserialize version {version} response {response}: {e}"));
    let reserialized = parsed.serialize(version).expect("re-serialize response").into_buffer();
    assert_eq!(bytes, reserialized, "Response {response}failed equality test");
}

/// Translates `RequestResponseTest#checkErrorResponse` for Java's
/// `unknownServerException` (`new UnknownServerException("secret")`).
///
/// The `AbstractRequest::get_error_response` dispatch takes the [`Errors`] code
/// that Java derives with `Errors.forException(e)`, so the `"secret"` message
/// never reaches a wrapper through it, and the "message not included" assertion
/// holds by construction here. A wrapper whose error response does copy the
/// throwable's message (e.g. `DescribeShareGroupOffsetsRequest`) asserts it
/// separately, with an error that carries `"secret"`.
pub(crate) fn check_error_response(request: &AbstractRequest) {
    let error = Errors::UnknownServerError;
    let response = request
        .get_error_response(0, &error)
        .expect("every request in this suite has an error response");
    check_response(&response, request.version());
    let error_counts = response.error_counts();
    assert_eq!(
        HashSet::from([error]),
        error_counts.keys().copied().collect::<HashSet<_>>(),
        "API Key {} v{} failed errorCounts test",
        request.api_key(),
        request.version()
    );
    assert!(error_counts[&error] > 0);
    assert!(
        !response.to_string().contains("secret"),
        "Unknown message included in response for {}: {response} ",
        request.api_key()
    );
}
