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

//! Translation of `org.apache.kafka.clients.ClientRequest`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use std::sync::Arc;

use crate::RequestCompletionHandler;
use crate::common::protocol::ApiKey;
use crate::common::requests::{AbstractRequestBuilder, RequestHeader};

/// A request being sent to the broker. Holds the network send together
/// with the client-level metadata.
///
/// Mirrors the Java `ClientRequest`. The `clientId` field uses
/// `Arc<str>` (not `String`) so per-message clones on the producer hot
/// path are O(1) refcount bumps (CLAUDE.md rule 11).
pub struct ClientRequest {
    destination: Arc<str>,
    request_builder: Arc<dyn AbstractRequestBuilder>,
    correlation_id: i32,
    client_id: Arc<str>,
    created_time_ms: i64,
    expect_response: bool,
    request_timeout_ms: i32,
    callback: Option<Arc<dyn RequestCompletionHandler>>,
}

impl ClientRequest {
    /// Mirrors the Java public constructor.
    ///
    /// * `destination` — broker id; `Arc<str>` to share with `NetworkSend`.
    /// * `request_builder` — type-erased builder (see
    ///   [`crate::common::requests::AbstractRequestBuilder`]).
    /// * `correlation_id` — pre-assigned correlation id for this request.
    /// * `client_id` — header `clientId`.
    /// * `created_time_ms` — Unix ms when the request was created.
    /// * `expect_response` — `false` for fire-and-forget (e.g. produce
    ///   with acks=0).
    /// * `request_timeout_ms` — per-request timeout used by the network
    ///   client to expire idle in-flight requests.
    /// * `callback` — optional completion handler; invoked from
    ///   [`crate::ClientResponse::on_complete`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        destination: Arc<str>,
        request_builder: Arc<dyn AbstractRequestBuilder>,
        correlation_id: i32,
        client_id: Arc<str>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Option<Arc<dyn RequestCompletionHandler>>,
    ) -> Self {
        ClientRequest {
            destination,
            request_builder,
            correlation_id,
            client_id,
            created_time_ms,
            expect_response,
            request_timeout_ms,
            callback,
        }
    }

    /// Mirrors `ClientRequest.expectResponse()`.
    pub fn expect_response(&self) -> bool {
        self.expect_response
    }

    /// Mirrors `ClientRequest.apiKey()`.
    pub fn api_key(&self) -> &'static ApiKey {
        self.request_builder.api_key()
    }

    /// Mirrors `ClientRequest.makeHeader(short version)`.
    pub fn make_header(&self, version: i16) -> RequestHeader {
        let api_key = self.api_key();
        RequestHeader::new(api_key, version, &self.client_id, self.correlation_id)
    }

    /// Mirrors `ClientRequest.requestBuilder()`.
    pub fn request_builder(&self) -> &Arc<dyn AbstractRequestBuilder> {
        &self.request_builder
    }

    /// Mirrors `ClientRequest.destination()`.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Cheap clone of the destination as an `Arc<str>` — for handing to a
    /// `NetworkSend` without re-allocating.
    pub fn destination_arc(&self) -> Arc<str> {
        Arc::clone(&self.destination)
    }

    /// Mirrors `ClientRequest.callback()`.
    pub fn callback(&self) -> Option<&Arc<dyn RequestCompletionHandler>> {
        self.callback.as_ref()
    }

    /// Mirrors `ClientRequest.createdTimeMs()`.
    pub fn created_time_ms(&self) -> i64 {
        self.created_time_ms
    }

    /// Mirrors `ClientRequest.correlationId()`.
    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    /// Mirrors `ClientRequest.requestTimeoutMs()`.
    pub fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }

    /// Borrow the `clientId` as a `&str`.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRequest")
            .field("expect_response", &self.expect_response)
            .field("destination", &&*self.destination)
            .field("correlation_id", &self.correlation_id)
            .field("client_id", &&*self.client_id)
            .field("created_time_ms", &self.created_time_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("api_key", &self.request_builder.api_key().name)
            .field("has_callback", &self.callback.is_some())
            .finish()
    }
}

impl std::fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientRequest(expectResponse={}, destination={}, correlationId={}, clientId={}, createdTimeMs={}, apiKey={})",
            self.expect_response,
            self.destination,
            self.correlation_id,
            self.client_id,
            self.created_time_ms,
            self.request_builder.api_key().name,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::errors::KafkaError;
    use crate::common::message::api_versions_request_data::ApiVersionsRequestData;
    use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
    use crate::common::requests::{AbstractRequest, AbstractRequestBuilder, AbstractRequestResponse, AbstractResponse};

    /// Minimal test builder backed by a real `ApiVersionsRequestData`.
    /// We only need the builder to satisfy the trait — Phase 5d will
    /// replace this with the production `ApiVersionsRequest::Builder`.
    #[derive(Debug)]
    struct TestApiVersionsBuilder {
        api_key: &'static ApiKey,
    }

    impl TestApiVersionsBuilder {
        fn new() -> Self {
            TestApiVersionsBuilder { api_key: ApiKeys::for_id(18).expect("API_VERSIONS") }
        }
    }

    /// Stub `AbstractRequest` whose only role here is to be returned from
    /// `Builder.build` so the trait compiles. The actual round-trip wire
    /// logic for `ApiVersionsRequest` is exercised in its own tests.
    struct StubRequest {
        api_key: &'static ApiKey,
        version: i16,
        data: ApiVersionsRequestData,
    }

    impl AbstractRequestResponse for StubRequest {
        fn data(&self) -> &dyn Message {
            &self.data
        }
    }

    impl AbstractRequest for StubRequest {
        fn version(&self) -> i16 {
            self.version
        }

        fn api_key(&self) -> &'static ApiKey {
            self.api_key
        }

        fn get_error_response(&self, _throttle_time_ms: i32, _error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
            None
        }

        fn error_counts(&self, _error: &KafkaError) -> Result<HashMap<Errors, i32>, KafkaError> {
            Ok(HashMap::new())
        }
    }

    impl AbstractRequestBuilder for TestApiVersionsBuilder {
        fn api_key(&self) -> &'static ApiKey {
            self.api_key
        }
        fn oldest_allowed_version(&self) -> i16 {
            self.api_key.oldest_version()
        }
        fn latest_allowed_version(&self) -> i16 {
            self.api_key.latest_version()
        }
        fn build(&self, version: i16) -> Result<Box<dyn AbstractRequest>, KafkaError> {
            Ok(Box::new(StubRequest {
                api_key: self.api_key,
                version,
                data: ApiVersionsRequestData::new(),
            }))
        }
    }

    #[test]
    fn fields_are_accessible() {
        let builder: Arc<dyn AbstractRequestBuilder> = Arc::new(TestApiVersionsBuilder::new());
        let req = ClientRequest::new(
            Arc::from("broker-1"),
            builder,
            42,
            Arc::from("test-client"),
            1_000,
            true,
            30_000,
            None,
        );
        assert_eq!(req.destination(), "broker-1");
        assert_eq!(req.correlation_id(), 42);
        assert_eq!(req.client_id(), "test-client");
        assert_eq!(req.created_time_ms(), 1_000);
        assert!(req.expect_response());
        assert_eq!(req.request_timeout_ms(), 30_000);
        assert_eq!(req.api_key().id, 18); // API_VERSIONS
        assert!(req.callback().is_none());
    }

    #[test]
    fn make_header_uses_correlation_and_client_id() {
        let builder: Arc<dyn AbstractRequestBuilder> = Arc::new(TestApiVersionsBuilder::new());
        let req = ClientRequest::new(
            Arc::from("broker-1"),
            builder,
            7,
            Arc::from("the-client"),
            0,
            true,
            10_000,
            None,
        );
        let header = req.make_header(3);
        assert_eq!(header.correlation_id(), 7);
        assert_eq!(header.client_id(), "the-client");
        assert_eq!(header.api_version(), 3);
        assert_eq!(header.api_key().expect("known").id, 18);
    }

    #[test]
    fn destination_arc_clones_cheaply() {
        let builder: Arc<dyn AbstractRequestBuilder> = Arc::new(TestApiVersionsBuilder::new());
        let dest: Arc<str> = Arc::from("broker-7");
        let req = ClientRequest::new(Arc::clone(&dest), builder, 0, Arc::from("c"), 0, true, 1_000, None);
        let cloned = req.destination_arc();
        assert!(Arc::ptr_eq(&dest, &cloned));
    }
}
