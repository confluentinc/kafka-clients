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

//! Translation of `org.apache.kafka.clients.ClientResponse`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use std::sync::Arc;

use crate::RequestCompletionHandler;
use crate::common::errors::KafkaError;
use crate::common::requests::{AbstractResponse, RequestHeader};

/// A response from the broker. Contains the response body together with
/// the correlated request metadata.
///
/// Mirrors the Java `ClientResponse`. The Java fields
/// `versionMismatch: UnsupportedVersionException` and
/// `authenticationException: AuthenticationException` collapse onto
/// [`KafkaError::UnsupportedVersion`] and [`KafkaError::Authentication`]
/// respectively in our unified error enum (see `KafkaError::is_fatal`).
pub struct ClientResponse {
    request_header: RequestHeader,
    callback: Option<Arc<dyn RequestCompletionHandler>>,
    destination: Arc<str>,
    received_time_ms: i64,
    latency_ms: i64,
    disconnected: bool,
    timed_out: bool,
    version_mismatch: Option<KafkaError>,
    authentication_exception: Option<KafkaError>,
    response_body: Option<Box<dyn AbstractResponse>>,
}

impl ClientResponse {
    /// Mirrors the 9-arg Java constructor (without `timedOut`). Defaults
    /// `timed_out` to `false`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_header: RequestHeader,
        callback: Option<Arc<dyn RequestCompletionHandler>>,
        destination: Arc<str>,
        created_time_ms: i64,
        received_time_ms: i64,
        disconnected: bool,
        version_mismatch: Option<KafkaError>,
        authentication_exception: Option<KafkaError>,
        response_body: Option<Box<dyn AbstractResponse>>,
    ) -> Self {
        Self::with_timed_out(
            request_header,
            callback,
            destination,
            created_time_ms,
            received_time_ms,
            disconnected,
            false,
            version_mismatch,
            authentication_exception,
            response_body,
        )
    }

    /// Mirrors the 10-arg Java constructor with explicit `timedOut`.
    /// Java throws `IllegalStateException` if `timedOut == true` and
    /// `disconnected == false`; we mirror that contract by panicking,
    /// matching CLAUDE.md rule 10.1 (panic for unrecoverable invariant
    /// violations).
    #[allow(clippy::too_many_arguments)]
    pub fn with_timed_out(
        request_header: RequestHeader,
        callback: Option<Arc<dyn RequestCompletionHandler>>,
        destination: Arc<str>,
        created_time_ms: i64,
        received_time_ms: i64,
        disconnected: bool,
        timed_out: bool,
        version_mismatch: Option<KafkaError>,
        authentication_exception: Option<KafkaError>,
        response_body: Option<Box<dyn AbstractResponse>>,
    ) -> Self {
        if !disconnected && timed_out {
            // Mirrors Java's `throw new IllegalStateException(...)`. This
            // is a programmer-error invariant violation (the same way Java
            // raises an unchecked `IllegalStateException`), so panicking
            // matches the Java contract exactly.
            panic!("The client response can't be in the state of connected, yet timed out");
        }
        ClientResponse {
            request_header,
            callback,
            destination,
            received_time_ms,
            latency_ms: received_time_ms - created_time_ms,
            disconnected,
            timed_out,
            version_mismatch,
            authentication_exception,
            response_body,
        }
    }

    /// Mirrors `ClientResponse.receivedTimeMs()`.
    pub fn received_time_ms(&self) -> i64 {
        self.received_time_ms
    }

    /// Mirrors `ClientResponse.wasDisconnected()`.
    pub fn was_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Mirrors `ClientResponse.wasTimedOut()`.
    pub fn was_timed_out(&self) -> bool {
        self.timed_out
    }

    /// Mirrors `ClientResponse.versionMismatch()`. The Java return type is
    /// `UnsupportedVersionException`; we return the unified
    /// [`KafkaError::UnsupportedVersion`] (callers can pattern-match).
    pub fn version_mismatch(&self) -> Option<&KafkaError> {
        self.version_mismatch.as_ref()
    }

    /// Mirrors `ClientResponse.authenticationException()`. Same naming
    /// note as [`Self::version_mismatch`].
    pub fn authentication_exception(&self) -> Option<&KafkaError> {
        self.authentication_exception.as_ref()
    }

    /// Mirrors `ClientResponse.requestHeader()`.
    pub fn request_header(&self) -> &RequestHeader {
        &self.request_header
    }

    /// Mirrors `ClientResponse.destination()`.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Mirrors `ClientResponse.responseBody()`. Returns `None` when the
    /// request did not produce a response (e.g. produce with acks=0),
    /// when the channel disconnected, or on a version mismatch.
    pub fn response_body(&self) -> Option<&dyn AbstractResponse> {
        self.response_body.as_deref()
    }

    /// Mirrors `ClientResponse.hasResponse()`.
    pub fn has_response(&self) -> bool {
        self.response_body.is_some()
    }

    /// Mirrors `ClientResponse.requestLatencyMs()`.
    pub fn request_latency_ms(&self) -> i64 {
        self.latency_ms
    }

    /// Fire the registered completion handler, if any. Mirrors
    /// `ClientResponse.onComplete()`.
    pub fn on_complete(&self) {
        if let Some(cb) = &self.callback {
            cb.on_complete(self);
        }
    }
}

impl std::fmt::Debug for ClientResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientResponse")
            .field("received_time_ms", &self.received_time_ms)
            .field("latency_ms", &self.latency_ms)
            .field("disconnected", &self.disconnected)
            .field("timed_out", &self.timed_out)
            .field("destination", &&*self.destination)
            .field("has_response", &self.response_body.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::common::protocol::ApiKeys;

    fn header() -> RequestHeader {
        let api_versions = ApiKeys::for_id(18).expect("API_VERSIONS");
        RequestHeader::new(api_versions, 0, "test-client", 42)
    }

    #[derive(Debug)]
    struct CountingHandler {
        invocations: AtomicUsize,
        captured_disconnected: Mutex<Option<bool>>,
    }

    impl CountingHandler {
        fn new() -> Arc<Self> {
            Arc::new(CountingHandler { invocations: AtomicUsize::new(0), captured_disconnected: Mutex::new(None) })
        }
    }

    impl RequestCompletionHandler for CountingHandler {
        fn on_complete(&self, response: &ClientResponse) {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            *self.captured_disconnected.lock().unwrap() = Some(response.was_disconnected());
        }
    }

    #[test]
    fn fields_are_accessible() {
        let resp = ClientResponse::new(header(), None, Arc::from("broker-1"), 100, 150, false, None, None, None);
        assert_eq!(resp.received_time_ms(), 150);
        assert_eq!(resp.request_latency_ms(), 50);
        assert!(!resp.was_disconnected());
        assert!(!resp.was_timed_out());
        assert!(resp.version_mismatch().is_none());
        assert!(resp.authentication_exception().is_none());
        assert!(!resp.has_response());
        assert_eq!(resp.destination(), "broker-1");
        assert_eq!(resp.request_header().correlation_id(), 42);
    }

    #[test]
    fn on_complete_invokes_callback() {
        let handler = CountingHandler::new();
        let cb: Arc<dyn RequestCompletionHandler> = handler.clone();
        let resp = ClientResponse::new(header(), Some(cb), Arc::from("broker-1"), 100, 150, true, None, None, None);
        resp.on_complete();
        assert_eq!(handler.invocations.load(Ordering::SeqCst), 1);
        assert_eq!(*handler.captured_disconnected.lock().unwrap(), Some(true));
    }

    #[test]
    fn on_complete_with_no_callback_is_noop() {
        let resp = ClientResponse::new(header(), None, Arc::from("broker-1"), 100, 150, false, None, None, None);
        resp.on_complete(); // should not panic
    }

    #[test]
    #[should_panic(expected = "connected, yet timed out")]
    fn connected_and_timed_out_panics() {
        // Mirrors the Java `IllegalStateException` thrown when the
        // `disconnected`/`timedOut` invariant is violated.
        let _resp = ClientResponse::with_timed_out(
            header(),
            None,
            Arc::from("broker-1"),
            100,
            150,
            false,
            true,
            None,
            None,
            None,
        );
    }
}
