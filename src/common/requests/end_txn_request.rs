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

//! `EndTxn` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.EndTxnRequest`.
//!
//! Sent to commit or abort a transaction. The `committed` boolean on the wire
//! selects which — see [`EndTxnRequest::result`].

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::end_txn_request_data::EndTxnRequestData;
use crate::end_txn_response_data::EndTxnResponseData;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::EndTxnResponse;
use super::RequestBuilder;
use super::TransactionResult;

/// Highest version predating KIP-890 Transaction V2.
///
/// A client that has not negotiated Transaction V2 must not send above this, so
/// [`EndTxnRequestBuilder::build_version`] clamps to it. Corresponds to
/// `EndTxnRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 4;

/// An `EndTxn` request.
///
/// Corresponds to `org.apache.kafka.common.requests.EndTxnRequest`.
#[derive(Debug, Clone)]
pub struct EndTxnRequest {
    data: EndTxnRequestData,
    version: i16,
}

impl EndTxnRequest {
    /// Creates a new `EndTxnRequest` from data and version.
    ///
    /// Java's constructor is private — instances come from the builder or
    /// [`Self::parse`]. This is `pub` because the dispatch enum in
    /// `abstract_request.rs` constructs variants directly, as it does for every
    /// other request type.
    pub fn new(data: EndTxnRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &EndTxnRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut EndTxnRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::END_TXN
    }

    /// Whether this request commits or aborts the transaction.
    ///
    /// Corresponds to Java's `result()`.
    pub fn result(&self) -> TransactionResult {
        if self.data.committed {
            TransactionResult::Commit
        } else {
            TransactionResult::Abort
        }
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `EndTxnRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = EndTxnResponseData::new();
        response.set_error_code(error.code()).set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::EndTxn(EndTxnResponse::new(response))
    }

    /// Parses an `EndTxnRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = EndTxnRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for EndTxnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EndTxnRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`EndTxnRequest`].
///
/// Corresponds to `EndTxnRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct EndTxnRequestBuilder {
    data: EndTxnRequestData,
    is_transaction_v2_enabled: bool,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl EndTxnRequestBuilder {
    /// Creates a builder.
    ///
    /// `is_transaction_v2_enabled` reflects whether the broker's finalized
    /// features advertise KIP-890 Transaction V2; when false, the built version
    /// is clamped — see [`Self::build_version`].
    ///
    /// Java also has a three-argument constructor taking
    /// `enableUnstableLastVersion`. That flag belongs to Java's
    /// `AbstractRequest.Builder` machinery for gating unreleased API versions,
    /// which this codebase's `RequestBuilder` has no equivalent of; the two-arg
    /// form passes `false`, and that is the only form the producer uses.
    pub fn new(data: EndTxnRequestData, is_transaction_v2_enabled: bool) -> Self {
        Self {
            data,
            is_transaction_v2_enabled,
            oldest_allowed_version: ApiKeys::END_TXN.oldest_version(),
            // Java's `AbstractRequest.Builder(ApiKeys)` delegates to
            // `Builder(apiKey, false)` → `latestVersion(false)`, i.e. "any
            // supported and *released* version". This spec sets
            // `latestVersionUnstable: true`, so the unstable-inclusive
            // `latest_version()` would offer one version higher than Java ever
            // does. Use the explicit `false` form.
            latest_allowed_version: ApiKeys::END_TXN.latest_version_with_unstable(false),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &EndTxnRequestData {
        &self.data
    }

    /// Whether Transaction V2 was negotiated.
    pub fn is_transaction_v2_enabled(&self) -> bool {
        self.is_transaction_v2_enabled
    }
}

impl RequestBuilder for EndTxnRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::END_TXN
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    /// Builds at `version`, **clamped** to
    /// [`LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`] when Transaction V2 is not
    /// enabled.
    ///
    /// Mirrors Java's `Builder.build(short)`, which does
    /// `version = min(version, LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)`. Note
    /// it silently downgrades rather than erroring: a client that has not
    /// negotiated TV2 must speak the older dialect even if the broker offers a
    /// higher version.
    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        let version = if self.is_transaction_v2_enabled {
            version
        } else {
            version.min(LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)
        };
        Ok(ConcreteRequest::EndTxn(EndTxnRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(committed: bool) -> EndTxnRequestData {
        let mut data = EndTxnRequestData::new();
        data.set_transactional_id("txn-1".to_string())
            .set_producer_id(42)
            .set_producer_epoch(7)
            .set_committed(committed);
        data
    }

    fn build_at(version: i16, transaction_v2: bool) -> EndTxnRequest {
        let mut builder = EndTxnRequestBuilder::new(data(true), transaction_v2);
        match builder.build_version(version).expect("build") {
            ConcreteRequest::EndTxn(request) => request,
            other => panic!("expected EndTxn, got {other:?}"),
        }
    }

    #[test]
    fn test_build_preserves_all_fields() {
        let request = build_at(0, false);
        assert_eq!(request.data().transactional_id, "txn-1");
        assert_eq!(request.data().producer_id, 42);
        assert_eq!(request.data().producer_epoch, 7);
        assert!(request.data().committed);
    }

    /// `committed` selects commit vs abort.
    #[test]
    fn test_result_maps_committed_flag() {
        let mut builder = EndTxnRequestBuilder::new(data(true), false);
        match builder.build_version(0).expect("build") {
            ConcreteRequest::EndTxn(request) => {
                assert_eq!(request.result(), TransactionResult::Commit)
            },
            other => panic!("unexpected {other:?}"),
        }

        let mut builder = EndTxnRequestBuilder::new(data(false), false);
        match builder.build_version(0).expect("build") {
            ConcreteRequest::EndTxn(request) => {
                assert_eq!(request.result(), TransactionResult::Abort)
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    /// Without Transaction V2 the version is clamped, silently — Java downgrades
    /// rather than erroring.
    #[test]
    fn test_build_clamps_version_without_transaction_v2() {
        let latest = ApiKeys::END_TXN.latest_version();
        assert!(
            latest > LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2,
            "the clamp is only meaningful if the API supports higher versions"
        );

        // Above the cap: clamped down.
        assert_eq!(build_at(latest, false).version(), LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2);
        // Exactly at the cap: unchanged.
        assert_eq!(
            build_at(LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2, false).version(),
            LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        );
        // Below the cap: unchanged — the clamp is a maximum, not a target.
        assert_eq!(build_at(1, false).version(), 1);
    }

    /// With Transaction V2 the requested version passes through untouched.
    #[test]
    fn test_build_does_not_clamp_with_transaction_v2() {
        let latest = ApiKeys::END_TXN.latest_version();
        assert_eq!(build_at(latest, true).version(), latest);
        assert_eq!(build_at(1, true).version(), 1);
    }

    #[test]
    fn test_get_error_response() {
        let request = EndTxnRequest::new(data(true), 0);
        match request.get_error_response(12, &Errors::InvalidTxnState) {
            ConcreteResponse::EndTxn(response) => {
                assert_eq!(response.data().error_code, Errors::InvalidTxnState.code());
                assert_eq!(response.data().throttle_time_ms, 12);
            },
            other => panic!("expected EndTxn response, got {other:?}"),
        }
    }

    #[test]
    fn test_serialization_round_trip_all_versions() {
        for version in ApiKeys::END_TXN.oldest_version()..=ApiKeys::END_TXN.latest_version() {
            // Enable TV2 so the requested version is not clamped, letting the
            // round trip cover every version the API supports.
            let mut builder = EndTxnRequestBuilder::new(data(true), true);
            let mut built = builder.build_version(version).expect("build");
            let mut buffer = built.serialize().expect("serialize");
            buffer.flip();
            let parsed = EndTxnRequest::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data().transactional_id, "txn-1", "v{version}");
            assert_eq!(parsed.data().producer_id, 42, "v{version}");
            assert_eq!(parsed.data().producer_epoch, 7, "v{version}");
            assert_eq!(parsed.result(), TransactionResult::Commit, "v{version}");
        }
    }

    /// Translated from `EndTxnRequestTest.testConstructor`.
    ///
    /// Java loops `ApiKeys.END_TXN.allVersions()` with Transaction V2 enabled, so
    /// no clamping occurs and every version is exercised.
    #[test]
    fn test_constructor() {
        const THROTTLE_TIME_MS: i32 = 10;

        for version in ApiKeys::END_TXN.oldest_version()..=ApiKeys::END_TXN.latest_version() {
            let mut builder = EndTxnRequestBuilder::new(data(true), true);
            let request = match builder.build_version(version).expect("build") {
                ConcreteRequest::EndTxn(request) => request,
                other => panic!("expected EndTxn, got {other:?}"),
            };

            match request.get_error_response(THROTTLE_TIME_MS, &Errors::NotCoordinator) {
                ConcreteResponse::EndTxn(response) => {
                    let counts = response.error_counts();
                    assert_eq!(counts.len(), 1, "v{version}");
                    assert_eq!(counts.get(&Errors::NotCoordinator), Some(&1), "v{version}");
                    assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
                },
                other => panic!("expected EndTxn response, got {other:?}"),
            }

            assert_eq!(request.result(), TransactionResult::Commit, "v{version}");
        }
    }

    /// Translated from
    /// `EndTxnRequestTest.testEndTxnRequestWithParameterizedTransactionsV2`.
    ///
    /// Java's `@ValueSource(booleans = {true, false})` becomes a loop. Java uses
    /// the three-argument builder here; the extra `enableUnstableLastVersion`
    /// argument has no counterpart in this codebase (see
    /// [`EndTxnRequestBuilder::new`]) and Java passes `false`, which is the
    /// default the two-argument form gives.
    #[test]
    fn test_end_txn_request_with_parameterized_transactions_v2() {
        let latest_version = ApiKeys::END_TXN.latest_version();

        for is_transaction_v2_enabled in [true, false] {
            let mut builder = EndTxnRequestBuilder::new(data(true), is_transaction_v2_enabled);
            let request = match builder.build_version(latest_version).expect("build") {
                ConcreteRequest::EndTxn(request) => request,
                other => panic!("expected EndTxn, got {other:?}"),
            };

            let expected_version = if is_transaction_v2_enabled {
                latest_version
            } else {
                LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            };
            assert_eq!(
                request.version(),
                expected_version,
                "transaction_v2={is_transaction_v2_enabled}"
            );

            // The producer state travels regardless of the negotiated version.
            assert_eq!(request.data().producer_id, 42);
            assert_eq!(request.data().producer_epoch, 7);
        }
    }

    #[test]
    fn test_api_key_and_accessors() {
        let builder = EndTxnRequestBuilder::new(data(true), false);
        assert_eq!(builder.api_key(), &ApiKeys::END_TXN);
        assert!(!builder.is_transaction_v2_enabled());
        assert!(EndTxnRequestBuilder::new(data(true), true).is_transaction_v2_enabled());

        let request = EndTxnRequest::new(data(true), 3);
        assert_eq!(request.api_key(), &ApiKeys::END_TXN);
        assert_eq!(request.version(), 3);
    }
    /// The builder must call the same accessor Java calls.
    ///
    /// Java's `EndTxnRequest.Builder` reaches
    /// `super(ApiKeys.END_TXN, enableUnstableLastVersion)` with `false` from the
    /// public two-argument form, i.e. `latestVersion(false)`. `END_TXN`'s spec
    /// currently sets `latestVersionUnstable: false`, so the two accessors agree
    /// today — this pins the *call*, not the coincidence, so the builder stays
    /// correct if the flag ever flips upstream.
    #[test]
    fn test_builder_offers_only_released_versions() {
        let builder = EndTxnRequestBuilder::new(data(true), true);
        assert_eq!(
            builder.latest_allowed_version(),
            ApiKeys::END_TXN.latest_version_with_unstable(false)
        );
    }
}
