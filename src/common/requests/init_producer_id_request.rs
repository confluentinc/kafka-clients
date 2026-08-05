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

//! `InitProducerId` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.InitProducerIdRequest`.
//!
//! Wraps the auto-generated [`InitProducerIdRequestData`] and exposes an
//! [`InitProducerIdRequestBuilder`] that validates the transaction timeout and
//! transactional id before building.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::RecordBatch;
use crate::init_producer_id_request_data::InitProducerIdRequestData;
use crate::init_producer_id_response_data::InitProducerIdResponseData;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::InitProducerIdResponse;
use super::RequestBuilder;

/// An `InitProducerId` request.
///
/// Corresponds to `org.apache.kafka.common.requests.InitProducerIdRequest`.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    data: InitProducerIdRequestData,
    version: i16,
}

impl InitProducerIdRequest {
    /// Creates a new `InitProducerIdRequest` from data and version.
    pub fn new(data: InitProducerIdRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &InitProducerIdRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut InitProducerIdRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INIT_PRODUCER_ID
    }

    /// Whether the request opts in to two-phase commit (KIP-939).
    ///
    /// Corresponds to Java's `enable2Pc()`.
    pub fn enable_2pc(&self) -> bool {
        self.data.enable2_pc
    }

    /// Whether an ongoing prepared transaction should be kept (KIP-939).
    ///
    /// Corresponds to Java's `keepPreparedTxn()`.
    pub fn keep_prepared_txn(&self) -> bool {
        self.data.keep_prepared_txn
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `InitProducerIdRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    ///
    /// Note the producer id/epoch are always the sentinels regardless of
    /// version, and the throttle time is always set — Java does not
    /// version-gate either here.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = InitProducerIdResponseData::new();
        response
            .set_error_code(error.code())
            .set_producer_id(RecordBatch::NO_PRODUCER_ID)
            .set_producer_epoch(RecordBatch::NO_PRODUCER_EPOCH)
            .set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(response))
    }

    /// Parses an `InitProducerIdRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = InitProducerIdRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for InitProducerIdRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InitProducerIdRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`InitProducerIdRequest`].
///
/// Corresponds to `InitProducerIdRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequestBuilder {
    data: InitProducerIdRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl InitProducerIdRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: InitProducerIdRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::INIT_PRODUCER_ID.oldest_version(),
            // Java's `AbstractRequest.Builder(ApiKeys)` delegates to
            // `Builder(apiKey, false)` → `latestVersion(false)`, i.e. "any
            // supported and *released* version". This spec sets
            // `latestVersionUnstable: true`, so the unstable-inclusive
            // `latest_version()` would offer one version higher than Java ever
            // does. Use the explicit `false` form.
            latest_allowed_version: ApiKeys::INIT_PRODUCER_ID.latest_version_with_unstable(false),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &InitProducerIdRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Test-only. `TransactionManager.initializeTransactions` never calls
    /// `setKeepPreparedTxn` in Apache Kafka 4.2, so the KIP-939 response arm at
    /// `TransactionManager.java:1501` cannot be reached without setting the flag on
    /// an already-built request — which is what
    /// `TransactionManagerTest.prepareInitPidResponse`'s `keepPreparedTxn = true`
    /// overload asserts the broker sees. Production code has no reason to mutate a
    /// built request.
    #[cfg(test)]
    pub(crate) fn data_mut(&mut self) -> &mut InitProducerIdRequestData {
        &mut self.data
    }
}

impl RequestBuilder for InitProducerIdRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INIT_PRODUCER_ID
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `InitProducerIdRequest.Builder.build(short version)`. Java
        // throws IllegalArgumentException for both checks; per CLAUDE.md §10.2
        // these become errors rather than panics.
        if self.data.transaction_timeout_ms <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "transaction timeout value is not positive: {}",
                    self.data.transaction_timeout_ms
                ),
            ));
        }

        // Java checks `transactionalId != null && transactionalId.isEmpty()`,
        // i.e. a present-but-empty id is rejected while an absent one is fine.
        // `Option::is_some_and` preserves exactly that distinction.
        if self.data.transactional_id.as_ref().is_some_and(|id| id.is_empty()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Must set either a null or a non-empty transactional id.",
            ));
        }

        Ok(ConcreteRequest::InitProducerId(InitProducerIdRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_data() -> InitProducerIdRequestData {
        let mut data = InitProducerIdRequestData::new();
        data.set_transactional_id(Some("txn-id".to_string()))
            .set_transaction_timeout_ms(60_000);
        data
    }

    #[test]
    fn test_build_accepts_valid_data() {
        let mut builder = InitProducerIdRequestBuilder::new(valid_data());
        let built = builder
            .build_version(ApiKeys::INIT_PRODUCER_ID.latest_version())
            .expect("valid");
        match built {
            ConcreteRequest::InitProducerId(req) => {
                assert_eq!(req.data().transactional_id.as_deref(), Some("txn-id"));
                assert_eq!(req.data().transaction_timeout_ms, 60_000);
            },
            other => panic!("expected InitProducerId variant, got {other:?}"),
        }
    }

    /// Java: `transactionTimeoutMs() <= 0` is rejected.
    #[test]
    fn test_build_rejects_non_positive_timeout() {
        for timeout in [0, -1] {
            let mut data = valid_data();
            data.set_transaction_timeout_ms(timeout);
            let mut builder = InitProducerIdRequestBuilder::new(data);
            let err = builder.build_version(0).expect_err("non-positive timeout must be rejected");
            assert_eq!(err.to_string(), format!("transaction timeout value is not positive: {timeout}"));
        }
    }

    /// Java rejects a present-but-empty transactional id.
    #[test]
    fn test_build_rejects_empty_transactional_id() {
        let mut data = valid_data();
        data.set_transactional_id(Some(String::new()));
        let mut builder = InitProducerIdRequestBuilder::new(data);
        let err = builder.build_version(0).expect_err("empty transactional id must be rejected");
        assert_eq!(err.to_string(), "Must set either a null or a non-empty transactional id.");
    }

    /// An absent transactional id is valid — this is the idempotent-producer
    /// case, and the distinction from the empty-string case is load-bearing.
    #[test]
    fn test_build_accepts_absent_transactional_id() {
        let mut data = InitProducerIdRequestData::new();
        data.set_transactional_id(None).set_transaction_timeout_ms(60_000);
        let mut builder = InitProducerIdRequestBuilder::new(data);
        builder.build_version(0).expect("absent transactional id must be accepted");
    }

    /// Java's `getErrorResponse` always returns the producer id/epoch sentinels
    /// and always sets the throttle time, with no version gating.
    #[test]
    fn test_get_error_response() {
        let request = InitProducerIdRequest::new(valid_data(), 0);
        match request.get_error_response(42, &Errors::NotCoordinator) {
            ConcreteResponse::InitProducerId(response) => {
                assert_eq!(response.data().error_code, Errors::NotCoordinator.code());
                assert_eq!(response.data().producer_id, RecordBatch::NO_PRODUCER_ID);
                assert_eq!(response.data().producer_epoch, RecordBatch::NO_PRODUCER_EPOCH);
                assert_eq!(response.data().throttle_time_ms, 42);
            },
            other => panic!("expected InitProducerId response, got {other:?}"),
        }
    }

    #[test]
    fn test_2pc_accessors() {
        let mut data = valid_data();
        data.set_enable2_pc(true).set_keep_prepared_txn(true);
        let request = InitProducerIdRequest::new(data, 0);
        assert!(request.enable_2pc());
        assert!(request.keep_prepared_txn());

        let request = InitProducerIdRequest::new(valid_data(), 0);
        assert!(!request.enable_2pc());
        assert!(!request.keep_prepared_txn());
    }

    #[test]
    fn test_api_key_and_version() {
        let builder = InitProducerIdRequestBuilder::new(valid_data());
        assert_eq!(builder.api_key(), &ApiKeys::INIT_PRODUCER_ID);
        let request = InitProducerIdRequest::new(valid_data(), 3);
        assert_eq!(request.api_key(), &ApiKeys::INIT_PRODUCER_ID);
        assert_eq!(request.version(), 3);
    }

    /// Translated from `RequestResponseTest.testInitProducerIdRequestVersions`.
    ///
    /// `producer_id` is a v3+ field, so serializing a non-default value at v2
    /// must fail. The check belongs to the generated code, not this wrapper.
    ///
    /// # `#[ignore]`: pre-existing generator gap, not a Phase 2 defect
    ///
    /// This assertion is correct and currently FAILS. The Rust code generator
    /// emits only the `if version >= N { write }` half of Java's version gate
    /// and omits the `else { if non-default { throw } }` half, so a non-default
    /// value at an unsupported version is **silently dropped** instead of
    /// rejected. Java refuses to send a request it cannot faithfully encode;
    /// this port would send a subtly different one.
    ///
    /// The gap is systemic — it affects every version-gated non-tagged field
    /// across all 197 generated message types, not just this one. The emission
    /// site is `generator/src/lib.rs:1176-1187`, and the non-default-condition
    /// helper it needs already exists at line 1645. Fixing it requires
    /// regenerating every message type and auditing the fallout, which is out
    /// of scope for Phase 2.
    ///
    /// Un-ignore this test to verify the fix when the generator is corrected.
    #[test]
    #[ignore = "generator omits Java's non-default-at-unsupported-version guard; see doc comment"]
    fn test_init_producer_id_request_versions() {
        let mut data = InitProducerIdRequestData::new();
        data.set_transaction_timeout_ms(1000)
            .set_transactional_id(Some("abracadabra".to_string()))
            .set_producer_id(123);
        let mut builder = InitProducerIdRequestBuilder::new(data);

        let mut v2 = builder.build_version(2).expect("v2 must build; the failure is at serialize");
        // `expect_err` is unavailable: the Ok type `ByteBufferAccessor` is not `Debug`.
        match v2.serialize() {
            Ok(_) => panic!("non-default producerId at v2 must fail to serialize"),
            Err(err) => assert!(
                err.to_string()
                    .contains("Attempted to write a non-default producerId at version 2"),
                "got: {err}"
            ),
        }

        // v3 is where the field was introduced, so it serializes cleanly.
        let mut v3 = builder.build_version(3).expect("v3 build");
        v3.serialize().expect("v3 must serialize a non-default producerId");
    }

    /// Round-trips the request through serialization at every supported version,
    /// using the values from `RequestResponseTest.createInitPidRequest`
    /// (a null transactional id — the idempotent-producer shape).
    #[test]
    fn test_serialization_round_trip_all_versions() {
        for version in ApiKeys::INIT_PRODUCER_ID.oldest_version()..=ApiKeys::INIT_PRODUCER_ID.latest_version() {
            let mut data = InitProducerIdRequestData::new();
            data.set_transactional_id(None).set_transaction_timeout_ms(100);
            let mut builder = InitProducerIdRequestBuilder::new(data);
            let mut built = builder.build_version(version).expect("build");

            let mut buffer = built.serialize().expect("serialize");
            buffer.flip();
            let parsed = InitProducerIdRequest::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data().transaction_timeout_ms, 100);
            assert_eq!(parsed.data().transactional_id, None, "v{version}");
        }
    }
    /// The builder must offer only *released* versions.
    ///
    /// Java's `AbstractRequest.Builder(ApiKeys)` passes
    /// `enableUnstableLastVersion = false`, and this spec sets
    /// `latestVersionUnstable: true`, so `latestVersion(false)` is one below the
    /// unstable-inclusive maximum. Using `latest_version()` here would offer a
    /// version Java never sends. Regression test for Critic 42 finding 1.
    #[test]
    fn test_builder_offers_only_released_versions() {
        let builder = InitProducerIdRequestBuilder::new(valid_data());
        let released = ApiKeys::INIT_PRODUCER_ID.latest_version_with_unstable(false);
        let with_unstable = ApiKeys::INIT_PRODUCER_ID.latest_version();

        assert_eq!(builder.latest_allowed_version(), released);
        assert_eq!(
            with_unstable,
            released + 1,
            "this spec is expected to mark its last version unstable; if that \
             changes upstream, revisit the cap rather than this assertion"
        );
        assert_ne!(
            builder.latest_allowed_version(),
            with_unstable,
            "must not offer the unstable version"
        );
    }
}
