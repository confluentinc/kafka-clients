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

//! ApiVersions response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ApiVersionsResponse`.

use std::collections::HashMap;

use crate::api_message_type::ListenerType;
use crate::api_versions_response_data::{
    ApiVersion, ApiVersionsResponseData, FinalizedFeatureKey, SupportedFeatureKey,
};
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};

/// Unknown finalized features epoch sentinel.
pub const API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH: i64 = -1;

/// Possible error codes:
/// - [`Errors::UnsupportedVersion`]
/// - [`Errors::InvalidRequest`]
#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    data: ApiVersionsResponseData,
}

impl ApiVersionsResponse {
    /// Creates a new `ApiVersionsResponse` from data.
    pub fn new(data: ApiVersionsResponseData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ApiVersionsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub fn data_mut(&mut self) -> &mut ApiVersionsResponseData {
        &mut self.data
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::API_VERSIONS
    }

    /// Find the API version entry for a given API key id.
    pub fn api_version(&self, api_key_id: i16) -> Option<&ApiVersion> {
        self.data.api_keys.iter().find(|v| v.api_key == api_key_id)
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        super::abstract_response::single_error_count(Errors::for_code(self.data.error_code))
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle upon receiving this response.
    ///
    /// Client-side throttling is enabled starting from version 2.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 2
    }

    /// Whether ZK migration is ready.
    pub fn zk_migration_ready(&self) -> bool {
        self.data.zk_migration_ready
    }

    /// Parses an `ApiVersionsResponse` from a readable buffer at the given version.
    ///
    /// Implements fallback-to-version-0 logic: if parsing at the given version fails
    /// and the version is not 0, re-try parsing at version 0. This handles the case
    /// where the broker returns a v0 response for unsupported versions.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails at both the requested version and version 0.
    pub fn parse(readable: &mut ByteBufferAccessor, version: i16) -> std::io::Result<Self> {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it which means that the version received by this
        // method is not necessarily the real one. It may be version 0 as well.
        let readable_copy = readable.snapshot_remaining();
        match ApiVersionsResponseData::read(readable, version) {
            Ok(data) => Ok(Self::new(data)),
            Err(e) => {
                if version != 0 {
                    let mut fallback = readable_copy;
                    let data = ApiVersionsResponseData::read(&mut fallback, 0)?;
                    Ok(Self::new(data))
                } else {
                    Err(e)
                }
            },
        }
    }

    /// Filters APIs available for the given listener type.
    ///
    /// Corresponds to `ApiVersionsResponse.filterApis` in Java.
    pub fn filter_apis(
        listener_type: ListenerType,
        enable_unstable_last_version: bool,
        client_telemetry_enabled: bool,
    ) -> Vec<ApiVersion> {
        let mut api_keys = Vec::new();
        for api_key in ApiKeys::apis_for_listener(listener_type) {
            // Skip telemetry APIs if client telemetry is disabled.
            if (*api_key == ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS || *api_key == ApiKeys::PUSH_TELEMETRY)
                && !client_telemetry_enabled
            {
                continue;
            }
            if let Some(v) = api_key.to_api_version_for_api_response(enable_unstable_last_version, listener_type) {
                api_keys.push(v);
            }
        }
        api_keys
    }

    /// Collects API versions for a specific set of API keys.
    ///
    /// Corresponds to `ApiVersionsResponse.collectApis` in Java.
    pub fn collect_apis(
        listener_type: ListenerType,
        api_keys_set: &[&ApiKeys],
        enable_unstable_last_version: bool,
    ) -> Vec<ApiVersion> {
        let mut result = Vec::new();
        for api_key in api_keys_set {
            if let Some(v) = api_key.to_api_version_for_api_response(enable_unstable_last_version, listener_type) {
                result.push(v);
            }
        }
        result
    }

    /// Find the common range of supported API versions between the locally
    /// known range and that of another set.
    ///
    /// Corresponds to `ApiVersionsResponse.intersectForwardableApis` in Java.
    pub fn intersect_forwardable_apis(
        listener_type: ListenerType,
        active_controller_api_versions: &HashMap<ApiKeys, ApiVersion>,
        enable_unstable_last_version: bool,
        client_telemetry_enabled: bool,
    ) -> Vec<ApiVersion> {
        let mut api_keys = Vec::new();
        for api_key in ApiKeys::apis_for_listener(listener_type) {
            let broker_api_version =
                api_key.to_api_version_for_api_response(enable_unstable_last_version, listener_type);
            let broker_api_version = match broker_api_version {
                Some(v) => v,
                None => continue, // Broker does not support this API key
            };

            // Skip telemetry APIs if client telemetry is disabled.
            if (*api_key == ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS || *api_key == ApiKeys::PUSH_TELEMETRY)
                && !client_telemetry_enabled
            {
                continue;
            }

            let final_api_version;
            if !api_key.is_forwardable() {
                final_api_version = broker_api_version;
            } else {
                let controller_version = active_controller_api_versions.get(api_key);
                let intersected = Self::intersect(Some(&broker_api_version), controller_version);
                match intersected {
                    Some(v) => final_api_version = v,
                    None => continue, // No intersection
                }
            }

            api_keys.push(final_api_version);
        }
        api_keys
    }

    /// Computes the intersection of two API version ranges.
    ///
    /// Returns `None` if either version is `None`, or if the ranges do not overlap.
    ///
    /// # Panics
    ///
    /// Panics if both versions are `Some` but have different API keys.
    pub fn intersect(this_version: Option<&ApiVersion>, other: Option<&ApiVersion>) -> Option<ApiVersion> {
        let this = this_version?;
        let other = other?;
        assert_eq!(
            this.api_key, other.api_key,
            "thisVersion.apiKey: {} must be equal to other.apiKey: {}",
            this.api_key, other.api_key,
        );
        let min_version = this.min_version.max(other.min_version);
        let max_version = this.max_version.min(other.max_version);
        if min_version > max_version {
            None
        } else {
            let mut v = ApiVersion::new();
            v.set_api_key(this.api_key);
            v.set_min_version(min_version);
            v.set_max_version(max_version);
            Some(v)
        }
    }

    /// Converts an API key to its API version entry with the full supported range.
    ///
    /// Corresponds to `ApiVersionsResponse.toApiVersion` in Java.
    pub fn to_api_version(api_key: &ApiKeys) -> ApiVersion {
        let mut v = ApiVersion::new();
        v.set_api_key(api_key.id());
        v.set_min_version(api_key.oldest_version());
        v.set_max_version(api_key.latest_version());
        v
    }

    /// Creates a default API versions response for testing.
    ///
    /// Corresponds to `TestUtils.defaultApiVersionsResponse` in Java.
    pub fn default_api_versions_response(listener_type: ListenerType) -> Self {
        Self::default_api_versions_response_with_options(listener_type, true, true)
    }

    /// Creates a default API versions response with configurable options.
    pub fn default_api_versions_response_with_options(
        listener_type: ListenerType,
        enable_unstable_last_version: bool,
        client_telemetry_enabled: bool,
    ) -> Self {
        ApiVersionsResponseBuilder::new()
            .set_api_versions(Self::filter_apis(
                listener_type,
                enable_unstable_last_version,
                client_telemetry_enabled,
            ))
            .set_supported_features(Vec::new())
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .build()
    }
}

impl std::fmt::Display for ApiVersionsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ApiVersionsResponse(data={:?})", self.data)
    }
}

/// Builder for [`ApiVersionsResponse`].
///
/// Corresponds to `ApiVersionsResponse.Builder` in Java.
#[derive(Debug)]
pub struct ApiVersionsResponseBuilder {
    error: Errors,
    throttle_time_ms: i32,
    api_versions: Option<Vec<ApiVersion>>,
    supported_features: Option<Vec<SupportedFeatureKey>>,
    finalized_features: Option<HashMap<String, i16>>,
    finalized_features_epoch: i64,
    zk_migration_enabled: bool,
    alter_feature_level0: bool,
}

impl ApiVersionsResponseBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self {
            error: Errors::None,
            throttle_time_ms: 0,
            api_versions: None,
            supported_features: None,
            finalized_features: None,
            finalized_features_epoch: 0,
            zk_migration_enabled: false,
            alter_feature_level0: false,
        }
    }

    /// Sets the error code.
    pub fn set_error(mut self, error: Errors) -> Self {
        self.error = error;
        self
    }

    /// Sets the throttle time in milliseconds.
    pub fn set_throttle_time_ms(mut self, throttle_time_ms: i32) -> Self {
        self.throttle_time_ms = throttle_time_ms;
        self
    }

    /// Sets the API version collection.
    pub fn set_api_versions(mut self, api_versions: Vec<ApiVersion>) -> Self {
        self.api_versions = Some(api_versions);
        self
    }

    /// Sets the supported features.
    pub fn set_supported_features(mut self, supported_features: Vec<SupportedFeatureKey>) -> Self {
        self.supported_features = Some(supported_features);
        self
    }

    /// Sets the finalized features map.
    pub fn set_finalized_features(mut self, finalized_features: HashMap<String, i16>) -> Self {
        self.finalized_features = Some(finalized_features);
        self
    }

    /// Sets the finalized features epoch.
    pub fn set_finalized_features_epoch(mut self, epoch: i64) -> Self {
        self.finalized_features_epoch = epoch;
        self
    }

    /// Sets whether ZK migration is enabled.
    pub fn set_zk_migration_enabled(mut self, enabled: bool) -> Self {
        self.zk_migration_enabled = enabled;
        self
    }

    /// Sets whether to alter feature level 0.
    ///
    /// When true, features with a minimum supported version of 0 are omitted
    /// to avoid deserialization problems with older clients (see KAFKA-17492).
    pub fn set_alter_feature_level0(mut self, alter: bool) -> Self {
        self.alter_feature_level0 = alter;
        self
    }

    /// Builds the `ApiVersionsResponse`.
    ///
    /// # Panics
    ///
    /// Panics if `api_versions`, `supported_features`, or `finalized_features` was not set.
    pub fn build(self) -> ApiVersionsResponse {
        let mut data = ApiVersionsResponseData::new();
        data.set_error_code(self.error.code());
        data.set_api_keys(self.api_versions.expect("api_versions must be set"));
        data.set_throttle_time_ms(self.throttle_time_ms);
        data.set_supported_features(maybe_filter_supported_feature_keys(
            &self.supported_features.expect("supported_features must be set"),
            self.alter_feature_level0,
        ));
        data.set_finalized_features(create_finalized_feature_keys(
            &self.finalized_features.expect("finalized_features must be set"),
        ));
        data.set_finalized_features_epoch(self.finalized_features_epoch);
        data.set_zk_migration_ready(self.zk_migration_enabled);
        ApiVersionsResponse::new(data)
    }
}

impl Default for ApiVersionsResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Filters supported feature keys, optionally excluding features with min version 0.
///
/// Some older clients will have deserialization problems if a feature's
/// minimum supported level is 0. Therefore, when preparing ApiVersionResponse
/// at versions less than 4, we must omit these features. See KAFKA-17492.
fn maybe_filter_supported_feature_keys(features: &[SupportedFeatureKey], alter_v0: bool) -> Vec<SupportedFeatureKey> {
    let mut converted = Vec::new();
    for feature in features {
        if alter_v0 && feature.min_version == 0 {
            // Omit features with min version 0 when alter_v0 is true
        } else {
            converted.push(feature.clone());
        }
    }
    converted
}

/// Converts finalized features from a map to `FinalizedFeatureKey` collection.
fn create_finalized_feature_keys(finalized_features: &HashMap<String, i16>) -> Vec<FinalizedFeatureKey> {
    let mut converted = Vec::new();
    for (name, &version_level) in finalized_features {
        if version_level != 0 {
            let mut key = FinalizedFeatureKey::new();
            key.set_name(name.clone());
            key.set_min_version_level(version_level);
            key.set_max_version_level(version_level);
            converted.push(key);
        }
    }
    converted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_versions_response_data::SupportedFeatureKey;
    use crate::common::protocol::api_keys::PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION;
    use std::collections::HashSet;

    // Helper: extract the set of ApiKeys in a response
    fn api_keys_in_response(response: &ApiVersionsResponse) -> HashSet<ApiKeys> {
        let mut keys = HashSet::new();
        for version in &response.data().api_keys {
            if let Some(k) = ApiKeys::for_id(version.api_key) {
                keys.insert(*k);
            }
        }
        keys
    }

    // Helper: verify a specific API key has the expected version range in the collection
    fn verify_versions(api_key_id: i16, min_version: i16, max_version: i16, collection: &[ApiVersion]) {
        let expected = {
            let mut v = ApiVersion::new();
            v.set_api_key(api_key_id);
            v.set_min_version(min_version);
            v.set_max_version(max_version);
            v
        };
        let found = collection.iter().find(|v| v.api_key == api_key_id);
        assert_eq!(Some(&expected), found);
    }

    // Helper: count telemetry API keys in response
    fn verify_api_keys_for_telemetry(response: &ApiVersionsResponse, expected_count: usize) {
        let count = response
            .data()
            .api_keys
            .iter()
            .filter(|v| {
                v.api_key == ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS.id() || v.api_key == ApiKeys::PUSH_TELEMETRY.id()
            })
            .count();
        assert_eq!(expected_count, count);
    }

    /// Translated from `ApiVersionsResponseTest.shouldHaveCorrectDefaultApiVersionsResponse`.
    ///
    /// This test checks that the default response for each listener type contains
    /// the correct set of API keys with proper version ranges. The original test
    /// also checks `requestSchemas()`/`responseSchemas()` arrays on the Java
    /// `ApiMessageType`, but our generator does not emit those schema arrays.
    /// We verify the version range consistency instead.
    #[test]
    fn test_should_have_correct_default_api_versions_response_broker() {
        test_default_api_versions_response(ListenerType::Broker);
    }

    #[test]
    fn test_should_have_correct_default_api_versions_response_controller() {
        test_default_api_versions_response(ListenerType::Controller);
    }

    fn test_default_api_versions_response(scope: ListenerType) {
        let default_response = ApiVersionsResponse::default_api_versions_response(scope);
        assert_eq!(
            ApiKeys::apis_for_listener(scope).len(),
            default_response.data().api_keys.len(),
            "API versions for all API keys must be maintained."
        );

        for key in ApiKeys::apis_for_listener(scope) {
            let version = default_response.api_version(key.id());
            assert!(version.is_some(), "Could not find ApiVersion for API {}", key.name());
            let version = version.unwrap();

            if *key == ApiKeys::PRODUCE {
                assert_eq!(
                    PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION,
                    version.min_version,
                    "Incorrect min version for Api {}",
                    key.name()
                );
            } else {
                assert_eq!(
                    key.oldest_version(),
                    version.min_version,
                    "Incorrect min version for Api {}",
                    key.name()
                );
            }
            assert_eq!(
                key.latest_version(),
                version.max_version,
                "Incorrect max version for Api {}",
                key.name()
            );
        }

        assert!(default_response.data().supported_features.is_empty());
        assert!(default_response.data().finalized_features.is_empty());
        assert_eq!(
            API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH,
            default_response.data().finalized_features_epoch
        );
    }

    /// Translated from `ApiVersionsResponseTest.shouldHaveCommonlyAgreedApiVersionResponseWithControllerOnForwardableAPIs`.
    #[test]
    fn test_should_have_commonly_agreed_api_version_response_with_controller_on_forwardable_apis() {
        let forwardable_api_key = ApiKeys::CREATE_ACLS;
        let non_forwardable_api_key = ApiKeys::JOIN_GROUP;
        let min_version: i16 = 2;
        let max_version: i16 = 3;

        let mut active_controller_api_versions = HashMap::new();
        let mut fwd_v = ApiVersion::new();
        fwd_v.set_api_key(forwardable_api_key.id());
        fwd_v.set_min_version(min_version);
        fwd_v.set_max_version(max_version);
        active_controller_api_versions.insert(forwardable_api_key, fwd_v);

        let mut non_fwd_v = ApiVersion::new();
        non_fwd_v.set_api_key(non_forwardable_api_key.id());
        non_fwd_v.set_min_version(min_version);
        non_fwd_v.set_max_version(max_version);
        active_controller_api_versions.insert(non_forwardable_api_key, non_fwd_v);

        let common_response = ApiVersionsResponse::intersect_forwardable_apis(
            ListenerType::Broker,
            &active_controller_api_versions,
            true,
            false,
        );

        verify_versions(forwardable_api_key.id(), min_version, max_version, &common_response);

        verify_versions(
            non_forwardable_api_key.id(),
            ApiKeys::JOIN_GROUP.oldest_version(),
            ApiKeys::JOIN_GROUP.latest_version(),
            &common_response,
        );
    }

    /// Translated from `ApiVersionsResponseTest.shouldReturnAllKeysWhenThrottleMsIsDefaultThrottle`.
    #[test]
    fn test_should_return_all_keys_when_throttle_ms_is_default_throttle() {
        let response = ApiVersionsResponseBuilder::new()
            .set_throttle_time_ms(super::super::abstract_response::DEFAULT_THROTTLE_TIME)
            .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, true, true))
            .set_supported_features(Vec::new())
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .build();

        let broker_apis: HashSet<ApiKeys> =
            ApiKeys::apis_for_listener(ListenerType::Broker).into_iter().copied().collect();
        assert_eq!(broker_apis, api_keys_in_response(&response));
        assert_eq!(
            super::super::abstract_response::DEFAULT_THROTTLE_TIME,
            response.throttle_time_ms()
        );
        assert!(response.data().supported_features.is_empty());
        assert!(response.data().finalized_features.is_empty());
        assert_eq!(
            API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH,
            response.data().finalized_features_epoch
        );
    }

    /// Translated from `ApiVersionsResponseTest.shouldCreateApiResponseWithTelemetryWhenEnabled`.
    #[test]
    fn test_should_create_api_response_with_telemetry_when_enabled() {
        let response = ApiVersionsResponseBuilder::new()
            .set_throttle_time_ms(10)
            .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, true, true))
            .set_supported_features(Vec::new())
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .build();
        verify_api_keys_for_telemetry(&response, 2);
    }

    /// Translated from `ApiVersionsResponseTest.shouldNotCreateApiResponseWithTelemetryWhenDisabled`.
    #[test]
    fn test_should_not_create_api_response_with_telemetry_when_disabled() {
        let response = ApiVersionsResponseBuilder::new()
            .set_throttle_time_ms(10)
            .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, true, false))
            .set_supported_features(Vec::new())
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .build();
        verify_api_keys_for_telemetry(&response, 0);
    }

    /// Translated from `ApiVersionsResponseTest.testBrokerApisAreEnabled`.
    #[test]
    fn test_broker_apis_are_enabled() {
        let response = ApiVersionsResponseBuilder::new()
            .set_throttle_time_ms(super::super::abstract_response::DEFAULT_THROTTLE_TIME)
            .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, true, true))
            .set_supported_features(Vec::new())
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .build();

        let exposed = api_keys_in_response(&response);

        for key in ApiKeys::ALL {
            if key.in_scope(ListenerType::Broker) {
                assert!(exposed.contains(key), "Expected {} in response", key.name());
            } else {
                assert!(!exposed.contains(key), "Did not expect {} in response", key.name());
            }
        }
    }

    /// Translated from `ApiVersionsResponseTest.testIntersect`.
    #[test]
    fn test_intersect() {
        assert!(ApiVersionsResponse::intersect(None, None).is_none());

        let v10 = {
            let mut v = ApiVersion::new();
            v.set_api_key(10);
            v
        };
        let v3 = {
            let mut v = ApiVersion::new();
            v.set_api_key(3);
            v
        };
        let result = std::panic::catch_unwind(|| ApiVersionsResponse::intersect(Some(&v10), Some(&v3)));
        assert!(result.is_err(), "Should panic when api keys differ");

        let min: i16 = 0;
        let max: i16 = 10;
        let this_version = {
            let mut v = ApiVersion::new();
            v.set_api_key(ApiKeys::FETCH.id());
            v.set_min_version(min);
            v.set_max_version(i16::MAX);
            v
        };
        let other = {
            let mut v = ApiVersion::new();
            v.set_api_key(ApiKeys::FETCH.id());
            v.set_min_version(i16::MIN);
            v.set_max_version(max);
            v
        };
        let expected = {
            let mut v = ApiVersion::new();
            v.set_api_key(ApiKeys::FETCH.id());
            v.set_min_version(min);
            v.set_max_version(max);
            v
        };

        assert!(ApiVersionsResponse::intersect(Some(&this_version), None).is_none());
        assert!(ApiVersionsResponse::intersect(None, Some(&other)).is_none());

        assert_eq!(
            expected,
            ApiVersionsResponse::intersect(Some(&this_version), Some(&other)).unwrap()
        );
        // test for symmetric
        assert_eq!(
            expected,
            ApiVersionsResponse::intersect(Some(&other), Some(&this_version)).unwrap()
        );
    }

    /// Translated from `ApiVersionsResponseTest.testAlterV0Features`.
    #[test]
    fn test_alter_v0_features_false() {
        test_alter_v0_features(false);
    }

    #[test]
    fn test_alter_v0_features_true() {
        test_alter_v0_features(true);
    }

    fn test_alter_v0_features(alter_v0_features: bool) {
        let mut feature = SupportedFeatureKey::new();
        feature.set_name("my.feature".to_string());
        feature.set_min_version(0);
        feature.set_max_version(1);

        let response = ApiVersionsResponseBuilder::new()
            .set_api_versions(ApiVersionsResponse::filter_apis(ListenerType::Broker, true, true))
            .set_supported_features(vec![feature])
            .set_finalized_features(HashMap::new())
            .set_finalized_features_epoch(API_VERSIONS_RESPONSE_UNKNOWN_FINALIZED_FEATURES_EPOCH)
            .set_alter_feature_level0(alter_v0_features)
            .build();

        let found = response.data().supported_features.iter().find(|f| f.name == "my.feature");

        if alter_v0_features {
            assert!(found.is_none());
        } else {
            let expected = {
                let mut k = SupportedFeatureKey::new();
                k.set_name("my.feature".to_string());
                k.set_min_version(0);
                k.set_max_version(1);
                k
            };
            assert_eq!(Some(&expected), found);
        }
    }
}
