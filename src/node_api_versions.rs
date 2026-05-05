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

//! Translation of `org.apache.kafka.clients.NodeApiVersions`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use std::collections::{BTreeMap, HashMap};

use crate::common::errors::KafkaError;
use crate::common::feature::SupportedVersionRange;
use crate::common::message::api_versions_response_data::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey};
use crate::common::protocol::{ApiKey, ApiKeys};
use crate::common::requests::ApiVersionsResponse;

/// An internal class which represents the API versions supported by a
/// particular node.
///
/// Mirrors the Java class.
pub struct NodeApiVersions {
    /// A map of the usable versions of each API, keyed by the api key
    /// `id`. Mirrors Java's `EnumMap<ApiKeys, ApiVersion>`.
    supported_versions: HashMap<i16, ApiVersion>,
    /// List of APIs which the broker supports, but which are unknown to
    /// the client. Mirrors Java's `List<ApiVersion> unknownApis`.
    unknown_apis: Vec<ApiVersion>,
    /// Mirrors Java's `Map<String, SupportedVersionRange> supportedFeatures`.
    supported_features: HashMap<String, SupportedVersionRange>,
    /// Mirrors Java's `Map<String, Short> finalizedFeatures`.
    finalized_features: HashMap<String, i16>,
    /// Mirrors Java's `long finalizedFeaturesEpoch`.
    finalized_features_epoch: i64,
}

impl NodeApiVersions {
    /// Create a `NodeApiVersions` object with the current ApiVersions.
    /// Mirrors `NodeApiVersions.create()`.
    pub fn create_default() -> Self {
        Self::create_with_overrides(Vec::new()).expect("default range is always valid")
    }

    /// Create a `NodeApiVersions` object.
    ///
    /// `overrides` — API versions to override. Any `ApiVersion` not
    /// specified here will be set to the current client value.
    ///
    /// Mirrors `NodeApiVersions.create(Collection<ApiVersion>)`.
    ///
    /// # Errors
    ///
    /// Propagates any [`KafkaError`] from the inner constructor.
    pub fn create_with_overrides(overrides: Vec<ApiVersion>) -> Result<Self, KafkaError> {
        let mut api_versions: Vec<ApiVersion> = overrides;
        for api_key in ApiKeys::client_apis() {
            let exists = api_versions.iter().any(|av| av.api_key == api_key.id);
            if !exists {
                api_versions.push(ApiVersionsResponse::to_api_version(api_key));
            }
        }
        Self::with_features(api_versions, Vec::new(), Vec::new(), -1)
    }

    /// Create a `NodeApiVersions` object with a single ApiKey. Mainly
    /// used in tests.
    ///
    /// Mirrors `NodeApiVersions.create(short, short, short)`.
    pub fn create_single(api_key: i16, min_version: i16, max_version: i16) -> Result<Self, KafkaError> {
        Self::create_with_overrides(vec![ApiVersion {
            api_key,
            min_version,
            max_version,
            unknown_tagged_fields: Vec::new(),
        }])
    }

    /// Mirrors `new NodeApiVersions(Collection<ApiVersion>, Collection<SupportedFeatureKey>)`.
    pub fn new(
        node_api_versions: Vec<ApiVersion>,
        node_supported_features: Vec<SupportedFeatureKey>,
    ) -> Result<Self, KafkaError> {
        Self::with_features(node_api_versions, node_supported_features, Vec::new(), -1)
    }

    /// Mirrors `new NodeApiVersions(Collection<ApiVersion>,
    /// Collection<SupportedFeatureKey>, Collection<FinalizedFeatureKey>, long)`.
    pub fn with_features(
        node_api_versions: Vec<ApiVersion>,
        node_supported_features: Vec<SupportedFeatureKey>,
        node_finalized_features: Vec<FinalizedFeatureKey>,
        finalized_features_epoch: i64,
    ) -> Result<Self, KafkaError> {
        let mut supported_versions: HashMap<i16, ApiVersion> = HashMap::new();
        let mut unknown_apis: Vec<ApiVersion> = Vec::new();
        for node_api_version in node_api_versions {
            if ApiKeys::has_id(node_api_version.api_key as i32) {
                supported_versions.insert(node_api_version.api_key, node_api_version);
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknown_apis.push(node_api_version);
            }
        }

        let mut supported_features = HashMap::new();
        for supported_feature in node_supported_features {
            supported_features.insert(
                supported_feature.name.clone(),
                SupportedVersionRange::new(supported_feature.min_version, supported_feature.max_version)?,
            );
        }

        let mut finalized_features = HashMap::new();
        for finalized_feature in node_finalized_features {
            finalized_features.insert(finalized_feature.name.clone(), finalized_feature.max_version_level);
        }

        Ok(NodeApiVersions {
            supported_versions,
            unknown_apis,
            supported_features,
            finalized_features,
            finalized_features_epoch,
        })
    }

    /// Return the most recent version supported by both the node and
    /// the local software. Mirrors
    /// `NodeApiVersions.latestUsableVersion(ApiKeys)`.
    pub fn latest_usable_version(&self, api_key: &ApiKey) -> Result<i16, KafkaError> {
        self.latest_usable_version_in_range(api_key, api_key.oldest_version(), api_key.latest_version())
    }

    /// Get the latest version supported by the broker within an allowed
    /// range of versions. Mirrors
    /// `NodeApiVersions.latestUsableVersion(ApiKeys, short, short)`.
    pub fn latest_usable_version_in_range(
        &self,
        api_key: &ApiKey,
        oldest_allowed_version: i16,
        latest_allowed_version: i16,
    ) -> Result<i16, KafkaError> {
        let supported_version = match self.supported_versions.get(&api_key.id) {
            Some(v) => v,
            None => {
                return Err(KafkaError::UnsupportedVersion(format!(
                    "The node does not support {}",
                    api_key.name
                )));
            },
        };

        let candidate = ApiVersion {
            api_key: api_key.id,
            min_version: oldest_allowed_version,
            max_version: latest_allowed_version,
            unknown_tagged_fields: Vec::new(),
        };
        let intersect = ApiVersionsResponse::intersect(Some(supported_version), Some(&candidate))?;
        match intersect {
            Some(v) => Ok(v.max_version),
            None => Err(KafkaError::UnsupportedVersion(format!(
                "The node does not support {} with version in range [{},{}]. The supported range is [{},{}].",
                api_key.name,
                oldest_allowed_version,
                latest_allowed_version,
                supported_version.min_version,
                supported_version.max_version
            ))),
        }
    }

    /// Get the version information for a given API. Mirrors
    /// `NodeApiVersions.apiVersion(ApiKeys)`. Returns `None` if the API
    /// is unsupported.
    pub fn api_version(&self, api_key: &ApiKey) -> Option<&ApiVersion> {
        self.supported_versions.get(&api_key.id)
    }

    /// Mirrors `NodeApiVersions.allSupportedApiVersions()`.
    pub fn all_supported_api_versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.supported_versions
    }

    /// Mirrors `NodeApiVersions.supportedFeatures()`.
    pub fn supported_features(&self) -> &HashMap<String, SupportedVersionRange> {
        &self.supported_features
    }

    /// Mirrors `NodeApiVersions.finalizedFeatures()`.
    pub fn finalized_features(&self) -> &HashMap<String, i16> {
        &self.finalized_features
    }

    /// Mirrors `NodeApiVersions.finalizedFeaturesEpoch()`.
    pub fn finalized_features_epoch(&self) -> i64 {
        self.finalized_features_epoch
    }

    fn api_version_to_text(&self, api_version: &ApiVersion) -> String {
        let mut bld = String::new();
        let api_key = if ApiKeys::has_id(api_version.api_key as i32) {
            let key = ApiKeys::for_id(api_version.api_key as i32).expect("checked by has_id");
            bld.push_str(&format!("{}({}): ", key.name, key.id));
            Some(key)
        } else {
            bld.push_str(&format!("UNKNOWN({}): ", api_version.api_key));
            None
        };

        if api_version.min_version == api_version.max_version {
            bld.push_str(&api_version.min_version.to_string());
        } else {
            bld.push_str(&format!("{} to {}", api_version.min_version, api_version.max_version));
        }

        if let Some(api_key) = api_key {
            let supported_version = self.supported_versions.get(&api_key.id).expect(
                "supported_versions contains every api_key.id known to api_version_to_text — \
                 the only callers thread the id through `supported_versions.values()` or `unknown_apis` (latter excluded by has_id)",
            );
            if api_key.latest_version() < supported_version.min_version {
                bld.push_str(" [unusable: node too new]");
            } else if supported_version.max_version < api_key.oldest_version() {
                bld.push_str(" [unusable: node too old]");
            } else {
                let latest_usable = api_key.latest_version().min(supported_version.max_version);
                bld.push_str(&format!(" [usable: {latest_usable}]"));
            }
        }
        bld
    }

    /// Mirrors `NodeApiVersions.toString(boolean)`.
    pub fn to_string_with_line_breaks(&self, line_breaks: bool) -> String {
        // The apiVersion collection may not be in sorted order. Put it
        // into a `BTreeMap` (Java uses TreeMap) before printing so we
        // always print in ascending order of api key id.
        let mut api_keys_text: BTreeMap<i16, String> = BTreeMap::new();
        for supported_version in self.supported_versions.values() {
            api_keys_text.insert(supported_version.api_key, self.api_version_to_text(supported_version));
        }
        for api_version in &self.unknown_apis {
            api_keys_text.insert(api_version.api_key, self.api_version_to_text(api_version));
        }

        // Also handle the case where some apiKey types are not specified
        // at all in the given ApiVersions, which may happen when the
        // remote is too old.
        for api_key in ApiKeys::client_apis() {
            if let std::collections::btree_map::Entry::Vacant(entry) = api_keys_text.entry(api_key.id) {
                entry.insert(format!("{}({}): UNSUPPORTED", api_key.name, api_key.id));
            }
        }

        let separator = if line_breaks { ",\n\t" } else { ", " };
        let mut bld = String::new();
        bld.push('(');
        if line_breaks {
            bld.push_str("\n\t");
        }
        let joined: Vec<&String> = api_keys_text.values().collect();
        let joined_str: Vec<&str> = joined.iter().map(|s| s.as_str()).collect();
        bld.push_str(&joined_str.join(separator));
        if line_breaks {
            bld.push('\n');
        }
        bld.push(')');
        bld
    }
}

impl std::fmt::Display for NodeApiVersions {
    /// Mirrors `NodeApiVersions.toString()` (no line breaks). The Java
    /// rustdoc warns this method is relatively expensive — same caveat
    /// applies here.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_with_line_breaks(false))
    }
}

impl std::fmt::Debug for NodeApiVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeApiVersions")
            .field("supported_versions", &self.supported_versions.len())
            .field("unknown_apis", &self.unknown_apis.len())
            .field("supported_features", &self.supported_features.len())
            .field("finalized_features_epoch", &self.finalized_features_epoch)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `NodeApiVersionsTest`.

    use super::*;
    use crate::common::message::api_versions_response_data::{FinalizedFeatureKey, SupportedFeatureKey};

    /// Java: `testUnsupportedVersionsToString`.
    #[test]
    fn unsupported_versions_to_string() {
        let versions = NodeApiVersions::new(Vec::new(), Vec::new()).expect("constructor");
        let mut expected = String::new();
        let mut prefix = "(";
        for api_key in ApiKeys::client_apis() {
            expected.push_str(prefix);
            expected.push_str(&format!("{}({}): UNSUPPORTED", api_key.name, api_key.id));
            prefix = ", ";
        }
        expected.push(')');
        assert_eq!(versions.to_string(), expected);
    }

    /// Java: `testUnknownApiVersionsToString`.
    #[test]
    fn unknown_api_versions_to_string() {
        let versions = NodeApiVersions::create_single(337, 0, 1).expect("constructor");
        assert!(versions.to_string().ends_with("UNKNOWN(337): 0 to 1)"), "got: {}", versions);
    }

    /// Java: `testVersionsToString`.
    #[test]
    fn versions_to_string() {
        let mut version_list: Vec<ApiVersion> = Vec::new();
        for api_key in ApiKeys::values() {
            if api_key.id == 20 {
                // DELETE_TOPICS
                version_list.push(ApiVersion {
                    api_key: api_key.id,
                    min_version: 10000,
                    max_version: 10001,
                    unknown_tagged_fields: Vec::new(),
                });
            } else {
                version_list.push(ApiVersionsResponse::to_api_version(api_key));
            }
        }
        let versions = NodeApiVersions::new(version_list, Vec::new()).expect("constructor");

        let mut expected = String::new();
        let mut prefix = "(";
        for api_key in ApiKeys::values() {
            expected.push_str(prefix);
            if api_key.id == 20 {
                expected.push_str("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else if !api_key.has_valid_version() {
                expected.push_str(&format!("{}({}): 0 to -1 [unusable: node too new]", api_key.name, api_key.id));
            } else {
                expected.push_str(&format!("{}({}): ", api_key.name, api_key.id));
                if api_key.oldest_version() == api_key.latest_version() {
                    expected.push_str(&api_key.oldest_version().to_string());
                } else {
                    expected.push_str(&format!("{} to {}", api_key.oldest_version(), api_key.latest_version()));
                }
                expected.push_str(&format!(" [usable: {}]", api_key.latest_version()));
            }
            prefix = ", ";
        }
        expected.push(')');
        assert_eq!(versions.to_string(), expected);
    }

    /// Java: `testLatestUsableVersion`.
    #[test]
    fn latest_usable_version() {
        let api_versions = NodeApiVersions::create_single(0, 8, 10).expect("constructor"); // PRODUCE
        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        assert_eq!(api_versions.latest_usable_version(produce).unwrap(), 10);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 7, 8).unwrap(), 8);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 8, 8).unwrap(), 8);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 8, 9).unwrap(), 9);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 8, 10).unwrap(), 10);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 9, 9).unwrap(), 9);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 9, 10).unwrap(), 10);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 10, 10).unwrap(), 10);
        assert_eq!(api_versions.latest_usable_version_in_range(produce, 10, 11).unwrap(), 10);
    }

    /// Java: `testLatestUsableVersionOutOfRangeLow`.
    #[test]
    fn latest_usable_version_out_of_range_low() {
        let api_versions = NodeApiVersions::create_single(0, 1, 2).expect("constructor"); // PRODUCE
        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        let err = api_versions.latest_usable_version_in_range(produce, 3, 4).unwrap_err();
        assert!(matches!(err, KafkaError::UnsupportedVersion(_)));
    }

    /// Java: `testLatestUsableVersionOutOfRangeHigh`.
    #[test]
    fn latest_usable_version_out_of_range_high() {
        let api_versions = NodeApiVersions::create_single(0, 2, 3).expect("constructor"); // PRODUCE
        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        let err = api_versions.latest_usable_version_in_range(produce, 0, 1).unwrap_err();
        assert!(matches!(err, KafkaError::UnsupportedVersion(_)));
    }

    /// Java: `testUsableVersionCalculationNoKnownVersions`.
    #[test]
    fn usable_version_calculation_no_known_versions() {
        let versions = NodeApiVersions::new(Vec::new(), Vec::new()).expect("constructor");
        let fetch = ApiKeys::for_id(1).expect("FETCH");
        let err = versions.latest_usable_version(fetch).unwrap_err();
        assert!(matches!(err, KafkaError::UnsupportedVersion(_)));
    }

    /// Java: `testLatestUsableVersionOutOfRange`.
    #[test]
    fn latest_usable_version_out_of_range() {
        let api_versions = NodeApiVersions::create_single(0, 300, 300).expect("constructor"); // PRODUCE
        let produce = ApiKeys::for_id(0).expect("PRODUCE");
        let err = api_versions.latest_usable_version(produce).unwrap_err();
        assert!(matches!(err, KafkaError::UnsupportedVersion(_)));
    }

    /// Java: `testUsableVersionLatestVersions` (`@ParameterizedTest`
    /// with `EnumSource(ApiMessageType.ListenerType.class)`). The Rust
    /// translation iterates the same enum.
    #[test]
    fn usable_version_latest_versions_for_all_listeners() {
        use crate::common::message::api_message_type::ListenerType;

        for scope in [ListenerType::Broker, ListenerType::Controller] {
            // Mirror Java's `TestUtils.defaultApiVersionsResponse(scope)`
            // — Java's helper synthesises a default `ApiVersionsResponse`
            // covering every api key in the listener scope at its current
            // min/max version.
            let mut version_list: Vec<ApiVersion> = ApiKeys::apis_for_listener(scope)
                .into_iter()
                .map(ApiVersionsResponse::to_api_version)
                .collect();
            // Add an API key that we don't know about.
            version_list.push(ApiVersion {
                api_key: 100,
                min_version: 0,
                max_version: 1,
                unknown_tagged_fields: Vec::new(),
            });
            let versions = NodeApiVersions::new(version_list, Vec::new()).expect("constructor");
            for api_key in ApiKeys::apis_for_listener(scope) {
                if !api_key.has_valid_version() {
                    // Java skips keys with no valid version (ApiVersionsResponse
                    // omits them too); our helper-generated ApiVersion would
                    // have min=0, max=-1 which ApiVersionsResponse.intersect
                    // returns None for.
                    continue;
                }
                assert_eq!(
                    versions.latest_usable_version(api_key).unwrap(),
                    api_key.latest_version(),
                    "api_key {}({})",
                    api_key.name,
                    api_key.id
                );
            }
        }
    }

    /// Java: `testConstructionFromApiVersionsResponse` (`@ParameterizedTest`).
    #[test]
    fn construction_from_api_versions_response() {
        use crate::common::message::api_message_type::ListenerType;

        for scope in [ListenerType::Broker, ListenerType::Controller] {
            let api_keys: Vec<ApiVersion> = ApiKeys::apis_for_listener(scope)
                .into_iter()
                .map(ApiVersionsResponse::to_api_version)
                .collect();
            let versions = NodeApiVersions::new(api_keys.clone(), Vec::new()).expect("constructor");
            for api_version_key in &api_keys {
                let api_key = ApiKeys::for_id(api_version_key.api_key as i32).expect("known api key");
                let api_version = versions.api_version(api_key).expect("present");
                assert_eq!(api_version_key.api_key, api_version.api_key);
                assert_eq!(api_version_key.min_version, api_version.min_version);
                assert_eq!(api_version_key.max_version, api_version.max_version);
            }
        }
    }

    /// Java: `testFeatures`.
    #[test]
    fn features() {
        let versions = NodeApiVersions::with_features(
            Vec::new(),
            vec![SupportedFeatureKey {
                name: "transaction.version".into(),
                min_version: 0,
                max_version: 2,
                unknown_tagged_fields: Vec::new(),
            }],
            vec![FinalizedFeatureKey {
                name: "transaction.version".into(),
                max_version_level: 2,
                min_version_level: 2,
                unknown_tagged_fields: Vec::new(),
            }],
            0,
        )
        .expect("constructor");
        let supported_version_range = versions.supported_features().get("transaction.version").expect("present");
        assert_eq!(supported_version_range.min(), 0);
        assert_eq!(supported_version_range.max(), 2);
        assert_eq!(*versions.finalized_features().get("transaction.version").expect("present"), 2);
        assert_eq!(versions.finalized_features_epoch(), 0);
    }
}
