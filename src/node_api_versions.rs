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

//! Per-node API version information.
//!
//! Translated from `org.apache.kafka.clients.NodeApiVersions`.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::api_versions_response_data::{ApiVersion, FinalizedFeatureKey, SupportedFeatureKey};
use crate::common::Error;
use crate::common::feature::SupportedVersionRange;
use crate::common::protocol::ApiKeys;
use crate::common::requests::ApiVersionsResponse;

/// An internal class which represents the API versions supported by a particular node.
#[derive(Debug, Clone)]
pub struct NodeApiVersions {
    /// A map of the usable versions of each API, keyed by the ApiKeys instance.
    supported_versions: HashMap<ApiKeys, ApiVersion>,
    /// List of APIs which the broker supports, but which are unknown to the client.
    unknown_apis: Vec<ApiVersion>,
    /// Supported features advertised by the node.
    supported_features: HashMap<String, SupportedVersionRange>,
    /// Finalized features advertised by the node.
    finalized_features: HashMap<String, i16>,
    /// The finalized features epoch.
    finalized_features_epoch: i64,
}

impl NodeApiVersions {
    /// Create a `NodeApiVersions` object with the current ApiVersions.
    pub fn create() -> Self {
        Self::create_with_overrides(&[])
    }

    /// Create a `NodeApiVersions` object.
    ///
    /// Any ApiVersion not specified in `overrides` will be set to the current client value.
    pub fn create_with_overrides(overrides: &[ApiVersion]) -> Self {
        let mut api_versions: Vec<ApiVersion> = overrides.to_vec();
        for api_key in ApiKeys::client_apis() {
            let exists = api_versions.iter().any(|v| v.api_key == api_key.id());
            if !exists {
                api_versions.push(ApiVersionsResponse::to_api_version(api_key));
            }
        }
        Self::new(&api_versions, &[], &[], -1)
    }

    /// Create a `NodeApiVersions` object with a single ApiKey. Mainly used in tests.
    pub fn create_single(api_key: i16, min_version: i16, max_version: i16) -> Self {
        let mut v = ApiVersion::new();
        v.set_api_key(api_key);
        v.set_min_version(min_version);
        v.set_max_version(max_version);
        Self::create_with_overrides(&[v])
    }

    /// Create a `NodeApiVersions` from API versions and supported features.
    pub fn with_supported_features(
        node_api_versions: &[ApiVersion],
        node_supported_features: &[SupportedFeatureKey],
    ) -> Self {
        Self::new(node_api_versions, node_supported_features, &[], -1)
    }

    /// Create a `NodeApiVersions` from API versions, supported features,
    /// finalized features, and epoch.
    pub fn new(
        node_api_versions: &[ApiVersion],
        node_supported_features: &[SupportedFeatureKey],
        node_finalized_features: &[FinalizedFeatureKey],
        finalized_features_epoch: i64,
    ) -> Self {
        let mut supported_versions = HashMap::new();
        let mut unknown_apis = Vec::new();

        for node_api_version in node_api_versions {
            if let Some(api_key) = ApiKeys::for_id(node_api_version.api_key) {
                supported_versions.insert(*api_key, node_api_version.clone());
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknown_apis.push(node_api_version.clone());
            }
        }

        let mut supported_features = HashMap::new();
        for supported_feature in node_supported_features {
            // SupportedVersionRange::new returns Result; since the data comes from the broker
            // we trust the values are valid, but handle the error gracefully.
            if let Ok(range) = SupportedVersionRange::new(supported_feature.min_version, supported_feature.max_version)
            {
                supported_features.insert(supported_feature.name.clone(), range);
            }
        }

        let mut finalized_features = HashMap::new();
        for finalized_feature in node_finalized_features {
            finalized_features.insert(finalized_feature.name.clone(), finalized_feature.max_version_level);
        }

        Self {
            supported_versions,
            unknown_apis,
            supported_features,
            finalized_features,
            finalized_features_epoch,
        }
    }

    /// Return the most recent version supported by both the node and the local software.
    ///
    /// # Errors
    /// Returns an error if the node does not support the given API key.
    pub fn latest_usable_version(&self, api_key: &ApiKeys) -> Result<i16, Error> {
        self.latest_usable_version_in_range(api_key, api_key.oldest_version(), api_key.latest_version())
    }

    /// Get the latest version supported by the broker within an allowed range of versions.
    ///
    /// # Errors
    /// Returns an error if the node does not support the API key or if there is no
    /// intersection between the node's supported range and the allowed range.
    pub fn latest_usable_version_in_range(
        &self,
        api_key: &ApiKeys,
        oldest_allowed_version: i16,
        latest_allowed_version: i16,
    ) -> Result<i16, Error> {
        let supported_version = self
            .supported_versions
            .get(api_key)
            .ok_or_else(|| Error::unsupported_version(format!("The node does not support {}", api_key.name())))?;

        let mut allowed = ApiVersion::new();
        allowed.set_api_key(api_key.id());
        allowed.set_min_version(oldest_allowed_version);
        allowed.set_max_version(latest_allowed_version);

        let intersect_version = ApiVersionsResponse::intersect(Some(supported_version), Some(&allowed));
        match intersect_version {
            Some(v) => Ok(v.max_version),
            None => Err(Error::unsupported_version(format!(
                "The node does not support {} with version in range [{},{}]. \
                 The supported range is [{},{}].",
                api_key.name(),
                oldest_allowed_version,
                latest_allowed_version,
                supported_version.min_version,
                supported_version.max_version
            ))),
        }
    }

    /// Convert the object to a string.
    ///
    /// If `line_breaks` is true, a linebreak is added after each API.
    pub fn to_string_with_line_breaks(&self, line_breaks: bool) -> String {
        // The apiVersion collection may not be in sorted order. We put it into
        // a BTreeMap before printing it out to ensure ascending order.
        let mut api_keys_text: BTreeMap<i16, String> = BTreeMap::new();

        for supported_version in self.supported_versions.values() {
            api_keys_text.insert(supported_version.api_key, self.api_version_to_text(supported_version));
        }
        for api_version in &self.unknown_apis {
            api_keys_text.insert(api_version.api_key, self.api_version_to_text(api_version));
        }

        // Also handle the case where some apiKey types are not specified at all in the given
        // ApiVersions, which may happen when the remote is too old.
        for api_key in ApiKeys::client_apis() {
            api_keys_text
                .entry(api_key.id())
                .or_insert_with(|| format!("{}({}): UNSUPPORTED", api_key.name(), api_key.id()));
        }

        let separator = if line_breaks { ",\n\t" } else { ", " };
        let mut bld = String::new();
        bld.push('(');
        if line_breaks {
            bld.push_str("\n\t");
        }
        let values: Vec<&String> = api_keys_text.values().collect();
        bld.push_str(&values.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(separator));
        if line_breaks {
            bld.push('\n');
        }
        bld.push(')');
        bld
    }

    /// Format a single API version entry as a human-readable string.
    fn api_version_to_text(&self, api_version: &ApiVersion) -> String {
        let mut bld = String::new();
        let api_key = ApiKeys::for_id(api_version.api_key);

        if let Some(key) = api_key {
            bld.push_str(key.name());
            bld.push('(');
            bld.push_str(&key.id().to_string());
            bld.push_str("): ");
        } else {
            bld.push_str("UNKNOWN(");
            bld.push_str(&api_version.api_key.to_string());
            bld.push_str("): ");
        }

        if api_version.min_version == api_version.max_version {
            bld.push_str(&api_version.min_version.to_string());
        } else {
            bld.push_str(&api_version.min_version.to_string());
            bld.push_str(" to ");
            bld.push_str(&api_version.max_version.to_string());
        }

        if let Some(key) = api_key {
            let supported_version = &self.supported_versions[key];
            if key.latest_version() < supported_version.min_version {
                bld.push_str(" [unusable: node too new]");
            } else if supported_version.max_version < key.oldest_version() {
                bld.push_str(" [unusable: node too old]");
            } else {
                let latest_usable_version = key.latest_version().min(supported_version.max_version);
                bld.push_str(" [usable: ");
                bld.push_str(&latest_usable_version.to_string());
                bld.push(']');
            }
        }
        bld
    }

    /// Get the version information for a given API.
    ///
    /// Returns `None` if the API is unsupported by this node.
    pub fn api_version(&self, api_key: &ApiKeys) -> Option<&ApiVersion> {
        self.supported_versions.get(api_key)
    }

    /// Returns all supported API versions.
    pub fn all_supported_api_versions(&self) -> &HashMap<ApiKeys, ApiVersion> {
        &self.supported_versions
    }

    /// Returns the supported features.
    pub fn supported_features(&self) -> &HashMap<String, SupportedVersionRange> {
        &self.supported_features
    }

    /// Returns the finalized features.
    pub fn finalized_features(&self) -> &HashMap<String, i16> {
        &self.finalized_features
    }

    /// Returns the finalized features epoch.
    pub fn finalized_features_epoch(&self) -> i64 {
        self.finalized_features_epoch
    }
}

impl fmt::Display for NodeApiVersions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_with_line_breaks(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_message_type::ListenerType;

    /// Translated from `NodeApiVersionsTest.testUnsupportedVersionsToString`
    #[test]
    fn test_unsupported_versions_to_string() {
        let versions = NodeApiVersions::with_supported_features(&[], &[]);
        let mut bld = String::new();
        let mut prefix = "(";
        for api_key in ApiKeys::client_apis() {
            bld.push_str(prefix);
            bld.push_str(api_key.name());
            bld.push('(');
            bld.push_str(&api_key.id().to_string());
            bld.push_str("): UNSUPPORTED");
            prefix = ", ";
        }
        bld.push(')');
        assert_eq!(bld, versions.to_string());
    }

    /// Translated from `NodeApiVersionsTest.testUnknownApiVersionsToString`
    #[test]
    fn test_unknown_api_versions_to_string() {
        let versions = NodeApiVersions::create_single(337, 0, 1);
        assert!(
            versions.to_string().ends_with("UNKNOWN(337): 0 to 1)"),
            "Unexpected toString: {}",
            versions
        );
    }

    /// Translated from `NodeApiVersionsTest.testVersionsToString`
    #[test]
    fn test_versions_to_string() {
        let mut version_list = Vec::new();
        for api_key in ApiKeys::ALL {
            if *api_key == ApiKeys::DELETE_TOPICS {
                let mut v = ApiVersion::new();
                v.set_api_key(api_key.id());
                v.set_min_version(10000);
                v.set_max_version(10001);
                version_list.push(v);
            } else {
                version_list.push(ApiVersionsResponse::to_api_version(api_key));
            }
        }
        let versions = NodeApiVersions::with_supported_features(&version_list, &[]);
        let mut bld = String::new();
        let mut prefix = "(";
        for api_key in ApiKeys::ALL {
            bld.push_str(prefix);
            if *api_key == ApiKeys::DELETE_TOPICS {
                bld.push_str("DeleteTopics(20): 10000 to 10001 [unusable: node too new]");
            } else if !api_key.has_valid_version() {
                bld.push_str(&format!(
                    "{}({}): 0 to -1 [unusable: node too new]",
                    api_key.name(),
                    api_key.id()
                ));
            } else {
                bld.push_str(api_key.name());
                bld.push('(');
                bld.push_str(&api_key.id().to_string());
                bld.push_str("): ");
                if api_key.oldest_version() == api_key.latest_version() {
                    bld.push_str(&api_key.oldest_version().to_string());
                } else {
                    bld.push_str(&api_key.oldest_version().to_string());
                    bld.push_str(" to ");
                    bld.push_str(&api_key.latest_version().to_string());
                }
                bld.push_str(" [usable: ");
                bld.push_str(&api_key.latest_version().to_string());
                bld.push(']');
            }
            prefix = ", ";
        }
        bld.push(')');
        assert_eq!(bld, versions.to_string());
    }

    /// Translated from `NodeApiVersionsTest.testLatestUsableVersion`
    #[test]
    fn test_latest_usable_version() {
        let api_versions = NodeApiVersions::create_single(ApiKeys::PRODUCE.id(), 8, 10);
        assert_eq!(10, api_versions.latest_usable_version(&ApiKeys::PRODUCE).unwrap());
        assert_eq!(8, api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 7, 8).unwrap());
        assert_eq!(8, api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 8, 8).unwrap());
        assert_eq!(9, api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 8, 9).unwrap());
        assert_eq!(
            10,
            api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 8, 10).unwrap()
        );
        assert_eq!(9, api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 9, 9).unwrap());
        assert_eq!(
            10,
            api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 9, 10).unwrap()
        );
        assert_eq!(
            10,
            api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 10, 10).unwrap()
        );
        assert_eq!(
            10,
            api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 10, 11).unwrap()
        );
    }

    /// Translated from `NodeApiVersionsTest.testLatestUsableVersionOutOfRangeLow`
    #[test]
    fn test_latest_usable_version_out_of_range_low() {
        let api_versions = NodeApiVersions::create_single(ApiKeys::PRODUCE.id(), 1, 2);
        assert!(api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 3, 4).is_err());
    }

    /// Translated from `NodeApiVersionsTest.testLatestUsableVersionOutOfRangeHigh`
    #[test]
    fn test_latest_usable_version_out_of_range_high() {
        let api_versions = NodeApiVersions::create_single(ApiKeys::PRODUCE.id(), 2, 3);
        assert!(api_versions.latest_usable_version_in_range(&ApiKeys::PRODUCE, 0, 1).is_err());
    }

    /// Translated from `NodeApiVersionsTest.testUsableVersionCalculationNoKnownVersions`
    #[test]
    fn test_usable_version_calculation_no_known_versions() {
        let versions = NodeApiVersions::with_supported_features(&[], &[]);
        assert!(versions.latest_usable_version(&ApiKeys::FETCH).is_err());
    }

    /// Translated from `NodeApiVersionsTest.testLatestUsableVersionOutOfRange`
    #[test]
    fn test_latest_usable_version_out_of_range() {
        let api_versions = NodeApiVersions::create_single(ApiKeys::PRODUCE.id(), 300, 300);
        assert!(api_versions.latest_usable_version(&ApiKeys::PRODUCE).is_err());
    }

    /// Translated from `NodeApiVersionsTest.testUsableVersionLatestVersions`
    ///
    /// Parameterized in Java over `ApiMessageType.ListenerType`; expanded here.
    #[test]
    fn test_usable_version_latest_versions_broker() {
        test_usable_version_latest_versions(ListenerType::Broker);
    }

    #[test]
    fn test_usable_version_latest_versions_controller() {
        test_usable_version_latest_versions(ListenerType::Controller);
    }

    fn test_usable_version_latest_versions(scope: ListenerType) {
        let default_response = ApiVersionsResponse::default_api_versions_response(scope);
        let mut version_list: Vec<ApiVersion> = default_response.data().api_keys.clone();
        // Add an API key that we don't know about.
        let mut unknown = ApiVersion::new();
        unknown.set_api_key(100);
        unknown.set_min_version(0);
        unknown.set_max_version(1);
        version_list.push(unknown);
        let versions = NodeApiVersions::with_supported_features(&version_list, &[]);
        for api_key in ApiKeys::apis_for_listener(scope) {
            assert_eq!(
                api_key.latest_version(),
                versions.latest_usable_version(api_key).unwrap(),
                "Failed for {}",
                api_key.name()
            );
        }
    }

    /// Translated from `NodeApiVersionsTest.testConstructionFromApiVersionsResponse`
    ///
    /// Parameterized in Java over `ApiMessageType.ListenerType`; expanded here.
    #[test]
    fn test_construction_from_api_versions_response_broker() {
        test_construction_from_api_versions_response(ListenerType::Broker);
    }

    #[test]
    fn test_construction_from_api_versions_response_controller() {
        test_construction_from_api_versions_response(ListenerType::Controller);
    }

    fn test_construction_from_api_versions_response(scope: ListenerType) {
        let api_versions_response = ApiVersionsResponse::default_api_versions_response(scope);
        let versions = NodeApiVersions::with_supported_features(&api_versions_response.data().api_keys, &[]);

        for api_version_key in &api_versions_response.data().api_keys {
            let api_key = ApiKeys::for_id(api_version_key.api_key).unwrap();
            let api_version = versions.api_version(api_key).unwrap();
            assert_eq!(api_version_key.api_key, api_version.api_key);
            assert_eq!(api_version_key.min_version, api_version.min_version);
            assert_eq!(api_version_key.max_version, api_version.max_version);
        }
    }

    /// Translated from `NodeApiVersionsTest.testFeatures`
    #[test]
    fn test_features() {
        let mut supported_feature = SupportedFeatureKey::new();
        supported_feature.set_name("transaction.version".to_string());
        supported_feature.set_max_version(2);
        supported_feature.set_min_version(0);

        let mut finalized_feature = FinalizedFeatureKey::new();
        finalized_feature.set_name("transaction.version".to_string());
        finalized_feature.set_max_version_level(2);
        finalized_feature.set_min_version_level(2);

        let versions = NodeApiVersions::new(&[], &[supported_feature], &[finalized_feature], 0);

        let supported_version_range = versions.supported_features().get("transaction.version").unwrap();
        assert_eq!(0, supported_version_range.min());
        assert_eq!(2, supported_version_range.max());
        assert_eq!(&2_i16, versions.finalized_features().get("transaction.version").unwrap());
        assert_eq!(0, versions.finalized_features_epoch());
    }
}
