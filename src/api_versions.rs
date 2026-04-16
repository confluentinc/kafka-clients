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

//! Maintains node api versions for access outside of NetworkClient
//! (which is where the information is derived).
//! The pattern is akin to the use of Metadata for topic metadata.
//!
//! NOTE: This class is intended for INTERNAL usage only within Kafka.
//!
//! Translated from `org.apache.kafka.clients.ApiVersions`.

use std::collections::HashMap;
use std::sync::RwLock;

use super::NodeApiVersions;

/// Information about finalized features and their epoch.
#[derive(Debug, Clone)]
pub struct FinalizedFeaturesInfo {
    /// The epoch of the finalized features.
    pub finalized_features_epoch: i64,
    /// Map of finalized feature name to its version.
    pub finalized_features: Option<HashMap<String, i16>>,
}

impl FinalizedFeaturesInfo {
    /// Creates a new `FinalizedFeaturesInfo`.
    fn new(finalized_features_epoch: i64, finalized_features: Option<HashMap<String, i16>>) -> Self {
        Self { finalized_features_epoch, finalized_features }
    }
}

/// Maintains node API versions for access outside of NetworkClient.
///
/// Thread-safe: all access is synchronized via `RwLock`.
///
/// Translated from `org.apache.kafka.clients.ApiVersions`.
#[derive(Debug)]
pub struct ApiVersions {
    inner: RwLock<ApiVersionsInner>,
}

#[derive(Debug)]
struct ApiVersionsInner {
    node_api_versions: HashMap<String, NodeApiVersions>,
    /// The maximum finalized feature epoch of all the node api versions.
    max_finalized_features_epoch: i64,
    finalized_features: Option<HashMap<String, i16>>,
}

impl ApiVersions {
    /// Creates a new `ApiVersions` instance.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(ApiVersionsInner {
                node_api_versions: HashMap::new(),
                max_finalized_features_epoch: -1,
                finalized_features: None,
            }),
        }
    }

    /// Updates the API versions for a given node.
    ///
    /// If the node's finalized features epoch is higher than the current maximum,
    /// the finalized features are updated.
    pub fn update(&self, node_id: &str, node_api_versions: NodeApiVersions) {
        let mut inner = self.inner.write().unwrap();
        if inner.max_finalized_features_epoch < node_api_versions.finalized_features_epoch() {
            inner.max_finalized_features_epoch = node_api_versions.finalized_features_epoch();
            inner.finalized_features = Some(node_api_versions.finalized_features().clone());
        }
        inner.node_api_versions.insert(node_id.to_string(), node_api_versions);
    }

    /// Removes the API versions for a given node.
    pub fn remove(&self, node_id: &str) {
        let mut inner = self.inner.write().unwrap();
        inner.node_api_versions.remove(node_id);
    }

    /// Gets the API versions for a given node.
    ///
    /// Returns `None` if the node is not known.
    pub fn get(&self, node_id: &str) -> Option<NodeApiVersions> {
        let inner = self.inner.read().unwrap();
        inner.node_api_versions.get(node_id).cloned()
    }

    /// Returns the maximum finalized features epoch.
    pub fn max_finalized_features_epoch(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        inner.max_finalized_features_epoch
    }

    /// Returns the finalized features info containing the epoch and features map.
    pub fn finalized_features_info(&self) -> FinalizedFeaturesInfo {
        let inner = self.inner.read().unwrap();
        FinalizedFeaturesInfo::new(inner.max_finalized_features_epoch, inner.finalized_features.clone())
    }
}

impl Default for ApiVersions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeApiVersions;
    use crate::api_versions_response_data::{FinalizedFeatureKey, SupportedFeatureKey};

    /// Translated from `ApiVersionsTest.testFinalizedFeaturesUpdate`
    #[test]
    fn test_finalized_features_update() {
        let api_versions = ApiVersions::new();
        assert_eq!(-1, api_versions.max_finalized_features_epoch());

        let default_versions: Vec<_> = NodeApiVersions::create()
            .all_supported_api_versions()
            .values()
            .cloned()
            .collect();

        let mut supported_feature = SupportedFeatureKey::new();
        supported_feature.set_name("transaction.version".to_string());
        supported_feature.set_max_version(2);
        supported_feature.set_min_version(0);

        let mut finalized_feature = FinalizedFeatureKey::new();
        finalized_feature.set_name("transaction.version".to_string());
        finalized_feature.set_max_version_level(2);
        finalized_feature.set_min_version_level(2);

        api_versions.update(
            "2",
            NodeApiVersions::new(&default_versions, &[supported_feature.clone()], &[finalized_feature], 1),
        );

        let info = api_versions.finalized_features_info();
        assert_eq!(1, info.finalized_features_epoch);
        assert_eq!(
            &2_i16,
            info.finalized_features.as_ref().unwrap().get("transaction.version").unwrap()
        );

        let mut finalized_feature_stale = FinalizedFeatureKey::new();
        finalized_feature_stale.set_name("transaction.version".to_string());
        finalized_feature_stale.set_max_version_level(1);
        finalized_feature_stale.set_min_version_level(1);

        api_versions.update(
            "1",
            NodeApiVersions::new(&default_versions, &[supported_feature], &[finalized_feature_stale], 0),
        );

        // The stale update should be fenced.
        let info = api_versions.finalized_features_info();
        assert_eq!(1, info.finalized_features_epoch);
        assert_eq!(
            &2_i16,
            info.finalized_features.as_ref().unwrap().get("transaction.version").unwrap()
        );
    }
}
