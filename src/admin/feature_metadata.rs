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

//! Details about finalized as well as supported features.
//!
//! Corresponds to `org.apache.kafka.clients.admin.FeatureMetadata`.

use std::collections::HashMap;

use super::{FinalizedVersionRange, SupportedVersionRange};

/// Encapsulates details about finalized as well as supported features.
///
/// This is particularly useful to hold the result returned by the
/// [`describe_features_with_options`](crate::admin::Admin::describe_features_with_options) API.
///
/// Corresponds to `org.apache.kafka.clients.admin.FeatureMetadata`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureMetadata {
    finalized_features: HashMap<String, FinalizedVersionRange>,
    finalized_features_epoch: Option<i64>,
    supported_features: HashMap<String, SupportedVersionRange>,
}

impl FeatureMetadata {
    /// Creates a new `FeatureMetadata`.
    pub(crate) fn new(
        finalized_features: HashMap<String, FinalizedVersionRange>,
        finalized_features_epoch: Option<i64>,
        supported_features: HashMap<String, SupportedVersionRange>,
    ) -> Self {
        Self { finalized_features, finalized_features_epoch, supported_features }
    }

    /// Returns a map of finalized feature versions. Each entry contains a
    /// feature name and the range of version levels supported by every broker
    /// in the cluster.
    pub fn finalized_features(&self) -> &HashMap<String, FinalizedVersionRange> {
        &self.finalized_features
    }

    /// The epoch for the finalized features. If empty, the finalized features
    /// are absent/unavailable.
    pub fn finalized_features_epoch(&self) -> Option<i64> {
        self.finalized_features_epoch
    }

    /// Returns a map of supported feature versions. Each entry contains a
    /// feature name and the range of versions supported by a particular broker
    /// in the cluster.
    pub fn supported_features(&self) -> &HashMap<String, SupportedVersionRange> {
        &self.supported_features
    }
}

impl std::fmt::Display for FeatureMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let epoch = match self.finalized_features_epoch {
            Some(e) => e.to_string(),
            None => "<none>".to_string(),
        };
        write!(
            f,
            "FeatureMetadata{{finalizedFeatures:{}, finalizedFeaturesEpoch:{}, supportedFeatures:{}}}",
            map_to_string(&self.finalized_features),
            epoch,
            map_to_string(&self.supported_features)
        )
    }
}

/// Formats a feature map as `{(name -> range), ...}`, mirroring Java's
/// `FeatureMetadata.mapToString`.
fn map_to_string<V: std::fmt::Display>(map: &HashMap<String, V>) -> String {
    let entries: Vec<String> = map.iter().map(|(k, v)| format!("({k} -> {v})")).collect();
    format!("{{{}}}", entries.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_is_order_insensitive() {
        let mut finalized_a = HashMap::new();
        finalized_a.insert("f1".to_string(), FinalizedVersionRange::new(1, 2).unwrap());
        finalized_a.insert("f2".to_string(), FinalizedVersionRange::new(2, 3).unwrap());
        let mut finalized_b = HashMap::new();
        finalized_b.insert("f2".to_string(), FinalizedVersionRange::new(2, 3).unwrap());
        finalized_b.insert("f1".to_string(), FinalizedVersionRange::new(1, 2).unwrap());

        let a = FeatureMetadata::new(finalized_a, Some(1), HashMap::new());
        let b = FeatureMetadata::new(finalized_b, Some(1), HashMap::new());
        assert_eq!(a, b);
    }

    #[test]
    fn accessors_return_inner_state() {
        let mut supported = HashMap::new();
        supported.insert("f1".to_string(), SupportedVersionRange::new(1, 5).unwrap());
        let metadata = FeatureMetadata::new(HashMap::new(), None, supported);
        assert!(metadata.finalized_features().is_empty());
        assert_eq!(metadata.finalized_features_epoch(), None);
        assert_eq!(
            metadata.supported_features().get("f1"),
            Some(&SupportedVersionRange::new(1, 5).unwrap())
        );
    }
}
