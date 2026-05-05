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

//! Translation of `org.apache.kafka.clients.ApiVersions`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.
//!
//! The Java class is annotated `synchronized` on every method. The Rust
//! translation guards the inner state with a single `Mutex` and never
//! holds the guard across an `.await` (CLAUDE.md rule 9.6 — `ApiVersions`
//! is currently called from synchronous code, but the rule still applies
//! once Phase 5d wires it into the network loop).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::NodeApiVersions;

/// Snapshot returned by [`ApiVersions::finalized_features_info`].
#[derive(Debug, Clone)]
pub struct FinalizedFeaturesInfo {
    /// The maximum finalized feature epoch observed across all known
    /// nodes. Mirrors `FinalizedFeaturesInfo.finalizedFeaturesEpoch`.
    pub finalized_features_epoch: i64,
    /// The finalized feature versions captured at the same epoch. May
    /// be empty if no node has reported any. Mirrors
    /// `FinalizedFeaturesInfo.finalizedFeatures`.
    pub finalized_features: HashMap<String, i16>,
}

#[derive(Default)]
struct ApiVersionsInner {
    node_api_versions: HashMap<String, Arc<NodeApiVersions>>,
    /// The maximum finalized feature epoch of all the node api
    /// versions. Mirrors Java's `private long maxFinalizedFeaturesEpoch = -1;`.
    max_finalized_features_epoch: i64,
    /// The finalized features captured alongside
    /// [`Self::max_finalized_features_epoch`]. Java's `null` collapses
    /// onto an empty `HashMap` here (the Java class never reads the
    /// field as null without first checking the epoch).
    finalized_features: HashMap<String, i16>,
}

/// Maintains node api versions for access outside of `NetworkClient`
/// (which is where the information is derived). The pattern is akin to
/// the use of `Metadata` for topic metadata.
///
/// **Note**: This class is intended for internal usage only within
/// Kafka. Mirrors the Java `ApiVersions`.
///
/// Thread-safe: every public method takes `&self` and uses an internal
/// `Mutex` for synchronisation, mirroring Java's `synchronized` methods.
pub struct ApiVersions {
    inner: Mutex<ApiVersionsInner>,
}

impl ApiVersions {
    /// Mirrors `new ApiVersions()`.
    pub fn new() -> Self {
        ApiVersions {
            inner: Mutex::new(ApiVersionsInner { max_finalized_features_epoch: -1, ..Default::default() }),
        }
    }

    /// Mirrors `ApiVersions.update(String, NodeApiVersions)`.
    ///
    /// Java passes the `NodeApiVersions` by value; we accept an
    /// `Arc<NodeApiVersions>` so the cache can hand out cheap clones via
    /// [`Self::get`]. Pre-existing callers can wrap a fresh
    /// `NodeApiVersions` with `Arc::new` at the call site.
    pub fn update(&self, node_id: &str, node_api_versions: Arc<NodeApiVersions>) {
        let mut inner = self.inner.lock().expect("ApiVersions inner not poisoned");
        if inner.max_finalized_features_epoch < node_api_versions.finalized_features_epoch() {
            inner.max_finalized_features_epoch = node_api_versions.finalized_features_epoch();
            inner.finalized_features = node_api_versions.finalized_features().clone();
        }
        inner.node_api_versions.insert(node_id.to_owned(), node_api_versions);
    }

    /// Mirrors `ApiVersions.remove(String)`.
    pub fn remove(&self, node_id: &str) {
        let mut inner = self.inner.lock().expect("ApiVersions inner not poisoned");
        inner.node_api_versions.remove(node_id);
    }

    /// Mirrors `ApiVersions.get(String)`. Returns `None` if no entry is
    /// known for the given node id.
    pub fn get(&self, node_id: &str) -> Option<Arc<NodeApiVersions>> {
        let inner = self.inner.lock().expect("ApiVersions inner not poisoned");
        inner.node_api_versions.get(node_id).cloned()
    }

    /// Mirrors `ApiVersions.getMaxFinalizedFeaturesEpoch()`.
    pub fn max_finalized_features_epoch(&self) -> i64 {
        let inner = self.inner.lock().expect("ApiVersions inner not poisoned");
        inner.max_finalized_features_epoch
    }

    /// Mirrors `ApiVersions.getFinalizedFeaturesInfo()`.
    pub fn finalized_features_info(&self) -> FinalizedFeaturesInfo {
        let inner = self.inner.lock().expect("ApiVersions inner not poisoned");
        FinalizedFeaturesInfo {
            finalized_features_epoch: inner.max_finalized_features_epoch,
            finalized_features: inner.finalized_features.clone(),
        }
    }
}

impl Default for ApiVersions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ApiVersionsTest`.

    use super::*;
    use crate::common::message::api_versions_response_data::{FinalizedFeatureKey, SupportedFeatureKey};

    /// Java: `testFinalizedFeaturesUpdate`.
    #[test]
    fn finalized_features_update() {
        let api_versions = ApiVersions::new();
        assert_eq!(api_versions.max_finalized_features_epoch(), -1);

        // First update at epoch 1
        let default_versions: Vec<_> = NodeApiVersions::create_default()
            .all_supported_api_versions()
            .values()
            .cloned()
            .collect();
        let node_2 = Arc::new(
            NodeApiVersions::with_features(
                default_versions.clone(),
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
                1,
            )
            .expect("ctor"),
        );
        api_versions.update("2", node_2);
        let info = api_versions.finalized_features_info();
        assert_eq!(info.finalized_features_epoch, 1);
        assert_eq!(*info.finalized_features.get("transaction.version").expect("present"), 2);

        // Second update at the older epoch 0 must NOT overwrite the
        // newer state — Java's "stale update should be fenced" check.
        let node_1 = Arc::new(
            NodeApiVersions::with_features(
                default_versions,
                vec![SupportedFeatureKey {
                    name: "transaction.version".into(),
                    min_version: 0,
                    max_version: 2,
                    unknown_tagged_fields: Vec::new(),
                }],
                vec![FinalizedFeatureKey {
                    name: "transaction.version".into(),
                    max_version_level: 1,
                    min_version_level: 1,
                    unknown_tagged_fields: Vec::new(),
                }],
                0,
            )
            .expect("ctor"),
        );
        api_versions.update("1", node_1);
        let info = api_versions.finalized_features_info();
        assert_eq!(info.finalized_features_epoch, 1);
        assert_eq!(*info.finalized_features.get("transaction.version").expect("present"), 2);
    }
}
