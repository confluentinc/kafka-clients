// Copyright 2026 Confluent Inc.
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

//! Internal result class for `describeFeatures` that also exposes the node's
//! API versions (KAFKA-19663).
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult`.

use crate::NodeApiVersions;
use crate::admin::{DescribeFeaturesResult, FeatureMetadata};
use crate::common::KafkaFuture;

/// Internal result class for `describeFeatures` that exposes API version
/// information. This class is intended for use by internal Kafka tools that
/// need access to the raw API versions returned in the `ApiVersionsResponse`.
///
/// Java's class `extends DescribeFeaturesResult` (whose constructor 4.4 made
/// `protected` for exactly this subclass). Rust has no inheritance, so the base
/// is held by value and converted back with [`From`]: `KafkaAdminClient`
/// builds this type and its `Admin::describe_features_with_options` returns
/// the base, as Java returns the subclass typed as its parent. Java's tools
/// recover the subclass with a cast; inside the crate the caller asks
/// `KafkaAdminClient::describe_features_internal` for it directly.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult")]
pub(crate) struct InternalDescribeFeaturesResult {
    base: DescribeFeaturesResult,
    node_api_versions: KafkaFuture<NodeApiVersions>,
}

impl InternalDescribeFeaturesResult {
    /// Creates the result from the feature-metadata and node-API-versions
    /// futures.
    #[doc(
        alias = "org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult#InternalDescribeFeaturesResult"
    )]
    pub(crate) fn new(
        feature_metadata: KafkaFuture<FeatureMetadata>,
        node_api_versions: KafkaFuture<NodeApiVersions>,
    ) -> Self {
        Self { base: DescribeFeaturesResult::new(feature_metadata), node_api_versions }
    }

    /// The inherited `DescribeFeaturesResult.featureMetadata()`.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn feature_metadata(&self) -> KafkaFuture<FeatureMetadata> {
        self.base.feature_metadata()
    }

    /// Returns the node API versions future. This contains the API keys and
    /// version ranges supported by the broker, as returned in the
    /// `ApiVersionsResponse`.
    // Java's only callers are the `kafka-cluster` tools, which this client does
    // not ship; the crate reads it in tests.
    #[cfg_attr(not(test), expect(dead_code))]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult#nodeApiVersions")]
    pub(crate) fn node_api_versions(&self) -> KafkaFuture<NodeApiVersions> {
        self.node_api_versions.clone()
    }
}

impl From<InternalDescribeFeaturesResult> for DescribeFeaturesResult {
    /// The upcast Java performs implicitly when `describeFeatures` returns the
    /// subclass as a `DescribeFeaturesResult`.
    fn from(result: InternalDescribeFeaturesResult) -> Self {
        result.base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::ApiKeys;
    use std::collections::HashMap;

    #[tokio::test]
    async fn both_futures_resolve_and_the_upcast_keeps_the_feature_metadata() {
        let metadata_handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();
        let versions_handle: KafkaFutureImpl<NodeApiVersions> = KafkaFutureImpl::new();
        let result = InternalDescribeFeaturesResult::new(metadata_handle.future(), versions_handle.future());
        let metadata = FeatureMetadata::new(HashMap::new(), Some(3), HashMap::new());
        metadata_handle.complete(metadata.clone());
        versions_handle.complete(NodeApiVersions::create());

        assert_eq!(result.feature_metadata().get().await.unwrap(), metadata);
        let versions = result.node_api_versions().get().await.unwrap();
        assert!(versions.api_version(&ApiKeys::API_VERSIONS).is_some());

        let base: DescribeFeaturesResult = result.into();
        assert_eq!(base.feature_metadata().get().await.unwrap(), metadata);
    }

    #[tokio::test]
    async fn node_api_versions_carries_its_own_error() {
        let metadata_handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();
        let versions_handle: KafkaFutureImpl<NodeApiVersions> = KafkaFutureImpl::new();
        let result = InternalDescribeFeaturesResult::new(metadata_handle.future(), versions_handle.future());
        versions_handle.complete_with_error(Error::local_illegal_state("boom"));
        assert!(!result.feature_metadata().is_done());
        assert_eq!(result.node_api_versions().get().await.unwrap_err().message(), "boom");
    }
}
