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

//! The result of `Admin::describe_features`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeFeaturesResult`.

use super::FeatureMetadata;
use crate::common::KafkaFuture;

/// The result of the [`describe_features`](crate::admin::Admin::describe_features)
/// call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeFeaturesResult`.
#[derive(Clone, Debug)]
pub struct DescribeFeaturesResult {
    future: KafkaFuture<FeatureMetadata>,
}

impl DescribeFeaturesResult {
    /// Creates a result wrapping the given future.
    pub(crate) fn new(future: KafkaFuture<FeatureMetadata>) -> Self {
        Self { future }
    }

    /// The future which completes with the cluster's feature metadata.
    ///
    /// Mirrors `featureMetadata()`.
    pub fn feature_metadata(&self) -> KafkaFuture<FeatureMetadata> {
        self.future.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;
    use std::collections::HashMap;

    #[tokio::test]
    async fn feature_metadata_resolves() {
        let handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();
        let result = DescribeFeaturesResult::new(handle.future());
        let metadata = FeatureMetadata::new(HashMap::new(), Some(1), HashMap::new());
        handle.complete(metadata.clone());
        assert_eq!(result.feature_metadata().get().await.unwrap(), metadata);
    }
}
