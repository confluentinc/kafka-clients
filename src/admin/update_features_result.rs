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

//! The result of `Admin::update_features`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.UpdateFeaturesResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

/// The result of the [`update_features_options`](crate::admin::Admin::update_features_options)
/// call.
///
/// Corresponds to `org.apache.kafka.clients.admin.UpdateFeaturesResult`.
#[derive(Clone, Debug)]
pub struct UpdateFeaturesResult {
    futures: HashMap<String, KafkaFuture<()>>,
}

impl UpdateFeaturesResult {
    /// Creates a result from the per-feature futures.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Return a map from feature name to future, which can be used to check the
    /// status of individual feature updates.
    ///
    /// Mirrors `values()`.
    pub fn values(&self) -> &HashMap<String, KafkaFuture<()>> {
        &self.futures
    }

    /// Return a future which succeeds if all the feature updates succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::kafka_future::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    #[tokio::test]
    async fn all_succeeds_when_each_completes() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("f1".to_string(), h.future());
        let result = UpdateFeaturesResult::new(map);
        h.complete(());
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    async fn all_fails_if_one_fails() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("f1".to_string(), h.future());
        let result = UpdateFeaturesResult::new(map);
        h.complete_with_error(Error::new(Errors::InvalidRequest));
        assert!(result.all().get().await.is_err());
    }
}
