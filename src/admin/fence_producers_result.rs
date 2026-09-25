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

//! The result of `Admin::fence_producers`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.FenceProducersResult`.

use std::collections::HashMap;

use crate::common::utils::ProducerIdAndEpoch;
use crate::common::{Error, KafkaFuture};

/// The result of `Admin::fence_producers`.
///
/// Corresponds to `org.apache.kafka.clients.admin.FenceProducersResult`. Java
/// keys the per-producer futures by `CoordinatorKey`; since `CoordinatorKey` is
/// an internal (`pub(crate)`) type, the Rust result keys them by the
/// transactional id string, following the `DescribeConsumerGroupsResult`
/// precedent.
#[derive(Clone, Debug)]
pub struct FenceProducersResult {
    futures: HashMap<String, KafkaFuture<ProducerIdAndEpoch>>,
}

impl FenceProducersResult {
    /// Creates a result from the per-transactional-id futures.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<ProducerIdAndEpoch>>) -> Self {
        Self { futures }
    }

    /// Returns the underlying per-transactional-id futures.
    ///
    /// Crate-internal, for the C FFI's per-key (unjoined) delivery. Java exposes
    /// only the `producerId(id)` / `epochId(id)` / `fencedProducers()`
    /// projections publicly, each a `thenApply` view over this same map; the FFI
    /// needs the whole `ProducerIdAndEpoch` value per key — exactly the field
    /// those projections are built from — so it reads the map directly rather
    /// than joining two scalar projections back together.
    #[cfg_attr(not(feature = "ffi"), allow(dead_code))]
    pub(crate) fn futures(&self) -> &HashMap<String, KafkaFuture<ProducerIdAndEpoch>> {
        &self.futures
    }

    /// Return a map from transactional id to futures which can be used to check
    /// the status of individual fencings.
    ///
    /// Mirrors `FenceProducersResult.fencedProducers`.
    pub fn fenced_producers(&self) -> HashMap<String, KafkaFuture<()>> {
        self.futures.iter().map(|(id, f)| (id.clone(), f.then_apply(|_| ()))).collect()
    }

    /// Returns a future that provides the producer id generated while
    /// initializing the given transaction when the request completes.
    ///
    /// Mirrors `FenceProducersResult.producerId`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] if `transactional_id` was not
    /// included in the request.
    pub fn producer_id(&self, transactional_id: &str) -> Result<KafkaFuture<i64>, Error> {
        self.find_and_apply(transactional_id, |p| p.producer_id)
    }

    /// Returns a future that provides the epoch generated while initializing the
    /// given transaction when the request completes.
    ///
    /// Mirrors `FenceProducersResult.epochId`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] if `transactional_id` was not
    /// included in the request.
    pub fn epoch_id(&self, transactional_id: &str) -> Result<KafkaFuture<i16>, Error> {
        self.find_and_apply(transactional_id, |p| p.epoch)
    }

    /// Return a future which succeeds only if all the producer fencings succeed.
    ///
    /// Mirrors `FenceProducersResult.all`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }

    /// Mirrors `FenceProducersResult.findAndApply`.
    fn find_and_apply<T, F>(&self, transactional_id: &str, followup: F) -> Result<KafkaFuture<T>, Error>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&ProducerIdAndEpoch) -> T + Send + Sync + 'static,
    {
        self.futures
            .get(transactional_id)
            .map(|future| future.then_apply(move |p| followup(&p)))
            .ok_or_else(|| {
                Error::local_illegal_argument(format!(
                    "TransactionalId `{transactional_id}` was not included in the request"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn producer_id_and_epoch_id_project_the_future() {
        let h: KafkaFutureImpl<ProducerIdAndEpoch> = KafkaFutureImpl::new();
        let result = FenceProducersResult::new(HashMap::from([("t".to_string(), h.future())]));
        h.complete(ProducerIdAndEpoch::new(7, 57));
        assert_eq!(result.producer_id("t").unwrap().get().await.unwrap(), 7);
        assert_eq!(result.epoch_id("t").unwrap().get().await.unwrap(), 57);
        assert_eq!(result.fenced_producers()["t"].get().await.unwrap(), ());
        result.all().get().await.unwrap();
    }

    #[test]
    fn projections_error_for_unknown_id() {
        let result = FenceProducersResult::new(HashMap::new());
        assert!(
            result
                .producer_id("t")
                .unwrap_err()
                .message()
                .contains("was not included in the request")
        );
        assert!(
            result
                .epoch_id("t")
                .unwrap_err()
                .message()
                .contains("was not included in the request")
        );
    }
}
