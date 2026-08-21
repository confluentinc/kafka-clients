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

//! The result of `Admin::list_transactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListTransactionsResult`.

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::admin::transaction_listing::TransactionListing;
use crate::common::Error;
use crate::common::kafka_future::{KafkaFuture, KafkaFutureOps};

/// The (top-level) future value: a map from broker id to that broker's listing
/// future, produced once the broker list is discovered.
type BrokerFutures = HashMap<i32, KafkaFuture<Vec<TransactionListing>>>;

/// The result of `Admin::list_transactions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListTransactionsResult`. The
/// wrapped future completes when the broker list is discovered, yielding a map
/// from broker id to that broker's per-request future (mirroring Java's
/// `KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>>`).
#[derive(Clone, Debug)]
pub struct ListTransactionsResult {
    future: KafkaFuture<BrokerFutures>,
}

impl ListTransactionsResult {
    /// Creates a result wrapping the top-level broker-discovery future.
    pub(crate) fn new(future: KafkaFuture<BrokerFutures>) -> Self {
        Self { future }
    }

    /// Get all transaction listings. If any of the underlying requests fail,
    /// the returned future also fails with the first encountered error.
    ///
    /// Mirrors `ListTransactionsResult.all`.
    pub fn all(&self) -> KafkaFuture<Vec<TransactionListing>> {
        self.all_by_broker_id()
            .then_apply(|map: HashMap<i32, Vec<TransactionListing>>| {
                map.into_values().flatten().collect::<Vec<TransactionListing>>()
            })
    }

    /// Get a future returning the per-broker listing futures. Useful for a
    /// partial listing or more granular error details.
    ///
    /// Mirrors `ListTransactionsResult.byBrokerId`.
    pub fn by_broker_id(&self) -> KafkaFuture<BrokerFutures> {
        self.future.clone()
    }

    /// Get all transaction listings keyed by the broker id currently managing
    /// them. If any underlying request fails, the returned future also fails.
    ///
    /// Mirrors `ListTransactionsResult.allByBrokerId`.
    pub fn all_by_broker_id(&self) -> KafkaFuture<HashMap<i32, Vec<TransactionListing>>> {
        KafkaFuture::new(std::sync::Arc::new(AllByBrokerIdFuture { source: self.future.clone() }))
    }
}

/// Polls `future` exactly once with a no-op waker, returning its output if it is
/// immediately ready. Used to synchronously observe an already-completed
/// [`KafkaFuture`] for [`is_done`](KafkaFutureOps::is_done).
fn poll_once<F: std::future::Future>(future: F) -> Option<F::Output> {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

/// A [`KafkaFutureOps`] that flattens the top-level broker-discovery future and
/// each per-broker future into a single `broker id -> listings` map, failing on
/// the first error (mirrors `ListTransactionsResult.allByBrokerId`).
struct AllByBrokerIdFuture {
    source: KafkaFuture<BrokerFutures>,
}

impl KafkaFutureOps<HashMap<i32, Vec<TransactionListing>>> for AllByBrokerIdFuture {
    fn get(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<HashMap<i32, Vec<TransactionListing>>, Error>> + Send + '_>>
    {
        Box::pin(async move {
            let broker_futures = self.source.get().await?;
            let mut result = HashMap::with_capacity(broker_futures.len());
            for (broker_id, future) in broker_futures {
                result.insert(broker_id, future.get().await?);
            }
            Ok(result)
        })
    }

    fn get_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<HashMap<i32, Vec<TransactionListing>>, Error>> + Send + '_>>
    {
        Box::pin(async move {
            match tokio::time::timeout(timeout, self.get()).await {
                Ok(result) => result,
                Err(_) => Err(Error::concurrent_timeout(format!(
                    "Timed out waiting for KafkaFuture after {} ms",
                    timeout.as_millis()
                ))),
            }
        })
    }

    fn is_done(&self) -> bool {
        if !self.source.is_done() {
            return false;
        }
        // The source is done, so its `get()` resolves in a single poll.
        match poll_once(self.source.get()) {
            Some(Ok(broker_futures)) => broker_futures.values().all(KafkaFuture::is_done),
            Some(Err(_)) => true,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::transaction_state::TransactionState;
    use crate::common::Error;
    use crate::common::kafka_future::KafkaFutureImpl;
    use std::collections::HashSet;

    fn listing(id: &str, producer_id: i64, state: TransactionState) -> TransactionListing {
        TransactionListing::new(id, producer_id, state)
    }

    // Mirrors `ListTransactionsResultTest.testAllFuturesFailIfLookupFails`.
    #[tokio::test]
    async fn all_futures_fail_if_lookup_fails() {
        let top: KafkaFutureImpl<BrokerFutures> = KafkaFutureImpl::new();
        let result = ListTransactionsResult::new(top.future());
        top.complete_with_error(Error::new(crate::common::protocol::Errors::UnknownServerError));
        assert!(result.all().get().await.is_err());
        assert!(result.all_by_broker_id().get().await.is_err());
        assert!(result.by_broker_id().get().await.is_err());
    }

    // Mirrors `ListTransactionsResultTest.testAllFuturesSucceed`.
    #[tokio::test]
    async fn all_futures_succeed() {
        let top: KafkaFutureImpl<BrokerFutures> = KafkaFutureImpl::new();
        let result = ListTransactionsResult::new(top.future());

        let f1: KafkaFutureImpl<Vec<TransactionListing>> = KafkaFutureImpl::new();
        let f2: KafkaFutureImpl<Vec<TransactionListing>> = KafkaFutureImpl::new();
        top.complete(HashMap::from([(1, f1.future()), (2, f2.future())]));

        let broker1 = vec![
            listing("foo", 12345, TransactionState::Ongoing),
            listing("bar", 98765, TransactionState::PrepareAbort),
        ];
        f1.complete(broker1.clone());
        let broker2 = vec![listing("baz", 13579, TransactionState::CompleteCommit)];
        f2.complete(broker2.clone());

        let by_broker = result.by_broker_id().get().await.unwrap();
        assert_eq!(by_broker.keys().copied().collect::<HashSet<_>>(), HashSet::from([1, 2]));
        assert_eq!(by_broker[&1].get().await.unwrap(), broker1);
        assert_eq!(by_broker[&2].get().await.unwrap(), broker2);

        let all_by_broker = result.all_by_broker_id().get().await.unwrap();
        assert_eq!(all_by_broker[&1], broker1);
        assert_eq!(all_by_broker[&2], broker2);

        let mut all_expected: HashSet<TransactionListing> = HashSet::new();
        all_expected.extend(broker1);
        all_expected.extend(broker2);
        assert_eq!(
            result.all().get().await.unwrap().into_iter().collect::<HashSet<_>>(),
            all_expected
        );
    }

    // Mirrors `ListTransactionsResultTest.testPartialFailure`.
    #[tokio::test]
    async fn partial_failure() {
        let top: KafkaFutureImpl<BrokerFutures> = KafkaFutureImpl::new();
        let result = ListTransactionsResult::new(top.future());

        let f1: KafkaFutureImpl<Vec<TransactionListing>> = KafkaFutureImpl::new();
        let f2: KafkaFutureImpl<Vec<TransactionListing>> = KafkaFutureImpl::new();
        top.complete(HashMap::from([(1, f1.future()), (2, f2.future())]));

        let broker1 = vec![listing("foo", 12345, TransactionState::Ongoing)];
        f1.complete(broker1.clone());
        f2.complete_with_error(Error::new(crate::common::protocol::Errors::UnknownServerError));

        let by_broker = result.by_broker_id().get().await.unwrap();
        assert_eq!(by_broker.keys().copied().collect::<HashSet<_>>(), HashSet::from([1, 2]));
        assert_eq!(by_broker[&1].get().await.unwrap(), broker1);

        assert!(result.all().get().await.is_err());
        assert!(result.all_by_broker_id().get().await.is_err());
        assert!(by_broker[&2].get().await.is_err());
    }
}
