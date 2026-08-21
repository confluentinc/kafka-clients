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

//! The result of `Admin::describe_transactions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeTransactionsResult`.

use std::collections::HashMap;

use crate::admin::transaction_description::TransactionDescription;
use crate::common::{Error, KafkaFuture};

/// The result of `Admin::describe_transactions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeTransactionsResult`.
/// Java keys the per-transaction futures by `CoordinatorKey`; since
/// `CoordinatorKey` is an internal (`pub(crate)`) type, the Rust result keys
/// them by the transactional id string (the `idValue`), following the
/// `DescribeConsumerGroupsResult` precedent.
#[derive(Clone, Debug)]
pub struct DescribeTransactionsResult {
    futures: HashMap<String, KafkaFuture<TransactionDescription>>,
}

impl DescribeTransactionsResult {
    /// Creates a result from the per-transactional-id futures.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<TransactionDescription>>) -> Self {
        Self { futures }
    }

    /// Get the description of a specific transactional id.
    ///
    /// Mirrors `DescribeTransactionsResult.description`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] if `transactional_id` was not
    /// included in the request (mirroring Java's `IllegalArgumentException`).
    pub fn description(&self, transactional_id: &str) -> Result<KafkaFuture<TransactionDescription>, Error> {
        self.futures.get(transactional_id).cloned().ok_or_else(|| {
            Error::local_illegal_argument(format!(
                "TransactionalId `{transactional_id}` was not included in the request"
            ))
        })
    }

    /// Get a future yielding a map of every requested transactional id's
    /// description. Fails if any transactional id's request fails.
    ///
    /// Mirrors `DescribeTransactionsResult.all`.
    pub fn all(&self) -> KafkaFuture<HashMap<String, TransactionDescription>> {
        KafkaFuture::join_map(self.futures.iter().map(|(id, f)| (id.clone(), f.clone())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::transaction_state::TransactionState;
    use crate::common::kafka_future::KafkaFutureImpl;
    use std::collections::HashSet;

    fn description() -> TransactionDescription {
        TransactionDescription::new(1, TransactionState::Ongoing, 1, 2, 3, None, HashSet::new())
    }

    #[tokio::test]
    async fn description_returns_future_for_known_id() {
        let h: KafkaFutureImpl<TransactionDescription> = KafkaFutureImpl::new();
        let result = DescribeTransactionsResult::new(HashMap::from([("t".to_string(), h.future())]));
        h.complete(description());
        assert_eq!(result.description("t").unwrap().get().await.unwrap(), description());
    }

    #[test]
    fn description_errors_for_unknown_id() {
        let result = DescribeTransactionsResult::new(HashMap::new());
        let err = result.description("t").unwrap_err();
        assert!(err.message().contains("was not included in the request"));
    }

    #[tokio::test]
    async fn all_collects_every_id() {
        let h: KafkaFutureImpl<TransactionDescription> = KafkaFutureImpl::new();
        let result = DescribeTransactionsResult::new(HashMap::from([("t".to_string(), h.future())]));
        h.complete(description());
        let all = result.all().get().await.unwrap();
        assert_eq!(all["t"], description());
    }
}
