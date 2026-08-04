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

//! The result of `Admin::force_terminate_transaction`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TerminateTransactionResult`.

use crate::common::KafkaFuture;

/// The result of `Admin::force_terminate_transaction`.
///
/// Corresponds to `org.apache.kafka.clients.admin.TerminateTransactionResult`.
#[derive(Clone, Debug)]
pub struct TerminateTransactionResult {
    future: KafkaFuture<()>,
}

impl TerminateTransactionResult {
    /// Creates a result wrapping the terminating future.
    pub(crate) fn new(future: KafkaFuture<()>) -> Self {
        Self { future }
    }

    /// Return a future which indicates whether the transaction was successfully
    /// terminated.
    ///
    /// Mirrors `TerminateTransactionResult.result`.
    pub fn result(&self) -> KafkaFuture<()> {
        self.future.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

    #[tokio::test]
    async fn result_returns_the_wrapped_future() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let result = TerminateTransactionResult::new(h.future());
        h.complete(());
        result.result().get().await.unwrap();
    }
}
