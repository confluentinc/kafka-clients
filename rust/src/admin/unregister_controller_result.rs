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

//! The result of `Admin::unregister_controller`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.UnregisterControllerResult`
//! (KAFKA-20395, Kafka 4.4).

use crate::common::KafkaFuture;

/// The result of the [`unregister_controller_with_options`](crate::admin::Admin::unregister_controller_with_options)
/// call.
///
/// The API of this class is evolving, see [`Admin`](crate::admin::Admin) for details.
///
/// Corresponds to `org.apache.kafka.clients.admin.UnregisterControllerResult`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.UnregisterControllerResult")]
pub struct UnregisterControllerResult {
    future: KafkaFuture<()>,
}

impl UnregisterControllerResult {
    /// Creates a result wrapping the given future. Package-private in Java.
    #[doc(alias = "org.apache.kafka.clients.admin.UnregisterControllerResult#UnregisterControllerResult")]
    pub(crate) fn new(future: KafkaFuture<()>) -> Self {
        Self { future }
    }

    /// Return a future which succeeds if the operation is successful.
    #[doc(alias = "org.apache.kafka.clients.admin.UnregisterControllerResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        self.future.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn all_resolves_with_the_wrapped_future() {
        let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let result = UnregisterControllerResult::new(handle.future());
        assert!(!result.all().is_done());
        handle.complete(());
        result.all().get().await.unwrap();
    }

    #[tokio::test]
    async fn all_fails_with_the_wrapped_error() {
        let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let result = UnregisterControllerResult::new(handle.future());
        handle.complete_with_error(Error::local_illegal_state("boom"));
        assert_eq!(result.all().get().await.unwrap_err().message(), "boom");
    }
}
