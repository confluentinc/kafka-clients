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

//! The result of `Admin::alter_client_quotas`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterClientQuotasResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::quota::ClientQuotaEntity;

/// The result of the `Admin::alter_client_quotas` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterClientQuotasResult`.
#[derive(Clone, Debug)]
pub struct AlterClientQuotasResult {
    futures: HashMap<ClientQuotaEntity, KafkaFuture<()>>,
}

impl AlterClientQuotasResult {
    /// Creates a new result from the per-entity alteration futures.
    pub fn new(futures: HashMap<ClientQuotaEntity, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from quota entity to a future which can be used to check
    /// the status of the operation.
    ///
    /// Mirrors `AlterClientQuotasResult.values()`.
    pub fn values(&self) -> &HashMap<ClientQuotaEntity, KafkaFuture<()>> {
        &self.futures
    }

    /// Returns a future which succeeds only if all quota alterations succeed.
    ///
    /// Mirrors `AlterClientQuotasResult.all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}
