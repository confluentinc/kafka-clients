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

//! The result of `Admin::describe_client_quotas`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeClientQuotasResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::quota::ClientQuotaEntity;

/// The result of the `Admin::describe_client_quotas` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeClientQuotasResult`.
#[derive(Clone, Debug)]
pub struct DescribeClientQuotasResult {
    entities: KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>>,
}

impl DescribeClientQuotasResult {
    /// Creates a new result from the future for the matched entities.
    pub fn new(entities: KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>>) -> Self {
        Self { entities }
    }

    /// Returns a future which maps each matched quota entity to its configured
    /// quota value(s). If no value is defined for a quota type for that
    /// entity's config, then it is not included in the resulting value map.
    ///
    /// Mirrors `DescribeClientQuotasResult.entities()`.
    pub fn entities(&self) -> &KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>> {
        &self.entities
    }
}
