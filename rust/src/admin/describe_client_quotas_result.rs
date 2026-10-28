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

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::quota::ClientQuotaEntity;

/// The result of the `Admin::describe_client_quotas` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeClientQuotasResult")]
pub struct DescribeClientQuotasResult {
    entities: KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>>,
}

impl DescribeClientQuotasResult {
    /// Maps an entity to its configured quota value(s). Note if no value is defined for a quota
    /// type for that entity's config, then it is not included in the resulting value map.
    ///
    /// # Arguments
    ///
    /// - `entities` - future for the collection of entities that matched the filter
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClientQuotasResult#DescribeClientQuotasResult")]
    pub fn new(entities: KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>>) -> Self {
        Self { entities }
    }

    /// Returns a map from quota entity to a future which can be used to check the status of
    /// the operation.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClientQuotasResult#entities")]
    pub fn entities(&self) -> &KafkaFuture<HashMap<ClientQuotaEntity, HashMap<String, f64>>> {
        &self.entities
    }
}
