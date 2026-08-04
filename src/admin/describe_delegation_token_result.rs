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

//! The result of `Admin::describe_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.DescribeDelegationTokenResult`.

use crate::common::KafkaFuture;
use crate::common::security::token::delegation::DelegationToken;

/// The result of the `Admin::describe_delegation_token` call.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.DescribeDelegationTokenResult`.
#[derive(Clone, Debug)]
pub struct DescribeDelegationTokenResult {
    delegation_tokens: KafkaFuture<Vec<DelegationToken>>,
}

impl DescribeDelegationTokenResult {
    /// Creates a new result from the delegation-tokens future.
    pub fn new(delegation_tokens: KafkaFuture<Vec<DelegationToken>>) -> Self {
        Self { delegation_tokens }
    }

    /// Returns a future which yields the list of delegation tokens.
    ///
    /// Mirrors `DescribeDelegationTokenResult.delegationTokens()`.
    pub fn delegation_tokens(&self) -> &KafkaFuture<Vec<DelegationToken>> {
        &self.delegation_tokens
    }
}
