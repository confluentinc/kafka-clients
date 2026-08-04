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

//! The result of `Admin::create_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.CreateDelegationTokenResult`.

use crate::common::KafkaFuture;
use crate::common::security::token::delegation::DelegationToken;

/// The result of the `Admin::create_delegation_token` call.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.CreateDelegationTokenResult`.
#[derive(Clone, Debug)]
pub struct CreateDelegationTokenResult {
    delegation_token: KafkaFuture<DelegationToken>,
}

impl CreateDelegationTokenResult {
    /// Creates a new result from the delegation-token future.
    pub fn new(delegation_token: KafkaFuture<DelegationToken>) -> Self {
        Self { delegation_token }
    }

    /// Returns a future which yields a delegation token.
    ///
    /// Mirrors `CreateDelegationTokenResult.delegationToken()`.
    pub fn delegation_token(&self) -> &KafkaFuture<DelegationToken> {
        &self.delegation_token
    }
}
