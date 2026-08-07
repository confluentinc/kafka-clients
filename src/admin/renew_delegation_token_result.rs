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

//! The result of `Admin::renew_delegation_token`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.RenewDelegationTokenResult`.

use crate::common::KafkaFuture;

/// The result of the `Admin::renew_delegation_token` call.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.RenewDelegationTokenResult`.
#[derive(Clone, Debug)]
pub struct RenewDelegationTokenResult {
    expiry_timestamp: KafkaFuture<i64>,
}

impl RenewDelegationTokenResult {
    /// Creates a new result from the expiry-timestamp future.
    pub fn new(expiry_timestamp: KafkaFuture<i64>) -> Self {
        Self { expiry_timestamp }
    }

    /// Returns a future which yields the new expiry timestamp.
    ///
    /// Mirrors `RenewDelegationTokenResult.expiryTimestamp()`.
    pub fn expiry_timestamp(&self) -> &KafkaFuture<i64> {
        &self.expiry_timestamp
    }
}
