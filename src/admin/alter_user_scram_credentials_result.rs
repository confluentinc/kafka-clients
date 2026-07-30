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

//! The result of the `Admin::alter_user_scram_credentials` call.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.AlterUserScramCredentialsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

/// The result of the `Admin::alter_user_scram_credentials` call.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.AlterUserScramCredentialsResult`.
#[derive(Debug, Clone)]
pub struct AlterUserScramCredentialsResult {
    futures: HashMap<String, KafkaFuture<()>>,
}

impl AlterUserScramCredentialsResult {
    /// Creates a new result from the per-user futures.
    ///
    /// * `futures` — the required map from user names to futures representing the
    ///   results of the alteration(s) for each user
    pub fn new(futures: HashMap<String, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from user names to futures, which can be used to check the
    /// status of the alteration(s) for each user.
    ///
    /// Mirrors `AlterUserScramCredentialsResult.values()`.
    pub fn values(&self) -> &HashMap<String, KafkaFuture<()>> {
        &self.futures
    }

    /// Returns a future which succeeds only if all the user SCRAM credential
    /// alterations succeed.
    ///
    /// Mirrors `AlterUserScramCredentialsResult.all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}
