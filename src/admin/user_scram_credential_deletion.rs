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

//! A request to delete a SASL/SCRAM credential for a user.
//!
//! Corresponds to `org.apache.kafka.clients.admin.UserScramCredentialDeletion`.

use super::ScramMechanism;

/// A request to delete a SASL/SCRAM credential for a user.
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
#[derive(Debug, Clone)]
pub struct UserScramCredentialDeletion {
    user: String,
    mechanism: ScramMechanism,
}

impl UserScramCredentialDeletion {
    /// Creates a new deletion.
    ///
    /// * `user` — the mandatory user
    /// * `mechanism` — the mandatory mechanism
    pub fn new(user: impl Into<String>, mechanism: ScramMechanism) -> Self {
        Self { user: user.into(), mechanism }
    }

    /// Returns the always non-null user.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns the always non-null mechanism.
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors() {
        let deletion = UserScramCredentialDeletion::new("carol", ScramMechanism::ScramSha256);
        assert_eq!(deletion.user(), "carol");
        assert_eq!(deletion.mechanism(), ScramMechanism::ScramSha256);
    }
}
