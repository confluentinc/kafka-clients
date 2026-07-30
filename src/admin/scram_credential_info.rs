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

//! Mechanism and iterations for a SASL/SCRAM credential.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ScramCredentialInfo`.

use super::ScramMechanism;

/// Mechanism and iterations for a SASL/SCRAM credential associated with a user.
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScramCredentialInfo {
    mechanism: ScramMechanism,
    iterations: i32,
}

impl ScramCredentialInfo {
    /// Creates a new credential info.
    ///
    /// * `mechanism` — the required mechanism
    /// * `iterations` — the number of iterations used when creating the
    ///   credential
    pub fn new(mechanism: ScramMechanism, iterations: i32) -> Self {
        Self { mechanism, iterations }
    }

    /// Returns the mechanism.
    pub fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// Returns the number of iterations used when creating the credential.
    pub fn iterations(&self) -> i32 {
        self.iterations
    }
}

impl std::fmt::Display for ScramCredentialInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScramCredentialInfo{{mechanism={:?}, iterations={}}}",
            self.mechanism, self.iterations
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_equality() {
        let info = ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096);
        assert_eq!(info.mechanism(), ScramMechanism::ScramSha256);
        assert_eq!(info.iterations(), 4096);
        assert_eq!(info, ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096));
        assert_ne!(info, ScramCredentialInfo::new(ScramMechanism::ScramSha512, 4096));
        assert_ne!(info, ScramCredentialInfo::new(ScramMechanism::ScramSha256, 8192));
    }
}
