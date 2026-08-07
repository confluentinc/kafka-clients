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

//! All SASL/SCRAM credentials associated with a user.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.UserScramCredentialsDescription`.

use super::ScramCredentialInfo;

/// Representation of all SASL/SCRAM credentials associated with a user that can
/// be retrieved.
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentialsDescription {
    name: String,
    credential_infos: Vec<ScramCredentialInfo>,
}

impl UserScramCredentialsDescription {
    /// Creates a new description.
    ///
    /// * `name` — the required user name
    /// * `credential_infos` — the required SASL/SCRAM credential representations
    ///   for the user
    pub fn new(name: impl Into<String>, credential_infos: Vec<ScramCredentialInfo>) -> Self {
        Self { name: name.into(), credential_infos }
    }

    /// Returns the user name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the always non-null list of SASL/SCRAM credential representations
    /// for the user.
    pub fn credential_infos(&self) -> &[ScramCredentialInfo] {
        &self.credential_infos
    }
}

impl std::fmt::Display for UserScramCredentialsDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UserScramCredentialsDescription{{name='{}', credentialInfos={:?}}}",
            self.name, self.credential_infos
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::ScramMechanism;

    #[test]
    fn accessors_and_equality() {
        let d = UserScramCredentialsDescription::new(
            "u",
            vec![ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096)],
        );
        assert_eq!(d.name(), "u");
        assert_eq!(d.credential_infos().len(), 1);
        assert_eq!(
            d,
            UserScramCredentialsDescription::new(
                "u",
                vec![ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096)]
            )
        );
    }
}
