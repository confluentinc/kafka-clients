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

//! A request to alter a user's SASL/SCRAM credentials.
//!
//! Corresponds to `org.apache.kafka.clients.admin.UserScramCredentialAlteration`.

use super::{UserScramCredentialDeletion, UserScramCredentialUpsertion};

/// A request to alter a user's SASL/SCRAM credentials.
///
/// Java models this as an `abstract` base with the two concrete subclasses
/// `UserScramCredentialUpsertion` and `UserScramCredentialDeletion`. Rust models
/// the closed subclass hierarchy as an enum: the Java `instanceof` dispatch in
/// `KafkaAdminClient.alterUserScramCredentials` becomes a `match` here, and the
/// shared `user()` accessor of the abstract base becomes a method delegating to
/// the active variant. This is faithful because the Java hierarchy is sealed
/// (both subclasses live in the same package and there is no public subclassing
/// contract).
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
#[derive(Debug, Clone)]
pub enum UserScramCredentialAlteration {
    /// An update/insertion of a credential ([`UserScramCredentialUpsertion`]).
    Upsertion(UserScramCredentialUpsertion),
    /// A deletion of a credential ([`UserScramCredentialDeletion`]).
    Deletion(UserScramCredentialDeletion),
}

impl UserScramCredentialAlteration {
    /// Returns the always non-null user.
    ///
    /// Mirrors `UserScramCredentialAlteration.user()`.
    pub fn user(&self) -> &str {
        match self {
            UserScramCredentialAlteration::Upsertion(u) => u.user(),
            UserScramCredentialAlteration::Deletion(d) => d.user(),
        }
    }
}

impl From<UserScramCredentialUpsertion> for UserScramCredentialAlteration {
    fn from(upsertion: UserScramCredentialUpsertion) -> Self {
        UserScramCredentialAlteration::Upsertion(upsertion)
    }
}

impl From<UserScramCredentialDeletion> for UserScramCredentialAlteration {
    fn from(deletion: UserScramCredentialDeletion) -> Self {
        UserScramCredentialAlteration::Deletion(deletion)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{ScramCredentialInfo, ScramMechanism};

    #[test]
    fn user_accessor_delegates_to_variant() {
        let upsertion: UserScramCredentialAlteration =
            UserScramCredentialUpsertion::new("u1", ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096), "pw")
                .into();
        assert_eq!(upsertion.user(), "u1");

        let deletion: UserScramCredentialAlteration =
            UserScramCredentialDeletion::new("u2", ScramMechanism::ScramSha512).into();
        assert_eq!(deletion.user(), "u2");
    }
}
