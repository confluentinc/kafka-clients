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

//! A request to update/insert a SASL/SCRAM credential for a user.
//!
//! Corresponds to `org.apache.kafka.clients.admin.UserScramCredentialUpsertion`.

use rand::Rng;

use super::ScramCredentialInfo;

/// A request to update/insert a SASL/SCRAM credential for a user.
///
/// See [KIP-554: Add Broker-side SCRAM Config API](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API).
#[derive(Debug, Clone)]
pub struct UserScramCredentialUpsertion {
    user: String,
    info: ScramCredentialInfo,
    salt: Vec<u8>,
    password: Vec<u8>,
}

impl UserScramCredentialUpsertion {
    /// Constructor that accepts a string password and generates a random salt.
    ///
    /// Mirrors `UserScramCredentialUpsertion(String, ScramCredentialInfo, String)`,
    /// which encodes the password with UTF-8.
    pub fn new(user: impl Into<String>, credential_info: ScramCredentialInfo, password: &str) -> Self {
        Self::with_password_bytes(user, credential_info, password.as_bytes().to_vec())
    }

    /// Constructor that accepts a byte password and generates a random salt.
    ///
    /// Mirrors `UserScramCredentialUpsertion(String, ScramCredentialInfo, byte[])`.
    pub fn with_password_bytes(
        user: impl Into<String>,
        credential_info: ScramCredentialInfo,
        password: Vec<u8>,
    ) -> Self {
        let salt = generate_random_salt();
        Self::with_salt(user, credential_info, password, salt)
    }

    /// Constructor that accepts an explicit salt.
    ///
    /// Mirrors `UserScramCredentialUpsertion(String, ScramCredentialInfo, byte[], byte[])`.
    pub fn with_salt(
        user: impl Into<String>,
        credential_info: ScramCredentialInfo,
        password: Vec<u8>,
        salt: Vec<u8>,
    ) -> Self {
        Self { user: user.into(), info: credential_info, salt, password }
    }

    /// Returns the always non-null user.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns the mechanism and iterations.
    pub fn credential_info(&self) -> &ScramCredentialInfo {
        &self.info
    }

    /// Returns the salt.
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    /// Returns the password.
    pub fn password(&self) -> &[u8] {
        &self.password
    }
}

/// Generates a random salt.
///
/// Mirrors `ScramFormatter.secureRandomBytes(new SecureRandom())` in spirit:
/// Java produces the UTF-8 bytes of a 130-bit random base-36 string. Rather than
/// pull in the full `ScramFormatter` (out of scope — only `hi()` is translated),
/// we generate the equivalent: a random radix-36 string of the same magnitude.
/// The salt is opaque random data sent verbatim to the broker, so any
/// cryptographically-random value of adequate length is behavior-equivalent.
fn generate_random_salt() -> Vec<u8> {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    // 26 base-36 characters is ~134 bits of entropy, matching Java's 130-bit
    // `new BigInteger(130, random)`.
    let mut rng = rand::rng();
    (0..26).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::ScramMechanism;

    #[test]
    fn string_constructor_encodes_password_utf8_and_generates_salt() {
        let upsertion = UserScramCredentialUpsertion::new(
            "alice",
            ScramCredentialInfo::new(ScramMechanism::ScramSha256, 4096),
            "pw",
        );
        assert_eq!(upsertion.user(), "alice");
        assert_eq!(upsertion.password(), b"pw");
        assert_eq!(upsertion.credential_info().mechanism(), ScramMechanism::ScramSha256);
        assert!(!upsertion.salt().is_empty());
    }

    #[test]
    fn explicit_salt_is_preserved() {
        let upsertion = UserScramCredentialUpsertion::with_salt(
            "bob",
            ScramCredentialInfo::new(ScramMechanism::ScramSha512, 8192),
            b"secret".to_vec(),
            b"my-salt".to_vec(),
        );
        assert_eq!(upsertion.salt(), b"my-salt");
        assert_eq!(upsertion.password(), b"secret");
    }

    #[test]
    fn generated_salts_are_random() {
        let a = generate_random_salt();
        let b = generate_random_salt();
        assert_eq!(a.len(), 26);
        assert_ne!(a, b);
    }
}
