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

//! Translation of `org.apache.kafka.common.security.auth.KafkaPrincipal`.

use std::fmt;

/// The standard principal type used by Kafka's default authorizer. Mirrors
/// Java's `KafkaPrincipal.USER_TYPE`.
pub const USER_TYPE: &str = "User";

/// Principals in Kafka are defined by a type and a name. The principal type
/// is always `"User"` for the simple authorizer enabled by default;
/// custom authorizers can leverage different principal types (e.g. to
/// enable group or role-based ACLs).
///
/// Mirrors the Java `KafkaPrincipal` value type.
///
/// `PartialEq`/`Eq`/`Hash` are implemented by hand to match Java semantics:
/// equality and hashing depend only on `(principal_type, name)` —
/// `token_authenticated` is excluded, mirroring Java's `equals` /
/// `hashCode`.
#[derive(Debug, Clone)]
pub struct KafkaPrincipal {
    principal_type: String,
    name: String,
    token_authenticated: bool,
}

impl PartialEq for KafkaPrincipal {
    fn eq(&self, other: &Self) -> bool {
        self.principal_type == other.principal_type && self.name == other.name
    }
}

impl Eq for KafkaPrincipal {}

impl std::hash::Hash for KafkaPrincipal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.principal_type.hash(state);
        self.name.hash(state);
    }
}

impl KafkaPrincipal {
    /// Construct a `KafkaPrincipal` with `token_authenticated = false`.
    /// Mirrors `new KafkaPrincipal(String principalType, String name)`.
    pub fn new(principal_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self::with_token_authenticated(principal_type, name, false)
    }

    /// Mirrors `new KafkaPrincipal(String principalType, String name,
    /// boolean tokenAuthenticated)`.
    pub fn with_token_authenticated(
        principal_type: impl Into<String>,
        name: impl Into<String>,
        token_authenticated: bool,
    ) -> Self {
        let principal_type = principal_type.into();
        let name = name.into();
        // Mirrors Java's `requireNonNull` — Rust's `String` cannot be null,
        // so we instead reject empty strings, which is the closest moral
        // equivalent and matches the Java intent that the type/name be
        // present.
        assert!(!principal_type.is_empty(), "Principal type cannot be empty");
        assert!(!name.is_empty(), "Principal name cannot be empty");
        KafkaPrincipal { principal_type, name, token_authenticated }
    }

    /// The well-known anonymous principal. Mirrors `KafkaPrincipal.ANONYMOUS`.
    pub fn anonymous() -> Self {
        KafkaPrincipal::new(USER_TYPE, "ANONYMOUS")
    }

    /// Mirrors `KafkaPrincipal.getName()`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Mirrors `KafkaPrincipal.getPrincipalType()`.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Mirrors `KafkaPrincipal.tokenAuthenticated()` (getter).
    pub fn token_authenticated(&self) -> bool {
        self.token_authenticated
    }

    /// Mirrors `KafkaPrincipal.tokenAuthenticated(boolean)` (setter).
    pub fn set_token_authenticated(&mut self, token_authenticated: bool) {
        self.token_authenticated = token_authenticated;
    }
}

impl fmt::Display for KafkaPrincipal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `toString()`: `principalType + ":" + name`.
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_constants() {
        let p = KafkaPrincipal::anonymous();
        assert_eq!(p.principal_type(), USER_TYPE);
        assert_eq!(p.name(), "ANONYMOUS");
        assert!(!p.token_authenticated());
    }

    #[test]
    fn to_string_format() {
        let p = KafkaPrincipal::new("User", "alice");
        assert_eq!(p.to_string(), "User:alice");
    }

    #[test]
    fn equality_is_by_type_and_name() {
        let a = KafkaPrincipal::new("User", "alice");
        let b = KafkaPrincipal::new("User", "alice");
        let c = KafkaPrincipal::new("User", "bob");
        assert_eq!(a, b);
        assert_ne!(a, c);

        // Mirrors Java: `token_authenticated` does not affect equality.
        let mut d = KafkaPrincipal::new("User", "alice");
        d.set_token_authenticated(true);
        assert_eq!(a, d, "token_authenticated must not affect equality");
    }
}
