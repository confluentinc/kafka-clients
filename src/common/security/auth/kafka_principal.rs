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

//! Kafka principal type.
//!
//! Corresponds to `org.apache.kafka.common.security.auth.KafkaPrincipal`.

use std::fmt;
use std::hash::{Hash, Hasher};

/// Principals in Kafka are defined by a type and a name.
///
/// The principal type will always be `"User"` for the simple authorizer that is
/// enabled by default, but custom authorizers can leverage different principal
/// types (such as to enable group or role-based ACLs).
///
/// Corresponds to `org.apache.kafka.common.security.auth.KafkaPrincipal`.
#[derive(Debug, Clone)]
pub struct KafkaPrincipal {
    principal_type: String,
    name: String,
    token_authenticated: bool,
}

impl KafkaPrincipal {
    /// The principal type used by the default authorizer.
    pub const USER_TYPE: &'static str = "User";

    /// Creates a new principal from a type and name (not token-authenticated).
    ///
    /// Mirrors `new KafkaPrincipal(principalType, name)`.
    pub fn new(principal_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self::with_token_authenticated(principal_type, name, false)
    }

    /// Creates a new principal from a type, name, and token-authenticated flag.
    ///
    /// Mirrors `new KafkaPrincipal(principalType, name, tokenAuthenticated)`.
    pub fn with_token_authenticated(
        principal_type: impl Into<String>,
        name: impl Into<String>,
        token_authenticated: bool,
    ) -> Self {
        Self { principal_type: principal_type.into(), name: name.into(), token_authenticated }
    }

    /// The anonymous principal (`User:ANONYMOUS`).
    ///
    /// Mirrors `KafkaPrincipal.ANONYMOUS`. Modeled as a function rather than a
    /// constant because the name is an owned `String`.
    pub fn anonymous() -> Self {
        Self::new(Self::USER_TYPE, "ANONYMOUS")
    }

    /// Returns the principal name.
    ///
    /// Mirrors `KafkaPrincipal.getName`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the principal type.
    ///
    /// Mirrors `KafkaPrincipal.getPrincipalType`.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Sets whether the principal was authenticated with a delegation token.
    ///
    /// Mirrors `KafkaPrincipal.tokenAuthenticated(boolean)`.
    pub fn set_token_authenticated(&mut self, token_authenticated: bool) {
        self.token_authenticated = token_authenticated;
    }

    /// Whether the principal was authenticated with a delegation token.
    ///
    /// Mirrors `KafkaPrincipal.tokenAuthenticated()`.
    pub fn token_authenticated(&self) -> bool {
        self.token_authenticated
    }
}

impl fmt::Display for KafkaPrincipal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

// `equals` / `hashCode` in Java consider only `principalType` and `name`
// (never the mutable `tokenAuthenticated` flag), so we implement them by hand
// rather than deriving.
impl PartialEq for KafkaPrincipal {
    fn eq(&self, other: &Self) -> bool {
        self.principal_type == other.principal_type && self.name == other.name
    }
}

impl Eq for KafkaPrincipal {}

impl Hash for KafkaPrincipal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.principal_type.hash(state);
        self.name.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(p: &KafkaPrincipal) -> u64 {
        let mut hasher = DefaultHasher::new();
        p.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn to_string_is_type_colon_name() {
        let principal = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "alice");
        assert_eq!(principal.to_string(), "User:alice");
    }

    #[test]
    fn accessors_return_components() {
        let principal = KafkaPrincipal::new("Group", "admins");
        assert_eq!(principal.principal_type(), "Group");
        assert_eq!(principal.name(), "admins");
        assert!(!principal.token_authenticated());
    }

    #[test]
    fn equals_and_hash_ignore_token_authenticated() {
        let a = KafkaPrincipal::with_token_authenticated(KafkaPrincipal::USER_TYPE, "bob", false);
        let b = KafkaPrincipal::with_token_authenticated(KafkaPrincipal::USER_TYPE, "bob", true);
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn differs_by_type_or_name() {
        let base = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "bob");
        assert_ne!(base, KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "carol"));
        assert_ne!(base, KafkaPrincipal::new("Group", "bob"));
    }

    #[test]
    fn anonymous_is_user_anonymous() {
        let anon = KafkaPrincipal::anonymous();
        assert_eq!(anon.principal_type(), KafkaPrincipal::USER_TYPE);
        assert_eq!(anon.name(), "ANONYMOUS");
        assert_eq!(anon.to_string(), "User:ANONYMOUS");
    }

    #[test]
    fn setter_updates_token_authenticated() {
        let mut principal = KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, "dave");
        principal.set_token_authenticated(true);
        assert!(principal.token_authenticated());
    }
}
