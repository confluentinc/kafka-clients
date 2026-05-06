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
use std::sync::OnceLock;

/// The standard principal type used by Kafka's default authorizer. Mirrors
/// Java's `KafkaPrincipal.USER_TYPE`.
pub const USER_TYPE: &str = "User";

/// The well-known anonymous principal name. Mirrors Java's literal
/// `"ANONYMOUS"` argument to the static `ANONYMOUS` field.
pub const ANONYMOUS_NAME: &str = "ANONYMOUS";

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
    ///
    /// Java's constructor uses `requireNonNull` on the type/name strings;
    /// in Rust the parameter type already excludes `null`, so no further
    /// validation is performed. Empty strings are accepted, matching the
    /// Java contract exactly.
    pub fn with_token_authenticated(
        principal_type: impl Into<String>,
        name: impl Into<String>,
        token_authenticated: bool,
    ) -> Self {
        KafkaPrincipal { principal_type: principal_type.into(), name: name.into(), token_authenticated }
    }

    /// The well-known anonymous principal. Mirrors Java's
    /// `public static final KafkaPrincipal ANONYMOUS`. Java caches a single
    /// instance; in Rust we cache the base `(type, name)` strings via a
    /// `OnceLock` so each call clones the cached instance instead of
    /// allocating two fresh `String`s. The result is by-value because
    /// callers (e.g. `peer_principal()`) expect ownership; the underlying
    /// strings are short, so the per-call cost is two `String::clone`s of
    /// `"User"` / `"ANONYMOUS"`.
    pub fn anonymous() -> Self {
        static ANONYMOUS: OnceLock<KafkaPrincipal> = OnceLock::new();
        ANONYMOUS.get_or_init(|| KafkaPrincipal::new(USER_TYPE, ANONYMOUS_NAME)).clone()
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

    /// Java's `requireNonNull` only rejects `null`; empty `String`s are
    /// accepted. Rust must match — `String` is non-nullable already, so
    /// no further validation is needed.
    #[test]
    fn empty_strings_are_accepted() {
        let p = KafkaPrincipal::new("", "");
        assert_eq!(p.principal_type(), "");
        assert_eq!(p.name(), "");
        assert_eq!(p.to_string(), ":");

        let p = KafkaPrincipal::new("User", "");
        assert_eq!(p.principal_type(), "User");
        assert_eq!(p.name(), "");
        assert_eq!(p.to_string(), "User:");
    }

    /// `anonymous()` returns the singleton-equivalent: every call yields an
    /// equal value, mirroring Java's `public static final ANONYMOUS`. The
    /// strings are cached via `OnceLock`, so callers don't pay for two
    /// fresh `String` allocations on every invocation.
    #[test]
    fn anonymous_is_idempotent() {
        let a = KafkaPrincipal::anonymous();
        let b = KafkaPrincipal::anonymous();
        assert_eq!(a, b);
        assert_eq!(a.principal_type(), USER_TYPE);
        assert_eq!(a.name(), ANONYMOUS_NAME);
    }
}
