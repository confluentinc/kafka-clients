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

//! Password config value wrapper.
//!
//! Corresponds to `org.apache.kafka.common.config.types.Password`.

use std::fmt;

/// A wrapper class for passwords to hide them while logging a config.
///
/// Corresponds to `org.apache.kafka.common.config.types.Password`. Java wraps
/// every `ConfigDef.Type.PASSWORD` value in this class, so the secret never
/// reaches a log line through `toString()`. Java has a single stringification;
/// Rust has two, so **both** [`fmt::Display`] (Java's `toString()`) and
/// [`fmt::Debug`] render [`Password::HIDDEN`] and nothing else — no type name
/// and no length, since the length is information about the secret too.
///
/// The plaintext is reachable only through the explicit [`Password::value`]
/// accessor. There is deliberately no `Deref<Target = str>`, no `From`
/// conversion and no `Default`: Java has none of them, and each would make an
/// accidental plaintext path easier. Construction is explicit through
/// [`Password::new`].
///
/// Equality and hashing follow the wrapped value, as Java's `equals` /
/// `hashCode` do.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Password {
    value: String,
}

impl Password {
    /// The string every rendering of a [`Password`] produces.
    ///
    /// Corresponds to Java's `Password.HIDDEN`.
    pub const HIDDEN: &'static str = "[hidden]";

    /// Construct a new `Password` object.
    ///
    /// `value` is the value of a password.
    ///
    /// Corresponds to Java's `Password(String value)` constructor.
    pub fn new(value: impl Into<String>) -> Self {
        Self { value: value.into() }
    }

    /// Returns the real password string.
    ///
    /// Corresponds to Java's `Password.value()`.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for Password {
    /// Returns the hidden password string, [`Password::HIDDEN`].
    ///
    /// Corresponds to Java's `Password.toString()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::HIDDEN)
    }
}

impl fmt::Debug for Password {
    /// Returns the hidden password string, [`Password::HIDDEN`], exactly as
    /// [`fmt::Display`] does.
    ///
    /// Java has no separate debug rendering: `toString()` is the only one and
    /// it hides the value, so the Rust `Debug` must hide it as well.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::HIDDEN)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    fn hash_of<T: Hash + ?Sized>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_hidden_constant_matches_java() {
        assert_eq!(Password::HIDDEN, "[hidden]");
    }

    #[test]
    fn test_value_round_trips() {
        assert_eq!(Password::new("s3cr3t-Pa55").value(), "s3cr3t-Pa55");
        assert_eq!(Password::new(String::from("owned-secret")).value(), "owned-secret");
        assert_eq!(Password::new("").value(), "");
    }

    #[test]
    fn test_display_renders_exactly_hidden() {
        let password = Password::new("s3cr3t-Pa55");
        assert_eq!(password.to_string(), "[hidden]");
        assert_eq!(format!("{password}"), "[hidden]");
        // Width and precision flags do not change the output.
        assert_eq!(format!("{password:>30}"), "[hidden]");
        assert_eq!(format!("{password:.2}"), "[hidden]");
    }

    #[test]
    fn test_debug_renders_exactly_hidden() {
        let password = Password::new("s3cr3t-Pa55");
        assert_eq!(format!("{password:?}"), "[hidden]");
        assert_eq!(format!("{password:#?}"), "[hidden]");
        // No type name is rendered around the placeholder.
        assert!(!format!("{password:?}").contains("Password"));
        // Nested inside another derived `Debug`, only the placeholder shows.
        assert_eq!(format!("{:?}", Some(password)), "Some([hidden])");
    }

    #[test]
    fn test_different_lengths_render_identically() {
        let short = Password::new("a");
        let long = Password::new("a-much-longer-secret-value-of-another-length");
        assert_eq!(short.to_string(), long.to_string());
        assert_eq!(format!("{short:?}"), format!("{long:?}"));
        assert_eq!(format!("{short:#?}"), format!("{long:#?}"));
        assert_eq!(format!("{:?}", Password::new("")), format!("{:?}", Password::new("x")));
    }

    #[test]
    fn test_equality_follows_value() {
        assert_eq!(Password::new("same"), Password::new("same"));
        assert_eq!(Password::new("same"), Password::new(String::from("same")));
        assert_ne!(Password::new("same"), Password::new("other"));
        assert_ne!(Password::new("same"), Password::new("Same"));
    }

    #[test]
    fn test_hash_follows_value() {
        // Java's `hashCode` is `value.hashCode()`; the derive hashes the one
        // `value` field, so the hash is the hash of the wrapped string.
        assert_eq!(hash_of(&Password::new("same")), hash_of(&Password::new("same")));
        assert_eq!(hash_of(&Password::new("same")), hash_of(&String::from("same")));

        let set: HashSet<Password> = [Password::new("first"), Password::new("first"), Password::new("second")]
            .into_iter()
            .collect();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&Password::new("first")));
        assert!(set.contains(&Password::new("second")));
        assert!(!set.contains(&Password::new("third")));
    }

    #[test]
    fn test_clone() {
        let original = Password::new("clone-me");
        let cloned = original.clone();
        assert_eq!(cloned, original);
        assert_eq!(cloned.value(), "clone-me");
        assert_eq!(format!("{cloned:?}"), "[hidden]");
    }
}
