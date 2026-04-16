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

//! SSL client authentication policy.
//!
//! Translated from `org.apache.kafka.common.config.SslClientAuth`.

use std::fmt;
use std::str::FromStr;

/// Whether the server requires or requests client TLS authentication.
///
/// This is primarily a server-side setting, but included for config
/// compatibility with Java's `SslClientAuth`.
///
/// Translated from `org.apache.kafka.common.config.SslClientAuth`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslClientAuth {
    /// Server requires client certificate.
    Required,
    /// Server requests but does not require client certificate.
    Requested,
    /// Server does not request client certificate.
    #[default]
    None,
}

impl SslClientAuth {
    /// All variants in declaration order.
    const ALL: [SslClientAuth; 3] = [SslClientAuth::Required, SslClientAuth::Requested, SslClientAuth::None];

    /// Returns the variant name in uppercase, matching the Java enum name.
    fn name(self) -> &'static str {
        match self {
            SslClientAuth::Required => "REQUIRED",
            SslClientAuth::Requested => "REQUESTED",
            SslClientAuth::None => "NONE",
        }
    }

    /// Case-insensitive lookup by config string.
    ///
    /// Returns `SslClientAuth::None` if `key` is `None`.
    /// Returns `None` if `key` is `Some` but does not match any variant.
    ///
    /// Matches Java's `SslClientAuth.forConfig(String)` behavior:
    /// - `null` maps to `NONE`
    /// - Unknown string returns `null` (here: `Option::None`)
    pub fn for_config(key: Option<&str>) -> Option<Self> {
        match key {
            Option::None => Some(SslClientAuth::None),
            Some(k) => {
                let upper = k.to_uppercase();
                Self::ALL.iter().find(|a| a.name() == upper).copied()
            },
        }
    }
}

impl fmt::Display for SslClientAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java's toString() returns lowercase
        write!(f, "{}", self.name().to_lowercase())
    }
}

impl FromStr for SslClientAuth {
    type Err = String;

    /// Parses an `SslClientAuth` from its name (case-insensitive).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.to_uppercase();
        SslClientAuth::ALL
            .iter()
            .find(|a| a.name() == upper)
            .copied()
            .ok_or_else(|| format!("No enum constant SslClientAuth.{}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(SslClientAuth::default(), SslClientAuth::None);
    }

    #[test]
    fn test_for_config_none_key() {
        assert_eq!(SslClientAuth::for_config(Option::None), Some(SslClientAuth::None));
    }

    #[test]
    fn test_for_config_required() {
        assert_eq!(SslClientAuth::for_config(Some("REQUIRED")), Some(SslClientAuth::Required));
    }

    #[test]
    fn test_for_config_requested() {
        assert_eq!(SslClientAuth::for_config(Some("REQUESTED")), Some(SslClientAuth::Requested));
    }

    #[test]
    fn test_for_config_none_variant() {
        assert_eq!(SslClientAuth::for_config(Some("NONE")), Some(SslClientAuth::None));
    }

    #[test]
    fn test_for_config_case_insensitive() {
        assert_eq!(SslClientAuth::for_config(Some("required")), Some(SslClientAuth::Required));
        assert_eq!(SslClientAuth::for_config(Some("Requested")), Some(SslClientAuth::Requested));
        assert_eq!(SslClientAuth::for_config(Some("none")), Some(SslClientAuth::None));
    }

    #[test]
    fn test_for_config_invalid() {
        assert_eq!(SslClientAuth::for_config(Some("INVALID")), Option::None);
        assert_eq!(SslClientAuth::for_config(Some("")), Option::None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", SslClientAuth::Required), "required");
        assert_eq!(format!("{}", SslClientAuth::Requested), "requested");
        assert_eq!(format!("{}", SslClientAuth::None), "none");
    }

    #[test]
    fn test_from_str_roundtrip() {
        for variant in &SslClientAuth::ALL {
            let name = variant.name();
            let parsed: SslClientAuth = name.parse().unwrap();
            assert_eq!(*variant, parsed);
        }
    }

    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!("required".parse::<SslClientAuth>().unwrap(), SslClientAuth::Required);
        assert_eq!("None".parse::<SslClientAuth>().unwrap(), SslClientAuth::None);
    }

    #[test]
    fn test_from_str_invalid() {
        assert!("INVALID".parse::<SslClientAuth>().is_err());
        assert!("".parse::<SslClientAuth>().is_err());
    }

    #[test]
    fn test_clone_and_copy() {
        let a = SslClientAuth::Required;
        let b = a;
        assert_eq!(a, b);
    }
}
