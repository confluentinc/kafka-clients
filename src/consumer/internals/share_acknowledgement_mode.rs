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

//! Share-group acknowledgement mode (KIP-932).
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode`.

// Phase 2 (M9) translates the acknowledgement core types; the config wiring
// (`share.acknowledgement.mode`) that consumes these accessors lands in a
// later phase.
#![allow(dead_code)]

use crate::common::KafkaError;

/// The inner acknowledgement mode enum. Corresponds to Java's nested
/// `ShareAcknowledgementMode.AcknowledgementMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AcknowledgementMode {
    /// Records are acknowledged implicitly by the next `poll`.
    Implicit,
    /// Records must be acknowledged explicitly.
    Explicit,
}

impl std::fmt::Display for AcknowledgementMode {
    /// Matches Java's `toString()`, which lowercases the enum name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Implicit => "implicit",
            Self::Explicit => "explicit",
        };
        write!(f, "{s}")
    }
}

/// The share acknowledgement mode controls whether records delivered by a
/// share consumer are acknowledged implicitly or explicitly.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode`.
///
/// deferred: the Java nested `ShareAcknowledgementMode.Validator` (implements
/// `ConfigDef.Validator`) is not translated here because the project has no
/// `ConfigDef::Validator` trait yet. It (and its `ensureValid`/`toString`
/// tests) belong to the `share.acknowledgement.mode` config-wiring phase,
/// where the `ConfigDef` validation surface is introduced — mirroring the
/// same deferral already recorded for
/// [`ShareAcquireMode`](super::share_acquire_mode::ShareAcquireMode). Until
/// then, an invalid value is rejected by [`ShareAcknowledgementMode::from_string`]
/// at the point the config value is parsed — same rejection, later point than
/// Java's `ConfigDef` validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ShareAcknowledgementMode {
    acknowledgement_mode: AcknowledgementMode,
}

impl ShareAcknowledgementMode {
    /// The implicit acknowledgement mode. Mirrors Java's
    /// `public static final ShareAcknowledgementMode IMPLICIT`.
    pub(crate) const IMPLICIT: Self = Self { acknowledgement_mode: AcknowledgementMode::Implicit };

    /// The explicit acknowledgement mode. Mirrors Java's
    /// `public static final ShareAcknowledgementMode EXPLICIT`.
    pub(crate) const EXPLICIT: Self = Self { acknowledgement_mode: AcknowledgementMode::Explicit };

    /// Returns the [`ShareAcknowledgementMode`] from the given string.
    ///
    /// Corresponds to Java's `fromString(String acknowledgementMode)`.
    ///
    /// Note the Java behaviour: the match is case-sensitive against the
    /// lowercase enum-option names (`"implicit"` / `"explicit"`), so
    /// `"IMPLICIT"` and `"EXPLICIT"` are rejected. This mirrors Java's
    /// `Utils.enumOptions(...)` returning the lowercased `toString()` values.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] for an unrecognized value,
    /// matching Java's `IllegalArgumentException`. (Java's `null` case has no
    /// Rust analogue since the parameter is a non-null `&str`.)
    pub(crate) fn from_string(acknowledgement_mode: &str) -> Result<Self, KafkaError> {
        match acknowledgement_mode {
            "implicit" => Ok(Self::IMPLICIT),
            "explicit" => Ok(Self::EXPLICIT),
            other => Err(KafkaError::illegal_argument(format!("Invalid acknowledgement mode: {other}"))),
        }
    }

    /// Returns the name of the acknowledgement mode.
    ///
    /// Corresponds to Java's `public String name()`.
    pub(crate) fn name(&self) -> String {
        self.acknowledgement_mode.to_string()
    }
}

impl std::fmt::Display for ShareAcknowledgementMode {
    /// Matches Java's `toString()`:
    /// `ShareAcknowledgementMode{mode=<MODE>}`, where `<MODE>` is the
    /// lowercased mode name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShareAcknowledgementMode{{mode={}}}", self.acknowledgement_mode)
    }
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.ShareAcknowledgementModeTest`.

    use super::*;

    /// Translated from `ShareAcknowledgementModeTest.testFromString`.
    #[test]
    fn test_from_string() {
        assert_eq!(
            ShareAcknowledgementMode::from_string("implicit").unwrap(),
            ShareAcknowledgementMode::IMPLICIT
        );
        assert_eq!(
            ShareAcknowledgementMode::from_string("explicit").unwrap(),
            ShareAcknowledgementMode::EXPLICIT
        );
        // Unrecognized / wrong-case values are rejected (Java is case-sensitive).
        assert!(ShareAcknowledgementMode::from_string("invalid").is_err());
        assert!(ShareAcknowledgementMode::from_string("IMPLICIT").is_err());
        assert!(ShareAcknowledgementMode::from_string("EXPLICIT").is_err());
        assert!(ShareAcknowledgementMode::from_string("").is_err());
        // Java's `fromString(null)` throws; the Rust `&str` signature has no
        // null analogue, so that case is omitted.

        // Error message content is part of the contract (DoD §3).
        let err = ShareAcknowledgementMode::from_string("invalid").expect_err("invalid mode must be rejected");
        assert!(err.to_string().contains("Invalid acknowledgement mode: invalid"), "got: {err}");
    }

    /// Translated from `ShareAcknowledgementModeTest.testEqualsAndHashCode`.
    /// The Java test asserts value-equality and `hashCode` consistency; in
    /// Rust `Eq` + `Hash` are derived, so we assert the equivalent value
    /// semantics.
    #[test]
    fn test_equals_and_hash_code() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mode1 = ShareAcknowledgementMode::IMPLICIT;
        let mode2 = ShareAcknowledgementMode::IMPLICIT;
        let mode3 = ShareAcknowledgementMode::EXPLICIT;

        assert_eq!(mode1, mode2);
        assert_ne!(mode1, mode3);
        assert_ne!(mode2, mode3);

        let hash = |m: &ShareAcknowledgementMode| {
            let mut h = DefaultHasher::new();
            m.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&mode1), hash(&mode2));
        assert_ne!(hash(&mode1), hash(&mode3));
    }

    /// Java's `Validator.ensureValid` / `Validator.toString` are deferred with
    /// the `Validator` type (see the deferral note on
    /// [`ShareAcknowledgementMode`]); `ShareAcknowledgementModeTest.testValidator`
    /// is deferred to the config-wiring phase alongside it.
    #[test]
    fn test_name_and_display() {
        assert_eq!(ShareAcknowledgementMode::IMPLICIT.name(), "implicit");
        assert_eq!(ShareAcknowledgementMode::EXPLICIT.name(), "explicit");
        assert_eq!(
            ShareAcknowledgementMode::IMPLICIT.to_string(),
            "ShareAcknowledgementMode{mode=implicit}"
        );
        assert_eq!(
            ShareAcknowledgementMode::EXPLICIT.to_string(),
            "ShareAcknowledgementMode{mode=explicit}"
        );
    }
}
