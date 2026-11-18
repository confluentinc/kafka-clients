// Copyright 2026 Confluent Inc.
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

//! The acknowledgement mode of a share consumer, `share.acknowledgement.mode`.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode`,
//! with its nested `AcknowledgementMode` enum and `Validator` class.

use std::fmt;

use crate::common::Error;
use crate::common::config::config_def::{self, ParsedValue, as_string, config_error};

/// The acknowledgement mode of a share consumer: implicit (every delivered
/// record is accepted on the next `poll`, `commitSync` or `commitAsync`) or
/// explicit (the application acknowledges each record).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode")]
pub(crate) struct ShareAcknowledgementMode {
    acknowledgement_mode: AcknowledgementMode,
}

/// The two acknowledgement modes. `Display` is Java's `toString()`, the
/// constant's name in lower case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode$AcknowledgementMode")]
pub(crate) enum AcknowledgementMode {
    Implicit,
    Explicit,
}

impl AcknowledgementMode {
    /// Java's `values()`, in declaration order.
    const VALUES: [Self; 2] = [Self::Implicit, Self::Explicit];

    /// Java's `toString()`: the constant's name in lower case.
    fn as_str(self) -> &'static str {
        match self {
            Self::Implicit => "implicit",
            Self::Explicit => "explicit",
        }
    }
}

impl fmt::Display for AcknowledgementMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ShareAcknowledgementMode {
    pub(crate) const IMPLICIT: Self = Self { acknowledgement_mode: AcknowledgementMode::Implicit };
    pub(crate) const EXPLICIT: Self = Self { acknowledgement_mode: AcknowledgementMode::Explicit };

    /// Returns the ShareAcknowledgementMode from the given string.
    ///
    /// The match is case-sensitive: only the lower-case names are accepted
    /// (Java checks the string against `Utils.enumOptions`, the constants'
    /// `toString()`, before upper-casing it for `valueOf`). `None` is Java's
    /// `null`.
    ///
    /// # Errors
    ///
    /// `Error::LocalIllegalArgument` with `"Acknowledgement mode is null"` for
    /// `None`, and `"Invalid acknowledgement mode: <value>"` otherwise.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode#fromString")]
    pub(crate) fn from_string(acknowledgement_mode: Option<&str>) -> Result<Self, Error> {
        let Some(acknowledgement_mode) = acknowledgement_mode else {
            return Err(Error::local_illegal_argument("Acknowledgement mode is null"));
        };
        AcknowledgementMode::VALUES
            .into_iter()
            .find(|mode| mode.as_str() == acknowledgement_mode)
            .map(|mode| match mode {
                AcknowledgementMode::Implicit => Self::IMPLICIT,
                AcknowledgementMode::Explicit => Self::EXPLICIT,
            })
            .ok_or_else(|| {
                Error::local_illegal_argument(format!("Invalid acknowledgement mode: {acknowledgement_mode}"))
            })
    }

    /// Returns the name of the acknowledgement mode.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode#name")]
    pub(crate) fn name(&self) -> &'static str {
        self.acknowledgement_mode.as_str()
    }
}

impl fmt::Display for ShareAcknowledgementMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShareAcknowledgementMode{{mode={}}}", self.acknowledgement_mode)
    }
}

/// Java's `ShareAcknowledgementMode.Validator`: the value must be accepted by
/// [`ShareAcknowledgementMode::from_string`].
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode$Validator")]
pub(crate) struct Validator;

impl config_def::Validator for Validator {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        let acknowledgement_mode = as_string(value)?;
        ShareAcknowledgementMode::from_string(acknowledgement_mode).map(|_| ()).map_err(|_| {
            config_error(
                name,
                value,
                format!(
                    "Invalid value `{}` for configuration {name}. The value must either be 'implicit' or 'explicit'.",
                    acknowledgement_mode.unwrap_or("null")
                ),
            )
        })
    }
}

impl fmt::Display for Validator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = AcknowledgementMode::VALUES.map(AcknowledgementMode::as_str);
        write!(f, "[{}]", values.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;
    use crate::common::config::config_def::Validator as _;

    fn string_value(value: &str) -> ParsedValue {
        ParsedValue::String(value.to_owned())
    }

    fn hash_code(mode: &ShareAcknowledgementMode) -> u64 {
        let mut hasher = DefaultHasher::new();
        mode.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_from_string() {
        assert_eq!(
            ShareAcknowledgementMode::IMPLICIT,
            ShareAcknowledgementMode::from_string(Some("implicit")).unwrap()
        );
        assert_eq!(
            ShareAcknowledgementMode::EXPLICIT,
            ShareAcknowledgementMode::from_string(Some("explicit")).unwrap()
        );
        for value in ["invalid", "IMPLICIT", "EXPLICIT", ""] {
            let error = ShareAcknowledgementMode::from_string(Some(value)).unwrap_err();
            assert!(error.is_local_illegal_argument_error(), "{error:?}");
            assert_eq!(format!("Invalid acknowledgement mode: {value}"), error.message());
        }
        let error = ShareAcknowledgementMode::from_string(None).unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!("Acknowledgement mode is null", error.message());
    }

    #[test]
    fn test_validator() {
        let validator = Validator;
        assert!(validator.ensure_valid("test", Some(&string_value("implicit"))).is_ok());
        assert!(validator.ensure_valid("test", Some(&string_value("explicit"))).is_ok());
        for value in ["invalid", "IMPLICIT", "EXPLICIT", ""] {
            let error = validator.ensure_valid("test", Some(&string_value(value))).unwrap_err();
            assert!(matches!(error, Error::Config(_)), "{error:?}");
            assert_eq!(
                format!(
                    "Invalid value {value} for configuration test: Invalid value `{value}` for configuration test. \
                     The value must either be 'implicit' or 'explicit'."
                ),
                error.message()
            );
        }
        let error = validator.ensure_valid("test", None).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error:?}");
        assert_eq!(
            "Invalid value null for configuration test: Invalid value `null` for configuration test. \
             The value must either be 'implicit' or 'explicit'.",
            error.message()
        );
    }

    #[test]
    fn test_equals_and_hash_code() {
        let mode1 = ShareAcknowledgementMode::IMPLICIT;
        let mode2 = ShareAcknowledgementMode::IMPLICIT;
        let mode3 = ShareAcknowledgementMode::EXPLICIT;

        assert_eq!(mode1, mode2);
        assert_ne!(mode1, mode3);
        assert_ne!(mode2, mode3);

        assert_eq!(hash_code(&mode1), hash_code(&mode2));
        assert_ne!(hash_code(&mode1), hash_code(&mode3));
    }

    // Not in `ShareAcknowledgementModeTest`: pins `name()`, `toString()` and the
    // validator's `toString()`, which Java defines and the Rust translation
    // must keep.
    #[test]
    fn test_name_and_to_string() {
        assert_eq!("implicit", ShareAcknowledgementMode::IMPLICIT.name());
        assert_eq!("explicit", ShareAcknowledgementMode::EXPLICIT.name());
        assert_eq!(
            "ShareAcknowledgementMode{mode=implicit}",
            ShareAcknowledgementMode::IMPLICIT.to_string()
        );
        assert_eq!("[implicit, explicit]", Validator.to_string());
    }
}
