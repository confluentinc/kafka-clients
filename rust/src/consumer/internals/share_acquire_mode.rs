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

//! The acquire mode of a share consumer, `share.acquire.mode`.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.ShareAcquireMode`,
//! with its nested `Validator` class.

use std::fmt;

use crate::common::Error;
use crate::common::config::config_def::{self, ParsedValue, as_string, config_error};

/// How a share consumer acquires records: in whole batches, or limited to
/// `max.poll.records`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode")]
pub(crate) enum ShareAcquireMode {
    BatchOptimized,
    RecordLimit,
}

impl ShareAcquireMode {
    /// Java's `values()`, in declaration order.
    const VALUES: [Self; 2] = [Self::BatchOptimized, Self::RecordLimit];

    /// The configuration name of the mode, Java's `public final String name`
    /// field (not `Enum.name()`, which is the upper-case constant name).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode#name")]
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::BatchOptimized => "batch_optimized",
            Self::RecordLimit => "record_limit",
        }
    }

    /// Java's `Enum.name()`, which `ShareAcquireMode.valueOf` matches.
    fn constant_name(self) -> &'static str {
        match self {
            Self::BatchOptimized => "BATCH_OPTIMIZED",
            Self::RecordLimit => "RECORD_LIMIT",
        }
    }

    /// Case-insensitive acquire mode lookup by string name. `None` is Java's
    /// `null`.
    ///
    /// # Errors
    ///
    /// `Error::LocalIllegalArgument` with `"ShareAcquireMode is null"` for
    /// `None`, and Java's message otherwise, which names the value where the
    /// configuration name would be (`Invalid value `<name>` for configuration
    /// <name>. ...`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode#of")]
    pub(crate) fn of(name: Option<&str>) -> Result<Self, Error> {
        let Some(name) = name else {
            return Err(Error::local_illegal_argument("ShareAcquireMode is null"));
        };
        // Java's `name.toUpperCase(Locale.ROOT)` then `valueOf`, an exact match
        // on the constant name.
        let upper_case = name.to_uppercase();
        Self::VALUES
            .into_iter()
            .find(|mode| mode.constant_name() == upper_case)
            .ok_or_else(|| {
                Error::local_illegal_argument(format!(
                    "Invalid value `{name}` for configuration {name}. The value must either be 'batch_optimized' or \
                 'record_limit'."
                ))
            })
    }

    /// The identifier of the mode.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode#id")]
    pub(crate) fn id(self) -> i8 {
        match self {
            Self::BatchOptimized => 0,
            Self::RecordLimit => 1,
        }
    }

    /// The mode with the identifier `id`.
    ///
    /// # Errors
    ///
    /// `Error::LocalIllegalArgument` with `"Unknown share acquire mode id: <id>"`
    /// if the identifier is not recognized.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode#forId")]
    pub(crate) fn for_id(id: i8) -> Result<Self, Error> {
        match id {
            0 => Ok(Self::BatchOptimized),
            1 => Ok(Self::RecordLimit),
            _ => Err(Error::local_illegal_argument(format!("Unknown share acquire mode id: {id}"))),
        }
    }
}

impl fmt::Display for ShareAcquireMode {
    // Java's `toString()` returns the `name` field.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Java's `ShareAcquireMode.Validator`: the value must be accepted by
/// [`ShareAcquireMode::of`].
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareAcquireMode$Validator")]
pub(crate) struct Validator;

impl config_def::Validator for Validator {
    fn ensure_valid(&self, name: &str, value: Option<&ParsedValue>) -> Result<(), Error> {
        let acquire_mode = as_string(value)?;
        ShareAcquireMode::of(acquire_mode).map(|_| ()).map_err(|_| {
            config_error(
                name,
                value,
                format!(
                    "Invalid value `{}` for configuration {name}. The value must either be 'batch_optimized' or \
                     'record_limit'.",
                    acquire_mode.unwrap_or("null")
                ),
            )
        })
    }
}

impl fmt::Display for Validator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = ShareAcquireMode::VALUES.map(ShareAcquireMode::name);
        write!(f, "[{}]", values.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::config_def::Validator as _;

    fn string_value(value: &str) -> ParsedValue {
        ParsedValue::String(value.to_owned())
    }

    #[test]
    fn test_from_string() {
        assert_eq!(
            ShareAcquireMode::BatchOptimized,
            ShareAcquireMode::of(Some("batch_optimized")).unwrap()
        );
        assert_eq!(
            ShareAcquireMode::BatchOptimized,
            ShareAcquireMode::of(Some("BATCH_OPTIMIZED")).unwrap()
        );
        assert_eq!(
            ShareAcquireMode::RecordLimit,
            ShareAcquireMode::of(Some("record_limit")).unwrap()
        );
        assert_eq!(
            ShareAcquireMode::RecordLimit,
            ShareAcquireMode::of(Some("RECORD_LIMIT")).unwrap()
        );
        for value in ["invalid_mode", ""] {
            let error = ShareAcquireMode::of(Some(value)).unwrap_err();
            assert!(error.is_local_illegal_argument_error(), "{error:?}");
            assert_eq!(
                format!(
                    "Invalid value `{value}` for configuration {value}. The value must either be \
                     'batch_optimized' or 'record_limit'."
                ),
                error.message()
            );
        }
        let error = ShareAcquireMode::of(None).unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!("ShareAcquireMode is null", error.message());
    }

    #[test]
    fn test_validator() {
        let validator = Validator;
        for value in ["batch_optimized", "BATCH_OPTIMIZED", "record_limit", "RECORD_LIMIT"] {
            assert!(validator.ensure_valid("test", Some(&string_value(value))).is_ok(), "{value}");
        }
        for value in ["invalid_mode", ""] {
            let error = validator.ensure_valid("test", Some(&string_value(value))).unwrap_err();
            assert!(matches!(error, Error::Config(_)), "{error:?}");
            assert_eq!(
                format!(
                    "Invalid value {value} for configuration test: Invalid value `{value}` for configuration test. \
                     The value must either be 'batch_optimized' or 'record_limit'."
                ),
                error.message()
            );
        }
        let error = validator.ensure_valid("test", None).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error:?}");
        assert_eq!(
            "Invalid value null for configuration test: Invalid value `null` for configuration test. \
             The value must either be 'batch_optimized' or 'record_limit'.",
            error.message()
        );
    }

    #[test]
    fn test_validator_to_string() {
        assert_eq!("[batch_optimized, record_limit]", Validator.to_string());
    }

    // Not in `ShareAcquireModeTest`: pins `id`, `forId` and `toString`, which
    // Java defines and the share fetch request uses.
    #[test]
    fn test_id_for_id_and_to_string() {
        for (mode, id, name) in [
            (ShareAcquireMode::BatchOptimized, 0, "batch_optimized"),
            (ShareAcquireMode::RecordLimit, 1, "record_limit"),
        ] {
            assert_eq!(id, mode.id());
            assert_eq!(mode, ShareAcquireMode::for_id(id).unwrap());
            assert_eq!(name, mode.to_string());
        }
        let error = ShareAcquireMode::for_id(2).unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!("Unknown share acquire mode id: 2", error.message());
    }
}
