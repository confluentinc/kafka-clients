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

//! Translated from `org.apache.kafka.common.internals.UnsupportedProtocolFieldException`
//! (Kafka 4.4, KAFKA-18157).

use std::io;

use crate::common::Error;
use crate::common::errors::{UnsupportedVersionError, UnsupportedVersionKind};

/// Indicates that a request contains a field or field value that is not
/// supported by the API version negotiated with the broker, and that a higher
/// version would be required to use it. This is a more specific subtype of
/// `UnsupportedVersionException` that lets callers distinguish an unsupported
/// field from a wholly unsupported API version.
///
/// Java's class is in `common.internals`, so it is crate-private, and the public
/// `Error` has no variant for it (Milestone-16 PLAN D4): each constructor returns
/// an [`Error::UnsupportedVersion`] whose payload is marked
/// [`UnsupportedVersionKind::UnsupportedProtocolField`]. Java's
/// `instanceof UnsupportedProtocolFieldException` is
/// [`Self::is_unsupported_protocol_field_error`]; everything that tests for the
/// parent class (`Error::is_unsupported_version_error`, the
/// `Errors::UnsupportedVersion` code) answers for it unchanged, as it does in Java.
#[doc(alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldException")]
pub(crate) struct UnsupportedProtocolFieldError;

/// Parameters for [`UnsupportedProtocolFieldError::with_options`].
///
/// No Java counterpart (DoD #7). Java's two constructors,
/// `(String fieldOrValue, String apiKeyName, int apiVersion, int lowestSupportedVersion)`
/// and `(String message)`, share no parameter, so under CLAUDE.md §2 neither keeps
/// a plain name; the four-parameter one is past §2's cap of three, so this struct
/// carries its parameters.
#[non_exhaustive]
pub(crate) struct UnsupportedProtocolFieldErrorOptions<'a> {
    /// The field, or the field value, the negotiated version cannot carry.
    pub field_or_value: &'a str,
    /// The API key's name, as Java's `apiKey().name()` (the enum constant, e.g.
    /// `CREATE_TOPICS`).
    pub api_key_name: &'a str,
    /// The negotiated API version.
    pub api_version: i16,
    /// The lowest API version that supports the field.
    pub lowest_supported_version: i16,
}

/// Fluent builder for [`UnsupportedProtocolFieldErrorOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones. No Java
/// counterpart (DoD #7).
pub(crate) struct UnsupportedProtocolFieldErrorOptionsBuilder<'a> {
    field_or_value: Option<&'a str>,
    api_key_name: Option<&'a str>,
    api_version: Option<i16>,
    lowest_supported_version: Option<i16>,
}

impl Default for UnsupportedProtocolFieldErrorOptionsBuilder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> UnsupportedProtocolFieldErrorOptionsBuilder<'a> {
    /// Creates a builder with every parameter unset.
    pub(crate) fn new() -> Self {
        Self {
            field_or_value: None,
            api_key_name: None,
            api_version: None,
            lowest_supported_version: None,
        }
    }

    /// Sets [`UnsupportedProtocolFieldErrorOptions::field_or_value`].
    pub(crate) fn set_field_or_value(mut self, field_or_value: &'a str) -> Self {
        self.field_or_value = Some(field_or_value);
        self
    }

    /// Sets [`UnsupportedProtocolFieldErrorOptions::api_key_name`].
    pub(crate) fn set_api_key_name(mut self, api_key_name: &'a str) -> Self {
        self.api_key_name = Some(api_key_name);
        self
    }

    /// Sets [`UnsupportedProtocolFieldErrorOptions::api_version`].
    pub(crate) fn set_api_version(mut self, api_version: i16) -> Self {
        self.api_version = Some(api_version);
        self
    }

    /// Sets [`UnsupportedProtocolFieldErrorOptions::lowest_supported_version`].
    pub(crate) fn set_lowest_supported_version(mut self, lowest_supported_version: i16) -> Self {
        self.lowest_supported_version = Some(lowest_supported_version);
        self
    }

    /// Returns the built options. All four parameters are mandatory: Java's
    /// constructor takes them all.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter that was
    /// not set.
    pub(crate) fn build(self) -> Result<UnsupportedProtocolFieldErrorOptions<'a>, Error> {
        Ok(UnsupportedProtocolFieldErrorOptions {
            field_or_value: self.field_or_value.ok_or_else(|| Self::missing("field_or_value"))?,
            api_key_name: self.api_key_name.ok_or_else(|| Self::missing("api_key_name"))?,
            api_version: self.api_version.ok_or_else(|| Self::missing("api_version"))?,
            lowest_supported_version: self
                .lowest_supported_version
                .ok_or_else(|| Self::missing("lowest_supported_version"))?,
        })
    }

    /// Builds the options and returns [`UnsupportedProtocolFieldError::with_options`]
    /// for them; a parameter left unset yields the [`Self::build`] error instead.
    ///
    /// No Java counterpart: it folds the builder's `Result` into the error every
    /// call site is about to return anyway.
    pub(crate) fn into_error(self) -> Error {
        match self.build() {
            Ok(options) => UnsupportedProtocolFieldError::with_options(options),
            Err(e) => e,
        }
    }

    /// [`Self::into_error`] as the `io::Error` a request builder returns; see
    /// [`UnsupportedProtocolFieldError::io_error_with_options`].
    pub(crate) fn into_io_error(self) -> io::Error {
        match self.build() {
            Ok(options) => UnsupportedProtocolFieldError::io_error_with_options(options),
            Err(e) => io::Error::other(e),
        }
    }

    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "UnsupportedProtocolFieldErrorOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl UnsupportedProtocolFieldError {
    /// Java's `UnsupportedProtocolFieldException(String fieldOrValue, String apiKeyName,
    /// int apiVersion, int lowestSupportedVersion)`: names the field and the
    /// version that would support it.
    #[doc(
        alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldException#UnsupportedProtocolFieldException"
    )]
    pub(crate) fn with_options(options: UnsupportedProtocolFieldErrorOptions<'_>) -> Error {
        Error::UnsupportedVersion(Self::payload(Self::field_message(options)))
    }

    /// Java's `UnsupportedProtocolFieldException(String message)`.
    ///
    /// Its one production caller, `ConsumerGroupHeartbeatRequest.Builder`, needs
    /// the `io::Error` form ([`Self::io_error_with_message`]), so this form is
    /// reached only from tests.
    #[cfg_attr(not(test), expect(dead_code))]
    #[doc(
        alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldException#UnsupportedProtocolFieldException"
    )]
    pub(crate) fn with_message(message: impl Into<String>) -> Error {
        Error::UnsupportedVersion(Self::payload(message))
    }

    /// Java's `exception instanceof UnsupportedProtocolFieldException`.
    pub(crate) fn is_unsupported_protocol_field_error(error: &Error) -> bool {
        matches!(error, Error::UnsupportedVersion(e) if e.kind() == UnsupportedVersionKind::UnsupportedProtocolField)
    }

    /// [`Self::with_options`] as the `io::Error` a request builder returns. Java's
    /// builders throw the exception from `build(version)`; the Rust builder trait
    /// reports through `io::Error`, which carries the object (see
    /// [`UnsupportedVersionError::into_io_error`]).
    pub(crate) fn io_error_with_options(options: UnsupportedProtocolFieldErrorOptions<'_>) -> io::Error {
        Self::payload(Self::field_message(options)).into_io_error()
    }

    /// [`Self::with_message`] as the `io::Error` a request builder returns; see
    /// [`Self::io_error_with_options`].
    pub(crate) fn io_error_with_message(message: impl Into<String>) -> io::Error {
        Self::payload(message).into_io_error()
    }

    /// The message Java's four-parameter constructor passes to `super(..)`.
    fn field_message(options: UnsupportedProtocolFieldErrorOptions<'_>) -> String {
        let UnsupportedProtocolFieldErrorOptions {
            field_or_value,
            api_key_name,
            api_version,
            lowest_supported_version,
        } = options;
        format!(
            "The cluster does not support [{field_or_value}] in {api_key_name} API version {api_version}. \
             Upgrade the cluster to {api_key_name} API version >= {lowest_supported_version} to enable \
             [{field_or_value}]."
        )
    }

    fn payload(message: impl Into<String>) -> UnsupportedVersionError {
        UnsupportedVersionError::new(message).with_kind(UnsupportedVersionKind::UnsupportedProtocolField)
    }
}

/// Asserts that a request builder failed with Java's
/// `UnsupportedProtocolFieldException` carrying exactly `expected_message`.
#[cfg(test)]
pub(crate) fn assert_unsupported_protocol_field(error: &io::Error, expected_message: &str) {
    let recovered = Error::UnsupportedVersion(UnsupportedVersionError::from_io_error(error));
    assert!(
        UnsupportedProtocolFieldError::is_unsupported_protocol_field_error(&recovered),
        "expected the UnsupportedProtocolField kind, got {recovered:?}"
    );
    assert_eq!(expected_message, recovered.message());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options<'a>(
        field_or_value: &'a str,
        api_key_name: &'a str,
        api_version: i16,
        lowest_supported_version: i16,
    ) -> UnsupportedProtocolFieldErrorOptions<'a> {
        UnsupportedProtocolFieldErrorOptionsBuilder::new()
            .set_field_or_value(field_or_value)
            .set_api_key_name(api_key_name)
            .set_api_version(api_version)
            .set_lowest_supported_version(lowest_supported_version)
            .build()
            .unwrap()
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldExceptionTest#testFieldConstructorFormatsMessage"
    )]
    fn test_field_constructor_formats_message() {
        let error = UnsupportedProtocolFieldError::with_options(options("validateOnly", "CREATE_TOPICS", 0, 1));
        assert_eq!(
            "The cluster does not support [validateOnly] in CREATE_TOPICS API version 0. \
             Upgrade the cluster to CREATE_TOPICS API version >= 1 to enable [validateOnly].",
            error.message()
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldExceptionTest#testMessageConstructorPassesMessageThrough"
    )]
    fn test_message_constructor_passes_message_through() {
        let error = UnsupportedProtocolFieldError::with_message("some custom message");
        assert_eq!("some custom message", error.message());
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.internals.UnsupportedProtocolFieldExceptionTest#testIsUnsupportedVersionException"
    )]
    fn test_is_unsupported_version_error() {
        let error = UnsupportedProtocolFieldError::with_options(options("field", "SOME_API", 0, 1));
        assert!(error.is_unsupported_version_error());
        assert!(matches!(error, Error::UnsupportedVersion(_)));
        assert!(UnsupportedProtocolFieldError::is_unsupported_protocol_field_error(&error));
        // The parent class is not an instance of the subclass.
        assert!(!UnsupportedProtocolFieldError::is_unsupported_protocol_field_error(
            &Error::unsupported_version("field")
        ));
    }

    #[test]
    fn test_options_builder_requires_every_parameter() {
        let error = UnsupportedProtocolFieldErrorOptionsBuilder::new()
            .set_field_or_value("f")
            .set_api_key_name("API")
            .set_api_version(0)
            .build()
            .err()
            .unwrap();
        assert_eq!(
            "UnsupportedProtocolFieldErrorOptionsBuilder::build: mandatory parameter `lowest_supported_version` was not set",
            error.message()
        );
    }

    #[test]
    fn test_io_error_keeps_the_subclass() {
        let io = UnsupportedProtocolFieldError::io_error_with_options(options("f", "API", 0, 1));
        let recovered = Error::UnsupportedVersion(UnsupportedVersionError::from_io_error(&io));
        assert!(UnsupportedProtocolFieldError::is_unsupported_protocol_field_error(&recovered));
    }
}
