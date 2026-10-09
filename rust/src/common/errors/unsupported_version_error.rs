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

//! Translated from `org.apache.kafka.common.errors.UnsupportedVersionException`.

use std::io;

use crate::common::error::kafka_error_type;
use crate::common::protocol::Errors;

kafka_error_type! {
    /// The version of API is not supported.
    ///
    /// Corresponds to Java's `UnsupportedVersionException`, error code `Errors::UnsupportedVersion`.
    ///
    /// Java `extends` chain:
    ///    `UnsupportedVersionException` -> `InvalidConfigurationException` ->
    ///   `ApiException` -> `KafkaException`
    ///
    /// Java's crate-private subclass `common.internals.UnsupportedProtocolFieldException`
    /// (Kafka 4.4) is this same type with a different crate-private kind, so the
    /// public `Error` surface keeps the one variant Java's public API shows.
    #[doc(alias = "org.apache.kafka.common.errors.UnsupportedVersionException")]
    UnsupportedVersionError,
    code: Errors::UnsupportedVersion,
    kind: UnsupportedVersionKind,
    extends: [
        is_kafka_error,
        is_api_error,
        is_invalid_configuration_error,
        is_unsupported_version_error,
    ],
}

/// Which Java class an [`UnsupportedVersionError`] stands for.
///
/// No Java counterpart (DoD #7): Rust's flat `Error` cannot name a crate-private
/// subclass as a variant of its own without making it public, so the subclass is
/// recorded here instead and tested where Java writes `instanceof`
/// (Milestone-16 PLAN D4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum UnsupportedVersionKind {
    /// Java's `UnsupportedVersionException` itself.
    #[default]
    UnsupportedVersion,
    /// Java's `common.internals.UnsupportedProtocolFieldException`: the request
    /// carries a field or value the negotiated version cannot express.
    UnsupportedProtocolField,
}

impl UnsupportedVersionError {
    /// Wraps this error as the [`io::Error`] a `RequestBuilder::build_version`
    /// returns, so that `NetworkClient` can recover the object, and with it the
    /// kind, through [`Self::from_io_error`].
    ///
    /// Java's builders throw the exception object and `NetworkClient.send`
    /// catches it whole (`NetworkClient.java:648-663`); the Rust builder trait
    /// reports through `io::Error`, so the object rides inside it.
    pub(crate) fn into_io_error(self) -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, self)
    }

    /// Recovers the error a request builder or serializer failed with.
    ///
    /// Returns the [`UnsupportedVersionError`] carried by [`Self::into_io_error`]
    /// when there is one, and otherwise a plain `UnsupportedVersionError` whose
    /// message is the `io::Error`'s text: that is how the generated `write`
    /// reports a field it cannot encode.
    pub(crate) fn from_io_error(error: &io::Error) -> Self {
        match error.get_ref().and_then(|inner| inner.downcast_ref::<Self>()) {
            Some(carried) => carried.clone(),
            None => Self::new(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_round_trip_keeps_message_and_kind() {
        let error = UnsupportedVersionError::new("field not supported")
            .with_kind(UnsupportedVersionKind::UnsupportedProtocolField)
            .into_io_error();
        assert_eq!(io::ErrorKind::Unsupported, error.kind());
        let recovered = UnsupportedVersionError::from_io_error(&error);
        assert_eq!("field not supported", recovered.message());
        assert_eq!(UnsupportedVersionKind::UnsupportedProtocolField, recovered.kind());
    }

    #[test]
    fn test_from_plain_io_error_is_the_parent_class() {
        let error = io::Error::new(io::ErrorKind::Unsupported, "Attempted to write a non-default x at version 1");
        let recovered = UnsupportedVersionError::from_io_error(&error);
        assert_eq!("Attempted to write a non-default x at version 1", recovered.message());
        assert_eq!(UnsupportedVersionKind::UnsupportedVersion, recovered.kind());
    }
}
