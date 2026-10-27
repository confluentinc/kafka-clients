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

//! Translated from `org.apache.kafka.common.config.ConfigException`.

use std::fmt::Display;

use crate::common::error::kafka_error_type;

kafka_error_type! {
    /// Thrown if the user supplies an invalid configuration
    ///
    /// # Translation notes
    ///
    /// No protocol code.
    #[doc(alias = "org.apache.kafka.common.config.ConfigException")]
    ConfigError,
    extends: [
        is_kafka_error,
        is_config_error,
    ],
}

impl ConfigError {
    // Java's three `ConfigException` constructors — `ConfigException(String)`,
    // `ConfigException(String, Object)` and
    // `ConfigException(String, Object, String)` — have an EMPTY
    // parameter-name intersection, and no constructor is no-arg. So under
    // CLAUDE.md §2 nobody keeps the plain name, and the two below take their
    // full parameter lists as the suffix.
    //
    // The `(String message)` form is the exception, and deliberately so: it is
    // generated as `new` by `kafka_error_type!` above, whose single body
    // declares 141 classes. Renaming it here would mean threading a per-class
    // constructor name through the macro so that exactly one of the 141 spells
    // it `with_message` — leaving `SomeError::new("msg")` valid or not depending
    // on a class the call site cannot see. The macro's uniformity is worth more
    // than this one suffix; see the note on `message_only_error!` for the same
    // trade-off.

    /// Creates a config error naming the offending value and configuration key:
    /// `"Invalid value {value} for configuration {name}"`.
    ///
    /// # Arguments
    ///
    /// - `name` - the configuration key
    /// - `value` - the invalid value
    #[doc(alias = "org.apache.kafka.common.config.ConfigException#ConfigException")]
    pub fn with_name_value(name: impl Display, value: impl Display) -> Self {
        Self::new(format!("Invalid value {value} for configuration {name}"))
    }

    /// Creates a config error naming the value, key, and a detail message:
    /// `"Invalid value {value} for configuration {name}: {message}"`.
    ///
    /// # Arguments
    ///
    /// - `name` - the configuration key
    /// - `value` - the invalid value
    /// - `message` - why the value is invalid
    #[doc(alias = "org.apache.kafka.common.config.ConfigException#ConfigException")]
    pub fn with_name_value_message(name: impl Display, value: impl Display, message: impl Display) -> Self {
        Self::new(format!("Invalid value {value} for configuration {name}: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigError;
    use crate::common::Error;

    /// The two structured constructors reproduce Java's
    /// `ConfigException(name, value)` and `ConfigException(name, value, message)`
    /// message formats byte-for-byte.
    #[test]
    fn message_matches_java_config_error_format() {
        assert_eq!(
            ConfigError::with_name_value("group.protocol", "bad").message(),
            "Invalid value bad for configuration group.protocol"
        );
        assert_eq!(
            ConfigError::with_name_value_message("max.poll.records", 0, "Value must be at least 1").message(),
            "Invalid value 0 for configuration max.poll.records: Value must be at least 1"
        );
    }

    /// `ConfigException extends KafkaException`, so a config error is a Kafka
    /// error — unlike a `java.lang.IllegalArgumentException`. This is the whole
    /// point of routing config validation through `ConfigError` (Critic finding 7).
    #[test]
    fn config_error_is_a_kafka_error() {
        let e = Error::config_name_value("group.protocol", "bad");
        assert!(e.is_kafka_error());
        assert!(!e.is_api_error(), "a config error is a Kafka error directly, not an API error");
        assert!(matches!(e, Error::Config(_)));
    }
}
