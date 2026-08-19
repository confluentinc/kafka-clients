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

use crate::common::kafka_error::kafka_error_class;

kafka_error_class! {
    /// A configuration value is invalid — wrong type, out of range, or otherwise
    /// unacceptable.
    ///
    /// Corresponds to Java's `ConfigException`, thrown throughout `ConfigDef` and
    /// the `*Config` constructors. It lives in `org.apache.kafka.common.config`,
    /// not `common.errors`, and has no entry in `Errors`, so it carries no
    /// protocol code.
    ///
    /// Java `extends` chain:
    ///    `ConfigException` -> `KafkaException`
    ///
    /// Note it extends `KafkaException` directly, NOT `IllegalArgumentException`:
    /// an invalid configuration is a Kafka error
    /// ([`is_kafka_error`](crate::common::Error::is_kafka_error) is `true`),
    /// unlike a `java.lang.IllegalArgumentException`.
    ConfigError,
    extends: [
        is_kafka_error,
    ],
}

impl ConfigError {
    /// Create a config error naming the offending value and configuration key,
    /// mirroring Java's `ConfigException(String name, Object value)`:
    /// `"Invalid value {value} for configuration {name}"`.
    pub fn with_value(name: impl Display, value: impl Display) -> Self {
        Self::new(format!("Invalid value {value} for configuration {name}"))
    }

    /// Create a config error naming the value, key, and a detail message,
    /// mirroring Java's `ConfigException(String name, Object value, String message)`:
    /// `"Invalid value {value} for configuration {name}: {message}"`.
    pub fn with_value_message(name: impl Display, value: impl Display, message: impl Display) -> Self {
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
    fn message_matches_java_config_exception_format() {
        assert_eq!(
            ConfigError::with_value("group.protocol", "bad").message(),
            "Invalid value bad for configuration group.protocol"
        );
        assert_eq!(
            ConfigError::with_value_message("max.poll.records", 0, "Value must be at least 1").message(),
            "Invalid value 0 for configuration max.poll.records: Value must be at least 1"
        );
    }

    /// `ConfigException extends KafkaException`, so a config error is a Kafka
    /// error — unlike a `java.lang.IllegalArgumentException`. This is the whole
    /// point of routing config validation through `ConfigError` (Critic finding 7).
    #[test]
    fn config_error_is_a_kafka_error() {
        let e = Error::config_value("group.protocol", "bad");
        assert!(e.is_kafka_error());
        assert!(
            !e.is_api_error(),
            "ConfigException extends KafkaException directly, not ApiException"
        );
        assert!(matches!(e, Error::Config(_)));
    }
}
