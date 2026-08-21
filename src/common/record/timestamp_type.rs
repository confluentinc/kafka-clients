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

//! The timestamp type of the records.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.TimestampType`.

use crate::common::Error;

/// The timestamp type of the records.
///
/// Corresponds to Java's `org.apache.kafka.common.record.TimestampType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimestampType {
    /// No timestamp type (magic v0).
    NoTimestampType = -1,
    /// Timestamp set by the producer at creation time.
    CreateTime = 0,
    /// Timestamp set by the broker when appending to the log.
    LogAppendTime = 1,
}

impl TimestampType {
    /// Returns the numeric ID for this timestamp type.
    pub fn id(self) -> i32 {
        self as i32
    }

    /// Returns the name of this timestamp type.
    pub fn name(self) -> &'static str {
        match self {
            Self::NoTimestampType => "NoTimestampType",
            Self::CreateTime => "CreateTime",
            Self::LogAppendTime => "LogAppendTime",
        }
    }

    /// Look up a `TimestampType` by name.
    ///
    /// # Errors
    ///
    /// Returns a `Error` if the name is not recognized, matching Java's
    /// `NoSuchElementException` thrown by `TimestampType.forName()`.
    pub fn for_name(name: &str) -> Result<Self, Error> {
        match name {
            "NoTimestampType" => Ok(Self::NoTimestampType),
            "CreateTime" => Ok(Self::CreateTime),
            "LogAppendTime" => Ok(Self::LogAppendTime),
            // Java: `throw new NoSuchElementException("Invalid timestamp type " + name)`
            // (`TimestampType.java:39`). `NoSuchElementException` is a plain
            // `java.util` `RuntimeException`, so — exactly like
            // `IllegalArgumentException` — it sits OUTSIDE the `KafkaException`
            // hierarchy: `is_kafka_error()` and `is_api_error()` both answer
            // `false`. The crate has no `NoSuchElement` variant, and the two Java
            // classes are indistinguishable through every §10.4 predicate, so
            // `IllegalArgument` is the faithful carrier here;
            // `Error::with_message(Errors::UnknownServerError, ..)` was not,
            // because it resolves the code to `UnknownServerException`.
            _ => Err(Error::illegal_argument(format!("Invalid timestamp type {name}"))),
        }
    }
}

impl std::fmt::Display for TimestampType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ids() {
        assert_eq!(TimestampType::NoTimestampType.id(), -1);
        assert_eq!(TimestampType::CreateTime.id(), 0);
        assert_eq!(TimestampType::LogAppendTime.id(), 1);
    }

    #[test]
    fn test_names() {
        assert_eq!(TimestampType::NoTimestampType.name(), "NoTimestampType");
        assert_eq!(TimestampType::CreateTime.name(), "CreateTime");
        assert_eq!(TimestampType::LogAppendTime.name(), "LogAppendTime");
    }

    #[test]
    fn test_for_name() {
        assert_eq!(
            TimestampType::for_name("NoTimestampType").unwrap(),
            TimestampType::NoTimestampType
        );
        assert_eq!(TimestampType::for_name("CreateTime").unwrap(), TimestampType::CreateTime);
        assert_eq!(TimestampType::for_name("LogAppendTime").unwrap(), TimestampType::LogAppendTime);
    }

    #[test]
    fn test_for_name_unknown() {
        let err = TimestampType::for_name("Unknown").unwrap_err();
        // Java: `new NoSuchElementException("Invalid timestamp type " + name)`
        // (`TimestampType.java:39`).
        assert_eq!(err.message(), "Invalid timestamp type Unknown");
        // `NoSuchElementException` is outside the `KafkaException` hierarchy, so
        // both predicates must answer `false`.
        assert!(!err.is_kafka_error(), "Java's NoSuchElementException is not a KafkaException");
        assert!(!err.is_api_error(), "Java's NoSuchElementException is not an ApiException");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", TimestampType::CreateTime), "CreateTime");
        assert_eq!(format!("{}", TimestampType::LogAppendTime), "LogAppendTime");
        assert_eq!(format!("{}", TimestampType::NoTimestampType), "NoTimestampType");
    }
}
