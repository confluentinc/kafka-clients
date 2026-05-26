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

//! Translation of `org.apache.kafka.common.record.TimestampType`.

use std::fmt;

use crate::common::errors::KafkaError;

/// The timestamp type of the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampType {
    NoTimestampType,
    CreateTime,
    LogAppendTime,
}

impl TimestampType {
    /// Numeric id stored on the wire (matches Java's `id` field).
    pub fn id(&self) -> i32 {
        match self {
            TimestampType::NoTimestampType => -1,
            TimestampType::CreateTime => 0,
            TimestampType::LogAppendTime => 1,
        }
    }

    /// Camel-case name used by the Java client and the metrics layer (matches
    /// Java's `name` field).
    pub fn name(&self) -> &'static str {
        match self {
            TimestampType::NoTimestampType => "NoTimestampType",
            TimestampType::CreateTime => "CreateTime",
            TimestampType::LogAppendTime => "LogAppendTime",
        }
    }

    /// Look up a `TimestampType` by its camel-case name.
    ///
    /// Mirrors Java's `TimestampType.forName(String)`. Java throws
    /// `NoSuchElementException`; we surface that as
    /// [`KafkaError::InvalidRequest`].
    pub fn for_name(name: &str) -> Result<TimestampType, KafkaError> {
        match name {
            "NoTimestampType" => Ok(TimestampType::NoTimestampType),
            "CreateTime" => Ok(TimestampType::CreateTime),
            "LogAppendTime" => Ok(TimestampType::LogAppendTime),
            other => Err(KafkaError::InvalidRequest(format!("Invalid timestamp type {other}"))),
        }
    }
}

impl fmt::Display for TimestampType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_match_wire_values() {
        assert_eq!(TimestampType::NoTimestampType.id(), -1);
        assert_eq!(TimestampType::CreateTime.id(), 0);
        assert_eq!(TimestampType::LogAppendTime.id(), 1);
    }

    #[test]
    fn names_match_java() {
        assert_eq!(TimestampType::NoTimestampType.name(), "NoTimestampType");
        assert_eq!(TimestampType::CreateTime.name(), "CreateTime");
        assert_eq!(TimestampType::LogAppendTime.name(), "LogAppendTime");
    }

    #[test]
    fn for_name_round_trips() {
        for t in [
            TimestampType::NoTimestampType,
            TimestampType::CreateTime,
            TimestampType::LogAppendTime,
        ] {
            assert_eq!(TimestampType::for_name(t.name()).unwrap(), t);
        }
    }

    #[test]
    fn for_name_unknown_is_error() {
        let err = TimestampType::for_name("Bogus").unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRequest(_)));
        assert!(err.to_string().contains("Invalid timestamp type Bogus"));
    }

    #[test]
    fn display_uses_name() {
        assert_eq!(TimestampType::CreateTime.to_string(), "CreateTime");
    }
}
