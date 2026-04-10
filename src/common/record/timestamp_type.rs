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
//! Translated from `org.apache.kafka.common.record.TimestampType`.

use std::fmt;

/// The timestamp type of the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampType {
    /// No timestamp type set (magic v0).
    NoTimestampType,
    /// The timestamp was set by the producer (create time).
    CreateTime,
    /// The timestamp was set by the broker (log append time).
    LogAppendTime,
}

impl TimestampType {
    /// Returns the numeric id of this timestamp type.
    pub fn id(self) -> i32 {
        match self {
            TimestampType::NoTimestampType => -1,
            TimestampType::CreateTime => 0,
            TimestampType::LogAppendTime => 1,
        }
    }

    /// Returns the name of this timestamp type.
    pub fn name(self) -> &'static str {
        match self {
            TimestampType::NoTimestampType => "NoTimestampType",
            TimestampType::CreateTime => "CreateTime",
            TimestampType::LogAppendTime => "LogAppendTime",
        }
    }

    /// Look up a timestamp type by name.
    ///
    /// # Errors
    /// Returns an error if the name doesn't match any known timestamp type.
    pub fn for_name(name: &str) -> Result<TimestampType, String> {
        match name {
            "NoTimestampType" => Ok(TimestampType::NoTimestampType),
            "CreateTime" => Ok(TimestampType::CreateTime),
            "LogAppendTime" => Ok(TimestampType::LogAppendTime),
            _ => Err(format!("Invalid timestamp type {}", name)),
        }
    }
}

impl fmt::Display for TimestampType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    fn test_for_name() {
        assert_eq!(TimestampType::for_name("CreateTime").unwrap(), TimestampType::CreateTime);
        assert_eq!(TimestampType::for_name("LogAppendTime").unwrap(), TimestampType::LogAppendTime);
        assert_eq!(
            TimestampType::for_name("NoTimestampType").unwrap(),
            TimestampType::NoTimestampType
        );
        assert!(TimestampType::for_name("invalid").is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", TimestampType::CreateTime), "CreateTime");
        assert_eq!(format!("{}", TimestampType::LogAppendTime), "LogAppendTime");
    }
}
