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
    /// Returns `None` if the name is not recognized.
    pub fn for_name(name: &str) -> Option<Self> {
        match name {
            "NoTimestampType" => Some(Self::NoTimestampType),
            "CreateTime" => Some(Self::CreateTime),
            "LogAppendTime" => Some(Self::LogAppendTime),
            _ => None,
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
        assert_eq!(TimestampType::for_name("NoTimestampType"), Some(TimestampType::NoTimestampType));
        assert_eq!(TimestampType::for_name("CreateTime"), Some(TimestampType::CreateTime));
        assert_eq!(TimestampType::for_name("LogAppendTime"), Some(TimestampType::LogAppendTime));
        assert_eq!(TimestampType::for_name("Unknown"), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", TimestampType::CreateTime), "CreateTime");
        assert_eq!(format!("{}", TimestampType::LogAppendTime), "LogAppendTime");
        assert_eq!(format!("{}", TimestampType::NoTimestampType), "NoTimestampType");
    }
}
