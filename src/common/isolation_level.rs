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

//! Transaction isolation level used by the consumer fetch path.
//!
//! Translated from `org.apache.kafka.common.IsolationLevel`.

use std::fmt;

use crate::common::KafkaError;

/// Isolation level used to control which records are visible to a consumer
/// when reading from a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Read records including those from in-flight (uncommitted) transactions.
    ReadUncommitted,
    /// Skip records from in-flight (uncommitted) transactions and only read
    /// records up to the last stable offset.
    ReadCommitted,
}

impl IsolationLevel {
    /// Returns the wire-protocol id used to encode this isolation level.
    ///
    /// Translated from Java's `byte id()`.
    pub fn id(&self) -> u8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }

    /// Returns the [`IsolationLevel`] for the given wire-protocol id.
    ///
    /// Translated from Java's `forId(byte id)`. Returns
    /// [`KafkaError::IllegalArgument`] for unknown ids; the Java implementation
    /// throws `IllegalArgumentException`.
    pub fn for_id(id: u8) -> Result<Self, KafkaError> {
        match id {
            0 => Ok(Self::ReadUncommitted),
            1 => Ok(Self::ReadCommitted),
            _ => Err(KafkaError::illegal_argument(format!("Unknown isolation level {id}"))),
        }
    }
}

impl fmt::Display for IsolationLevel {
    /// Matches Java's `toString()` which lower-cases the enum name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ReadUncommitted => "read_uncommitted",
            Self::ReadCommitted => "read_committed",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_id() {
        assert_eq!(IsolationLevel::for_id(0).unwrap(), IsolationLevel::ReadUncommitted);
        assert_eq!(IsolationLevel::for_id(1).unwrap(), IsolationLevel::ReadCommitted);
        assert!(IsolationLevel::for_id(2).is_err());
    }

    #[test]
    fn test_id() {
        assert_eq!(IsolationLevel::ReadUncommitted.id(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.id(), 1);
    }

    #[test]
    fn test_to_string() {
        assert_eq!(IsolationLevel::ReadUncommitted.to_string(), "read_uncommitted");
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "read_committed");
    }
}
