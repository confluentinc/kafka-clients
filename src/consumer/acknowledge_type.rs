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

//! Acknowledge type for share-group record consumption (KIP-932).
//!
//! Corresponds to `org.apache.kafka.clients.consumer.AcknowledgeType`.

use crate::common::KafkaError;

/// The acknowledge type is used with `KafkaShareConsumer::acknowledge` to
/// indicate whether the record was consumed successfully.
///
/// Corresponds to `org.apache.kafka.clients.consumer.AcknowledgeType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AcknowledgeType {
    /// The record was consumed successfully.
    Accept,
    /// The record was not consumed successfully. Release it for another
    /// delivery attempt.
    Release,
    /// The record was not consumed successfully. Reject it and do not release
    /// it for another delivery attempt.
    Reject,
    /// The record is still being processed. Renew the acquisition lock so
    /// processing can continue.
    Renew,
}

impl AcknowledgeType {
    /// Returns the wire id for this acknowledge type.
    ///
    /// Corresponds to the Java `public final byte id` field.
    pub fn id(&self) -> i8 {
        match self {
            Self::Accept => 1,
            Self::Release => 2,
            Self::Reject => 3,
            Self::Renew => 4,
        }
    }

    /// Returns the acknowledge type for the given wire id.
    ///
    /// Corresponds to Java's `AcknowledgeType.forId(byte id)`.
    ///
    /// # Errors
    ///
    /// Returns an [`KafkaError::IllegalArgument`] if the id is unknown, matching
    /// Java's `IllegalArgumentException`.
    pub fn for_id(id: i8) -> Result<Self, KafkaError> {
        match id {
            1 => Ok(Self::Accept),
            2 => Ok(Self::Release),
            3 => Ok(Self::Reject),
            4 => Ok(Self::Renew),
            other => Err(KafkaError::illegal_argument(format!("Unknown acknowledge type id: {other}"))),
        }
    }
}

impl std::fmt::Display for AcknowledgeType {
    /// Matches Java's `toString()`, which lowercases the enum name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Accept => "accept",
            Self::Release => "release",
            Self::Reject => "reject",
            Self::Renew => "renew",
        };
        write!(f, "{s}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id() {
        assert_eq!(AcknowledgeType::Accept.id(), 1);
        assert_eq!(AcknowledgeType::Release.id(), 2);
        assert_eq!(AcknowledgeType::Reject.id(), 3);
        assert_eq!(AcknowledgeType::Renew.id(), 4);
    }

    #[test]
    fn test_for_id_round_trip() {
        for t in [
            AcknowledgeType::Accept,
            AcknowledgeType::Release,
            AcknowledgeType::Reject,
            AcknowledgeType::Renew,
        ] {
            assert_eq!(AcknowledgeType::for_id(t.id()).unwrap(), t);
        }
    }

    #[test]
    fn test_for_id_unknown() {
        let err = AcknowledgeType::for_id(0).expect_err("id 0 must be rejected");
        assert!(err.to_string().contains("Unknown acknowledge type id: 0"), "got: {err}");
    }

    #[test]
    fn test_to_string_is_lowercase() {
        assert_eq!(AcknowledgeType::Accept.to_string(), "accept");
        assert_eq!(AcknowledgeType::Renew.to_string(), "renew");
    }
}
