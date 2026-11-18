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

//! The type of a share-group record acknowledgement.
//!
//! Translated from `org.apache.kafka.clients.consumer.AcknowledgeType`.

use std::fmt;

use crate::common::Error;

/// The acknowledge type is used with `KafkaShareConsumer::acknowledge` to
/// indicate whether the record was consumed successfully.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.consumer.AcknowledgeType")]
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
    /// The unique identifier for this acknowledge type.
    ///
    /// # Returns
    ///
    /// `1` for [`Accept`](Self::Accept), `2` for [`Release`](Self::Release),
    /// `3` for [`Reject`](Self::Reject), `4` for [`Renew`](Self::Renew).
    // Java's `public final byte id` field; a getter per CLAUDE.md §3.
    #[doc(alias = "org.apache.kafka.clients.consumer.AcknowledgeType#id")]
    pub fn id(self) -> i8 {
        match self {
            Self::Accept => 1,
            Self::Release => 2,
            Self::Reject => 3,
            Self::Renew => 4,
        }
    }

    /// Returns the AcknowledgeType for the given identifier.
    ///
    /// # Arguments
    ///
    /// - `id` - The identifier for the acknowledge type
    ///
    /// # Returns
    ///
    /// The corresponding AcknowledgeType
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` If the ID is not recognized
    #[doc(alias = "org.apache.kafka.clients.consumer.AcknowledgeType#forId")]
    pub fn for_id(id: i8) -> Result<Self, Error> {
        match id {
            1 => Ok(Self::Accept),
            2 => Ok(Self::Release),
            3 => Ok(Self::Reject),
            4 => Ok(Self::Renew),
            _ => Err(Error::local_illegal_argument(format!("Unknown acknowledge type id: {id}"))),
        }
    }
}

impl fmt::Display for AcknowledgeType {
    // Java's `toString()`: the constant's name in lower case.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Accept => "accept",
            Self::Release => "release",
            Self::Reject => "reject",
            Self::Renew => "renew",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Java has no `AcknowledgeTypeTest`; these pin the ids, `forId` and
    // `toString` that the share request wire format and logs depend on.

    #[test]
    fn test_id_and_for_id_round_trip() {
        for (acknowledge_type, id) in [
            (AcknowledgeType::Accept, 1),
            (AcknowledgeType::Release, 2),
            (AcknowledgeType::Reject, 3),
            (AcknowledgeType::Renew, 4),
        ] {
            assert_eq!(acknowledge_type.id(), id);
            assert_eq!(AcknowledgeType::for_id(id).unwrap(), acknowledge_type);
        }
    }

    #[test]
    fn test_for_id_unknown() {
        for id in [0, 5, -1] {
            let error = AcknowledgeType::for_id(id).unwrap_err();
            assert!(error.is_local_illegal_argument_error(), "{error:?}");
            assert_eq!(error.message(), format!("Unknown acknowledge type id: {id}"));
        }
    }

    #[test]
    fn test_display_is_lower_case() {
        assert_eq!(AcknowledgeType::Accept.to_string(), "accept");
        assert_eq!(AcknowledgeType::Release.to_string(), "release");
        assert_eq!(AcknowledgeType::Reject.to_string(), "reject");
        assert_eq!(AcknowledgeType::Renew.to_string(), "renew");
    }
}
