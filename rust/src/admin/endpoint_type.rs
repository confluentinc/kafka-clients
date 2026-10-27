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

//! The endpoint type of KIP-919.
//!
//! Corresponds to `org.apache.kafka.clients.admin.EndpointType`. A
//! `DescribeCluster` request names the endpoint type it describes, and its
//! response states the endpoint type that answered: the brokers, or the
//! controllers a client reaches through `bootstrap.controllers`.

/// Identifies the endpoint type, as specified by KIP-919.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.EndpointType")]
pub enum EndpointType {
    /// An unknown endpoint type (id `0`), and what [`EndpointType::from_id`]
    /// answers for an id it does not know.
    Unknown,
    /// The brokers (id `1`).
    Broker,
    /// The controllers (id `2`).
    Controller,
}

/// Java's inherited `Enum.toString()`: the constant's name.
impl std::fmt::Display for EndpointType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::Broker => "BROKER",
            Self::Controller => "CONTROLLER",
        })
    }
}

impl EndpointType {
    /// Returns the id of this endpoint type on the wire.
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointType#id")]
    pub fn id(self) -> i8 {
        match self {
            EndpointType::Unknown => 0,
            EndpointType::Broker => 1,
            EndpointType::Controller => 2,
        }
    }

    /// Returns the endpoint type with the given id: [`EndpointType::Broker`]
    /// for `1`, [`EndpointType::Controller`] for `2`, and
    /// [`EndpointType::Unknown`] for any other id.
    ///
    /// # Arguments
    ///
    /// - `id` - The id on the wire, Java's `byte`.
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointType#fromId")]
    pub fn from_id(id: i8) -> EndpointType {
        if id == EndpointType::Broker.id() {
            EndpointType::Broker
        } else if id == EndpointType::Controller.id() {
            EndpointType::Controller
        } else {
            EndpointType::Unknown
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `EndpointTypeTest.testRoundTripBroker`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointTypeTest#testRoundTripBroker")]
    fn test_round_trip_broker() {
        test_round_trip(EndpointType::Broker);
    }

    /// Translated from `EndpointTypeTest.testRoundTripController`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointTypeTest#testRoundTripController")]
    fn test_round_trip_controller() {
        test_round_trip(EndpointType::Controller);
    }

    /// Translated from `EndpointTypeTest.testUnknown`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointTypeTest#testUnknown")]
    fn test_unknown() {
        assert_eq!(EndpointType::Unknown, EndpointType::from_id(0));
        assert_eq!(EndpointType::Unknown, EndpointType::from_id(3));
    }

    /// `EndpointTypeTest.testRoundTrip`.
    fn test_round_trip(r#type: EndpointType) {
        let id = r#type.id();
        assert_eq!(r#type, EndpointType::from_id(id));
    }

    /// New test, no Java original: the wire ids, which `DescribeCluster`
    /// carries, and every other id (negative ones included) is unknown, as
    /// `EndpointType#fromId` maps them.
    #[test]
    fn ids_match_the_wire_protocol() {
        assert_eq!(EndpointType::Unknown.id(), 0);
        assert_eq!(EndpointType::Broker.id(), 1);
        assert_eq!(EndpointType::Controller.id(), 2);
        for id in i8::MIN..=i8::MAX {
            let expected = match id {
                1 => EndpointType::Broker,
                2 => EndpointType::Controller,
                _ => EndpointType::Unknown,
            };
            assert_eq!(EndpointType::from_id(id), expected, "id {id}");
        }
    }

    /// New test, no Java original: `Display` is Java's `Enum.toString()`, the
    /// constant's name, and the order is Java's `Enum.compareTo`, the
    /// declaration order.
    #[test]
    fn display_and_order_match_java() {
        assert_eq!(EndpointType::Unknown.to_string(), "UNKNOWN");
        assert_eq!(EndpointType::Broker.to_string(), "BROKER");
        assert_eq!(EndpointType::Controller.to_string(), "CONTROLLER");
        assert!(EndpointType::Unknown < EndpointType::Broker);
        assert!(EndpointType::Broker < EndpointType::Controller);
    }
}
