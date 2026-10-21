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
//! Corresponds to `org.apache.kafka.clients.admin.EndpointType`.

/// Identifies the endpoint type, as specified by KIP-919.
///
/// A `DescribeCluster` request names the endpoint type it describes, and its
/// response states the endpoint type that answered: brokers, or the
/// controllers a client reaches through `bootstrap.controllers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.EndpointType")]
pub enum EndpointType {
    /// An unknown endpoint type (id `0`), and any id this client does not know.
    Unknown,
    /// The brokers (id `1`).
    Broker,
    /// The controllers (id `2`).
    Controller,
}

impl EndpointType {
    /// The id of this endpoint type on the wire.
    ///
    /// Mirrors `EndpointType.id()`.
    #[doc(alias = "org.apache.kafka.clients.admin.EndpointType#id")]
    pub fn id(self) -> i8 {
        match self {
            EndpointType::Unknown => 0,
            EndpointType::Broker => 1,
            EndpointType::Controller => 2,
        }
    }

    /// The endpoint type with the given id, or [`EndpointType::Unknown`] for
    /// any id other than those of [`EndpointType::Broker`] and
    /// [`EndpointType::Controller`].
    ///
    /// Mirrors `EndpointType.fromId(byte)`.
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

    /// Beyond Java's test: the wire ids, which `DescribeCluster` carries, and
    /// that every id other than 1 and 2 (negative ones included) is unknown.
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
}
