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

//! A resource that has configs.
//!
//! Corresponds to `org.apache.kafka.common.config.ConfigResource`.

/// Type of a [`ConfigResource`].
///
/// Corresponds to `ConfigResource.Type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.config.ConfigResource$Type")]
pub enum Type {
    /// A consumer group.
    Group,
    /// A client-metrics subscription.
    ClientMetrics,
    /// A broker logger.
    BrokerLogger,
    /// A broker.
    Broker,
    /// A topic.
    Topic,
    /// An unknown resource type.
    Unknown,
}

impl Type {
    /// Returns the wire id for this resource type.
    ///
    /// Corresponds to `ConfigResource.Type.id()`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource$Type#id")]
    pub fn id(&self) -> i8 {
        match self {
            Type::Group => 32,
            Type::ClientMetrics => 16,
            Type::BrokerLogger => 8,
            Type::Broker => 4,
            Type::Topic => 2,
            Type::Unknown => 0,
        }
    }

    /// Returns the resource type for the given wire id, or
    /// [`Type::Unknown`] if the id is unrecognized.
    ///
    /// Corresponds to `ConfigResource.Type.forId(byte)`.
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource$Type#forId")]
    pub fn for_id(id: i8) -> Type {
        match id {
            32 => Type::Group,
            16 => Type::ClientMetrics,
            8 => Type::BrokerLogger,
            4 => Type::Broker,
            2 => Type::Topic,
            _ => Type::Unknown,
        }
    }
}

/// A class representing resources that have configs.
///
/// Corresponds to `org.apache.kafka.common.config.ConfigResource`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.common.config.ConfigResource")]
pub struct ConfigResource {
    resource_type: Type,
    name: String,
}

impl ConfigResource {
    /// Create an instance of this class with the provided parameters.
    ///
    /// * `resource_type` - a resource type
    /// * `name` - a resource name
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource#ConfigResource")]
    pub fn new(resource_type: Type, name: String) -> Self {
        Self { resource_type, name }
    }

    /// Return the resource type.
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource#type")]
    pub fn r#type(&self) -> Type {
        self.resource_type
    }

    /// Return the resource name.
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource#name")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns true if this is the default resource of a resource type.
    /// Resource name is empty for the default resource.
    #[doc(alias = "org.apache.kafka.common.config.ConfigResource#isDefault")]
    pub fn is_default(&self) -> bool {
        self.name.is_empty()
    }
}

impl std::fmt::Display for ConfigResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConfigResource(type={:?}, name='{}')", self.resource_type, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_round_trip() {
        for t in [
            Type::Group,
            Type::ClientMetrics,
            Type::BrokerLogger,
            Type::Broker,
            Type::Topic,
            Type::Unknown,
        ] {
            assert_eq!(Type::for_id(t.id()), t);
        }
    }

    #[test]
    fn type_ids_match_java() {
        assert_eq!(Type::Group.id(), 32);
        assert_eq!(Type::ClientMetrics.id(), 16);
        assert_eq!(Type::BrokerLogger.id(), 8);
        assert_eq!(Type::Broker.id(), 4);
        assert_eq!(Type::Topic.id(), 2);
        assert_eq!(Type::Unknown.id(), 0);
    }

    #[test]
    fn for_id_unknown_falls_back() {
        assert_eq!(Type::for_id(99), Type::Unknown);
    }

    #[test]
    fn is_default_when_name_empty() {
        assert!(ConfigResource::new(Type::Broker, String::new()).is_default());
        assert!(!ConfigResource::new(Type::Broker, "0".to_string()).is_default());
    }

    #[test]
    fn equality_uses_type_and_name() {
        let a = ConfigResource::new(Type::Topic, "t".to_string());
        let b = ConfigResource::new(Type::Topic, "t".to_string());
        let c = ConfigResource::new(Type::Broker, "t".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
