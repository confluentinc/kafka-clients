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
pub enum ConfigResourceType {
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

impl ConfigResourceType {
    /// Returns the wire id for this resource type.
    ///
    /// Corresponds to `ConfigResource.Type.id()`.
    pub fn id(&self) -> i8 {
        match self {
            ConfigResourceType::Group => 32,
            ConfigResourceType::ClientMetrics => 16,
            ConfigResourceType::BrokerLogger => 8,
            ConfigResourceType::Broker => 4,
            ConfigResourceType::Topic => 2,
            ConfigResourceType::Unknown => 0,
        }
    }

    /// Returns the resource type for the given wire id, or
    /// [`ConfigResourceType::Unknown`] if the id is unrecognized.
    ///
    /// Corresponds to `ConfigResource.Type.forId(byte)`.
    pub fn for_id(id: i8) -> ConfigResourceType {
        match id {
            32 => ConfigResourceType::Group,
            16 => ConfigResourceType::ClientMetrics,
            8 => ConfigResourceType::BrokerLogger,
            4 => ConfigResourceType::Broker,
            2 => ConfigResourceType::Topic,
            _ => ConfigResourceType::Unknown,
        }
    }
}

/// A class representing resources that have configs.
///
/// Corresponds to `org.apache.kafka.common.config.ConfigResource`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConfigResource {
    resource_type: ConfigResourceType,
    name: String,
}

impl ConfigResource {
    /// Create an instance of this class with the provided parameters.
    ///
    /// * `resource_type` - a resource type
    /// * `name` - a resource name
    pub fn new(resource_type: ConfigResourceType, name: String) -> Self {
        Self { resource_type, name }
    }

    /// Return the resource type.
    pub fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Return the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns true if this is the default resource of a resource type.
    /// Resource name is empty for the default resource.
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
            ConfigResourceType::Group,
            ConfigResourceType::ClientMetrics,
            ConfigResourceType::BrokerLogger,
            ConfigResourceType::Broker,
            ConfigResourceType::Topic,
            ConfigResourceType::Unknown,
        ] {
            assert_eq!(ConfigResourceType::for_id(t.id()), t);
        }
    }

    #[test]
    fn type_ids_match_java() {
        assert_eq!(ConfigResourceType::Group.id(), 32);
        assert_eq!(ConfigResourceType::ClientMetrics.id(), 16);
        assert_eq!(ConfigResourceType::BrokerLogger.id(), 8);
        assert_eq!(ConfigResourceType::Broker.id(), 4);
        assert_eq!(ConfigResourceType::Topic.id(), 2);
        assert_eq!(ConfigResourceType::Unknown.id(), 0);
    }

    #[test]
    fn for_id_unknown_falls_back() {
        assert_eq!(ConfigResourceType::for_id(99), ConfigResourceType::Unknown);
    }

    #[test]
    fn is_default_when_name_empty() {
        assert!(ConfigResource::new(ConfigResourceType::Broker, String::new()).is_default());
        assert!(!ConfigResource::new(ConfigResourceType::Broker, "0".to_string()).is_default());
    }

    #[test]
    fn equality_uses_type_and_name() {
        let a = ConfigResource::new(ConfigResourceType::Topic, "t".to_string());
        let b = ConfigResource::new(ConfigResourceType::Topic, "t".to_string());
        let c = ConfigResource::new(ConfigResourceType::Broker, "t".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
