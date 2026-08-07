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

//! ACL resource types.
//!
//! Corresponds to `org.apache.kafka.common.resource.ResourceType`.

/// Represents a type of resource which an ACL can be applied to.
///
/// Corresponds to `org.apache.kafka.common.resource.ResourceType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResourceType {
    /// Represents any `ResourceType` which this client cannot understand,
    /// perhaps because this client is too old.
    Unknown,
    /// In a filter, matches any `ResourceType`.
    Any,
    /// A Kafka topic.
    Topic,
    /// A consumer group.
    Group,
    /// The cluster as a whole.
    Cluster,
    /// A transactional ID.
    TransactionalId,
    /// A token ID.
    DelegationToken,
    /// A user principal.
    User,
}

impl ResourceType {
    /// All resource types, in declaration (code) order. Mirrors Java's
    /// `ResourceType.values()`.
    pub const VALUES: [ResourceType; 8] = [
        ResourceType::Unknown,
        ResourceType::Any,
        ResourceType::Topic,
        ResourceType::Group,
        ResourceType::Cluster,
        ResourceType::TransactionalId,
        ResourceType::DelegationToken,
        ResourceType::User,
    ];

    /// Return the code of this resource.
    pub fn code(&self) -> i8 {
        match self {
            ResourceType::Unknown => 0,
            ResourceType::Any => 1,
            ResourceType::Topic => 2,
            ResourceType::Group => 3,
            ResourceType::Cluster => 4,
            ResourceType::TransactionalId => 5,
            ResourceType::DelegationToken => 6,
            ResourceType::User => 7,
        }
    }

    /// Return the `ResourceType` with the provided code or
    /// [`ResourceType::Unknown`] if one cannot be found.
    pub fn from_code(code: i8) -> ResourceType {
        match code {
            0 => ResourceType::Unknown,
            1 => ResourceType::Any,
            2 => ResourceType::Topic,
            3 => ResourceType::Group,
            4 => ResourceType::Cluster,
            5 => ResourceType::TransactionalId,
            6 => ResourceType::DelegationToken,
            7 => ResourceType::User,
            _ => ResourceType::Unknown,
        }
    }

    /// Parse the given string as an ACL resource type.
    ///
    /// Returns the `ResourceType`, or [`ResourceType::Unknown`] if the string
    /// could not be matched (case-insensitive).
    pub fn from_string(str: &str) -> ResourceType {
        match str.to_uppercase().as_str() {
            "UNKNOWN" => ResourceType::Unknown,
            "ANY" => ResourceType::Any,
            "TOPIC" => ResourceType::Topic,
            "GROUP" => ResourceType::Group,
            "CLUSTER" => ResourceType::Cluster,
            "TRANSACTIONAL_ID" => ResourceType::TransactionalId,
            "DELEGATION_TOKEN" => ResourceType::DelegationToken,
            "USER" => ResourceType::User,
            _ => ResourceType::Unknown,
        }
    }

    /// Return whether this resource type is [`ResourceType::Unknown`].
    pub fn is_unknown(&self) -> bool {
        *self == ResourceType::Unknown
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ResourceType::Unknown => "UNKNOWN",
            ResourceType::Any => "ANY",
            ResourceType::Topic => "TOPIC",
            ResourceType::Group => "GROUP",
            ResourceType::Cluster => "CLUSTER",
            ResourceType::TransactionalId => "TRANSACTIONAL_ID",
            ResourceType::DelegationToken => "DELEGATION_TOKEN",
            ResourceType::User => "USER",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Info {
        resource_type: ResourceType,
        code: i8,
        name: &'static str,
        unknown: bool,
    }

    const INFOS: [Info; 8] = [
        Info { resource_type: ResourceType::Unknown, code: 0, name: "unknown", unknown: true },
        Info { resource_type: ResourceType::Any, code: 1, name: "any", unknown: false },
        Info { resource_type: ResourceType::Topic, code: 2, name: "topic", unknown: false },
        Info { resource_type: ResourceType::Group, code: 3, name: "group", unknown: false },
        Info { resource_type: ResourceType::Cluster, code: 4, name: "cluster", unknown: false },
        Info {
            resource_type: ResourceType::TransactionalId,
            code: 5,
            name: "transactional_id",
            unknown: false,
        },
        Info {
            resource_type: ResourceType::DelegationToken,
            code: 6,
            name: "delegation_token",
            unknown: false,
        },
        Info { resource_type: ResourceType::User, code: 7, name: "user", unknown: false },
    ];

    #[test]
    fn test_is_unknown() {
        for info in &INFOS {
            assert_eq!(info.unknown, info.resource_type.is_unknown());
        }
    }

    #[test]
    fn test_code() {
        assert_eq!(ResourceType::VALUES.len(), INFOS.len());
        for info in &INFOS {
            assert_eq!(info.code, info.resource_type.code());
            assert_eq!(info.resource_type, ResourceType::from_code(info.code));
        }
        assert_eq!(ResourceType::Unknown, ResourceType::from_code(120));
    }

    #[test]
    fn test_name() {
        for info in &INFOS {
            assert_eq!(info.resource_type, ResourceType::from_string(info.name));
        }
        assert_eq!(ResourceType::Unknown, ResourceType::from_string("something"));
    }

    #[test]
    fn test_exhaustive() {
        assert_eq!(INFOS.len(), ResourceType::VALUES.len());
        for (i, info) in INFOS.iter().enumerate() {
            assert_eq!(info.resource_type, ResourceType::VALUES[i]);
        }
    }
}
