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

//! ACL resources.
//!
//! Corresponds to `org.apache.kafka.common.resource.Resource`.

use super::ResourceType;

/// Represents a cluster resource with a tuple of (type, name).
///
/// Corresponds to `org.apache.kafka.common.resource.Resource`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Resource {
    resource_type: ResourceType,
    name: String,
}

impl Resource {
    /// The name of the `CLUSTER` resource.
    pub const CLUSTER_NAME: &str = "kafka-cluster";

    /// Create an instance of this class with the provided parameters.
    ///
    /// # Arguments
    /// * `resource_type` - resource type
    /// * `name` - resource name
    pub fn new(resource_type: ResourceType, name: impl Into<String>) -> Resource {
        Resource { resource_type, name: name.into() }
    }

    /// A resource representing the whole cluster.
    pub fn cluster() -> Resource {
        Resource::new(ResourceType::Cluster, Resource::CLUSTER_NAME)
    }

    /// Return the resource type.
    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    /// Return the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return true if this `Resource` has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.resource_type.is_unknown()
    }
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(resourceType={}, name={})", self.resource_type, self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_resource() {
        let cluster = Resource::cluster();
        assert_eq!(cluster.resource_type(), ResourceType::Cluster);
        assert_eq!(cluster.name(), "kafka-cluster");
        assert!(!cluster.is_unknown());
    }

    #[test]
    fn is_unknown_for_unknown_type() {
        assert!(Resource::new(ResourceType::Unknown, "foo").is_unknown());
        assert!(!Resource::new(ResourceType::Topic, "foo").is_unknown());
    }

    #[test]
    fn equality_and_to_string() {
        let a = Resource::new(ResourceType::Topic, "foo");
        let b = Resource::new(ResourceType::Topic, "foo");
        let c = Resource::new(ResourceType::Group, "foo");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.to_string(), "(resourceType=TOPIC, name=foo)");
    }
}
