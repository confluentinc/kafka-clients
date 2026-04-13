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

//! The `ClusterResource` class encapsulates metadata for a Kafka cluster.

use std::fmt;

/// The `ClusterResource` class encapsulates metadata for a Kafka cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClusterResource {
    cluster_id: Option<String>,
}

impl ClusterResource {
    /// Create a `ClusterResource` with a cluster id. Note that cluster id may be `None` if the
    /// metadata request was sent to a broker without support for cluster ids.
    pub fn new(cluster_id: Option<String>) -> Self {
        Self { cluster_id }
    }

    /// Return the cluster id. Note that it may be `None` if the metadata request was sent to a
    /// broker without support for cluster ids.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }
}

impl fmt::Display for ClusterResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClusterResource(clusterId={:?})", self.cluster_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_resource() {
        let cr = ClusterResource::new(Some("test-cluster".to_string()));
        assert_eq!(cr.cluster_id(), Some("test-cluster"));
    }

    #[test]
    fn test_cluster_resource_none() {
        let cr = ClusterResource::new(None);
        assert!(cr.cluster_id().is_none());
    }

    #[test]
    fn test_cluster_resource_equality() {
        let cr1 = ClusterResource::new(Some("id".to_string()));
        let cr2 = ClusterResource::new(Some("id".to_string()));
        let cr3 = ClusterResource::new(None);
        assert_eq!(cr1, cr2);
        assert_ne!(cr1, cr3);
    }
}
