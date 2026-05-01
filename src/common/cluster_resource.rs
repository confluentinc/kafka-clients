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

//! Translation of `org.apache.kafka.common.ClusterResource`.

use std::fmt;

/// Encapsulates metadata for a Kafka cluster.
///
/// Mirrors Java's `ClusterResource`. The cluster id may be `None` if the
/// metadata request was sent to a broker without support for cluster ids.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClusterResource {
    cluster_id: Option<String>,
}

impl ClusterResource {
    /// Create a [`ClusterResource`] with a cluster id. The cluster id may be
    /// `None` if the metadata request was sent to a broker without support
    /// for cluster ids.
    pub fn new(cluster_id: Option<String>) -> Self {
        Self { cluster_id }
    }

    /// Return the cluster id. Note that it may be `None` if the metadata
    /// request was sent to a broker without support for cluster ids.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }
}

impl fmt::Display for ClusterResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java's `+` against null prints "null"; mirror that.
        match &self.cluster_id {
            Some(id) => write!(f, "ClusterResource(clusterId={id})"),
            None => write!(f, "ClusterResource(clusterId=null)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equals_and_clone() {
        let a = ClusterResource::new(Some("abc".to_string()));
        let b = a.clone();
        assert_eq!(a, b);

        let c = ClusterResource::new(None);
        let d = ClusterResource::new(None);
        assert_eq!(c, d);

        assert_ne!(a, c);
    }

    #[test]
    fn display_matches_java() {
        let a = ClusterResource::new(Some("xyz".to_string()));
        assert_eq!(a.to_string(), "ClusterResource(clusterId=xyz)");
        let b = ClusterResource::new(None);
        assert_eq!(b.to_string(), "ClusterResource(clusterId=null)");
    }

    #[test]
    fn cluster_id_accessor() {
        let r = ClusterResource::new(Some("hello".to_string()));
        assert_eq!(r.cluster_id(), Some("hello"));
        let r2 = ClusterResource::new(None);
        assert_eq!(r2.cluster_id(), None);
    }
}
