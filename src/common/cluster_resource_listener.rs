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

//! Cluster resource listener callback.
//!
//! Corresponds to `org.apache.kafka.common.ClusterResourceListener`.

use super::ClusterResource;

/// A callback trait that users can implement to get notified about changes in the
/// cluster metadata.
///
/// Users who need access to cluster metadata in interceptors, metric reporters,
/// serializers and deserializers can implement this trait.
///
/// There will be one invocation of [`ClusterResourceListener::on_update`] after
/// each metadata response.
///
/// Corresponds to `org.apache.kafka.common.ClusterResourceListener`.
pub trait ClusterResourceListener: Send {
    /// Called when the cluster metadata is updated.
    fn on_update(&self, cluster_resource: &ClusterResource);
}
