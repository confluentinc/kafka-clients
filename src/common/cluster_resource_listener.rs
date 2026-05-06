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

//! Translation of `org.apache.kafka.common.ClusterResourceListener`.

use crate::common::ClusterResource;

/// Callback trait for users that wish to be notified about changes in the
/// cluster metadata.
///
/// Mirrors Java's `ClusterResourceListener`. There will be one invocation of
/// [`ClusterResourceListener::on_update`] after each metadata response.
///
/// Implementations are stored as `Arc<dyn ClusterResourceListener + Send +
/// Sync>` and may be called from any thread, so all interior mutability must
/// be synchronized by the implementor.
pub trait ClusterResourceListener: Send + Sync {
    /// A callback method that a user can implement to get updates for
    /// [`ClusterResource`].
    fn on_update(&self, cluster_resource: &ClusterResource);
}
