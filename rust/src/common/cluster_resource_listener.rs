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

/// A callback trait that users can implement when they wish to get notified
/// about changes in the Cluster metadata.
///
/// Users who need access to cluster metadata in interceptors, metric reporters,
/// serializers and deserializers can implement this trait. The order of method
/// calls for each of these types is described below.
///
/// # Clients
///
/// There will be one invocation of [`ClusterResourceListener::on_update`] after
/// each metadata response.
///
/// `ProducerInterceptor`: the `on_update` method will be invoked after
/// `ProducerInterceptor::on_send` but before
/// `ProducerInterceptor::on_acknowledgement`.
///
/// `ConsumerInterceptor`: the `on_update` method will be invoked before
/// `ConsumerInterceptor::on_consume`.
///
/// `Serializer`: the `on_update` method will be invoked before
/// `Serializer::serialize`.
///
/// `Deserializer`: the `on_update` method will be invoked before
/// `Deserializer::deserialize`.
///
/// `MetricsReporter`: the `on_update` method will be invoked after first
/// `KafkaProducer::send` invocation for Producer metrics reporter and after
/// first `KafkaConsumer::poll` invocation for Consumer metrics reporters. The
/// reporter may receive metric events from the network layer before this
/// method is invoked.
///
/// # Translation notes
///
/// WARNING: no client registers a user's listener, so `on_update` is never called.
#[doc(alias = "org.apache.kafka.common.ClusterResourceListener")]
pub trait ClusterResourceListener: Send {
    /// A callback method that a user can implement to get updates for
    /// [`ClusterResource`].
    ///
    /// # Arguments
    ///
    /// - `cluster_resource` - cluster metadata
    #[doc(alias = "org.apache.kafka.common.ClusterResourceListener#onUpdate")]
    fn on_update(&self, cluster_resource: &ClusterResource);
}
