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

//! Cluster resource listener support.
//!
//! Corresponds to `org.apache.kafka.common.ClusterResourceListener` and
//! `org.apache.kafka.common.internals.ClusterResourceListeners`.
//!
//! In Java, `ClusterResourceListeners.maybeAdd()` uses `instanceof` to check if
//! an arbitrary object implements the `ClusterResourceListener` interface. In Rust,
//! callers add listeners explicitly via `add_listener()`.

use crate::common::cluster_resource::ClusterResource;

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

/// A collection of [`ClusterResourceListener`]s that are notified when the cluster
/// resource (cluster ID) changes.
///
/// Corresponds to `org.apache.kafka.common.internals.ClusterResourceListeners`.
///
/// In Java, `maybeAdd(Object)` uses `instanceof` to check if the candidate
/// implements `ClusterResourceListener`. In Rust, callers must use [`add_listener`]
/// directly since there is no runtime type introspection.
///
/// [`add_listener`]: ClusterResourceListeners::add_listener
pub struct ClusterResourceListeners {
    listeners: Vec<Box<dyn ClusterResourceListener>>,
}

impl ClusterResourceListeners {
    /// Creates an empty `ClusterResourceListeners` collection.
    pub fn new() -> Self {
        Self { listeners: Vec::new() }
    }

    /// Adds a listener to the collection.
    ///
    /// This replaces Java's `maybeAdd(Object)` which uses `instanceof` to check
    /// if the candidate implements `ClusterResourceListener`. In Rust, callers
    /// must call this method directly with a concrete listener.
    pub fn add_listener(&mut self, listener: Box<dyn ClusterResourceListener>) {
        self.listeners.push(listener);
    }

    /// Convenience alias for `add_listener`, matching the Java method name pattern.
    ///
    /// In Java, `maybeAdd` checks if the candidate is a `ClusterResourceListener`
    /// using `instanceof`. In Rust, the caller already knows the type, so this is
    /// equivalent to `add_listener`.
    pub fn maybe_add(&mut self, listener: Box<dyn ClusterResourceListener>) {
        self.add_listener(listener);
    }

    /// Sends the updated cluster metadata to all registered listeners.
    pub fn on_update(&self, cluster_resource: &ClusterResource) {
        for listener in &self.listeners {
            listener.on_update(cluster_resource);
        }
    }
}

impl Default for ClusterResourceListeners {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A mock listener for testing.
    struct MockListener {
        called: Arc<AtomicBool>,
    }

    impl ClusterResourceListener for MockListener {
        fn on_update(&self, _cluster_resource: &ClusterResource) {
            self.called.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_on_update_notifies_all_listeners() {
        let called1 = Arc::new(AtomicBool::new(false));
        let called2 = Arc::new(AtomicBool::new(false));

        let listener1 = MockListener { called: called1.clone() };
        let listener2 = MockListener { called: called2.clone() };

        let mut listeners = ClusterResourceListeners::new();
        listeners.add_listener(Box::new(listener1));
        listeners.add_listener(Box::new(listener2));

        let cr = ClusterResource::new(Some("test-cluster".to_string()));
        listeners.on_update(&cr);

        assert!(called1.load(Ordering::SeqCst));
        assert!(called2.load(Ordering::SeqCst));
    }

    #[test]
    fn test_on_update_with_no_listeners() {
        let listeners = ClusterResourceListeners::new();
        let cr = ClusterResource::new(Some("test-cluster".to_string()));
        // Should not panic
        listeners.on_update(&cr);
    }

    #[test]
    fn test_maybe_add() {
        let called = Arc::new(AtomicBool::new(false));
        let listener = MockListener { called: called.clone() };

        let mut listeners = ClusterResourceListeners::new();
        listeners.maybe_add(Box::new(listener));

        let cr = ClusterResource::new(Some("test-cluster".to_string()));
        listeners.on_update(&cr);

        assert!(called.load(Ordering::SeqCst));
    }
}
