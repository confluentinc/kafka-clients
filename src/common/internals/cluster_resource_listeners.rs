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

//! Translation of `org.apache.kafka.common.internals.ClusterResourceListeners`.

// Phase 4a translation. The producer/consumer/metadata stack that consumes
// this aggregator lands in Phase 4b/Phase 6 — until then the public API is
// dead-code from the perspective of the lib build but lives behind tests.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use crate::common::ClusterResource;
use crate::common::cluster_resource_listener::ClusterResourceListener;

/// Aggregator for [`ClusterResourceListener`] instances. Mirrors Java's
/// `ClusterResourceListeners`.
///
/// Java's `maybeAdd(Object)` used `instanceof` to filter generic
/// `Object`/`List<?>` collections. The Rust translation drops the
/// `instanceof` check — call sites pass concretely-typed
/// `Arc<dyn ClusterResourceListener>` directly.
///
/// This type is `pub(crate)` because it lives under the
/// `org.apache.kafka.common.internals` package (CLAUDE.md naming rules).
pub(crate) struct ClusterResourceListeners {
    listeners: Mutex<Vec<Arc<dyn ClusterResourceListener>>>,
}

impl ClusterResourceListeners {
    pub(crate) fn new() -> Self {
        Self { listeners: Mutex::new(Vec::new()) }
    }

    /// Add a listener.
    pub(crate) fn add(&self, listener: Arc<dyn ClusterResourceListener>) {
        self.listeners.lock().expect("listeners mutex poisoned").push(listener);
    }

    /// Add all listeners from the given iterator.
    pub(crate) fn add_all<I>(&self, listeners: I)
    where
        I: IntoIterator<Item = Arc<dyn ClusterResourceListener>>,
    {
        let mut guard = self.listeners.lock().expect("listeners mutex poisoned");
        for l in listeners {
            guard.push(l);
        }
    }

    /// Send the updated cluster metadata to all listeners.
    ///
    /// Takes `&self` (not `&mut self`) because the listener collection is
    /// read-only at notification time — Java iterates over its `List` and
    /// calls `onUpdate` on each element without mutating the list.
    /// The mutex is dropped before any listener is invoked, so a listener
    /// is free to call back into [`ClusterResourceListeners::add`] or
    /// otherwise re-lock without deadlock.
    pub(crate) fn on_update(&self, cluster: &ClusterResource) {
        // Snapshot the current listeners so we don't hold the lock across
        // calls into user code.
        let snapshot: Vec<Arc<dyn ClusterResourceListener>> = {
            let guard = self.listeners.lock().expect("listeners mutex poisoned");
            guard.clone()
        };
        for listener in snapshot {
            listener.on_update(cluster);
        }
    }
}

impl Default for ClusterResourceListeners {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ClusterResourceListeners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.listeners.lock().map(|g| g.len()).unwrap_or_default();
        f.debug_struct("ClusterResourceListeners").field("len", &len).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting {
        count: AtomicUsize,
        last_id: Mutex<Option<String>>,
    }

    impl ClusterResourceListener for Counting {
        fn on_update(&self, cluster_resource: &ClusterResource) {
            self.count.fetch_add(1, Ordering::SeqCst);
            *self.last_id.lock().unwrap() = cluster_resource.cluster_id().map(str::to_owned);
        }
    }

    #[test]
    fn add_and_dispatch() {
        let listeners = ClusterResourceListeners::new();
        let l1 = Arc::new(Counting { count: AtomicUsize::new(0), last_id: Mutex::new(None) });
        let l2 = Arc::new(Counting { count: AtomicUsize::new(0), last_id: Mutex::new(None) });
        listeners.add(l1.clone());
        listeners.add(l2.clone());

        let cr = ClusterResource::new(Some("cid".to_string()));
        listeners.on_update(&cr);
        assert_eq!(l1.count.load(Ordering::SeqCst), 1);
        assert_eq!(l2.count.load(Ordering::SeqCst), 1);
        assert_eq!(l1.last_id.lock().unwrap().as_deref(), Some("cid"));
    }

    #[test]
    fn add_all_appends() {
        let listeners = ClusterResourceListeners::new();
        let l1: Arc<dyn ClusterResourceListener> =
            Arc::new(Counting { count: AtomicUsize::new(0), last_id: Mutex::new(None) });
        let l2: Arc<dyn ClusterResourceListener> =
            Arc::new(Counting { count: AtomicUsize::new(0), last_id: Mutex::new(None) });
        listeners.add_all(vec![l1, l2]);

        let cr = ClusterResource::new(None);
        listeners.on_update(&cr);
    }
}
