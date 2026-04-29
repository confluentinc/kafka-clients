// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Read-optimized concurrent map: writes copy the whole map under a lock,
//! reads expose an `Arc<HashMap<K, V>>` snapshot that can be iterated lock-free.
//!
//! Translated from `org.apache.kafka.common.utils.CopyOnWriteMap`.
//!
//! Use this when reads dominate writes by a wide margin (e.g.
//! `RecordAccumulator.topicInfo.batches`, where partitions are added once and
//! then iterated by `ready` / `expiredBatches` / `drain` on every Sender loop).
//! For roughly-balanced read/write workloads, prefer `DashMap`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// A copy-on-write concurrent map.
///
/// Writes serialize on the internal mutex, clone the whole `HashMap`, mutate
/// the copy, then atomically swap in a new `Arc<HashMap>`. Reads acquire the
/// mutex briefly to clone the `Arc` and then iterate the snapshot without any
/// lock — equivalent to Java's volatile-reference pattern.
pub struct CopyOnWriteMap<K, V> {
    inner: Mutex<Arc<HashMap<K, V>>>,
}

impl<K, V> CopyOnWriteMap<K, V> {
    pub fn new() -> Self {
        Self { inner: Mutex::new(Arc::new(HashMap::new())) }
    }

    /// Returns a lock-free snapshot of the current map. The mutex is held only
    /// for the `Arc::clone`; the returned snapshot is then read without any
    /// lock and remains valid even if other threads concurrently `put`.
    pub fn snapshot(&self) -> Arc<HashMap<K, V>> {
        Arc::clone(&self.inner.lock().unwrap())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl<K: Eq + Hash + Clone, V: Clone> CopyOnWriteMap<K, V> {
    /// Equivalent to Java's `containsKey(k)`.
    pub fn contains_key(&self, k: &K) -> bool {
        self.inner.lock().unwrap().contains_key(k)
    }

    /// Equivalent to Java's `get(k)` returning a clone of the value (Arc bump
    /// when V is `Arc<...>`, which is the typical use).
    pub fn get(&self, k: &K) -> Option<V> {
        self.inner.lock().unwrap().get(k).cloned()
    }

    /// Equivalent to Java's `put(k, v)`. Returns the previous value if any.
    pub fn put(&self, k: K, v: V) -> Option<V> {
        let mut guard = self.inner.lock().unwrap();
        let mut copy = (**guard).clone();
        let prev = copy.insert(k, v);
        *guard = Arc::new(copy);
        prev
    }

    /// Equivalent to Java's `putIfAbsent(k, v)`. Returns the existing value
    /// if `k` is already present, otherwise inserts and returns `None`.
    pub fn put_if_absent(&self, k: K, v: V) -> Option<V> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(existing) = guard.get(&k) {
            return Some(existing.clone());
        }
        let mut copy = (**guard).clone();
        copy.insert(k, v);
        *guard = Arc::new(copy);
        None
    }

    /// Equivalent to Java's `computeIfAbsent(k, f)`. Returns the existing
    /// value if present; otherwise calls `f`, inserts the result, and returns
    /// the new value.
    ///
    /// Fast path: lock-free snapshot read. Slow path under lock when the key
    /// is missing — the closure is invoked under lock so two concurrent
    /// inserts of the same key both see the winner's value (matches Java's
    /// synchronized contract).
    pub fn compute_if_absent(&self, k: &K, f: impl FnOnce() -> V) -> V {
        if let Some(v) = self.snapshot().get(k).cloned() {
            return v;
        }
        let mut guard = self.inner.lock().unwrap();
        if let Some(v) = guard.get(k).cloned() {
            return v;
        }
        let new_v = f();
        let mut copy = (**guard).clone();
        copy.insert(k.clone(), new_v.clone());
        *guard = Arc::new(copy);
        new_v
    }

    /// Equivalent to Java's `remove(k)`.
    pub fn remove(&self, k: &K) -> Option<V> {
        let mut guard = self.inner.lock().unwrap();
        if !guard.contains_key(k) {
            return None;
        }
        let mut copy = (**guard).clone();
        let prev = copy.remove(k);
        *guard = Arc::new(copy);
        prev
    }

    /// Equivalent to Java's `clear()`.
    pub fn clear(&self) {
        *self.inner.lock().unwrap() = Arc::new(HashMap::new());
    }
}

impl<K, V> Default for CopyOnWriteMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let m: CopyOnWriteMap<String, i32> = CopyOnWriteMap::new();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        assert_eq!(m.put("a".to_string(), 1), None);
        assert_eq!(m.put("a".to_string(), 2), Some(1));
        assert_eq!(m.get(&"a".to_string()), Some(2));
        assert!(m.contains_key(&"a".to_string()));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn put_if_absent_only_inserts_when_missing() {
        let m: CopyOnWriteMap<i32, i32> = CopyOnWriteMap::new();
        assert_eq!(m.put_if_absent(1, 10), None);
        assert_eq!(m.put_if_absent(1, 20), Some(10));
        assert_eq!(m.get(&1), Some(10));
    }

    #[test]
    fn compute_if_absent_runs_closure_only_when_missing() {
        let m: CopyOnWriteMap<i32, String> = CopyOnWriteMap::new();
        let v1 = m.compute_if_absent(&1, || "first".to_string());
        assert_eq!(v1, "first");
        let v2 = m.compute_if_absent(&1, || panic!("should not run for existing key"));
        assert_eq!(v2, "first");
    }

    #[test]
    fn snapshot_is_stable_across_concurrent_writes() {
        let m: Arc<CopyOnWriteMap<i32, i32>> = Arc::new(CopyOnWriteMap::new());
        m.put(1, 10);
        m.put(2, 20);
        let snapshot = m.snapshot();
        // Mutate after taking snapshot — should not affect the snapshot.
        m.put(3, 30);
        m.put(1, 100);
        assert_eq!(snapshot.get(&1), Some(&10));
        assert_eq!(snapshot.get(&2), Some(&20));
        assert_eq!(snapshot.get(&3), None);
        // The map itself reflects the new writes.
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.get(&3), Some(30));
    }

    #[test]
    fn remove_and_clear() {
        let m: CopyOnWriteMap<i32, i32> = CopyOnWriteMap::new();
        m.put(1, 10);
        m.put(2, 20);
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.remove(&1), None);
        assert_eq!(m.len(), 1);
        m.clear();
        assert!(m.is_empty());
    }

    #[test]
    fn iter_via_snapshot_does_not_block_writes() {
        let m: Arc<CopyOnWriteMap<i32, i32>> = Arc::new(CopyOnWriteMap::new());
        for i in 0..10 {
            m.put(i, i * 10);
        }
        let snapshot = m.snapshot();
        // Concurrent write while iterating
        let m2 = Arc::clone(&m);
        let writer = std::thread::spawn(move || {
            for i in 10..20 {
                m2.put(i, i * 10);
            }
        });
        let mut sum = 0;
        for (k, v) in snapshot.iter() {
            assert_eq!(*v, k * 10);
            sum += v;
        }
        assert_eq!(sum, (0..10).map(|i| i * 10).sum::<i32>());
        writer.join().unwrap();
        assert_eq!(m.len(), 20);
    }
}
