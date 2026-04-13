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

//! Object serialization cache for two-pass serialization.
//!
//! The ObjectSerializationCache stores sizes and values computed during the
//! first serialization pass. This avoids recalculating and recomputing the same
//! values during the second pass.
//!
//! It is intended to be used as part of a two-pass serialization process:
//! ```ignore
//! let mut cache = ObjectSerializationCache::new();
//! let size = message.size(&mut cache, version);
//! message.write(&mut writable, &cache, version);
//! ```
//!
//! Corresponds to org.apache.kafka.common.protocol.ObjectSerializationCache

use std::collections::HashMap;

/// Cache key based on object identity (pointer address).
///
/// In Java, this uses IdentityHashMap which compares by reference identity.
/// In Rust, we use the raw pointer address of the referenced object as the key.
type IdentityKey = usize;

/// Cached value that can be either an array size or serialized bytes.
#[derive(Debug, Clone)]
enum CachedValue {
    Size(i32),
    Bytes(Vec<u8>),
}

/// Stores sizes and serialized values computed during the first serialization pass.
///
/// This avoids recalculating and recomputing the same values during the second pass.
/// Uses object identity (pointer address) as keys, matching Java's IdentityHashMap.
#[derive(Debug, Default)]
pub struct ObjectSerializationCache {
    map: HashMap<IdentityKey, CachedValue>,
}

impl ObjectSerializationCache {
    /// Creates a new empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cache the serialized size in bytes for a given object (identified by reference).
    ///
    /// The object is identified by its memory address. The caller must ensure
    /// the object is not moved between `set` and `get` calls.
    pub fn set_array_size_in_bytes<T: ?Sized>(&mut self, obj: &T, size: i32) {
        let key = obj as *const T as *const () as usize;
        self.map.insert(key, CachedValue::Size(size));
    }

    /// Retrieve the cached serialized size in bytes for a given object.
    ///
    /// Returns `None` if no size was cached for this object.
    pub fn get_array_size_in_bytes<T: ?Sized>(&self, obj: &T) -> Option<i32> {
        let key = obj as *const T as *const () as usize;
        match self.map.get(&key) {
            Some(CachedValue::Size(size)) => Some(*size),
            _ => None,
        }
    }

    /// Cache a serialized byte representation for a given object.
    pub fn cache_serialized_value<T: ?Sized>(&mut self, obj: &T, val: Vec<u8>) {
        let key = obj as *const T as *const () as usize;
        self.map.insert(key, CachedValue::Bytes(val));
    }

    /// Retrieve a cached serialized byte representation for a given object.
    ///
    /// Returns `None` if no value was cached for this object.
    pub fn get_serialized_value<T: ?Sized>(&self, obj: &T) -> Option<&[u8]> {
        let key = obj as *const T as *const () as usize;
        match self.map.get(&key) {
            Some(CachedValue::Bytes(bytes)) => Some(bytes),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_array_size() {
        let mut cache = ObjectSerializationCache::new();
        let arr = vec![1, 2, 3];
        cache.set_array_size_in_bytes(&arr, 42);
        assert_eq!(cache.get_array_size_in_bytes(&arr), Some(42));
    }

    #[test]
    fn test_cache_serialized_value() {
        let mut cache = ObjectSerializationCache::new();
        let s = String::from("hello");
        cache.cache_serialized_value(&s, vec![0x68, 0x65, 0x6c, 0x6c, 0x6f]);
        assert_eq!(cache.get_serialized_value(&s), Some(&[0x68, 0x65, 0x6c, 0x6c, 0x6f][..]));
    }

    #[test]
    fn test_different_objects_different_keys() {
        let mut cache = ObjectSerializationCache::new();
        let arr1 = vec![1, 2, 3];
        let arr2 = vec![1, 2, 3];
        cache.set_array_size_in_bytes(&arr1, 10);
        cache.set_array_size_in_bytes(&arr2, 20);
        assert_eq!(cache.get_array_size_in_bytes(&arr1), Some(10));
        assert_eq!(cache.get_array_size_in_bytes(&arr2), Some(20));
    }

    #[test]
    fn test_uncached_returns_none() {
        let cache = ObjectSerializationCache::new();
        let arr = vec![1, 2, 3];
        assert_eq!(cache.get_array_size_in_bytes(&arr), None);
        assert_eq!(cache.get_serialized_value(&arr), None);
    }
}
