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

//! Translation of
//! `org.apache.kafka.common.protocol.ObjectSerializationCache`.
//!
//! The Java implementation uses an `IdentityHashMap` keyed by `Object`
//! references to cache (1) array sizes computed during the size pass and
//! (2) UTF-8-encoded bytes of strings so the write pass does not have to
//! recompute them. Rust does not have stable identity-by-reference on values,
//! so we key by the address of the Rust value the generated code holds. The
//! generated code always passes the *same* `Vec<u8>` / `String` reference to
//! `add_size` and to `write`, mirroring how the Java code passes the same
//! `Object` reference to both calls.

use std::collections::HashMap;

/// Cache of pre-computed array sizes and serialized values used to avoid
/// recomputing during the second pass of message serialisation.
///
/// Generated code calls into this cache via:
/// * [`Self::set_array_size_in_bytes`] / [`Self::get_array_size_in_bytes`]
///   — for arrays whose element count is known but whose total wire size has
///   to be summed up.
/// * [`Self::cache_serialized_value`] / [`Self::get_serialized_value`] —
///   for `String` fields that need to be UTF-8 encoded once and reused.
#[derive(Debug, Default)]
pub struct ObjectSerializationCache {
    sizes: HashMap<usize, i32>,
    values: HashMap<usize, Vec<u8>>,
}

impl ObjectSerializationCache {
    /// Construct an empty cache. Mirrors `new ObjectSerializationCache()`.
    pub fn new() -> Self {
        ObjectSerializationCache::default()
    }

    /// Cache `size` for the value identified by `key`. The key is the
    /// address of the Rust object the generated code holds, mirroring Java's
    /// `IdentityHashMap` semantics.
    pub fn set_array_size_in_bytes<T>(&mut self, key: &T, size: i32) {
        self.sizes.insert(key as *const T as usize, size);
    }

    /// Retrieve the cached size for `key`, or `None` if absent.
    pub fn get_array_size_in_bytes<T>(&self, key: &T) -> Option<i32> {
        self.sizes.get(&(key as *const T as usize)).copied()
    }

    /// Cache the UTF-8 byte representation of the value identified by `key`.
    pub fn cache_serialized_value<T>(&mut self, key: &T, value: Vec<u8>) {
        self.values.insert(key as *const T as usize, value);
    }

    /// Retrieve the cached UTF-8 representation for `key`, or `None`.
    pub fn get_serialized_value<T>(&self, key: &T) -> Option<&[u8]> {
        self.values.get(&(key as *const T as usize)).map(|v| v.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caches_size_by_identity() {
        let mut cache = ObjectSerializationCache::new();
        let a: Vec<i32> = vec![1, 2, 3];
        let b: Vec<i32> = vec![1, 2, 3];
        cache.set_array_size_in_bytes(&a, 12);
        // `b` is an equal but distinct value; Java would not match either.
        assert_eq!(cache.get_array_size_in_bytes(&a), Some(12));
        assert_eq!(cache.get_array_size_in_bytes(&b), None);
    }

    #[test]
    fn caches_serialized_value() {
        let mut cache = ObjectSerializationCache::new();
        let key = String::from("topic");
        let bytes = key.as_bytes().to_vec();
        cache.cache_serialized_value(&key, bytes.clone());
        assert_eq!(cache.get_serialized_value(&key), Some(bytes.as_slice()));
    }
}
