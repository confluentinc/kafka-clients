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

//! A cache of partition-to-leader mappings, reused across driver invocations.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.PartitionLeaderCache`.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::common::TopicPartition;

/// A cache of partition-to-leader mappings, shared across `deleteRecords` /
/// `listOffsets` calls so that repeated calls can skip the lookup stage.
///
/// Corresponds to `PartitionLeaderCache`. Java guards the map with
/// `synchronized`; the Rust port uses a `std::sync::Mutex` (short, non-awaiting
/// critical sections).
#[derive(Debug, Default)]
pub(crate) struct PartitionLeaderCache {
    cache: Mutex<HashMap<TopicPartition, i32>>,
}

impl PartitionLeaderCache {
    /// Creates an empty cache.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the cached leader ids for the subset of `keys` present in the
    /// cache.
    ///
    /// Mirrors `get`.
    pub(crate) fn get<'a, I>(&self, keys: I) -> HashMap<TopicPartition, i32>
    where
        I: IntoIterator<Item = &'a TopicPartition>,
    {
        let cache = self.cache.lock().unwrap();
        let mut result = HashMap::new();
        for key in keys {
            if let Some(broker_id) = cache.get(key) {
                result.insert(key.clone(), *broker_id);
            }
        }
        result
    }

    /// Inserts (or overwrites) the given mappings.
    ///
    /// Mirrors `put`.
    pub(crate) fn put(&self, values: &HashMap<TopicPartition, i32>) {
        let mut cache = self.cache.lock().unwrap();
        for (key, broker_id) in values {
            cache.insert(key.clone(), *broker_id);
        }
    }

    /// Removes the given keys from the cache.
    ///
    /// Mirrors `remove`.
    pub(crate) fn remove<'a, I>(&self, keys: I)
    where
        I: IntoIterator<Item = &'a TopicPartition>,
    {
        let mut cache = self.cache.lock().unwrap();
        for key in keys {
            cache.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_remove_round_trip() {
        let cache = PartitionLeaderCache::new();
        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let mut values = HashMap::new();
        values.insert(tp0.clone(), 5);
        values.insert(tp1.clone(), 6);
        cache.put(&values);

        let got = cache.get([&tp0, &tp1]);
        assert_eq!(got.get(&tp0), Some(&5));
        assert_eq!(got.get(&tp1), Some(&6));

        cache.remove([&tp0]);
        let got = cache.get([&tp0, &tp1]);
        assert!(!got.contains_key(&tp0));
        assert_eq!(got.get(&tp1), Some(&6));
    }
}
