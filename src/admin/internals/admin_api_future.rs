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

//! The future bundle that the driver completes as keys are resolved.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AdminApiFuture`.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::common::KafkaError;
use crate::common::kafka_future::{KafkaFuture, KafkaFutureImpl};

/// The broker id used for keys that have no cached mapping.
///
/// Corresponds to `AdminApiFuture.UNKNOWN_BROKER_ID`.
pub(crate) const UNKNOWN_BROKER_ID: i32 = -1;

/// The future bundle that the [`AdminApiDriver`](super::admin_api_driver::AdminApiDriver)
/// completes as keys are resolved.
///
/// Corresponds to `AdminApiFuture<K, V>`. Kept a plain (non-`async`) trait per
/// `.claude/rules/admin-client.md` §2 — the driver completes the futures on the
/// background task.
pub(crate) trait AdminApiFuture<K, V>: Send {
    /// The initial set of lookup keys.
    ///
    /// Mirrors `lookupKeys`.
    fn lookup_keys(&self) -> HashSet<K>;

    /// The cached key-to-broker-id mapping. The default maps every key to
    /// [`UNKNOWN_BROKER_ID`].
    ///
    /// Mirrors `cachedKeyBrokerIdMapping`.
    fn cached_key_broker_id_mapping(&self) -> HashMap<K, i32>
    where
        K: Clone + Eq + Hash,
    {
        self.lookup_keys().into_iter().map(|k| (k, UNKNOWN_BROKER_ID)).collect()
    }

    /// Completes the futures associated with the given keys.
    ///
    /// Mirrors `complete`.
    fn complete(&self, values: HashMap<K, V>);

    /// Invoked when lookup of a set of keys succeeds. The default is a no-op.
    ///
    /// Mirrors `completeLookup`.
    fn complete_lookup(&self, _broker_id_mapping: HashMap<K, i32>) {}

    /// Invoked when lookup fails with a fatal error on a set of keys. The
    /// default delegates to [`complete_exceptionally`](Self::complete_exceptionally).
    ///
    /// Mirrors `completeLookupExceptionally`.
    fn complete_lookup_exceptionally(&self, lookup_errors: HashMap<K, KafkaError>) {
        self.complete_exceptionally(lookup_errors);
    }

    /// Completes the futures associated with the given keys exceptionally.
    ///
    /// Mirrors `completeExceptionally`.
    fn complete_exceptionally(&self, errors: HashMap<K, KafkaError>);
}

/// A simple [`AdminApiFuture`] that holds one completable future per key with no
/// cached key→broker mapping.
///
/// Corresponds to `AdminApiFuture.SimpleAdminApiFuture` (created via
/// `AdminApiFuture.forKeys(keys)`). Used by the group-describe handlers, which
/// key their futures by [`CoordinatorKey`](super::coordinator_key::CoordinatorKey).
#[allow(dead_code)] // wired by group-describe handlers later in this phase
pub(crate) struct SimpleAdminApiFuture<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    futures: HashMap<K, KafkaFutureImpl<V>>,
}

#[allow(dead_code)] // wired by group-describe handlers later in this phase
impl<K, V> SimpleAdminApiFuture<K, V>
where
    K: Clone + Eq + Hash + Send,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a future bundle with one empty future per key.
    ///
    /// Mirrors `AdminApiFuture.forKeys(Set<K> keys)`.
    pub(crate) fn for_keys(keys: HashSet<K>) -> Self {
        let futures = keys.into_iter().map(|k| (k, KafkaFutureImpl::new())).collect();
        Self { futures }
    }

    /// Returns the per-key public futures.
    ///
    /// Mirrors `AdminApiFuture.all()`.
    pub(crate) fn all(&self) -> HashMap<K, KafkaFuture<V>> {
        self.futures.iter().map(|(k, v)| (k.clone(), v.future())).collect()
    }
}

impl<K, V> AdminApiFuture<K, V> for SimpleAdminApiFuture<K, V>
where
    K: Clone + Eq + Hash + Send,
    V: Clone + Send + Sync + 'static,
{
    fn lookup_keys(&self) -> HashSet<K> {
        self.futures.keys().cloned().collect()
    }

    fn complete(&self, values: HashMap<K, V>) {
        for (key, value) in values {
            if let Some(future) = self.futures.get(&key) {
                future.complete(value);
            }
        }
    }

    fn complete_exceptionally(&self, errors: HashMap<K, KafkaError>) {
        for (key, error) in errors {
            if let Some(future) = self.futures.get(&key) {
                future.complete_exceptionally(error);
            }
        }
    }
}
