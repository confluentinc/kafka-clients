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

/// The broker id used for keys that have no cached mapping.
///
/// Corresponds to `AdminApiFuture.UNKNOWN_BROKER_ID`.
pub(crate) const UNKNOWN_BROKER_ID: i32 = -1;

/// The future bundle that the [`AdminApiDriver`](super::admin_api_driver::AdminApiDriver)
/// completes as keys are resolved.
///
/// Corresponds to `AdminApiFuture<K, V>`. Kept a plain (non-`async`) trait per
/// `.claude/rules/admin-client.md` §2 — the driver completes the futures on the
/// background task. `SimpleAdminApiFuture` is not translated; the only future in
/// scope is `PartitionLeaderStrategy::PartitionLeaderFuture`.
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
