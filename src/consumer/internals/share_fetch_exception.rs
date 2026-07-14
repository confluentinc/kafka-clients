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

//! Error carrying a partially-collected [`ShareFetch`] alongside the cause.
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.ShareFetchException`.

#![allow(dead_code)]

use crate::common::KafkaError;

use super::share_fetch::ShareFetch;

/// Raised by [`ShareFetchCollector::collect`] when collecting records fails.
/// It carries the [`ShareFetch`] accumulated so far, so the caller can still
/// deliver already-collected records and later route acknowledgements, plus
/// the underlying [`KafkaError`] cause.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareFetchException`.
///
/// # Deviation from Java
///
/// Java's `ShareFetchException` extends `SerializationException` (a
/// `KafkaException`) and holds `ShareFetch<?, ?>`. In Rust it is a standalone
/// generic struct returned as the `Err` variant of
/// [`ShareFetchCollector::collect`]. Java's `collect` can throw either a bare
/// `KafkaException` (from `initialize`) or a `ShareFetchException` (from the
/// records branch); the Rust translation unifies both into this single `Err`
/// carrier — for the bare-error cases the carried `share_fetch` is whatever had
/// been accumulated (empty in the single-partition error tests) and `cause` is
/// the raw error. The downstream `ShareConsumerImpl` (later phase) mirrors
/// Java's `catch (ShareFetchException e) { currentFetch = e.shareFetch(); throw
/// e.cause(); }`, so the observable behaviour (the user sees `cause`, the
/// partial fetch is retained) is preserved.
///
/// [`ShareFetchCollector::collect`]: super::share_fetch_collector::ShareFetchCollector::collect
pub(crate) struct ShareFetchException<K, V> {
    share_fetch: ShareFetch<K, V>,
    cause: KafkaError,
}

impl<K, V> ShareFetchException<K, V> {
    /// Constructs a new `ShareFetchException`.
    ///
    /// Mirrors Java's `ShareFetchException(ShareFetch<?, ?> shareFetch, KafkaException cause)`.
    pub(crate) fn new(share_fetch: ShareFetch<K, V>, cause: KafkaError) -> Self {
        Self { share_fetch, cause }
    }

    /// Returns the accumulated share fetch. Mirrors Java's `ShareFetch<?, ?> shareFetch()`.
    pub(crate) fn share_fetch(&self) -> &ShareFetch<K, V> {
        &self.share_fetch
    }

    /// Consumes this error, returning the owned share fetch and cause. Rust
    /// convenience for the caller that needs to move both out (`ShareConsumerImpl`
    /// stashes the fetch as `currentFetch` and rethrows the cause).
    pub(crate) fn into_parts(self) -> (ShareFetch<K, V>, KafkaError) {
        (self.share_fetch, self.cause)
    }

    /// Returns the cause. Mirrors Java's `KafkaException cause()`.
    pub(crate) fn cause(&self) -> &KafkaError {
        &self.cause
    }
}

impl<K, V> std::fmt::Debug for ShareFetchException<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareFetchException")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}

impl<K, V> std::fmt::Display for ShareFetchException<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Java's `SerializationException` message is null (no message passed to
        // super); delegate to the cause for a meaningful description.
        write!(f, "{}", self.cause.message())
    }
}
