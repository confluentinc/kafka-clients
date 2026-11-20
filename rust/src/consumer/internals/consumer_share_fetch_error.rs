// Copyright 2026 Confluent Inc.
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

//! `ConsumerShareFetchError` — a share fetch whose collection failed, with
//! the cause.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareFetchException`.

use crate::common::Error;
use crate::consumer::internals::ShareFetch;

/// The share fetch `ShareFetchCollector#collect` had built when a batch's
/// failure ended it, and that failure.
///
/// A crate-private carrier, not an [`Error`] variant (ledger gap G9; the
/// `Consumer` prefix is CLAUDE.md §2's for an error outside `common`). Java's
/// class extends `SerializationException`, but only `ShareConsumerImpl#poll`
/// catches it: it makes the carried fetch the current one, so that the
/// failed records can be acknowledged, and rethrows the cause, which is what
/// reaches the application (a `RecordDeserializationException` or a
/// `CorruptRecordException`), unchanged.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchException")]
pub(crate) struct ConsumerShareFetchError<K, V> {
    share_fetch: ShareFetch<K, V>,
    cause: Error,
}

impl<K, V> ConsumerShareFetchError<K, V> {
    /// A failure with the fetch built so far and its cause.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchException#ShareFetchException")]
    pub(crate) fn new(share_fetch: ShareFetch<K, V>, cause: Error) -> Self {
        Self { share_fetch, cause }
    }

    /// The fetch built so far.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchException#shareFetch")]
    pub(crate) fn share_fetch(&self) -> &ShareFetch<K, V> {
        &self.share_fetch
    }

    /// The failure that ended the collection.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchException#cause")]
    pub(crate) fn cause(&self) -> &Error {
        &self.cause
    }

    /// The fetch and the cause, by value, for `ShareConsumerImpl#poll`'s
    /// `currentFetch = e.shareFetch(); throw e.cause();`. Rust-only: Java
    /// shares the references.
    pub(crate) fn into_parts(self) -> (ShareFetch<K, V>, Error) {
        (self.share_fetch, self.cause)
    }
}

impl<K, V> std::fmt::Debug for ConsumerShareFetchError<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerShareFetchError")
            .field("share_fetch", &self.share_fetch)
            .field("cause", &self.cause)
            .finish()
    }
}
