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

//! `SharePollEvent` — application event marking a share-consumer poll
//! (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.SharePollEvent`.
//!
//! Java's `SharePollEvent extends ApplicationEvent` (a bare, non-completable
//! event). The app-side dispatch lands with the share application-event
//! processor in a later phase; this phase ships the event type only.

/// Application event carrying the current poll time, used to keep the
/// share-consumer poll timer fresh.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.SharePollEvent`.
pub(crate) struct SharePollEvent {
    poll_time_ms: i64,
}

impl SharePollEvent {
    /// Constructs a new [`SharePollEvent`].
    pub(crate) fn new(poll_time_ms: i64) -> Self {
        Self { poll_time_ms }
    }

    /// The time (ms) at which the poll was issued.
    pub(crate) fn poll_time_ms(&self) -> i64 {
        self.poll_time_ms
    }
}
