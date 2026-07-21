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

//! `ShareAcknowledgeAsyncEvent` — application event for `commitAsync`-style
//! share acknowledgements (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeAsyncEvent`.
//!
//! Java's `ShareAcknowledgeAsyncEvent extends ApplicationEvent` (a bare,
//! non-completable event — an async acknowledge does not block the caller).
//! The app-side dispatch lands with the share application-event processor in
//! a later phase; this phase ships the event type only.

use indexmap::IndexMap;

use crate::common::TopicIdPartition;
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;

/// Application event carrying acknowledgements to commit asynchronously.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeAsyncEvent`.
pub(crate) struct ShareAcknowledgeAsyncEvent {
    acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    deadline_ms: i64,
}

impl ShareAcknowledgeAsyncEvent {
    /// Constructs a new [`ShareAcknowledgeAsyncEvent`].
    pub(crate) fn new(
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) -> Self {
        Self { acknowledgements_map, deadline_ms }
    }

    /// The acknowledgements to commit.
    pub(crate) fn acknowledgements_map(&self) -> &IndexMap<TopicIdPartition, NodeAcknowledgements> {
        &self.acknowledgements_map
    }

    /// Consumes the event, returning the acknowledgements map.
    pub(crate) fn into_acknowledgements_map(self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.acknowledgements_map
    }

    /// Time (ms) until which the request will be retried on retriable errors.
    pub(crate) fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }
}
