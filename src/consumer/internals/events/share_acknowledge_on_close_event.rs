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

//! `ShareAcknowledgeOnCloseEvent` — completable application event that flushes
//! final acknowledgements and closes the share sessions (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeOnCloseEvent`.
//!
//! Java's class `extends CompletableApplicationEvent<Void>`; per
//! `consumer-threading.md` §28 the completable event carries a
//! [`CompletableEventHandle`]. The app-side dispatch lands with the share
//! application-event processor in a later phase; this phase ships the event
//! type only.

use indexmap::IndexMap;
use tokio::sync::oneshot;

use crate::common::{KafkaError, TopicIdPartition};
use crate::consumer::internals::events::completable_event::CompletableEventHandle;
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;

/// Application event carrying the final acknowledgements to commit while
/// closing the consumer, completing once the share sessions are closed.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeOnCloseEvent`.
pub(crate) struct ShareAcknowledgeOnCloseEvent {
    acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    handle: CompletableEventHandle<()>,
}

impl ShareAcknowledgeOnCloseEvent {
    /// Constructs a new [`ShareAcknowledgeOnCloseEvent`], returning the event
    /// and the receiver the application task awaits for completion.
    pub(crate) fn new(
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) -> (Self, oneshot::Receiver<Result<(), KafkaError>>) {
        let (handle, rx) = CompletableEventHandle::new(deadline_ms);
        (Self { acknowledgements_map, handle }, rx)
    }

    /// The final acknowledgements to commit on close.
    pub(crate) fn acknowledgements_map(&self) -> &IndexMap<TopicIdPartition, NodeAcknowledgements> {
        &self.acknowledgements_map
    }

    /// Consumes the event, returning the acknowledgements map.
    pub(crate) fn into_acknowledgements_map(self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.acknowledgements_map
    }

    /// The completion handle the network task completes once close finishes.
    pub(crate) fn handle(&self) -> &CompletableEventHandle<()> {
        &self.handle
    }
}
