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

//! `ShareAcknowledgeSyncEvent` — completable application event for
//! `commitSync`-style share acknowledgements (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeSyncEvent`.
//!
//! Java's class `extends CompletableApplicationEvent<Map<TopicIdPartition,
//! Acknowledgements>>`; per `consumer-threading.md` §28 the completable event
//! carries a [`CompletableEventHandle`] with the same generic payload. The
//! app-side dispatch lands with the share application-event processor in a
//! later phase; this phase ships the event type only.

use indexmap::IndexMap;
use tokio::sync::oneshot;

use crate::common::{KafkaError, TopicIdPartition};
use crate::consumer::internals::acknowledgements::Acknowledgements;
use crate::consumer::internals::events::completable_event::CompletableEventHandle;
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;

/// The result payload the sync acknowledge future completes with.
pub(crate) type ShareAcknowledgeSyncResult = IndexMap<TopicIdPartition, Acknowledgements>;

/// Application event carrying acknowledgements to commit synchronously,
/// completing when all the acknowledgements have finished.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeSyncEvent`.
pub(crate) struct ShareAcknowledgeSyncEvent {
    acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    handle: CompletableEventHandle<ShareAcknowledgeSyncResult>,
}

impl ShareAcknowledgeSyncEvent {
    /// Constructs a new [`ShareAcknowledgeSyncEvent`], returning the event and
    /// the receiver the application task awaits for the acknowledge result.
    pub(crate) fn new(
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) -> (Self, oneshot::Receiver<Result<ShareAcknowledgeSyncResult, KafkaError>>) {
        let (handle, rx) = CompletableEventHandle::new(deadline_ms);
        (Self { acknowledgements_map, handle }, rx)
    }

    /// The acknowledgements to commit.
    pub(crate) fn acknowledgements_map(&self) -> &IndexMap<TopicIdPartition, NodeAcknowledgements> {
        &self.acknowledgements_map
    }

    /// Consumes the event, returning the acknowledgements map.
    pub(crate) fn into_acknowledgements_map(self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.acknowledgements_map
    }

    /// The completion handle the network task completes with the per-partition
    /// acknowledge results.
    pub(crate) fn handle(&self) -> &CompletableEventHandle<ShareAcknowledgeSyncResult> {
        &self.handle
    }

    /// Consumes the event, returning the acknowledgements map and the owned
    /// completion handle. Used by the `ApplicationEventProcessor` to dispatch
    /// the acks to the request manager and bridge the manager's response future
    /// to this event's handle.
    pub(crate) fn into_parts(
        self,
    ) -> (
        IndexMap<TopicIdPartition, NodeAcknowledgements>,
        CompletableEventHandle<ShareAcknowledgeSyncResult>,
    ) {
        (self.acknowledgements_map, self.handle)
    }
}
