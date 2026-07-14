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

//! `ShareUnsubscribeEvent` — completable application event raised when a
//! share consumer unsubscribes (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeEvent`.
//!
//! Java's class `extends CompletableApplicationEvent<Void>`; per
//! `consumer-threading.md` §28 the completable event carries a
//! [`CompletableEventHandle`]. The app-side dispatch lands with the share
//! application-event processor in a later phase; this phase ships the event
//! type only.

use tokio::sync::oneshot;

use crate::common::KafkaError;
use crate::consumer::internals::events::completable_event::CompletableEventHandle;

/// Application event triggered when a user calls the unsubscribe API. This
/// will make the consumer release all its assignments and send a heartbeat
/// request to leave the share group. The completion handle completes when the
/// invocation of callbacks to release complete and the leave-group heartbeat
/// has been sent out.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeEvent`.
pub(crate) struct ShareUnsubscribeEvent {
    handle: CompletableEventHandle<()>,
}

impl ShareUnsubscribeEvent {
    /// Constructs a new [`ShareUnsubscribeEvent`], returning the event and the
    /// receiver the application task awaits for completion.
    pub(crate) fn new(deadline_ms: i64) -> (Self, oneshot::Receiver<Result<(), KafkaError>>) {
        let (handle, rx) = CompletableEventHandle::new(deadline_ms);
        (Self { handle }, rx)
    }

    /// The completion handle the network task completes once the unsubscribe
    /// has been processed.
    pub(crate) fn handle(&self) -> &CompletableEventHandle<()> {
        &self.handle
    }

    /// Consumes the event, returning the owned completion handle.
    pub(crate) fn into_handle(self) -> CompletableEventHandle<()> {
        self.handle
    }
}
