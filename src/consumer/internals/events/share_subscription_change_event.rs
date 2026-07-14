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

//! `ShareSubscriptionChangeEvent` — completable application event raised when
//! a share consumer's subscription changes (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeEvent`.
//!
//! Java's class `extends CompletableApplicationEvent<Void>`; per
//! `consumer-threading.md` §28 the completable event carries a
//! [`CompletableEventHandle`]. The app-side dispatch lands with the share
//! application-event processor in a later phase; this phase ships the event
//! type only.

use std::collections::HashSet;

use tokio::sync::oneshot;

use crate::common::KafkaError;
use crate::consumer::internals::events::completable_event::CompletableEventHandle;

/// Application event indicating that the subscription state has changed,
/// triggered when a user calls the subscribe API. This will make the consumer
/// join a share group if not part of it yet, or just send the updated
/// subscription to the broker if it's already a member of the group.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeEvent`.
pub(crate) struct ShareSubscriptionChangeEvent {
    topics: HashSet<String>,
    handle: CompletableEventHandle<()>,
}

impl ShareSubscriptionChangeEvent {
    /// Constructs a new [`ShareSubscriptionChangeEvent`], returning the event
    /// and the receiver the application task awaits for completion.
    ///
    /// Java fixes the deadline at `Long.MAX_VALUE`.
    pub(crate) fn new(topics: HashSet<String>) -> (Self, oneshot::Receiver<Result<(), KafkaError>>) {
        let (handle, rx) = CompletableEventHandle::new(i64::MAX);
        (Self { topics, handle }, rx)
    }

    /// The set of topics the consumer is now subscribed to.
    pub(crate) fn topics(&self) -> &HashSet<String> {
        &self.topics
    }

    /// The completion handle the network task completes once the subscription
    /// change has been applied.
    pub(crate) fn handle(&self) -> &CompletableEventHandle<()> {
        &self.handle
    }

    /// Consumes the event, returning the owned completion handle.
    pub(crate) fn into_handle(self) -> CompletableEventHandle<()> {
        self.handle
    }
}
