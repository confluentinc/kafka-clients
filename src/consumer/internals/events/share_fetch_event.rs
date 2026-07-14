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

//! `ShareFetchEvent` — application event requesting a share fetch (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareFetchEvent`.
//!
//! Java's `ShareFetchEvent extends ApplicationEvent` (a bare, non-completable
//! event). The app-side dispatch that turns this into a
//! [`crate::consumer::internals::share_consume_request_manager::ShareConsumeRequestManager::fetch`]
//! call lands with the share application-event processor in a later phase;
//! this phase ships the event type only.

use indexmap::IndexMap;

use crate::common::TopicIdPartition;
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;

/// Application event carrying the piggyback acknowledgements to send with
/// the next share fetch.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareFetchEvent`.
pub(crate) struct ShareFetchEvent {
    acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
}

impl ShareFetchEvent {
    /// Constructs a new [`ShareFetchEvent`].
    pub(crate) fn new(acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>) -> Self {
        Self { acknowledgements_map }
    }

    /// The acknowledgements to piggyback on the next share fetch.
    pub(crate) fn acknowledgements_map(&self) -> &IndexMap<TopicIdPartition, NodeAcknowledgements> {
        &self.acknowledgements_map
    }

    /// Consumes the event, returning the acknowledgements map.
    pub(crate) fn into_acknowledgements_map(self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.acknowledgements_map
    }
}
