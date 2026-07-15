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

//! `ShareAcknowledgementEvent` — created by the network task to indicate
//! completion of acknowledgements (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent`.

use indexmap::IndexMap;

use crate::common::TopicIdPartition;
use crate::consumer::internals::acknowledgements::Acknowledgements;

/// This is the class of events created by the network task to indicate
/// completion of acknowledgements.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent`.
#[derive(Debug, Clone)]
pub(crate) struct ShareAcknowledgementEvent {
    acknowledgements_map: IndexMap<TopicIdPartition, Acknowledgements>,
    check_for_renew_acknowledgements: bool,
    acquisition_lock_timeout_ms: Option<i32>,
}

impl ShareAcknowledgementEvent {
    /// Constructs a new [`ShareAcknowledgementEvent`].
    pub(crate) fn new(
        acknowledgements_map: IndexMap<TopicIdPartition, Acknowledgements>,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) -> Self {
        Self {
            acknowledgements_map,
            check_for_renew_acknowledgements,
            acquisition_lock_timeout_ms,
        }
    }

    /// The map of acknowledgements whose delivery has completed.
    pub(crate) fn acknowledgements_map(&self) -> &IndexMap<TopicIdPartition, Acknowledgements> {
        &self.acknowledgements_map
    }

    /// Whether the receiver should check for renew acknowledgements.
    pub(crate) fn check_for_renew_acknowledgements(&self) -> bool {
        self.check_for_renew_acknowledgements
    }

    /// The acquisition-lock timeout advertised by the broker, if any.
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }
}
