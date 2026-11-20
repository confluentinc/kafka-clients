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

//! `ShareAcknowledgementEvent` — completed acknowledgements handed from the
//! background task to the application task.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent`.
//! Java's class extends nothing: it is **not** a `BackgroundEvent`, and it
//! travels on its own queue ([`super::ShareAcknowledgementEventHandler`],
//! `share-consumer.md` §5).

use std::collections::HashMap;

use crate::common::TopicIdPartition;
use crate::consumer::internals::Acknowledgements;

/// The acknowledgements a share acknowledge request (or a share fetch
/// carrying piggy-backed acknowledgements) completed, for the acknowledgement
/// commit callback and for renewing records.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent")]
pub(crate) struct ShareAcknowledgementEvent {
    acknowledgements_map: HashMap<TopicIdPartition, Acknowledgements>,
    check_for_renew_acknowledgements: bool,
    acquisition_lock_timeout_ms: Option<i32>,
}

impl ShareAcknowledgementEvent {
    /// An event for `acknowledgements_map`.
    ///
    /// # Translation notes
    ///
    /// Java copies the map (`Map.copyOf`) so the caller cannot change the
    /// event; the event takes ownership of it instead.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent#ShareAcknowledgementEvent"
    )]
    pub(crate) fn new(
        acknowledgements_map: HashMap<TopicIdPartition, Acknowledgements>,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) -> Self {
        Self {
            acknowledgements_map,
            check_for_renew_acknowledgements,
            acquisition_lock_timeout_ms,
        }
    }

    /// The completed acknowledgements, by partition.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent#acknowledgementsMap")]
    pub(crate) fn acknowledgements_map(&self) -> &HashMap<TopicIdPartition, Acknowledgements> {
        &self.acknowledgements_map
    }

    /// Whether the application task checks the acknowledgements for `RENEW`s
    /// and renews the records they name.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent#checkForRenewAcknowledgements"
    )]
    pub(crate) fn check_for_renew_acknowledgements(&self) -> bool {
        self.check_for_renew_acknowledgements
    }

    /// The acquisition lock timeout the broker returned with the response, if
    /// any.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent#acquisitionLockTimeoutMs"
    )]
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::consumer::AcknowledgeType;

    /// Rust-only: Java has no test of its own for this class. The event keeps
    /// the map and the two flags it is given.
    #[test]
    fn test_event_keeps_its_fields() {
        let tip = TopicIdPartition::with_partition_topic(Uuid::random_uuid(), 0, "t");
        let mut acknowledgements = Acknowledgements::empty();
        acknowledgements.add(1, Some(AcknowledgeType::Renew));
        let map = HashMap::from([(tip.clone(), acknowledgements)]);

        let event = ShareAcknowledgementEvent::new(map, true, Some(30_000));

        assert_eq!(1, event.acknowledgements_map().len());
        assert_eq!(1, event.acknowledgements_map()[&tip].size());
        assert!(event.check_for_renew_acknowledgements());
        assert_eq!(Some(30_000), event.acquisition_lock_timeout_ms());
    }
}
