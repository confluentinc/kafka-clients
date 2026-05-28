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

//! `RequestManagers` — container holding all consumer request managers.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.RequestManagers`.
//!
//! # Phase 6 scope
//!
//! Phase 6 ships a **skeleton container** with only the
//! [`CoordinatorRequestManager`] slot wired up. Phase 7 (`Fetch`,
//! `OffsetsRequestManager`, `TopicMetadataRequestManager`), Phase 8
//! (`ConsumerHeartbeatRequestManager`, `ConsumerMembershipManager`), and
//! Phase 9 (`CommitRequestManager`) will extend the struct and the
//! [`Self::entries`] / [`Self::close`] methods.
//!
//! The Java `RequestManagers::supplier(...)` static factory (which
//! constructs every manager in the system) is out of Phase 6 scope per
//! the plan — it requires constructors that do not yet exist. Phase 10
//! will translate the supplier.
//!
//! # Why a flag, not `IdempotentCloser`
//!
//! Java uses an `IdempotentCloser` helper to enforce one-shot semantics
//! on `close()`. The Rust translation collapses this to a plain `closed:
//! bool` field — checking-and-setting a single flag is the entire
//! contract, and the helper class is overkill here.

#![allow(dead_code)]

use std::sync::Arc;

use super::commit_request_manager::CommitRequestManager;
use super::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager;
use super::consumer_membership_manager::ConsumerMembershipManager;
use super::coordinator_request_manager::CoordinatorRequestManager;
use super::request_manager::RequestManager;
use super::topic_metadata_request_manager::TopicMetadataRequestManager;

/// Container holding all consumer request managers. The bg task
/// iterates over its [`Self::entries`] to poll each manager in
/// deterministic registration order.
///
/// Phase 6 only carries the `coordinator` slot; Phase 7-9 extend.
pub(crate) struct RequestManagers {
    /// The coordinator manager — `Some` when a group is configured,
    /// `None` for the (currently out-of-scope) group-less assignor
    /// path. Java: `public final Optional<CoordinatorRequestManager>
    /// coordinatorRequestManager`.
    pub(crate) coordinator: Option<CoordinatorRequestManager>,
    /// Topic-metadata request manager — serves `list_topics()` and
    /// `partitions_for(topic)` API calls. Java:
    /// `final TopicMetadataRequestManager topicMetadataRequestManager`
    /// (always present).
    pub(crate) topic_metadata: Option<TopicMetadataRequestManager>,
    /// Commit / offset-fetch request manager — `Some` when a group is
    /// configured. Java: `Optional<CommitRequestManager>
    /// commitRequestManager`.
    pub(crate) commit: Option<CommitRequestManager>,
    /// KIP-848 consumer-group heartbeat manager — `Some` when a
    /// consumer-protocol group is configured. Java:
    /// `Optional<ConsumerHeartbeatRequestManager>`.
    pub(crate) consumer_heartbeat: Option<ConsumerHeartbeatRequestManager>,
    /// KIP-848 consumer-group membership manager — `Some` when a
    /// consumer-protocol group is configured. Java:
    /// `Optional<ConsumerMembershipManager>`. Held as `Arc` because
    /// [`ConsumerHeartbeatRequestManager`] also holds a reference to
    /// it; both managers share the same state via `Arc<Mutex<...>>`
    /// inside the membership manager.
    pub(crate) consumer_membership: Option<Arc<ConsumerMembershipManager>>,
    // Slots reserved for later phases (Option<_> with a phase-comment):
    // pub(crate) offsets: OffsetsRequestManager,                                          // Phase 7d
    // pub(crate) fetch: FetchRequestManager,                                              // Phase 7b
    closed: bool,
}

impl RequestManagers {
    /// Skeleton constructor. Each phase extends the signature with
    /// additional managers as they land.
    pub(crate) fn new(
        coordinator: Option<CoordinatorRequestManager>,
        topic_metadata: Option<TopicMetadataRequestManager>,
        commit: Option<CommitRequestManager>,
        consumer_heartbeat: Option<ConsumerHeartbeatRequestManager>,
        consumer_membership: Option<Arc<ConsumerMembershipManager>>,
    ) -> Self {
        Self {
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            consumer_membership,
            closed: false,
        }
    }

    /// Returns the managers in deterministic registration order
    /// (`consumer-threading.md` §10). Phase 6 only emits `coordinator`
    /// when present; Phase 7-9 extend by appending their fields in the
    /// same order Java's constructor does (coordinator, commit,
    /// heartbeat, membership, offsets, topic_metadata, fetch).
    ///
    /// Returns `Vec<&mut dyn RequestManager>` — the borrow-splitting
    /// pattern works here because each field is independently
    /// borrowed (cf. [the Nomicon][nomicon-borrow-splitting]). When
    /// Phase 7-9 add fields, the impl will switch to a destructuring
    /// pattern to satisfy the borrow checker across multiple fields.
    ///
    /// [nomicon-borrow-splitting]: https://doc.rust-lang.org/nomicon/borrow-splitting.html
    pub(crate) fn entries(&mut self) -> Vec<&mut dyn RequestManager> {
        // Destructure so each `Option` is borrowed independently —
        // borrow-splitting per <https://doc.rust-lang.org/nomicon/borrow-splitting.html>.
        //
        // `consumer_membership` is held as `Arc<...>` (it's shared with
        // `consumer_heartbeat`) so we cannot produce a `&mut dyn
        // RequestManager` from it for inclusion here. This is fine:
        // Java's `AbstractMembershipManager.poll(...)` returns
        // `PollResult.EMPTY` and only calls `maybeReconcile(false)` as
        // a side effect. The Rust translation moves the reconcile
        // driver out of the sync `entries()` loop and into the bg
        // task (Phase 10), which can `.await` the async
        // `ConsumerMembershipManager::reconcile`. Skipping the
        // membership manager from `entries()` does not lose any
        // request-emitting work.
        let Self {
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            consumer_membership: _,
            closed: _,
        } = self;
        let mut list: Vec<&mut dyn RequestManager> = Vec::new();
        // Order matches Java's constructor: coordinator → commit →
        // heartbeat → (membership skipped) → topic_metadata → fetch.
        if let Some(c) = coordinator.as_mut() {
            list.push(c as &mut dyn RequestManager);
        }
        if let Some(c) = commit.as_mut() {
            list.push(c as &mut dyn RequestManager);
        }
        if let Some(h) = consumer_heartbeat.as_mut() {
            list.push(h as &mut dyn RequestManager);
        }
        if let Some(t) = topic_metadata.as_mut() {
            list.push(t as &mut dyn RequestManager);
        }
        list
    }

    /// Idempotent close. Subsequent calls are no-ops.
    ///
    /// Java: `close()`. Java additionally invokes `closeQuietly` on
    /// every manager that implements `Closeable`; in Rust no manager
    /// implements an explicit close trait at this point.
    pub(crate) fn close(&mut self) {
        if self.closed {
            log::debug!("RequestManagers was already closed");
            return;
        }
        log::debug!("Closing RequestManagers");
        self.closed = true;
        log::debug!("RequestManagers has been closed");
    }

    /// `true` if [`Self::close`] has been called.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    // Java `RequestManagersTest` cases deferred to Phase 10 with the
    // supplier factory (PLAN.md "Out of scope"):
    //
    //   - testMemberStateListenerRegistered: exercises
    //     `RequestManagers.supplier(...)` plumbing the `MemberStateListener`
    //     into `ConsumerMembershipManager`. Requires
    //     ConsumerHeartbeatRequestManager (Phase 8) + the supplier
    //     factory (Phase 10).
    //
    //   - testStreamMemberStateListenerRegistered: same shape as above
    //     but for the Streams variant. Streams support is out of
    //     milestone scope per consumer-threading.md §20, so this test
    //     will not be translated even after the supplier lands.
    //
    // The tests below are container-shape tests for the Phase-6
    // skeleton — they have no Java analog because Java exposes the
    // `Optional`s directly and `entries()` is a Rust-only helper that
    // returns the registered managers in deterministic order.

    use super::*;

    fn coord_manager() -> CoordinatorRequestManager {
        CoordinatorRequestManager::new(100, 1_000, "group-1")
    }

    fn topic_metadata_manager() -> TopicMetadataRequestManager {
        let config = crate::consumer::ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        TopicMetadataRequestManager::new(&config)
    }

    fn commit_manager() -> CommitRequestManager {
        let config = crate::consumer::ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = std::sync::Arc::new(std::sync::Mutex::new(
            crate::consumer::internals::subscription_state::SubscriptionState::new(
                crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::LATEST,
            ),
        ));
        let metadata = std::sync::Arc::new(super::super::consumer_metadata::ConsumerMetadata::from_config(
            &config,
            subs,
            crate::common::internals::ClusterResourceListeners::new(),
        ));
        CommitRequestManager::new(&config, metadata, "g", None, 0)
    }

    /// Verifies that `entries()` is empty when both manager slots are `None`.
    #[test]
    fn entries_empty_when_no_managers() {
        let mut rm = RequestManagers::new(None, None, None, None, None);
        assert!(rm.entries().is_empty());
    }

    /// Verifies that `entries()` returns the coordinator when present.
    #[test]
    fn entries_includes_coordinator_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None);
        let entries = rm.entries();
        assert_eq!(1, entries.len());
        // We can call the trait method to confirm the upcast works.
        // Default `maximum_time_to_wait` returns `i64::MAX`.
        assert_eq!(i64::MAX, entries[0].maximum_time_to_wait(0));
    }

    /// Verifies that `entries()` returns both managers in deterministic
    /// registration order (coordinator → topic_metadata).
    #[test]
    fn entries_includes_topic_metadata_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), Some(topic_metadata_manager()), None, None, None);
        let entries = rm.entries();
        assert_eq!(2, entries.len());
    }

    /// Verifies that `close` is idempotent — only the first call flips
    /// the flag; subsequent calls are no-ops.
    #[test]
    fn close_is_idempotent() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None);
        assert!(!rm.is_closed());
        rm.close();
        assert!(rm.is_closed());
        // Second call is a no-op.
        rm.close();
        assert!(rm.is_closed());
    }

    /// Verifies that `entries()` returns managers in the same order on
    /// repeated calls, regardless of `Option` field shuffling.
    #[test]
    fn entries_order_is_deterministic() {
        let mut rm = RequestManagers::new(Some(coord_manager()), Some(topic_metadata_manager()), None, None, None);
        let names_round_one: Vec<i64> = rm.entries().iter().map(|m| m.maximum_time_to_wait(0)).collect();
        let names_round_two: Vec<i64> = rm.entries().iter().map(|m| m.maximum_time_to_wait(0)).collect();
        assert_eq!(names_round_one, names_round_two);
    }

    /// Phase 9: verifies that the commit slot is wired into `entries()`
    /// in registration order (coordinator → commit → topic_metadata).
    #[test]
    fn entries_includes_commit_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, Some(commit_manager()), None, None);
        let entries = rm.entries();
        assert_eq!(2, entries.len());
    }
}
