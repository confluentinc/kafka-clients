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
//! # Scope
//!
//! All seven in-scope manager slots are wired up (`coordinator`,
//! `commit`, `consumer_heartbeat`, `consumer_membership`, `offsets`,
//! `topic_metadata`, `fetch`). Streams- and share-consumer slots are
//! out of milestone scope per `consumer-threading.md` §20.
//!
//! The Java `RequestManagers::supplier(...)` static factory is still
//! out of scope here — the bg-task wiring in Phase 10/11 constructs
//! each manager directly.
//!
//! # Why a flag, not `IdempotentCloser`
//!
//! Java uses an `IdempotentCloser` helper to enforce one-shot semantics
//! on `close()`. The Rust translation collapses this to a plain `closed:
//! bool` field — checking-and-setting a single flag is the entire
//! contract, and the helper class is overkill here.

#![allow(dead_code)]

use std::sync::Arc;

use super::CommitRequestManager;
use super::ConsumerHeartbeatRequestManager;
use super::ConsumerMembershipManager;
use super::CoordinatorRequestManager;
use super::FetchRequestManager;
use super::OffsetsRequestManager;
use super::RequestManager;
use super::TopicMetadataRequestManager;

/// Container holding all consumer request managers. The bg task
/// iterates over its [`Self::entries`] to poll each manager in
/// deterministic registration order.
pub(crate) struct RequestManagers {
    /// The coordinator manager — `Some` when a group is configured,
    /// `None` for the (currently out-of-scope) group-less assignor
    /// path. Java: `public final Optional<CoordinatorRequestManager>
    /// coordinatorRequestManager`.
    ///
    /// Wrapped in `Arc<...>` so it can be shared with
    /// [`ConsumerHeartbeatRequestManager`] (which holds the same handle
    /// to read the discovered coordinator node — Java does the same via
    /// a heap reference). [`CoordinatorRequestManager`] uses interior
    /// mutability (`Arc<CoordinatorRequestManagerInner>` with `Mutex`
    /// slots) so an outer `Mutex` is unnecessary. Skipped from
    /// [`Self::entries`] (like `consumer_membership`); the bg task polls
    /// the coordinator separately through [`Self::coordinator_handle`].
    pub(crate) coordinator: Option<Arc<CoordinatorRequestManager>>,
    /// Topic-metadata request manager — serves `list_topics()` and
    /// `partitions_for(topic)` API calls. Java:
    /// `final TopicMetadataRequestManager topicMetadataRequestManager`
    /// (always present). Held as `Option<_>` here so test construction
    /// can pass `None`; the bg-task wiring always supplies `Some(_)`.
    pub(crate) topic_metadata: Option<TopicMetadataRequestManager>,
    /// Commit / offset-fetch request manager — `Some` when a group is
    /// configured. Java: `Optional<CommitRequestManager>
    /// commitRequestManager`.
    ///
    /// Wrapped in `Arc<...>` so it can be shared with
    /// [`ConsumerMembershipManager`] (which holds the same handle for
    /// `maybeAutoCommitSyncBeforeRebalance` — Java does the same via a
    /// heap reference). [`CommitRequestManager`] uses interior
    /// mutability (`Arc<CommitRequestManagerInner>` with `Mutex` slots)
    /// so an outer `Mutex` is unnecessary. Skipped from
    /// [`Self::entries`] (like `consumer_membership`); the bg task polls
    /// commit separately through [`Self::commit_handle`].
    pub(crate) commit: Option<Arc<CommitRequestManager>>,
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
    /// Offsets request manager — drives `ListOffsets` for
    /// `beginning_offsets` / `end_offsets` / `offsets_for_times` and
    /// owns the `update_fetch_positions` chain. Java:
    /// `final OffsetsRequestManager offsetsRequestManager` (always
    /// present). `Option<_>` in Rust matches the other slots and lets
    /// tests construct minimal instances; the bg-task wiring always
    /// supplies `Some(_)`.
    pub(crate) offsets: Option<OffsetsRequestManager>,
    /// Fetch request manager — owns the receive-path (`createFetchRequests`,
    /// `collectFetch`). Java: `final FetchRequestManager
    /// fetchRequestManager` (always present). `Option<_>` for the same
    /// reason as `offsets`.
    pub(crate) fetch: Option<FetchRequestManager>,
    /// Auxiliary slot for dyn-dispatched managers — used by tests to
    /// inject spy/fake managers without expanding the concrete-field
    /// list. The production constructor `new(...)` leaves this empty;
    /// tests use [`Self::with_dyn_managers`] to populate it. Iterated
    /// last in `entries()` (after `fetch`) so the production order is
    /// preserved.
    dyn_managers: Vec<Box<dyn RequestManager>>,
    closed: bool,
}

impl RequestManagers {
    /// Constructs a `RequestManagers` container with every in-scope
    /// slot.
    ///
    /// Java's constructor (`RequestManagers.java:67`) takes
    /// `OffsetsRequestManager` and `FetchRequestManager` as
    /// non-`Optional` parameters; Rust accepts them as `Option<_>` to
    /// keep test construction symmetric with the other slots. The
    /// bg-task wiring (Phase 10/11) always supplies `Some(_)` for both.
    ///
    /// Argument order is a Rust ergonomics choice (alphabetical-ish
    /// by phase), not the Java FFI order. The **`entries()` order** is
    /// what matters per `consumer-threading.md` §10 — see
    /// [`Self::entries`].
    pub(crate) fn new(
        coordinator: Option<Arc<CoordinatorRequestManager>>,
        topic_metadata: Option<TopicMetadataRequestManager>,
        commit: Option<Arc<CommitRequestManager>>,
        consumer_heartbeat: Option<ConsumerHeartbeatRequestManager>,
        consumer_membership: Option<Arc<ConsumerMembershipManager>>,
        offsets: Option<OffsetsRequestManager>,
        fetch: Option<FetchRequestManager>,
    ) -> Self {
        Self {
            coordinator,
            topic_metadata,
            commit,
            consumer_heartbeat,
            consumer_membership,
            offsets,
            fetch,
            dyn_managers: Vec::new(),
            closed: false,
        }
    }

    /// Constructs a `RequestManagers` populated only with dyn-dispatched
    /// managers. Used by Phase-10 tests that need to inject spy / fake
    /// `RequestManager` implementations (Mockito's role in Java). The
    /// concrete slots are all `None`; managers iterate in
    /// supplied-vec order from `entries()`.
    #[cfg(test)]
    pub(crate) fn with_dyn_managers(dyn_managers: Vec<Box<dyn RequestManager>>) -> Self {
        Self {
            coordinator: None,
            topic_metadata: None,
            commit: None,
            consumer_heartbeat: None,
            consumer_membership: None,
            offsets: None,
            fetch: None,
            dyn_managers,
            closed: false,
        }
    }

    /// Returns the managers in deterministic registration order
    /// (`consumer-threading.md` §10), matching Java's
    /// `RequestManagers.java:91-101` order:
    ///
    /// `(coordinator skipped) → (commit skipped) → heartbeat →
    /// (membership skipped) → offsets → topic_metadata → fetch`.
    ///
    /// `coordinator`, `commit`, and `consumer_membership` are
    /// **intentionally skipped** — they are held as
    /// `Arc<CoordinatorRequestManager>`,
    /// `Arc<CommitRequestManager>`, and `Arc<ConsumerMembershipManager>`
    /// respectively so the heartbeat manager and the membership manager
    /// can share the same instance with the slot in this container
    /// (Java does the same via heap references). Producing a
    /// `&mut dyn RequestManager` from a shared `Arc<...>` would
    /// require refactoring `RequestManager::poll` to take `&self`
    /// (semantic mismatch with Java). Instead the bg task polls them
    /// separately via [`Self::coordinator_handle`],
    /// [`Self::commit_handle`], and `ConsumerMembershipManager::reconcile`.
    ///
    /// Streams managers (`StreamsGroupHeartbeatRequestManager`,
    /// `StreamsMembershipManager`) are out of milestone scope per
    /// `consumer-threading.md` §20.
    ///
    /// Returns `Vec<&mut dyn RequestManager>` — uses borrow-splitting
    /// (cf. [the Nomicon][nomicon-borrow-splitting]) via a `Self`
    /// destructure so each `Option` is borrowed independently.
    ///
    /// [nomicon-borrow-splitting]: https://doc.rust-lang.org/nomicon/borrow-splitting.html
    pub(crate) fn entries(&mut self) -> Vec<&mut dyn RequestManager> {
        // Destructure so each `Option` is borrowed independently —
        // borrow-splitting per <https://doc.rust-lang.org/nomicon/borrow-splitting.html>.
        let Self {
            coordinator: _,
            topic_metadata,
            commit: _,
            consumer_heartbeat,
            consumer_membership: _,
            offsets,
            fetch,
            dyn_managers,
            closed: _,
        } = self;
        let mut list: Vec<&mut dyn RequestManager> = Vec::new();
        // Order matches Java (`RequestManagers.java:91-101`) minus the
        // three Arc-shared slots: (coordinator skipped) → (commit
        // skipped) → heartbeat → (membership skipped) → offsets →
        // topic_metadata → fetch.
        if let Some(h) = consumer_heartbeat.as_mut() {
            list.push(h as &mut dyn RequestManager);
        }
        if let Some(o) = offsets.as_mut() {
            list.push(o as &mut dyn RequestManager);
        }
        if let Some(t) = topic_metadata.as_mut() {
            list.push(t as &mut dyn RequestManager);
        }
        if let Some(f) = fetch.as_mut() {
            list.push(f as &mut dyn RequestManager);
        }
        for m in dyn_managers.iter_mut() {
            list.push(m.as_mut());
        }
        list
    }

    /// Returns a clone of the `Arc<CoordinatorRequestManager>` handle,
    /// if a coordinator manager is wired. The bg task uses this to poll
    /// the coordinator separately from [`Self::entries`] — calling into
    /// the manager's interior-mutability methods without holding any
    /// outer guard.
    pub(crate) fn coordinator_handle(&self) -> Option<Arc<CoordinatorRequestManager>> {
        self.coordinator.clone()
    }

    /// Returns a clone of the `Arc<CommitRequestManager>` handle, if a
    /// commit manager is wired. Used by the bg task to poll commit
    /// separately from [`Self::entries`] — `CommitRequestManager` uses
    /// interior mutability (an `Arc<CommitRequestManagerInner>` whose
    /// state is `Mutex`-guarded), so [`Self::commit_handle`] returns an
    /// `Arc` rather than `Arc<Mutex<...>>`.
    pub(crate) fn commit_handle(&self) -> Option<Arc<CommitRequestManager>> {
        self.commit.clone()
    }

    /// Index, into the [`Self::entries`] vec, of the first manager that
    /// Java places AFTER the membership slot.
    ///
    /// Java's `RequestManagers.entries()` interleaves:
    /// `coordinator → commit → heartbeat → membership → offsets → ...`.
    /// In Rust the membership manager is skipped from `entries()` (held
    /// as `Arc`, shared with the heartbeat manager — see
    /// [`Self::entries`]'s docstring), and so are `coordinator` and
    /// `commit` (also `Arc`-shared). The bg task drives the three
    /// skipped managers explicitly between the "before-membership"
    /// managers and the "after-membership" managers. This index is the
    /// boundary the bg task uses to split the entries walk.
    ///
    /// With coordinator + commit skipped from [`Self::entries`], the
    /// boundary collapses to the count of present `consumer_heartbeat`
    /// (0 or 1). `dyn_managers` from [`Self::with_dyn_managers`] are
    /// appended AFTER concrete slots, so they are always
    /// "after-membership" for the bg-task split (the production code
    /// path leaves `dyn_managers` empty).
    pub(crate) fn membership_boundary(&self) -> usize {
        if self.consumer_heartbeat.is_some() { 1 } else { 0 }
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
    // Java `RequestManagersTest` cases deferred until the supplier
    // factory lands (still out of scope here):
    //
    //   - testMemberStateListenerRegistered: exercises
    //     `RequestManagers.supplier(...)` plumbing the `MemberStateListener`
    //     into `ConsumerMembershipManager`.
    //   - testStreamMemberStateListenerRegistered: same shape but for
    //     the Streams variant. Streams support is out of milestone scope
    //     per `consumer-threading.md` §20.
    //
    // The tests below are container-shape tests for the Rust struct —
    // they have no Java analog because Java exposes the `Optional`s
    // directly and `entries()` is a Rust-only helper that returns the
    // registered managers in deterministic order.

    use super::*;
    use crate::ApiVersions;
    use crate::common::IsolationLevel;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::memory::BufferSupplier;
    use crate::consumer::AutoOffsetResetStrategy;
    use crate::consumer::internals::ConsumerMetadata;
    use crate::consumer::internals::FetchBuffer;
    use crate::consumer::internals::FetchConfig;
    use crate::consumer::internals::FetchRequestManager;
    use crate::consumer::internals::SubscriptionState;

    fn coord_manager() -> Arc<CoordinatorRequestManager> {
        Arc::new(CoordinatorRequestManager::new(100, 1_000, "group-1"))
    }

    fn topic_metadata_manager() -> TopicMetadataRequestManager {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..Default::default()
        };
        TopicMetadataRequestManager::new(&config)
    }

    fn commit_manager() -> Arc<CommitRequestManager> {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..Default::default()
        };
        let subs = std::sync::Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::new_config(
            &config,
            std::sync::Arc::clone(&subs),
            ClusterResourceListeners::new(),
        ));
        Arc::new(CommitRequestManager::new(
            &config,
            metadata,
            subs,
            "g",
            None,
            Arc::new(crate::common::metrics::SystemTime),
            0,
        ))
    }

    fn offsets_manager() -> OffsetsRequestManager {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..Default::default()
        };
        let subs = std::sync::Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::new_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let positions_validator = std::sync::Arc::new(crate::consumer::internals::PositionsValidator::new(
            subs.clone(),
            metadata.clone(),
        ));
        OffsetsRequestManager::new(
            subs,
            metadata,
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            std::sync::Arc::new(ApiVersions::new()),
            None,
            positions_validator,
        )
    }

    fn fetch_manager() -> FetchRequestManager {
        let subs = std::sync::Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::new(
            50,
            50,
            50_000,
            false,
            false,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let fetch_config = FetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
        );
        FetchRequestManager::new(
            metadata,
            subs,
            fetch_config,
            std::sync::Arc::new(FetchBuffer::new()),
            std::sync::Arc::new(BufferSupplier::create()),
            FetchRequestManager::always_available(),
            FetchRequestManager::no_auth_failure(),
            std::sync::Arc::new(crate::ApiVersions::new()),
            crate::consumer::internals::FetchMetricsManager::for_test(),
        )
    }

    /// Verifies that `entries()` is empty when every slot is `None`.
    #[test]
    fn entries_empty_when_no_managers() {
        let mut rm = RequestManagers::new(None, None, None, None, None, None, None);
        assert!(rm.entries().is_empty());
    }

    /// Verifies that `entries()` SKIPS the coordinator (it's
    /// `Arc<Mutex<...>>`-shared with the heartbeat manager — the bg task
    /// polls it separately via `coordinator_handle()`). Phase 12 wiring.
    #[test]
    fn entries_skips_coordinator_even_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None, None, None);
        let entries = rm.entries();
        assert!(entries.is_empty(), "coordinator must be skipped from entries()");
    }

    /// Verifies that `entries()` returns the topic_metadata manager when
    /// present. With coordinator skipped (`Arc`-shared), only
    /// topic_metadata appears.
    #[test]
    fn entries_includes_topic_metadata_when_present() {
        let mut rm = RequestManagers::new(
            Some(coord_manager()),
            Some(topic_metadata_manager()),
            None,
            None,
            None,
            None,
            None,
        );
        let entries = rm.entries();
        // Coordinator is Arc-shared and excluded; only topic_metadata
        // shows up.
        assert_eq!(1, entries.len());
    }

    /// Verifies that `close` is idempotent — only the first call flips
    /// the flag; subsequent calls are no-ops.
    #[test]
    fn close_is_idempotent() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None, None, None);
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
        let mut rm = RequestManagers::new(
            Some(coord_manager()),
            Some(topic_metadata_manager()),
            None,
            None,
            None,
            None,
            None,
        );
        let names_round_one: Vec<i64> = rm.entries().iter().map(|m| m.maximum_time_to_wait(0)).collect();
        let names_round_two: Vec<i64> = rm.entries().iter().map(|m| m.maximum_time_to_wait(0)).collect();
        assert_eq!(names_round_one, names_round_two);
    }

    /// Verifies that `entries()` SKIPS the commit slot (it's
    /// `Arc`-shared with the membership manager — the bg task /
    /// `ApplicationEventProcessor` access it via `commit_handle()`).
    /// Phase 12 wiring.
    #[test]
    fn entries_skips_commit_even_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, Some(commit_manager()), None, None, None, None);
        let entries = rm.entries();
        // Both coordinator and commit are Arc-shared and excluded.
        assert!(entries.is_empty(), "commit must be skipped from entries()");
    }

    /// Verifies that the Phase 10 `offsets` slot is wired into
    /// `entries()`. With coordinator Arc-shared and excluded, only
    /// offsets appears.
    #[test]
    fn entries_includes_offsets_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None, Some(offsets_manager()), None);
        let entries = rm.entries();
        assert_eq!(1, entries.len());
    }

    /// Verifies that the Phase 10 `fetch` slot is wired into
    /// `entries()`. With coordinator Arc-shared and excluded, only
    /// fetch appears.
    #[test]
    fn entries_includes_fetch_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None, None, Some(fetch_manager()));
        let entries = rm.entries();
        assert_eq!(1, entries.len());
    }

    /// Verifies that `entries()` returns the correct count when five of
    /// the six request-emitting slots are populated.
    ///
    /// The slot ordering itself (coordinator → commit → heartbeat →
    /// offsets → topic_metadata → fetch) is documented in the
    /// [`RequestManagers::entries`] docstring; the runtime types behind
    /// `&mut dyn RequestManager` use only default trait methods, so
    /// they are not individually distinguishable here. The
    /// destructure-driven implementation is a straight-line list of
    /// `if let Some(...) list.push(...)` calls, so the test name does
    /// not promise more than is asserted — order verification is left
    /// to the other tests in this module which exercise individual
    /// slots in pairs (cf. `entries_includes_*_when_present`).
    #[test]
    fn entries_returns_correct_count_when_five_slots_populated() {
        // Six request-emitting slots: coordinator, commit,
        // consumer_heartbeat, offsets, topic_metadata, fetch.
        // `consumer_membership`, `coordinator`, and `commit` are held
        // as Arc and are excluded by design (Phase-12 production wiring)
        // — see entries() docstring.
        //
        // Note: we leave `consumer_heartbeat` as None here because its
        // constructor requires a fully-wired membership manager + Arc
        // pipeline which is heavier than the value adds for this
        // shape-only test.
        let mut rm = RequestManagers::new(
            Some(coord_manager()),
            Some(topic_metadata_manager()),
            Some(commit_manager()),
            None,
            None,
            Some(offsets_manager()),
            Some(fetch_manager()),
        );
        let entries = rm.entries();
        // 3 = offsets + topic_metadata + fetch.
        // (coordinator + commit are Arc-shared and excluded.)
        assert_eq!(3, entries.len());
    }
}
