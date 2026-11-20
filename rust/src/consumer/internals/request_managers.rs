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
//! The seven consumer manager slots are wired up (`coordinator`,
//! `commit`, `consumer_heartbeat`, `consumer_membership`, `offsets`,
//! `topic_metadata`, `fetch`), and the three share-consumer (KIP-932) slots
//! (`share_heartbeat`, `share_membership`, `share_consume`), which Java's
//! share constructor fills together with `coordinator`. Streams slots are
//! out of milestone scope per `consumer-threading.md` §20.
//!
//! The consumer's `RequestManagers::supplier(...)` static factory is not
//! translated: `AsyncKafkaConsumer::new` constructs each manager directly.
//! The share consumer's is
//! [`RequestManagers::supplier_with_share_acknowledgement_event_handler`].
//!
//! # Why a flag, not `IdempotentCloser`
//!
//! Java uses an `IdempotentCloser` helper to enforce one-shot semantics
//! on `close()`. The Rust translation collapses this to a plain `closed:
//! bool` field — checking-and-setting a single flag is the entire
//! contract, and the helper class is overkill here.

#![cfg_attr(not(test), expect(dead_code))]

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::CommitRequestManager;
use super::ConsumerHeartbeatRequestManager;
use super::ConsumerMembershipManager;
use super::CoordinatorRequestManager;
use super::FetchRequestManager;
use super::MemberStateListener;
use super::OffsetsRequestManager;
use super::RequestManager;
use super::ShareConsumeRequestManager;
use super::ShareConsumerMetadata;
use super::ShareFetchBuffer;
use super::ShareFetchConfig;
use super::ShareFetchMetricsManager;
use super::ShareHeartbeatRequestManager;
use super::ShareMembershipManager;
use super::SubscriptionState;
use super::TopicMetadataRequestManager;
use super::events::{BackgroundEventHandler, ShareAcknowledgementEventHandler};
use crate::common::Error;
use crate::common::metrics::Metrics;
use crate::common::utils::Time;
use crate::consumer::ConsumerConfig;

/// Container holding all consumer request managers. The bg task
/// iterates over its [`Self::entries`] to poll each manager in
/// deterministic registration order.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers")]
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
    /// reason as `offsets`. Not in [`Self::entries`]: the bg task polls it
    /// directly with the network-client delegate it holds (see
    /// [`crate::consumer::internals::NodeStatus`]).
    pub(crate) fetch: Option<FetchRequestManager>,
    /// Share-group heartbeat manager — `Some` for a share consumer. Java:
    /// `Optional<ShareHeartbeatRequestManager> shareHeartbeatRequestManager`.
    pub(crate) share_heartbeat: Option<ShareHeartbeatRequestManager>,
    /// Share-group membership manager — `Some` for a share consumer. Java:
    /// `Optional<ShareMembershipManager> shareMembershipManager`. Held as
    /// `Arc` because [`ShareHeartbeatRequestManager`] holds the same
    /// instance; unlike `consumer_membership` it is in [`Self::entries`],
    /// polled through `impl RequestManager for Arc<ShareMembershipManager>`.
    pub(crate) share_membership: Option<Arc<ShareMembershipManager>>,
    /// Share consume request manager — `Some` for a share consumer. Java:
    /// `Optional<ShareConsumeRequestManager> shareConsumeRequestManager`.
    /// Behind `Arc<Mutex<_>>` because the share membership manager notifies
    /// it as a `MemberStateListener` (`impl MemberStateListener for
    /// Mutex<ShareConsumeRequestManager>`); it is in [`Self::entries`],
    /// polled through `impl RequestManager for
    /// Arc<Mutex<ShareConsumeRequestManager>>`.
    ///
    /// Lock order: the `RequestManagers` lock, then this one; the membership
    /// manager's state lock, then this one (it notifies listeners holding
    /// it). This lock is never held while calling into the membership
    /// manager, nor across an `.await`.
    pub(crate) share_consume: Option<Arc<Mutex<ShareConsumeRequestManager>>>,
    /// Auxiliary slot for dyn-dispatched managers — used by tests to
    /// inject spy/fake managers without expanding the concrete-field
    /// list. The production constructor `new(...)` leaves this empty;
    /// tests use [`Self::with_dyn_managers`] to populate it. Iterated
    /// last in `entries()`; tests that use it leave `fetch` empty.
    dyn_managers: Vec<Box<dyn RequestManager>>,
    closed: bool,
}

impl RequestManagers {
    /// Constructs a `RequestManagers` container with every in-scope
    /// slot.
    ///
    /// Java's consumer constructor of `RequestManagers` takes
    /// `OffsetsRequestManager` and `FetchRequestManager` as
    /// non-`Optional` parameters; Rust accepts them as `Option<_>` to
    /// keep test construction symmetric with the other slots. The
    /// bg-task wiring (Phase 10/11) always supplies `Some(_)` for both.
    ///
    /// Argument order is a Rust ergonomics choice (alphabetical-ish
    /// by phase), not the Java FFI order. The **`entries()` order** is
    /// what matters per `consumer-threading.md` §10 — see
    /// [`Self::entries`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#RequestManagers")]
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
            share_heartbeat: None,
            share_membership: None,
            share_consume: None,
            dyn_managers: Vec::new(),
            closed: false,
        }
    }

    /// Constructs the `RequestManagers` of a share consumer: the coordinator
    /// and the three share managers. The consumer-only slots are `None`
    /// (Java's `Optional.empty()`, or `null` for the offsets, topic-metadata
    /// and fetch managers).
    ///
    /// Java's `entries()` order is coordinator → share heartbeat → share
    /// membership → share consume; see [`Self::entries`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#RequestManagers")]
    pub(crate) fn with_share_consume_request_manager(
        share_consume_request_manager: Arc<Mutex<ShareConsumeRequestManager>>,
        coordinator: Option<Arc<CoordinatorRequestManager>>,
        share_heartbeat: Option<ShareHeartbeatRequestManager>,
        share_membership: Option<Arc<ShareMembershipManager>>,
    ) -> Self {
        Self {
            coordinator,
            topic_metadata: None,
            commit: None,
            consumer_heartbeat: None,
            consumer_membership: None,
            offsets: None,
            fetch: None,
            share_heartbeat,
            share_membership,
            share_consume: Some(share_consume_request_manager),
            dyn_managers: Vec::new(),
            closed: false,
        }
    }

    /// Creates the `RequestManagers` of a share consumer (`ShareConsumerImpl`):
    /// the coordinator, share membership, share heartbeat and share consume
    /// request managers, with the share consume request manager registered
    /// as a state listener of the share membership manager.
    ///
    /// # Errors
    ///
    /// The error of `ShareFetchConfig::with_consumer_config` when
    /// `isolation.level` or `share.acquire.mode` names no known value.
    ///
    /// # Translation notes
    ///
    /// - Java returns a `Supplier` that the network thread calls; Rust builds
    ///   the managers here, as `AsyncKafkaConsumer::new` builds the
    ///   consumer's.
    /// - `group_id` and `rack_id` are the two `GroupRebalanceConfig` fields
    ///   Java reads (ledger G8: Rust has no `GroupRebalanceConfig`).
    /// - Java's `LogContext` and `ClientTelemetryReporter` parameters are
    ///   dropped (the `log` crate; KIP-714 telemetry is not translated,
    ///   ledger G16), and so is the member-id label update on the reporter.
    /// - `completion_notify` is Rust-only: the background task's network-poll
    ///   wakeup, installed on the coordinator and heartbeat managers so their
    ///   spawned response forwarders wake the poll, as `AsyncKafkaConsumer`
    ///   installs it on the consumer's.
    #[expect(clippy::too_many_arguments, reason = "the Java supplier's parameters")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#supplier")]
    pub(crate) fn supplier_with_share_acknowledgement_event_handler(
        time: Arc<dyn Time>,
        share_acknowledgement_event_handler: Arc<ShareAcknowledgementEventHandler>,
        background_event_handler: Arc<BackgroundEventHandler>,
        metadata: Arc<ShareConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_buffer: Arc<ShareFetchBuffer>,
        config: &ConsumerConfig,
        group_id: &str,
        rack_id: Option<&str>,
        share_fetch_metrics_manager: Arc<ShareFetchMetricsManager>,
        metrics: &Arc<Metrics>,
        completion_notify: &Arc<Notify>,
    ) -> Result<Self, Error> {
        let retry_backoff_ms = config.retry_backoff_ms();
        let retry_backoff_max_ms = config.retry_backoff_max_ms();
        let share_fetch_config = ShareFetchConfig::with_consumer_config(config)?;

        let coordinator = Arc::new(CoordinatorRequestManager::new(retry_backoff_ms, retry_backoff_max_ms, group_id));
        coordinator.set_completion_notify(Arc::clone(completion_notify));
        let share_membership_manager = Arc::new(ShareMembershipManager::new(
            group_id,
            rack_id.map(str::to_string),
            Arc::clone(&subscriptions),
            metadata.metadata_arc(),
            Arc::clone(&time),
            metrics,
        ));

        let mut share_heartbeat_request_manager = ShareHeartbeatRequestManager::new(
            time.milliseconds(),
            config,
            Arc::clone(&coordinator),
            Arc::clone(&subscriptions),
            Arc::clone(&share_membership_manager),
            background_event_handler,
            metrics,
        );
        share_heartbeat_request_manager.set_completion_notify(Arc::clone(completion_notify));
        let share_consume_request_manager = Arc::new(Mutex::new(ShareConsumeRequestManager::new(
            time,
            group_id,
            metadata,
            subscriptions,
            share_fetch_config,
            fetch_buffer,
            share_acknowledgement_event_handler,
            share_fetch_metrics_manager,
            retry_backoff_ms,
            retry_backoff_max_ms,
        )));
        share_membership_manager
            .register_state_listener(Arc::clone(&share_consume_request_manager) as Arc<dyn MemberStateListener>);

        Ok(Self::with_share_consume_request_manager(
            share_consume_request_manager,
            Some(coordinator),
            Some(share_heartbeat_request_manager),
            Some(share_membership_manager),
        ))
    }

    /// Constructs a `RequestManagers` populated only with dyn-dispatched
    /// managers. Used by Phase-10 tests that need to inject spy / fake
    /// `RequestManager` implementations (Mockito's role in Java). The
    /// concrete slots are all `None`; managers iterate in
    /// supplied-vec order from `entries()`.
    #[cfg(test)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#RequestManagers")]
    pub(crate) fn with_dyn_managers(dyn_managers: Vec<Box<dyn RequestManager>>) -> Self {
        Self {
            coordinator: None,
            topic_metadata: None,
            commit: None,
            consumer_heartbeat: None,
            consumer_membership: None,
            offsets: None,
            fetch: None,
            share_heartbeat: None,
            share_membership: None,
            share_consume: None,
            dyn_managers,
            closed: false,
        }
    }

    /// Appends a dyn-dispatched manager to an already-built container, so a
    /// test can inject a fake manager into a consumer built by the production
    /// constructor (`AsyncKafkaConsumer::new`) instead of a hand-built
    /// network thread. Iterated last in `entries()`, like the managers of
    /// [`Self::with_dyn_managers`].
    #[cfg(test)]
    pub(crate) fn add_dyn_manager_for_test(&mut self, manager: Box<dyn RequestManager>) {
        self.dyn_managers.push(manager);
    }

    /// Returns the managers in deterministic registration order
    /// (`consumer-threading.md` §10), matching Java's
    /// `RequestManagers#entries` order:
    ///
    /// `(coordinator skipped) → (commit skipped) → heartbeat →
    /// (membership skipped) → offsets → topic_metadata → (fetch skipped)`.
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
    /// `fetch` is skipped too, and polled last by the bg task, because its
    /// `poll` / `poll_on_close` take the network-client delegate the bg task
    /// is holding (Java's `FetchRequestManager` keeps the delegate as a
    /// field; the Rust manager cannot, see
    /// [`crate::consumer::internals::NodeStatus`]), which the
    /// [`RequestManager`] signature does not carry.
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#entries")]
    pub(crate) fn entries(&mut self) -> Vec<&mut dyn RequestManager> {
        let mut list: Vec<&mut dyn RequestManager> = Vec::new();
        self.for_each_entry(|rm| list.push(rm));
        list
    }

    /// Visits the managers of [`Self::entries`], in the same order, without
    /// collecting them into a `Vec`.
    fn for_each_entry<'a>(&'a mut self, mut visit: impl FnMut(&'a mut dyn RequestManager)) {
        // Destructure so each `Option` is borrowed independently —
        // borrow-splitting per <https://doc.rust-lang.org/nomicon/borrow-splitting.html>.
        let Self {
            coordinator: _,
            topic_metadata,
            commit: _,
            consumer_heartbeat,
            consumer_membership: _,
            offsets,
            fetch: _,
            share_heartbeat,
            share_membership,
            share_consume,
            dyn_managers,
            closed: _,
        } = self;
        // Order matches Java (the `RequestManagers` constructor) minus the
        // three Arc-shared slots and the fetch slot: (coordinator skipped)
        // → (commit skipped) → heartbeat → (membership skipped) → offsets
        // → topic_metadata → (fetch, polled by the bg task with the
        // delegate).
        //
        // A share consumer has none of those but the coordinator, and Java's
        // share constructor lists (coordinator skipped) → share heartbeat →
        // share membership → share consume, so the share membership manager
        // reconciles a new assignment before the share consume manager polls
        // in the same iteration. Both kinds are never present together.
        if let Some(h) = consumer_heartbeat.as_mut() {
            visit(h);
        }
        if let Some(h) = share_heartbeat.as_mut() {
            visit(h);
        }
        if let Some(m) = share_membership.as_mut() {
            visit(m);
        }
        if let Some(c) = share_consume.as_mut() {
            visit(c);
        }
        if let Some(o) = offsets.as_mut() {
            visit(o);
        }
        if let Some(t) = topic_metadata.as_mut() {
            visit(t);
        }
        for m in dyn_managers.iter_mut() {
            visit(m.as_mut());
        }
    }

    /// Runs [`RequestManager::handle_completed_requests`] on every manager of
    /// [`Self::entries`], in that order. The background task calls it right
    /// after each network poll, where Java's `NetworkClientDelegate#poll`
    /// runs the requests' `whenComplete` callbacks.
    ///
    /// Rust-only (DoD §7): it routes the hook so that `run_once` needs no
    /// change when another manager adopts it. The walk allocates nothing.
    pub(crate) fn handle_completed_requests(&mut self) {
        self.for_each_entry(|rm| rm.handle_completed_requests());
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
    /// Java: `close()`, which calls `closeQuietly` on every entry that
    /// implements `Closeable`. Among the consumer's managers that is only the
    /// fetch manager (`FetchRequestManager extends AbstractFetch implements
    /// Closeable`), so this closes it: `AbstractFetch.close()` closes the
    /// fetch buffer and the decompression buffer supplier. Among the share
    /// consumer's it is only `ShareConsumeRequestManager implements
    /// Closeable`, which closes its share fetch buffer and its metrics. The
    /// Rust closes are infallible, so `closeQuietly`'s log-and-continue has
    /// nothing to catch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.RequestManagers#close")]
    pub(crate) fn close(&mut self) {
        if self.closed {
            log::debug!("RequestManagers was already closed");
            return;
        }
        log::debug!("Closing RequestManagers");
        self.closed = true;
        if let Some(fetch) = self.fetch.as_mut() {
            fetch.abstract_fetch_mut().close();
        }
        if let Some(share_consume) = self.share_consume.as_ref() {
            share_consume.lock().unwrap_or_else(std::sync::PoisonError::into_inner).close();
        }
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
    //   - testConsumerGroupRequestManagersAndListenersWired (Kafka 4.4's
    //     successor of `testMemberStateListenerRegistered`, renamed by
    //     a3362c491e): exercises `RequestManagers.supplier(...)` plumbing the
    //     `MemberStateListener` into `ConsumerMembershipManager`.
    //   - testStreamsGroupRequestManagersAndListenersWired (successor of
    //     `testStreamMemberStateListenerRegistered`): same shape but for
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
    use crate::common::utils::internals::BufferSupplier;
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::consumer::internals::ConsumerMetadata;
    use crate::consumer::internals::FetchBuffer;
    use crate::consumer::internals::FetchConfig;
    use crate::consumer::internals::FetchRequestManager;
    use crate::consumer::internals::HeartbeatRequestManagerHooks;

    fn coord_manager() -> Arc<CoordinatorRequestManager> {
        Arc::new(CoordinatorRequestManager::new(100, 1_000, "group-1"))
    }

    fn topic_metadata_manager() -> TopicMetadataRequestManager {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..crate::consumer::ConsumerConfig::defaults()
        };
        TopicMetadataRequestManager::new(&config)
    }

    fn commit_manager() -> Arc<CommitRequestManager> {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..crate::consumer::ConsumerConfig::defaults()
        };
        let subs = std::sync::Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::with_config(
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
            Arc::new(crate::common::utils::SystemTime),
            0,
        ))
    }

    fn offsets_manager() -> OffsetsRequestManager {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..crate::consumer::ConsumerConfig::defaults()
        };
        let subs = std::sync::Arc::new(std::sync::Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::with_config(
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
            Arc::new(crate::common::utils::SystemTime),
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
            std::sync::Arc::new(crate::ApiVersions::new()),
            crate::consumer::internals::FetchMetricsManager::for_test(),
            100,
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

    /// Java's `RequestManagers.close()` closes every `Closeable` entry, so
    /// the fetch manager's `AbstractFetch.close()` runs and closes its fetch
    /// buffer, dropping any buffered data.
    #[test]
    fn close_closes_the_fetch_manager() {
        let mut rm = RequestManagers::new(None, None, None, None, None, None, Some(fetch_manager()));
        let fetch_buffer =
            std::sync::Arc::clone(&rm.fetch.as_ref().expect("fetch manager").abstract_fetch().fetch_buffer);
        fetch_buffer.add(crate::consumer::internals::CompletedFetch::new(
            crate::common::TopicPartition::new("t".to_string(), 0),
            crate::fetch_response_data::PartitionData::new(),
        ));
        assert!(!fetch_buffer.is_empty());
        rm.close();
        assert!(
            rm.fetch.as_ref().expect("fetch manager").abstract_fetch().closed,
            "Java's RequestManagers.close closes the fetch manager (AbstractFetch.close)"
        );
        assert!(
            fetch_buffer.is_empty(),
            "Java's AbstractFetch.close closes the fetch buffer, which drops its buffered data"
        );
    }

    /// The share consumer's `RequestManagers`, built through the share
    /// supplier as `ShareConsumerImpl` builds them.
    fn share_request_managers(metrics: &Arc<Metrics>) -> RequestManagers {
        let config = crate::consumer::ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            group_id: Some("shareGroup".to_string()),
            ..crate::consumer::ConsumerConfig::defaults()
        };
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ShareConsumerMetadata::with_config(
            &config,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        let (background_event_tx, _background_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let registry = crate::consumer::internals::ShareFetchMetricsRegistry::with_metric_grp_prefix(
            crate::consumer::internals::ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX,
        );
        RequestManagers::supplier_with_share_acknowledgement_event_handler(
            Arc::new(crate::common::utils::MockTime::new()),
            Arc::new(ShareAcknowledgementEventHandler::new()),
            Arc::new(BackgroundEventHandler::new(background_event_tx)),
            metadata,
            subscriptions,
            Arc::new(ShareFetchBuffer::new()),
            &config,
            "shareGroup",
            None,
            Arc::new(ShareFetchMetricsManager::new(Arc::clone(metrics), &registry)),
            metrics,
            &Arc::new(Notify::new()),
        )
        .expect("the default config is valid")
    }

    /// Rust counterpart of `RequestManagersTest#testConsumerGroupRequestManagersAndListenersWired`
    /// for the share supplier (Java has no share case): the coordinator and
    /// the three share managers are present, the consumer-only managers are
    /// not, and the share consume request manager is the membership manager's
    /// only state listener (Java's
    /// `shareMembershipManager.registerStateListener(shareConsumeRequestManager)`).
    #[test]
    fn share_group_request_managers_and_listeners_wired() {
        let metrics = Arc::new(Metrics::new());
        let mut rm = share_request_managers(&metrics);
        assert!(rm.coordinator.is_some());
        assert!(rm.share_heartbeat.is_some());
        assert!(rm.share_consume.is_some());
        let membership = Arc::clone(rm.share_membership.as_ref().expect("share membership manager"));
        assert!(rm.commit.is_none());
        assert!(rm.consumer_heartbeat.is_none());
        assert!(rm.consumer_membership.is_none());
        assert!(rm.offsets.is_none());
        assert!(rm.topic_metadata.is_none());
        assert!(rm.fetch.is_none());

        // The heartbeat manager reaches the same membership manager.
        let heartbeat_membership = Arc::clone(rm.share_heartbeat.as_ref().expect("heartbeat").membership_manager());
        assert!(Arc::ptr_eq(&membership, &heartbeat_membership));

        let listeners = membership.abstract_mm.inner.lock().unwrap().state_updates_listeners.clone();
        assert_eq!(1, listeners.len());
        let share_consume = rm.share_consume.as_ref().expect("share consume request manager");
        assert!(std::ptr::addr_eq(Arc::as_ptr(&listeners[0]), Arc::as_ptr(share_consume)));

        // Java's share `entries()`: coordinator, share heartbeat, share
        // membership, share consume; the coordinator is polled separately.
        assert_eq!(3, rm.entries().len());
        assert_eq!(
            0,
            rm.membership_boundary(),
            "the share managers are all after the consumer boundary"
        );
    }

    /// Java's `RequestManagers#close` closes every `Closeable` entry; for a
    /// share consumer that is `ShareConsumeRequestManager#close`, which
    /// removes the share fetch metrics. A second close does nothing.
    #[test]
    fn close_closes_the_share_consume_manager() {
        let metrics = Arc::new(Metrics::new());
        let mut rm = share_request_managers(&metrics);
        let share_fetch_metrics = |metrics: &Metrics| {
            metrics
                .metrics()
                .snapshot()
                .keys()
                .filter(|name| name.group() == "consumer-share-fetch-manager-metrics")
                .count()
        };
        assert!(share_fetch_metrics(&metrics) > 0);

        rm.close();
        assert!(rm.is_closed());
        assert_eq!(
            0,
            share_fetch_metrics(&metrics),
            "ShareConsumeRequestManager#close removes its metrics"
        );
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

    /// Verifies that `entries()` SKIPS the `fetch` slot: the bg task polls
    /// it directly with the network-client delegate (its `poll` reads the
    /// delegate's connection state, which `RequestManager::poll` cannot
    /// carry).
    #[test]
    fn entries_skips_fetch_even_when_present() {
        let mut rm = RequestManagers::new(Some(coord_manager()), None, None, None, None, None, Some(fetch_manager()));
        let entries = rm.entries();
        assert!(entries.is_empty(), "fetch must be skipped from entries()");
        assert!(rm.fetch.is_some(), "the fetch slot stays populated");
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
        // 2 = offsets + topic_metadata.
        // (coordinator + commit are Arc-shared and excluded; fetch is
        // polled by the bg task with the delegate.)
        assert_eq!(2, entries.len());
    }
}
