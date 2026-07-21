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

//! `ShareConsumerImpl` — the KIP-848/KIP-932 async share-consumer implementation.
//!
//! Translates `org.apache.kafka.clients.consumer.internals.ShareConsumerImpl`
//! (Apache Kafka 4.2). Uses an event handler to submit
//! [`ApplicationEvent`]s so the network I/O runs on a dedicated background task
//! (`ConsumerNetworkThread`, §10), with a rotating-`CancellationToken`
//! [`WakeupTrigger`] for `wakeup()` (§11).
//!
//! ## Injectable seams (Java `mock(...)` translation)
//!
//! Java's `ShareConsumerImplTest` injects `mock(ApplicationEventHandler.class)`
//! and `mock(ShareFetchCollector.class)`. The faithful Rust translation of that
//! mockability is two `pub(crate)` traits — [`ShareApplicationEventHandler`]
//! and [`ShareFetchCollect`] — with the real `ApplicationEventHandler` /
//! [`ShareFetchCollector`] behind them in production and test doubles in unit
//! tests. The impl always does `handler.add(event)` and then awaits the event's
//! own `oneshot::Receiver` (exactly Java's `addAndGet` + `doAnswer`-completes-
//! future pattern).
//!
//! ## §31 acknowledgement-callback drain
//!
//! [`Self::handle_completed_acknowledgements`] runs at the TOP of every public
//! blocking-style API (`poll`, `commit_sync`, `commit_async`, `close`), drains
//! the share-acknowledgement event queue, and invokes the registered
//! [`AcknowledgementCommitCallback`] INLINE on the caller's task (via
//! [`AcknowledgementCommitCallbackHandler::on_complete`]) — never on the bg task
//! and never `tokio::spawn`ed. See consumer-threading.md §31.
//!
//! ## Metrics (KIP-714 deferral)
//!
//! All `KafkaShareConsumerMetrics` / `AsyncConsumerMetrics` recording is omitted
//! with `// metrics: deferred to KIP-714`; the surrounding logic is preserved.

// The production wiring that constructs `ShareConsumerImpl` (the
// `KafkaShareConsumer` facade + `new_share_consumer` factory) lands in the
// following commit; until then the type is reachable only from the `cfg(test)`
// unit tests, so silence dead-code in non-test builds (matching the staging
// pattern used by the other share modules, e.g. `share_in_flight_batch.rs`).
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use indexmap::IndexMap;
use tokio::sync::Notify;

use crate::common::{KafkaError, TopicIdPartition, Uuid};
use crate::consumer::acknowledge_type::AcknowledgeType;
use crate::consumer::acknowledgement_commit_callback::AcknowledgementCommitCallback;
use crate::consumer::internals::acknowledgement_commit_callback_handler::AcknowledgementCommitCallbackHandler;
use crate::consumer::internals::acknowledgements::Acknowledgements;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::events::application_event::ApplicationEvent;
use crate::consumer::internals::events::background_event::{BackgroundEvent, BackgroundEventEnvelope};
use crate::consumer::internals::events::share_acknowledge_async_event::ShareAcknowledgeAsyncEvent;
use crate::consumer::internals::events::share_acknowledge_on_close_event::ShareAcknowledgeOnCloseEvent;
use crate::consumer::internals::events::share_acknowledge_sync_event::ShareAcknowledgeSyncEvent;
use crate::consumer::internals::events::share_acknowledgement_commit_callback_registration_event::ShareAcknowledgementCommitCallbackRegistrationEvent;
use crate::consumer::internals::events::share_acknowledgement_event_handler::ShareAcknowledgementEventHandler;
use crate::consumer::internals::events::share_fetch_event::ShareFetchEvent;
use crate::consumer::internals::events::share_poll_event::SharePollEvent;
use crate::consumer::internals::events::share_subscription_change_event::ShareSubscriptionChangeEvent;
use crate::consumer::internals::events::share_unsubscribe_event::ShareUnsubscribeEvent;
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;
use crate::consumer::internals::share_acknowledgement_mode::ShareAcknowledgementMode;
use crate::consumer::internals::share_fetch::ShareFetch;
use crate::consumer::internals::share_fetch_buffer::ShareFetchBuffer;
use crate::consumer::internals::share_fetch_collector::ShareFetchCollector;
use crate::consumer::internals::share_fetch_exception::ShareFetchException;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::internals::wakeup_trigger::WakeupTrigger;
use crate::consumer::{ConsumerRecord, ConsumerRecords};

/// Time source abstraction (`org.apache.kafka.common.utils.Time`), matching the
/// pattern used by `FetchCollectorTime` / `ShareConsumeTime`.
pub(crate) trait ShareConsumerTime: Send + Sync + 'static {
    /// Java: `Time.milliseconds()`.
    fn milliseconds(&self) -> i64;
}

/// System-clock implementation of [`ShareConsumerTime`].
pub(crate) struct SystemShareConsumerTime;

impl ShareConsumerTime for SystemShareConsumerTime {
    fn milliseconds(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Application-event submission seam — the Rust translation of Java's mockable
/// `ApplicationEventHandler` for `ShareConsumerImpl`.
///
/// The impl always creates a completable event together with its
/// `oneshot::Receiver`, calls [`Self::add`] with the event, and (for blocking
/// operations) awaits the receiver itself. The real production handler forwards
/// `add` to the bg-task channel; the bg task completes the event's handle. A
/// unit-test double completes the handle synchronously inside `add` (Java's
/// `doAnswer` pattern).
#[async_trait]
pub(crate) trait ShareApplicationEventHandler: Send + Sync {
    /// Java: `applicationEventHandler.add(event)`.
    fn add(&self, event: ApplicationEvent, enqueued_ms: i64);

    /// Java: `applicationEventHandler.maximumTimeToWait()`.
    fn maximum_time_to_wait(&self) -> i64;

    /// Java: `applicationEventHandler.wakeupNetworkThread()`.
    fn wakeup_network_thread(&self);

    /// Java: `applicationEventHandler.close(Duration)`.
    async fn close(&self, timeout_ms: i64);
}

/// Production [`ShareApplicationEventHandler`] used by `new_share_consumer`.
///
/// Wraps the same channel + wakeup + bg-task-close machinery the KIP-848
/// `AsyncKafkaConsumer` uses. Java's `ShareConsumerImpl` holds a real
/// `ApplicationEventHandler` directly; Rust's `ShareConsumerImpl` takes the
/// mockable [`ShareApplicationEventHandler`] trait, so this struct is the
/// production impl behind it.
pub(crate) struct ProductionShareApplicationEventHandler {
    /// The shared channel-side adapter that pushes [`ApplicationEvent`]s onto
    /// the bg-task's application-event channel and pokes the selector-wakeup
    /// notify (Java's `wakeupNetworkThread()` on every `add`).
    application_event_handler:
        Arc<crate::consumer::internals::events::application_event_handler::ApplicationEventHandler>,
    /// Mirror of the bg task's `cachedMaximumTimeToWait`.
    max_time_to_wait_ms: Arc<AtomicI64>,
    /// Selector-wakeup notify shared with the bg task's `run_once` `select!`;
    /// fired by [`Self::wakeup_network_thread`].
    event_notify: Arc<Notify>,
    /// Erased handle to the bg task, used by [`Self::close`] to signal shutdown
    /// and join. `tokio::sync::Mutex` because `await_join` is `&mut` + async
    /// and the trait method is `&self`.
    network_thread_close: tokio::sync::Mutex<crate::consumer::async_kafka_consumer::NetworkThreadCloseHandle>,
}

impl ProductionShareApplicationEventHandler {
    pub(crate) fn new(
        application_event_handler: Arc<
            crate::consumer::internals::events::application_event_handler::ApplicationEventHandler,
        >,
        max_time_to_wait_ms: Arc<AtomicI64>,
        event_notify: Arc<Notify>,
        network_thread_close: crate::consumer::async_kafka_consumer::NetworkThreadCloseHandle,
    ) -> Self {
        Self {
            application_event_handler,
            max_time_to_wait_ms,
            event_notify,
            network_thread_close: tokio::sync::Mutex::new(network_thread_close),
        }
    }
}

#[async_trait]
impl ShareApplicationEventHandler for ProductionShareApplicationEventHandler {
    fn add(&self, event: ApplicationEvent, enqueued_ms: i64) {
        // Java's `add` throws if the queue is closed; here a send error means
        // the bg task has already shut down. Completable events left
        // unanswered resolve on the app side via a dropped-sender error
        // (mapped to a timeout in `ShareConsumerImpl`).
        if let Err(e) = self.application_event_handler.add(event, enqueued_ms) {
            log::debug!("Failed to enqueue share application event: {e}");
        }
    }

    fn maximum_time_to_wait(&self) -> i64 {
        self.max_time_to_wait_ms.load(Ordering::Acquire)
    }

    fn wakeup_network_thread(&self) {
        // Java: `networkClientDelegate.wakeup()` → `Selector.wakeup()`. Poking
        // the shared notify makes the bg task's in-progress network poll return
        // at a safe boundary (consumer-threading.md §10).
        self.event_notify.notify_one();
    }

    async fn close(&self, timeout_ms: i64) {
        let mut guard = self.network_thread_close.lock().await;
        guard.signal_close();
        guard.wakeup();
        // Bound the join so `close` cannot hang if the bg task is stuck; the
        // acknowledge/leave-group steps already awaited with their own timeout
        // before this call.
        let join = guard.await_join();
        let dur = Duration::from_millis(timeout_ms.max(0) as u64);
        match tokio::time::timeout(dur, join).await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => log::warn!("Share consumer network task terminated with error: {e}"),
            Err(_) => log::warn!("Timed out waiting for the share consumer network task to stop"),
        }
    }
}

/// Fetch-collection seam — the Rust translation of Java's mockable
/// `ShareFetchCollector`.
#[allow(clippy::result_large_err)]
pub(crate) trait ShareFetchCollect<K, V>: Send + Sync {
    /// Java: `ShareFetch<K, V> collect(ShareFetchBuffer)`.
    fn collect(&self, fetch_buffer: &ShareFetchBuffer) -> Result<ShareFetch<K, V>, ShareFetchException<K, V>>;
}

impl<K, V> ShareFetchCollect<K, V> for ShareFetchCollector<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn collect(&self, fetch_buffer: &ShareFetchBuffer) -> Result<ShareFetch<K, V>, ShareFetchException<K, V>> {
        ShareFetchCollector::collect(self, fetch_buffer)
    }
}

/// Minimal translation of Java's `org.apache.kafka.common.utils.Timer`, backed
/// by a [`ShareConsumerTime`] source.
struct Timer {
    time: Arc<dyn ShareConsumerTime>,
    deadline_ms: i64,
    current_ms: i64,
}

impl Timer {
    fn new(time: Arc<dyn ShareConsumerTime>, timeout_ms: i64) -> Self {
        let now = time.milliseconds();
        Self { time, deadline_ms: now.saturating_add(timeout_ms.max(0)), current_ms: now }
    }

    fn update(&mut self) {
        self.current_ms = self.time.milliseconds();
    }

    fn current_time_ms(&self) -> i64 {
        self.current_ms
    }

    fn remaining_ms(&self) -> i64 {
        (self.deadline_ms - self.current_ms).max(0)
    }

    fn not_expired(&self) -> bool {
        self.current_ms < self.deadline_ms
    }
}

/// The share-consumer implementation. See the module docs.
pub(crate) struct ShareConsumerImpl<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    application_event_handler: Arc<dyn ShareApplicationEventHandler>,
    fetch_collector: Box<dyn ShareFetchCollect<K, V>>,
    fetch_buffer: Arc<ShareFetchBuffer>,
    current_fetch: ShareFetch<K, V>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    metadata: Arc<ConsumerMetadata>,
    acknowledgement_event_handler: ShareAcknowledgementEventHandler,
    background_event_rx: tokio::sync::mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    acknowledgement_commit_callback_handler: Option<AcknowledgementCommitCallbackHandler>,
    completed_acknowledgements: Vec<HashMap<TopicIdPartition, Acknowledgements>>,
    acknowledgement_mode: ShareAcknowledgementMode,
    wakeup_trigger: Arc<WakeupTrigger>,
    time: Arc<dyn ShareConsumerTime>,
    client_id: String,
    group_id: String,
    request_timeout_ms: i32,
    default_api_timeout_ms: i32,
    closed: bool,
    should_send_share_fetch_event: bool,
}

/// Aggregates the pieces needed to build a [`ShareConsumerImpl`]. Mirrors Java's
/// visible-for-testing constructor
/// (`ShareConsumerImpl(LogContext, clientId, keyDeser, valueDeser, fetchBuffer,
/// fetchCollector, time, applicationEventHandler, ackQueue, bgQueue,
/// bgReaper, metrics, subscriptions, metadata, requestTimeoutMs,
/// defaultApiTimeoutMs, groupId, acknowledgementModeConfig)`), with the mockable
/// dependencies replaced by the [`ShareApplicationEventHandler`] /
/// [`ShareFetchCollect`] seams.
pub(crate) struct ShareConsumerComponents<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub application_event_handler: Arc<dyn ShareApplicationEventHandler>,
    pub fetch_collector: Box<dyn ShareFetchCollect<K, V>>,
    pub fetch_buffer: Arc<ShareFetchBuffer>,
    pub subscriptions: Arc<Mutex<SubscriptionState>>,
    pub metadata: Arc<ConsumerMetadata>,
    pub acknowledgement_event_handler: ShareAcknowledgementEventHandler,
    pub background_event_rx: tokio::sync::mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    pub wakeup_trigger: Arc<WakeupTrigger>,
    pub time: Arc<dyn ShareConsumerTime>,
    pub client_id: String,
    pub group_id: String,
    pub request_timeout_ms: i32,
    pub default_api_timeout_ms: i32,
    pub acknowledgement_mode: ShareAcknowledgementMode,
}

impl<K, V> ShareConsumerImpl<K, V>
where
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    /// Builds a `ShareConsumerImpl` from its assembled components. Mirrors the
    /// visible-for-testing constructor (see [`ShareConsumerComponents`]).
    pub(crate) fn from_components(components: ShareConsumerComponents<K, V>) -> Self {
        Self {
            application_event_handler: components.application_event_handler,
            fetch_collector: components.fetch_collector,
            fetch_buffer: components.fetch_buffer,
            current_fetch: ShareFetch::empty(),
            subscriptions: components.subscriptions,
            metadata: components.metadata,
            acknowledgement_event_handler: components.acknowledgement_event_handler,
            background_event_rx: components.background_event_rx,
            acknowledgement_commit_callback_handler: None,
            completed_acknowledgements: Vec::new(),
            acknowledgement_mode: components.acknowledgement_mode,
            wakeup_trigger: components.wakeup_trigger,
            time: components.time,
            client_id: components.client_id,
            group_id: components.group_id,
            request_timeout_ms: components.request_timeout_ms,
            default_api_timeout_ms: components.default_api_timeout_ms,
            closed: false,
            should_send_share_fetch_event: false,
        }
    }

    /// The consumer's `client.id`.
    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    // ── Light-lock / open check ──────────────────────────────────────────

    /// Java's `acquireAndEnsureOpen()` open check. Rust's `&mut self` receiver
    /// already prevents multithreaded access, so only the closed check remains.
    fn ensure_open(&self) -> Result<(), KafkaError> {
        if self.closed {
            return Err(KafkaError::illegal_state("This consumer has already been closed."));
        }
        Ok(())
    }

    fn now_ms(&self) -> i64 {
        self.time.milliseconds()
    }

    fn maybe_return_invalid_group_id(&self) -> Result<(), KafkaError> {
        if self.group_id.is_empty() {
            return Err(KafkaError::invalid_group_id(
                "You must provide a valid group.id in the consumer configuration.",
            ));
        }
        Ok(())
    }

    // ── Public API ───────────────────────────────────────────────────────

    /// Java: `Set<String> subscription()`.
    pub(crate) fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
        self.ensure_open()?;
        let guard = self.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard.subscription())
    }

    /// Java: `void subscribe(Collection<String> topics)`.
    pub(crate) async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.maybe_return_invalid_group_id()?;
        if topics.is_empty() {
            // Treat subscribing to empty topic list as the same as unsubscribing.
            return self.unsubscribe().await;
        }
        for topic in &topics {
            if topic.trim().is_empty() {
                return Err(KafkaError::illegal_argument(
                    "Topic collection to subscribe to cannot contain null or empty topic",
                ));
            }
        }
        let topics_set: HashSet<String> = topics.into_iter().collect();
        let (event, rx) = ShareSubscriptionChangeEvent::new(topics_set);
        let now = self.now_ms();
        self.application_event_handler
            .add(ApplicationEvent::ShareSubscriptionChange(event), now);
        rx.await
            .map_err(|_| KafkaError::timeout("subscribe event dropped without completion"))??;
        Ok(())
    }

    /// Java: `void unsubscribe()`.
    pub(crate) async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        self.ensure_open()?;
        let deadline = self.now_ms().saturating_add(self.default_api_timeout_ms as i64);
        let (event, rx) = ShareUnsubscribeEvent::new(deadline);
        let now = self.now_ms();
        self.application_event_handler
            .add(ApplicationEvent::ShareUnsubscribe(event), now);
        rx.await
            .map_err(|_| KafkaError::timeout("unsubscribe event dropped without completion"))??;
        Ok(())
    }

    /// Java: `ConsumerRecords<K, V> poll(Duration timeout)`.
    pub(crate) async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        let mut timer = Timer::new(Arc::clone(&self.time), timeout.as_millis() as i64);
        self.ensure_open()?;

        // Throw any errors notified by the background thread.
        self.process_background_events()?;
        // §31: handle any completed acknowledgements for which we have responses.
        self.handle_completed_acknowledgements().await;
        // If using implicit acknowledgement, acknowledge the previously fetched records.
        self.acknowledge_batch_if_implicit_acknowledgement();
        // If using explicit acknowledgement, ensure all in-flight records were acknowledged.
        self.ensure_in_flight_acknowledged_if_explicit_acknowledgement()?;

        {
            let guard = self.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            if guard.has_no_subscription_or_user_assignment() {
                return Err(KafkaError::illegal_state("Consumer is not subscribed to any topics."));
            }
        }

        self.should_send_share_fetch_event = true;

        loop {
            // Make sure the network thread can tell the application is polling.
            let now = timer.current_time_ms();
            self.application_event_handler
                .add(ApplicationEvent::SharePoll(SharePollEvent::new(now)), now);

            // We must not allow wake-ups between polling for fetches and
            // returning the records, so trigger a possible wake-up first.
            if let Err(e) = self.wakeup_trigger.maybe_trigger_wakeup() {
                self.wakeup_trigger.rotate();
                return Err(e);
            }

            // `poll_for_fetches` mutates `self.current_fetch` in place (Java's
            // `collect` returns a reference to `currentFetch`). On error it
            // carries the accumulated `ShareFetch` (Java's
            // `ShareFetchException.shareFetch()`) which becomes `currentFetch`.
            if let Err(share_fetch_exception) = self.poll_for_fetches(&mut timer).await {
                let (share_fetch, cause) = share_fetch_exception.into_parts();
                self.current_fetch = share_fetch;
                return Err(cause);
            }
            if !self.current_fetch.is_empty() {
                self.handle_completed_acknowledgements().await;
                let records = self.current_fetch.take_records();
                return Ok(ConsumerRecords::new(records, HashMap::new()));
            }

            // Throw any errors notified by the background thread.
            self.process_background_events()?;
            self.metadata.maybe_return_any_error()?;

            timer.update();
            if !timer.not_expired() {
                break;
            }
        }

        self.handle_completed_acknowledgements().await;
        Ok(ConsumerRecords::empty())
    }

    /// Translates Java's `pollForFetches`. Mutates `self.current_fetch` in place
    /// (the "collected fetch" IS `currentFetch`); the caller reads
    /// `self.current_fetch` afterwards.
    async fn poll_for_fetches(&mut self, timer: &mut Timer) -> Result<(), ShareFetchException<K, V>> {
        let poll_timeout = self.application_event_handler.maximum_time_to_wait().min(timer.remaining_ms());

        let acknowledgements_map = self.current_fetch.take_acknowledged_records();

        // If data is available already, return it immediately.
        self.collect(acknowledgements_map)?;
        if !self.current_fetch.is_empty() {
            return Ok(());
        }

        // Wait a bit — this is where we will fetch records. A wakeup interrupts
        // the wait (§11); the poll loop then re-checks `maybe_trigger_wakeup`.
        let token = self.wakeup_trigger.current_token();
        tokio::select! {
            _ = self.fetch_buffer.await_not_empty(Duration::from_millis(poll_timeout.max(0) as u64)) => {},
            _ = token.cancelled() => {},
        }
        timer.update();

        self.collect(IndexMap::new())
    }

    /// Translates Java's `collect`. Operates on `self.current_fetch` in place:
    /// when `currentFetch` is empty and has no renewals it replaces it with a
    /// fresh fetch from the collector; the renewal branch moves renewed records
    /// back into `currentFetch`. (Java returns a reference to `currentFetch`;
    /// Rust cannot return owned records without draining `currentFetch`, so the
    /// caller reads `self.current_fetch` after this returns.)
    #[allow(clippy::result_large_err)]
    fn collect(
        &mut self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    ) -> Result<(), ShareFetchException<K, V>> {
        let mut acks_to_send = acknowledgements_map;

        if self.current_fetch.is_empty() && !self.current_fetch.has_renewals() {
            let mut fetch = self.fetch_collector.collect(&self.fetch_buffer)?;
            let fetch_is_empty = fetch.is_empty();
            if fetch_is_empty {
                // Check for acknowledgements from control records (GAP) and send them.
                let control_record_acknowledgements = fetch.take_acknowledged_records();
                if !control_record_acknowledgements.is_empty() {
                    self.send_share_acknowledge_async_event(control_record_acknowledgements);
                }

                // We only send one ShareFetchEvent per poll call.
                if self.should_send_share_fetch_event {
                    let now = self.now_ms();
                    self.application_event_handler.add(
                        ApplicationEvent::ShareFetch(ShareFetchEvent::new(std::mem::take(&mut acks_to_send))),
                        now,
                    );
                    self.should_send_share_fetch_event = false;
                    self.application_event_handler.wakeup_network_thread();
                }
            }

            if !acks_to_send.is_empty() {
                self.send_share_acknowledge_async_event(acks_to_send);
            }
            // Java assigns `currentFetch = fetch` ONLY when the fetch is
            // non-empty (`ShareConsumerImpl.java:628-629`); an empty collect
            // leaves `currentFetch` untouched so it retains its
            // `acquisitionLockTimeoutMs` (and any other state) from the prior
            // non-empty fetch. Clobbering it with a fresh empty fetch would make
            // `acquisition_lock_timeout_ms()` regress to `None`.
            if !fetch_is_empty {
                self.current_fetch = fetch;
            }
            return Ok(());
        } else if self.current_fetch.has_renewals() {
            // Move any renewed records back into in-flight records.
            self.current_fetch.take_renewed_records();

            if self.current_fetch.has_renewals() && self.should_send_share_fetch_event {
                let now = self.now_ms();
                self.application_event_handler.add(
                    ApplicationEvent::ShareFetch(ShareFetchEvent::new(std::mem::take(&mut acks_to_send))),
                    now,
                );
                self.should_send_share_fetch_event = false;
                self.application_event_handler.wakeup_network_thread();
            }
        }

        if !acks_to_send.is_empty() {
            self.send_share_acknowledge_async_event(acks_to_send);
        }
        // Java returns `currentFetch` unchanged in the renewal / else branch;
        // Rust leaves `self.current_fetch` in place.
        Ok(())
    }

    fn send_share_acknowledge_async_event(
        &self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    ) {
        let now = self.now_ms();
        let deadline = now.saturating_add(self.default_api_timeout_ms as i64);
        self.application_event_handler.add(
            ApplicationEvent::ShareAcknowledgeAsync(ShareAcknowledgeAsyncEvent::new(acknowledgements_map, deadline)),
            now,
        );
        self.application_event_handler.wakeup_network_thread();
    }

    /// Java: `void acknowledge(ConsumerRecord<K, V> record)`.
    pub(crate) fn acknowledge(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        self.acknowledge_with_type(record, AcknowledgeType::Accept)
    }

    /// Java: `void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type)`.
    pub(crate) fn acknowledge_with_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.ensure_explicit_acknowledgement()?;
        self.current_fetch.acknowledge(record, ack_type)
    }

    /// Java: `void acknowledge(String topic, int partition, long offset, AcknowledgeType type)`.
    pub(crate) fn acknowledge_by_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.ensure_explicit_acknowledgement()?;
        self.current_fetch.acknowledge_on_exception(topic, partition, offset, ack_type)
    }

    /// Java: `Map<TopicIdPartition, Optional<KafkaException>> commitSync()`.
    pub(crate) async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        let timeout = Duration::from_millis(self.default_api_timeout_ms as u64);
        self.commit_sync_timeout(timeout).await
    }

    /// Java: `Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout)`.
    pub(crate) async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        self.ensure_open()?;
        self.handle_completed_acknowledgements().await;
        self.acknowledge_batch_if_implicit_acknowledgement();

        let acknowledgements_map = self.acknowledgements_to_send();
        if acknowledgements_map.is_empty() {
            return Ok(HashMap::new());
        }

        let now = self.now_ms();
        let deadline = now.saturating_add(timeout.as_millis() as i64);
        let (event, rx) = ShareAcknowledgeSyncEvent::new(acknowledgements_map, deadline);
        self.application_event_handler
            .add(ApplicationEvent::ShareAcknowledgeSync(event), now);

        let token = self.wakeup_trigger.current_token();
        let completed = tokio::select! {
            r = rx => r.map_err(|_| KafkaError::timeout("commit-sync event dropped without completion"))?,
            _ = token.cancelled() => {
                self.wakeup_trigger.rotate();
                return Err(KafkaError::wakeup("WakeupTrigger fired"));
            },
        };
        let completed = completed?;

        let mut result: HashMap<TopicIdPartition, Option<KafkaError>> = HashMap::new();
        for (tip, acks) in completed {
            result.insert(tip, acks.get_acknowledge_exception().cloned());
        }

        // Handle any acknowledgements which completed while we were waiting.
        self.handle_completed_acknowledgements().await;
        Ok(result)
    }

    /// Java: `void commitAsync()`.
    pub(crate) async fn commit_async(&mut self) -> Result<(), KafkaError> {
        self.ensure_open()?;
        self.handle_completed_acknowledgements().await;
        self.acknowledge_batch_if_implicit_acknowledgement();

        let acknowledgements_map = self.acknowledgements_to_send();
        if !acknowledgements_map.is_empty() {
            let now = self.now_ms();
            let deadline = now.saturating_add(self.default_api_timeout_ms as i64);
            self.application_event_handler.add(
                ApplicationEvent::ShareAcknowledgeAsync(ShareAcknowledgeAsyncEvent::new(
                    acknowledgements_map,
                    deadline,
                )),
                now,
            );
        }
        Ok(())
    }

    /// Java: `void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback)`.
    pub(crate) fn set_acknowledgement_commit_callback(
        &mut self,
        callback: Option<Arc<dyn AcknowledgementCommitCallback>>,
    ) {
        match callback {
            Some(cb) => {
                if self.acknowledgement_commit_callback_handler.is_none() {
                    let now = self.now_ms();
                    self.application_event_handler.add(
                        ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration(
                            ShareAcknowledgementCommitCallbackRegistrationEvent::new(true),
                        ),
                        now,
                    );
                }
                self.acknowledgement_commit_callback_handler = Some(AcknowledgementCommitCallbackHandler::new(cb));
            },
            None => {
                if self.acknowledgement_commit_callback_handler.is_some() {
                    let now = self.now_ms();
                    self.application_event_handler.add(
                        ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration(
                            ShareAcknowledgementCommitCallbackRegistrationEvent::new(false),
                        ),
                        now,
                    );
                }
                self.completed_acknowledgements.clear();
                self.acknowledgement_commit_callback_handler = None;
            },
        }
    }

    /// Java: `Uuid clientInstanceId(Duration timeout)`.
    ///
    /// Telemetry (KIP-714) is not wired in this client, so — like a Java
    /// consumer with `enable.metrics.push=false` — this reports that telemetry
    /// is disabled.
    pub(crate) async fn client_instance_id(&mut self, _timeout: Duration) -> Result<Uuid, KafkaError> {
        Err(KafkaError::illegal_state(
            "Telemetry is not enabled. Set config `enable.metrics.push` to `true`.",
        ))
    }

    /// Java: `Optional<Integer> acquisitionLockTimeoutMs()`.
    pub(crate) fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
        self.ensure_open()?;
        Ok(self.current_fetch.acquisition_lock_timeout_ms())
    }

    /// Java: `void wakeup()`.
    pub(crate) fn wakeup(&self) {
        self.wakeup_trigger.wakeup();
    }

    /// Java: `void close()`.
    pub(crate) async fn close(&mut self) -> Result<(), KafkaError> {
        // ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS = 30_000.
        self.close_timeout(Duration::from_millis(30_000)).await
    }

    /// Java: `void close(Duration timeout)`.
    pub(crate) async fn close_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError> {
        if (timeout.as_millis() as i64) < 0 {
            return Err(KafkaError::illegal_argument("The timeout cannot be negative."));
        }
        let mut result = Ok(());
        if !self.closed {
            result = self.close_internal(timeout, false).await;
        }
        self.closed = true;
        result
    }

    async fn close_internal(&mut self, timeout: Duration, swallow_exception: bool) -> Result<(), KafkaError> {
        let mut first_exception: Option<KafkaError> = None;

        // We are already closing with a timeout — don't allow wake-ups.
        self.wakeup_trigger.disable();

        let close_timeout_ms = (timeout.as_millis() as i64).min(self.request_timeout_ms as i64);

        // 1. commit pending acknowledgements + leave the group.
        if let Err(e) = self.send_acknowledgements_and_leave_group(close_timeout_ms).await {
            Self::record_first_error(&mut first_exception, e);
        }
        // 2. stop finding the coordinator.
        self.stop_find_coordinator_on_close();
        // 3. invoke the acknowledgement commit callback for anything completed.
        self.handle_completed_acknowledgements().await;
        // 4. process background events (swallowing the expected close errors).
        if let Err(e) = self.process_background_events_on_close() {
            Self::record_first_error(&mut first_exception, e);
        }

        self.application_event_handler.close(close_timeout_ms).await;

        match first_exception {
            // Java wraps in `new KafkaException("Failed to close Kafka share
            // consumer", exception)`; KafkaError has no source-chaining
            // constructor, so we propagate the first error unchanged (its
            // message/code is preserved). The close tests never reach this arm.
            Some(exception) if !swallow_exception => Err(exception),
            _ => Ok(()),
        }
    }

    fn stop_find_coordinator_on_close(&self) {
        let now = self.now_ms();
        self.application_event_handler
            .add(ApplicationEvent::StopFindCoordinatorOnClose, now);
    }

    async fn send_acknowledgements_and_leave_group(&mut self, close_timeout_ms: i64) -> Result<(), KafkaError> {
        // Send pending acknowledgements + close the share sessions.
        let acks = self.acknowledgements_to_send();
        let deadline = self.now_ms().saturating_add(close_timeout_ms);
        let (ack_event, ack_rx) = ShareAcknowledgeOnCloseEvent::new(acks, deadline);
        let now = self.now_ms();
        self.application_event_handler
            .add(ApplicationEvent::ShareAcknowledgeOnClose(ack_event), now);
        // completeQuietly: a timeout is logged, other errors recorded.
        let mut first_error: Option<KafkaError> = None;
        match ack_rx.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => {
                if !matches!(e, KafkaError::Timeout(_)) {
                    first_error = Some(e);
                }
            },
            Err(_) => {},
        }

        // Leave the group.
        let leave_deadline = self.now_ms().saturating_add(close_timeout_ms);
        let (unsub_event, unsub_rx) = ShareUnsubscribeEvent::new(leave_deadline);
        let now = self.now_ms();
        self.application_event_handler
            .add(ApplicationEvent::ShareUnsubscribe(unsub_event), now);

        // Ignore fatal auth/topic errors while leaving the group so unsubscribe
        // can complete (Java's `processBackgroundEvents(future, timer, pred)`).
        let mut timer = Timer::new(Arc::clone(&self.time), close_timeout_ms);
        if let Err(e) = self
            .process_background_events_until(unsub_rx, &mut timer, is_ignorable_close_error)
            .await
            && !is_ignorable_close_error(&e)
        {
            Self::record_first_error(&mut first_error, e);
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // ── §31 callback drain + acknowledgement / background event processing ──

    /// §31 + Java's `handleCompletedAcknowledgements()`. Drains the
    /// share-acknowledgement events and — INLINE on the caller's task — invokes
    /// the registered [`AcknowledgementCommitCallback`].
    async fn handle_completed_acknowledgements(&mut self) {
        self.process_acknowledgement_events();

        if !self.completed_acknowledgements.is_empty() {
            if let Some(handler) = self.acknowledgement_commit_callback_handler.as_mut() {
                let list = std::mem::take(&mut self.completed_acknowledgements);
                // INLINE on the caller's task — never `tokio::spawn`, never on
                // the bg task (consumer-threading.md §31).
                handler.on_complete(list).await;
            } else {
                self.completed_acknowledgements.clear();
            }
        }
    }

    /// Java's `ShareAcknowledgementEventProcessor.process` + `processAcknowledgementEvents`.
    fn process_acknowledgement_events(&mut self) {
        let events = self.acknowledgement_event_handler.drain_events();
        for event in events {
            // If a callback is registered, accumulate the completed acks for it.
            if self.acknowledgement_commit_callback_handler.is_some() {
                let map: HashMap<TopicIdPartition, Acknowledgements> = event
                    .acknowledgements_map()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                self.completed_acknowledgements.push(map);
            }
            // If the event carries renew acknowledgements, apply them.
            if event.check_for_renew_acknowledgements()
                && let Err(e) = self
                    .current_fetch
                    .renew(event.acknowledgements_map(), event.acquisition_lock_timeout_ms())
            {
                log::warn!("An error occurred when processing the acknowledgement event: {e}");
            }
        }
    }

    fn acknowledge_batch_if_implicit_acknowledgement(&mut self) {
        if self.acknowledgement_mode == ShareAcknowledgementMode::IMPLICIT {
            self.current_fetch.acknowledge_all(AcknowledgeType::Accept);
        }
    }

    fn ensure_in_flight_acknowledged_if_explicit_acknowledgement(&self) -> Result<(), KafkaError> {
        if self.acknowledgement_mode == ShareAcknowledgementMode::EXPLICIT
            && !self.current_fetch.check_all_in_flight_are_acknowledged()
        {
            return Err(KafkaError::illegal_state(
                "All records must be acknowledged in explicit acknowledgement mode.",
            ));
        }
        Ok(())
    }

    fn acknowledgements_to_send(&mut self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.current_fetch.take_acknowledged_records()
    }

    fn ensure_explicit_acknowledgement(&self) -> Result<(), KafkaError> {
        if self.acknowledgement_mode == ShareAcknowledgementMode::IMPLICIT {
            return Err(KafkaError::illegal_state("Implicit acknowledgement of delivery is being used."));
        }
        Ok(())
    }

    /// Java's `processBackgroundEvents()` (no future). Drains the background
    /// event queue, records the first error, and returns whether any events
    /// were seen.
    fn process_background_events(&mut self) -> Result<bool, KafkaError> {
        let mut first_error: Option<KafkaError> = None;
        let mut had_events = false;
        while let Ok(envelope) = self.background_event_rx.try_recv() {
            had_events = true;
            match envelope.event {
                BackgroundEvent::Error { error } => {
                    Self::record_first_error(&mut first_error, error);
                },
                BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { .. } => {
                    // Share groups do not use the rebalance-listener handshake.
                },
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(had_events)
    }

    /// Java's `processBackgroundEventsOnClose()` — swallow the expected
    /// auth/topic errors during close.
    fn process_background_events_on_close(&mut self) -> Result<(), KafkaError> {
        match self.process_background_events() {
            Ok(_) => Ok(()),
            Err(e) if is_ignorable_close_error(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Java's `<T> T processBackgroundEvents(Future<T>, Timer, Predicate)`.
    /// Loops: process background events (ignoring predicate-matching errors),
    /// then briefly wait for the future to complete.
    async fn process_background_events_until(
        &mut self,
        mut rx: tokio::sync::oneshot::Receiver<Result<(), KafkaError>>,
        timer: &mut Timer,
        ignore_error: fn(&KafkaError) -> bool,
    ) -> Result<(), KafkaError> {
        loop {
            match self.process_background_events() {
                Ok(_) => {},
                Err(e) if ignore_error(&e) => {},
                Err(e) => return Err(e),
            }

            match rx.try_recv() {
                Ok(result) => return result,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    // The sender was dropped without completing — treat as done.
                    return Ok(());
                },
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    timer.update();
                    if !timer.not_expired() {
                        return Err(KafkaError::timeout("Operation timed out before completion"));
                    }
                    tokio::task::yield_now().await;
                },
            }
        }
    }

    fn record_first_error(slot: &mut Option<KafkaError>, err: KafkaError) {
        if slot.is_none() {
            *slot = Some(err);
        }
    }
}

#[async_trait]
impl<K, V> crate::consumer::share_consumer::ShareConsumer<K, V> for ShareConsumerImpl<K, V>
where
    K: Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    fn subscription(&self) -> Result<HashSet<String>, KafkaError> {
        ShareConsumerImpl::subscription(self)
    }

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        ShareConsumerImpl::subscribe(self, topics).await
    }

    async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        ShareConsumerImpl::unsubscribe(self).await
    }

    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError> {
        ShareConsumerImpl::poll(self, timeout).await
    }

    fn acknowledge(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), KafkaError> {
        ShareConsumerImpl::acknowledge(self, record)
    }

    fn acknowledge_with_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        ShareConsumerImpl::acknowledge_with_type(self, record, ack_type)
    }

    fn acknowledge_by_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_type: AcknowledgeType,
    ) -> Result<(), KafkaError> {
        ShareConsumerImpl::acknowledge_by_offset(self, topic, partition, offset, ack_type)
    }

    async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        ShareConsumerImpl::commit_sync(self).await
    }

    async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<KafkaError>>, KafkaError> {
        ShareConsumerImpl::commit_sync_timeout(self, timeout).await
    }

    async fn commit_async(&mut self) -> Result<(), KafkaError> {
        ShareConsumerImpl::commit_async(self).await
    }

    fn set_acknowledgement_commit_callback(&mut self, callback: Option<Arc<dyn AcknowledgementCommitCallback>>) {
        ShareConsumerImpl::set_acknowledgement_commit_callback(self, callback);
    }

    async fn client_instance_id(&mut self, timeout: Duration) -> Result<Uuid, KafkaError> {
        ShareConsumerImpl::client_instance_id(self, timeout).await
    }

    fn acquisition_lock_timeout_ms(&self) -> Result<Option<i32>, KafkaError> {
        ShareConsumerImpl::acquisition_lock_timeout_ms(self)
    }

    async fn close(&mut self) -> Result<(), KafkaError> {
        ShareConsumerImpl::close(self).await
    }

    async fn close_timeout(&mut self, timeout: Duration) -> Result<(), KafkaError> {
        ShareConsumerImpl::close_timeout(self, timeout).await
    }

    fn wakeup(&self) {
        ShareConsumerImpl::wakeup(self);
    }
}

/// Java's close-path `ignoreErrorEventException` predicate — the errors that are
/// swallowed while leaving the group / on close so the consumer can still close.
fn is_ignorable_close_error(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::GroupAuthorization(_) | KafkaError::TopicAuthorization(_) | KafkaError::InvalidTopic(_)
    )
}

#[cfg(test)]
mod tests {
    //! Translated from
    //! `org.apache.kafka.clients.consumer.internals.ShareConsumerImplTest`.
    //!
    //! Java injects `mock(ApplicationEventHandler.class)` + `mock(ShareFetchCollector.class)`
    //! and drives them with `doAnswer` / `doReturn`. The Rust translation uses
    //! the [`ShareApplicationEventHandler`] / [`ShareFetchCollect`] seams with a
    //! [`TestEventHandler`] (records added events + completes handles inline,
    //! mirroring `doAnswer`) and a [`TestFetchCollector`] (returns queued
    //! fetches, mirroring `doReturn` / `doAnswer`). Mockito `verify(...)` maps to
    //! assertions over `TestEventHandler`'s recorded event list.

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use tokio::sync::mpsc;

    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::{TopicPartition, Uuid};
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::events::share_acknowledgement_event::ShareAcknowledgementEvent;
    use crate::consumer::internals::events::share_acknowledgement_event_handler::ShareAcknowledgementEventQueue;
    use crate::consumer::internals::share_in_flight_batch::ShareInFlightBatch;

    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30_000);

    /// Auto-advancing mock clock (+1ms per read), mirroring `MockTime(1)`. The
    /// advance guarantees `poll`'s `Timer` eventually expires even when the
    /// fetch stays empty.
    struct MockClock {
        ms: AtomicI64,
    }
    impl MockClock {
        fn new() -> Self {
            Self { ms: AtomicI64::new(1_000) }
        }
    }
    impl ShareConsumerTime for MockClock {
        fn milliseconds(&self) -> i64 {
            self.ms.fetch_add(1, Ordering::SeqCst)
        }
    }

    /// Records added events and completes completable-event handles inline
    /// (Java's `doAnswer(... event.future().complete(...))`).
    struct TestEventHandler {
        added: Mutex<Vec<String>>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        max_wait: AtomicI64,
    }
    impl TestEventHandler {
        fn new(subscriptions: Arc<Mutex<SubscriptionState>>) -> Self {
            Self { added: Mutex::new(Vec::new()), subscriptions, max_wait: AtomicI64::new(0) }
        }
        fn added_names(&self) -> Vec<String> {
            self.added.lock().unwrap().clone()
        }
        fn count_of(&self, type_name: &str) -> usize {
            self.added.lock().unwrap().iter().filter(|n| n.as_str() == type_name).count()
        }
    }
    #[async_trait]
    impl ShareApplicationEventHandler for TestEventHandler {
        fn add(&self, event: ApplicationEvent, _enqueued_ms: i64) {
            self.added.lock().unwrap().push(event.type_name().to_string());
            match event {
                ApplicationEvent::ShareSubscriptionChange(e) => {
                    let mut subs = self.subscriptions.lock().unwrap();
                    let _ = subs.subscribe_to_share_group(e.topics().clone());
                    drop(subs);
                    e.handle().complete(());
                },
                ApplicationEvent::ShareUnsubscribe(e) => {
                    self.subscriptions.lock().unwrap().unsubscribe();
                    e.handle().complete(());
                },
                ApplicationEvent::ShareAcknowledgeOnClose(e) => {
                    e.handle().complete(());
                },
                ApplicationEvent::ShareAcknowledgeSync(e) => {
                    e.handle().complete(IndexMap::new());
                },
                // SharePoll / ShareFetch / ShareAcknowledgeAsync / registration /
                // StopFindCoordinatorOnClose: fire-and-forget.
                _ => {},
            }
        }
        fn maximum_time_to_wait(&self) -> i64 {
            self.max_wait.load(Ordering::SeqCst)
        }
        fn wakeup_network_thread(&self) {}
        async fn close(&self, _timeout_ms: i64) {}
    }

    struct CollectorState {
        results: VecDeque<ShareFetch<String, String>>,
        wakeup_on_call: std::collections::HashSet<usize>,
        call_count: usize,
    }

    struct TestFetchCollector {
        state: Arc<Mutex<CollectorState>>,
        wakeup_trigger: Arc<WakeupTrigger>,
    }
    impl ShareFetchCollect<String, String> for TestFetchCollector {
        fn collect(
            &self,
            _fetch_buffer: &ShareFetchBuffer,
        ) -> Result<ShareFetch<String, String>, ShareFetchException<String, String>> {
            let mut st = self.state.lock().unwrap();
            st.call_count += 1;
            let n = st.call_count;
            let fire = st.wakeup_on_call.contains(&n);
            if fire {
                self.wakeup_trigger.wakeup();
            }
            Ok(st.results.pop_front().unwrap_or_else(ShareFetch::empty))
        }
    }

    /// The test fixture: the consumer plus handles to drive it.
    struct Fixture {
        consumer: ShareConsumerImpl<String, String>,
        handler: Arc<TestEventHandler>,
        collector_state: Arc<Mutex<CollectorState>>,
        bg_tx: mpsc::UnboundedSender<BackgroundEventEnvelope>,
        ack_handler: ShareAcknowledgementEventHandler,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        wakeup_trigger: Arc<WakeupTrigger>,
    }

    fn build_fixture(group_id: &str, mode: ShareAcknowledgementMode) -> Fixture {
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));
        let handler = Arc::new(TestEventHandler::new(Arc::clone(&subscriptions)));
        let wakeup_trigger = Arc::new(WakeupTrigger::new());
        let collector_state = Arc::new(Mutex::new(CollectorState {
            results: VecDeque::new(),
            wakeup_on_call: std::collections::HashSet::new(),
            call_count: 0,
        }));
        let collector = Box::new(TestFetchCollector {
            state: Arc::clone(&collector_state),
            wakeup_trigger: Arc::clone(&wakeup_trigger),
        });
        let (bg_tx, bg_rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
        let ack_queue: ShareAcknowledgementEventQueue = Arc::new(Mutex::new(VecDeque::new()));
        let ack_handler = ShareAcknowledgementEventHandler::new(ack_queue);
        let metadata = Arc::new(ConsumerMetadata::new(
            0,
            1_000,
            300_000,
            false,
            false,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        let time: Arc<dyn ShareConsumerTime> = Arc::new(MockClock::new());

        let components = ShareConsumerComponents {
            application_event_handler: Arc::clone(&handler) as Arc<dyn ShareApplicationEventHandler>,
            fetch_collector: collector,
            fetch_buffer: Arc::new(ShareFetchBuffer::new()),
            subscriptions: Arc::clone(&subscriptions),
            metadata,
            acknowledgement_event_handler: ack_handler.clone(),
            background_event_rx: bg_rx,
            wakeup_trigger: Arc::clone(&wakeup_trigger),
            time,
            client_id: "client-id".to_string(),
            group_id: group_id.to_string(),
            request_timeout_ms: 30_000,
            default_api_timeout_ms: 1_000,
            acknowledgement_mode: mode,
        };
        Fixture {
            consumer: ShareConsumerImpl::from_components(components),
            handler,
            collector_state,
            bg_tx,
            ack_handler,
            subscriptions,
            wakeup_trigger,
        }
    }

    fn tip(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(Uuid::random_uuid(), TopicPartition::new(topic.to_string(), partition))
    }

    fn record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(
            topic,
            partition,
            offset,
            Some(format!("key{offset}")),
            Some(format!("value{offset}")),
        )
    }

    fn fetch_with_records(topic: &str, partition: i32, offsets: &[i64]) -> ShareFetch<String, String> {
        let tp = tip(topic, partition);
        let mut batch = ShareInFlightBatch::new(0, tp.clone(), DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS);
        for &o in offsets {
            batch.add_record(record(topic, partition, o));
        }
        let mut fetch = ShareFetch::empty();
        fetch.add(tp, batch);
        fetch
    }

    fn push_fetch(fx: &Fixture, fetch: ShareFetch<String, String>) {
        fx.collector_state.lock().unwrap().results.push_back(fetch);
    }

    fn wakeup_on_collect_call(fx: &Fixture, call: usize) {
        fx.collector_state.lock().unwrap().wakeup_on_call.insert(call);
    }

    async fn subscribe_ok(fx: &mut Fixture, topics: &[&str]) {
        fx.consumer
            .subscribe(topics.iter().map(|s| s.to_string()).collect())
            .await
            .unwrap();
    }

    // ── Tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_successful_startup_shutdown() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        assert!(fx.consumer.close().await.is_ok());
    }

    /// Java `testInvalidGroupId` fails at construction; the Rust injectable
    /// constructor doesn't validate (mirrors Java's 17-arg
    /// visible-for-testing ctor, which also doesn't), so the group-id guard is
    /// exercised through `subscribe` (which calls `maybeThrowInvalidGroupIdException`).
    #[tokio::test]
    async fn test_invalid_group_id() {
        let mut fx = build_fixture("", ShareAcknowledgementMode::IMPLICIT);
        let err = fx
            .consumer
            .subscribe(vec!["foo".to_string()])
            .await
            .expect_err("empty group id");
        assert!(err.to_string().contains("valid group.id"), "got: {err}");
    }

    #[tokio::test]
    async fn test_wakeup_before_calling_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["foo"]).await;
        fx.consumer.wakeup();
        let err = fx.consumer.poll(Duration::ZERO).await.expect_err("wakeup");
        assert!(matches!(err, KafkaError::Wakeup(_)), "got: {err}");
        // The previously-consumed wakeup does not fire again.
        fx.consumer.poll(Duration::ZERO).await.expect("second poll ok");
    }

    #[tokio::test]
    async fn test_wakeup_after_empty_fetch() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["foo"]).await;
        // First collect fires wakeup and returns empty; then the poll loop's
        // next `maybe_trigger_wakeup` throws.
        wakeup_on_collect_call(&fx, 1);
        let err = fx
            .consumer
            .poll(Duration::from_secs(60))
            .await
            .expect_err("wakeup after empty fetch");
        assert!(matches!(err, KafkaError::Wakeup(_)), "got: {err}");
        fx.consumer.poll(Duration::ZERO).await.expect("second poll ok");
    }

    #[tokio::test]
    async fn test_wakeup_after_non_empty_fetch() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["foo"]).await;
        // collect fires wakeup AND returns records — the records are returned,
        // so the wakeup is ignored on this poll.
        wakeup_on_collect_call(&fx, 1);
        push_fetch(&fx, fetch_with_records("foo", 3, &[2]));
        let recs = fx
            .consumer
            .poll(Duration::from_secs(60))
            .await
            .expect("records returned, wakeup ignored");
        assert_eq!(recs.count(), 1);
        // The ignored wakeup fires on the next poll.
        let err = fx.consumer.poll(Duration::ZERO).await.expect_err("deferred wakeup");
        assert!(matches!(err, KafkaError::Wakeup(_)), "got: {err}");
    }

    #[tokio::test]
    async fn test_fail_on_closed_consumer() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.consumer.close().await.unwrap();
        let e = fx.consumer.subscription().expect_err("closed");
        assert!(e.to_string().contains("This consumer has already been closed."), "got: {e}");
        let e2 = fx.consumer.acquisition_lock_timeout_ms().expect_err("closed");
        assert!(e2.to_string().contains("This consumer has already been closed."), "got: {e2}");
    }

    #[tokio::test]
    async fn test_should_send_one_share_fetch_event_per_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["test-topic"]).await;
        // Empty fetches — the poll loop spins until the timer expires, but only
        // one ShareFetchEvent may be sent.
        fx.consumer.poll(Duration::from_millis(100)).await.expect("poll ok");
        assert_eq!(fx.handler.count_of("ShareFetch"), 1, "exactly one ShareFetchEvent per poll");
    }

    #[tokio::test]
    async fn test_control_records_on_empty_fetch() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["foo"]).await;
        // A fetch containing only a GAP (no records) — the GAP acknowledgement
        // must be sent as a ShareAcknowledgeAsyncEvent.
        let tp = tip("foo", 0);
        let mut batch = ShareInFlightBatch::new(0, tp.clone(), DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS);
        batch.add_gap(1);
        let mut fetch = ShareFetch::empty();
        fetch.add(tp, batch);
        push_fetch(&fx, fetch);

        fx.consumer.poll(Duration::ZERO).await.expect("poll ok");
        assert!(
            fx.handler.count_of("ShareAcknowledgeAsync") >= 1,
            "a ShareAcknowledgeAsyncEvent must be sent for the control-record GAP: {:?}",
            fx.handler.added_names()
        );
    }

    #[tokio::test]
    async fn test_explicit_mode_unacknowledged_records() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::EXPLICIT);
        subscribe_ok(&mut fx, &["test-topic"]).await;

        push_fetch(&fx, fetch_with_records("test-topic", 0, &[0, 1]));
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("first poll ok");
        assert_eq!(records.count(), 2, "should receive 2 records");
        assert_eq!(
            fx.consumer.acquisition_lock_timeout_ms().unwrap(),
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS
        );

        // Second poll fails: records not acknowledged.
        let e = fx.consumer.poll(Duration::from_millis(100)).await.expect_err("unacked");
        assert!(
            e.to_string()
                .contains("All records must be acknowledged in explicit acknowledgement mode.")
        );

        // Acknowledge one of the two — still fails.
        let recs: Vec<&ConsumerRecord<String, String>> = (&records).into_iter().collect();
        fx.consumer.acknowledge(recs[0]).unwrap();
        let e = fx.consumer.poll(Duration::from_millis(100)).await.expect_err("still unacked");
        assert!(e.to_string().contains("All records must be acknowledged"));

        // Acknowledge the other — now poll succeeds with a fresh fetch.
        fx.consumer.acknowledge(recs[1]).unwrap();
        push_fetch(&fx, fetch_with_records("test-topic", 0, &[2, 3]));
        let new_records = fx
            .consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll ok after acking all");
        assert_eq!(new_records.count(), 2, "should receive 2 new records");
    }

    /// Java `testExplicitModeRenewAndAcknowledgeOnPoll`. RENEW re-delivery is
    /// implemented faithfully: `acknowledge(_, RENEW)` captures a clone of the
    /// renewed record (the rare path; the hot ACCEPT path never clones —
    /// §27/§11), which `take_acknowledged_records` routes into `renewing_records`
    /// and `renew`/`take_renewals` cycle back into in-flight for re-delivery on
    /// a later poll. Also asserts a post-renew `acknowledge(rec, ACCEPT)` on the
    /// re-delivered record succeeds.
    #[tokio::test]
    async fn test_explicit_mode_renew_and_acknowledge_on_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::EXPLICIT);
        subscribe_ok(&mut fx, &["test-topic"]).await;

        // A single fixed topic-id-partition so the later renew event matches.
        let tp = tip("test-topic", 0);
        let mut batch = ShareInFlightBatch::new(0, tp.clone(), DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS);
        batch.add_record(record("test-topic", 0, 0));
        batch.add_record(record("test-topic", 0, 1));
        let mut first_fetch = ShareFetch::empty();
        first_fetch.add(tp.clone(), batch);
        push_fetch(&fx, first_fetch);

        // First poll returns the 2 records.
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("first poll ok");
        assert_eq!(records.count(), 2, "should receive 2 records");
        assert_eq!(
            fx.consumer.acquisition_lock_timeout_ms().unwrap(),
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS
        );

        // Renew offset 0, accept offset 1.
        let recs: Vec<&ConsumerRecord<String, String>> = (&records).into_iter().collect();
        fx.consumer.acknowledge_with_type(recs[0], AcknowledgeType::Renew).unwrap();
        fx.consumer.acknowledge_with_type(recs[1], AcknowledgeType::Accept).unwrap();

        // Second poll: offset 0 is renewing (awaiting the broker RENEW response),
        // so no records are returned.
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("second poll ok");
        assert_eq!(records.count(), 0, "renewing means no records yet");

        // The broker RENEW response arrives via a ShareAcknowledgementEvent.
        let mut acks = Acknowledgements::empty();
        acks.add(0, AcknowledgeType::Renew);
        acks.complete(None);
        let mut map = IndexMap::new();
        map.insert(tp.clone(), acks);
        fx.ack_handler.add(ShareAcknowledgementEvent::new(map, true, None));

        // Third poll re-delivers the renewed record (offset 0).
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("third poll ok");
        assert_eq!(records.count(), 1, "renewed record re-delivered");
        let recs: Vec<&ConsumerRecord<String, String>> = (&records).into_iter().collect();
        assert_eq!(recs[0].offset(), 0, "the re-delivered record is offset 0");

        // A post-renew ACCEPT on the re-delivered record succeeds (would return
        // Err("The record cannot be acknowledged.") if offset tracking had been dropped).
        fx.consumer
            .acknowledge(recs[0])
            .expect("post-renew ACCEPT on re-delivered record must succeed");
    }

    /// After a poll returns records (setting the acquisition-lock timeout), a
    /// later poll that finds no new data must NOT clobber the retained timeout —
    /// Java assigns `currentFetch = fetch` only when the fetch is non-empty
    /// (`ShareConsumerImpl.java:628-629`), so `acquisition_lock_timeout_ms()`
    /// keeps returning the last `Some(t)`. Regression test for the in-place
    /// `collect` restructuring (would return `None` if `current_fetch` were
    /// overwritten with an empty fetch).
    #[tokio::test]
    async fn test_acquisition_lock_timeout_retained_across_empty_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["test-topic"]).await;

        push_fetch(&fx, fetch_with_records("test-topic", 0, &[0, 1]));
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("first poll ok");
        assert_eq!(records.count(), 2);
        assert_eq!(
            fx.consumer.acquisition_lock_timeout_ms().unwrap(),
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
            "timeout set after a records poll"
        );

        // Second poll finds no new data (collector queue empty → empty fetch).
        let records = fx.consumer.poll(Duration::from_millis(100)).await.expect("second poll ok");
        assert!(records.is_empty(), "no new records");
        assert_eq!(
            fx.consumer.acquisition_lock_timeout_ms().unwrap(),
            DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS,
            "an empty poll must NOT clobber the retained acquisition-lock timeout"
        );
    }

    #[tokio::test]
    async fn test_subscribe_generates_event() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["topic1"]).await;
        assert_eq!(fx.consumer.subscription().unwrap(), HashSet::from(["topic1".to_string()]));
        assert_eq!(fx.handler.count_of("ShareSubscriptionChange"), 1);
    }

    #[tokio::test]
    async fn test_unsubscribe_generates_unsubscribe_event() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.consumer.unsubscribe().await.unwrap();
        assert_eq!(fx.handler.count_of("ShareUnsubscribe"), 1);
    }

    #[tokio::test]
    async fn test_subscribe_to_empty_list_acts_as_unsubscribe() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.consumer.subscribe(Vec::new()).await.unwrap();
        assert_eq!(fx.handler.count_of("ShareUnsubscribe"), 1);
        assert_eq!(fx.handler.count_of("ShareSubscriptionChange"), 0);
    }

    #[tokio::test]
    async fn test_subscription_on_empty_topic() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        let err = fx.consumer.subscribe(vec!["  ".to_string()]).await.expect_err("blank topic");
        assert!(err.to_string().contains("cannot contain null or empty topic"), "got: {err}");
    }

    #[tokio::test]
    async fn test_background_error() {
        let mut fx = build_fixture("shareGroupA", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["t1"]).await;
        fx.bg_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: KafkaError::illegal_state("Nobody expects the Spanish Inquisition"),
                },
                enqueued_ms: 0,
            })
            .unwrap();
        let err = fx.consumer.poll(Duration::ZERO).await.expect_err("bg error");
        assert!(err.to_string().contains("Nobody expects the Spanish Inquisition"), "got: {err}");
    }

    #[tokio::test]
    async fn test_multiple_background_errors() {
        let mut fx = build_fixture("shareGroupA", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["t1"]).await;
        fx.bg_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: KafkaError::illegal_state("Nobody expects the Spanish Inquisition"),
                },
                enqueued_ms: 0,
            })
            .unwrap();
        fx.bg_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error { error: KafkaError::illegal_state("Spam, Spam, Spam") },
                enqueued_ms: 0,
            })
            .unwrap();
        let err = fx.consumer.poll(Duration::ZERO).await.expect_err("bg error");
        // The FIRST error is surfaced.
        assert!(err.to_string().contains("Nobody expects the Spanish Inquisition"), "got: {err}");
    }

    #[tokio::test]
    async fn test_ensure_poll_event_sent_on_consumer_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["topic"]).await;
        push_fetch(&fx, fetch_with_records("topic", 0, &[2]));
        fx.consumer.poll(Duration::from_millis(100)).await.expect("poll ok");
        assert!(fx.handler.count_of("SharePoll") >= 1, "a SharePollEvent must be sent");
        assert_eq!(fx.handler.count_of("ShareSubscriptionChange"), 1);
    }

    #[tokio::test]
    async fn test_acknowledgement_commit_callback_registration_event() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        let callback = Arc::new(NoopCallback);
        fx.consumer.set_acknowledgement_commit_callback(Some(callback.clone()));
        assert_eq!(fx.handler.count_of("ShareAcknowledgementCommitCallbackRegistration"), 1);
        // Setting again does not add another registration event.
        fx.consumer.set_acknowledgement_commit_callback(Some(callback));
        assert_eq!(fx.handler.count_of("ShareAcknowledgementCommitCallbackRegistration"), 1);
    }

    #[tokio::test]
    async fn test_acknowledgement_commit_callback_registration_event_null() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        // Initially null → setting null adds no event.
        fx.consumer.set_acknowledgement_commit_callback(None);
        assert_eq!(fx.handler.count_of("ShareAcknowledgementCommitCallbackRegistration"), 0);
        // Set a callback → one registration event.
        fx.consumer.set_acknowledgement_commit_callback(Some(Arc::new(NoopCallback)));
        assert_eq!(fx.handler.count_of("ShareAcknowledgementCommitCallbackRegistration"), 1);
        // Clear it → one more (de-)registration event.
        fx.consumer.set_acknowledgement_commit_callback(None);
        assert_eq!(fx.handler.count_of("ShareAcknowledgementCommitCallbackRegistration"), 2);
    }

    #[tokio::test]
    async fn test_stop_find_coordinator_on_close() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.consumer.close().await.unwrap();
        let names = fx.handler.added_names();
        let pos = |n: &str| names.iter().position(|x| x == n);
        let ack = pos("ShareAcknowledgeOnClose").expect("ack-on-close sent");
        let unsub = pos("ShareUnsubscribe").expect("unsubscribe sent");
        let stop = pos("StopFindCoordinatorOnClose").expect("stop-find-coordinator sent");
        assert!(ack < unsub && unsub < stop, "event order wrong: {names:?}");
    }

    #[tokio::test]
    async fn test_close_with_topic_authorization_error_is_swallowed() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.bg_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: KafkaError::topic_authorization(HashSet::from(["test-topic".to_string()])),
                },
                enqueued_ms: 0,
            })
            .unwrap();
        assert!(
            fx.consumer.close().await.is_ok(),
            "topic-auth error must be swallowed during close"
        );
    }

    #[tokio::test]
    async fn test_close_with_invalid_topic_error_is_swallowed() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        fx.bg_tx
            .send(BackgroundEventEnvelope {
                event: BackgroundEvent::Error {
                    error: KafkaError::invalid_topics(HashSet::from(["!test-topic".to_string()])),
                },
                enqueued_ms: 0,
            })
            .unwrap();
        assert!(
            fx.consumer.close().await.is_ok(),
            "invalid-topic error must be swallowed during close"
        );
    }

    // ── §31 regression tests (consumer-threading.md §31 "Tests required") ──

    /// A callback that records how many times it fired and whether the most
    /// recent invocation observed an error.
    #[derive(Default)]
    struct CountingCallback {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl AcknowledgementCommitCallback for CountingCallback {
        async fn on_complete(&self, _offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&KafkaError>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct NoopCallback;
    #[async_trait]
    impl AcknowledgementCommitCallback for NoopCallback {
        async fn on_complete(&self, _offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&KafkaError>) {}
    }

    fn completed_ack_event(topic: &str, partition: i32, offset: i64) -> ShareAcknowledgementEvent {
        let mut acks = Acknowledgements::empty();
        acks.add(offset, AcknowledgeType::Accept);
        acks.complete(None);
        let mut map = IndexMap::new();
        map.insert(tip(topic, partition), acks);
        ShareAcknowledgementEvent::new(map, false, None)
    }

    /// §31 test 1: the acknowledgement commit callback is invoked INLINE on the
    /// caller's task during `poll` (synchronously observable when poll returns —
    /// not deferred to a spawned task).
    #[tokio::test]
    async fn test_acknowledgement_callback_invoked_on_caller_task_during_poll() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["t"]).await;
        let callback = Arc::new(CountingCallback::default());
        fx.consumer.set_acknowledgement_commit_callback(Some(callback.clone()));

        // The bg task delivered a completed acknowledgement.
        fx.ack_handler.add(completed_ack_event("t", 0, 0));

        assert_eq!(callback.calls.load(Ordering::SeqCst), 0, "not yet invoked before poll");
        fx.consumer.poll(Duration::ZERO).await.expect("poll ok");
        // Invoked synchronously during the poll call (caller's task), so it is
        // already observable immediately after poll returns.
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1, "callback fired inline during poll");
    }

    /// §31 test 2: the callback fires exactly once per completed commit — the
    /// completed-acknowledgements list is cleared after invocation, so a
    /// subsequent poll does not re-invoke it.
    #[tokio::test]
    async fn test_acknowledgement_callback_fires_exactly_once_per_commit() {
        let mut fx = build_fixture("group-id", ShareAcknowledgementMode::IMPLICIT);
        subscribe_ok(&mut fx, &["t"]).await;
        let callback = Arc::new(CountingCallback::default());
        fx.consumer.set_acknowledgement_commit_callback(Some(callback.clone()));

        fx.ack_handler.add(completed_ack_event("t", 0, 0));
        fx.consumer.poll(Duration::ZERO).await.expect("poll 1 ok");
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);

        // No new acknowledgement event → no further callback invocation.
        fx.consumer.poll(Duration::ZERO).await.expect("poll 2 ok");
        assert_eq!(
            callback.calls.load(Ordering::SeqCst),
            1,
            "callback must fire exactly once per commit"
        );
    }
}
