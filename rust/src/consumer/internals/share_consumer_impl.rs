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

//! `ShareConsumerImpl` — the share consumer (KIP-932) on the application
//! side: the `poll` loop, the acknowledgement modes, the acknowledgement
//! commit callback and the close sequence.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.ShareConsumerImpl`.
//!
//! # Background task
//!
//! One background task per instance, running the consumer's
//! [`ConsumerNetworkThread`] over the share request managers
//! ([`RequestManagers::supplier_with_share_acknowledgement_event_handler`]),
//! as `share-consumer.md` §2 requires. The application side talks to it as
//! `AsyncKafkaConsumer` does: [`ApplicationEventHandler`] events one way,
//! background events and [`ShareAcknowledgementEvent`]s the other.
//!
//! # Cancellation
//!
//! Every `async fn` here is a future the caller can drop (a
//! `tokio::time::timeout` or a `select!` around it). Java's calls cannot be
//! abandoned half-way, so a dropped call must leave the consumer as if the
//! call had stopped at a point where Java's state is consistent. For `poll`
//! that means: no record the application did not receive is ever
//! acknowledged, and no acknowledgement the application made is lost.
//!
//! - **Acknowledgements taken for sending.** `pollForFetches` takes the
//!   current fetch's acknowledgements before it collects and sends them once
//!   it knows whether to piggy-back them on a `ShareFetchEvent`. They are held
//!   in a [`PendingAcknowledgements`] guard: a call dropped in between sends
//!   them in a `ShareAcknowledgeAsyncEvent`, as Java sends them when the fetch
//!   is not empty.
//! - **Records decoded and not returned.** The collector gathers into a
//!   [`ShareFetchCollectState`] the consumer keeps, so a call dropped while a
//!   deserializer is pending leaves every record it decoded there, and the
//!   next `poll` goes on from there and returns them all
//!   ([`ShareFetchCollector::collect_with_state`]). A renewed record
//!   redelivered by a dropped call stays in the current fetch.
//! - **Records not handed out.** Records reach the application only through
//!   [`ShareFetch::records`], called after the last `.await` of `poll`. A
//!   record taken into the current fetch and not handed out (the call was
//!   dropped while the acknowledgement commit callback ran) is not
//!   acknowledged implicitly, and does not count for the explicit-mode check
//!   ([`ShareFetch::acknowledge_all_handed_out`],
//!   [`ShareFetch::check_all_handed_out_are_acknowledged`]); the next `poll`
//!   returns it.
//! - **Completed acknowledgements.** Each `(partition, acknowledgements)`
//!   entry leaves the queue before its callback runs, so a call dropped while
//!   the callback runs neither repeats that entry nor loses the ones after it.
//! - **`close`.** A dropped `close` leaves the consumer open; the next `close`
//!   resumes it without repeating the acknowledge-on-close or the stop signal,
//!   and stops the background task ([`ShareConsumerImpl::close_with_timeout`]).
//!
//! # Not translated
//!
//! - Java's `acquire()` / `release()` reentrancy guard: `&mut self` makes a
//!   second caller, and a call from inside the acknowledgement commit
//!   callback (which gets no consumer reference), a compile-time error
//!   (Milestone 18 decision D6).
//! - `ClientTelemetryReporter`, `AppInfoParser` and the metric reporters, as
//!   for the consumer (`async_kafka_consumer.rs`, ledger G16).
//!   `registerMetricForSubscription` / `unregisterMetricFromSubscription`
//!   are not translated either, and `clientInstanceId` is not on the public
//!   `ShareConsumer` trait: KIP-714 is left out as the `Consumer` trait leaves
//!   it out (Milestone 18 decision D2, 2026-11-20).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use log::{debug, error, info, trace, warn};
use tokio::sync::{mpsc, oneshot};

use crate::KafkaClient;
use crate::common::internals::ClusterResourceListeners;
use crate::common::metrics::{Metrics, MetricsView};
use crate::common::serialization::DynDeserializer;
use crate::common::utils::internals::LogContext;
use crate::common::utils::{Time, Utils};
use crate::common::{Error, TopicIdPartition, Uuid};
use crate::consumer::AcknowledgeOptions;
use crate::consumer::AcknowledgeType;
use crate::consumer::AcknowledgementCommitCallback;
use crate::consumer::ConsumerConfig;
use crate::consumer::ConsumerRecord;
use crate::consumer::ConsumerRecords;
use crate::consumer::ShareConsumerHandle;
use crate::consumer::internals::AcknowledgementCommitCallbackHandler;
use crate::consumer::internals::Acknowledgements;
use crate::consumer::internals::ConsumerNetworkThread;
use crate::consumer::internals::ConsumerShareFetchError;
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::Deserializers;
use crate::consumer::internals::NetworkClientDelegate;
use crate::consumer::internals::NodeAcknowledgements;
use crate::consumer::internals::RequestManagers;
use crate::consumer::internals::ShareAcknowledgementMode;
use crate::consumer::internals::ShareConsumerMetadata;
use crate::consumer::internals::ShareFetch;
use crate::consumer::internals::ShareFetchBuffer;
use crate::consumer::internals::ShareFetchCollector;
use crate::consumer::internals::ShareFetchConfig;
use crate::consumer::internals::ShareFetchMetricsManager;
use crate::consumer::internals::SubscriptionState;
use crate::consumer::internals::WakeupTrigger;
use crate::consumer::internals::async_kafka_consumer::{
    CloseDeserializersOnError, NetworkThreadCloseHandle, await_event_completion, build_close_handle_fns,
};
use crate::consumer::internals::events::{
    ApplicationEvent, ApplicationEventHandler, ApplicationEventProcessor, BackgroundEvent, BackgroundEventEnvelope,
    BackgroundEventHandler, CompletableEvent, CompletableEventHandle, CompletableEventReaper,
    ShareAcknowledgementEventHandler,
};
use crate::consumer::internals::metrics::{AsyncConsumerMetrics, KafkaShareConsumerMetrics};
use crate::consumer::internals::share_fetch_collector::{ShareFetchCollectResult, ShareFetchCollectState};

#[cfg(test)]
type CollectStub<K, V> = Box<dyn FnMut() -> ShareFetchCollectResult<K, V> + Send>;
#[cfg(test)]
type AwaitNotEmptyStub = Box<dyn FnMut(i64) + Send>;

/// Java's `IllegalStateException` message for a call on a closed consumer.
const CONSUMER_CLOSED_MSG: &str = "This consumer has already been closed.";

/// This share consumer implementation uses an event handler to process
/// application events so that the network I/O can be processed in a dedicated
/// background task.
///
/// Translated from `org.apache.kafka.clients.consumer.internals.ShareConsumerImpl`.
/// Confined to the caller's task: the fetch, the acknowledgement state and the
/// callback handler are plain fields (`share-consumer.md` §6); only the
/// subscription state, the metadata and the fetch buffer are shared with the
/// background task. No lock is held across an `.await`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl")]
pub(crate) struct ShareConsumerImpl<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    application_event_handler: Arc<ApplicationEventHandler>,
    /// The background task, closed by `close` (Java's
    /// `applicationEventHandler.close(Duration)`, which closes the network
    /// thread the handler owns).
    network_thread_close: NetworkThreadCloseHandle,
    /// Java's `applicationEventHandler.maximumTimeToWait()`: written by the
    /// background task after each iteration.
    max_time_to_wait_ms: Arc<AtomicI64>,
    time: Arc<dyn Time>,
    share_fetch_metrics_manager: Arc<ShareFetchMetricsManager>,
    kafka_share_consumer_metrics: Arc<KafkaShareConsumerMetrics>,
    async_consumer_metrics: Arc<AsyncConsumerMetrics>,
    client_id: Arc<str>,
    group_id: Option<String>,
    acknowledgement_event_handler: Arc<ShareAcknowledgementEventHandler>,
    background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    /// The background-event queue depth Java reads with
    /// `backgroundEventQueue.size()`; the channel has no `len()`
    /// (`AsyncKafkaConsumer` keeps the same counter).
    background_event_queue_size: Arc<AtomicI64>,
    background_event_reaper: CompletableEventReaper,
    current_fetch: ShareFetch<K, V>,
    acknowledgement_commit_callback_handler: Option<AcknowledgementCommitCallbackHandler>,
    completed_acknowledgements: VecDeque<HashMap<TopicIdPartition, Acknowledgements>>,
    acknowledgement_mode: ShareAcknowledgementMode,
    /// A thread-safe [`ShareFetchBuffer`] for the results that are populated in
    /// the background task when the results are available. Because of the
    /// interaction of the fetch buffer in the application task and the
    /// background task, this is shared between the two and is thus designed to
    /// be thread-safe.
    fetch_buffer: Arc<ShareFetchBuffer>,
    /// Holds the consumer's deserializers (Java's `deserializers` field and the
    /// collector share one object).
    fetch_collector: ShareFetchCollector<K, V>,
    /// What a dropped `poll` had collected (see the module doc).
    collect_state: ShareFetchCollectState<K, V>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    metadata: Arc<ShareConsumerMetadata>,
    metrics: Arc<Metrics>,
    request_timeout_ms: i32,
    default_api_timeout_ms: i32,
    /// Java's `volatile boolean closed`, set once `close` has run every step
    /// (see `close_with_timeout`).
    closed: Arc<AtomicBool>,
    /// How far a dropped `close` got, so a retried one resumes there
    /// (see `close_with_timeout`).
    close_progress: CloseProgress,
    /// The first failure of a close step, kept with `close_progress` so that a
    /// `close` resuming a dropped one returns it, as Java's single `close`
    /// returns `firstException`.
    close_first_error: Option<Error>,
    in_flight_poll: Option<InFlightPoll>,
    wakeup_trigger: WakeupTrigger,
    should_send_share_fetch_event: bool,
    /// Test-only stand-in for Java's `mock(ShareFetchCollector.class)`
    /// (`ShareConsumerImplTest`): when set, `collect` returns what it returns
    /// instead of reading the fetch buffer (translation-checklist R3 allows a
    /// faithful port of a Java test override).
    #[cfg(test)]
    collect_stub: Option<CollectStub<K, V>>,
    /// Test-only stand-in for Java's `mock(ShareFetchBuffer.class)` with a
    /// stubbed `awaitNotEmpty`: called with the wait's timeout instead of
    /// waiting on the buffer.
    #[cfg(test)]
    await_not_empty_stub: Option<AwaitNotEmptyStub>,
    /// The background task's request managers, for the tests that check
    /// what the constructor gave them (the consumer keeps the same `Arc`).
    #[cfg(test)]
    request_managers: Option<Arc<Mutex<RequestManagers>>>,
}

/// The pieces [`ShareConsumerImpl::with_components`] assembles the consumer
/// from: Java's visible-for-testing 19-argument constructor, which takes the
/// collaborators the other constructors build.
///
/// Rust-only (DoD §7): a struct where Java has a parameter list, as
/// `AsyncKafkaConsumerComponents` for the consumer. The key and value
/// deserializers are in `fetch_collector`; the application event handler comes
/// with the background task it talks to (`network_thread_close`,
/// `max_time_to_wait_ms`, `wakeup_trigger`); the background event queue is the
/// receiver and its depth counter.
pub(crate) struct ShareConsumerImplComponents<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub(crate) client_id: Arc<str>,
    pub(crate) fetch_buffer: Arc<ShareFetchBuffer>,
    pub(crate) fetch_collector: ShareFetchCollector<K, V>,
    pub(crate) share_fetch_metrics_manager: Arc<ShareFetchMetricsManager>,
    pub(crate) time: Arc<dyn Time>,
    pub(crate) application_event_handler: Arc<ApplicationEventHandler>,
    pub(crate) network_thread_close: NetworkThreadCloseHandle,
    pub(crate) max_time_to_wait_ms: Arc<AtomicI64>,
    pub(crate) wakeup_trigger: WakeupTrigger,
    pub(crate) acknowledgement_event_handler: Arc<ShareAcknowledgementEventHandler>,
    pub(crate) background_event_rx: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
    pub(crate) background_event_queue_size: Arc<AtomicI64>,
    pub(crate) background_event_reaper: CompletableEventReaper,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) async_consumer_metrics: Arc<AsyncConsumerMetrics>,
    pub(crate) subscriptions: Arc<Mutex<SubscriptionState>>,
    pub(crate) metadata: Arc<ShareConsumerMetadata>,
    pub(crate) request_timeout_ms: i32,
    pub(crate) default_api_timeout_ms: i32,
    pub(crate) group_id: Option<String>,
    /// Java passes the configuration string and parses it in the constructor.
    pub(crate) acknowledgement_mode: ShareAcknowledgementMode,
}

/// The application side of the `SharePollEvent` last submitted: Java keeps
/// the event object (`inFlightPoll`); the event moves to the background task
/// here, so its deadline and its completion flag are kept.
///
/// Rust-only (DoD §7; PLAN §8.1 row 12).
struct InFlightPoll {
    deadline_ms: i64,
    is_complete: Arc<AtomicBool>,
}

impl InFlightPoll {
    /// `SharePollEvent#isComplete`.
    fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Acquire)
    }

    /// `SharePollEvent#isExpired(Time)`.
    fn is_expired(&self, time: &dyn Time) -> bool {
        time.milliseconds() >= self.deadline_ms
    }
}

/// What `ShareConsumerImpl#collect` returns: a new `ShareFetch`, or
/// `currentFetch` itself. Rust-only: Java returns the object either way.
enum Collected<K, V> {
    New(ShareFetch<K, V>),
    Current,
}

/// A failure of a `poll` step: Java's `ShareFetchException`, which `poll`
/// catches, or any other error.
enum PollError<K, V> {
    ShareFetch(ConsumerShareFetchError<K, V>),
    Other(Error),
}

impl<K, V> From<Error> for PollError<K, V> {
    fn from(error: Error) -> Self {
        Self::Other(error)
    }
}

/// The acknowledgements `pollForFetches` took from the current fetch and has
/// not sent yet.
///
/// Rust-only (translation-checklist R4 item 8): Java holds them in a local
/// across `collect`, which cannot be abandoned. A `poll` dropped while it
/// collects drops this guard, which sends them in a
/// `ShareAcknowledgeAsyncEvent`, as Java does when the fetch is not empty, so
/// the application's acknowledgements are not lost.
struct PendingAcknowledgements {
    acknowledgements_map: Option<IndexMap<TopicIdPartition, NodeAcknowledgements>>,
    application_event_handler: Arc<ApplicationEventHandler>,
    time: Arc<dyn Time>,
    default_api_timeout_ms: i32,
}

impl PendingAcknowledgements {
    /// Takes the acknowledgements out, to be sent by the caller.
    fn take(&mut self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.acknowledgements_map.take().unwrap_or_default()
    }

    /// Drops the acknowledgements without sending them: Java's `collect`
    /// loses them when the collector throws (see `ShareConsumerImpl::collect`).
    fn discard(&mut self) {
        self.acknowledgements_map = None;
    }
}

impl Drop for PendingAcknowledgements {
    fn drop(&mut self) {
        let Some(acknowledgements_map) = self.acknowledgements_map.take() else {
            return;
        };
        if acknowledgements_map.is_empty() {
            return;
        }
        let now = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now, i64::from(self.default_api_timeout_ms));
        let event = ApplicationEvent::ShareAcknowledgeAsync { acknowledgements_map, deadline_ms };
        if let Err(e) = self.application_event_handler.add(event, now) {
            warn!("Failed to send the acknowledgements of a cancelled poll: {e}");
        }
    }
}

/// Records the end of a `poll` when dropped: Java's `finally` in `poll`
/// (translation-checklist R4 item 8).
struct RecordPollEnd {
    kafka_share_consumer_metrics: Arc<KafkaShareConsumerMetrics>,
    time: Arc<dyn Time>,
}

impl Drop for RecordPollEnd {
    fn drop(&mut self) {
        self.kafka_share_consumer_metrics.record_poll_end(self.time.milliseconds());
    }
}

/// What `ShareConsumerImpl::start_background_task` returns: Java's
/// `ApplicationEventHandler`, which owns the network thread it starts.
/// Rust-only (DoD §7): the handler is the channel's sending side, and the task
/// is joined through its close handle.
struct BackgroundTask {
    application_event_handler: Arc<ApplicationEventHandler>,
    network_thread_close: NetworkThreadCloseHandle,
    max_time_to_wait_ms: Arc<AtomicI64>,
    #[cfg(test)]
    request_managers: Arc<Mutex<RequestManagers>>,
}

/// How far `close` has gone, kept so that a `close` retried after a dropped
/// one resumes where it stopped.
///
/// Rust-only (DoD §7): Java's `close` cannot be abandoned, so it makes each
/// step once. Sending the `ShareAcknowledgeOnCloseEvent` twice is not
/// harmless (the background task's second `acknowledgeOnClose` fails the close
/// requests in flight and reports their acknowledgements again), so it is
/// sent once and a retry awaits the same completion; signalling the
/// background task to stop is also done once, a retry after it only joins.
/// Re-sending the `ShareUnsubscribeEvent` is harmless (a second leave returns
/// the one in progress, or completes at once).
enum CloseProgress {
    /// Nothing sent yet.
    NotStarted,
    /// The `ShareAcknowledgeOnCloseEvent` is sent; its completion is awaited.
    AcknowledgingOnClose(oneshot::Receiver<Result<(), Error>>),
    /// The acknowledge-on-close step is done.
    AcknowledgedOnClose,
    /// The background task was told to stop; only the join is left.
    Signalled,
    /// The background task has been joined.
    Joined,
}

impl<K, V> ShareConsumerImpl<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a share consumer from `config`, starting its background task.
    ///
    /// # Errors
    ///
    /// Any construction failure, as a `KafkaError` with the message
    /// `"Failed to construct Kafka share consumer"` and the failure as its
    /// source (Java wraps every throwable so): for example an
    /// `InvalidGroupIdError` when `group.id` is missing or empty.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#ShareConsumerImpl")]
    pub(crate) fn new(
        config: ConsumerConfig,
        key_deserializer: Box<dyn DynDeserializer<K>>,
        value_deserializer: Box<dyn DynDeserializer<V>>,
    ) -> Result<Self, Error> {
        Self::new_inner(config, key_deserializer, value_deserializer)
            .map_err(|err| Error::kafka_message_source("Failed to construct Kafka share consumer", err))
    }

    /// The body of Java's constructor `try` block. Java's `catch` closes what
    /// was built; nothing here needs closing on a failure but the
    /// deserializers (`CloseDeserializersOnError`): the background task starts
    /// after the last fallible step.
    fn new_inner(
        mut config: ConsumerConfig,
        key_deserializer: Box<dyn DynDeserializer<K>>,
        value_deserializer: Box<dyn DynDeserializer<V>>,
    ) -> Result<Self, Error> {
        config.log_values();
        let time: Arc<dyn Time> = Arc::new(crate::common::utils::SystemTime);
        // Java's configuration is finalised by `postProcessParsedConfig`; as
        // `AsyncKafkaConsumer::new` does, apply its `client.id` rule here for a
        // configuration built field by field.
        config.maybe_override_client_id()?;

        // Java: `new GroupRebalanceConfig(config, ProtocolType.SHARE)` reads the
        // group id and the rack id (ledger G8).
        let rack_id = Some(config.client_rack()).filter(|rack| !rack.is_empty()).map(str::to_string);
        let client_id: Arc<str> = Arc::from(config.client_id());
        let group_id = config.group_id().map(str::to_string);
        Self::maybe_return_invalid_group_id_error(group_id.as_deref())?;
        let group_id_value = group_id.clone().unwrap_or_default();
        let log_context = Self::create_log_context(&client_id, &group_id_value);

        debug!("Initializing the Kafka share consumer");
        let request_timeout_ms = config.request_timeout_ms();
        let default_api_timeout_ms = config.default_api_timeout_ms;
        let metrics = ConsumerUtils::create_metrics(&config, Arc::clone(&time));
        let async_consumer_metrics = Arc::new(AsyncConsumerMetrics::new(
            Arc::clone(&metrics),
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP,
        ));

        let acknowledgement_mode = Self::initialize_acknowledgement_mode(&config)?;
        let deserializers =
            CloseDeserializersOnError::new(Arc::new(Deserializers::new(key_deserializer, value_deserializer)));
        let subscriptions = Arc::new(Mutex::new(ConsumerUtils::create_subscription_state(&config)?));
        let metadata = Arc::new(ShareConsumerMetadata::with_config(
            &config,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        crate::ClientUtils::maybe_bootstrap_metadata_synchronously(
            config.bootstrap_resolve_timeout_ms(),
            config.client_dns_lookup(),
            config.bootstrap_servers(),
            &metadata,
        )?;

        let share_fetch_metrics_manager =
            Arc::new(ConsumerUtils::create_share_fetch_metrics_manager(Arc::clone(&metrics)));
        let api_versions = Arc::new(crate::ApiVersions::new());
        let acknowledgement_event_handler = Arc::new(ShareAcknowledgementEventHandler::new());
        let (background_event_tx, background_event_rx) = mpsc::unbounded_channel();
        let background_event_queue_size = Arc::new(AtomicI64::new(0));
        let mut background_event_handler = BackgroundEventHandler::new(background_event_tx);
        background_event_handler
            .set_async_consumer_metrics(Arc::clone(&async_consumer_metrics), Arc::clone(&background_event_queue_size));
        let background_event_handler = Arc::new(background_event_handler);

        // This fetch buffer is shared between the application and the background task.
        let fetch_buffer = Arc::new(ShareFetchBuffer::new());
        let network_client_delegate = NetworkClientDelegate::supplier(
            Arc::clone(&time),
            log_context,
            metadata.metadata_arc(),
            &config,
            api_versions,
            Some(share_fetch_metrics_manager.throttle_time_sensor()),
            Arc::clone(&background_event_handler),
            true,
            Arc::clone(&async_consumer_metrics),
        )?;
        // Built before the background task, as the last fallible step (Java
        // builds it after the handler; it reads nothing the handler sets).
        let share_fetch_config = ShareFetchConfig::with_consumer_config(&config)?;
        let wakeup_trigger = WakeupTrigger::new();
        let completion_notify = network_client_delegate.wakeup_handle();
        let request_managers = RequestManagers::supplier_with_share_acknowledgement_event_handler(
            Arc::clone(&time),
            Arc::clone(&acknowledgement_event_handler),
            background_event_handler,
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            Arc::clone(&fetch_buffer),
            &config,
            &group_id_value,
            rack_id.as_deref(),
            Arc::clone(&share_fetch_metrics_manager),
            &metrics,
            &completion_notify,
        )?;

        let fetch_collector = ShareFetchCollector::new(
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            share_fetch_config,
            deserializers.disarm(),
        );
        let background_task = Self::start_background_task(
            &time,
            network_client_delegate,
            request_managers,
            &metadata,
            &subscriptions,
            &wakeup_trigger,
            &async_consumer_metrics,
            true,
        );

        config.mark_read_keys_used();
        config.log_unused();
        debug!("Kafka share consumer initialized");
        #[cfg(test)]
        let request_managers_for_test = Arc::clone(&background_task.request_managers);
        let consumer = Self::with_components(ShareConsumerImplComponents {
            client_id,
            fetch_buffer,
            fetch_collector,
            share_fetch_metrics_manager,
            time,
            application_event_handler: background_task.application_event_handler,
            network_thread_close: background_task.network_thread_close,
            max_time_to_wait_ms: background_task.max_time_to_wait_ms,
            wakeup_trigger,
            acknowledgement_event_handler,
            background_event_rx,
            background_event_queue_size,
            background_event_reaper: CompletableEventReaper::new(),
            metrics,
            async_consumer_metrics,
            subscriptions,
            metadata,
            request_timeout_ms,
            default_api_timeout_ms,
            group_id,
            acknowledgement_mode,
        });
        #[cfg(test)]
        let consumer = Self { request_managers: Some(request_managers_for_test), ..consumer };
        Ok(consumer)
    }

    /// Creates a share consumer over `client` (Java's visible-for-testing
    /// constructor over a `KafkaClient`, the `MockClient` of the tests): the
    /// background task runs the real request managers over it, spawned on the
    /// caller's runtime. Java builds `new Metrics(time)` and the
    /// `ShareConsumerMetrics` templates here; neither validates the group id.
    #[cfg(test)]
    #[expect(clippy::too_many_arguments, reason = "the Java constructor's parameters")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#ShareConsumerImpl")]
    pub(crate) fn with_client<C: KafkaClient + Send + 'static>(
        client_id: &str,
        group_id: &str,
        config: &ConsumerConfig,
        key_deserializer: Box<dyn DynDeserializer<K>>,
        value_deserializer: Box<dyn DynDeserializer<V>>,
        time: Arc<dyn Time>,
        client: C,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ShareConsumerMetadata>,
    ) -> Result<Self, Error> {
        use crate::consumer::internals::ShareConsumerMetrics;

        let metrics = Arc::new(Metrics::with_time(Arc::clone(&time)));
        let deserializers = Arc::new(Deserializers::new(key_deserializer, value_deserializer));
        let request_timeout_ms = config.request_timeout_ms();
        let default_api_timeout_ms = config.default_api_timeout_ms;
        let acknowledgement_mode = Self::initialize_acknowledgement_mode(config)?;
        let fetch_buffer = Arc::new(ShareFetchBuffer::new());
        let metrics_registry = ShareConsumerMetrics::new();
        let share_fetch_metrics_manager = Arc::new(ShareFetchMetricsManager::new(
            Arc::clone(&metrics),
            &metrics_registry.share_fetch_metrics,
        ));
        let fetch_collector = ShareFetchCollector::new(
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            ShareFetchConfig::with_consumer_config(config)?,
            deserializers,
        );
        let async_consumer_metrics = Arc::new(AsyncConsumerMetrics::new(
            Arc::clone(&metrics),
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP,
        ));
        let acknowledgement_event_handler = Arc::new(ShareAcknowledgementEventHandler::new());
        let (background_event_tx, background_event_rx) = mpsc::unbounded_channel();
        let background_event_queue_size = Arc::new(AtomicI64::new(0));
        let mut background_event_handler = BackgroundEventHandler::new(background_event_tx);
        background_event_handler
            .set_async_consumer_metrics(Arc::clone(&async_consumer_metrics), Arc::clone(&background_event_queue_size));
        let background_event_handler = Arc::new(background_event_handler);
        let mut network_client_delegate = NetworkClientDelegate::new(
            config,
            client,
            metadata.metadata_arc(),
            Arc::clone(&background_event_handler),
            true,
        );
        network_client_delegate.set_async_consumer_metrics(Arc::clone(&async_consumer_metrics));
        let rack_id = Some(config.client_rack()).filter(|rack| !rack.is_empty()).map(str::to_string);
        let completion_notify = network_client_delegate.wakeup_handle();
        let request_managers = RequestManagers::supplier_with_share_acknowledgement_event_handler(
            Arc::clone(&time),
            Arc::clone(&acknowledgement_event_handler),
            background_event_handler,
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            Arc::clone(&fetch_buffer),
            config,
            group_id,
            rack_id.as_deref(),
            Arc::clone(&share_fetch_metrics_manager),
            &metrics,
            &completion_notify,
        )?;
        let wakeup_trigger = WakeupTrigger::new();
        let background_task = Self::start_background_task(
            &time,
            network_client_delegate,
            request_managers,
            &metadata,
            &subscriptions,
            &wakeup_trigger,
            &async_consumer_metrics,
            false,
        );
        let consumer = Self::with_components(ShareConsumerImplComponents {
            client_id: Arc::from(client_id),
            fetch_buffer,
            fetch_collector,
            share_fetch_metrics_manager,
            time,
            application_event_handler: background_task.application_event_handler,
            network_thread_close: background_task.network_thread_close,
            max_time_to_wait_ms: background_task.max_time_to_wait_ms,
            wakeup_trigger,
            acknowledgement_event_handler,
            background_event_rx,
            background_event_queue_size,
            background_event_reaper: CompletableEventReaper::new(),
            metrics,
            async_consumer_metrics,
            subscriptions,
            metadata,
            request_timeout_ms,
            default_api_timeout_ms,
            group_id: Some(group_id.to_string()),
            acknowledgement_mode,
        });
        Ok(Self {
            request_managers: Some(Arc::clone(&background_task.request_managers)),
            ..consumer
        })
    }

    /// Assembles a share consumer from its collaborators: Java's
    /// visible-for-testing 19-argument constructor, which the other
    /// constructors' assembly ends in here.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#ShareConsumerImpl")]
    pub(crate) fn with_components(components: ShareConsumerImplComponents<K, V>) -> Self {
        let kafka_share_consumer_metrics = Arc::new(KafkaShareConsumerMetrics::new(Arc::clone(&components.metrics)));
        Self {
            application_event_handler: components.application_event_handler,
            network_thread_close: components.network_thread_close,
            max_time_to_wait_ms: components.max_time_to_wait_ms,
            time: components.time,
            share_fetch_metrics_manager: components.share_fetch_metrics_manager,
            kafka_share_consumer_metrics,
            async_consumer_metrics: components.async_consumer_metrics,
            client_id: components.client_id,
            group_id: components.group_id,
            acknowledgement_event_handler: components.acknowledgement_event_handler,
            background_event_rx: components.background_event_rx,
            background_event_queue_size: components.background_event_queue_size,
            background_event_reaper: components.background_event_reaper,
            current_fetch: ShareFetch::empty(),
            acknowledgement_commit_callback_handler: None,
            completed_acknowledgements: VecDeque::new(),
            acknowledgement_mode: components.acknowledgement_mode,
            fetch_buffer: components.fetch_buffer,
            fetch_collector: components.fetch_collector,
            collect_state: ShareFetchCollectState::new(),
            subscriptions: components.subscriptions,
            metadata: components.metadata,
            metrics: components.metrics,
            request_timeout_ms: components.request_timeout_ms,
            default_api_timeout_ms: components.default_api_timeout_ms,
            closed: Arc::new(AtomicBool::new(false)),
            close_progress: CloseProgress::NotStarted,
            close_first_error: None,
            in_flight_poll: None,
            wakeup_trigger: components.wakeup_trigger,
            should_send_share_fetch_event: false,
            #[cfg(test)]
            collect_stub: None,
            #[cfg(test)]
            await_not_empty_stub: None,
            #[cfg(test)]
            request_managers: None,
        }
    }

    /// Starts the background task over `network_client_delegate` and
    /// `request_managers`: Java's `ApplicationEventHandler` constructor, which
    /// creates and starts the `ConsumerNetworkThread` with the application
    /// event processor, as `AsyncKafkaConsumer::new` does. Returns the
    /// application event handler, the task's close handle and its
    /// `maximumTimeToWait` cell ([`BackgroundTask`]). `dedicated` runs the task on its own thread
    /// (production), otherwise on the caller's runtime (tests).
    #[expect(clippy::too_many_arguments, reason = "the collaborators of the background task")]
    fn start_background_task<C: KafkaClient + Send + 'static>(
        time: &Arc<dyn Time>,
        network_client_delegate: NetworkClientDelegate<C>,
        request_managers: RequestManagers,
        metadata: &Arc<ShareConsumerMetadata>,
        subscriptions: &Arc<Mutex<SubscriptionState>>,
        wakeup_trigger: &WakeupTrigger,
        async_consumer_metrics: &Arc<AsyncConsumerMetrics>,
        dedicated: bool,
    ) -> BackgroundTask {
        let event_notify = network_client_delegate.wakeup_handle();
        let (application_event_tx, application_event_rx) = mpsc::unbounded_channel();
        let application_event_queue_size = Arc::new(AtomicI64::new(0));
        let application_event_reaper = Arc::new(Mutex::new(CompletableEventReaper::new()));
        let request_managers = Arc::new(Mutex::new(request_managers));
        #[cfg(test)]
        let request_managers_for_test = Arc::clone(&request_managers);
        let application_event_processor = ApplicationEventProcessor::new(
            Arc::clone(&request_managers),
            metadata.metadata_arc(),
            Arc::clone(subscriptions),
            Arc::clone(&application_event_reaper),
            Arc::clone(time),
        );
        let mut application_event_handler =
            ApplicationEventHandler::new(application_event_tx, Arc::clone(&event_notify));
        application_event_handler
            .set_async_consumer_metrics(Arc::clone(async_consumer_metrics), Arc::clone(&application_event_queue_size));
        let max_time_to_wait_ms = Arc::new(AtomicI64::new(ConsumerNetworkThread::<C>::MAX_POLL_TIMEOUT_MS));
        let mut network_thread = ConsumerNetworkThread::new(
            Arc::clone(time),
            application_event_rx,
            application_event_reaper,
            application_event_processor,
            Arc::new(tokio::sync::Mutex::new(network_client_delegate)),
            request_managers,
            None,
            wakeup_trigger.clone(),
            Arc::clone(&max_time_to_wait_ms),
        );
        network_thread.set_async_consumer_metrics(Arc::clone(async_consumer_metrics), application_event_queue_size);
        let (signal_close_fn, wakeup_fn) = build_close_handle_fns(
            network_thread.running_handle(),
            network_thread.close_timeout_handle(),
            event_notify,
        );
        let network_thread_close = if dedicated {
            let (done_rx, thread_handle) = network_thread.spawn_dedicated();
            NetworkThreadCloseHandle::with_dedicated(signal_close_fn, wakeup_fn, done_rx, thread_handle)
        } else {
            NetworkThreadCloseHandle::new(signal_close_fn, wakeup_fn, tokio::spawn(network_thread.run()))
        };
        BackgroundTask {
            application_event_handler: Arc::new(application_event_handler),
            network_thread_close,
            max_time_to_wait_ms,
            #[cfg(test)]
            request_managers: Arc::clone(&request_managers_for_test),
        }
    }

    /// Returns the current subscription.
    ///
    /// # Panics
    ///
    /// With `"This consumer has already been closed."` when the consumer is
    /// closed (Java's `IllegalStateException`; CLAUDE.md §12.5).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#subscription")]
    pub(crate) fn subscription(&mut self) -> HashSet<String> {
        // Java's `acquireAndEnsureOpen` also throws a permanent bootstrap
        // failure (KIP-909); with no error channel, this keeps working after
        // one, as the consumer's sync getters do
        // (`AsyncKafkaConsumer::subscription`, `assignment`, `paused`).
        self.panic_if_closed();
        self.lock_subscriptions().subscription()
    }

    /// Subscribes to `topics`; an empty list is the same as
    /// [`Self::unsubscribe`].
    ///
    /// # Errors
    ///
    /// - `InvalidGroupIdError` when the consumer has no group id;
    /// - `LocalIllegalArgumentError` when a topic is blank;
    /// - `LocalIllegalStateError` when the consumer is closed;
    /// - the error the background task completes the subscription with.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#subscribe")]
    pub(crate) async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), Error> {
        self.acquire_and_ensure_open()?;
        Self::maybe_return_invalid_group_id_error(self.group_id.as_deref())?;
        // Java's `topics == null` check has no Rust counterpart.
        if topics.is_empty() {
            // treat subscribing to empty topic list as the same as unsubscribing
            return self.unsubscribe().await;
        }
        for topic in &topics {
            if Utils::is_blank(Some(topic)) {
                return Err(Error::local_illegal_argument(
                    "Topic collection to subscribe to cannot contain null or empty topic",
                ));
            }
        }

        // Trigger subscribe event to effectively join the group if not already part of it,
        // or just send the new subscription to the broker.
        let (handle, receiver) = CompletableEventHandle::new(i64::MAX);
        let joined = topics.join(", ");
        let event = ApplicationEvent::ShareSubscriptionChange { handle, topics: topics.into_iter().collect() };
        self.application_event_handler
            .add_and_get(event, receiver, self.time.milliseconds())
            .await
            .map_err(Self::result_error)?;

        info!("Subscribed to topics: {joined}");
        Ok(())
    }

    /// Unsubscribes from every topic and leaves the share group.
    ///
    /// # Errors
    ///
    /// `LocalIllegalStateError` when the consumer is closed, and the error the
    /// background task completes the leave with (a `TimeoutError` once
    /// `default.api.timeout.ms` has passed).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#unsubscribe")]
    pub(crate) async fn unsubscribe(&mut self) -> Result<(), Error> {
        self.acquire_and_ensure_open()?;
        let now = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now, i64::from(self.default_api_timeout_ms));
        let (handle, receiver) = CompletableEventHandle::new(deadline_ms);
        self.application_event_handler
            .add_and_get(ApplicationEvent::ShareUnsubscribe { handle }, receiver, now)
            .await
            .map_err(Self::result_error)?;

        info!("Unsubscribed all topics");
        Ok(())
    }

    /// Fetches records of the subscribed topics, waiting up to `timeout` for
    /// some to arrive.
    ///
    /// # Errors
    ///
    /// - `LocalIllegalStateError` when the consumer is closed, when it is not
    ///   subscribed (`"Consumer is not subscribed to any topics."`), or in
    ///   explicit acknowledgement mode when a record returned before is not
    ///   acknowledged (`"All records must be acknowledged in explicit
    ///   acknowledgement mode."`);
    /// - `WakeupError` when [`Self::wakeup`] was called;
    /// - the first error the background task reported;
    /// - the failure of a record batch, such as a `RecordDeserializationError`,
    ///   when no other record could be returned.
    ///
    /// # Cancellation
    ///
    /// A dropped call loses nothing: see the module doc.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#poll")]
    pub(crate) async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, Error> {
        let start_ms = self.time.milliseconds();
        let poll_deadline_ms =
            CompletableEvent::calculate_deadline_ms(start_ms, ConsumerUtils::duration_to_millis(timeout));

        self.acquire_and_ensure_open()?;
        // Java's `finally { kafkaShareConsumerMetrics.recordPollEnd(...) }`.
        let _record_poll_end = RecordPollEnd {
            kafka_share_consumer_metrics: Arc::clone(&self.kafka_share_consumer_metrics),
            time: Arc::clone(&self.time),
        };
        match self.poll_inner(start_ms, poll_deadline_ms).await {
            Ok(records) => Ok(records),
            Err(PollError::ShareFetch(share_fetch_error)) => {
                // Java's `catch (ShareFetchException e)` (ledger G9).
                let (share_fetch, cause) = share_fetch_error.into_parts();
                self.current_fetch = share_fetch;
                Err(cause)
            },
            Err(PollError::Other(error)) => Err(error),
        }
    }

    /// The `try` body of `poll`.
    async fn poll_inner(
        &mut self,
        start_ms: i64,
        poll_deadline_ms: i64,
    ) -> Result<ConsumerRecords<K, V>, PollError<K, V>> {
        // Throw any errors notified by the background thread
        self.process_background_events()?;

        // Handle any completed acknowledgements for which we already have the responses
        self.handle_completed_acknowledgements().await;

        // If using implicit acknowledgement, acknowledge the previously fetched records
        self.acknowledge_batch_if_implicit_acknowledgement();

        // If using explicit acknowledgement, make sure all in-flight records have been acknowledged
        self.ensure_in_flight_acknowledged_if_explicit_acknowledgement()?;

        self.kafka_share_consumer_metrics.record_poll_start(start_ms);

        if self.lock_subscriptions().has_no_subscription_or_user_assignment() {
            return Err(Error::local_illegal_state("Consumer is not subscribed to any topics.").into());
        }

        self.should_send_share_fetch_event = true;

        // This distinguishes the first pass of the inner do/while loop from subsequent passes for the
        // in-flight poll event logic.
        let mut first_pass = true;

        loop {
            // We must not allow wake-ups between polling for fetches and returning the records.
            // A wake-up between returned fetches and returning records would lead to never
            // returning the records in the fetches. Thus, we trigger a possible wake-up before we poll fetches.
            self.maybe_trigger_wakeup()?;

            // Make sure the network thread can tell the application is actively polling
            self.check_in_flight_poll(poll_deadline_ms, first_pass)?;
            first_pass = false;
            let collected = self.poll_for_fetches(poll_deadline_ms).await?;
            if self.has_records(collected) {
                self.handle_completed_acknowledgements().await;
                // No `.await` follows: the records leave the fetch only as they
                // are returned.
                let records = self.current_fetch.records().into_iter().collect();
                return Ok(ConsumerRecords::with_next_offsets(records, HashMap::new()));
            }

            // Throw any errors notified by the background thread
            self.process_background_events()?;
            self.metadata.maybe_return_any_error()?;

            // We will wait for retryBackoffMs
            if self.time.milliseconds() >= poll_deadline_ms {
                break;
            }
            // Rust-only (translation-checklist R4 item 6): with a zero wait the
            // loop comes round without suspending; let co-scheduled tasks run,
            // as `AsyncKafkaConsumer::poll` does.
            tokio::task::yield_now().await;
        }

        self.handle_completed_acknowledgements().await;
        Ok(ConsumerRecords::empty())
    }

    /// Java's `if (!fetch.isEmpty()) { currentFetch = fetch; ... }`: whether
    /// `collected` has records, the current fetch then holding them.
    fn has_records(&mut self, collected: Collected<K, V>) -> bool {
        match collected {
            Collected::New(mut fetch) => {
                if fetch.is_empty() {
                    return false;
                }
                self.current_fetch = fetch;
                true
            },
            Collected::Current => !self.current_fetch.is_empty(),
        }
    }

    /// Submits the `SharePollEvent` that tells the background task the
    /// application is polling, unless the one submitted before is still in
    /// flight.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#checkInFlightPoll")]
    fn check_in_flight_poll(&mut self, poll_deadline_ms: i64, first_pass: bool) -> Result<(), Error> {
        if first_pass && self.in_flight_poll.is_some() {
            self.maybe_clear_previous_in_flight_poll();
        }

        let mut newly_submitted_event = false;

        if self.in_flight_poll.is_none() {
            let now = self.time.milliseconds();
            let is_complete = Arc::new(AtomicBool::new(false));
            let event = ApplicationEvent::SharePoll {
                deadline_ms: poll_deadline_ms,
                poll_time_ms: now,
                is_complete: Arc::clone(&is_complete),
            };
            self.application_event_handler.add(event, now)?;
            self.in_flight_poll = Some(InFlightPoll { deadline_ms: poll_deadline_ms, is_complete });
            newly_submitted_event = true;
            trace!("In-flight event SharePoll(deadlineMs={poll_deadline_ms}, pollTimeMs={now}) submitted");
        }

        if self.in_flight_poll.is_some() {
            self.maybe_clear_current_in_flight_poll(newly_submitted_event);
        }
        Ok(())
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#maybeClearPreviousInFlightPoll")]
    fn maybe_clear_previous_in_flight_poll(&mut self) {
        let Some(in_flight_poll) = self.in_flight_poll.as_ref() else {
            return;
        };
        if in_flight_poll.is_complete() {
            trace!("Previous in-flight event SharePoll completed, clearing");
            self.in_flight_poll = None;
        } else if in_flight_poll.is_expired(self.time.as_ref()) {
            trace!("Previous in-flight event SharePoll expired without completing, clearing");
            self.in_flight_poll = None;
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#maybeClearCurrentInFlightPoll")]
    fn maybe_clear_current_in_flight_poll(&mut self, newly_submitted_event: bool) {
        let Some(in_flight_poll) = self.in_flight_poll.as_ref() else {
            return;
        };
        if in_flight_poll.is_complete() {
            trace!("In-flight event SharePoll completed without error, clearing");
            self.in_flight_poll = None;
        } else if !newly_submitted_event && in_flight_poll.is_expired(self.time.as_ref()) {
            trace!("In-flight event SharePoll expired without completing, clearing");
            self.in_flight_poll = None;
        }
    }

    /// Collects the records available, waiting on the fetch buffer for some
    /// when there are none.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#pollForFetches")]
    async fn poll_for_fetches(&mut self, poll_deadline_ms: i64) -> Result<Collected<K, V>, PollError<K, V>> {
        let remaining_ms = poll_deadline_ms.saturating_sub(self.time.milliseconds()).max(0);
        let poll_timeout_ms = self.max_time_to_wait_ms.load(Ordering::Acquire).min(remaining_ms);

        let acknowledgements_map = self.current_fetch.take_acknowledged_records();

        // If data is available already, return it immediately
        let pending = self.pending_acknowledgements(acknowledgements_map);
        let mut collected = self.collect(pending).await?;
        if self.is_not_empty(&mut collected) {
            return Ok(collected);
        }

        // Wait a bit - this is where we will fetch records. Java's
        // `wakeupTrigger.setShareFetchAction(fetchBuffer)` throws for a pending
        // wakeup; one arriving during the wait ends it (ledger G7) and stays
        // pending for the top of the `poll` loop.
        self.maybe_trigger_wakeup()?;
        self.await_not_empty(poll_timeout_ms).await;

        let pending = self.pending_acknowledgements(IndexMap::new());
        self.collect(pending).await
    }

    /// Whether `collected` holds records.
    fn is_not_empty(&mut self, collected: &mut Collected<K, V>) -> bool {
        match collected {
            Collected::New(fetch) => !fetch.is_empty(),
            Collected::Current => !self.current_fetch.is_empty(),
        }
    }

    /// Java's `fetchBuffer.awaitNotEmpty(pollTimer)`, ended early by a wakeup.
    async fn await_not_empty(&mut self, poll_timeout_ms: i64) {
        #[cfg(test)]
        if let Some(stub) = self.await_not_empty_stub.as_mut() {
            stub(poll_timeout_ms);
            return;
        }
        let token = self.wakeup_trigger.current_token();
        let timeout = Duration::from_millis(u64::try_from(poll_timeout_ms).unwrap_or(0));
        tokio::select! {
            biased;
            _ = token.cancelled() => {},
            _ = self.fetch_buffer.await_not_empty(timeout) => {},
        }
    }

    fn pending_acknowledgements(
        &self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    ) -> PendingAcknowledgements {
        PendingAcknowledgements {
            acknowledgements_map: Some(acknowledgements_map),
            application_event_handler: Arc::clone(&self.application_event_handler),
            time: Arc::clone(&self.time),
            default_api_timeout_ms: self.default_api_timeout_ms,
        }
    }

    /// Returns the records to deliver: newly collected ones when the current
    /// fetch is used up, otherwise the current fetch, with its renewed records
    /// back in flight. Sends `acks_to_send` piggy-backed on the poll's one
    /// `ShareFetchEvent`, or else in a `ShareAcknowledgeAsyncEvent`.
    ///
    /// When the collector fails, Java's `collect` throws before it sends
    /// `acksToSend`, so those acknowledgements are lost; Rust does the same.
    /// A failure to deliver a renewed record again (decision D5) has no Java
    /// counterpart: the acknowledgements are sent first.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#collect")]
    async fn collect(&mut self, mut acks_to_send: PendingAcknowledgements) -> Result<Collected<K, V>, PollError<K, V>> {
        if self.current_fetch.is_empty() && !self.current_fetch.has_renewals() {
            let mut fetch = match self.collect_from_buffer().await {
                Ok(Ok(fetch)) => fetch,
                Ok(Err(share_fetch_error)) => {
                    acks_to_send.discard();
                    return Err(PollError::ShareFetch(share_fetch_error));
                },
                Err(error) => {
                    acks_to_send.discard();
                    return Err(PollError::Other(error));
                },
            };
            if fetch.is_empty() {
                // Check for any acknowledgements which could have come from control records (GAP) and send them.
                let control_record_acknowledgements = fetch.take_acknowledged_records();
                if !control_record_acknowledgements.is_empty() {
                    // Asynchronously commit any waiting acknowledgements from control records.
                    self.send_share_acknowledge_async_event(control_record_acknowledgements)?;
                }

                // We only send one ShareFetchEvent per poll call.
                self.maybe_send_share_fetch_event(&mut acks_to_send)?;
            }

            let acknowledgements_map = acks_to_send.take();
            if !acknowledgements_map.is_empty() {
                // Asynchronously commit any waiting acknowledgements
                self.send_share_acknowledge_async_event(acknowledgements_map)?;
            }
            return Ok(Collected::New(fetch));
        } else if self.current_fetch.has_renewals() {
            // First, take any records which have been renewed and move them back into in-flight records.
            let renewed = self
                .current_fetch
                .take_renewed_records(self.fetch_collector.deserializers())
                .await;

            // If some records are in renewing state...
            if self.current_fetch.has_renewals() {
                // We only send one ShareFetchEvent per poll call.
                self.maybe_send_share_fetch_event(&mut acks_to_send)?;
            }
            let acknowledgements_map = acks_to_send.take();
            if !acknowledgements_map.is_empty() {
                // Asynchronously commit any waiting acknowledgements
                self.send_share_acknowledge_async_event(acknowledgements_map)?;
            }
            renewed?;
            return Ok(Collected::Current);
        }

        let acknowledgements_map = acks_to_send.take();
        if !acknowledgements_map.is_empty() {
            // Asynchronously commit any waiting acknowledgements
            self.send_share_acknowledge_async_event(acknowledgements_map)?;
        }
        Ok(Collected::Current)
    }

    /// Sends the poll's one `ShareFetchEvent`, with `acks_to_send`, and wakes
    /// the background task, if it has not been sent yet.
    fn maybe_send_share_fetch_event(&mut self, acks_to_send: &mut PendingAcknowledgements) -> Result<(), Error> {
        if self.should_send_share_fetch_event {
            let event = ApplicationEvent::ShareFetch { acknowledgements_map: acks_to_send.take() };
            self.application_event_handler.add(event, self.time.milliseconds())?;
            self.should_send_share_fetch_event = false;
            // Notify the network thread to wake up and start the next round of fetching
            self.application_event_handler.wake_background_task();
        }
        Ok(())
    }

    /// `fetchCollector.collect(fetchBuffer)`, into the state a dropped call
    /// left (see the module doc).
    async fn collect_from_buffer(&mut self) -> ShareFetchCollectResult<K, V> {
        #[cfg(test)]
        if let Some(stub) = self.collect_stub.as_mut() {
            return stub();
        }
        self.fetch_collector
            .collect_with_state(&self.fetch_buffer, &mut self.collect_state)
            .await
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#sendShareAcknowledgeAsyncEvent")]
    fn send_share_acknowledge_async_event(
        &self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
    ) -> Result<(), Error> {
        let now = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now, i64::from(self.default_api_timeout_ms));
        self.application_event_handler.add(
            ApplicationEvent::ShareAcknowledgeAsync { acknowledgements_map, deadline_ms },
            now,
        )?;

        // Notify the network thread to wake up and start the next round of fetching
        self.application_event_handler.wake_background_task();
        Ok(())
    }

    /// Acknowledges `record` as `ACCEPT` (explicit acknowledgement mode).
    ///
    /// # Errors
    ///
    /// As [`Self::acknowledge_with_record_type`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acknowledge")]
    pub(crate) fn acknowledge_with_record(&mut self, record: &ConsumerRecord<K, V>) -> Result<(), Error> {
        self.acknowledge_with_record_type(record, AcknowledgeType::Accept)
    }

    /// Acknowledges `record` with `type` (explicit acknowledgement mode).
    ///
    /// # Errors
    ///
    /// `LocalIllegalStateError` with `"Implicit acknowledgement of delivery is
    /// being used."` in implicit mode, `"The record cannot be acknowledged."`
    /// when the record is not in flight, or the closed message (Milestone 18
    /// decision D7: a `Result`, where Java throws).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acknowledge")]
    pub(crate) fn acknowledge_with_record_type(
        &mut self,
        record: &ConsumerRecord<K, V>,
        r#type: AcknowledgeType,
    ) -> Result<(), Error> {
        self.acquire_and_ensure_open()?;
        self.ensure_explicit_acknowledgement()?;
        self.current_fetch.acknowledge_with_record(record, r#type)
    }

    /// Acknowledges, with the options' type, the record at the options' offset
    /// of their topic partition whose delivery failed (explicit acknowledgement
    /// mode). Java's `acknowledge(String, int, long, AcknowledgeType)`: its four
    /// parameters are the fields of [`AcknowledgeOptions`], named as the public
    /// `ShareConsumer::acknowledge_with_options` (CLAUDE.md §2).
    ///
    /// # Errors
    ///
    /// `LocalIllegalStateError` with `"Implicit acknowledgement of delivery is
    /// being used."` in implicit mode, `"The record cannot be acknowledged."`
    /// when no batch failed to deliver that record, or the closed message
    /// (decision D7).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acknowledge")]
    pub(crate) fn acknowledge_with_options(&mut self, options: AcknowledgeOptions) -> Result<(), Error> {
        self.acquire_and_ensure_open()?;
        self.ensure_explicit_acknowledgement()?;
        let AcknowledgeOptions { topic, partition, offset, acknowledge_type } = options;
        self.current_fetch
            .acknowledge_with_topic_partition_offset(&topic, partition, offset, acknowledge_type)
    }

    /// [`Self::commit_sync_with_timeout`] with `default.api.timeout.ms`.
    ///
    /// # Errors
    ///
    /// As [`Self::commit_sync_with_timeout`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#commitSync")]
    pub(crate) async fn commit_sync(&mut self) -> Result<HashMap<TopicIdPartition, Option<Error>>, Error> {
        let timeout = Duration::from_millis(u64::try_from(self.default_api_timeout_ms).unwrap_or(0));
        self.commit_sync_with_timeout(timeout).await
    }

    /// Sends the acknowledgements made so far (all delivered records as
    /// `ACCEPT` in implicit mode) and waits for their result, by partition.
    ///
    /// # Errors
    ///
    /// `LocalIllegalStateError` when the consumer is closed, `WakeupError`
    /// when [`Self::wakeup`] was called, and the error the background task
    /// completes the commit with.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#commitSync")]
    pub(crate) async fn commit_sync_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<TopicIdPartition, Option<Error>>, Error> {
        self.acquire_and_ensure_open()?;
        // Handle any completed acknowledgements for which we already have the responses
        self.handle_completed_acknowledgements().await;

        // If using implicit acknowledgement, acknowledge the previously fetched records
        self.acknowledge_batch_if_implicit_acknowledgement();

        let now = self.time.milliseconds();
        let deadline_ms = CompletableEvent::calculate_deadline_ms(now, ConsumerUtils::duration_to_millis(timeout));
        let acknowledgements_map = self.acknowledgements_to_send();
        if acknowledgements_map.is_empty() {
            return Ok(HashMap::new());
        }
        let (handle, receiver) = CompletableEventHandle::new(deadline_ms);
        self.application_event_handler
            .add(ApplicationEvent::ShareAcknowledgeSync { handle, acknowledgements_map }, now)?;
        // Java's `wakeupTrigger.setActiveTask(commitFuture)` and the unbounded
        // `ConsumerUtils.getResult(commitFuture)`: the background task
        // completes the event by its deadline.
        let completed_acknowledgements = await_event_completion(
            &self.wakeup_trigger,
            self.time.as_ref(),
            receiver,
            i64::MAX,
            "The share acknowledge did not complete",
            true,
            true,
        )
        .await
        .map_err(Self::result_error)?;
        let result = completed_acknowledgements
            .iter()
            .map(|(tip, acks)| (tip.clone(), acks.acknowledge_error().cloned()))
            .collect();

        // Handle any acknowledgements which completed while we were waiting
        self.handle_completed_acknowledgements().await;

        Ok(result)
    }

    /// Sends the acknowledgements made so far (all delivered records as
    /// `ACCEPT` in implicit mode) without waiting for their result.
    ///
    /// Async because it runs the acknowledgement commit callback for the
    /// acknowledgements completed before (`share-consumer.md` §1).
    ///
    /// # Errors
    ///
    /// `LocalIllegalStateError` when the consumer is closed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#commitAsync")]
    pub(crate) async fn commit_async(&mut self) -> Result<(), Error> {
        self.acquire_and_ensure_open()?;
        // Handle any completed acknowledgements for which we already have the responses
        self.handle_completed_acknowledgements().await;

        // If using implicit acknowledgement, acknowledge the previously fetched records
        self.acknowledge_batch_if_implicit_acknowledgement();

        let acknowledgements_map = self.acknowledgements_to_send();
        if !acknowledgements_map.is_empty() {
            let now = self.time.milliseconds();
            let deadline_ms = CompletableEvent::calculate_deadline_ms(now, i64::from(self.default_api_timeout_ms));
            self.application_event_handler.add(
                ApplicationEvent::ShareAcknowledgeAsync { acknowledgements_map, deadline_ms },
                now,
            )?;
        }
        Ok(())
    }

    /// Registers `callback` to be called with the result of each completed
    /// acknowledgement, or removes it for `None`.
    ///
    /// # Panics
    ///
    /// With `"This consumer has already been closed."` when the consumer is
    /// closed (Java's `IllegalStateException`; CLAUDE.md §12.5).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#setAcknowledgementCommitCallback")]
    pub(crate) fn set_acknowledgement_commit_callback(
        &mut self,
        callback: Option<Arc<dyn AcknowledgementCommitCallback>>,
    ) {
        // Java's `acquireAndEnsureOpen` also throws a permanent bootstrap
        // failure (KIP-909); with no error channel, this keeps working after
        // one, as the consumer's sync getters do
        // (`AsyncKafkaConsumer::subscription`, `assignment`, `paused`).
        self.panic_if_closed();
        match callback {
            Some(callback) => {
                if self.acknowledgement_commit_callback_handler.is_none() {
                    self.add_callback_registration_event(true);
                }
                self.acknowledgement_commit_callback_handler =
                    Some(AcknowledgementCommitCallbackHandler::new(callback));
            },
            None => {
                if self.acknowledgement_commit_callback_handler.is_some() {
                    self.add_callback_registration_event(false);
                }
                self.completed_acknowledgements.clear();
                self.acknowledgement_commit_callback_handler = None;
            },
        }
    }

    /// `applicationEventHandler.add(new ShareAcknowledgementCommitCallbackRegistrationEvent(..))`.
    /// The handler fails only once the background task has stopped, which
    /// happens only after `close`, which this method's caller checked.
    fn add_callback_registration_event(&self, is_callback_registered: bool) {
        let event = ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration { is_callback_registered };
        if let Err(e) = self.application_event_handler.add(event, self.time.milliseconds()) {
            error!("Failed to send the acknowledgement commit callback registration: {e}");
        }
    }

    /// Returns the client instance id of the consumer (KIP-714).
    ///
    /// # Errors
    ///
    /// Always `LocalIllegalStateError` with ``"Telemetry is not enabled. Set
    /// config `enable.metrics.push` to `true`."``: Java's answer when the
    /// consumer has no telemetry reporter, which a Rust consumer never has
    /// (ledger G16; decision D2 keeps it off the public trait).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#clientInstanceId")]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "KIP-714: not on the ShareConsumer trait (Milestone 18 decision D2)"
        )
    )]
    pub(crate) fn client_instance_id(&self, _timeout: Duration) -> Result<Uuid, Error> {
        // Java names `ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG`, which the
        // Rust configuration does not define (KIP-714 is not translated).
        Err(Error::local_illegal_state(
            "Telemetry is not enabled. Set config `enable.metrics.push` to `true`.",
        ))
    }

    /// Returns the acquisition lock timeout of the last records fetched, if
    /// the broker sent one.
    ///
    /// # Panics
    ///
    /// With `"This consumer has already been closed."` when the consumer is
    /// closed (Java's `IllegalStateException`; CLAUDE.md §12.5).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acquisitionLockTimeoutMs")]
    pub(crate) fn acquisition_lock_timeout_ms(&mut self) -> Option<i32> {
        // Java's `acquireAndEnsureOpen` also throws a permanent bootstrap
        // failure (KIP-909); with no error channel, this keeps working after
        // one, as the consumer's sync getters do
        // (`AsyncKafkaConsumer::subscription`, `assignment`, `paused`).
        self.panic_if_closed();
        self.current_fetch.acquisition_lock_timeout_ms()
    }

    /// Returns the consumer's metrics.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#metrics")]
    pub(crate) fn metrics(&self) -> MetricsView {
        self.metrics.metrics()
    }

    /// [`Self::close_with_timeout`] with the default close timeout (30 s).
    ///
    /// # Errors
    ///
    /// As [`Self::close_with_timeout`].
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#close")]
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        let timeout = Duration::from_millis(u64::try_from(ConsumerUtils::DEFAULT_CLOSE_TIMEOUT_MS).unwrap_or(0));
        self.close_with_timeout(timeout).await
    }

    /// Sends the acknowledgements made so far, leaves the group and stops the
    /// background task, waiting up to `timeout` (capped at
    /// `request.timeout.ms`). Records acquired and not acknowledged are
    /// released by the broker. A second call does nothing.
    ///
    /// Java's negative-timeout check has no counterpart: a `Duration` cannot
    /// be negative.
    ///
    /// # Cancellation
    ///
    /// Java sets `closed` in a `finally`, after every close step has run,
    /// the network thread's join included. A dropped call has not run them
    /// all, so it leaves the consumer open, as `AsyncKafkaConsumer::close`
    /// does, and a later `close` resumes it: it awaits the acknowledge-on-close
    /// the dropped call sent instead of sending another, sends the leave again,
    /// and only joins the background task when the dropped call had already
    /// told it to stop. The later `close` stops the task and returns the first
    /// failure of the close, the dropped call's steps included. It waits for
    /// the dropped call's acknowledge-on-close only up to its own timeout.
    ///
    /// # Errors
    ///
    /// A `KafkaError` with `"Failed to close Kafka share consumer"` whose
    /// source is the first failure of a close step.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#close")]
    pub(crate) async fn close_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        if self.is_closed() {
            return Ok(());
        }
        // need to close before setting the flag since the close function
        // itself may trigger code that needs the consumer to be open still
        let result = self.close_internal(timeout, false).await;
        self.closed.store(true, Ordering::Release);
        result
    }

    /// Java's `close(Duration, boolean swallowException)`.
    async fn close_internal(&mut self, timeout: Duration, swallow_error: bool) -> Result<(), Error> {
        trace!("Closing the Kafka consumer");
        // The first error, `self.close_first_error`, outlives a dropped call.

        // We are already closing with a timeout, don't allow wake-ups from here on.
        self.wakeup_trigger.disable();

        let close_deadline_ms = self.create_timer_for_close_requests(timeout);

        // A `close` retried after a dropped one resumes where that one stopped
        // (`CloseProgress`).
        if !matches!(self.close_progress, CloseProgress::Signalled | CloseProgress::Joined) {
            // Prepare shutting down the network thread
            if let Err(e) = self.send_acknowledgements_and_leave_group(close_deadline_ms).await {
                Self::swallow(
                    "Failed to release assignment before closing consumer",
                    e,
                    &mut self.close_first_error,
                );
            }
            if let Err(e) = self.stop_find_coordinator_on_close() {
                Self::swallow("Failed to stop finding coordinator", e, &mut self.close_first_error);
            }
            // Java swallows a failure of the callback here; the handler catches it.
            self.handle_completed_acknowledgements().await;
            if let Err(e) = self.process_background_events_on_close() {
                Self::swallow("Failed processing background events", e, &mut self.close_first_error);
            }
            // Java's `applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs()))`.
            let remaining_ms = close_deadline_ms.saturating_sub(self.time.milliseconds()).max(0);
            self.network_thread_close
                .signal_close(Duration::from_millis(u64::try_from(remaining_ms).unwrap_or(0)));
            self.network_thread_close.wakeup();
            self.close_progress = CloseProgress::Signalled;
        }
        if matches!(self.close_progress, CloseProgress::Signalled) {
            if let Err(e) = self.network_thread_close.await_join().await {
                Self::close_quietly_failed("Failed shutting down network thread", e, &mut self.close_first_error);
            }
            self.close_progress = CloseProgress::Joined;
        }

        // Java's `backgroundEventReaper.reap(backgroundEventQueue)`: the queue's
        // completable events are cancelled and the queue cleared (the share
        // consumer's background events carry no completion).
        while self.background_event_rx.try_recv().is_ok() {
            self.background_event_queue_size.fetch_sub(1, Ordering::SeqCst);
        }
        self.background_event_reaper.reap_on_close(&mut Vec::new());

        self.kafka_share_consumer_metrics.close();
        self.async_consumer_metrics.close();
        self.share_fetch_metrics_manager.close();
        self.metrics.close();
        if let Err(e) = self.fetch_collector.close_deserializers().await {
            Self::close_quietly_failed("consumer deserializers", e, &mut self.close_first_error);
        }

        debug!("Kafka share consumer has been closed");
        // Java's `InterruptException` pass-through has no counterpart: no
        // close step can record one.
        match self.close_first_error.take() {
            Some(error) if !swallow_error => {
                Err(Error::kafka_message_source("Failed to close Kafka share consumer", error))
            },
            _ => Ok(()),
        }
    }

    /// Java's `Utils.swallow`: logs `msg` and the error at `ERROR`, and keeps
    /// the first.
    fn swallow(msg: &str, error: Error, first_error: &mut Option<Error>) {
        error!("{msg}: {error}");
        first_error.get_or_insert(error);
    }

    /// Java's `Utils.closeQuietly(.., name, firstException)`.
    fn close_quietly_failed(name: &str, error: Error, first_error: &mut Option<Error>) {
        error!("Failed to close {name} with type {}: {error}", std::any::type_name::<Self>());
        first_error.get_or_insert(error);
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#stopFindCoordinatorOnClose")]
    fn stop_find_coordinator_on_close(&self) -> Result<(), Error> {
        debug!("Stop finding coordinator during consumer close");
        self.application_event_handler
            .add(ApplicationEvent::StopFindCoordinatorOnClose, self.time.milliseconds())
    }

    /// The deadline of the close requests: `timeout`, capped at
    /// `request.timeout.ms`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#createTimerForCloseRequests")]
    fn create_timer_for_close_requests(&self, timeout: Duration) -> i64 {
        let timeout_ms = ConsumerUtils::duration_to_millis(timeout).min(i64::from(self.request_timeout_ms));
        CompletableEvent::calculate_deadline_ms(self.time.milliseconds(), timeout_ms)
    }

    /// Prior to closing the network thread, we need to make sure the following operations happen in the right sequence:
    /// 1. commit pending acknowledgements and close any share sessions
    /// 2. leave the group
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#sendAcknowledgementsAndLeaveGroup")]
    async fn send_acknowledgements_and_leave_group(&mut self, deadline_ms: i64) -> Result<(), Error> {
        let timeout_ms = deadline_ms.saturating_sub(self.time.milliseconds()).max(0);
        // A `close` resuming a dropped one waits for the event that one sent
        // only up to its own deadline.
        let resumed = !matches!(self.close_progress, CloseProgress::NotStarted);
        // Java's `applicationEventHandler.addAndGet(new ShareAcknowledgeOnCloseEvent(..))`,
        // made once: a `close` retried after a dropped one awaits the event the
        // dropped one sent (`CloseProgress`).
        if matches!(self.close_progress, CloseProgress::NotStarted) {
            let acknowledgements_map = self.acknowledgements_to_send();
            let (handle, receiver) = CompletableEventHandle::new(deadline_ms);
            let event = ApplicationEvent::ShareAcknowledgeOnClose { handle, acknowledgements_map };
            match self.application_event_handler.add(event, self.time.milliseconds()) {
                Ok(()) => self.close_progress = CloseProgress::AcknowledgingOnClose(receiver),
                Err(e) => {
                    self.close_progress = CloseProgress::AcknowledgedOnClose;
                    Self::complete_quietly(
                        Err(e),
                        &format!("Failed to send pending acknowledgements with a timeout(ms)={timeout_ms}"),
                        &mut self.close_first_error,
                    );
                },
            }
        }
        if let CloseProgress::AcknowledgingOnClose(receiver) = &mut self.close_progress {
            let completion = if resumed {
                tokio::time::timeout(Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0)), receiver)
                    .await
                    .ok()
            } else {
                // Java's `addAndGet` waits until the background task completes
                // the event, by its deadline at the latest.
                Some(receiver.await)
            };
            let acknowledged = match completion {
                Some(Ok(result)) => result.map_err(Self::result_error),
                Some(Err(_)) => Err(Error::local_illegal_state(
                    "Background task dropped the completion sender for ShareAcknowledgeOnClose without completing it",
                )),
                // The resumed wait reached its deadline: Java's quiet timeout. The
                // background task still completes or reaps the event, and the
                // acknowledgements report through their events.
                None => Err(Error::timeout("The acknowledge-on-close of a cancelled close did not complete")),
            };
            self.close_progress = CloseProgress::AcknowledgedOnClose;
            Self::complete_quietly(
                acknowledged,
                &format!("Failed to send pending acknowledgements with a timeout(ms)={timeout_ms}"),
                &mut self.close_first_error,
            );
        }

        let (handle, receiver) = CompletableEventHandle::new(deadline_ms);
        self.application_event_handler
            .add(ApplicationEvent::ShareUnsubscribe { handle }, self.time.milliseconds())?;
        // If users have fatal error, they will get some exceptions in the background queue.
        // When running unsubscribe, these exceptions should be ignored, or users can't unsubscribe successfully.
        // BootstrapResolutionException is also ignored here: a permanent DNS failure prevents the
        // unsubscribe from completing normally, but it is not actionable during close.
        match self
            .process_background_events_with_future_deadline_ms_ignore_error_event_error(receiver, deadline_ms, |e| {
                matches!(
                    e,
                    Error::GroupAuthorization(_)
                        | Error::TopicAuthorization(_)
                        | Error::InvalidTopic(_)
                        | Error::BootstrapResolution(_)
                )
            })
            .await
        {
            Ok(()) => info!("Completed releasing assignment and leaving group to close consumer."),
            Err(Error::Timeout(_)) => warn!(
                "Consumer triggered an unsubscribe event to leave the group but couldn't complete it within {timeout_ms} ms. It will proceed to close."
            ),
            Err(e) => return Err(e),
        }
        Ok(())
    }

    /// Wakes up a `poll` or `commit_sync` waiting, which then returns a
    /// `WakeupError`; with no such call in progress, the next one returns it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#wakeup")]
    pub(crate) fn wakeup(&self) {
        self.wakeup_trigger.wakeup();
    }

    /// Returns the handle that wakes this consumer up from another task: it
    /// fires the same wakeup trigger as [`Self::wakeup`] (Milestone 18
    /// decision D6).
    pub(crate) fn handle(&self) -> ShareConsumerHandle {
        ShareConsumerHandle::with_wakeup_trigger(self.wakeup_trigger.clone())
    }

    /// Java's `acquireAndEnsureOpen()`: the closed check, then
    /// `metadata.maybeThrowBootstrapFatalException()`. `acquire()` is the
    /// compile-time `&mut self` (see the module doc).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acquireAndEnsureOpen")]
    fn acquire_and_ensure_open(&self) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::local_illegal_state(CONSUMER_CLOSED_MSG));
        }
        self.metadata.maybe_return_bootstrap_fatal_error()
    }

    /// The closed half of `acquireAndEnsureOpen()` for the methods with no
    /// error channel.
    fn panic_if_closed(&self) {
        if self.is_closed() {
            panic!("{CONSUMER_CLOSED_MSG}");
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// `[ShareConsumer clientId=.., groupId=..] `.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#createLogContext")]
    pub(crate) fn create_log_context(client_id: &str, group_id: &str) -> LogContext {
        LogContext::new(format!("[ShareConsumer clientId={client_id}, groupId={group_id}] "))
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#maybeThrowInvalidGroupIdException")]
    fn maybe_return_invalid_group_id_error(group_id: Option<&str>) -> Result<(), Error> {
        if group_id.is_none_or(str::is_empty) {
            return Err(Error::invalid_group_id(format!(
                "You must provide a valid {} in the consumer configuration.",
                ConsumerConfig::GROUP_ID_CONFIG
            )));
        }
        Ok(())
    }

    /// Handles any completed acknowledgements. If there is an acknowledgement
    /// commit callback registered, call it. Otherwise, discard the information
    /// about completed acknowledgements because the application is not
    /// interested.
    ///
    /// Each `(partition, acknowledgements)` entry leaves the queue before its
    /// callback runs, so a dropped call neither calls an entry twice nor loses
    /// the entries after it.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#handleCompletedAcknowledgements")]
    async fn handle_completed_acknowledgements(&mut self) {
        self.process_acknowledgement_events();

        if !self.completed_acknowledgements.is_empty() {
            if let Some(handler) = self.acknowledgement_commit_callback_handler.as_mut() {
                while let Some(acknowledgements_map) = self.completed_acknowledgements.front_mut() {
                    let Some(partition) = acknowledgements_map.keys().next().cloned() else {
                        self.completed_acknowledgements.pop_front();
                        continue;
                    };
                    let Some(acknowledgements) = acknowledgements_map.remove(&partition) else {
                        continue;
                    };
                    if acknowledgements_map.is_empty() {
                        self.completed_acknowledgements.pop_front();
                    }
                    handler.on_complete(&[HashMap::from([(partition, acknowledgements)])]).await;
                }
            }
            self.completed_acknowledgements.clear();
        }
    }

    /// If the acknowledgement mode is IMPLICIT, acknowledges all records in
    /// the current batch: those handed out to the application (see the module
    /// doc).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acknowledgeBatchIfImplicitAcknowledgement"
    )]
    fn acknowledge_batch_if_implicit_acknowledgement(&mut self) {
        // If IMPLICIT, acknowledge all records
        if self.acknowledgement_mode == ShareAcknowledgementMode::IMPLICIT {
            self.current_fetch.acknowledge_all_handed_out(AcknowledgeType::Accept);
        }
    }

    /// If the acknowledgement mode is EXPLICIT, ensure that all in-flight
    /// records handed out to the application have been acknowledged.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#ensureInFlightAcknowledgedIfExplicitAcknowledgement"
    )]
    fn ensure_in_flight_acknowledged_if_explicit_acknowledgement(&self) -> Result<(), Error> {
        if self.acknowledgement_mode == ShareAcknowledgementMode::EXPLICIT
            && !self.current_fetch.check_all_handed_out_are_acknowledged()
        {
            // We cannot leave unacknowledged records in EXPLICIT acknowledgement mode, so we throw an exception to the application.
            return Err(Error::local_illegal_state(
                "All records must be acknowledged in explicit acknowledgement mode.",
            ));
        }
        Ok(())
    }

    /// Returns any ready acknowledgements to be sent to the cluster.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#acknowledgementsToSend")]
    fn acknowledgements_to_send(&mut self) -> IndexMap<TopicIdPartition, NodeAcknowledgements> {
        self.current_fetch.take_acknowledged_records()
    }

    /// Called to verify if the acknowledgement mode is EXPLICIT, else returns
    /// an error.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#ensureExplicitAcknowledgement")]
    fn ensure_explicit_acknowledgement(&self) -> Result<(), Error> {
        if self.acknowledgement_mode == ShareAcknowledgementMode::IMPLICIT {
            return Err(Error::local_illegal_state(
                "Implicit acknowledgement of delivery is being used.",
            ));
        }
        Ok(())
    }

    /// Initializes the acknowledgement mode based on the configuration.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#initializeAcknowledgementMode")]
    fn initialize_acknowledgement_mode(config: &ConsumerConfig) -> Result<ShareAcknowledgementMode, Error> {
        let s = config.get_string(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG)?;
        ShareAcknowledgementMode::from_string(s)
    }

    /// Process acknowledgement events, if any, that were produced by the
    /// background task: Java's `ShareAcknowledgementEventProcessor#process`
    /// for each. A failure to renew is logged, as Java logs what the processor
    /// throws.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#processAcknowledgementEvents")]
    fn process_acknowledgement_events(&mut self) {
        for event in self.acknowledgement_event_handler.drain_events() {
            if event.check_for_renew_acknowledgements()
                && let Err(e) = self
                    .current_fetch
                    .renew(event.acknowledgements_map(), event.acquisition_lock_timeout_ms())
            {
                warn!("An error occurred when processing the acknowledgement event: {e}");
            }
            if self.acknowledgement_commit_callback_handler.is_some() {
                self.completed_acknowledgements.push_back(event.into_acknowledgements_map());
            }
        }
    }

    /// Process background events on close. Except some expected exceptions
    /// which might occur during close, errors encountered are returned.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#processBackgroundEventsOnClose")]
    fn process_background_events_on_close(&mut self) -> Result<(), Error> {
        match self.process_background_events() {
            Err(e)
                if !matches!(
                    e,
                    Error::GroupAuthorization(_) | Error::TopicAuthorization(_) | Error::InvalidTopic(_)
                ) =>
            {
                Err(e)
            },
            _ => Ok(()),
        }
    }

    /// Process background events, if any, that were produced by the
    /// background task. It is possible that an error could occur when
    /// processing the events. In such cases, the processor will take a
    /// reference to the first error, continue to process the remaining events,
    /// and then return the first error that occurred.
    ///
    /// Returns whether any event was processed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#processBackgroundEvents")]
    fn process_background_events(&mut self) -> Result<bool, Error> {
        let mut first_error: Option<Error> = None;
        let mut had_events = false;
        let start_ms = self.time.milliseconds();
        loop {
            let envelope = match self.background_event_rx.try_recv() {
                Ok(envelope) => envelope,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    if first_error.is_none() && !self.is_closed() {
                        first_error =
                            Some(Error::local_illegal_state("Consumer background task is no longer running."));
                    }
                    break;
                },
            };
            had_events = true;
            self.background_event_queue_size.fetch_sub(1, Ordering::SeqCst);
            self.async_consumer_metrics
                .record_background_event_queue_time(self.time.milliseconds() - envelope.enqueued_ms);
            // Java's `BackgroundEventProcessor#process`. No share background
            // event is a `CompletableEvent`, so none goes to the reaper.
            let error = match envelope.event {
                BackgroundEvent::Error { error } => error,
                other => Error::local_illegal_argument(format!(
                    "Background event type {} was not expected",
                    other.type_name()
                )),
            };
            let error = ConsumerUtils::maybe_wrap_as_kafka_error(error);
            if first_error.is_none() {
                first_error = Some(error);
            } else {
                warn!("An error occurred when processing the background event: {error}");
            }
        }
        // Java's drain records the size 0; the depth counter is what is left
        // (see `AsyncKafkaConsumer::process_background_events`).
        self.async_consumer_metrics.record_background_event_queue_size(
            i32::try_from(self.background_event_queue_size.load(Ordering::SeqCst).max(0)).unwrap_or(i32::MAX),
        );
        if had_events {
            self.async_consumer_metrics
                .record_background_event_queue_processing_time(self.time.milliseconds() - start_ms);
        }

        self.background_event_reaper.reap(self.time.milliseconds());

        match first_error {
            Some(error) => Err(error),
            None => Ok(had_events),
        }
    }

    /// Waits up to `deadline_ms` for the event of `future` to complete, processing
    /// background events meanwhile, as the background task may need the
    /// application task to complete it. Each iteration processes background
    /// events, then returns the result if the event is done, or else, when
    /// there were no events, waits *briefly* (100 ms) for it.
    ///
    /// `ignore_error_event_error` selects the background errors not returned.
    ///
    /// # Errors
    ///
    /// A background error the predicate does not ignore, the error the event
    /// completed with, or a `TimeoutError` with `"Operation timed out before
    /// completion"` when `deadline_ms` passes first.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#processBackgroundEvents")]
    async fn process_background_events_with_future_deadline_ms_ignore_error_event_error<T: Send + 'static>(
        &mut self,
        mut future: oneshot::Receiver<Result<T, Error>>,
        deadline_ms: i64,
        ignore_error_event_error: impl Fn(&Error) -> bool,
    ) -> Result<T, Error> {
        trace!(
            "Will wait up to {} ms for an event to complete",
            deadline_ms.saturating_sub(self.time.milliseconds())
        );
        // Java catches the `TimeoutException` of an event completed with one,
        // as it catches its own wait's, and goes on processing background
        // events until the timer expires: without waiting, as its future is
        // done; here in the 100 ms waits, the receiver being consumed.
        let mut timed_out = false;
        loop {
            let had_events = match self.process_background_events() {
                Ok(had_events) => had_events,
                Err(e) if ignore_error_event_error(&e) => false,
                Err(e) => return Err(e),
            };

            if !timed_out {
                match future.try_recv() {
                    Ok(Ok(result)) => {
                        trace!("Event completed successfully");
                        return Ok(result);
                    },
                    Ok(Err(e)) => match Self::result_error(e) {
                        Error::Timeout(_) => timed_out = true,
                        e => return Err(e),
                    },
                    Err(oneshot::error::TryRecvError::Closed) => {
                        return Err(Error::local_illegal_state(
                            "Background task dropped the completion sender without completing it",
                        ));
                    },
                    Err(oneshot::error::TryRecvError::Empty) => {},
                }
            }
            if !had_events || timed_out {
                // If the above processing yielded no events, then let's sit tight for a bit to allow the
                // background thread to either finish the task, or populate the background event
                // queue with things to process in our next loop.
                let remaining_ms = deadline_ms.saturating_sub(self.time.milliseconds()).max(0);
                let wait = Duration::from_millis(u64::try_from(remaining_ms.min(100)).unwrap_or(0));
                if timed_out {
                    tokio::time::sleep(wait).await;
                } else {
                    match tokio::time::timeout(wait, &mut future).await {
                        Ok(Ok(Ok(result))) => {
                            trace!("Event completed successfully");
                            return Ok(result);
                        },
                        Ok(Ok(Err(e))) => match Self::result_error(e) {
                            Error::Timeout(_) => timed_out = true,
                            e => return Err(e),
                        },
                        Ok(Err(_)) => {
                            return Err(Error::local_illegal_state(
                                "Background task dropped the completion sender without completing it",
                            ));
                        },
                        // Ignore this as we will retry the event until the timeout expires.
                        Err(_elapsed) => {},
                    }
                }
            }
            if self.time.milliseconds() >= deadline_ms {
                break;
            }
        }

        trace!("Event did not complete within timeout");
        Err(Error::timeout("Operation timed out before completion"))
    }

    /// Runs `function` (already started by the caller) to completion: a
    /// `TimeoutError` is logged at debug, any other error is kept as the first
    /// error.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#completeQuietly")]
    fn complete_quietly(outcome: Result<(), Error>, msg: &str, first_error: &mut Option<Error>) {
        match outcome {
            Ok(()) => {},
            Err(Error::Timeout(_)) => debug!("Timeout expired before the {msg} operation could complete."),
            Err(e) => {
                first_error.get_or_insert(e);
            },
        }
    }

    /// Java's `ConsumerUtils.getResult` mapping of a failed event: an
    /// `IllegalStateException` as it is, anything else as a `KafkaException`
    /// (`ConsumerUtils#maybeWrapAsKafkaException`).
    fn result_error(error: Error) -> Error {
        if matches!(error, Error::LocalIllegalState(_)) {
            error
        } else {
            ConsumerUtils::maybe_wrap_as_kafka_error(error)
        }
    }

    /// The client id.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#clientId")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The metrics registry.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#metricsRegistry")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn metrics_registry(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// The async-consumer metrics (visible for testing in Java).
    #[cfg(test)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#asyncConsumerMetrics")]
    pub(crate) fn async_consumer_metrics(&self) -> &AsyncConsumerMetrics {
        &self.async_consumer_metrics
    }

    /// The share-consumer poll metrics.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumerImpl#kafkaShareConsumerMetrics")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java uses it only in its tests"))]
    pub(crate) fn kafka_share_consumer_metrics(&self) -> &KafkaShareConsumerMetrics {
        &self.kafka_share_consumer_metrics
    }

    /// Returns the subscription state, locked: a guard is never held across an
    /// `.await` (consumer-threading.md §16).
    fn lock_subscriptions(&self) -> std::sync::MutexGuard<'_, SubscriptionState> {
        self.subscriptions.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Java's `wakeupTrigger.maybeTriggerWakeup()`, rotating the token after a
    /// wakeup is raised (consumer-threading.md §11).
    fn maybe_trigger_wakeup(&self) -> Result<(), Error> {
        let result = self.wakeup_trigger.maybe_trigger_wakeup();
        if result.is_err() {
            self.wakeup_trigger.rotate();
        }
        result
    }
}

impl<K, V> std::fmt::Debug for ShareConsumerImpl<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// The identifiers Java's log context prints.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumerImpl")
            .field("client_id", &self.client_id)
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ShareConsumerImplTest` (36 of its 38 tests, and the one
    //! parameterised over three errors), plus Rust-only tests of the cancellation contract
    //! (module doc) and of the production constructors (translation-checklist
    //! R3).
    //!
    //! Java builds the consumer around Mockito mocks of the
    //! `ApplicationEventHandler`, the `ShareFetchCollector`, the
    //! `ShareFetchBuffer` and the metadata. Here:
    //!
    //! - the application event handler is a real one whose channel a
    //!   [`Responder`] reads; it records every event (Mockito's `verify`) and
    //!   completes the completable ones as Java's unstubbed mock returns from
    //!   `addAndGet` at once, applying the side effects the Java test stubs
    //!   (`completeShareSubscriptionChangeApplicationEventSuccessfully`, ...).
    //!   The mock's `add` versus `addAndGet` is not observable here; the
    //!   events' order is. An unstubbed `ShareUnsubscribeEvent` sent by `close`
    //!   completes at once too, where Java's mock never completes it and
    //!   `close` gives up after its timer; only the time `close` takes differs;
    //! - the collector is the real one, replaced by a stub when the Java test
    //!   stubs `fetchCollector.collect` (`collect_stub`); `doReturn(fetch)`
    //!   returns the same object on every call, a stub builds a new fetch;
    //! - the fetch buffer is a real, empty one, whose wait the zero
    //!   `maximumTimeToWait` of Java's mock makes immediate, or a stub
    //!   (`await_not_empty_stub`) where the Java test stubs `awaitNotEmpty`;
    //! - the metadata is real.
    //!
    //! Java's tests built through the configuration constructor with mocked
    //! factories (`newConsumer()`, `newConsumer(config)`) use the same fixture
    //! where they assert behaviour, and `ShareConsumerImpl::new` where they
    //! assert construction. Java's `@AfterEach` closes the consumer with a zero
    //! timeout; the fixture's consumer is dropped instead, which stops nothing
    //! the tests observe.
    //!
    //! Not translated: `testSubscribeToNullTopicCollection` and
    //! `testSubscriptionOnNullTopic` pass a `null` collection or topic, which a
    //! `Vec<String>` cannot hold. No test of the class asserts Java's runtime
    //! "methods are not accessible from user-defined acknowledgement commit
    //! callback" check (`&mut self`, decision D6).

    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use futures_util::FutureExt;

    use super::*;
    use crate::common::errors::InvalidRecordStateError;
    use crate::common::protocol::Errors;
    use crate::common::serialization::{Deserializer, StringDeserializer};
    use crate::common::utils::MockTime;
    use crate::common::{TopicPartition, Uuid};
    use crate::consumer::internals::ShareInFlightBatch;
    use crate::consumer::internals::events::{ApplicationEventEnvelope, ShareAcknowledgementEvent};
    use crate::consumer::internals::share_completed_fetch::tests::{
        PendOnceDeserializer, acquired_records, fetch, new_records, new_share_completed_fetch,
        new_string_deserializers, partition_data, tip,
    };

    const DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS: Option<i32> = Some(30000);

    /// The acknowledgement type of each offset an event sent.
    type AcknowledgementTypes = Vec<(i64, Option<AcknowledgeType>)>;

    /// An event the [`Responder`] saw, with the payload the tests assert.
    struct RecordedEvent {
        name: &'static str,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        is_callback_registered: Option<bool>,
    }

    /// Stand-in for Java's `mock(ApplicationEventHandler.class)`: reads the
    /// channel of the consumer's real handler (see the module doc).
    struct Responder {
        rx: mpsc::UnboundedReceiver<ApplicationEventEnvelope>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        events: Vec<RecordedEvent>,
        /// `completeShareSubscriptionChangeApplicationEventSuccessfully`.
        complete_share_subscription_change: bool,
        /// `completeShareUnsubscribeApplicationEventSuccessfully`.
        complete_share_unsubscribe: bool,
        /// Called after the `ShareUnsubscribeEvent` is completed: the last
        /// event `close` waits for before it processes the background events.
        on_share_unsubscribe: Option<Box<dyn FnMut() + Send>>,
        /// Whether the `ShareAcknowledgeOnCloseEvent` is held instead of
        /// completed (an acknowledge-on-close in flight).
        hold_share_acknowledge_on_close: bool,
        held_share_acknowledge_on_close: Option<CompletableEventHandle<()>>,
    }

    impl Responder {
        fn respond(&mut self, envelope: ApplicationEventEnvelope) {
            let name = envelope.event.type_name();
            let mut recorded =
                RecordedEvent { name, acknowledgements_map: IndexMap::new(), is_callback_registered: None };
            match envelope.event {
                ApplicationEvent::ShareSubscriptionChange { handle, topics } => {
                    if self.complete_share_subscription_change {
                        self.subscriptions
                            .lock()
                            .unwrap()
                            .subscribe_to_share_group(topics)
                            .expect("a share subscription");
                    }
                    handle.complete(());
                },
                ApplicationEvent::ShareUnsubscribe { handle } => {
                    if self.complete_share_unsubscribe {
                        self.subscriptions.lock().unwrap().unsubscribe();
                    }
                    handle.complete(());
                    if let Some(hook) = self.on_share_unsubscribe.as_mut() {
                        hook();
                    }
                },
                ApplicationEvent::ShareAcknowledgeOnClose { handle, acknowledgements_map } => {
                    recorded.acknowledgements_map = acknowledgements_map;
                    if self.hold_share_acknowledge_on_close {
                        self.held_share_acknowledge_on_close = Some(handle);
                    } else {
                        handle.complete(());
                    }
                },
                ApplicationEvent::ShareAcknowledgeSync { handle, acknowledgements_map } => {
                    recorded.acknowledgements_map = acknowledgements_map;
                    handle.complete(HashMap::new());
                },
                ApplicationEvent::ShareAcknowledgeAsync { acknowledgements_map, .. }
                | ApplicationEvent::ShareFetch { acknowledgements_map } => {
                    recorded.acknowledgements_map = acknowledgements_map;
                },
                ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration { is_callback_registered } => {
                    recorded.is_callback_registered = Some(is_callback_registered);
                },
                _ => {},
            }
            self.events.push(recorded);
        }

        fn drain(&mut self) {
            while let Ok(envelope) = self.rx.try_recv() {
                self.respond(envelope);
            }
        }

        fn count(&self, name: &str) -> usize {
            self.events.iter().filter(|e| e.name == name).count()
        }

        fn names(&self) -> Vec<&'static str> {
            self.events.iter().map(|e| e.name).collect()
        }

        /// The acknowledgement types each event sent for `tip`, in order.
        fn acknowledgements_sent(&self, tip: &TopicIdPartition) -> Vec<(&'static str, AcknowledgementTypes)> {
            self.events
                .iter()
                .filter_map(|e| {
                    e.acknowledgements_map.get(tip).map(|acks| {
                        let types = acks
                            .acknowledgements()
                            .acknowledgements_type_map()
                            .iter()
                            .map(|(&offset, &r#type)| (offset, r#type))
                            .collect();
                        (e.name, types)
                    })
                })
                .collect()
        }
    }

    /// Runs `fut`, answering the events it sends as they come.
    async fn drive<F: Future>(responder: &mut Responder, fut: F) -> F::Output {
        let mut fut = std::pin::pin!(fut);
        loop {
            tokio::select! {
                biased;
                out = &mut fut => {
                    responder.drain();
                    return out;
                },
                Some(envelope) = responder.rx.recv() => responder.respond(envelope),
            }
        }
    }

    struct Fixture {
        consumer: ShareConsumerImpl<String, String>,
        responder: Responder,
        background_event_tx: mpsc::UnboundedSender<BackgroundEventEnvelope>,
        acknowledgement_event_handler: Arc<ShareAcknowledgementEventHandler>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_buffer: Arc<ShareFetchBuffer>,
        max_time_to_wait_ms: Arc<AtomicI64>,
        wakeup_trigger: WakeupTrigger,
    }

    /// Java's `newConsumer(fetchBuffer, subscriptions, "group-id", "client-id",
    /// acknowledgementMode)`, over the visible-for-testing 19-argument
    /// constructor (`with_components`).
    fn new_consumer_with(
        time: Arc<dyn Time>,
        group_id: &str,
        acknowledgement_mode: &str,
        value_deserializer: Box<dyn DynDeserializer<String>>,
    ) -> Fixture {
        let default_api_timeout_ms = 1000;
        let request_timeout_ms = 30000;
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::AutoOffsetResetStrategy::NONE,
        )));
        let metadata = Arc::new(ShareConsumerMetadata::new(
            100,
            1000,
            i64::MAX,
            false,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        let metrics = Arc::new(Metrics::new());
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9091".to_string()],
            ..ConsumerConfig::defaults()
        };
        let fetch_collector = ShareFetchCollector::new(
            Arc::clone(&metadata),
            Arc::clone(&subscriptions),
            ShareFetchConfig::with_consumer_config(&config).expect("a valid configuration"),
            Arc::new(Deserializers::new(Box::new(StringDeserializer::new()), value_deserializer)),
        );
        let (application_event_tx, rx) = mpsc::unbounded_channel();
        let application_event_handler = Arc::new(ApplicationEventHandler::new(
            application_event_tx,
            Arc::new(tokio::sync::Notify::new()),
        ));
        let network_thread_close =
            NetworkThreadCloseHandle::new(Box::new(|_| {}), Box::new(|| {}), tokio::spawn(async {}));
        let (background_event_tx, background_event_rx) = mpsc::unbounded_channel();
        let acknowledgement_event_handler = Arc::new(ShareAcknowledgementEventHandler::new());
        let fetch_buffer = Arc::new(ShareFetchBuffer::new());
        let max_time_to_wait_ms = Arc::new(AtomicI64::new(0));
        let wakeup_trigger = WakeupTrigger::new();
        let consumer = ShareConsumerImpl::with_components(ShareConsumerImplComponents {
            client_id: Arc::from("client-id"),
            fetch_buffer: Arc::clone(&fetch_buffer),
            fetch_collector,
            // Java passes `mock(ShareFetchMetricsManager.class)`; a real manager
            // over the fixture's untagged registry stands in for it.
            share_fetch_metrics_manager: Arc::new(ShareFetchMetricsManager::new(
                Arc::clone(&metrics),
                &crate::consumer::internals::ShareConsumerMetrics::new().share_fetch_metrics,
            )),
            time,
            application_event_handler,
            network_thread_close,
            max_time_to_wait_ms: Arc::clone(&max_time_to_wait_ms),
            wakeup_trigger: wakeup_trigger.clone(),
            acknowledgement_event_handler: Arc::clone(&acknowledgement_event_handler),
            background_event_rx,
            background_event_queue_size: Arc::new(AtomicI64::new(0)),
            background_event_reaper: CompletableEventReaper::new(),
            metrics: Arc::clone(&metrics),
            async_consumer_metrics: Arc::new(AsyncConsumerMetrics::new(
                Arc::clone(&metrics),
                ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP,
            )),
            subscriptions: Arc::clone(&subscriptions),
            metadata,
            request_timeout_ms,
            default_api_timeout_ms,
            group_id: Some(group_id.to_string()),
            acknowledgement_mode: ShareAcknowledgementMode::from_string(Some(acknowledgement_mode))
                .expect("a valid mode"),
        });
        Fixture {
            consumer,
            responder: Responder {
                rx,
                subscriptions: Arc::clone(&subscriptions),
                events: Vec::new(),
                complete_share_subscription_change: false,
                complete_share_unsubscribe: false,
                on_share_unsubscribe: None,
                hold_share_acknowledge_on_close: false,
                held_share_acknowledge_on_close: None,
            },
            background_event_tx,
            acknowledgement_event_handler,
            subscriptions,
            fetch_buffer,
            max_time_to_wait_ms,
            wakeup_trigger,
        }
    }

    /// Java's `newConsumer(subscriptions)`: implicit mode, `new MockTime(1)`.
    fn new_consumer() -> Fixture {
        new_consumer_mode("implicit")
    }

    fn new_consumer_mode(acknowledgement_mode: &str) -> Fixture {
        new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "group-id",
            acknowledgement_mode,
            Box::new(StringDeserializer::new()),
        )
    }

    impl Fixture {
        /// `doReturn(..).when(fetchCollector).collect(any())`.
        fn stub_collect(&mut self, stub: impl FnMut() -> ShareFetch<String, String> + Send + 'static) {
            let mut stub = stub;
            self.consumer.collect_stub = Some(Box::new(move || Ok(Ok(stub()))));
        }

        /// `doReturn(first).doReturn(ShareFetch.empty())`.
        fn stub_collect_once(&mut self, first: ShareFetch<String, String>) {
            let mut first = Some(first);
            self.stub_collect(move || first.take().unwrap_or_else(ShareFetch::empty));
        }

        async fn subscribe(&mut self, topics: &[&str]) -> Result<(), Error> {
            let topics: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
            drive(&mut self.responder, self.consumer.subscribe(topics)).await
        }

        async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<String, String>, Error> {
            drive(&mut self.responder, self.consumer.poll(timeout)).await
        }

        async fn close(&mut self) -> Result<(), Error> {
            drive(&mut self.responder, self.consumer.close()).await
        }

        fn add_background_error(&self, error: Error) {
            self.background_event_tx
                .send(BackgroundEventEnvelope { event: BackgroundEvent::Error { error }, enqueued_ms: 0 })
                .expect("the consumer holds the receiver");
        }

        /// The two stubs `close` needs in the Java tests.
        fn complete_close_events(&mut self) {
            self.responder.complete_share_unsubscribe = true;
        }
    }

    fn new_tip(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::with_partition_topic(Uuid::random_uuid(), partition, topic)
    }

    fn record(tip: &TopicIdPartition, offset: i64, key: &str, value: &str) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(
            tip.topic(),
            tip.partition(),
            offset,
            Some(key.to_string()),
            Some(value.to_string()),
        )
    }

    /// A fetch of `tip` delivering `offsets` (Java's `addRecord`s).
    fn fetch_with_records(tip: &TopicIdPartition, offsets: &[i64]) -> ShareFetch<String, String> {
        let mut batch = ShareInFlightBatch::new(0, tip.clone(), DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS);
        for &offset in offsets {
            batch.add_record(record(tip, offset, &format!("key{offset}"), &format!("value{offset}")));
        }
        let mut fetch = ShareFetch::empty();
        fetch.add(tip.clone(), batch);
        fetch
    }

    fn offsets(records: &ConsumerRecords<String, String>) -> Vec<i64> {
        let mut offsets: Vec<i64> = records.into_iter().map(ConsumerRecord::offset).collect();
        offsets.sort_unstable();
        offsets
    }

    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default()
    }

    fn required_consumer_properties() -> HashMap<String, String> {
        HashMap::from([(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            "localhost:9091".to_string(),
        )])
    }

    fn new_consumer_from_config(props: &HashMap<String, String>) -> Result<ShareConsumerImpl<String, String>, Error> {
        let config = ConsumerConfig::new(props)?;
        ShareConsumerImpl::new(config, Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()))
    }

    fn source_of(error: &Error) -> &Error {
        std::error::Error::source(error)
            .and_then(|source| source.downcast_ref::<Error>())
            .expect("the construction failure is the source")
    }

    #[tokio::test]
    async fn test_successful_startup_shutdown() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.close().await.expect("close succeeds");
    }

    /// Through the production constructor (`newConsumerWithEmptyGroupId`).
    #[tokio::test]
    async fn test_invalid_group_id() {
        let mut props = required_consumer_properties();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), String::new());
        let e = new_consumer_from_config(&props).expect_err("an empty group id fails");
        assert!(e.is_kafka_error());
        assert!(matches!(source_of(&e), Error::InvalidGroupId(_)), "{e:?}");
        assert_eq!(
            "You must provide a valid group.id in the consumer configuration.",
            source_of(&e).message()
        );
    }

    /// Java fails the construction with an unknown metric reporter class,
    /// which a Rust configuration cannot name, and checks that closing what
    /// was built logs no `NullPointerException`. Here an unparseable
    /// bootstrap address fails it after the deserializers are built, and the
    /// test checks that they are closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fail_constructor() {
        #[derive(Clone, Default)]
        struct CountingDeserializer(Arc<AtomicUsize>);
        impl Deserializer<String> for CountingDeserializer {
            async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                Ok(String::from_utf8_lossy(data).into_owned())
            }
            async fn close(&mut self) -> Result<(), Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let mut props = required_consumer_properties();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "group-id".to_string());
        props.insert(ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), "no-port-here".to_string());
        let closed = CountingDeserializer::default();
        let config = ConsumerConfig::new(&props).expect("the configuration itself is valid");
        let e = ShareConsumerImpl::<String, String>::new(config, Box::new(closed.clone()), Box::new(closed.clone()))
            .expect_err("an unparseable bootstrap address fails construction");
        assert!(matches!(e, Error::KafkaError(_)), "{e:?}");
        assert!(
            e.message().contains("Failed to construct Kafka share consumer"),
            "Unexpected error message: {}",
            e.message()
        );
        assert!(
            source_of(&e).message().contains("Invalid url in bootstrap.servers"),
            "Unexpected cause: {:?}",
            source_of(&e)
        );
        assert_eq!(2, closed.0.load(Ordering::SeqCst), "both deserializers are closed");
    }

    #[tokio::test]
    async fn test_wakeup_before_calling_poll() {
        let mut f = new_consumer();
        f.stub_collect(ShareFetch::empty);
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["foo"]).await.expect("subscribed");

        f.consumer.wakeup();

        let e = f.poll(Duration::ZERO).await.expect_err("the wakeup is raised");
        assert!(matches!(e, Error::Wakeup(_)), "{e:?}");
        f.poll(Duration::ZERO).await.expect("the wakeup was raised once");
    }

    /// Rust-only: pins the current behaviour that wakeups are disabled once
    /// `close` starts (`disable()` runs before close's first `.await`, as
    /// Java's `disableWakeups()` is close's first step). A wakeup pending when
    /// `close` starts is discarded, and later ones, also through the handle, do
    /// nothing. Java's `close` cannot be abandoned; here a dropped `close`
    /// leaves the consumer open with wakeups still disabled (PLAN §8.1 row 19,
    /// an open question for the user), which is how the next `poll` observes it.
    #[tokio::test]
    async fn wakeup_is_disabled_once_close_starts() {
        let mut f = new_consumer();
        f.stub_collect(ShareFetch::empty);
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["foo"]).await.expect("subscribed");

        f.consumer.handle().wakeup();
        assert!(
            f.consumer.close_with_timeout(Duration::from_secs(5)).now_or_never().is_none(),
            "dropped while it waits for the acknowledge-on-close event"
        );
        f.consumer.handle().wakeup();
        f.consumer.wakeup();

        f.poll(Duration::ZERO)
            .await
            .expect("the pending wakeup was discarded and the later ones ignored");
    }

    #[tokio::test]
    async fn test_control_records_on_empty_fetch() {
        let mut f = new_consumer();
        // Set up subscription
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["foo"]).await.expect("subscribed");

        // Create a fetch with only GAP (no records)
        let tip = new_tip("foo", 0);
        let mut batch = ShareInFlightBatch::new(0, tip.clone(), DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS);
        // Add GAP without adding any records
        batch.add_gap(1);
        let mut fetch_with_only_gap = ShareFetch::empty();
        fetch_with_only_gap.add(tip.clone(), batch);
        f.stub_collect_once(fetch_with_only_gap);

        f.poll(Duration::ZERO).await.expect("poll succeeds");

        // Verify that a ShareAcknowledgeAsyncEvent was sent with the acknowledgement GAP for offset 1
        assert_eq!(
            vec![("ShareAcknowledgeAsync", vec![(1, None)])],
            f.responder.acknowledgements_sent(&tip)
        );
    }

    #[tokio::test]
    async fn test_wakeup_after_empty_fetch() {
        let mut f = new_consumer();
        let wakeup = f.wakeup_trigger.clone();
        let mut first = true;
        f.stub_collect(move || {
            if std::mem::replace(&mut first, false) {
                wakeup.wakeup();
            }
            ShareFetch::empty()
        });
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["foo"]).await.expect("subscribed");

        let e = f.poll(Duration::from_secs(60)).await.expect_err("the wakeup is raised");
        assert!(matches!(e, Error::Wakeup(_)), "{e:?}");
        f.poll(Duration::ZERO).await.expect("the wakeup was raised once");
    }

    #[tokio::test]
    async fn test_wakeup_after_non_empty_fetch() {
        let mut f = new_consumer();
        let tip = new_tip("foo", 3);
        let wakeup = f.wakeup_trigger.clone();
        let tip_for_stub = tip.clone();
        f.stub_collect(move || {
            wakeup.wakeup();
            fetch_with_records(&tip_for_stub, &[2])
        });
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["foo"]).await.expect("subscribed");

        // since wakeup() is called when the non-empty fetch is returned the wakeup should be ignored
        f.poll(Duration::from_secs(60)).await.expect("the records are returned");
        // the previously ignored wake-up should not be ignored in the next call
        let e = f.poll(Duration::ZERO).await.expect_err("the wakeup is raised");
        assert!(matches!(e, Error::Wakeup(_)), "{e:?}");
    }

    #[tokio::test]
    async fn test_fail_on_closed_consumer() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.close().await.expect("close succeeds");
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| f.consumer.subscription()))
            .expect_err("a closed consumer panics");
        assert_eq!("This consumer has already been closed.", panic_message(res));
        let res2 = std::panic::catch_unwind(AssertUnwindSafe(|| f.consumer.acquisition_lock_timeout_ms()))
            .expect_err("a closed consumer panics");
        assert_eq!("This consumer has already been closed.", panic_message(res2));
    }

    #[tokio::test]
    async fn test_should_send_one_share_fetch_event_per_poll() {
        let mut f = new_consumer();
        // Set up an empty fetch.
        f.stub_collect(ShareFetch::empty);
        // Set up subscription
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["test-topic"]).await.expect("subscribed");

        f.max_time_to_wait_ms.store(0, Ordering::Release);
        // Check that only 1 ShareFetchEvent is sent per poll
        f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(1, f.responder.count("ShareFetch"));
    }

    #[tokio::test]
    async fn test_unsubscribe_with_topic_authorization_exception() {
        let mut f = new_consumer();
        f.add_background_error(Error::topic_authorization(HashSet::from(["test-topic".to_string()])));
        f.complete_close_events();
        drive(&mut f.responder, f.consumer.unsubscribe())
            .await
            .expect("unsubscribe does not process background events");
        f.close().await.expect("close ignores the authorization error");
    }

    #[tokio::test]
    async fn test_close_with_invalid_topic_exception() {
        let mut f = new_consumer();
        f.add_background_error(Error::invalid_topics(HashSet::from(["!test-topic".to_string()])));
        f.complete_close_events();
        f.close().await.expect("close ignores the invalid topic error");
    }

    #[tokio::test]
    async fn test_explicit_mode_unacknowledged_records() {
        // Set up consumer with explicit acknowledgement mode
        let mut f = new_consumer_mode("explicit");

        // Set up test data
        let tip = new_tip("test-topic", 0);
        // Set up first fetch to return records
        f.stub_collect_once(fetch_with_records(&tip, &[0, 1]));

        // Set up subscription
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["test-topic"]).await.expect("subscribed");
        assert_eq!(None, f.consumer.acquisition_lock_timeout_ms());

        // First poll should succeed and return records
        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(2, records.count(), "Should have received 2 records");
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());

        // Second poll should fail because records weren't acknowledged
        let exception = f
            .poll(Duration::from_millis(100))
            .await
            .expect_err("records are unacknowledged");
        assert_eq!(
            "All records must be acknowledged in explicit acknowledgement mode.",
            exception.message(),
            "Unexpected error message: {exception:?}"
        );
        assert!(matches!(exception, Error::LocalIllegalState(_)));
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());

        // Verify that acknowledging one record but not all still throws exception
        let mut iterator = records.into_iter();
        let first = iterator.next().expect("a first record");
        f.consumer.acknowledge_with_record(&first).expect("in flight");
        let exception = f
            .poll(Duration::from_millis(100))
            .await
            .expect_err("one record is unacknowledged");
        assert!(matches!(exception, Error::LocalIllegalState(_)), "{exception:?}");
        assert_eq!(
            "All records must be acknowledged in explicit acknowledgement mode.",
            exception.message(),
            "Unexpected error message: {exception:?}"
        );

        // Verify that after acknowledging all records, poll succeeds
        let second = iterator.next().expect("a second record");
        f.consumer.acknowledge_with_record(&second).expect("in flight");

        // Set up second fetch to return new records
        let tip_for_stub = tip.clone();
        f.stub_collect(move || fetch_with_records(&tip_for_stub, &[2, 3]));

        // Verify that poll succeeds and returns new records
        let new_records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(2, new_records.count(), "Should have received 2 new records");
    }

    /// The renewed record is delivered again from the batch's bytes
    /// (decision D5), so the first fetch is decoded from a real record batch
    /// (`ShareCompletedFetch`) where Java adds the records to the batch.
    #[tokio::test]
    async fn test_explicit_mode_renew_and_acknowledge_on_poll() {
        // Set up consumer with explicit acknowledgement mode
        let mut f = new_consumer_mode("explicit");

        // Set up test data
        let tip = tip();
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(new_records(0, 2)), acquired_records(0, 2)));
        let batch = fetch(&mut completed_fetch, &new_string_deserializers(), 10, true);
        let mut first_fetch = ShareFetch::empty();
        first_fetch.add(tip.clone(), batch);
        // Set up first fetch to return records
        f.stub_collect_once(first_fetch);

        // Set up subscription
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&[tip.topic()]).await.expect("subscribed");

        // First poll should succeed and return records
        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(2, records.count(), "Should have received 2 records");
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());

        // Renew the first record and accept the second
        let mut iterator = records.into_iter();
        f.consumer
            .acknowledge_with_record_type(&iterator.next().expect("record 0"), AcknowledgeType::Renew)
            .expect("in flight");
        f.consumer
            .acknowledge_with_record_type(&iterator.next().expect("record 1"), AcknowledgeType::Accept)
            .expect("in flight");

        // Second poll should succeed and return the renewed record again
        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(0, records.count(), "Should have received 1 record");
        assert!(f.consumer.current_fetch.has_renewals());
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());

        let mut acks = Acknowledgements::empty();
        acks.add(0, Some(AcknowledgeType::Renew));
        acks.complete(None);
        let e = ShareAcknowledgementEvent::new(HashMap::from([(tip.clone(), acks)]), true, None);
        f.acknowledgement_event_handler.add(e);

        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(1, records.count(), "Should have received 1 record");
        assert!(!f.consumer.current_fetch.has_renewals());
        let renewed_record = records.into_iter().next().expect("the renewed record");
        assert_eq!(0, renewed_record.offset());
        f.consumer.acknowledge_with_record(&renewed_record).expect("in flight");
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());

        // Set up next fetch to return no records
        f.stub_collect(ShareFetch::empty);

        // Third poll should return no records
        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert!(records.is_empty());

        // Set up next fetch to return new records
        let tip_for_stub = tip.clone();
        f.stub_collect(move || fetch_with_records(&tip_for_stub, &[2, 3]));

        // Verify that poll succeeds and returns new records
        let new_records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(2, new_records.count(), "Should have received 2 new records");
        assert_eq!(DEFAULT_ACQUISITION_LOCK_TIMEOUT_MS, f.consumer.acquisition_lock_timeout_ms());
    }

    #[tokio::test]
    async fn test_close_with_topic_authorization_exception() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.close().await.expect("close succeeds");
    }

    #[tokio::test]
    async fn test_stop_find_coordinator_on_close() {
        let mut f = new_consumer();
        // Set up the expected successful completion of close events
        f.complete_close_events();

        // Close the consumer
        f.close().await.expect("close succeeds");

        // Verify events are sent in correct order
        let names: Vec<&str> = f
            .responder
            .names()
            .into_iter()
            .filter(|name| {
                [
                    "ShareAcknowledgeOnClose",
                    "ShareUnsubscribe",
                    "StopFindCoordinatorOnClose",
                ]
                .contains(name)
            })
            .collect();
        assert_eq!(
            vec![
                "ShareAcknowledgeOnClose",
                "ShareUnsubscribe",
                "StopFindCoordinatorOnClose"
            ],
            names
        );
    }

    #[tokio::test]
    async fn test_verify_application_event_on_shutdown() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.close().await.expect("close succeeds");
        assert_eq!(1, f.responder.count("ShareAcknowledgeOnClose"));
        assert_eq!(1, f.responder.count("ShareUnsubscribe"));
    }

    struct NoopCallback;

    #[async_trait]
    impl AcknowledgementCommitCallback for NoopCallback {
        async fn on_complete(&self, _offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&Error>) {}
    }

    fn registrations(f: &mut Fixture) -> Vec<bool> {
        f.responder.drain();
        f.responder.events.iter().filter_map(|e| e.is_callback_registered).collect()
    }

    #[tokio::test]
    async fn test_acknowledgement_commit_callback_registration_event() {
        let mut f = new_consumer();
        let callback: Arc<dyn AcknowledgementCommitCallback> = Arc::new(NoopCallback);

        f.consumer.set_acknowledgement_commit_callback(Some(Arc::clone(&callback)));
        assert_eq!(vec![true], registrations(&mut f));

        f.consumer.set_acknowledgement_commit_callback(Some(callback));
        // As we have already set the callback, we should not add another event. We only add when we initially register.
        assert_eq!(vec![true], registrations(&mut f));
    }

    #[tokio::test]
    async fn test_acknowledgement_commit_callback_registration_event_null() {
        let mut f = new_consumer();
        let callback: Arc<dyn AcknowledgementCommitCallback> = Arc::new(NoopCallback);

        f.consumer.set_acknowledgement_commit_callback(None);
        // Initially callback is set to null, setting again to null should not add an event.
        assert!(registrations(&mut f).is_empty());

        f.consumer.set_acknowledgement_commit_callback(Some(callback));
        assert_eq!(vec![true], registrations(&mut f));

        // Now we are changing from a non-null callback to null, this should add an event.
        f.consumer.set_acknowledgement_commit_callback(None);
        assert_eq!(vec![true, false], registrations(&mut f));
    }

    /// Java runs a completed future's `get` and a throwing runnable; Rust's
    /// `complete_quietly` takes their outcome.
    #[test]
    fn test_complete_quietly() {
        let mut exception = None;
        ShareConsumerImpl::<String, String>::complete_quietly(Ok(()), "test", &mut exception);
        assert!(exception.is_none());

        ShareConsumerImpl::<String, String>::complete_quietly(
            Err(Error::kafka_message("Test exception")),
            "test",
            &mut exception,
        );
        assert!(matches!(exception, Some(Error::KafkaError(_))), "{exception:?}");

        // Rust-only: a timeout is not recorded.
        let mut exception = None;
        ShareConsumerImpl::<String, String>::complete_quietly(Err(Error::timeout("late")), "test", &mut exception);
        assert!(exception.is_none());
    }

    #[tokio::test]
    async fn test_subscribe_generates_event() {
        let mut f = new_consumer();
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["topic1"]).await.expect("subscribed");
        assert_eq!(HashSet::from(["topic1".to_string()]), f.consumer.subscription());
        assert_eq!(1, f.responder.count("ShareSubscriptionChange"));
    }

    #[tokio::test]
    async fn test_unsubscribe_generates_unsubscribe_event() {
        let mut f = new_consumer();
        f.complete_close_events();
        drive(&mut f.responder, f.consumer.unsubscribe()).await.expect("unsubscribed");
        assert_eq!(1, f.responder.count("ShareUnsubscribe"));
    }

    #[tokio::test]
    async fn test_subscribe_to_empty_list_acts_as_unsubscribe() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.subscribe(&[]).await.expect("unsubscribed");
        assert_eq!(1, f.responder.count("ShareUnsubscribe"));
        assert_eq!(0, f.responder.count("ShareSubscriptionChange"));
    }

    #[tokio::test]
    async fn test_subscription_on_empty_topic() {
        let mut f = new_consumer();
        let e = f.subscribe(&["  "]).await.expect_err("a blank topic is rejected");
        assert!(matches!(e, Error::LocalIllegalArgument(_)), "{e:?}");
        assert_eq!(
            "Topic collection to subscribe to cannot contain null or empty topic",
            e.message()
        );
    }

    #[tokio::test]
    async fn test_background_error() {
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "shareGroupA",
            "implicit",
            Box::new(StringDeserializer::new()),
        );
        let expected_exception = Error::kafka_message("Nobody expects the Spanish Inquisition");
        f.add_background_error(expected_exception.clone());
        f.subscribe(&["t1"]).await.expect("subscribe returns");
        let exception = f.poll(Duration::ZERO).await.expect_err("the background error is raised");
        assert!(exception.is_kafka_error());
        assert_eq!(expected_exception.message(), exception.message());
    }

    #[tokio::test]
    async fn test_multiple_background_errors() {
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "shareGroupA",
            "implicit",
            Box::new(StringDeserializer::new()),
        );
        let expected_exception1 = Error::kafka_message("Nobody expects the Spanish Inquisition");
        f.add_background_error(expected_exception1.clone());
        f.add_background_error(Error::kafka_message("Spam, Spam, Spam"));
        f.subscribe(&["t1"]).await.expect("subscribe returns");
        let exception = f.poll(Duration::ZERO).await.expect_err("the background error is raised");
        assert!(exception.is_kafka_error(), "{exception:?}");
        assert_eq!(expected_exception1.message(), exception.message());
        assert!(f.consumer.background_event_rx.is_empty());
    }

    #[tokio::test]
    async fn test_group_id_null() {
        let e = new_consumer_from_config(&required_consumer_properties()).expect_err("no group id fails");
        assert!(matches!(e, Error::KafkaError(_)), "{e:?}");
        assert_eq!("Failed to construct Kafka share consumer", e.message());
    }

    async fn test_invalid_group_id_with(group_id: &str) {
        let mut props = required_consumer_properties();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), group_id.to_string());
        let e = new_consumer_from_config(&props).expect_err("an invalid group id fails");
        assert!(matches!(e, Error::KafkaError(_)), "{e:?}");
        assert_eq!("Failed to construct Kafka share consumer", e.message());
    }

    #[tokio::test]
    async fn test_group_id_empty() {
        test_invalid_group_id_with("").await;
    }

    #[tokio::test]
    async fn test_group_id_only_whitespaces() {
        test_invalid_group_id_with("       ").await;
    }

    #[tokio::test]
    async fn test_ensure_poll_event_sent_on_consumer_poll() {
        let mut f = new_consumer();
        let tip = new_tip("topic", 0);
        f.stub_collect_once(fetch_with_records(&tip, &[2]));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["topic"]).await.expect("subscribed");

        f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert_eq!(1, f.responder.count("SharePoll"));
        assert_eq!(1, f.responder.count("ShareSubscriptionChange"));

        f.complete_close_events();
        f.close().await.expect("close succeeds");
        assert_eq!(1, f.responder.count("ShareAcknowledgeOnClose"));
    }

    #[tokio::test]
    async fn test_poll_does_not_add_new_share_poll_event_when_one_is_already_in_flight() {
        let time = Arc::new(MockTime::with_auto_tick_ms(1));
        let mut f = new_consumer_with(
            Arc::clone(&time) as Arc<dyn Time>,
            "group-id",
            "implicit",
            Box::new(StringDeserializer::new()),
        );
        let tp = TopicPartition::new("topic1".to_string(), 0);
        f.subscriptions
            .lock()
            .unwrap()
            .assign_from_user(HashSet::from([tp.clone()]))
            .expect("assigned");
        f.subscriptions.lock().unwrap().seek(&tp, 0).expect("seeked");

        // Keep pollForFetches from spinning by making it "wait" and advance MockTime.
        f.max_time_to_wait_ms.store(100, Ordering::Release);
        let waits = Arc::new(AtomicUsize::new(0));
        let waits_in_stub = Arc::clone(&waits);
        f.consumer.await_not_empty_stub = Some(Box::new(move |poll_timeout_ms| {
            waits_in_stub.fetch_add(1, Ordering::SeqCst);
            time.sleep(poll_timeout_ms);
        }));

        // Always empty fetch: forces multiple loop iterations until the overall poll timeout expires.
        f.stub_collect(ShareFetch::empty);

        let result = f.poll(Duration::from_millis(450)).await.expect("poll succeeds");
        assert!(result.is_empty());

        // Ensure we actually exercised the "wait for fetches" path (i.e., more than a trivial single pass).
        assert!(waits.load(Ordering::SeqCst) >= 1);

        // Only one SharePollEvent must have been added despite multiple poll loop iterations.
        assert_eq!(1, f.responder.count("SharePoll"));
    }

    /// Java's `doAnswer` on `add(StopFindCoordinatorOnCloseEvent)` queues the
    /// error inside `close`, before `processBackgroundEventsOnClose` drains
    /// it. The responder runs only while `close` waits, and the last wait
    /// before that step is the unsubscribe, so the error is queued as the
    /// unsubscribe completes. The queue is empty after `close` whatever the
    /// step does (the final reap clears it, where Java's mocked reaper does
    /// not), so `close`'s success is what shows the error was swallowed; the
    /// next test shows the step runs.
    #[tokio::test]
    async fn test_close_with_background_queue_errors_after_unsubscribe() {
        for error in [
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::InvalidTopicError,
        ] {
            let mut f = new_consumer();
            // Complete the acknowledge on close event and the unsubscribe event successfully
            f.complete_close_events();

            // Add errors to the queue after unsubscribe event is processed
            f.responder.on_share_unsubscribe = Some(queue_background_error(&f, move || error.error()));

            // Close should complete successfully despite the errors in the background queue
            f.close().await.unwrap_or_else(|e| panic!("{error:?}: {e:?}"));

            // Verify that the background queue was processed
            assert!(
                f.consumer.background_event_rx.is_empty(),
                "Background queue should be empty after close"
            );
        }
    }

    /// Rust-only: the background error queued as the unsubscribe completes is
    /// processed before the background task is stopped; one that `close` does
    /// not ignore fails it.
    #[tokio::test]
    async fn close_processes_the_background_errors_after_unsubscribe() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.responder.on_share_unsubscribe = Some(queue_background_error(&f, || {
            Some(Error::kafka_message("not ignored on close"))
        }));

        let e = f.close().await.expect_err("the error is not ignored");
        assert!(e.is_kafka_error(), "{e:?}");
        assert_eq!("Failed to close Kafka share consumer", e.message());
        assert_eq!("not ignored on close", source_of(&e).message());
    }

    /// A hook queuing the error `error` returns on the background-event queue.
    fn queue_background_error(
        f: &Fixture,
        error: impl Fn() -> Option<Error> + Send + 'static,
    ) -> Box<dyn FnMut() + Send> {
        let background_event_tx = f.background_event_tx.clone();
        Box::new(move || {
            let error = error().expect("an error");
            background_event_tx
                .send(BackgroundEventEnvelope { event: BackgroundEvent::Error { error }, enqueued_ms: 0 })
                .expect("the consumer holds the receiver");
        })
    }

    /// The fixture on `time`. Java's tests time the wait with a `MockTime`
    /// that the mocked future's `get` sleeps; the Rust wait is a real
    /// `tokio::time::timeout`, so those tests run on the system clock.
    fn consumer_on_clock<T: Time>(time: Arc<T>) -> (Fixture, Arc<T>) {
        let f = new_consumer_with(
            Arc::clone(&time) as Arc<dyn Time>,
            "group-id",
            "implicit",
            Box::new(StringDeserializer::new()),
        );
        (f, time)
    }

    /// Java's future times out twice in its 100 ms slices and then completes,
    /// leaving exactly 800 ms of the mock clock: the assertion pins the slice.
    /// The Rust wait is a `tokio::time::timeout` the test cannot intercept, so
    /// this checks less: the event completes after 200 ms of real time, so at
    /// most 800 ms are left, and it is returned well before the deadline (the
    /// lower bound only allows for a slow machine). The slice is not pinned.
    #[tokio::test]
    async fn test_process_background_events_with_initial_delay() {
        let (mut f, time) = consumer_on_clock(Arc::new(crate::common::utils::SystemTime));
        let (handle, receiver) = CompletableEventHandle::<()>::new(i64::MAX);
        let completer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            handle.complete(());
        });
        let deadline_ms = time.milliseconds() + 1000;

        f.consumer
            .process_background_events_with_future_deadline_ms_ignore_error_event_error(receiver, deadline_ms, |_| {
                false
            })
            .await
            .expect("the event completes");

        // 800 is the 1000 ms timeout (above) minus the 200 ms delay for the two incremental timeouts/retries.
        let remaining_ms = deadline_ms - time.milliseconds();
        assert!((500..=800).contains(&remaining_ms), "{remaining_ms}");
        completer.await.expect("the completer ends");
    }

    #[tokio::test]
    async fn test_process_background_events_without_delay() {
        let (mut f, time) = consumer_on_clock(Arc::new(MockTime::new()));
        // Create a future that is already completed.
        let (handle, receiver) = CompletableEventHandle::<()>::new(i64::MAX);
        handle.complete(());
        let deadline_ms = time.milliseconds() + 1000;

        f.consumer
            .process_background_events_with_future_deadline_ms_ignore_error_event_error(receiver, deadline_ms, |_| {
                false
            })
            .await
            .expect("the event is complete");

        // Because we didn't need to perform a timed get, we should still have every last millisecond
        // of our initial timeout.
        assert_eq!(1000, deadline_ms - time.milliseconds());
    }

    #[tokio::test]
    async fn test_record_background_event_queue_size() {
        let mut f = new_consumer();
        let metrics = Arc::clone(f.consumer.metrics_registry());
        let name = metrics.metric_name("background-event-queue-size", ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP);
        let value = |metrics: &Metrics| {
            use crate::common::Metric;
            metrics.metric(&name).expect("registered").metric_value().as_double()
        };

        f.add_background_error(Error::InvalidRecordState(InvalidRecordStateError::new(
            "The record is in the wrong state",
        )));
        f.consumer.async_consumer_metrics().record_background_event_queue_size(1);
        assert_eq!(Some(1.0), value(&metrics));

        let e = f.consumer.process_background_events().expect_err("the error is raised");
        assert!(matches!(e, Error::InvalidRecordState(_)), "{e:?}");
        assert_eq!(Some(0.0), value(&metrics));
    }

    #[tokio::test]
    async fn test_process_background_events_times_out() {
        let (mut f, time) = consumer_on_clock(Arc::new(crate::common::utils::SystemTime));
        let (_handle, receiver) = CompletableEventHandle::<()>::new(i64::MAX);
        let deadline_ms = time.milliseconds() + 1000;

        let e = f
            .consumer
            .process_background_events_with_future_deadline_ms_ignore_error_event_error(receiver, deadline_ms, |_| {
                false
            })
            .await
            .expect_err("the event never completes");
        assert!(matches!(e, Error::Timeout(_)), "{e:?}");
        assert_eq!("Operation timed out before completion", e.message());

        // Because we forced our mocked future to continuously time out, we should have no time remaining.
        assert_eq!(0, (deadline_ms - time.milliseconds()).max(0));
    }

    fn assert_metrics_map(f: &Fixture, metrics_should_be_present: bool) {
        let metrics = f.consumer.metrics().snapshot();
        let mut names: Vec<String> = metrics
            .keys()
            // There's a meta-metric named "count" that is automatically added to the metrics map.
            .filter(|name| !(name.name() == "count" && name.group() == "kafka-metrics-count"))
            .map(|name| format!("{}:{}", name.group(), name.name()))
            .collect();
        assert_eq!(
            metrics.len(),
            names.len() + 1,
            "The \"count\" meta-metric was unexpectedly missing from the Consumer metrics"
        );
        if metrics_should_be_present {
            assert!(
                !names.is_empty(),
                "The consumer should have created metrics, but they are unexpectedly empty"
            );
        } else {
            names.sort();
            assert_eq!(
                Vec::<String>::new(),
                names,
                "The consumer should have removed its metrics on close(), but there are metrics remaining"
            );
        }
    }

    #[tokio::test]
    async fn test_metrics_removed_on_close() {
        let mut f = new_consumer();
        assert_metrics_map(&f, true);
        drive(&mut f.responder, f.consumer.close_with_timeout(Duration::ZERO))
            .await
            .expect("close succeeds");
        assert_metrics_map(&f, false);
    }

    /// Java's configuration: SASL `PLAIN` with a JAAS configuration naming a
    /// login module that does not exist. The outer two levels of the chain are
    /// Java's; beneath them Java has the `LoginException` of the missing module,
    /// the Rust client (which reads no login module) the channel builder's
    /// `ConfigError` for the missing PLAIN username. Built as Java builds it,
    /// with the public `KafkaShareConsumer` constructor (`ShareConsumerConfig`
    /// first, then this class).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_constructor_fails_on_network_client_constructor_failure() {
        let mut props = required_consumer_properties();
        props.insert(
            ConsumerConfig::GROUP_ID_CONFIG.to_string(),
            "invalid-login-test-group".to_string(),
        );
        props.insert("security.protocol".to_string(), "SASL_PLAINTEXT".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.example.InvalidLoginModule required ;".to_string(),
        );
        let e = crate::consumer::KafkaShareConsumer::<String, String>::new(
            &props,
            StringDeserializer::new(),
            StringDeserializer::new(),
        )
        .expect_err("the network client cannot be created");
        assert!(matches!(e, Error::KafkaError(_)), "{e:?}");
        assert_eq!("Failed to construct Kafka share consumer", e.message());

        // The root cause is multiple errors deep.
        let cause = source_of(&e);
        assert!(matches!(cause, Error::KafkaError(_)), "{cause:?}");
        assert_eq!("Failed to create new NetworkClient", cause.message());

        let cause = source_of(cause);
        assert!(matches!(cause, Error::Config(_)), "{cause:?}");
        assert_eq!(
            "Failed to create channel builder: SASL PLAIN authentication requires a username",
            cause.message()
        );
    }

    // ── KafkaShareConsumerMetricsTest, through ShareConsumerImpl ───────────

    /// Java's `KafkaShareConsumerMetricsTest#newShareConsumer`: the
    /// visible-for-testing constructor over a `MockClient`, its background task
    /// running the real share request managers.
    fn new_share_consumer_over_mock_client(time: Arc<MockTime>) -> ShareConsumerImpl<String, String> {
        let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ShareConsumerMetadata::new(
            0,
            0,
            i64::MAX,
            false,
            Arc::clone(&subscriptions),
            ClusterResourceListeners::new(),
        ));
        let props = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
            (ConsumerConfig::CLIENT_ID_CONFIG.to_string(), "mock-consumer".to_string()),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "mock-group".to_string()),
            (ConsumerConfig::DEFAULT_API_TIMEOUT_MS_CONFIG.to_string(), "60000".to_string()),
            (ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG.to_string(), "30000".to_string()),
        ]);
        let config = crate::consumer::ShareConsumerConfig::new(&props)
            .expect("a valid share configuration")
            .into_consumer_config();
        let client = crate::MockClient::new(Arc::clone(&time) as Arc<dyn Time>);
        ShareConsumerImpl::with_client(
            "mock-consumer",
            "mock-group",
            &config,
            Box::new(StringDeserializer::new()),
            Box::new(StringDeserializer::new()),
            time,
            client,
            subscriptions,
            metadata,
        )
        .expect("the consumer is built")
    }

    fn share_metric(consumer: &ShareConsumerImpl<String, String>, name: &str) -> Option<f64> {
        use crate::common::Metric;
        let metric_name = consumer
            .metrics_registry()
            .metric_name(name, format!("{}-metrics", ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX));
        consumer
            .metrics()
            .get(&metric_name)
            .and_then(|metric| metric.metric_value().as_double())
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollTimeMetrics`, through real
    /// `subscribe` and `poll` calls on the background task.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_poll_time_metrics() {
        let time = Arc::new(MockTime::new());
        time.sleep(1_000_000);
        let mut consumer = new_share_consumer_over_mock_client(Arc::clone(&time));
        consumer.subscribe(vec!["test".to_string()]).await.expect("subscribed");
        // Test default values
        assert_eq!(Some(-1.0), share_metric(&consumer, "last-poll-seconds-ago"));
        assert!(share_metric(&consumer, "time-between-poll-avg").expect("registered").is_nan());
        assert!(share_metric(&consumer, "time-between-poll-max").expect("registered").is_nan());
        // Call first poll
        consumer.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(Some(0.0), share_metric(&consumer, "last-poll-seconds-ago"));
        assert_eq!(Some(0.0), share_metric(&consumer, "time-between-poll-avg"));
        assert_eq!(Some(0.0), share_metric(&consumer, "time-between-poll-max"));
        // Advance time by 5,000 (total time = 5,000)
        time.sleep(5 * 1000);
        assert_eq!(Some(5.0), share_metric(&consumer, "last-poll-seconds-ago"));
        // Call second poll
        consumer.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(Some(2.5 * 1000.0), share_metric(&consumer, "time-between-poll-avg"));
        assert_eq!(Some(5.0 * 1000.0), share_metric(&consumer, "time-between-poll-max"));
        // Advance time by 10,000 (total time = 15,000)
        time.sleep(10 * 1000);
        assert_eq!(Some(10.0), share_metric(&consumer, "last-poll-seconds-ago"));
        // Call third poll
        consumer.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(Some(5.0 * 1000.0), share_metric(&consumer, "time-between-poll-avg"));
        assert_eq!(Some(10.0 * 1000.0), share_metric(&consumer, "time-between-poll-max"));
        // Advance time by 5,000 (total time = 20,000)
        time.sleep(5 * 1000);
        assert_eq!(Some(5.0), share_metric(&consumer, "last-poll-seconds-ago"));
        // Call fourth poll
        consumer.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(Some(5.0 * 1000.0), share_metric(&consumer, "time-between-poll-avg"));
        assert_eq!(Some(10.0 * 1000.0), share_metric(&consumer, "time-between-poll-max"));
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollIdleRatio`, recording
    /// through the consumer's `kafka_share_consumer_metrics`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_poll_idle_ratio() {
        let time = Arc::new(MockTime::new());
        let mut consumer = new_share_consumer_over_mock_client(Arc::clone(&time));
        // Test default value
        assert!(share_metric(&consumer, "poll-idle-ratio-avg").expect("registered").is_nan());

        // 1st poll
        // Spend 50ms in poll so value = 1.0
        consumer.kafka_share_consumer_metrics().record_poll_start(time.milliseconds());
        time.sleep(50);
        consumer.kafka_share_consumer_metrics().record_poll_end(time.milliseconds());
        assert_eq!(Some(1.0), share_metric(&consumer, "poll-idle-ratio-avg"));

        // 2nd poll
        // Spend 50m outside poll and 0ms in poll so value = 0.0
        time.sleep(50);
        consumer.kafka_share_consumer_metrics().record_poll_start(time.milliseconds());
        consumer.kafka_share_consumer_metrics().record_poll_end(time.milliseconds());
        // Avg of first two data points
        assert_eq!(Some((1.0 + 0.0) / 2.0), share_metric(&consumer, "poll-idle-ratio-avg"));

        // 3rd poll
        // Spend 25ms outside poll and 25ms in poll so value = 0.5
        time.sleep(25);
        consumer.kafka_share_consumer_metrics().record_poll_start(time.milliseconds());
        time.sleep(25);
        consumer.kafka_share_consumer_metrics().record_poll_end(time.milliseconds());
        // Avg of three data points
        assert_eq!(Some((1.0 + 0.0 + 0.5) / 3.0), share_metric(&consumer, "poll-idle-ratio-avg"));
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// Java `KafkaShareConsumerMetricsTest#testPollIdleRatioZero`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_poll_idle_ratio_zero() {
        let time = Arc::new(MockTime::new());
        let mut consumer = new_share_consumer_over_mock_client(Arc::clone(&time));
        // Test default value
        assert!(share_metric(&consumer, "poll-idle-ratio-avg").expect("registered").is_nan());

        // Poll starts and ends within the same millisecond, so the metric should be 0.
        consumer.kafka_share_consumer_metrics().record_poll_start(time.milliseconds());
        consumer.kafka_share_consumer_metrics().record_poll_end(time.milliseconds());
        assert_eq!(Some(0.0), share_metric(&consumer, "poll-idle-ratio-avg"));
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    /// Java `KafkaShareConsumerMetricsTest#testClosingConsumerUnregistersConsumerMetrics`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_closing_consumer_unregisters_consumer_metrics() {
        let time = Arc::new(MockTime::with_auto_tick_ms(1));
        let mut consumer = new_share_consumer_over_mock_client(time);
        consumer.subscribe(vec!["test".to_string()]).await.expect("subscribed");
        for name in [
            "last-poll-seconds-ago",
            "time-between-poll-avg",
            "time-between-poll-max",
        ] {
            assert!(share_metric(&consumer, name).is_some(), "{name}");
        }
        consumer.close().await.expect("closed");
        for name in [
            "last-poll-seconds-ago",
            "time-between-poll-avg",
            "time-between-poll-max",
        ] {
            assert!(share_metric(&consumer, name).is_none(), "{name}");
        }
    }

    // ── Rust-only: the production constructor (translation-checklist R3) ──

    /// A share consumer built as `KafkaShareConsumer` builds it, from a
    /// `ShareConsumerConfig`.
    fn new_share_consumer_from_config(
        props: &HashMap<String, String>,
    ) -> Result<ShareConsumerImpl<String, String>, Error> {
        let config = crate::consumer::ShareConsumerConfig::new(props)?.into_consumer_config();
        ShareConsumerImpl::new(config, Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()))
    }

    fn production_config(acknowledgement_mode: Option<&str>) -> HashMap<String, String> {
        let mut props = required_consumer_properties();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "group-id".to_string());
        props.insert(ConsumerConfig::CLIENT_ID_CONFIG.to_string(), "client-id".to_string());
        props.insert(ConsumerConfig::CLIENT_RACK_CONFIG.to_string(), "rack-a".to_string());
        if let Some(mode) = acknowledgement_mode {
            props.insert(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG.to_string(), mode.to_string());
        }
        props
    }

    /// Rust-only: `ShareConsumerImpl::new` reads `share.acknowledgement.mode`
    /// and `client.id`, and its background task serves `subscribe` and `close`.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_reads_the_configuration_and_serves_subscribe_and_close() {
        let mut consumer = new_share_consumer_from_config(&production_config(Some("explicit"))).expect("built");
        assert_eq!("client-id", consumer.client_id());
        consumer.subscribe(vec!["topic-a".to_string()]).await.expect("subscribed");
        assert_eq!(HashSet::from(["topic-a".to_string()]), consumer.subscription());
        let not_in_flight = ConsumerRecord::new("topic-a", 0, 0, None::<String>, None::<String>);
        let e = consumer
            .acknowledge_with_record(&not_in_flight)
            .expect_err("no record is in flight");
        assert_eq!("The record cannot be acknowledged.", e.message());
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
        let e = consumer.unsubscribe().await.expect_err("closed");
        assert_eq!("This consumer has already been closed.", e.message());
    }

    /// Rust-only (R3): `client.rack` reaches the share membership manager,
    /// as Java's `GroupRebalanceConfig#rackId` (absent when empty), and
    /// `group.id` the share managers.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_passes_the_group_id_and_the_rack_id_to_the_share_managers() {
        for (configured, expected) in [(Some("rack-a"), Some("rack-a")), (None, None), (Some(""), None)] {
            let mut props = production_config(None);
            props.remove(ConsumerConfig::CLIENT_RACK_CONFIG);
            if let Some(rack) = configured {
                props.insert(ConsumerConfig::CLIENT_RACK_CONFIG.to_string(), rack.to_string());
            }
            let mut consumer = new_share_consumer_from_config(&props).expect("built");
            let membership = consumer
                .request_managers
                .as_ref()
                .expect("built by the constructor")
                .lock()
                .unwrap()
                .share_membership
                .clone()
                .expect("a share membership manager");
            assert_eq!(expected, membership.rack_id(), "client.rack = {configured:?}");
            assert_eq!("group-id", membership.group_id());
            consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
        }
    }

    /// Rust-only: the default acknowledgement mode is implicit (decision D7
    /// error text).
    #[tokio::test(flavor = "multi_thread")]
    async fn new_defaults_to_implicit_acknowledgement() {
        let mut consumer = new_share_consumer_from_config(&production_config(None)).expect("built");
        let any = ConsumerRecord::new("topic-a", 0, 0, None::<String>, None::<String>);
        let e = consumer.acknowledge_with_record(&any).expect_err("implicit mode");
        assert!(matches!(e, Error::LocalIllegalState(_)), "{e:?}");
        assert_eq!("Implicit acknowledgement of delivery is being used.", e.message());
        let e = consumer
            .acknowledge_with_options(acknowledge_options("topic-a", 0, 0, AcknowledgeType::Release))
            .expect_err("implicit mode");
        assert_eq!("Implicit acknowledgement of delivery is being used.", e.message());
        consumer.close_with_timeout(Duration::ZERO).await.expect("closed");
    }

    // ── Rust-only: the methods without a Java test ─────────────────────────

    fn acknowledge_options(topic: &str, partition: i32, offset: i64, r#type: AcknowledgeType) -> AcknowledgeOptions {
        crate::consumer::AcknowledgeOptionsBuilder::new()
            .set_topic(topic)
            .set_partition(partition)
            .set_offset(offset)
            .set_type(r#type)
            .build()
            .expect("every parameter is set")
    }

    #[tokio::test]
    async fn acknowledge_by_offset_needs_a_failed_delivery() {
        let mut f = new_consumer_mode("explicit");
        let e = f
            .consumer
            .acknowledge_with_options(acknowledge_options("t", 0, 5, AcknowledgeType::Reject))
            .expect_err("no batch failed");
        assert_eq!("The record cannot be acknowledged.", e.message());
    }

    #[tokio::test]
    async fn poll_without_subscription_fails() {
        let mut f = new_consumer();
        let e = f.poll(Duration::ZERO).await.expect_err("not subscribed");
        assert_eq!("Consumer is not subscribed to any topics.", e.message());
    }

    #[tokio::test]
    async fn closed_consumer_rejects_the_guarded_calls_and_close_again_is_a_no_op() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.close().await.expect("closed");
        let any = ConsumerRecord::new("t", 0, 0, None::<String>, None::<String>);
        for e in [
            f.subscribe(&["t"]).await.expect_err("closed"),
            drive(&mut f.responder, f.consumer.unsubscribe()).await.expect_err("closed"),
            f.poll(Duration::ZERO).await.expect_err("closed"),
            drive(&mut f.responder, f.consumer.commit_sync()).await.expect_err("closed"),
            drive(&mut f.responder, f.consumer.commit_async()).await.expect_err("closed"),
            f.consumer.acknowledge_with_record(&any).expect_err("closed"),
        ] {
            assert_eq!("This consumer has already been closed.", e.message());
        }
        let events = f.responder.events.len();
        f.close().await.expect("a second close does nothing");
        assert_eq!(events, f.responder.events.len());
        // Not guarded, as in Java.
        f.consumer.wakeup();
        assert!(!f.consumer.metrics().snapshot().is_empty());
        let e = f.consumer.client_instance_id(Duration::ZERO).expect_err("no telemetry");
        assert_eq!(
            "Telemetry is not enabled. Set config `enable.metrics.push` to `true`.",
            e.message()
        );
    }

    #[tokio::test]
    async fn commit_sync_sends_the_implicit_acknowledgements_and_returns_their_result() {
        let mut f = new_consumer();
        let tip = new_tip("t", 0);
        f.stub_collect_once(fetch_with_records(&tip, &[0, 1]));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        f.poll(Duration::ZERO).await.expect("records");

        let result = drive(&mut f.responder, f.consumer.commit_sync()).await.expect("committed");
        assert!(result.is_empty(), "the responder completes with no partition");
        assert_eq!(
            vec![(
                "ShareAcknowledgeSync",
                vec![(0, Some(AcknowledgeType::Accept)), (1, Some(AcknowledgeType::Accept))]
            )],
            f.responder.acknowledgements_sent(&tip)
        );
        // Nothing is left to commit.
        let result = drive(&mut f.responder, f.consumer.commit_sync()).await.expect("committed");
        assert!(result.is_empty());
        assert_eq!(1, f.responder.count("ShareAcknowledgeSync"));
    }

    #[tokio::test]
    async fn commit_sync_is_woken_up() {
        let mut f = new_consumer();
        let tip = new_tip("t", 0);
        f.stub_collect_once(fetch_with_records(&tip, &[0]));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        f.poll(Duration::ZERO).await.expect("records");
        f.consumer.wakeup();
        // The responder does not run: the event stays pending until the wakeup.
        let e = f.consumer.commit_sync().await.expect_err("woken up");
        assert!(matches!(e, Error::Wakeup(_)), "{e:?}");
    }

    /// Records each entry the callback is called with.
    struct RecordingCallback {
        calls: Arc<Mutex<Vec<TopicIdPartition>>>,
        /// Whether the first call stays pending once (a dropped caller).
        pend_once: AtomicBool,
    }

    #[async_trait]
    impl AcknowledgementCommitCallback for RecordingCallback {
        async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, _error: Option<&Error>) {
            self.calls.lock().unwrap().extend(offsets.keys().cloned());
            if self.pend_once.swap(false, Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        }
    }

    fn completed_event(tip: &TopicIdPartition, offset: i64) -> ShareAcknowledgementEvent {
        let mut acks = Acknowledgements::empty();
        acks.add(offset, Some(AcknowledgeType::Accept));
        acks.complete(None);
        ShareAcknowledgementEvent::new(HashMap::from([(tip.clone(), acks)]), false, None)
    }

    /// Rust-only: `commit_async` runs the callback, once per completed entry,
    /// and the entries are gone after it.
    #[tokio::test]
    async fn commit_async_runs_the_callback_once_per_entry() {
        let mut f = new_consumer();
        let calls = Arc::new(Mutex::new(Vec::new()));
        f.consumer.set_acknowledgement_commit_callback(Some(Arc::new(RecordingCallback {
            calls: Arc::clone(&calls),
            pend_once: AtomicBool::new(false),
        })));
        let (tip_a, tip_b) = (new_tip("a", 0), new_tip("b", 0));
        f.acknowledgement_event_handler.add(completed_event(&tip_a, 0));
        f.acknowledgement_event_handler.add(completed_event(&tip_b, 0));
        drive(&mut f.responder, f.consumer.commit_async()).await.expect("committed");
        assert_eq!(vec![tip_a, tip_b], *calls.lock().unwrap());
        drive(&mut f.responder, f.consumer.commit_async()).await.expect("committed");
        assert_eq!(2, calls.lock().unwrap().len(), "each entry once");
    }

    // ── Rust-only: a dropped call (module doc, translation-checklist R4 item 8) ──

    /// A dropped `commit_async` (or `poll`, `commit_sync`, `close`) that was
    /// running the callback for one entry neither calls that entry again nor
    /// loses the next one.
    #[tokio::test]
    async fn dropped_call_in_the_callback_neither_repeats_nor_loses_an_entry() {
        let mut f = new_consumer();
        let calls = Arc::new(Mutex::new(Vec::new()));
        f.consumer.set_acknowledgement_commit_callback(Some(Arc::new(RecordingCallback {
            calls: Arc::clone(&calls),
            pend_once: AtomicBool::new(true),
        })));
        let (tip_a, tip_b) = (new_tip("a", 0), new_tip("b", 0));
        f.acknowledgement_event_handler.add(completed_event(&tip_a, 0));
        f.acknowledgement_event_handler.add(completed_event(&tip_b, 0));

        assert!(
            f.consumer.commit_async().now_or_never().is_none(),
            "dropped in the first callback"
        );
        assert_eq!(vec![tip_a.clone()], *calls.lock().unwrap());

        drive(&mut f.responder, f.consumer.commit_async()).await.expect("committed");
        assert_eq!(
            vec![tip_a, tip_b],
            *calls.lock().unwrap(),
            "the next entry once, the first not again"
        );
    }

    /// A `poll` dropped while a deserializer is pending loses no record: the
    /// next `poll` returns every record of the fetch, the one being decoded
    /// included, none of them acknowledged before; the poll after it accepts
    /// them.
    #[tokio::test]
    async fn dropped_poll_mid_deserialize_returns_every_record_on_the_next_poll() {
        let value = PendOnceDeserializer::default();
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "group-id",
            "implicit",
            Box::new(value.clone()),
        );
        let tip = tip();
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&[tip.topic()]).await.expect("subscribed");
        f.fetch_buffer.add([new_share_completed_fetch(
            &tip,
            partition_data(Some(new_records(0, 3)), acquired_records(0, 3)),
        )]);
        value.pend_on.store(2, Ordering::SeqCst);

        assert!(f.consumer.poll(Duration::ZERO).now_or_never().is_none(), "dropped on offset 1");
        f.responder.drain();
        assert!(f.consumer.collect_state.has_records(), "offset 0 is kept");

        let records = f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(vec![0, 1, 2], offsets(&records));
        assert!(
            f.responder.acknowledgements_sent(&tip).is_empty(),
            "nothing acknowledged before the records were returned"
        );

        f.stub_collect(ShareFetch::empty);
        f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(
            vec![(
                "ShareFetch",
                vec![
                    (0, Some(AcknowledgeType::Accept)),
                    (1, Some(AcknowledgeType::Accept)),
                    (2, Some(AcknowledgeType::Accept))
                ]
            )],
            f.responder.acknowledgements_sent(&tip)
        );
    }

    /// A `poll` dropped after it took records into the current fetch, while
    /// the acknowledgement commit callback ran, hands them out on the next
    /// `poll`; neither that `poll` nor a `commit_async` before it accepts
    /// them.
    #[tokio::test]
    async fn dropped_poll_in_the_callback_hands_the_records_out_on_the_next_poll() {
        let mut f = new_consumer();
        let calls = Arc::new(Mutex::new(Vec::new()));
        f.consumer.set_acknowledgement_commit_callback(Some(Arc::new(RecordingCallback {
            calls: Arc::clone(&calls),
            pend_once: AtomicBool::new(true),
        })));
        let (tip, tip_done) = (new_tip("t", 0), new_tip("done", 0));
        let acknowledgement_event_handler = Arc::clone(&f.acknowledgement_event_handler);
        let (tip_in_stub, tip_done_in_stub) = (tip.clone(), tip_done.clone());
        let mut first = true;
        f.stub_collect(move || {
            if !std::mem::replace(&mut first, false) {
                return ShareFetch::empty();
            }
            // Completes while the poll collects, so its callback runs after
            // the records are taken into the current fetch.
            acknowledgement_event_handler.add(completed_event(&tip_done_in_stub, 7));
            fetch_with_records(&tip_in_stub, &[0, 1])
        });
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");

        assert!(
            f.consumer.poll(Duration::ZERO).now_or_never().is_none(),
            "dropped in the callback"
        );
        assert!(f.consumer.current_fetch.has_records_not_handed_out());
        drive(&mut f.responder, f.consumer.commit_async()).await.expect("committed");
        assert!(
            f.responder.acknowledgements_sent(&tip).is_empty(),
            "commit_async accepts nothing"
        );

        let records = f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(vec![0, 1], offsets(&records));
        assert!(f.responder.acknowledgements_sent(&tip).is_empty(), "nothing accepted yet");
        assert_eq!(vec![tip_done], *calls.lock().unwrap(), "the callback ran once");

        f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(
            vec![(
                "ShareFetch",
                vec![(0, Some(AcknowledgeType::Accept)), (1, Some(AcknowledgeType::Accept))]
            )],
            f.responder.acknowledgements_sent(&tip)
        );
    }

    /// The explicit-mode twin of the test above: the explicit-mode check
    /// skips the records the dropped `poll` did not hand out, so the next
    /// `poll` returns them instead of failing with "All records must be
    /// acknowledged in explicit acknowledgement mode.".
    #[tokio::test]
    async fn dropped_poll_in_the_callback_hands_the_records_out_in_explicit_mode() {
        let mut f = new_consumer_mode("explicit");
        f.consumer.set_acknowledgement_commit_callback(Some(Arc::new(RecordingCallback {
            calls: Arc::new(Mutex::new(Vec::new())),
            pend_once: AtomicBool::new(true),
        })));
        let (tip, tip_done) = (new_tip("t", 0), new_tip("done", 0));
        let acknowledgement_event_handler = Arc::clone(&f.acknowledgement_event_handler);
        let (tip_in_stub, tip_done_in_stub) = (tip.clone(), tip_done.clone());
        let mut first = true;
        f.stub_collect(move || {
            if !std::mem::replace(&mut first, false) {
                return ShareFetch::empty();
            }
            acknowledgement_event_handler.add(completed_event(&tip_done_in_stub, 7));
            fetch_with_records(&tip_in_stub, &[0, 1])
        });
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");

        assert!(
            f.consumer.poll(Duration::ZERO).now_or_never().is_none(),
            "dropped in the callback"
        );
        let records = f
            .poll(Duration::ZERO)
            .await
            .expect("the records not handed out need no acknowledgement");
        assert_eq!(vec![0, 1], offsets(&records));
        let e = f.poll(Duration::ZERO).await.expect_err("now they do");
        assert_eq!(
            "All records must be acknowledged in explicit acknowledgement mode.",
            e.message()
        );
    }

    /// Java 4.4 (PLAN §8.1 row 17): when the collector fails, the
    /// acknowledgements `pollForFetches` took (here the implicit ACCEPTs of
    /// the records the first `poll` returned) are not sent.
    #[tokio::test]
    async fn collector_failure_does_not_send_the_acknowledgements_taken() {
        let mut f = new_consumer();
        let tip = new_tip("t", 0);
        let tip_in_stub = tip.clone();
        let mut calls = 0;
        f.consumer.collect_stub = Some(Box::new(move || {
            calls += 1;
            match calls {
                1 => Ok(Ok(fetch_with_records(&tip_in_stub, &[0, 1]))),
                _ => Err(Error::topic_authorization(HashSet::from(["t".to_string()]))),
            }
        }));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        let records = f.poll(Duration::ZERO).await.expect("records");
        assert_eq!(vec![0, 1], offsets(&records));

        let e = f.poll(Duration::ZERO).await.expect_err("the collector fails");
        assert!(matches!(e, Error::TopicAuthorization(_)), "{e:?}");
        assert!(
            f.responder.acknowledgements_sent(&tip).is_empty(),
            "{:?}",
            f.responder.acknowledgements_sent(&tip)
        );
    }

    /// A `close` dropped while it waits for the acknowledge-on-close event
    /// leaves the consumer open; the next `close` finishes it and stops the
    /// background task, which then lets go of the request managers.
    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_close_is_finished_by_the_next_close() {
        let time = Arc::new(MockTime::with_auto_tick_ms(1));
        let mut consumer = new_share_consumer_over_mock_client(time);
        let request_managers = Arc::clone(consumer.request_managers.as_ref().expect("built"));
        let held_by_the_consumer = 2;
        assert!(
            Arc::strong_count(&request_managers) > held_by_the_consumer,
            "the task holds them"
        );

        assert!(
            consumer.close_with_timeout(Duration::from_secs(5)).now_or_never().is_none(),
            "dropped while it waits for the acknowledge-on-close event"
        );
        assert_eq!(HashSet::<String>::new(), consumer.subscription(), "the consumer is still open");

        consumer
            .close_with_timeout(Duration::from_secs(5))
            .await
            .expect("the second close finishes");
        assert_eq!(
            held_by_the_consumer,
            Arc::strong_count(&request_managers),
            "the background task has stopped"
        );
        let e = consumer.unsubscribe().await.expect_err("closed");
        assert_eq!("This consumer has already been closed.", e.message());
    }

    /// A `close` dropped after it joined the background task (here while a
    /// deserializer closes) leaves the consumer open; the next `close` sends
    /// nothing to the stopped task and succeeds.
    #[tokio::test]
    async fn dropped_close_after_the_join_is_finished_by_the_next_close() {
        #[derive(Clone, Default)]
        struct PendOnceOnClose {
            entered: Arc<AtomicBool>,
        }
        impl Deserializer<String> for PendOnceOnClose {
            async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
                Ok(String::from_utf8_lossy(data).into_owned())
            }
            async fn close(&mut self) -> Result<(), Error> {
                if !self.entered.swap(true, Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            }
        }
        let deserializer = PendOnceOnClose::default();
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "group-id",
            "implicit",
            Box::new(deserializer.clone()),
        );
        f.complete_close_events();
        {
            let mut close = std::pin::pin!(f.consumer.close());
            while !deserializer.entered.load(Ordering::SeqCst) {
                assert!(futures_util::poll!(close.as_mut()).is_pending(), "dropped before it ends");
                f.responder.drain();
                tokio::task::yield_now().await;
            }
        }
        let events = f.responder.events.len();
        assert_eq!(1, f.responder.count("ShareUnsubscribe"));

        f.close().await.expect("the second close finishes");
        assert_eq!(events, f.responder.events.len(), "nothing is sent to the stopped task");
        let e = f.poll(Duration::ZERO).await.expect_err("closed");
        assert_eq!("This consumer has already been closed.", e.message());
    }

    /// A `close` dropped while the acknowledge-on-close event is in flight
    /// does not make the retried `close` send another one (the background
    /// task's second `acknowledgeOnClose` would fail the close requests in
    /// flight and report their acknowledgements twice): the retry awaits the
    /// same event, succeeds, and each acknowledgement gets one callback with
    /// its result.
    #[tokio::test]
    async fn dropped_close_does_not_send_the_acknowledge_on_close_again() {
        let mut f = new_consumer_mode("explicit");
        let calls = Arc::new(Mutex::new(Vec::new()));
        f.consumer
            .set_acknowledgement_commit_callback(Some(Arc::new(ResultRecordingCallback { calls: Arc::clone(&calls) })));
        let tip = new_tip("t", 0);
        f.stub_collect_once(fetch_with_records(&tip, &[0, 1]));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        let records = f.poll(Duration::ZERO).await.expect("records");
        let mut iterator = records.into_iter();
        f.consumer
            .acknowledge_with_record_type(&iterator.next().expect("0"), AcknowledgeType::Accept)
            .expect("in flight");
        f.consumer
            .acknowledge_with_record_type(&iterator.next().expect("1"), AcknowledgeType::Reject)
            .expect("in flight");
        f.complete_close_events();
        f.responder.hold_share_acknowledge_on_close = true;

        {
            let mut close = std::pin::pin!(f.consumer.close());
            while f.responder.held_share_acknowledge_on_close.is_none() {
                assert!(futures_util::poll!(close.as_mut()).is_pending());
                f.responder.drain();
            }
            assert!(futures_util::poll!(close.as_mut()).is_pending(), "it waits for the event");
        }
        assert_eq!(
            vec![(
                "ShareAcknowledgeOnClose",
                vec![(0, Some(AcknowledgeType::Accept)), (1, Some(AcknowledgeType::Reject))]
            )],
            f.responder.acknowledgements_sent(&tip)
        );

        // The background task completes the close acknowledgements and the
        // event; a second event would be completed at once (and counted).
        f.responder.hold_share_acknowledge_on_close = false;
        let mut acks = Acknowledgements::empty();
        acks.add(0, Some(AcknowledgeType::Accept));
        acks.add(1, Some(AcknowledgeType::Reject));
        acks.complete(None);
        f.acknowledgement_event_handler.add(ShareAcknowledgementEvent::new(
            HashMap::from([(tip.clone(), acks)]),
            false,
            None,
        ));
        f.responder.held_share_acknowledge_on_close.take().expect("held").complete(());

        f.close().await.expect("the retried close succeeds");
        assert_eq!(1, f.responder.count("ShareAcknowledgeOnClose"), "sent once");
        assert_eq!(
            vec![(tip, vec![0, 1], None)],
            *calls.lock().unwrap(),
            "one callback, with the result"
        );
    }

    /// A `close` dropped while it joins the background task, which it has
    /// already told to stop, is retried with the join only: no event is sent
    /// again, and the retry succeeds.
    #[tokio::test]
    async fn dropped_close_at_the_join_only_joins_on_retry() {
        let mut f = new_consumer();
        f.complete_close_events();
        let signalled = Arc::new(AtomicBool::new(false));
        let signalled_in_fn = Arc::clone(&signalled);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        f.consumer.network_thread_close = NetworkThreadCloseHandle::new(
            Box::new(move |_| signalled_in_fn.store(true, Ordering::SeqCst)),
            Box::new(|| {}),
            tokio::spawn(async move {
                let _ = stop_rx.await;
            }),
        );

        {
            let mut close = std::pin::pin!(f.consumer.close());
            while !signalled.load(Ordering::SeqCst) {
                assert!(futures_util::poll!(close.as_mut()).is_pending());
                f.responder.drain();
            }
            assert!(futures_util::poll!(close.as_mut()).is_pending(), "it waits for the join");
        }
        f.responder.drain();
        let counts = |r: &Responder| {
            [
                r.count("ShareAcknowledgeOnClose"),
                r.count("ShareUnsubscribe"),
                r.count("StopFindCoordinatorOnClose"),
            ]
        };
        assert_eq!([1, 1, 1], counts(&f.responder));
        signalled.store(false, Ordering::SeqCst);
        stop_tx.send(()).expect("the task waits");

        f.close().await.expect("the retried close succeeds");
        assert_eq!([1, 1, 1], counts(&f.responder), "no event step runs again");
        assert!(!signalled.load(Ordering::SeqCst), "the task is not told to stop again");
    }

    /// A failure recorded by a `close` dropped at the join (a background
    /// error `close` does not ignore) is returned by the `close` that resumes
    /// it, as one undropped `close` returns it.
    #[tokio::test]
    async fn dropped_close_keeps_its_first_failure_for_the_retry() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.responder.on_share_unsubscribe = Some(queue_background_error(&f, || {
            Some(Error::kafka_message("not ignored on close"))
        }));
        let signalled = Arc::new(AtomicBool::new(false));
        let signalled_in_fn = Arc::clone(&signalled);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        f.consumer.network_thread_close = NetworkThreadCloseHandle::new(
            Box::new(move |_| signalled_in_fn.store(true, Ordering::SeqCst)),
            Box::new(|| {}),
            tokio::spawn(async move {
                let _ = stop_rx.await;
            }),
        );

        {
            let mut close = std::pin::pin!(f.consumer.close());
            while !signalled.load(Ordering::SeqCst) {
                assert!(futures_util::poll!(close.as_mut()).is_pending());
                f.responder.drain();
            }
        }
        stop_tx.send(()).expect("the task waits");

        let e = f.close().await.expect_err("the dropped call's failure");
        assert!(e.is_kafka_error(), "{e:?}");
        assert_eq!("Failed to close Kafka share consumer", e.message());
        assert_eq!("not ignored on close", source_of(&e).message());
    }

    /// A `close` resuming a dropped one waits for the acknowledge-on-close
    /// that one sent only up to its own timeout: with a zero timeout it
    /// returns at once, though the event is still in flight.
    #[tokio::test]
    async fn resumed_close_waits_for_the_acknowledge_on_close_within_its_timeout() {
        let mut f = new_consumer();
        f.complete_close_events();
        f.responder.hold_share_acknowledge_on_close = true;
        {
            let mut close = std::pin::pin!(f.consumer.close_with_timeout(Duration::from_secs(30)));
            while f.responder.held_share_acknowledge_on_close.is_none() {
                assert!(futures_util::poll!(close.as_mut()).is_pending());
                f.responder.drain();
            }
        }

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            drive(&mut f.responder, f.consumer.close_with_timeout(Duration::ZERO)),
        )
        .await
        .expect("the retry is bounded by its own timeout");
        result.expect("a timed-out acknowledge-on-close is not a close failure");
        assert!(f.responder.held_share_acknowledge_on_close.is_some(), "still in flight");
        assert_eq!(1, f.responder.count("ShareAcknowledgeOnClose"));
    }

    /// A callback's partition, sorted offsets and error message.
    type CallbackCall = (TopicIdPartition, Vec<i64>, Option<String>);

    /// Records each callback's partition, offsets and error message.
    struct ResultRecordingCallback {
        calls: Arc<Mutex<Vec<CallbackCall>>>,
    }

    #[async_trait]
    impl AcknowledgementCommitCallback for ResultRecordingCallback {
        async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&Error>) {
            for (tip, offsets) in offsets {
                let mut offsets: Vec<i64> = offsets.iter().copied().collect();
                offsets.sort_unstable();
                self.calls
                    .lock()
                    .unwrap()
                    .push((tip.clone(), offsets, error.map(|e| e.message().to_string())));
            }
        }
    }

    /// A `poll` dropped while it collects sends the acknowledgements it had
    /// taken (the previous records, accepted implicitly) in a
    /// `ShareAcknowledgeAsyncEvent`, and the next `poll` returns the new
    /// records.
    #[tokio::test]
    async fn dropped_poll_sends_the_acknowledgements_it_took() {
        let value = PendOnceDeserializer::default();
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "group-id",
            "implicit",
            Box::new(value.clone()),
        );
        let tip = tip();
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&[tip.topic()]).await.expect("subscribed");
        f.fetch_buffer.add([new_share_completed_fetch(
            &tip,
            partition_data(Some(new_records(0, 2)), acquired_records(0, 2)),
        )]);
        let records = f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(vec![0, 1], offsets(&records));

        f.fetch_buffer.add([new_share_completed_fetch(
            &tip,
            partition_data(Some(new_records(2, 2)), acquired_records(2, 2)),
        )]);
        value.pend_on.store(value.calls.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        assert!(f.consumer.poll(Duration::ZERO).now_or_never().is_none(), "dropped on offset 2");
        f.responder.drain();
        assert_eq!(
            vec![(
                "ShareAcknowledgeAsync",
                vec![(0, Some(AcknowledgeType::Accept)), (1, Some(AcknowledgeType::Accept))]
            )],
            f.responder.acknowledgements_sent(&tip),
            "the acknowledgements the dropped poll took are sent"
        );

        let records = f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(vec![2, 3], offsets(&records));
        assert_eq!(1, f.responder.acknowledgements_sent(&tip).len(), "nothing else acknowledged");
    }

    /// A `poll` dropped while it redelivers renewed records (decision D5)
    /// accepts none of them: the next `poll` returns both, the one redelivered
    /// before the drop and the one after.
    #[tokio::test]
    async fn dropped_poll_mid_redelivery_returns_the_renewed_records_on_the_next_poll() {
        let value = PendOnceDeserializer::default();
        let mut f = new_consumer_with(
            Arc::new(MockTime::with_auto_tick_ms(1)),
            "group-id",
            "implicit",
            Box::new(value.clone()),
        );
        let tip = tip();
        let mut completed_fetch =
            new_share_completed_fetch(&tip, partition_data(Some(new_records(0, 2)), acquired_records(0, 2)));
        let mut batch = fetch(&mut completed_fetch, &new_string_deserializers(), 10, true);
        let delivered = batch.in_flight_records();
        for record in &delivered {
            batch.acknowledge(record, AcknowledgeType::Renew).expect("in flight");
        }
        let mut renewing = batch.take_acknowledged_records();
        renewing.complete(None);
        let mut renewed_fetch = ShareFetch::empty();
        renewed_fetch.add(tip.clone(), batch);
        renewed_fetch
            .renew(&HashMap::from([(tip.clone(), renewing)]), None)
            .expect("completed");
        f.consumer.current_fetch = renewed_fetch;
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&[tip.topic()]).await.expect("subscribed");
        value.pend_on.store(value.calls.load(Ordering::SeqCst) + 2, Ordering::SeqCst);

        assert!(f.consumer.poll(Duration::ZERO).now_or_never().is_none(), "dropped on offset 1");
        assert!(f.consumer.current_fetch.has_records_not_handed_out(), "offset 0 is redelivered");

        let records = f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(vec![0, 1], offsets(&records));
        assert!(f.responder.acknowledgements_sent(&tip).iter().all(|(_, acks)| acks.is_empty()));

        f.stub_collect(ShareFetch::empty);
        f.poll(Duration::ZERO).await.expect("poll succeeds");
        assert_eq!(
            vec![(0, Some(AcknowledgeType::Accept)), (1, Some(AcknowledgeType::Accept))],
            f.responder
                .acknowledgements_sent(&tip)
                .into_iter()
                .flat_map(|(_, acks)| acks)
                .collect::<Vec<_>>()
        );
    }

    /// A `commit_sync` dropped while it waits has sent its acknowledgements;
    /// they are not sent again.
    #[tokio::test]
    async fn dropped_commit_sync_keeps_the_acknowledgements_it_sent() {
        let mut f = new_consumer();
        let tip = new_tip("t", 0);
        f.stub_collect_once(fetch_with_records(&tip, &[0]));
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        f.poll(Duration::ZERO).await.expect("records");

        assert!(f.consumer.commit_sync().now_or_never().is_none(), "dropped while waiting");
        drive(&mut f.responder, f.consumer.commit_async()).await.expect("committed");
        assert_eq!(
            vec![("ShareAcknowledgeSync", vec![(0, Some(AcknowledgeType::Accept))])],
            f.responder.acknowledgements_sent(&tip)
        );
    }

    /// Translation-checklist R4 item 6: with a zero wait, the `poll` loop
    /// yields each time round, so a task on the same `current_thread` runtime
    /// runs (here, the one that moves the clock past the deadline; without the
    /// yield the test never ends).
    #[tokio::test(flavor = "current_thread")]
    async fn poll_loop_yields_on_a_current_thread_runtime() {
        let time = Arc::new(MockTime::new());
        let mut f = new_consumer_with(
            Arc::clone(&time) as Arc<dyn Time>,
            "group-id",
            "implicit",
            Box::new(StringDeserializer::new()),
        );
        f.stub_collect(ShareFetch::empty);
        f.responder.complete_share_subscription_change = true;
        f.subscribe(&["t"]).await.expect("subscribed");
        let clock = tokio::spawn(async move {
            time.sleep(1_000);
        });
        let records = f.poll(Duration::from_millis(100)).await.expect("poll succeeds");
        assert!(records.is_empty());
        clock.await.expect("the clock task ran");
    }

    /// `ShareConsumerImpl` and its futures cross tasks.
    #[test]
    fn share_consumer_impl_and_its_futures_are_send() {
        fn assert_send<T: Send>(_: &T) {}
        fn check(consumer: &mut ShareConsumerImpl<String, String>) {
            assert_send(&consumer.poll(Duration::ZERO));
            assert_send(&consumer.commit_sync());
            assert_send(&consumer.commit_async());
            assert_send(&consumer.subscribe(Vec::new()));
            assert_send(&consumer.unsubscribe());
            assert_send(&consumer.close());
        }
        let _ = check as fn(&mut ShareConsumerImpl<String, String>);
        fn is_send<T: Send>() {}
        is_send::<ShareConsumerImpl<String, String>>();
    }
}
