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

//! Backend-agnostic *observation* of the three user callbacks — producer
//! delivery, `OffsetCommitCallback`, and `ConsumerRebalanceListener` — for the
//! multilanguage integration tests.
//!
//! # Why this is not on the `Consumer` / `Producer` trait
//!
//! For the native Rust backend a test could simply pass an
//! `Arc<dyn ConsumerRebalanceListener>` to `subscribe_with_topics_listener`. For the
//! gRPC backends it cannot: the callback must be registered *by the server's own
//! binding* (that is the thing under test), so it fires in another process and
//! nothing can be handed across the wire. `MultilanguageConsumer`'s
//! `subscribe_with_topics_listener` / `commit_async_*_with_callback` therefore stay
//! `unsupported` — synthesizing local listener invocations out of a remote log
//! would re-test the Rust core rather than the binding, and would be timing
//! fragile.
//!
//! Instead each backend records what its callbacks actually saw into a
//! **callback log**, and the harness reads it back:
//!
//!   - native  — an in-process `Arc<Mutex<Vec<CallbackLogEntry>>>` appended to
//!     by real `ConsumerRebalanceListener` / `OffsetCommitCallback` / `Callback`
//!     implementations,
//!   - python / python_async / c / dotnet / dotnet_async — the server-side
//!     per-client log, read with the `GetCallbackLog` unary RPC.
//!
//! Both produce the same [`CallbackLogEntry`] shape, so one generic test body
//! asserts the same thing against all six backends.
//!
//! The log is *eventually consistent*: a callback fires when the client next
//! makes progress (a later `poll`/`commit`/`close`, per
//! `consumer-threading.md` §31), and for the gRPC backends an extra RPC hop
//! later. Tests must poll-until-present with a deadline rather than assert
//! immediately — see `poll_until_kind` in the integration tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{Consumer, ConsumerRebalanceListener, OffsetAndMetadata, OffsetCommitCallback};
use confluent_kafka::producer::{Producer, ProducerRecord, RecordMetadata};

/// `kind` value for `ConsumerRebalanceListener::on_partitions_assigned`.
pub const KIND_ASSIGNED: &str = "assigned";
/// `kind` value for `ConsumerRebalanceListener::on_partitions_revoked`.
pub const KIND_REVOKED: &str = "revoked";
/// `kind` value for `ConsumerRebalanceListener::on_partitions_lost`.
pub const KIND_LOST: &str = "lost";
/// `kind` value for `OffsetCommitCallback::on_complete`.
pub const KIND_COMMIT: &str = "commit";
/// `kind` value for a producer delivery callback.
pub const KIND_DELIVERY: &str = "delivery";

/// One recorded user-callback invocation, normalized across backends.
///
/// Mirrors the proto `CallbackLogEntry` (see `producer_service.proto` for the
/// per-`kind` field encoding, which every server must follow).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CallbackLogEntry {
    /// One of the `KIND_*` constants.
    pub kind: String,
    /// The partitions the callback was invoked with, in the order the backend
    /// reported them (rebalance callbacks are unordered — sort before
    /// comparing).
    pub partitions: Vec<(String, i32)>,
    /// `"<topic>-<partition>"` -> offset. Empty for the rebalance kinds.
    pub offsets: HashMap<String, i64>,
    /// The callback's error message; empty when the callback saw no error.
    pub error: String,
}

impl CallbackLogEntry {
    /// The `offsets` key this module (and every server) uses for `partition`.
    pub fn offset_key(topic: &str, partition: i32) -> String {
        format!("{topic}-{partition}")
    }

    /// Whether `partitions` contains `(topic, partition)`.
    pub fn has_partition(&self, topic: &str, partition: i32) -> bool {
        self.partitions.iter().any(|(t, p)| t == topic && *p == partition)
    }

    /// The logged offset for `(topic, partition)`, if any.
    pub fn offset_for(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.get(&Self::offset_key(topic, partition)).copied()
    }
}

/// The in-process log the native backend's callbacks append to.
type SharedLog = Arc<Mutex<Vec<CallbackLogEntry>>>;

fn push(log: &SharedLog, entry: CallbackLogEntry) {
    log.lock().expect("callback log poisoned").push(entry);
}

fn rebalance_entry(kind: &str, partitions: &[TopicPartition]) -> CallbackLogEntry {
    CallbackLogEntry {
        kind: kind.to_string(),
        partitions: partitions.iter().map(|tp| (tp.topic().to_string(), tp.partition())).collect(),
        offsets: HashMap::new(),
        error: String::new(),
    }
}

// ---------------------------------------------------------------------------
// Native (in-process) callback implementations
// ---------------------------------------------------------------------------

/// A real `ConsumerRebalanceListener` that records each invocation.
///
/// `on_partitions_lost` is implemented explicitly (rather than inheriting the
/// trait's Java-faithful "delegate to revoked" default) so a lost callback is
/// distinguishable from a revoke in the log — otherwise the two kinds could not
/// be told apart, and `unsubscribe`/fence paths fire `lost`.
struct LoggingRebalanceListener {
    log: SharedLog,
}

#[async_trait]
impl ConsumerRebalanceListener for LoggingRebalanceListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        push(&self.log, rebalance_entry(KIND_REVOKED, partitions));
        Ok(())
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        push(&self.log, rebalance_entry(KIND_ASSIGNED, partitions));
        Ok(())
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        push(&self.log, rebalance_entry(KIND_LOST, partitions));
        Ok(())
    }
}

/// A real `OffsetCommitCallback` that records the committed offsets.
struct LoggingCommitCallback {
    log: SharedLog,
}

#[async_trait]
impl OffsetCommitCallback for LoggingCommitCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        let mut entry = CallbackLogEntry {
            kind: KIND_COMMIT.to_string(),
            partitions: Vec::with_capacity(offsets.len()),
            offsets: HashMap::with_capacity(offsets.len()),
            error: error.map(|e| e.to_string()).unwrap_or_default(),
        };
        for (tp, oam) in offsets {
            entry.partitions.push((tp.topic().to_string(), tp.partition()));
            entry
                .offsets
                .insert(CallbackLogEntry::offset_key(tp.topic(), tp.partition()), oam.offset());
        }
        push(&self.log, entry);
    }
}

fn delivery_entry(metadata: Option<&RecordMetadata>, error: Option<&Error>) -> CallbackLogEntry {
    let mut entry = CallbackLogEntry {
        kind: KIND_DELIVERY.to_string(),
        error: error.map(|e| e.to_string()).unwrap_or_default(),
        ..Default::default()
    };
    if let Some(m) = metadata {
        entry.partitions.push((m.topic().to_string(), m.partition()));
        entry
            .offsets
            .insert(CallbackLogEntry::offset_key(m.topic(), m.partition()), m.offset());
    }
    entry
}

// ---------------------------------------------------------------------------
// ConsumerCallbackLog
// ---------------------------------------------------------------------------

/// Per-consumer handle used to register logging callbacks and read the log back.
///
/// Obtained from [`crate::common::backend_factory::ConsumerBackendFactory::create_with_callback_log`]
/// (the factory is the only place that has both the in-process pieces the native
/// backend needs and the channel + server-side id the gRPC backends need —
/// `Box<dyn Consumer>` erases all of it).
pub enum ConsumerCallbackLog {
    /// In-process log written by real Rust callbacks.
    Native(SharedLog),
    /// Server-side log read over `GetCallbackLog`.
    #[cfg(feature = "multilanguage-tests")]
    Grpc(grpc::ConsumerLog),
}

impl ConsumerCallbackLog {
    /// A fresh in-process log for the native backend.
    pub fn native() -> Self {
        ConsumerCallbackLog::Native(Arc::new(Mutex::new(Vec::new())))
    }

    /// Subscribe to `topics` with a rebalance listener that appends
    /// `"assigned"` / `"revoked"` / `"lost"` entries to this log.
    ///
    /// The gRPC arm deliberately bypasses `consumer` and issues its own
    /// `Subscribe` RPC with `with_listener = true` (same server-side consumer
    /// id), because the trait method is `unsupported` there — see the module
    /// docs. `consumer` is still taken so the two arms have one signature and
    /// so the native arm can reach the real trait method.
    ///
    /// Registration is per-subscribe: calling this again re-registers a listener
    /// writing to the same log, while a plain `consumer.subscribe_with_topics(topics)`
    /// releases it.
    pub async fn subscribe_with_logging_listener(
        &self,
        consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
        topics: Vec<String>,
    ) -> Result<(), Error> {
        match self {
            ConsumerCallbackLog::Native(log) => {
                let listener = Arc::new(LoggingRebalanceListener { log: Arc::clone(log) });
                consumer.subscribe_with_topics_listener(topics, listener).await
            },
            #[cfg(feature = "multilanguage-tests")]
            ConsumerCallbackLog::Grpc(remote) => remote.subscribe_with_listener(topics).await,
        }
    }

    /// Initiate an asynchronous commit of the current positions with a
    /// completion callback that appends a `"commit"` entry to this log.
    pub async fn commit_async_with_logging_callback(
        &self,
        consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    ) -> Result<(), Error> {
        match self {
            ConsumerCallbackLog::Native(log) => {
                let callback = Arc::new(LoggingCommitCallback { log: Arc::clone(log) });
                consumer.commit_async_with_callback(callback).await
            },
            #[cfg(feature = "multilanguage-tests")]
            ConsumerCallbackLog::Grpc(remote) => remote.commit_async_with_callback().await,
        }
    }

    /// Snapshot the log, oldest entry first. Does not clear it.
    pub async fn entries(&self) -> Result<Vec<CallbackLogEntry>, Error> {
        match self {
            ConsumerCallbackLog::Native(log) => Ok(log.lock().expect("callback log poisoned").clone()),
            #[cfg(feature = "multilanguage-tests")]
            ConsumerCallbackLog::Grpc(remote) => remote.entries().await,
        }
    }
}

// ---------------------------------------------------------------------------
// ProducerCallbackLog
// ---------------------------------------------------------------------------

/// Per-producer twin of [`ConsumerCallbackLog`], for delivery callbacks.
pub enum ProducerCallbackLog {
    /// In-process log written by a real Rust `Callback`.
    Native(SharedLog),
    /// Server-side log read over `GetCallbackLog`.
    #[cfg(feature = "multilanguage-tests")]
    Grpc(grpc::ProducerLog),
}

impl ProducerCallbackLog {
    /// A fresh in-process log for the native backend.
    pub fn native() -> Self {
        ProducerCallbackLog::Native(Arc::new(Mutex::new(Vec::new())))
    }

    /// Send `record` with a delivery callback that appends a `"delivery"` entry
    /// to this log. Returns the send future so the caller can still await the
    /// record's completion, exactly like `Producer::send_with_callback`.
    ///
    /// The gRPC arm passes a no-op callback rather than one that appends
    /// locally: `MultilanguageProducer::send_with_callback` sets the proto
    /// `with_callback` flag from `callback.is_some()` (which is what makes the
    /// *server* register a real delivery callback) and then invokes the closure
    /// client-side. Appending there too would double-count, and would record the
    /// harness's own plumbing instead of the binding's.
    pub async fn send_with_logging_callback<P>(
        &self,
        producer: &P,
        record: ProducerRecord<Vec<u8>, Vec<u8>>,
    ) -> Result<confluent_kafka::common::KafkaFuture<RecordMetadata>, Error>
    where
        P: Producer<Vec<u8>, Vec<u8>>,
    {
        match self {
            ProducerCallbackLog::Native(log) => {
                let log = Arc::clone(log);
                let callback: confluent_kafka::producer::Callback =
                    Box::new(move |metadata, error| push(&log, delivery_entry(metadata, error)));
                producer.send_with_callback(record, Some(callback)).await
            },
            #[cfg(feature = "multilanguage-tests")]
            ProducerCallbackLog::Grpc(_) => {
                let callback: confluent_kafka::producer::Callback = Box::new(|_, _| {});
                producer.send_with_callback(record, Some(callback)).await
            },
        }
    }

    /// Snapshot the log, oldest entry first. Does not clear it.
    pub async fn entries(&self) -> Result<Vec<CallbackLogEntry>, Error> {
        match self {
            ProducerCallbackLog::Native(log) => Ok(log.lock().expect("callback log poisoned").clone()),
            #[cfg(feature = "multilanguage-tests")]
            ProducerCallbackLog::Grpc(remote) => remote.entries().await,
        }
    }

    /// Poll [`Self::entries`] until at least one entry of `kind` is present, or
    /// `deadline` elapses; returns the last snapshot either way (so the caller
    /// can assert and print the whole log on failure).
    ///
    /// A delivery callback fires when the producer's completion path runs, which
    /// for the gRPC backends is inside the server process and one log RPC behind
    /// the `Send` response — hence poll-until rather than assert-immediately.
    pub async fn wait_for_kind(&self, kind: &str, deadline: std::time::Duration) -> Vec<CallbackLogEntry> {
        let start = std::time::Instant::now();
        loop {
            let entries = self.entries().await.expect("read producer callback log");
            if entries.iter().any(|e| e.kind == kind) || start.elapsed() >= deadline {
                return entries;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::wait_for_kind`] followed by a bounded **settle window**: keep
    /// re-reading the log for `grace` after the first matching entry appears,
    /// and return the last snapshot.
    ///
    /// Required by any assertion on the *number* of invocations.
    /// `wait_for_kind` deliberately returns at the earliest moment one matching
    /// entry exists, so a backend that fires the callback twice is, in the
    /// common case, sampled between the two appends and an `== 1` assertion
    /// passes vacuously — pinning only the "at least once" half of the
    /// contract. "At most once" is the interesting half: double-firing is the
    /// classic FFI/binding callback bug and CLAUDE.md §9.5 makes exactly-once
    /// invocation an explicit obligation. A stray second append lands within
    /// milliseconds of the first (same completion path, same dispatcher), so a
    /// sub-second grace window is enough to catch it, and the window is a flat
    /// cost — it does not retry or extend.
    pub async fn wait_for_kind_settled(
        &self,
        kind: &str,
        deadline: std::time::Duration,
        grace: std::time::Duration,
    ) -> Vec<CallbackLogEntry> {
        let mut latest = self.wait_for_kind(kind, deadline).await;
        let settle_start = std::time::Instant::now();
        while settle_start.elapsed() < grace {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            latest = self.entries().await.expect("read producer callback log");
        }
        latest
    }
}

// ---------------------------------------------------------------------------
// gRPC-backed logs
// ---------------------------------------------------------------------------

#[cfg(feature = "multilanguage-tests")]
pub mod grpc {
    //! The `GetCallbackLog` client side, plus the two flag-setting RPCs that
    //! ask a server to register real callbacks.

    use confluent_kafka::common::Error;
    use multilanguage_test_server::proto::consumer_service_client::ConsumerServiceClient;
    use multilanguage_test_server::proto::producer_service_client::ProducerServiceClient;
    use multilanguage_test_server::proto::{self};
    use tonic::transport::Channel;

    use super::CallbackLogEntry;
    use crate::common::multilanguage_producer::{kafka_error_from_proto, status_to_kafka_error};

    fn entries_from_proto(response: proto::CallbackLogResponse) -> Vec<CallbackLogEntry> {
        response
            .entries
            .into_iter()
            .map(|e| CallbackLogEntry {
                kind: e.kind,
                partitions: e.partitions.into_iter().map(|p| (p.topic, p.partition)).collect(),
                offsets: e.offsets,
                error: e.error,
            })
            .collect()
    }

    /// Server-side consumer callback log, keyed by the server-local consumer id.
    pub struct ConsumerLog {
        client: ConsumerServiceClient<Channel>,
        consumer_id: u64,
        backend: &'static str,
    }

    impl ConsumerLog {
        pub fn new(channel: Channel, consumer_id: u64, backend: &'static str) -> Self {
            Self { client: ConsumerServiceClient::new(channel), consumer_id, backend }
        }

        /// `Subscribe` with `with_listener = true`.
        pub async fn subscribe_with_listener(&self, topics: Vec<String>) -> Result<(), Error> {
            let mut client = self.client.clone();
            let response = client
                .subscribe(proto::SubscribeRequest { consumer_id: self.consumer_id, topics, with_listener: true })
                .await
                .map_err(|s| status_to_kafka_error(&s, self.backend))?
                .into_inner();
            match response.error {
                Some(err) => Err(kafka_error_from_proto(err)),
                None => Ok(()),
            }
        }

        /// `CommitAsync` with `with_callback = true` and no explicit offsets
        /// (commit the current positions).
        pub async fn commit_async_with_callback(&self) -> Result<(), Error> {
            let mut client = self.client.clone();
            let response = client
                .commit_async(proto::CommitAsyncRequest {
                    consumer_id: self.consumer_id,
                    offsets: Vec::new(),
                    with_callback: true,
                })
                .await
                .map_err(|s| status_to_kafka_error(&s, self.backend))?
                .into_inner();
            match response.error {
                Some(err) => Err(kafka_error_from_proto(err)),
                None => Ok(()),
            }
        }

        pub async fn entries(&self) -> Result<Vec<CallbackLogEntry>, Error> {
            let mut client = self.client.clone();
            let response = client
                .get_callback_log(proto::CallbackLogRequest { consumer_id: self.consumer_id })
                .await
                .map_err(|s| status_to_kafka_error(&s, self.backend))?
                .into_inner();
            Ok(entries_from_proto(response))
        }
    }

    /// Server-side producer callback log, keyed by the server-local producer id.
    pub struct ProducerLog {
        client: ProducerServiceClient<Channel>,
        producer_id: u64,
        backend: &'static str,
    }

    impl ProducerLog {
        pub fn new(channel: Channel, producer_id: u64, backend: &'static str) -> Self {
            Self { client: ProducerServiceClient::new(channel), producer_id, backend }
        }

        pub async fn entries(&self) -> Result<Vec<CallbackLogEntry>, Error> {
            let mut client = self.client.clone();
            let response = client
                .get_callback_log(proto::ProducerCallbackLogRequest { producer_id: self.producer_id })
                .await
                .map_err(|s| status_to_kafka_error(&s, self.backend))?
                .into_inner();
            Ok(entries_from_proto(response))
        }
    }
}
