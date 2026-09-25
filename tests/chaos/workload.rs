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

//! Pluggable chaos workloads.
//!
//! The orchestrator ([`super::harness::ChaosHarness`]) does **only** broker
//! fault injection; the workload is a swappable plug-in, mirroring how
//! librdkafka's `chaos.py` spawns any workload binary independent of the
//! chaos logic (`design/current/chaos-fault-injection-harness.md` §4).
//!
//! A [`Workload`] is built from a [`WorkloadSpec`] (role × backend) via
//! [`build_workload`], so the orchestrator can mix backends — e.g. a Rust
//! producer with a Python consumer — without knowing anything about
//! `KafkaProducer` / `Consumer`. Every backend is reached through the existing
//! `ProducerBackendFactory` / `ConsumerBackendFactory` abstraction
//! (`tests/common/backend_factory.rs`), which already covers rust / python /
//! c, so no new per-binding driver code is needed here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use confluent_kafka::common::Uuid;
use confluent_kafka::producer::{Callback, Producer, ProducerRecord};

use super::common::backend_factory::{ConsumerBackendFactory, ProducerBackendFactory, RustNativeFactory};
use super::verifier::{ConsumerOp, Verifier, WorkloadEvent};
use super::workload_config::{consumer_props, producer_props};

/// Which binding drives a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// Native Rust client, in-process (async-native).
    Rust,
    /// Sync Python binding (`KafkaProducer`/`Consumer`), via the multilanguage
    /// gRPC bridge (Docker image + `multilanguage-tests` feature required).
    Python,
    /// Asyncio-native Python binding (`AsyncKafkaProducer`/`AsyncKafkaConsumer`),
    /// via a distinct gRPC server image — the async client-flavor variant.
    PythonAsync,
    /// C FFI, via the multilanguage gRPC bridge (Docker image +
    /// `multilanguage-tests` feature required).
    C,
}

impl Backend {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "rust" => Some(Backend::Rust),
            "python" => Some(Backend::Python),
            "python-async" => Some(Backend::PythonAsync),
            "c" => Some(Backend::C),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Backend::Rust => "rust",
            Backend::Python => "python",
            Backend::PythonAsync => "python-async",
            Backend::C => "c",
        }
    }

    /// Whether this backend runs through the gRPC bridge (needs a server
    /// container + the `multilanguage-tests` feature).
    fn is_grpc(self) -> bool {
        !matches!(self, Backend::Rust)
    }
}

/// A workload's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Producer,
    Consumer,
}

/// How a consumer workload commits offsets.
///
/// `Async` is exercised by Phase 3 commit-mode variants; Phase 1 uses `Sync`.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum CommitMode {
    Sync,
    Async,
}

/// One plug-in workload to run during a chaos scenario: a role driven by a
/// backend. Parsed from `"producer:rust"` / `"consumer:python"` on the CLI.
#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub role: Role,
    pub backend: Backend,
    /// Stable id used in the client.id and log lines (e.g. `"consumer-1"`).
    pub instance: u32,
}

impl WorkloadSpec {
    /// Parse `"<role>:<backend>"` (e.g. `"producer:rust"`).
    pub fn parse(s: &str, instance: u32) -> Option<Self> {
        let (role, backend) = s.split_once(':')?;
        let role = match role {
            "producer" => Role::Producer,
            "consumer" => Role::Consumer,
            _ => return None,
        };
        Some(Self { role, backend: Backend::parse(backend)?, instance })
    }

    pub fn label(&self) -> String {
        let role = match self.role {
            Role::Producer => "producer",
            Role::Consumer => "consumer",
        };
        format!("{}-{}-{}", role, self.backend.label(), self.instance)
    }
}
pub type TopicIds = Arc<std::sync::Mutex<std::collections::HashMap<String, Uuid>>>;

/// Immutable per-run context handed to every workload.
#[derive(Clone)]
pub struct WorkloadContext {
    /// Client-facing bootstrap (host-mapped ports) for the Rust backend.
    pub bootstrap: String,
    /// Container-network bootstrap for the gRPC (python/c) backends, whose
    /// client runs inside a sibling container and must reach the broker by
    /// container hostname.
    pub container_bootstrap: String,
    /// The topic THIS workload's producer sends to (one producer per topic).
    /// For consumers this is the first topic and is not used for subscription —
    /// consumers subscribe to every topic in [`Self::topics`].
    pub topic: String,
    /// All topics in the run. Consumers subscribe to the full set (librdkafka
    /// passes every `-t` to each consumer); producers use only [`Self::topic`].
    pub topics: Vec<String>,
    /// Per-topic current topic id (all topics in the run), for the physical
    /// `(topic_id, partition, offset)` verification key. This is the harness's
    /// live map, not a copy: a topic recreate changes the id, and the verifier's
    /// `DestroyedGeneration` excusal is only sound if records produced after the
    /// recreate carry the NEW id. Read it through [`Self::topic_id_for`].
    pub topic_ids: TopicIds,
    pub group: String,
    pub target_rps: u32,
    /// Producer value payload size in bytes (`--msg-size`).
    pub msg_size: usize,
    pub commit_mode: CommitMode,
}

impl WorkloadContext {
    /// Bootstrap appropriate for `backend` (container-internal for gRPC
    /// backends, host-mapped for the in-process Rust backend).
    fn bootstrap_for(&self, backend: Backend) -> &str {
        if backend.is_grpc() {
            &self.container_bootstrap
        } else {
            &self.bootstrap
        }
    }

    /// The current id of `topic`, or `Uuid::zero()` if the harness has not
    /// resolved it.
    fn topic_id_for(&self, topic: &str) -> Uuid {
        topic_id_in(&self.topic_ids, topic)
    }
}

/// The current id of `topic` in the harness's live map, or `Uuid::zero()` if
/// the harness has not resolved it.
fn topic_id_in(ids: &TopicIds, topic: &str) -> Uuid {
    ids.lock()
        .expect("topic_ids poisoned")
        .get(topic)
        .copied()
        .unwrap_or_else(Uuid::zero)
}

/// Builds the completion callback for record `index` of `topic`. The callback
/// closes the record's in-flight window with `Delivered` or `SendFailed`, and
/// stamps a delivered record with the topic id current at **acknowledgement**
/// time, read from the harness's live map.
///
/// Why ack time and not send time: a recreate switches the map to the new
/// generation's id from the create-topics response, before the producer's own
/// metadata can learn the new generation's leaders. So every record written to
/// the new generation is acknowledged after the switch and carries the new id.
/// That includes a record sent while the topic was deleted, which sits in the
/// accumulator until the new generation appears, and a record already in flight
/// when the delete landed, which the client retries into the new generation.
/// Stamping at send time got both wrong: they carried the old id, the
/// destroyed-generation excusal forgave them, and real loss on the new
/// generation went unscored.
///
/// The race this leaves is an acknowledgement from the OLD generation arriving
/// after the switch. It cannot: the old leader answers or fails every request
/// before it drops the partition, so those callbacks fire within milliseconds
/// of the delete, while the switch happens only after the delete has been
/// confirmed absent and the create has returned, hundreds of milliseconds later.
fn delivery_callback(
    index: u64,
    topic: String,
    topic_ids: TopicIds,
    producer_label: String,
    verifier: Arc<dyn Verifier>,
) -> Callback {
    Box::new(move |metadata, error| {
        // The client passes both metadata and an error on the pre-enqueue
        // failure path, so the error decides.
        let event = match (error, metadata) {
            (Some(err), _) => WorkloadEvent::SendFailed { index, topic: topic.clone(), error: err.to_string() },
            (None, Some(meta)) => WorkloadEvent::Delivered {
                index,
                topic: topic.clone(),
                topic_id: topic_id_in(&topic_ids, &topic),
                partition: meta.partition(),
                offset: meta.offset(),
            },
            (None, None) => WorkloadEvent::SendFailed {
                index,
                topic: topic.clone(),
                error: "completion callback fired with neither metadata nor error".to_string(),
            },
        };
        if let WorkloadEvent::SendFailed { error, .. } = &event {
            // Surface the failure as it happens so it can be correlated with
            // the fault in progress; the verdict fails the run on any of these.
            eprintln!("chaos: {producer_label} send of {topic}#{index} failed: {error}");
        }
        verifier.record(event);
    })
}

/// A pluggable chaos workload. The orchestrator only starts it and, via the
/// shared `stop` flag, tells it to drain — it never touches the client type.
///
/// `?Send`: workloads are driven with `join_all` on the scenario's own task
/// (never `tokio::spawn`ed across threads), and the underlying client futures
/// (`Producer` / `Consumer` / the backend factories) are not `Send`-guaranteed
/// at their trait boundary. See [`super::harness::RunningWorkloads::drive`].
#[async_trait(?Send)]
pub trait Workload {
    /// Human-readable label (role-backend-instance).
    fn label(&self) -> String;

    /// Run until `stop` is set, recording outcomes into the ledgers captured
    /// at construction, then drain gracefully (flush / final commit / close).
    async fn run(self: Box<Self>, stop: Arc<AtomicBool>);
}

/// Build a workload from its spec, wiring it to the right backend factory and
/// the shared ledgers. `broker_network` is the Docker network the chaos
/// cluster runs on — the gRPC (python/c) backends attach their client
/// container to it. Async because starting a gRPC server container and opening
/// its channel are async.
///
/// Returns `Err` with a clear message if a gRPC backend is requested without
/// the `multilanguage-tests` feature.
pub async fn build_workload(
    spec: &WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
    broker_network: &str,
) -> Result<Box<dyn Workload>, String> {
    match (spec.role, spec.backend) {
        (Role::Producer, Backend::Rust) => Ok(Box::new(ProducerWorkload {
            factory: RustNativeFactory,
            spec: spec.clone(),
            ctx,
            verifier,
        })),
        (Role::Consumer, Backend::Rust) => Ok(Box::new(ConsumerWorkload {
            factory: RustNativeFactory,
            spec: spec.clone(),
            ctx,
            verifier,
        })),
        (_, _) if spec.backend.is_grpc() => build_grpc_workload(spec, ctx, verifier, broker_network).await,
        _ => unreachable!("rust backends handled above"),
    }
}

#[cfg(not(feature = "multilanguage-tests"))]
async fn build_grpc_workload(
    spec: &WorkloadSpec,
    _ctx: WorkloadContext,
    _verifier: Arc<dyn Verifier>,
    _broker_network: &str,
) -> Result<Box<dyn Workload>, String> {
    Err(format!(
        "workload '{}' needs a gRPC binding backend, which requires the `multilanguage-tests` \
         feature (and the python/c gRPC-server Docker image). Re-run with \
         `--features multilanguage-tests`.",
        spec.label()
    ))
}

#[cfg(feature = "multilanguage-tests")]
async fn build_grpc_workload(
    spec: &WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
    broker_network: &str,
) -> Result<Box<dyn Workload>, String> {
    use super::common::backend_factory::{CGrpcFactory, PythonAsyncGrpcFactory, PythonGrpcFactory};
    use super::common::backend_pool::{BackendKind, get_or_start};

    // Start (or reuse) the backend's gRPC server container, attached to the
    // chaos cluster's Docker network, and open a channel to it — exactly as
    // the multilanguage test harness does.
    let kind = match spec.backend {
        Backend::Python => BackendKind::Python,
        Backend::PythonAsync => BackendKind::PythonAsync,
        Backend::C => BackendKind::C,
        Backend::Rust => unreachable!("rust handled by build_workload"),
    };
    let handle = get_or_start(kind, broker_network).await;
    let channel = handle.channel().await;

    // Each factory is a distinct type, so build the workload inside the arm
    // that knows the concrete factory (they all satisfy the generic bounds).
    Ok(match (spec.role, spec.backend) {
        (Role::Producer, Backend::Python) => {
            Box::new(ProducerWorkload { factory: PythonGrpcFactory::new(channel), spec: spec.clone(), ctx, verifier })
        },
        (Role::Consumer, Backend::Python) => {
            Box::new(ConsumerWorkload { factory: PythonGrpcFactory::new(channel), spec: spec.clone(), ctx, verifier })
        },
        (Role::Producer, Backend::PythonAsync) => Box::new(ProducerWorkload {
            factory: PythonAsyncGrpcFactory::new(channel),
            spec: spec.clone(),
            ctx,
            verifier,
        }),
        (Role::Consumer, Backend::PythonAsync) => Box::new(ConsumerWorkload {
            factory: PythonAsyncGrpcFactory::new(channel),
            spec: spec.clone(),
            ctx,
            verifier,
        }),
        (Role::Producer, Backend::C) => {
            Box::new(ProducerWorkload { factory: CGrpcFactory::new(channel), spec: spec.clone(), ctx, verifier })
        },
        (Role::Consumer, Backend::C) => {
            Box::new(ConsumerWorkload { factory: CGrpcFactory::new(channel), spec: spec.clone(), ctx, verifier })
        },
        (_, Backend::Rust) => unreachable!("rust handled by build_workload"),
    })
}

/// Build the producer value payload: the 8-byte big-endian logical `index`
/// followed by zero padding to reach `msg_size` bytes. If `msg_size < 8`, only
/// the first `msg_size` bytes of the index are sent (the key still carries the
/// full 8-byte index, so logical identity is preserved).
fn build_value(index: u64, msg_size: usize) -> Vec<u8> {
    let idx = index.to_be_bytes();
    if msg_size <= idx.len() {
        idx[..msg_size].to_vec()
    } else {
        let mut v = Vec::with_capacity(msg_size);
        v.extend_from_slice(&idx);
        v.resize(msg_size, 0);
        v
    }
}

// ---------------------------------------------------------------------------
// Producer workload — generic over any ProducerBackendFactory.
// ---------------------------------------------------------------------------

struct ProducerWorkload<F: ProducerBackendFactory> {
    factory: F,
    spec: WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
}

#[async_trait(?Send)]
impl<F> Workload for ProducerWorkload<F>
where
    F: ProducerBackendFactory,
{
    fn label(&self) -> String {
        self.spec.label()
    }

    async fn run(self: Box<Self>, stop: Arc<AtomicBool>) {
        eprintln!("chaos: starting workload {}", self.label());
        let bootstrap = self.ctx.bootstrap_for(self.spec.backend).to_string();
        let producer = self
            .factory
            .create(producer_props(&bootstrap, &self.spec.label()))
            .await
            .expect("failed to build chaos producer");

        // Pacing runs on an absolute schedule: each record has a due time
        // `interval` after the previous one, and the loop sleeps only until that
        // time. Sleeping `interval` per iteration would add the timer's tick
        // granularity (about 1 ms) and the loop's own cost to every record, so
        // the achieved rate would fall well short of the target; on the absolute
        // schedule an overshoot is caught up by the next records, and the
        // average rate stays at the target.
        let interval = if self.ctx.target_rps == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / f64::from(self.ctx.target_rps))
        };
        let mut next_due = tokio::time::Instant::now();

        let topic = self.ctx.topic.clone();
        let msg_size = self.ctx.msg_size;
        let producer_label = self.label();
        // The loop does not await each record's outcome; the client reports it
        // through the send callback, which runs on the client's sender task.
        // This is what lets the client batch records and pipeline requests; a
        // loop that awaited each send would put one record per batch on the
        // wire and be bounded by one round trip per record. Backpressure is the
        // client's own: `send` blocks on `buffer.memory` / `max.block.ms` when
        // the accumulator is full, as it would for any application.
        let mut index: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            // Key = the 8-byte big-endian logical index (the record's logical
            // identity, read back by the consumer). Value = the same index in
            // its first 8 bytes, padded with zeros up to `msg_size` so the wire
            // payload matches librdkafka's `-s <msg_size>`. When `msg_size < 8`
            // the value is just its first `msg_size` bytes; the key is unchanged.
            let key = index.to_be_bytes().to_vec();
            let value = build_value(index, msg_size);
            let record = ProducerRecord::with_key(topic.clone(), Some(key), Some(value));
            // Open the record's in-flight window before handing it to the
            // client; the callback's `Delivered` / `SendFailed` closes it. The
            // verifier uses these events to report the per-producer peak and
            // to check that every window is closed by verdict time.
            self.verifier
                .record(WorkloadEvent::Sent { index, topic: topic.clone(), producer: producer_label.clone() });
            // The topic id is stamped when the ack arrives, not here; see
            // `delivery_callback` for why.
            let callback = delivery_callback(
                index,
                topic.clone(),
                self.ctx.topic_ids.clone(),
                producer_label.clone(),
                self.verifier.clone(),
            );
            if let Err(err) = producer.send_with_callback(record, Some(callback)).await {
                // The client invokes the callback itself for API errors and
                // returns a failed future. Other errors come back here without
                // the callback having run, so the outcome is recorded directly.
                eprintln!("chaos: {producer_label} send of {topic}#{index} failed: {err}");
                self.verifier
                    .record(WorkloadEvent::SendFailed { index, topic: topic.clone(), error: err.to_string() });
            }
            index += 1;
            if interval.is_zero() {
                // Unlimited rate (`--rps 0`): `send` returns ready without ever
                // touching a tokio resource while the accumulator has room, so
                // the loop would never return `Pending`. The drive loop polls
                // this workload, the consumers and the scenario timer from one
                // task, so without an explicit yield they run only when the
                // producer blocks on a full buffer: the warmup timer fired
                // minutes late and the consumer starved. One yield per record
                // costs well under a microsecond.
                tokio::task::yield_now().await;
            } else {
                next_due += interval;
                let now = tokio::time::Instant::now();
                if next_due > now {
                    tokio::time::sleep_until(next_due).await;
                } else if now - next_due > Duration::from_secs(1) {
                    // More than a second behind (a long `send` block while the
                    // client waits out a fault): do not replay the backlog as a
                    // burst, resume at the target rate from now.
                    next_due = now;
                    tokio::task::yield_now().await;
                } else {
                    // Behind schedule but catching up; still yield so the stop
                    // flag and other tasks on this runtime get a turn.
                    tokio::task::yield_now().await;
                }
            }
        }

        // Close waits for every buffered record's outcome, so all callbacks
        // have fired, and every in-flight window is closed, when it returns.
        producer.close().await.expect("chaos producer close failed");
    }
}

// ---------------------------------------------------------------------------
// Consumer workload — over any ConsumerBackendFactory (yields Box<dyn Consumer>).
// ---------------------------------------------------------------------------

struct ConsumerWorkload<F: ConsumerBackendFactory> {
    factory: F,
    spec: WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
}

#[async_trait(?Send)]
impl<F> Workload for ConsumerWorkload<F>
where
    F: ConsumerBackendFactory,
{
    fn label(&self) -> String {
        self.spec.label()
    }

    async fn run(self: Box<Self>, stop: Arc<AtomicBool>) {
        let bootstrap = self.ctx.bootstrap_for(self.spec.backend).to_string();
        let label = self.spec.label();
        let mut consumer = self
            .factory
            .create(consumer_props(&bootstrap, &self.ctx.group, &label))
            .await
            .expect("failed to build chaos consumer");

        // Subscribe to EVERY topic in the run (librdkafka passes all `-t` flags
        // to each consumer), not just one — so a multi-topic run's consumers
        // cover all topics.
        consumer
            .subscribe(self.ctx.topics.clone())
            .await
            .expect("chaos consumer subscribe failed");

        while !stop.load(Ordering::Relaxed) {
            let records = match consumer.poll(Duration::from_millis(500)).await {
                Ok(records) => records,
                Err(err) => {
                    eprintln!("chaos {label}: poll error: {err}");
                    self.verifier.record(WorkloadEvent::ConsumerError {
                        consumer: label.clone(),
                        op: ConsumerOp::Poll,
                        error: err.to_string(),
                    });
                    continue;
                },
            };

            let mut got_any = false;
            for record in &records {
                got_any = true;
                if let Some(bytes) = record.key()
                    && let Ok(arr) = bytes.as_slice().try_into()
                {
                    let topic = record.topic();
                    // Map the record's own topic to that topic's id (a consumer
                    // reads from every topic; ids differ per topic and per
                    // recreate generation). Falls back to zero if unresolved.
                    let topic_id = self.ctx.topic_id_for(topic);
                    self.verifier.record(WorkloadEvent::Consumed {
                        index: u64::from_be_bytes(arr),
                        topic: topic.to_string(),
                        topic_id,
                        partition: record.partition(),
                        offset: record.offset(),
                    });
                }
            }

            if got_any {
                let commit = match self.ctx.commit_mode {
                    CommitMode::Sync => consumer.commit_sync().await,
                    CommitMode::Async => consumer.commit_async().await,
                };
                if let Err(err) = commit {
                    eprintln!("chaos {label}: commit error: {err}");
                    self.verifier.record(WorkloadEvent::ConsumerError {
                        consumer: label.clone(),
                        op: ConsumerOp::Commit,
                        error: err.to_string(),
                    });
                }
            }
        }

        // Final commit before leaving; an error here is recorded like any other
        // commit error so the summary accounts for it.
        if let Err(err) = consumer.commit_sync().await {
            eprintln!("chaos {label}: final commit error: {err}");
            self.verifier.record(WorkloadEvent::ConsumerError {
                consumer: label.clone(),
                op: ConsumerOp::Commit,
                error: err.to_string(),
            });
        }
        consumer.close().await.expect("chaos consumer close failed");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use confluent_kafka::common::{KafkaError, TopicPartition};
    use confluent_kafka::producer::{MockProducer, RecordMetadata};

    use super::*;
    use crate::common::callback_log::ProducerCallbackLog;
    use crate::verifier::{ConservationVerifier, ExpectedLossHint};

    fn ctx_with(topic_ids: TopicIds) -> WorkloadContext {
        WorkloadContext {
            bootstrap: String::new(),
            container_bootstrap: String::new(),
            topic: "t".into(),
            topics: vec!["t".into()],
            topic_ids,
            group: String::new(),
            target_rps: 0,
            msg_size: 0,
            commit_mode: CommitMode::Sync,
        }
    }

    /// Backend factory over the crate's `MockProducer` (auto-complete: every
    /// send is acknowledged immediately), so the production producer loop can
    /// be driven without a broker.
    struct MockFactory;

    impl ProducerBackendFactory for MockFactory {
        type Producer = MockProducer<Vec<u8>, Vec<u8>>;

        fn name(&self) -> &'static str {
            "mock"
        }

        async fn create(&self, _config: HashMap<String, String>) -> Result<Self::Producer, KafkaError> {
            Ok(MockProducer::with_auto_complete(true))
        }

        async fn create_with_callback_log(
            &self,
            _config: HashMap<String, String>,
        ) -> Result<(Self::Producer, ProducerCallbackLog), KafkaError> {
            Err(KafkaError::illegal_state("not used by the chaos producer workload"))
        }
    }

    /// Drives the production `ProducerWorkload::run` loop against the verifier
    /// and checks what the verdict derives from its events: every record was
    /// `Sent` before its outcome, and no window was left open at close. This
    /// guards the emission order in the loop: a `Sent` emitted after the send,
    /// or omitted, would appear here as a peak of 0 or as unsettled sends. The
    /// mock acknowledges inside `send`, so the callback fires before the next
    /// iteration and the peak is 1.
    #[tokio::test]
    async fn producer_loop_emits_sent_before_each_outcome_and_settles_every_send() {
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut ctx = ctx_with(ids);
        // A non-zero rate makes the loop yield to the runtime (its `sleep`),
        // so the stop flag below can be set while it runs.
        ctx.target_rps = 2000;
        let verifier = Arc::new(ConservationVerifier::new());
        let spec = WorkloadSpec { role: Role::Producer, backend: Backend::Rust, instance: 1 };
        let workload = Box::new(ProducerWorkload { factory: MockFactory, spec, ctx, verifier: verifier.clone() });

        let stop = Arc::new(AtomicBool::new(false));
        let stopper = {
            let stop = stop.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                stop.store(true, Ordering::Relaxed);
            }
        };
        tokio::join!(workload.run(stop), stopper);

        let verdict = verifier.verdict(0);
        assert!(verdict.delivered > 0, "the loop must have produced: {verdict}");
        assert_eq!(verdict.failed_sends, 0, "{verdict}");
        assert_eq!(verdict.max_in_flight, 1, "the mock acknowledges inside send: {verdict}");
        assert_eq!(verdict.unsettled_sends, 0, "every Sent must have settled by close: {verdict}");
        assert!(
            !verdict.reasons.iter().any(|r| r.starts_with("unsettled sends")),
            "no unsettled sends expected, got {:?}",
            verdict.reasons
        );
    }

    /// The completion callback stamps the topic id current when the ACK
    /// arrives, not when the record was sent. Here the record is sent under the
    /// old generation, the harness switches the map to the new generation and
    /// marks the old one destroyed, and only then does the ack land: the record
    /// must carry the new id, so it is scored as loss when unconsumed rather
    /// than excused as destroyed. With send-time stamping this record would
    /// have been silently excused.
    #[test]
    fn delivery_callback_stamps_the_generation_current_at_ack_time() {
        let old_id = Uuid::from_bytes([7u8; 16]);
        let new_id = Uuid::from_bytes([8u8; 16]);
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::from([("t".to_string(), old_id)])));
        let verifier = Arc::new(ConservationVerifier::new());

        // Sent while the map still held the old generation.
        verifier.record(WorkloadEvent::Sent { index: 0, topic: "t".into(), producer: "p".into() });
        let callback = delivery_callback(0, "t".into(), ids.clone(), "p".into(), verifier.clone());

        // The recreate lands before the ack does.
        ids.lock().unwrap().insert("t".to_string(), new_id);
        verifier.note_expected_loss(ExpectedLossHint::DestroyedGeneration(old_id));

        let meta = RecordMetadata::new(TopicPartition::new("t".to_string(), 3), 5, 0, 0, 8, 8);
        callback(Some(&meta), None);

        let verdict = verifier.verdict(1);
        assert_eq!(verdict.unsettled_sends, 0, "the callback must settle the send: {verdict}");
        assert_eq!(
            verdict.lost,
            vec![("t".to_string(), 0)],
            "an ack landing after the switch belongs to the new generation and is real loss: {verdict}"
        );
        assert_eq!(verdict.expected_lost, 0, "nothing may be excused as destroyed: {verdict}");
        let tail = &verdict.lost_by_partition;
        assert_eq!(tail.len(), 1, "{verdict}");
        assert_eq!((tail[0].partition, tail[0].last_lost_offset), (3, 5), "{verdict}");
    }

    /// A failing ack settles the send without stamping anything: no id lookup,
    /// one `SendFailed` carrying the client's error text.
    #[test]
    fn delivery_callback_records_a_failed_send_with_its_error_text() {
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let verifier = Arc::new(ConservationVerifier::new());
        verifier.record(WorkloadEvent::Sent { index: 4, topic: "t".into(), producer: "p".into() });
        let callback = delivery_callback(4, "t".into(), ids, "p".into(), verifier.clone());

        let err = KafkaError::timeout("Expiring 1 record(s) for t-0: 120000 ms has passed");
        callback(None, Some(&err));

        let verdict = verifier.verdict(0);
        assert_eq!(verdict.unsettled_sends, 0, "{verdict}");
        assert_eq!(verdict.failed_sends, 1, "{verdict}");
        assert!(verdict.delivered == 0, "a failed send is not a delivery: {verdict}");
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("failed sends"))
            .unwrap_or_else(|| panic!("expected a failed-sends reason: {verdict}"));
        assert!(reason.contains("t#4: ") && reason.contains("Expiring 1 record(s)"), "{reason}");
    }

    /// A workload context built BEFORE a recreate must see the id the harness
    /// re-resolves AFTER it. With a startup snapshot, every post-recreate record
    /// kept the original id; once that id was marked a destroyed generation, all
    /// unconsumed records on the topic were excused for the rest of the run and
    /// real loss went undetected.
    #[test]
    fn records_after_recreate_are_stamped_with_new_generation_and_still_checked() {
        let old_id = Uuid::from_bytes([7u8; 16]);
        let new_id = Uuid::from_bytes([8u8; 16]);
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(std::collections::HashMap::from([(
            "t".to_string(),
            old_id,
        )])));
        // The workload captures its context once, at startup.
        let ctx = ctx_with(ids.clone());
        let v = ConservationVerifier::new();

        // Before the recreate: stamped with the old generation, never consumed.
        assert_eq!(ctx.topic_id_for("t"), old_id);
        v.record(WorkloadEvent::Delivered {
            index: 0,
            topic: "t".into(),
            topic_id: ctx.topic_id_for("t"),
            partition: 0,
            offset: 0,
        });

        // The harness recreates the topic: re-resolves the id into the shared map
        // and marks the old generation destroyed.
        ids.lock().unwrap().insert("t".to_string(), new_id);
        v.note_expected_loss(ExpectedLossHint::DestroyedGeneration(old_id));

        // After the recreate: the SAME context now stamps the new generation.
        assert_eq!(ctx.topic_id_for("t"), new_id);
        v.record(WorkloadEvent::Delivered {
            index: 1,
            topic: "t".into(),
            topic_id: ctx.topic_id_for("t"),
            partition: 0,
            offset: 0,
        });

        let verdict = v.verdict(1);
        assert_eq!(
            verdict.lost,
            vec![("t".to_string(), 1)],
            "the old-generation record is excused, but an unconsumed record on the new \
             generation must still be scored as loss: {verdict}"
        );
        assert_eq!(verdict.expected_lost, 1);
        assert!(!verdict.is_pass());
    }

    /// The consumer keys each record under its own topic's live id, so a
    /// multi-topic consumer picks up a recreate of one topic without touching
    /// the other.
    #[test]
    fn topic_id_for_is_per_topic_and_zero_when_unresolved() {
        let a = Uuid::from_bytes([1u8; 16]);
        let b = Uuid::from_bytes([2u8; 16]);
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(std::collections::HashMap::from([
            ("t0".to_string(), a),
            ("t1".to_string(), b),
        ])));
        let ctx = ctx_with(ids.clone());
        assert_eq!(ctx.topic_id_for("t0"), a);
        assert_eq!(ctx.topic_id_for("t1"), b);
        assert_eq!(ctx.topic_id_for("absent"), Uuid::zero());

        let b2 = Uuid::from_bytes([3u8; 16]);
        ids.lock().unwrap().insert("t1".to_string(), b2);
        assert_eq!(ctx.topic_id_for("t0"), a, "recreating t1 must not change t0's id");
        assert_eq!(ctx.topic_id_for("t1"), b2);
    }
}
