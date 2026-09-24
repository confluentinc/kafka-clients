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
use confluent_kafka::producer::{Producer, ProducerRecord};

use super::common::backend_factory::{ConsumerBackendFactory, ProducerBackendFactory, RustNativeFactory};
use super::verifier::{Verifier, WorkloadEvent};
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
        self.topic_ids
            .lock()
            .expect("topic_ids poisoned")
            .get(topic)
            .copied()
            .unwrap_or_else(Uuid::zero)
    }
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

        let interval = if self.ctx.target_rps == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / f64::from(self.ctx.target_rps))
        };

        let topic = self.ctx.topic.clone();
        let msg_size = self.ctx.msg_size;
        let producer_label = self.label();
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
            // Stamp the generation current at SEND time, not at ack time: an ack
            // from the old generation can land after the harness has re-resolved
            // the new id (the async-ack race), and stamping it with the new id
            // would score a legitimately destroyed record as loss. The cost: a
            // record sent before the re-resolve but written to the new generation
            // (the one in flight across the delete, plus any sent between the
            // create and the re-resolve) keeps the old id and is excused if
            // unconsumed — a handful per recreate, not the rest of the run.
            let topic_id = self.ctx.topic_id_for(&topic);
            // Open the record's in-flight window before handing it to the
            // client; the `Delivered` / `SendFailed` below closes it. The
            // verifier uses these events to verify that this loop never has
            // more than one record in flight, which is its contract (send,
            // await the outcome, send the next).
            self.verifier
                .record(WorkloadEvent::Sent { index, topic: topic.clone(), producer: producer_label.clone() });
            let event = match producer.send_with_callback(record, None).await {
                Ok(future) => match future.get_timeout(Duration::from_secs(120)).await {
                    Ok(meta) => WorkloadEvent::Delivered {
                        index,
                        topic: topic.clone(),
                        topic_id,
                        partition: meta.partition(),
                        offset: meta.offset(),
                    },
                    Err(err) => WorkloadEvent::SendFailed { index, topic: topic.clone(), error: err.to_string() },
                },
                Err(err) => WorkloadEvent::SendFailed { index, topic: topic.clone(), error: err.to_string() },
            };
            self.verifier.record(event);
            index += 1;
            if !interval.is_zero() {
                tokio::time::sleep(interval).await;
            }
        }

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
                }
            }
        }

        let _ = consumer.commit_sync().await;
        consumer.close().await.expect("chaos consumer close failed");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use confluent_kafka::common::KafkaError;
    use confluent_kafka::producer::MockProducer;

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
    /// `Sent` before it was `Delivered`, the peak in flight was exactly 1, and
    /// no window was left open at close. This guards the emission order in the
    /// loop: a `Sent` emitted after the send, or omitted, would appear here as a
    /// peak of 0 or as unsettled sends.
    #[tokio::test]
    async fn producer_loop_keeps_one_record_in_flight_and_settles_every_send() {
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
        assert_eq!(verdict.max_in_flight, 1, "one record in flight at a time: {verdict}");
        assert_eq!(verdict.unsettled_sends, 0, "every Sent must have settled by close: {verdict}");
        assert!(
            !verdict
                .reasons
                .iter()
                .any(|r| r.starts_with("in-flight bound") || r.starts_with("unsettled sends")),
            "no in-flight failure expected, got {:?}",
            verdict.reasons
        );
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
