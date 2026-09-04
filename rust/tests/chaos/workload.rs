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

/// Immutable per-run context handed to every workload.
#[derive(Clone)]
pub struct WorkloadContext {
    /// Client-facing bootstrap (host-mapped ports) for the Rust backend.
    pub bootstrap: String,
    /// Container-network bootstrap for the gRPC (python/c) backends, whose
    /// client runs inside a sibling container and must reach the broker by
    /// container hostname.
    pub container_bootstrap: String,
    pub topic: String,
    /// Current topic id, for the physical `(topic_id, partition, offset)`
    /// verification key. `Uuid::zero()` if the harness could not resolve it.
    /// After a topic recreate the id changes, so this is the id at workload
    /// construction; the producer stamps the id it delivered against.
    pub topic_id: Uuid,
    pub group: String,
    pub target_rps: u32,
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
        let topic_id = self.ctx.topic_id;
        let mut index: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            let key = index.to_be_bytes().to_vec();
            let record = ProducerRecord::with_key(topic.clone(), Some(key.clone()), Some(key));
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

        consumer
            .subscribe(vec![self.ctx.topic.clone()])
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
                    self.verifier.record(WorkloadEvent::Consumed {
                        index: u64::from_be_bytes(arr),
                        topic: record.topic().to_string(),
                        topic_id: self.ctx.topic_id,
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
