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

//! Broker fault-injection control for the chaos harness.
//!
//! Wraps `docker stop` / `docker kill` / `docker start` over a
//! [`KafkaCluster`]'s per-broker containers, plus a metadata-driven
//! "broker is operational again" wait. This is the Docker + AdminClient
//! translation of librdkafka's trivup `broker.stop(force=...)` /
//! `broker.start()` / `broker.wait_operational()` — see
//! `design/current/chaos-fault-injection-harness.md` §1.
//!
//! Test-only infrastructure: it drives Docker directly and has no Java
//! counterpart (see the DoD #7 note in the design doc). It must never be
//! used against a pooled cluster from [`super::cluster_pool`] — a chaos run
//! stops/kills brokers, which would corrupt state other tests depend on and
//! could race pool eviction (`docker rm -f`) mid-scenario. The chaos harness
//! owns its own [`KafkaCluster`] via `KafkaCluster::start_with_config`.

use std::process::Command;
use std::time::{Duration, Instant};

use confluent_kafka::admin::Admin;

use super::kafka_cluster::KafkaCluster;

/// How a broker is taken down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopKind {
    /// Graceful: `docker stop` sends SIGTERM and grants a grace period, so
    /// the broker runs its KRaft shutdown (flush + controlled leadership
    /// handoff) before exiting. The trivup `force=False` analog.
    Clean,
    /// Forceful: `docker kill` sends SIGKILL immediately — no flush, no
    /// handoff; the controller must detect the lost session and re-elect.
    /// The trivup `force=True` analog.
    Unclean,
}

/// Controls the brokers of a single (non-pooled) [`KafkaCluster`] for
/// fault injection. Brokers are addressed by **1-based node id**, matching
/// `KAFKA_NODE_ID`; `container_ids()[node_id - 1]` is that broker's
/// container (the two are constructed in the same order in
/// `KafkaCluster::try_start_with_config`).
pub struct BrokerControl<'a> {
    cluster: &'a KafkaCluster,
}

impl<'a> BrokerControl<'a> {
    /// Create a controller over `cluster`'s brokers.
    pub fn new(cluster: &'a KafkaCluster) -> Self {
        Self { cluster }
    }

    /// Number of brokers in the cluster (max valid node id).
    pub fn broker_count(&self) -> u16 {
        self.cluster.config().brokers
    }

    /// The Docker container id for a 1-based `node_id`.
    fn container_id(&self, node_id: u16) -> &str {
        let ids = self.cluster.container_ids();
        assert!(
            node_id >= 1 && (node_id as usize) <= ids.len(),
            "node_id {node_id} out of range 1..={}",
            ids.len()
        );
        &ids[(node_id - 1) as usize]
    }

    /// Stop a broker, cleanly (SIGTERM) or uncleanly (SIGKILL).
    ///
    /// `docker stop` waits out its grace period then SIGKILLs; for
    /// [`StopKind::Unclean`] we pass `docker kill` so there is no grace
    /// period at all.
    ///
    /// The docker command runs on a blocking thread: a clean stop waits out the
    /// broker's controlled shutdown (seconds). The chaos workloads run on their
    /// own threads (`tests/chaos/isolation.rs`), but the scenario task that calls
    /// this also beats the heartbeat and times the other actions, and a
    /// concurrent stop of several brokers (`join_all`) would run one after the
    /// other on a blocked task.
    pub async fn stop(&self, node_id: u16, kind: StopKind) {
        let id = self.container_id(node_id).to_string();
        let status = tokio::task::spawn_blocking(move || match kind {
            StopKind::Clean => Command::new("docker").args(["stop", &id]).status(),
            StopKind::Unclean => Command::new("docker").args(["kill", &id]).status(),
        })
        .await
        .expect("docker stop task panicked")
        .expect("failed to spawn docker to stop broker");
        assert!(status.success(), "docker stop/kill failed for node {node_id}");
    }

    /// Start a previously-stopped broker. It rejoins the KRaft quorum with
    /// the same node id and catches up on metadata. Runs off-task like
    /// [`BrokerControl::stop`].
    pub async fn start(&self, node_id: u16) {
        let id = self.container_id(node_id).to_string();
        let status = tokio::task::spawn_blocking(move || Command::new("docker").args(["start", &id]).status())
            .await
            .expect("docker start task panicked")
            .expect("failed to spawn docker to start broker");
        assert!(status.success(), "docker start failed for node {node_id}");
    }

    /// Whether the broker's container is currently running (the `pid()`
    /// liveness analog). Reads `docker inspect -f '{{.State.Running}}'`. Runs
    /// off-task like [`BrokerControl::stop`].
    pub async fn is_running(&self, node_id: u16) -> bool {
        let id = self.container_id(node_id).to_string();
        let out = tokio::task::spawn_blocking(move || {
            Command::new("docker")
                .args(["inspect", "-f", "{{.State.Running}}", &id])
                .output()
        })
        .await
        .expect("docker inspect task panicked")
        .expect("failed to spawn docker inspect");
        out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true"
    }

    /// Wait until the broker's container has logged the image's readiness line
    /// (`Kafka Server started`, the same line [`KafkaCluster`] gates startup
    /// on) at or after `since`. Returns `false` on timeout.
    ///
    /// Use this after [`BrokerControl::start`] rather than relying on
    /// [`BrokerControl::wait_operational`] alone: a broker killed moments ago
    /// stays in `describe_cluster().nodes()` until its controller session
    /// expires (~9 s), so presence there can be observed before the restarted
    /// process is up at all. The log line is written by the new process only.
    pub async fn wait_server_started(&self, node_id: u16, since: std::time::SystemTime, timeout: Duration) -> bool {
        // `docker logs --since` takes whole or fractional unix seconds; back
        // off one second so a clock-granularity edge cannot hide the line.
        let since_secs = since
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().saturating_sub(1))
            .unwrap_or(0)
            .to_string();
        let deadline = Instant::now() + timeout;
        loop {
            let id = self.container_id(node_id).to_string();
            let since_arg = since_secs.clone();
            let started = tokio::task::spawn_blocking(move || {
                Command::new("docker")
                    .args(["logs", "--since", &since_arg, &id])
                    .output()
                    .map(|out| {
                        out.status.success()
                            && (String::from_utf8_lossy(&out.stdout).contains("Kafka Server started")
                                || String::from_utf8_lossy(&out.stderr).contains("Kafka Server started"))
                    })
                    .unwrap_or(false)
            })
            .await
            .unwrap_or(false);
            if started {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Wait until `admin` reports `node_id` present in the cluster's broker
    /// set — the `broker.wait_operational()` analog. Returns `false` on
    /// timeout.
    ///
    /// Presence in `describe_cluster().nodes()` means the broker has
    /// re-registered with the controller quorum and is answering metadata,
    /// which is the readiness signal the harness gates its next action on.
    pub async fn wait_operational(&self, admin: &dyn Admin, node_id: u16, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            // Bound each describe attempt so a hung RPC on a degraded cluster
            // cannot stall the loop past `deadline`. Without this, the admin
            // client can retry `describe_cluster` internally forever and control
            // never reaches the deadline check below — the loop's `timeout` would
            // be silently ineffective.
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let attempt = remaining.min(Duration::from_secs(5));
            let result = admin.describe_cluster();
            let node_future = result.nodes();
            let describe = node_future.get();
            if let Ok(Ok(nodes)) = tokio::time::timeout(attempt, describe).await
                && nodes.iter().any(|n| n.id() == i32::from(node_id))
            {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
