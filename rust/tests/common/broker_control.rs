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

/// How long a clean stop lets the broker shut down before Docker SIGKILLs it.
/// A controlled shutdown moves every partition leadership off the broker and
/// flushes its logs, which under a heavy produce load takes well over
/// Docker's default 10 s.
pub const CLEAN_STOP_GRACE: Duration = Duration::from_secs(120);

/// Whether a container exit code (`{{.State.ExitCode}}`) after a clean stop
/// means the broker shut down by itself: 0, or 143 (128 + SIGTERM, the JVM's
/// exit status after its shutdown hooks ran). 137 (128 + SIGKILL) means Docker
/// killed it when the grace ran out; an unreadable code is not counted as
/// clean.
fn clean_stop_exit_code(exit_code: Option<&str>) -> bool {
    matches!(exit_code, Some("0" | "143"))
}

/// How a broker is taken down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopKind {
    /// Graceful: `docker stop` sends SIGTERM and grants a grace period
    /// ([`CLEAN_STOP_GRACE`]), so the broker runs its KRaft shutdown (flush +
    /// controlled leadership handoff) before exiting. The trivup
    /// `force=False` analog.
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

    /// Number of brokers in the cluster (max valid node id): one container per
    /// broker.
    pub fn broker_count(&self) -> u16 {
        self.cluster.container_ids().len() as u16
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
    /// A clean stop runs `docker stop -t` [`CLEAN_STOP_GRACE`]: SIGTERM, then
    /// SIGKILL only if the broker is still running after the grace period.
    /// Docker's default 10 s grace is shorter than a controlled shutdown can
    /// take under load, and the SIGKILL that follows is still reported as a
    /// successful stop. So the container's exit code is checked afterwards:
    /// 0 or 143 (SIGTERM) means the broker shut down by itself, anything else
    /// (137 = SIGKILL) means the "clean" stop was in fact unclean. That is
    /// logged as a WARN and reported by returning `false`. For
    /// [`StopKind::Unclean`] we run `docker kill` so there is no grace period
    /// at all, and the result is always `true`.
    ///
    /// The docker command runs on a blocking thread: a clean stop waits out the
    /// broker's controlled shutdown (seconds). The chaos workloads run on their
    /// own threads (`tests/chaos/isolation.rs`), but the scenario task that calls
    /// this also beats the heartbeat and times the other actions, and a
    /// concurrent stop of several brokers (`join_all`) would run one after the
    /// other on a blocked task.
    pub async fn stop(&self, node_id: u16, kind: StopKind) -> bool {
        let id = self.container_id(node_id).to_string();
        let grace = CLEAN_STOP_GRACE.as_secs().to_string();
        let status = tokio::task::spawn_blocking(move || match kind {
            StopKind::Clean => Command::new("docker").args(["stop", "-t", &grace, &id]).status(),
            StopKind::Unclean => Command::new("docker").args(["kill", &id]).status(),
        })
        .await
        .expect("docker stop task panicked")
        .expect("failed to spawn docker to stop broker");
        assert!(status.success(), "docker stop/kill failed for node {node_id}");
        if kind == StopKind::Unclean {
            return true;
        }
        let exit_code = self.inspect(node_id, "{{.State.ExitCode}}").await;
        let clean = clean_stop_exit_code(exit_code.as_deref());
        if !clean {
            eprintln!(
                "chaos: WARN clean stop of broker {node_id} was NOT clean: exit code {} after a {CLEAN_STOP_GRACE:?} \
                 grace (137 = SIGKILLed after the grace ran out, so no controlled shutdown)",
                exit_code.as_deref().unwrap_or("unknown")
            );
        }
        clean
    }

    /// Start a previously-stopped broker. It rejoins the KRaft quorum with
    /// the same node id and catches up on metadata. Runs off-task like
    /// [`BrokerControl::stop`].
    ///
    /// Returns the container's start time as the Docker daemon recorded it
    /// (`{{.State.StartedAt}}`, RFC 3339), for
    /// [`BrokerControl::wait_server_started`]. It is the daemon's clock, the
    /// same one that timestamps the container's log lines; the host clock can
    /// differ from it (Docker Desktop runs the daemon in a VM).
    pub async fn start(&self, node_id: u16) -> String {
        let id = self.container_id(node_id).to_string();
        let status = tokio::task::spawn_blocking(move || Command::new("docker").args(["start", &id]).status())
            .await
            .expect("docker start task panicked")
            .expect("failed to spawn docker to start broker");
        assert!(status.success(), "docker start failed for node {node_id}");
        self.inspect(node_id, "{{.State.StartedAt}}")
            .await
            .unwrap_or_else(|| panic!("docker inspect could not read the start time of node {node_id}"))
    }

    /// One `docker inspect -f <format>` field of a broker's container, trimmed,
    /// or `None` if the inspect failed. Runs off-task like
    /// [`BrokerControl::stop`].
    async fn inspect(&self, node_id: u16, format: &'static str) -> Option<String> {
        let id = self.container_id(node_id).to_string();
        let out =
            tokio::task::spawn_blocking(move || Command::new("docker").args(["inspect", "-f", format, &id]).output())
                .await
                .expect("docker inspect task panicked")
                .expect("failed to spawn docker inspect");
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Whether the broker's container is currently running (the `pid()`
    /// liveness analog). Reads `docker inspect -f '{{.State.Running}}'`. Runs
    /// off-task like [`BrokerControl::stop`].
    pub async fn is_running(&self, node_id: u16) -> bool {
        self.inspect(node_id, "{{.State.Running}}").await.as_deref() == Some("true")
    }

    /// Wait until the broker's container has logged the image's readiness line
    /// (`Kafka Server started`, the same line [`KafkaCluster`] gates startup
    /// on) at or after `started_at`, the daemon-side start time
    /// [`BrokerControl::start`] returned. Returns `false` on timeout.
    ///
    /// Use this after [`BrokerControl::start`] rather than relying on
    /// [`BrokerControl::wait_operational`] alone: a broker killed moments ago
    /// stays in `describe_cluster().nodes()` until its controller session
    /// expires (~9 s), so presence there can be observed before the restarted
    /// process is up at all. The log line is written by the new process only.
    ///
    /// `docker logs --since` filters on the daemon's log timestamps, so the
    /// bound must come from the daemon's clock too. A host timestamp would hide
    /// the new line when the daemon's clock is behind the host's, and match
    /// the previous process's line when it is ahead.
    pub async fn wait_server_started(&self, node_id: u16, started_at: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let id = self.container_id(node_id).to_string();
            let since_arg = started_at.to_string();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_stop_exit_code_accepts_only_a_self_shutdown() {
        assert!(clean_stop_exit_code(Some("0")));
        assert!(clean_stop_exit_code(Some("143")));
        assert!(!clean_stop_exit_code(Some("137")));
        assert!(!clean_stop_exit_code(Some("1")));
        assert!(!clean_stop_exit_code(Some("")));
        assert!(!clean_stop_exit_code(None));
    }
}
