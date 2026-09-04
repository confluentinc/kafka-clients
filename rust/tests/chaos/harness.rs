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

//! The chaos orchestrator: owns a dedicated (non-pooled) cluster and an
//! AdminClient, runs a set of **pluggable** [`Workload`]s, and injects broker
//! faults. It knows nothing about `KafkaProducer` / `Consumer` — the workload
//! is the plug-in seam (`design/current/chaos-fault-injection-harness.md`
//! §3–§5).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, CreateTopicsOptions, DescribeTopicsOptions, NewTopic, new_admin_client,
};
use confluent_kafka::common::{TopicCollection, Uuid};

use super::common::broker_control::BrokerControl;
use super::common::cluster_config::kip848_3_broker;
use super::common::kafka_cluster::KafkaCluster;
use super::verifier::{ConservationVerifier, ExpectedLossHint, Verifier};
use super::workload::{CommitMode, Role, Workload, WorkloadContext, WorkloadSpec, build_workload};

/// A running chaos harness for one scenario.
///
/// Holds its **own** [`KafkaCluster`] (never the pooled one — chaos
/// stops/kills brokers, which would corrupt other tests and race pool
/// eviction; see the design doc §2). Tear it down with [`Self::shutdown`].
pub struct ChaosHarness {
    cluster: KafkaCluster,
    admin: Box<dyn Admin>,
    topic: String,
    partitions: i32,
    /// Current topic id, for the physical verification key. Re-resolved after
    /// a topic recreate (the id changes). `Uuid::zero()` if unresolved.
    topic_id: std::sync::Mutex<Uuid>,
    /// The pluggable verifier every workload writes into (default:
    /// conservation). Swap this to check something else (e.g. share-consumer
    /// acks) without touching the orchestrator.
    verifier: Arc<dyn Verifier>,
}

impl ChaosHarness {
    /// Stand up a dedicated `brokers`-broker KIP-848 cluster and create the
    /// chaos topic with `partitions` partitions at replication factor 3.
    pub async fn start(topic: &str, brokers: u16, partitions: i32) -> Self {
        Self::start_with_verifier(topic, brokers, partitions, Arc::new(ConservationVerifier::new())).await
    }

    /// Like [`Self::start`] but with a caller-supplied verifier — the seam a
    /// future share consumer uses to plug in a `ShareAckVerifier`.
    pub async fn start_with_verifier(topic: &str, brokers: u16, partitions: i32, verifier: Arc<dyn Verifier>) -> Self {
        assert!(brokers >= 1, "chaos cluster needs >= 1 broker");
        let mut config = kip848_3_broker(partitions as u16);
        config.brokers = brokers;
        // Disable broker-side auto topic creation: the topic-recreate action
        // deletes the topic while workloads keep running, and their metadata
        // requests would otherwise silently recreate it with the broker's
        // default partition count — racing (and conflicting with) our explicit
        // recreate. The chaos harness owns topic lifecycle explicitly.
        config
            .server_properties
            .insert("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string());
        let cluster = KafkaCluster::start_with_config(&config).await;

        let admin = {
            let props = HashMap::from([
                ("bootstrap.servers".to_string(), cluster.bootstrap_servers().to_string()),
                ("client.id".to_string(), "chaos-admin".to_string()),
            ]);
            new_admin_client(AdminClientConfig::from_properties(&props).expect("valid admin config"))
                .expect("admin client")
        };

        let harness = Self {
            cluster,
            admin,
            topic: topic.to_string(),
            partitions,
            topic_id: std::sync::Mutex::new(Uuid::zero()),
            verifier,
        };
        harness.create_topic().await;
        harness.resolve_topic_id().await;
        harness
    }

    async fn create_topic(&self) {
        let replication = self.cluster.config().brokers.min(3) as i16;
        let new_topic = NewTopic::new(self.topic.clone(), self.partitions, replication);
        self.admin
            .create_topics(&[new_topic], CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .expect("chaos topic creation failed");
    }

    /// Resolve the current topic id (for the physical verification key) via
    /// `describe_topics`, and cache it. Best-effort: leaves `Uuid::zero()` on
    /// failure. Called after create and after each recreate (the id changes).
    async fn resolve_topic_id(&self) {
        let described = self
            .admin
            .describe_topics(
                TopicCollection::of_topic_names(vec![self.topic.clone()]),
                DescribeTopicsOptions::new(),
            )
            .all_topic_names()
            .expect("describe by name yields a name-keyed result")
            .get()
            .await;
        if let Ok(map) = described
            && let Some(desc) = map.get(&self.topic)
        {
            *self.topic_id.lock().expect("topic_id poisoned") = desc.topic_id();
        }
    }

    fn current_topic_id(&self) -> Uuid {
        *self.topic_id.lock().expect("topic_id poisoned")
    }

    /// Delete and recreate the chaos topic, optionally dwelling `dwell`
    /// between delete and recreate (librdkafka's `--topic-chaos
    /// recreate-immediate` / `recreate-delayed`).
    ///
    /// All records delivered before the delete are marked **expected-lost** in
    /// the verifier: the topic is gone, so the consumer legitimately cannot see
    /// them and they must not be scored as data loss.
    pub async fn recreate_topic(&self, dwell: Duration) {
        eprintln!("chaos: recreating topic {} (dwell {:?})", self.topic, dwell);
        // Capture the current topic id so we can prove the recreate produced a
        // genuinely new generation (a different id), not the old topic lingering.
        let old_id = self.current_topic_id();
        // Snapshot what's already delivered as expected-lost BEFORE deleting.
        self.verifier.note_expected_loss(ExpectedLossHint::AllDeliveredSoFar);

        self.admin
            .delete_topics(
                confluent_kafka::common::TopicCollection::of_topic_names(vec![self.topic.clone()]),
                confluent_kafka::admin::DeleteTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .expect("chaos topic delete failed");

        // `delete_topics` returning success only means the deletion was
        // accepted; the topic's removal propagates asynchronously across the
        // cluster, so an immediate recreate races with `TopicAlreadyExists`.
        // Wait until describe no longer sees it before recreating. The dwell
        // (recreate-delayed) is applied AFTER the topic is confirmed gone.
        self.wait_topic_absent(Duration::from_secs(30)).await;

        if !dwell.is_zero() {
            tokio::time::sleep(dwell).await;
        }

        // Recreate with the same shape, retrying while the broker still reports
        // the old topic as not-yet-collected.
        self.create_topic_retrying(Duration::from_secs(30)).await;

        // The recreated topic has a NEW topic id — re-resolve so post-recreate
        // records are keyed under the new generation.
        self.resolve_topic_id().await;
        let new_id = self.current_topic_id();

        // Prove the recreate produced a new generation: the id must differ from
        // the old one (unless we never resolved either, e.g. Uuid::zero()).
        eprintln!("chaos: topic {} recreated: id {old_id} -> {new_id}", self.topic);
        if old_id != Uuid::zero() && new_id != Uuid::zero() {
            assert_ne!(
                old_id, new_id,
                "topic-recreate produced the same topic id for {} — the topic was not \
                 actually recreated as a new generation",
                self.topic
            );
        }
    }

    /// Poll until `describe_topics` no longer finds the chaos topic.
    async fn wait_topic_absent(&self, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let described = self
                .admin
                .describe_topics(
                    confluent_kafka::common::TopicCollection::of_topic_names(vec![self.topic.clone()]),
                    confluent_kafka::admin::DescribeTopicsOptions::new(),
                )
                .all_topic_names()
                .expect("describe by name")
                .get()
                .await;
            // Absent when describe errors with unknown-topic, or returns no
            // entry for our topic.
            let absent = match described {
                Ok(map) => !map.contains_key(&self.topic),
                Err(_) => true,
            };
            if absent {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "topic {} was not deleted within {timeout:?}",
                self.topic
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Create the topic, retrying while the broker still reports the previous
    /// generation as pending deletion (`TopicAlreadyExists`).
    async fn create_topic_retrying(&self, timeout: Duration) {
        let replication = self.cluster.config().brokers.min(3) as i16;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let new_topic = NewTopic::new(self.topic.clone(), self.partitions, replication);
            match self
                .admin
                .create_topics(&[new_topic], CreateTopicsOptions::new())
                .all()
                .get()
                .await
            {
                Ok(()) => return,
                Err(err) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "recreate of topic {} did not succeed within {timeout:?}: {err}",
                        self.topic
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                },
            }
        }
    }

    /// AdminClient handle (for readiness detection / actions).
    pub fn admin(&self) -> &dyn Admin {
        self.admin.as_ref()
    }

    /// Broker fault-injection controller over this cluster.
    pub fn brokers(&self) -> BrokerControl<'_> {
        BrokerControl::new(&self.cluster)
    }

    /// The verifier every workload writes into — the runner reads its verdict
    /// at drain.
    pub fn verifier(&self) -> Arc<dyn Verifier> {
        self.verifier.clone()
    }

    /// Build the given workloads. They are **not** spawned onto separate
    /// threads (the client futures are not `Send`-guaranteed at the trait
    /// boundary); instead [`RunningWorkloads::drive`] runs them concurrently
    /// with the chaos actions on the scenario's own multi-thread task via
    /// `join_all`. Each carries an independent stop flag so producers can be
    /// drained before consumers.
    pub async fn build_workloads(
        &self,
        specs: &[WorkloadSpec],
        target_rps: u32,
        commit_mode: CommitMode,
    ) -> RunningWorkloads {
        let ctx = WorkloadContext {
            bootstrap: self.cluster.bootstrap_servers().to_string(),
            container_bootstrap: self.cluster.container_bootstrap_servers().to_string(),
            topic: self.topic.clone(),
            topic_id: self.current_topic_id(),
            group: format!("chaos-group-{}", self.topic),
            target_rps,
            commit_mode,
        };

        let mut running = Vec::with_capacity(specs.len());
        for spec in specs {
            let workload: Box<dyn Workload> =
                build_workload(spec, ctx.clone(), self.verifier.clone(), self.cluster.network_name())
                    .await
                    .expect("failed to build workload");
            let stop = Arc::new(AtomicBool::new(false));
            running.push(RunningWorkload { role: spec.role, stop, workload });
        }

        RunningWorkloads { workloads: running }
    }

    /// Tear down the cluster's containers. Call at the end of every scenario.
    pub fn shutdown(self) {
        for id in self.cluster.container_ids() {
            let _ = std::process::Command::new("docker").args(["rm", "-f", id]).output();
        }
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", self.cluster.network_name()])
            .output();
    }
}

/// One built (unspawned) workload plus its stop flag.
struct RunningWorkload {
    role: Role,
    stop: Arc<AtomicBool>,
    workload: Box<dyn Workload>,
}

/// All workloads for a scenario, ready to be driven concurrently with chaos.
pub struct RunningWorkloads {
    workloads: Vec<RunningWorkload>,
}

impl RunningWorkloads {
    /// Run all workloads concurrently with `scenario` (the chaos timeline),
    /// then perform the cooldown → drain: stop producers, give consumers
    /// `drain` to catch up on the tail, stop consumers, and wait for every
    /// workload to finish. Everything runs on the caller's (multi-thread)
    /// task — no cross-thread spawn — so the non-`Send` client futures are
    /// fine. Mirrors the librdkafka cooldown→drain sequence.
    pub async fn drive<Fut>(self, drain: Duration, scenario: Fut)
    where
        Fut: std::future::Future<Output = ()>,
    {
        // Split flags out before consuming the workloads into futures.
        let producer_stops: Vec<Arc<AtomicBool>> = self
            .workloads
            .iter()
            .filter(|w| matches!(w.role, Role::Producer))
            .map(|w| w.stop.clone())
            .collect();
        let consumer_stops: Vec<Arc<AtomicBool>> = self
            .workloads
            .iter()
            .filter(|w| matches!(w.role, Role::Consumer))
            .map(|w| w.stop.clone())
            .collect();

        // The control future: run the scenario, then drain producers → wait →
        // drain consumers. Runs concurrently with the workloads below.
        let control = async move {
            scenario.await;
            for stop in &producer_stops {
                stop.store(true, Ordering::Relaxed);
            }
            tokio::time::sleep(drain).await;
            for stop in &consumer_stops {
                stop.store(true, Ordering::Relaxed);
            }
        };

        // Drive every workload to completion (each returns when its stop flag
        // is set) concurrently with the control future.
        let workload_futs = futures_util::future::join_all(self.workloads.into_iter().map(|w| w.workload.run(w.stop)));

        tokio::join!(control, workload_futs);
    }
}
