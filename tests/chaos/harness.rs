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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// Drain-settle signal shared between [`ChaosHarness::recreate_topic`] and the
/// drain loop (`drain_wait`).
///
/// A topic recreate causes a legitimate, transient consumption stall: the
/// consumer must re-discover the recreated topic (a truncation-rewind / reset to
/// EARLIEST on a same-id immediate recreate, or a fresh subscription on a new-id
/// one) and then re-read it before it resumes on that topic. During that stall
/// the recreated topic's consumption is momentarily flat, which the idle-based
/// early-drain would otherwise misread as "consumers caught up" and end the
/// drain prematurely — dropping the just-recreated topic's tail as false loss
/// (the Issue-1 multi-topic failure).
///
/// The GLOBAL consume-progress signal cannot catch this: while the recreated
/// topic is rewinding, the OTHER topics keep being consumed, so the global
/// counter keeps climbing and looks like "resumed". So the settle records, PER
/// recreated topic, the topic's per-topic consume count at recreate time
/// (`baseline`); the drain must not quiesce until that topic's per-topic
/// progress has climbed past its baseline (proof the recreated topic itself
/// resumed) AND then gone flat for the idle threshold. Multiple recreates of the
/// same topic keep the latest (highest) baseline.
#[derive(Default)]
pub struct RecreateSettle {
    /// topic -> per-topic consume count captured at that topic's most recent
    /// recreate. The drain requires the topic's live per-topic progress to
    /// exceed this before it may quiesce.
    baselines: std::sync::Mutex<HashMap<String, u64>>,
}

impl RecreateSettle {
    /// Record that `topic` was just recreated, capturing its per-topic consume
    /// count at this instant as the resume baseline.
    fn note_recreate(&self, topic: &str, baseline: u64) {
        self.baselines
            .lock()
            .expect("recreate-settle poisoned")
            .insert(topic.to_string(), baseline);
    }

    /// Snapshot of the per-topic resume baselines — the drain reads this to know
    /// which topics were recreated and the progress each must exceed to count as
    /// resumed.
    pub fn baselines(&self) -> HashMap<String, u64> {
        self.baselines.lock().expect("recreate-settle poisoned").clone()
    }
}

/// A running chaos harness for one scenario.
///
/// Holds its **own** [`KafkaCluster`] (never the pooled one — chaos
/// stops/kills brokers, which would corrupt other tests and race pool
/// eviction; see the design doc §2). Tear it down with [`Self::shutdown`].
pub struct ChaosHarness {
    cluster: KafkaCluster,
    admin: Box<dyn Admin>,
    /// All topics in the run (`--num-topics`). A single-topic run has one
    /// entry; `topics[0]` is the primary/prefix topic.
    topics: Vec<String>,
    partitions: i32,
    /// Replication factor for every topic. `min(brokers, 3)` unless overridden
    /// by `--replication-factor`.
    replication: i16,
    /// Per-topic current topic id, for the physical verification key.
    /// Re-resolved after a topic recreate (the id changes). Missing / absent =
    /// unresolved (treated as `Uuid::zero()`).
    topic_ids: std::sync::Mutex<HashMap<String, Uuid>>,
    /// Drain-settle signal: per-topic resume baselines captured at each recreate.
    /// `recreate_topic` records one; the drain reads them (via
    /// [`Self::recreate_settle`]) to suppress idle-quiescence until every
    /// just-recreated topic's own consume progress has climbed past its baseline
    /// (proof that topic's consumer resumed re-reading the new generation). See
    /// [`RecreateSettle`] and `drain_wait`.
    recreate_settle: Arc<RecreateSettle>,
    /// Producer rate and consumer commit mode captured at `build_workloads`,
    /// so the runtime [`WorkloadPool`] can build matching consumers.
    rps: std::sync::atomic::AtomicU32,
    /// Producer value payload size in bytes, captured at `build_workloads`.
    msg_size: std::sync::atomic::AtomicU32,
    commit_mode: std::sync::Mutex<CommitMode>,
    /// The pluggable verifier every workload writes into (default:
    /// conservation). Swap this to check something else (e.g. share-consumer
    /// acks) without touching the orchestrator.
    verifier: Arc<dyn Verifier>,
}

impl ChaosHarness {
    /// Stand up a dedicated `brokers`-broker KIP-848 cluster and create a single
    /// chaos topic with `partitions` partitions at replication `min(brokers, 3)`.
    pub async fn start(topic: &str, brokers: u16, partitions: i32) -> Self {
        Self::start_with_verifier(topic, brokers, partitions, Arc::new(ConservationVerifier::new())).await
    }

    /// Like [`Self::start`] but with a caller-supplied verifier — the seam a
    /// future share consumer uses to plug in a `ShareAckVerifier`.
    pub async fn start_with_verifier(topic: &str, brokers: u16, partitions: i32, verifier: Arc<dyn Verifier>) -> Self {
        Self::start_with_topics(&[topic.to_string()], brokers, partitions, None, verifier).await
    }

    /// The multi-topic entry point used by the flag-driven runner. Creates every
    /// topic in `topics` with `partitions` partitions at `replication` (or
    /// `min(brokers, 3)` when `None`). Panics (librdkafka's FATAL) if an explicit
    /// replication exceeds the broker count.
    pub async fn start_with_topics(
        topics: &[String],
        brokers: u16,
        partitions: i32,
        replication: Option<i16>,
        verifier: Arc<dyn Verifier>,
    ) -> Self {
        assert!(brokers >= 1, "chaos cluster needs >= 1 broker");
        assert!(!topics.is_empty(), "chaos run needs >= 1 topic");
        let replication = match replication {
            Some(rf) => {
                assert!(rf >= 1, "replication factor must be >= 1, got {rf}");
                assert!(
                    i64::from(rf) <= i64::from(brokers),
                    "FATAL: --replication-factor={rf} > --brokers={brokers}"
                );
                rf
            },
            None => (brokers.min(3)) as i16,
        };
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
            topics: topics.to_vec(),
            partitions,
            replication,
            topic_ids: std::sync::Mutex::new(HashMap::new()),
            recreate_settle: Arc::new(RecreateSettle::default()),
            rps: std::sync::atomic::AtomicU32::new(0),
            msg_size: std::sync::atomic::AtomicU32::new(100),
            commit_mode: std::sync::Mutex::new(CommitMode::Sync),
            verifier,
        };
        for t in &harness.topics {
            harness.create_topic(t).await;
        }
        harness.resolve_topic_id_all().await;
        harness
    }

    /// The primary topic (first in the run) — the broker-roll / smoke-test
    /// leader-sampling target, and the `WorkloadContext::topic` fallback.
    fn primary_topic(&self) -> &str {
        &self.topics[0]
    }

    async fn create_topic(&self, topic: &str) {
        let new_topic = NewTopic::new(topic.to_string(), self.partitions, self.replication);
        self.admin
            .create_topics(&[new_topic], CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .expect("chaos topic creation failed");
    }

    /// Resolve the current topic id (physical verification key) for every topic.
    async fn resolve_topic_id_all(&self) {
        for t in self.topics.clone() {
            self.resolve_topic_id(&t).await;
        }
    }

    /// Resolve the current topic id (for the physical verification key) via
    /// `describe_topics`, and cache it. **Retries until a non-zero id is
    /// resolved**, bounded by `timeout` — under a concurrent broker roll,
    /// `describe_topics` can transiently return a zero/absent id (metadata
    /// churn), and keying post-recreate records under `Uuid::zero()` corrupts the
    /// physical accounting. Logs loudly if it genuinely cannot resolve within the
    /// bound (leaving the previous cached value untouched). Called after create
    /// and after each recreate (the id changes).
    async fn resolve_topic_id(&self, topic: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let described = self
                .admin
                .describe_topics(
                    TopicCollection::of_topic_names(vec![topic.to_string()]),
                    DescribeTopicsOptions::new(),
                )
                .all_topic_names()
                .expect("describe by name yields a name-keyed result")
                .get()
                .await;
            if let Ok(map) = described
                && let Some(desc) = map.get(topic)
            {
                let id = desc.topic_id();
                if id != Uuid::zero() {
                    self.topic_ids.lock().expect("topic_ids poisoned").insert(topic.to_string(), id);
                    return;
                }
            }
            if std::time::Instant::now() >= deadline {
                eprintln!(
                    "chaos: WARNING could not resolve a non-zero topic id for {topic} within 10s \
                     (metadata churn under broker roll); physical keying for it may be degraded"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn current_topic_id(&self, topic: &str) -> Uuid {
        self.topic_ids
            .lock()
            .expect("topic_ids poisoned")
            .get(topic)
            .copied()
            .unwrap_or_else(Uuid::zero)
    }

    /// Snapshot of every topic's current id (for the consumer's per-topic key).
    fn topic_ids_snapshot(&self) -> HashMap<String, Uuid> {
        self.topic_ids.lock().expect("topic_ids poisoned").clone()
    }

    /// Delete and recreate ONE chaos topic, optionally dwelling `dwell` between
    /// delete and recreate (librdkafka's `--topic-chaos recreate-immediate` /
    /// `recreate-delayed`). librdkafka's `_topic_chaos_thread` picks a single
    /// topic (`rng.choice(topics)`); the caller passes the chosen topic here.
    ///
    /// All records delivered on THIS topic before the delete are marked
    /// **expected-lost** in the verifier: the topic is gone, so the consumer
    /// legitimately cannot see them and they must not be scored as data loss.
    /// Loss on the other topics is still real loss (scoped hint).
    pub async fn recreate_topic(&self, topic: &str, dwell: Duration) {
        assert!(
            self.topics.iter().any(|t| t == topic),
            "recreate_topic called with unknown topic {topic}"
        );
        eprintln!("chaos: recreating topic {topic} (dwell {dwell:?})");
        // Capture the current topic id so we can prove the recreate produced a
        // genuinely new generation (a different id), not the old topic lingering.
        let old_id = self.current_topic_id(topic);
        // Snapshot what's already delivered on this topic as expected-lost
        // BEFORE deleting. For a single-topic run this is every delivered
        // record; for a multi-topic run only this topic's records.
        let hint = if self.topics.len() == 1 {
            ExpectedLossHint::AllDeliveredSoFar
        } else {
            ExpectedLossHint::AllDeliveredForTopic(topic.to_string())
        };
        self.verifier.note_expected_loss(hint);

        self.admin
            .delete_topics(
                confluent_kafka::common::TopicCollection::of_topic_names(vec![topic.to_string()]),
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
        self.wait_topic_absent(topic, Duration::from_secs(30)).await;

        if !dwell.is_zero() {
            tokio::time::sleep(dwell).await;
        }

        // Recreate with the same shape, retrying while the broker still reports
        // the old topic as not-yet-collected.
        self.create_topic_retrying(topic, Duration::from_secs(30)).await;

        // Re-resolve the topic id so post-recreate records are keyed under the
        // new generation. (librdkafka restarts the topic's producer here; our
        // in-process producer keeps running against the same topic name — the
        // re-resolved id is what it and the consumer key new records against.)
        self.resolve_topic_id(topic).await;
        let new_id = self.current_topic_id(topic);

        eprintln!("chaos: topic {topic} recreated: id {old_id} -> {new_id}");

        // Mark the topic as a recreate BLACKOUT. An immediate recreate (dwell 0)
        // can reuse the SAME topic id (in-place topic_id mutation — a
        // librdkafka-documented mode); when it does, the consumer's committed
        // offset from the old generation still points into this topic_id, so it
        // is now *past* the recreated topic's reset offsets and
        // `auto.offset.reset` does not fire — the new generation's low-offset
        // records are silently skipped until the topic's offsets climb back past
        // the stale commit and the consumer resumes. The in-process producer's
        // index does NOT reset across the recreate (librdkafka restarts its
        // per-topic producer; we cannot), so it keeps acking records into that
        // whole blackout window.
        //
        // Rather than guess the window with a wall-clock snapshot, the verifier
        // computes it from observed data: a delivered-but-unobserved record on a
        // blackout topic below the highest index the consumer eventually observed
        // on it was skipped in the blackout and is expected-lost; records above
        // the resume point remain in the loss check. This is librdkafka's
        // per-topic pre-delete-HWM window, and it is robust to whether the
        // recreate reused the id, minted a new one, or the id could not be
        // resolved under churn (all three occur — see the `old_id`/`new_id` log).
        self.verifier
            .note_expected_loss(ExpectedLossHint::RecreateBlackout(topic.to_string()));

        // Effect check — but only for recreate-DELAYED. With a dwell long enough
        // for the deletion to propagate, the recreate MUST be a genuinely new
        // generation (different topic id). recreate-IMMEDIATE (dwell 0) can
        // legitimately reuse the same id (in-place topic_id mutation — librdkafka
        // documents this as a valid mode), especially when the cluster metadata
        // is still churning from a concurrent broker roll, so we do not assert
        // there. Either way the id is now re-resolved for keying.
        if !dwell.is_zero() && old_id != Uuid::zero() && new_id != Uuid::zero() {
            assert_ne!(
                old_id, new_id,
                "recreate-delayed produced the same topic id for {topic} — the topic was not \
                 recreated as a new generation despite the dwell"
            );
        }

        // Arm the drain-settle for THIS topic: the consumer is now about to
        // re-discover / re-read the recreated topic; the drain must not declare
        // idle-quiescence until this topic's OWN consume progress climbs past the
        // baseline captured here (proof it resumed) and then goes flat. Capturing
        // the per-topic baseline now — not a global count — is what lets the
        // drain tell this one topic's stall apart from the others still flowing.
        let baseline = self.verifier.consumed_progress_for_topic(topic);
        self.recreate_settle.note_recreate(topic, baseline);
    }

    /// Poll until `describe_topics` no longer finds `topic`.
    async fn wait_topic_absent(&self, topic: &str, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let described = self
                .admin
                .describe_topics(
                    confluent_kafka::common::TopicCollection::of_topic_names(vec![topic.to_string()]),
                    confluent_kafka::admin::DescribeTopicsOptions::new(),
                )
                .all_topic_names()
                .expect("describe by name")
                .get()
                .await;
            // Absent when describe errors with unknown-topic, or returns no
            // entry for our topic.
            let absent = match described {
                Ok(map) => !map.contains_key(topic),
                Err(_) => true,
            };
            if absent {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "topic {topic} was not deleted within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Create `topic`, retrying while the broker still reports the previous
    /// generation as pending deletion (`TopicAlreadyExists`).
    async fn create_topic_retrying(&self, topic: &str, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let new_topic = NewTopic::new(topic.to_string(), self.partitions, self.replication);
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
                        "recreate of topic {topic} did not succeed within {timeout:?}: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                },
            }
        }
    }

    /// The topics in this run (in order) — the runner uses this to pick a random
    /// recreate target and to apply migrations across the whole set.
    pub fn topics(&self) -> &[String] {
        &self.topics
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

    /// The drain-settle signal — pass to [`RunningWorkloads::drive`] so the
    /// drain suppresses idle-quiescence while a just-recreated topic's consumer
    /// is still recovering.
    pub fn recreate_settle(&self) -> Arc<RecreateSettle> {
        self.recreate_settle.clone()
    }

    /// Build the given workloads. They are **not** spawned onto separate
    /// threads (the client futures are not `Send`-guaranteed at the trait
    /// boundary); instead [`RunningWorkloads::drive`] runs them concurrently
    /// with the chaos actions on the scenario's own multi-thread task via
    /// `join_all`. Each carries an independent stop flag so producers can be
    /// drained before consumers.
    /// Build the immutable per-workload context from current cluster state,
    /// bound to `topic` as the producer's target topic. Consumers ignore
    /// `topic` and subscribe to the full `topics` set. Shared by
    /// `build_workloads` and the runtime `WorkloadPool`. The group is keyed on
    /// the primary topic so every consumer joins the same group.
    fn workload_ctx(&self, topic: &str) -> WorkloadContext {
        WorkloadContext {
            bootstrap: self.cluster.bootstrap_servers().to_string(),
            container_bootstrap: self.cluster.container_bootstrap_servers().to_string(),
            topic: topic.to_string(),
            topics: self.topics.clone(),
            topic_id: self.current_topic_id(topic),
            topic_ids: self.topic_ids_snapshot(),
            group: format!("chaos-group-{}", self.primary_topic()),
            target_rps: self.rps.load(Ordering::Relaxed),
            msg_size: self.msg_size.load(Ordering::Relaxed) as usize,
            commit_mode: *self.commit_mode.lock().expect("commit_mode poisoned"),
        }
    }

    /// A [`WorkloadPool`] for adding/removing consumers mid-run. Capture it in
    /// the scenario; pass the same pool to [`RunningWorkloads::drive`]. Its
    /// context is bound to the primary topic, but pool-added consumers subscribe
    /// to every topic (they are consumers).
    pub fn workload_pool(&self) -> WorkloadPool<'_> {
        WorkloadPool {
            harness: self,
            ctx: self.workload_ctx(self.primary_topic()),
            inner: std::rc::Rc::new(WorkloadPoolInner {
                pending: std::cell::RefCell::new(Vec::new()),
                added_consumer_stops: std::cell::RefCell::new(Vec::new()),
                next_instance: std::cell::Cell::new(1000), // runtime ids start high
            }),
        }
    }

    pub async fn build_workloads(
        &self,
        specs: &[WorkloadSpec],
        target_rps: u32,
        msg_size: usize,
        commit_mode: CommitMode,
    ) -> RunningWorkloads {
        // Remember these so a runtime WorkloadPool builds matching consumers.
        self.rps.store(target_rps, Ordering::Relaxed);
        self.msg_size.store(msg_size as u32, Ordering::Relaxed);
        *self.commit_mode.lock().expect("commit_mode poisoned") = commit_mode;

        let mut running = Vec::with_capacity(specs.len());
        for spec in specs {
            match spec.role {
                // One producer per topic (librdkafka spawns one perf producer
                // per test topic, each at the per-topic `rps`). A single
                // `producer:rust` spec therefore expands to N producers when
                // `--num-topics N`; each is bound to its own topic.
                Role::Producer => {
                    for (topic_idx, topic) in self.topics.iter().enumerate() {
                        let ctx = self.workload_ctx(topic);
                        // Each per-topic producer needs a DISTINCT client.id and
                        // log identity (librdkafka names them producer-<topic
                        // index>). A shared client.id across N producers collides
                        // their logs and confuses per-client broker bookkeeping.
                        // For a single-topic run keep the original spec unchanged
                        // (label `producer-<backend>-<instance>`), so N==1
                        // behaviour is identical.
                        let per_topic_spec = if self.topics.len() == 1 {
                            spec.clone()
                        } else {
                            let mut s = spec.clone();
                            // Derive a stable, unique instance from the base
                            // instance and the topic index (e.g. base 1, 3
                            // topics -> 100, 101, 102), so ids stay distinct even
                            // with multiple producer specs.
                            s.instance = spec.instance * 100 + topic_idx as u32;
                            s
                        };
                        let workload: Box<dyn Workload> =
                            build_workload(&per_topic_spec, ctx, self.verifier.clone(), self.cluster.network_name())
                                .await
                                .expect("failed to build producer workload");
                        let stop = Arc::new(AtomicBool::new(false));
                        running.push(RunningWorkload { role: spec.role, stop, workload });
                    }
                },
                // One consumer per spec, subscribing to ALL topics.
                Role::Consumer => {
                    let ctx = self.workload_ctx(self.primary_topic());
                    let workload: Box<dyn Workload> =
                        build_workload(spec, ctx, self.verifier.clone(), self.cluster.network_name())
                            .await
                            .expect("failed to build consumer workload");
                    let stop = Arc::new(AtomicBool::new(false));
                    running.push(RunningWorkload { role: spec.role, stop, workload });
                },
            }
        }

        RunningWorkloads { workloads: running }
    }

    /// Tear down the cluster's containers. Call at the end of every scenario.
    ///
    /// The teardown itself lives in [`Drop`], so it also runs when a scenario
    /// **panics** (a broker that fails to recover, a failed verdict assertion,
    /// a describe timeout, …). Without that, every panicking run leaked its whole
    /// cluster, and the orphaned brokers starved the next run. Dropping `self`
    /// here performs the teardown; kept as an explicit, readable end-of-scenario
    /// call.
    pub fn shutdown(self) {
        drop(self);
    }
}

impl Drop for ChaosHarness {
    /// Best-effort teardown of the dedicated cluster's containers and network.
    /// Runs both on the success path (via [`ChaosHarness::shutdown`]) and on
    /// panic unwind, so a failed run cannot leak its brokers into the next one.
    fn drop(&mut self) {
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

/// A pending workload the scenario asked to add mid-run: its role, stop flag,
/// and run future, waiting to be pushed into the live `FuturesUnordered`.
type PendingWorkload = (Role, Arc<AtomicBool>, std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>);

/// Shared inner state of a [`WorkloadPool`], held behind `Rc` so the scenario
/// future can capture a clone and still let [`RunningWorkloads::drive`] poll
/// the queue it pushes into.
struct WorkloadPoolInner {
    /// Newly-built consumer futures waiting for the drive loop to poll them.
    pending: std::cell::RefCell<Vec<PendingWorkload>>,
    /// Stop flags of consumers added at runtime, newest last — `remove` pops.
    added_consumer_stops: std::cell::RefCell<Vec<Arc<AtomicBool>>>,
    next_instance: std::cell::Cell<u32>,
}

/// Handle the scenario uses to add or remove **consumer** workloads mid-run,
/// forcing a group rebalance (librdkafka's `--rebalance-add-cycle` /
/// `--rebalance-remove-cycle`). `Rc`-cloneable and not `Send`/`Sync` — it lives
/// on the single scenario task alongside the workload futures. Obtain one from
/// [`ChaosHarness::workload_pool`], capture it in the scenario, and pass its
/// [`WorkloadPool::pending_queue`] to `drive`.
#[derive(Clone)]
pub struct WorkloadPool<'h> {
    harness: &'h ChaosHarness,
    ctx: WorkloadContext,
    inner: std::rc::Rc<WorkloadPoolInner>,
}

impl<'h> WorkloadPool<'h> {
    /// The shared pending-workload queue `drive` drains into its live set.
    fn pending_queue(&self) -> std::rc::Rc<WorkloadPoolInner> {
        self.inner.clone()
    }

    /// Add a consumer workload of `backend` to the running set. It joins the
    /// group on its next poll, triggering a rebalance.
    pub async fn add_consumer(&self, backend: super::workload::Backend) {
        let instance = self.inner.next_instance.get();
        self.inner.next_instance.set(instance + 1);
        let spec = WorkloadSpec { role: Role::Consumer, backend, instance };
        let label = spec.label();
        let workload = build_workload(
            &spec,
            self.ctx.clone(),
            self.harness.verifier.clone(),
            self.harness.cluster.network_name(),
        )
        .await
        .expect("failed to build dynamic consumer workload");
        let stop = Arc::new(AtomicBool::new(false));
        self.inner.added_consumer_stops.borrow_mut().push(stop.clone());
        let fut = workload.run(stop.clone());
        self.inner.pending.borrow_mut().push((Role::Consumer, stop, Box::pin(fut)));
        eprintln!("chaos: added consumer {label} (rebalance)");
    }

    /// Stop the most-recently-added runtime consumer, triggering a rebalance.
    /// No-op if none were added.
    pub fn remove_consumer(&self) {
        if let Some(stop) = self.inner.added_consumer_stops.borrow_mut().pop() {
            stop.store(true, Ordering::Relaxed);
            eprintln!("chaos: removed a dynamically-added consumer (rebalance)");
        } else {
            eprintln!("chaos: remove_consumer requested but none were dynamically added");
        }
    }
}

/// All workloads for a scenario, ready to be driven concurrently with chaos.
pub struct RunningWorkloads {
    workloads: Vec<RunningWorkload>,
}

impl RunningWorkloads {
    /// Run all workloads concurrently with `scenario` (the chaos timeline),
    /// then perform the cooldown → drain: stop producers, give consumers
    /// `drain` to catch up on the tail, stop consumers, and wait for every
    /// workload to finish. `pool` is the [`WorkloadPool`] the scenario captured
    /// (from [`ChaosHarness::workload_pool`]); consumers it adds mid-run are
    /// absorbed into the live set here. Everything runs on the caller's
    /// (multi-thread) task — no cross-thread spawn — so the non-`Send` client
    /// futures are fine. Mirrors the librdkafka cooldown→drain sequence.
    /// `drain` is the maximum cooldown wait; `idle_threshold` (if non-zero)
    /// ends the drain early once consumption has been quiet for that long
    /// (idle-based early drain — see [`Self::drain_wait`]). `verifier` supplies
    /// the consume-progress signal.
    pub async fn drive<Fut>(
        self,
        pool: &WorkloadPool<'_>,
        drain: Duration,
        idle_threshold: Duration,
        verifier: Arc<dyn Verifier>,
        settle: Arc<RecreateSettle>,
        scenario: Fut,
    ) where
        Fut: std::future::Future<Output = ()>,
    {
        use futures_util::stream::{FuturesUnordered, StreamExt};

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

        // Wait for producers to FINISH (not just be signalled) before draining: a
        // producer still flushes its backlog after the stop flag is set (largest
        // right after a recreate), and draining first would deliver its tail with
        // no live consumer — the flaky tail loss. Each producer future bumps
        // `producers_done` on completion.
        let producer_count = producer_stops.len();
        let producers_done = Arc::new(AtomicUsize::new(0));

        // Live set of running workload futures; new ones are pushed in while it
        // is being polled (that is what `FuturesUnordered` allows and a static
        // `join_all` does not — required for mid-run add/remove).
        let mut live: FuturesUnordered<std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>> =
            FuturesUnordered::new();
        for w in self.workloads {
            match w.role {
                Role::Producer => {
                    let done = producers_done.clone();
                    live.push(Box::pin(async move {
                        w.workload.run(w.stop).await;
                        done.fetch_add(1, Ordering::Relaxed);
                    }));
                },
                Role::Consumer => {
                    live.push(Box::pin(w.workload.run(w.stop)));
                },
            }
        }

        // The scenario (which may push consumers into the pool) followed by the
        // cooldown → drain sequence.
        let control = async move {
            scenario.await;
            // (1) Signal producers, then wait for them to finish so the delivered
            // set is final before draining (they run concurrently via `select!`).
            for stop in &producer_stops {
                stop.store(true, Ordering::Relaxed);
            }
            while producers_done.load(Ordering::Relaxed) < producer_count {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            // (2) Drain until all delivered observed (or `drain` elapses); (3) stop consumers.
            drain_wait(drain, idle_threshold, verifier.as_ref(), settle.as_ref()).await;
            for stop in &consumer_stops {
                stop.store(true, Ordering::Relaxed);
            }
        };
        let mut control = Box::pin(control);
        let mut control_done = false;
        let pending = pool.pending_queue();

        // Drive loop: advance the control future and the live workload set
        // together, absorbing any consumers the scenario adds. Ends when the
        // control future has finished AND every workload future has drained.
        loop {
            // Absorb newly-added workloads before polling the set.
            for (_role, _stop, fut) in pending.pending.borrow_mut().drain(..) {
                live.push(fut);
            }
            tokio::select! {
                biased;
                _ = &mut control, if !control_done => { control_done = true; }
                next = live.next(), if !live.is_empty() => {
                    let _ = next; // one workload finished; keep going
                }
                else => {}
            }
            if control_done && live.is_empty() && pending.pending.borrow().is_empty() {
                break;
            }
        }
    }
}

/// Cooldown drain: wait up to `max` for consumers to catch up on the tail.
///
/// With `idle_threshold` = 0, waits the full `max` (a fixed drain). Otherwise
/// polls the verifier's consume-progress once a second and returns early once
/// progress has been flat for `idle_threshold` — the consumers have drained the
/// tail, so there is no reason to keep waiting. Bounded by `max` either way, so
/// a consumer that never catches up cannot hang the run. This is what lets a
/// run ending on a heavy fault (reassign/recreate) drain fully without a large
/// fixed `--drain-s`, while fast runs finish quickly.
///
/// **Recreate settle (per topic):** a topic recreate causes a transient
/// consumption stall — the consumer must re-discover / reset-to-EARLIEST onto the
/// recreated topic and re-read it before it resumes on that topic. The GLOBAL
/// progress signal cannot detect this, because the OTHER topics keep being
/// consumed and keep the global counter climbing. So quiescence is suppressed
/// until, for EVERY recreated topic, that topic's OWN per-topic consume progress
/// has climbed past the baseline captured at its recreate (proof the recreated
/// topic itself resumed re-reading). Only when every recreated topic has resumed
/// AND global progress has been flat for `idle_threshold` may the drain quiesce.
/// `max` (--drain-s) still caps the wait, so a topic that never resumes cannot
/// hang the run.
async fn drain_wait(max: Duration, idle_threshold: Duration, verifier: &dyn Verifier, settle: &RecreateSettle) {
    let deadline = std::time::Instant::now() + max;
    if verifier.outstanding().is_some() {
        loop {
            if verifier.outstanding() == Some(0) {
                eprintln!("chaos: drain complete — all delivered records observed");
                return;
            }
            if std::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    // Fallback (verifier does not report `outstanding`): the idle/settle-based
    // drain.
    if idle_threshold.is_zero() {
        tokio::time::sleep(max).await;
        return;
    }
    let mut last_progress = verifier.consumed_progress();
    let mut idle_since = std::time::Instant::now();
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let now_progress = verifier.consumed_progress();
        if now_progress > last_progress {
            last_progress = now_progress;
            idle_since = std::time::Instant::now();
            continue;
        }

        // Global progress has been flat. Before quiescing, require EVERY
        // recreated topic to have resumed re-reading — its per-topic progress
        // must exceed the baseline captured at its recreate. (Re-read live each
        // tick so a recreate that lands mid-drain is honoured.) A topic that has
        // not resumed keeps the drain running (bounded by `max`).
        if idle_since.elapsed() >= idle_threshold {
            let baselines = settle.baselines();
            let all_resumed = baselines
                .iter()
                .all(|(topic, &baseline)| verifier.consumed_progress_for_topic(topic) > baseline);
            if all_resumed {
                eprintln!("chaos: drain quiescent ({now_progress} consumed) — ending drain early");
                return;
            }
            // A recreated topic has not resumed yet — keep draining. Reset the
            // idle timer so we re-evaluate after another flat window rather than
            // spinning this branch every tick.
            idle_since = std::time::Instant::now();
        }
    }
}
